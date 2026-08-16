package governor

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jninng/observ"
)

// 执行层：把抑制因子翻译为三种控制动作（丢弃、批次、等待），
// 并维护 Decision 反馈令牌。见 docs/desc.md §4。

// ErrSuppressed 是操作被本地丢弃时的快速失败哨兵错误。
// 降级响应（默认值、兜底缓存等）由业务层基于此错误自行构造（ADR-0002）。
var ErrSuppressed = errors.New("governor: operation suppressed")

// Clock 是可注入的时间源，供测试模拟收敛过程。
type Clock interface {
	Now() time.Time
	// Sleep 阻塞约 d，ctx 取消时应尽快返回。
	Sleep(ctx context.Context, d time.Duration)
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) Sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// SignalSnapshot 是信号的只读观测值（暴露面，非控制动作）。
type SignalSnapshot struct {
	Name     string
	Pressure float64
}

// Governor 是绑定单一远端目标的流控器：聚合压力信号，输出抑制决策。
// 并发安全；滑动窗口惰性过期，无后台协程。
type Governor struct {
	mu      sync.Mutex
	signals []Signal
	cfg     *config

	alpha float64 // 最近一次计算的 α，用于跃迁判断与仪表
	rnd   *rand.Rand

	lastBatch int
	lastWait  time.Duration

	metrics *metrics
	logger  logger
}

// New 创建 Governor。无信号时压力恒为 0（全部放行），仅作透传。
func New(opts ...Option) (*Governor, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	var ol observ.Logger = cfg.logger
	if ol == nil {
		ol = observ.DefaultLogger()
	}
	m := cfg.meter
	if m == nil {
		m = observ.NoopMeter
	}
	return &Governor{
		signals: cfg.signals,
		cfg:     cfg,
		rnd:     rand.New(cfg.source),
		metrics: newMetrics(m, cfg.prefix),
		logger:  logger{l: ol},
	}, nil
}

// Begin 起表并返回反馈令牌，不做丢弃判定。
// 用于批处理（配 BatchSize）等自带节奏的场景。
func (g *Governor) Begin() *Decision {
	return g.newDecision(true, false)
}

// Allow 是策略一（概率性丢弃 + 探测放行）：
// 以概率 α 丢弃；对将丢弃的请求按 probeRatio 放行为探测，
// 实际放行率 = (1-α) + α×probeRatio。
func (g *Governor) Allow() *Decision {
	alpha := g.currentAlpha()
	r, pd := g.drawTwoUniforms()
	allowed, probe := decide(alpha, r, pd, g.cfg.probeRatio)
	switch {
	case probe:
		g.metrics.probe.Inc()
	case allowed:
		g.metrics.allow.Inc()
	default:
		g.metrics.drop.Inc()
	}
	return g.newDecision(allowed, probe)
}

// Wait 是策略三（延迟执行）：按 T_base × α 休眠后返回反馈令牌。
// 延迟而不丢弃——没有任何请求被丢，只是延后发出；不是失败后的退避重试（ADR-0002）。
// ctx 在调用时已取消/超时的，跳过睡眠直接返回令牌。
func (g *Governor) Wait(ctx context.Context) *Decision {
	alpha := g.currentAlpha()
	wait := Backoff(g.cfg.baseWait, alpha)
	g.mu.Lock()
	changed := wait != g.lastWait
	from := g.lastWait
	if changed {
		g.lastWait = wait
		g.metrics.waitSeconds.Set(seconds(wait))
	}
	g.mu.Unlock()
	if changed {
		g.logger.debug("governor: wait changed",
			slogAttrFloat("from_seconds", seconds(from)),
			slogAttrFloat("to_seconds", seconds(wait)))
	}
	if ctx.Err() == nil {
		g.cfg.clock.Sleep(ctx, wait)
	}
	return g.newDecision(true, false)
}

// BatchSize 是策略二（批次大小动态调整）：max(min, floor(max×(1-α)))。
func (g *Governor) BatchSize() int {
	n := ScaleBatch(g.cfg.batchMax, g.cfg.batchMin, g.currentAlpha())
	g.mu.Lock()
	changed := n != g.lastBatch
	from := g.lastBatch
	if changed {
		g.lastBatch = n
		g.metrics.batchSize.Set(float64(n))
	}
	g.mu.Unlock()
	if changed {
		g.logger.debug("governor: batch size changed",
			slogAttrInt("from", from),
			slogAttrInt("to", n))
	}
	return n
}

// Pressure 返回多信号合成后的当前压力指数 S（最差信号优先）。
func (g *Governor) Pressure() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pressureLocked(g.cfg.clock.Now())
}

// Suppression 返回当前抑制因子 α。
func (g *Governor) Suppression() float64 {
	return SuppressionFactor(g.Pressure(), g.cfg.c)
}

// Signals 返回各信号的只读快照，供业务自定义决策使用（观测非控制）。
func (g *Governor) Signals() []SignalSnapshot {
	now := g.cfg.clock.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]SignalSnapshot, len(g.signals))
	for i, s := range g.signals {
		out[i] = SignalSnapshot{Name: s.Name(), Pressure: s.Pressure(now)}
	}
	return out
}

// Do 包装一次远端操作：被抑制时直接快速失败（ErrSuppressed），
// 否则执行 op 并自动反馈结局与耗时。
func Do[T any](ctx context.Context, g *Governor, op func(context.Context) (T, error)) (T, error) {
	var zero T
	dec := g.Allow()
	if !dec.Allowed() {
		return zero, ErrSuppressed
	}
	v, err := op(ctx)
	dec.Record(err)
	return v, err
}

// Decision 是一次决策的反馈令牌：携带起始时间、放行与探测标记，
// Record 幂等；被本地丢弃的决策其 Record 为安全 no-op（统计口径见 desc.md §3.1）。
type Decision struct {
	g        *Governor
	started  time.Time
	allowed  bool
	probe    bool
	recorded atomic.Bool
}

// Allowed 报告本次决策是否放行。
func (d *Decision) Allowed() bool { return d != nil && d.allowed }

// Probe 报告本次放行是否为探测请求。
func (d *Decision) Probe() bool { return d != nil && d.probe }

// Record 反馈操作结局：按分类器归一为 Outcome 后喂入各信号，
// 并重算 S/α、更新仪表、记录状态跃迁日志。重复调用只生效一次。
func (d *Decision) Record(err error) {
	if d == nil || !d.allowed || d.recorded.Swap(true) {
		return
	}
	now := d.g.cfg.clock.Now()
	took := now.Sub(d.started)
	outcome := d.g.cfg.classifier(err)

	if outcome != OutcomeIgnore {
		d.g.metrics.opSeconds.Observe(seconds(took))
	}

	g := d.g
	g.mu.Lock()
	for _, s := range g.signals {
		s.Observe(now, outcome, took)
	}
	g.recomputeLocked(now)
	g.mu.Unlock()
}

// ---- 内部 ----

func (g *Governor) newDecision(allowed, probe bool) *Decision {
	return &Decision{
		g:       g,
		started: g.cfg.clock.Now(),
		allowed: allowed,
		probe:   probe,
	}
}

// currentAlpha 按当前信号状态即时计算 α（只读，不落仪表不记日志）。
func (g *Governor) currentAlpha() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return SuppressionFactor(g.pressureLocked(g.cfg.clock.Now()), g.cfg.c)
}

// pressureLocked 计算多信号合成压力，须持有 g.mu。
func (g *Governor) pressureLocked(now time.Time) float64 {
	ps := make([]float64, len(g.signals))
	for i, s := range g.signals {
		ps[i] = s.Pressure(now)
	}
	return MaxPressure(ps...)
}

// recomputeLocked 在反馈后重算 α 并处理仪表与跃迁日志，须持有 g.mu。
func (g *Governor) recomputeLocked(now time.Time) {
	s := g.pressureLocked(now)
	a := SuppressionFactor(s, g.cfg.c)
	prev := g.alpha
	g.alpha = a

	g.metrics.pressure.Set(s)
	g.metrics.suppression.Set(a)

	switch {
	case prev <= 0.5 && a > 0.5:
		g.logger.warn("governor: suppression started (alpha went above 0.5)",
			slogAttrFloat("pressure", s),
			slogAttrFloat("suppression", a))
	case prev > 0 && a == 0:
		g.logger.info("governor: suppression stopped (alpha reached 0)",
			slogAttrFloat("pressure", s),
			slogAttrFloat("suppression", a))
	}
}

// drawTwoUniforms 产出两个均匀随机数（一个作丢弃判定、一个作探测抽样）。
// rand.Rand 非并发安全；调用方（Allow）不持 g.mu，故由本方法自行加锁保护 rnd。
func (g *Governor) drawTwoUniforms() (float64, float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rnd.Float64(), g.rnd.Float64()
}

// decide 是概率决策纯函数：r < α 触发丢弃，除非探测抽样 pd < ratio 放行为探测。
func decide(alpha, r, pd, ratio float64) (allowed, probe bool) {
	if r >= alpha {
		return true, false
	}
	if pd < ratio {
		return true, true
	}
	return false, false
}
