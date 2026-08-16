package governor

import (
	"fmt"
	"sync"
	"time"
)

// 感知层：把异构反馈（拒绝计数、响应时长）归一化为压力指数 S >= 0。
// 见 docs/desc.md §3.1、§3.2。

// Outcome 是一次实际发往远端的操作的结局分类。
type Outcome int

const (
	// OutcomeSuccess 计入成功数。
	OutcomeSuccess Outcome = iota
	// OutcomeFailure 计入总数但计入失败（压力来源）。
	OutcomeFailure
	// OutcomeIgnore 不计入任何统计（如调用方主动取消）。
	OutcomeIgnore
)

// Classifier 判定一次操作结局。nil 错误默认为成功，非 nil 错误默认为失败，
// 业务可注入更细的语义（如把限流类错误归为 Ignore）。
type Classifier func(err error) Outcome

// Signal 是一类反馈输入到压力指数的独立计算单元，可脱离 Governor 单独复用。
// 实现须并发安全。Pressure 只读无副作用（不修改任何统计状态）；
// 统计饥饿（窗口无样本）时沿用上一次非空值，从未有样本时返回 0
// （冷启动语义：区分“无反馈”与“无压力”，见 desc.md §3.1）。
type Signal interface {
	// Name 返回信号名，用于快照与日志标识。
	Name() string
	// Observe 喂入一次实际发往远端的操作结局。
	// 本地丢弃的操作不得喂入（统计口径见 desc.md §3.1 / ADR-0002）。
	Observe(now time.Time, outcome Outcome, took time.Duration)
	// Pressure 返回当前压力指数 S，恒 >= 0。
	Pressure(now time.Time) float64
}

// ---- 场景 A：基于请求拒绝的反馈 ----

const (
	defaultRejectionK      = 2.0
	defaultRejectionWindow = 90 * time.Second
	rejectionBucketCount   = 60
)

// RejectionSignal 以滑动时间窗口内的成败计数计算压力：
// S = max(0, (N_total - K·N_success) / (N_total + 1))。
// 成功率高于 1/K 时压力恒为 0（SRE/gRPC 同类公式，desc.md §3.2 场景 A）。
type RejectionSignal struct {
	mu      sync.Mutex
	k       float64
	window  time.Duration
	width   time.Duration // 桶宽 = window/60
	buckets []rejBucket
	last    float64 // 窗口耗尽时沿用的上一次压力
}

type rejBucket struct {
	start   time.Time
	total   float64
	success float64
}

// RejectionOption 配置 RejectionSignal。
type RejectionOption func(*RejectionSignal) error

// WithRejectionK 设置调节系数 K（默认 2.0，即容忍约 50% 失败率），须 > 0。
func WithRejectionK(k float64) RejectionOption {
	return func(s *RejectionSignal) error {
		if k <= 0 {
			return fmt.Errorf("governor: rejection K must be positive, got %v", k)
		}
		s.k = k
		return nil
	}
}

// WithRejectionWindow 设置聚合窗口（默认 90s），须 > 0。
func WithRejectionWindow(d time.Duration) RejectionOption {
	return func(s *RejectionSignal) error {
		if d <= 0 {
			return fmt.Errorf("governor: rejection window must be positive, got %v", d)
		}
		s.window = d
		return nil
	}
}

// NewRejectionSignal 创建基于成败计数的压力信号。
func NewRejectionSignal(opts ...RejectionOption) (*RejectionSignal, error) {
	s := &RejectionSignal{
		k:      defaultRejectionK,
		window: defaultRejectionWindow,
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	s.width = s.window / rejectionBucketCount
	return s, nil
}

// Name 实现 Signal。
func (s *RejectionSignal) Name() string { return "rejection" }

// Observe 实现 Signal。took 参数被忽略（该信号只关心成败）。
func (s *RejectionSignal) Observe(now time.Time, outcome Outcome, _ time.Duration) {
	if outcome == OutcomeIgnore {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)
	start := now.Truncate(s.width)
	if n := len(s.buckets); n == 0 || !s.buckets[n-1].start.Equal(start) {
		if len(s.buckets) == rejectionBucketCount+1 {
			s.buckets = s.buckets[1:]
		}
		s.buckets = append(s.buckets, rejBucket{start: start})
	}
	b := &s.buckets[len(s.buckets)-1]
	b.total++
	if outcome == OutcomeSuccess {
		b.success++
	}
	// 在采样点同步更新 last，使 Pressure 成为纯读（无样本窗口沿用该值）。
	s.last = s.pressureLocked()
}

// Pressure 实现 Signal（只读，不修改任何状态）。
func (s *RejectionSignal) Pressure(now time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)
	return s.pressureLocked()
}

// pressureLocked 由当前窗口计数计算压力，须持有 s.mu。
func (s *RejectionSignal) pressureLocked() float64 {
	var total, success float64
	for _, b := range s.buckets {
		total += b.total
		success += b.success
	}
	if total == 0 {
		return s.last
	}
	return clampNonNeg((total - s.k*success) / (total + 1))
}

// expireLocked 惰性清理过期桶，不启后台协程。
func (s *RejectionSignal) expireLocked(now time.Time) {
	cutoff := now.Add(-s.window)
	drop := 0
	for _, b := range s.buckets {
		if b.start.Add(s.width).After(cutoff) {
			break
		}
		drop++
	}
	if drop > 0 {
		s.buckets = s.buckets[drop:]
	}
}

// ---- 场景 B：基于响应时长的反馈 ----

const (
	defaultLatencyTarget = 200 * time.Millisecond
	defaultLatencyBeta   = 0.4
	defaultLatencyWindow = 90 * time.Second
)

// LatencySignal 以 EWMA 平滑后的响应耗时计算压力：
// S = max(0, (L_actual - L_target) / L_target)，L_actual 为 EWMA 估计值
// （desc.md §3.2 场景 B）。平滑系数 β 越大越平滑。
// 超过 window 无新采样时沿用上一次非空压力（饥饿语义同 RejectionSignal）。
type LatencySignal struct {
	mu         sync.Mutex
	target     time.Duration
	beta       float64
	window     time.Duration // 采样饥饿阈值
	ewma       time.Duration
	has        bool
	lastSample time.Time
	last       float64 // 饥饿时沿用的上一次压力
}

// LatencyOption 配置 LatencySignal。
type LatencyOption func(*LatencySignal) error

// WithLatencyTarget 设置目标延迟 L_target（默认 200ms），须 > 0。
func WithLatencyTarget(d time.Duration) LatencyOption {
	return func(s *LatencySignal) error {
		if d <= 0 {
			return fmt.Errorf("governor: latency target must be positive, got %v", d)
		}
		s.target = d
		return nil
	}
}

// WithLatencyBeta 设置 EWMA 平滑系数 β（默认 0.4，建议 0.3~0.5），须在 (0,1) 内。
func WithLatencyBeta(b float64) LatencyOption {
	return func(s *LatencySignal) error {
		if b <= 0 || b >= 1 {
			return fmt.Errorf("governor: latency beta must be in (0,1), got %v", b)
		}
		s.beta = b
		return nil
	}
}

// WithLatencyWindow 设置采样饥饿阈值（默认 90s，与 RejectionSignal 窗口对齐），
// 超过该时长无新采样时压力沿用上一次非空值，须 > 0。
func WithLatencyWindow(d time.Duration) LatencyOption {
	return func(s *LatencySignal) error {
		if d <= 0 {
			return fmt.Errorf("governor: latency window must be positive, got %v", d)
		}
		s.window = d
		return nil
	}
}

// NewLatencySignal 创建基于响应时长的压力信号。
func NewLatencySignal(opts ...LatencyOption) (*LatencySignal, error) {
	s := &LatencySignal{
		target: defaultLatencyTarget,
		beta:   defaultLatencyBeta,
		window: defaultLatencyWindow,
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Name 实现 Signal。
func (s *LatencySignal) Name() string { return "latency" }

// Observe 实现 Signal。成败均计入（超时的长耗时同样是压力信号），Ignore 跳过。
func (s *LatencySignal) Observe(now time.Time, outcome Outcome, took time.Duration) {
	if outcome == OutcomeIgnore {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has {
		s.ewma = took
		s.has = true
	} else {
		s.ewma = time.Duration(s.beta*float64(s.ewma) + (1-s.beta)*float64(took))
	}
	// 在采样点同步更新 last，使 Pressure 成为纯读（饥饿期沿用该值）。
	s.lastSample = now
	s.last = s.ewmaPressureLocked()
}

// Pressure 实现 Signal（只读，不修改任何状态）。
// 超过 window 无采样时视为饥饿，沿用上一次非空压力（desc.md §3.1）。
func (s *LatencySignal) Pressure(now time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has {
		return 0
	}
	if now.After(s.lastSample.Add(s.window)) {
		return s.last
	}
	return s.ewmaPressureLocked()
}

// ewmaPressureLocked 由当前 EWMA 估计计算压力，须持有 s.mu。
func (s *LatencySignal) ewmaPressureLocked() float64 {
	return clampNonNeg(float64(s.ewma-s.target) / float64(s.target))
}

func clampNonNeg(v float64) float64 {
	if v < 0 {
		return 0
	}
	return v
}
