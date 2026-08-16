package governor

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

// 期望值全部取自 docs/desc.md §5 的两个完整闭环算例（独立真源）。

// ---- 测试基建 ----

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) Sleep(_ context.Context, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
}

func (c *fakeClock) lastSleep() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sleeps) == 0 {
		return -1
	}
	return c.sleeps[len(c.sleeps)-1]
}

// seqSource 返回固定序列，令 rand.Rand.Float64 依次产出 vals 中预定值。
type seqSource struct {
	vals []int64
	i    int
}

func (s *seqSource) Int63() int64 {
	v := s.vals[s.i%len(s.vals)] & (1<<63 - 1)
	s.i++
	return v
}

func (s *seqSource) Seed(int64) {}

// float64s 将预定的 Float64 结果转为 Int63 序列。
func float64s(fs ...float64) *seqSource {
	vals := make([]int64, len(fs))
	for i, f := range fs {
		vals[i] = int64(f * float64(1<<63))
	}
	return &seqSource{vals: vals}
}

func almostEqual(a, b float64) bool { return math.Abs(a-b) < 1e-12 }

// ---- §5.1 批次收敛闭环 ----

func TestGovernorConvergenceBatch(t *testing.T) {
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk), WithBatch(1000, 1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	steps := []struct {
		sample    time.Duration // 本轮响应耗时
		wantAlpha float64
		wantBatch int
	}{
		{150 * time.Millisecond, 1.0 / 3.0, 666},
		{110 * time.Millisecond, 0.3 / 1.3, 769},
		{90 * time.Millisecond, 0.1 / 1.1, 909},
		{75 * time.Millisecond, 0, 1000},
	}
	for i, step := range steps {
		dec := g.Begin()
		clk.advance(step.sample)
		dec.Record(nil)
		if got := g.Suppression(); !almostEqual(got, step.wantAlpha) {
			t.Fatalf("第%d轮 α = %v, want %v", i+1, got, step.wantAlpha)
		}
		if got := g.BatchSize(); got != step.wantBatch {
			t.Fatalf("第%d轮批次 = %d, want %d", i+1, got, step.wantBatch)
		}
	}
}

// ---- §5.2 退避收敛闭环 ----

func TestGovernorConvergenceBackoff(t *testing.T) {
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(500*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk), WithBaseWait(2000*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	// 第一轮：无先验压力，直接探测
	dec := g.Begin()
	clk.advance(1000 * time.Millisecond)
	dec.Record(nil)

	base := 2000 * time.Millisecond
	// 后续每轮：先按当前 α 退避，再执行同步；Wait 令牌记录退避后那次操作的耗时
	steps := []struct {
		sample   time.Duration
		wantWait time.Duration
	}{
		{600 * time.Millisecond, 1000 * time.Millisecond},
		{300 * time.Millisecond, 750 * time.Millisecond},
		{200 * time.Millisecond, time.Duration(float64(base) * (0.1 / 1.1))},
	}
	for i, step := range steps {
		w := g.Wait(ctx)
		if got, want := clk.lastSleep(), step.wantWait; math.Abs(float64(got-want)) > 10 {
			t.Fatalf("第%d轮等待 = %v, want %v", i+1, got, want)
		}
		clk.advance(step.sample)
		w.Record(nil)
	}
	// 收敛：α=0，等待归零
	g.Wait(ctx)
	if got := clk.lastSleep(); got != 0 {
		t.Fatalf("收敛后等待 = %v, want 0", got)
	}
	if g.Suppression() != 0 {
		t.Fatalf("收敛后 α 应为 0, got %v", g.Suppression())
	}
}

// ---- 概率决策纯函数 ----

func TestDecide(t *testing.T) {
	tests := []struct {
		name                string
		alpha, r, pd, ratio float64
		allowed, probe      bool
	}{
		{"压力内正常放行", 0.8, 0.9, 0.5, 0.02, true, false},
		{"超过α且未中探测", 0.8, 0.5, 0.5, 0.02, false, false},
		{"超过α但中探测", 0.8, 0.5, 0.01, 0.02, true, true},
		{"无压力永不丢弃", 0, 0, 0, 0.02, true, false},
		{"探测关闭", 0.99, 0.5, 0.001, 0, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, p := decide(tt.alpha, tt.r, tt.pd, tt.ratio)
			if a != tt.allowed || p != tt.probe {
				t.Fatalf("decide(%v,%v,%v,%v) = (%v,%v), want (%v,%v)",
					tt.alpha, tt.r, tt.pd, tt.ratio, a, p, tt.allowed, tt.probe)
			}
		})
	}
}

// ---- Do 快速失败 ----

func TestDoSuppressedFastFail(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(time.Millisecond))
	g, err := New(WithSignals(sig), WithClock(clk), withRandSource(float64s(0, 0.5)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 制造深度过载：S = (1s-1ms)/1ms ≈ 999 → α ≈ 0.999
	dec := g.Begin()
	clk.advance(time.Second)
	dec.Record(nil)

	called := false
	_, err = Do(context.Background(), g, func(context.Context) (string, error) {
		called = true
		return "", nil
	})
	if called {
		t.Fatal("被抑制时不得执行业务操作")
	}
	if !errors.Is(err, ErrSuppressed) {
		t.Fatalf("应返回 ErrSuppressed, got %v", err)
	}
}

func TestDoPassesThrough(t *testing.T) {
	g, err := New(withRandSource(float64s(0.9, 0.9)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	v, err := Do(context.Background(), g, func(context.Context) (int, error) {
		return 42, nil
	})
	if err != nil || v != 42 {
		t.Fatalf("Do = (%v, %v), want (42, nil)", v, err)
	}
}

// ---- Decision 生命周期 ----

func TestDecisionRecordIdempotent(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewRejectionSignal(WithRejectionWindow(time.Minute))
	g, _ := New(WithSignals(sig), WithClock(clk))

	dec := g.Begin()
	dec.Record(nil)
	dec.Record(nil) // 第二次应为 no-op

	if got := g.Signals()[0].Pressure; !almostEqual(got, 0) {
		t.Fatalf("重复 Record 不应叠加成功数, 压力 = %v", got)
	}
}

func TestDecisionDeniedRecordIsNoOp(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(time.Millisecond))
	g, _ := New(WithSignals(sig), WithClock(clk), withRandSource(float64s(0, 0.5)))

	dec := g.Begin()
	clk.advance(time.Second)
	dec.Record(nil) // 深度过载

	denied := g.Allow() // r=0 < α, pd=0.5 ≥ ratio → 丢弃
	if denied.Allowed() {
		t.Fatal("过载下应被丢弃")
	}
	pBefore := g.Pressure()
	denied.Record(errors.New("应被忽略")) // 本地丢弃不得污染统计
	if got := g.Pressure(); got != pBefore {
		t.Fatalf("被拒决策的 Record 应为 no-op: %v -> %v", pBefore, got)
	}
}

// ---- 成败分类器 ----

func TestClassifierDefault(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewRejectionSignal()
	g, _ := New(WithSignals(sig), WithClock(clk))

	dec := g.Begin()
	clk.advance(time.Millisecond)
	dec.Record(nil)
	if p := g.Signals()[0].Pressure; p != 0 {
		t.Fatalf("成功不应产生压力, got %v", p)
	}

	dec2 := g.Begin()
	dec2.Record(errors.New("boom"))
	dec3 := g.Begin()
	dec3.Record(errors.New("boom"))
	// 1 成功 + 2 失败：S = max(0, (3 - 2×1)/(3+1)) = 0.25
	if p := g.Signals()[0].Pressure; !almostEqual(p, 0.25) {
		t.Fatalf("默认非 nil 错误应计为失败, 压力 = %v, want 0.25", p)
	}
}

func TestClassifierCustom(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewRejectionSignal()
	sentinel := errors.New("biz-neutral")
	g, _ := New(WithSignals(sig), WithClock(clk),
		WithClassifier(func(err error) Outcome {
			if errors.Is(err, sentinel) {
				return OutcomeIgnore
			}
			if err == nil {
				return OutcomeSuccess
			}
			return OutcomeFailure
		}))

	for i := 0; i < 3; i++ {
		dec := g.Begin()
		dec.Record(sentinel)
	}
	if p := g.Signals()[0].Pressure; p != 0 {
		t.Fatalf("Ignore 不应计入统计, got %v", p)
	}
}

// ---- 只读暴露 ----

func TestSignalsSnapshot(t *testing.T) {
	clk := newFakeClock()
	lat, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	rej, _ := NewRejectionSignal()
	g, _ := New(WithSignals(lat, rej), WithClock(clk))

	dec := g.Begin()
	clk.advance(150 * time.Millisecond)
	dec.Record(nil)

	snap := g.Signals()
	if len(snap) != 2 {
		t.Fatalf("快照应含 2 个信号, got %d", len(snap))
	}
	byName := map[string]float64{}
	for _, s := range snap {
		byName[s.Name] = s.Pressure
	}
	if !almostEqual(byName["latency"], 0.5) || byName["rejection"] != 0 {
		t.Fatalf("快照 = %v, want latency=0.5 rejection=0", byName)
	}
	if !almostEqual(g.Pressure(), 0.5) {
		t.Fatalf("合成压力 = %v, want 0.5", g.Pressure())
	}
}

// ---- 参数校验 ----

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{"敏感度非正", WithSensitivity(0)},
		{"探测比例越界", WithProbeRatio(1)},
		{"批次下限小于1", WithBatch(100, 0)},
		{"批次上限小于下限", WithBatch(1, 10)},
		{"空指标前缀", WithMetricPrefix("")},
		{"nil 信号", WithSignals(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.opt); err == nil {
				t.Fatal("应返回校验错误")
			}
		})
	}
}

// ---- 并发冒烟：混合路径下无数据竞争 ----

func TestConcurrentMixedPaths(t *testing.T) {
	clk := newFakeClock()
	lat, _ := NewLatencySignal(WithLatencyTarget(50*time.Millisecond), WithLatencyBeta(0.5))
	rej, _ := NewRejectionSignal(WithRejectionWindow(10 * time.Second))
	g, err := New(
		WithSignals(lat, rej),
		WithClock(clk),
		WithBatch(100, 1),
		WithBaseWait(10*time.Millisecond),
		WithMeter(newFakeMeter()),
		WithLogger(&fakeLogger{}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				switch (i + j) % 4 {
				case 0:
					dec := g.Allow()
					if dec.Allowed() {
						dec.Record(nil)
					}
				case 1:
					_ = g.BatchSize()
				case 2:
					g.Wait(context.Background()).Record(nil)
				case 3:
					_ = g.Pressure()
					_ = g.Suppression()
					_ = g.Signals()
				}
			}
		}(i)
	}
	wg.Wait()
}

// ---- 运行时参数更新 ----

func TestUpdateAppliesTuningParams(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	lg := &fakeLogger{}
	g, _ := New(WithSignals(sig), WithClock(clk), WithLogger(lg), WithBatch(100, 1))

	// 制造压力：S=0.5
	dec := g.Begin()
	clk.advance(150 * time.Millisecond)
	dec.Record(nil)

	before := g.Suppression()
	if err := g.Update(
		WithSensitivity(0.1),
		WithBatch(10, 5),
		WithBaseWait(2*time.Second),
		WithProbeRatio(0.1),
	); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// C 从 1 降到 0.1：同压力下 α 应显著升高
	after := g.Suppression()
	if after <= before {
		t.Fatalf("α 未随 C 降低而升高: before=%v after=%v", before, after)
	}
	if want := SuppressionFactor(0.5, 0.1); !almostEqual(after, want) {
		t.Fatalf("α = %v, want %v", after, want)
	}
	// 批次上下限立即生效：α≈0.833 → floor(10×0.167)=1 < min → 5
	if got := g.BatchSize(); got != 5 {
		t.Fatalf("BatchSize = %d, want 5（新下限）", got)
	}
	// 基准等待立即生效：wait = 2s × α
	g.Wait(context.Background())
	if want := time.Duration(float64(2 * time.Second) * after); clk.lastSleep() != want {
		t.Fatalf("Wait = %v, want %v（新基准 2s × α）", clk.lastSleep(), want)
	}
	// 更新成功应记一条 info 日志
	found := false
	for _, e := range lg.snapshot() {
		if e.msg == "governor: config updated" {
			found = true
		}
	}
	if !found {
		t.Fatal("缺少 config updated 日志")
	}
}

func TestUpdateIsAtomic(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(WithSignals(sig), WithClock(clk))

	dec := g.Begin()
	clk.advance(150 * time.Millisecond)
	dec.Record(nil)
	before := g.Suppression()

	// 合法 option 后跟非法 option：整体失败
	if err := g.Update(WithSensitivity(0.1), WithSensitivity(-1)); err == nil {
		t.Fatal("应返回校验错误")
	}
	if got := g.Suppression(); !almostEqual(got, before) {
		t.Fatalf("失败的 Update 改变了配置: %v, want %v", got, before)
	}

	// 校验失败（batch max < min）
	if err := g.Update(WithBatch(1, 10)); err == nil {
		t.Fatal("应返回校验错误")
	}
	if got := g.Suppression(); !almostEqual(got, before) {
		t.Fatalf("失败的 Update 改变了配置: %v, want %v", got, before)
	}
}

func TestUpdateRejectsStructuralOptions(t *testing.T) {
	sig, _ := NewLatencySignal()
	other, _ := NewRejectionSignal()
	g, _ := New(WithSignals(sig), WithBatch(100, 1))
	structural := []Option{
		WithSignals(other),
		WithClassifier(func(error) Outcome { return OutcomeSuccess }),
		WithClock(newFakeClock()),
		WithMeter(newFakeMeter()),
		WithLogger(&fakeLogger{}),
		WithMetricPrefix("other"),
		withRandSource(float64s(0.5)),
	}
	for _, opt := range structural {
		if err := g.Update(WithSensitivity(0.5), opt); err == nil {
			t.Fatalf("结构性 option 应被 Update 拒绝: %v", opt)
		}
	}
	// 拒绝后调节参数也不得生效
	if err := g.Update(WithSensitivity(0.5)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !almostEqual(g.Suppression(), SuppressionFactor(0, 0.5)) {
		t.Fatalf("结构拒绝后配置应保持不变")
	}
}

func TestUpdateConcurrentWithHotPaths(t *testing.T) {
	clk := newFakeClock()
	lat, _ := NewLatencySignal(WithLatencyTarget(50*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(
		WithSignals(lat),
		WithClock(clk),
		WithBatch(100, 1),
		WithBaseWait(time.Millisecond),
		WithMeter(newFakeMeter()),
		WithLogger(&fakeLogger{}),
	)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = g.Update(WithSensitivity(0.5), WithProbeRatio(0.05), WithBatch(50, 2))
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				dec := g.Allow()
				if dec.Allowed() {
					dec.Record(nil)
				}
				_ = g.BatchSize()
				_ = g.Pressure()
				_ = g.Suppression()
			}
		}()
	}
	wg.Wait()
}
