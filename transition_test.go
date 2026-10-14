package governor

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"
)

// ---- 测试基建 ----

// collector 收集跃迁事件（回调并发安全）。
type collector struct {
	mu    sync.Mutex
	trans []Transition
}

func (c *collector) record(tr Transition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trans = append(c.trans, tr)
}

func (c *collector) events() []Transition {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Transition(nil), c.trans...)
}

func (c *collector) kinds() []TransitionKind {
	c.mu.Lock()
	defer c.mu.Unlock()
	kinds := make([]TransitionKind, 0, len(c.trans))
	for _, tr := range c.trans {
		kinds = append(kinds, tr.Kind)
	}
	return kinds
}

// stubSignal 固定压力的测试信号，压力可外部修改以驱动跃迁与归因。
type stubSignal struct {
	name string
	p    float64
}

func (s *stubSignal) Name() string                              { return s.name }
func (s *stubSignal) Observe(time.Time, Outcome, time.Duration) {}
func (s *stubSignal) Pressure(time.Time) float64                { return s.p }

// closeEnough 用于等待秒数断言（Duration→float64 换算有舍入）。
func closeEnough(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// capturingLogger 记录日志事件，验证跃迁日志的内容与级别。
type capturingLogger struct {
	mu      sync.Mutex
	entries []capturedLog
}

type capturedLog struct {
	level slog.Level
	msg   string
	attrs []slog.Attr
}

func (l *capturingLogger) Enabled(slog.Level) bool { return true }

func (l *capturingLogger) Log(level slog.Level, msg string, attrs ...slog.Attr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, capturedLog{level: level, msg: msg, attrs: attrs})
}

func (l *capturingLogger) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries {
		if e.msg == msg {
			n++
		}
	}
	return n
}

func (l *capturingLogger) attrOf(msg, key string) (slog.Value, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if e.msg != msg {
			continue
		}
		for _, a := range e.attrs {
			if a.Key == key {
				return a.Value, true
			}
		}
	}
	return slog.Value{}, false
}

// ---- α 三档跃迁 ----

func TestTransitionSuppressionLifecycle(t *testing.T) {
	c := &collector{}
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk), WithOnTransition(c.record))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 首个 300ms 样本：S=2 → α=2/3，上穿 0.5 → started。
	dec := g.Begin()
	clk.advance(300 * time.Millisecond)
	dec.Record(nil)
	trs := c.events()
	if len(trs) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(trs), c.kinds())
	}
	tr := trs[0]
	if tr.Kind != TransitionSuppressionStarted {
		t.Fatalf("kind = %v, want started", tr.Kind)
	}
	if !almostEqual(tr.From, 0) || !almostEqual(tr.To, 2.0/3.0) {
		t.Fatalf("α 区间 = [%v, %v], want [0, 2/3]", tr.From, tr.To)
	}
	if tr.Driver != "latency" || !almostEqual(tr.DriverPressure, 2) {
		t.Fatalf("归因 = %q/%v, want latency/2", tr.Driver, tr.DriverPressure)
	}

	// 加深：3000ms 样本 → EWMA=1650，S=15.5 → α≈0.939，上穿 0.9 → deep。
	dec = g.Begin()
	clk.advance(3000 * time.Millisecond)
	dec.Record(nil)
	trs = c.events()
	if len(trs) != 2 || trs[1].Kind != TransitionDeepSuppression {
		t.Fatalf("want [started deep], got %v", c.kinds())
	}
	if !almostEqual(trs[1].From, 2.0/3.0) {
		t.Fatalf("deep.From = %v, want 2/3", trs[1].From)
	}

	// 恢复：小样本拉低 EWMA 至目标以下 → α=0 → stopped。
	for i := 0; i < 8 && len(c.events()) < 3; i++ {
		dec = g.Begin()
		clk.advance(10 * time.Millisecond)
		dec.Record(nil)
	}
	trs = c.events()
	if len(trs) != 3 || trs[2].Kind != TransitionSuppressionStopped {
		t.Fatalf("want [started deep stopped], got %v", c.kinds())
	}
	if trs[2].To != 0 {
		t.Fatalf("stopped.To = %v, want 0", trs[2].To)
	}
}

func TestTransitionJumpSkipsDeep(t *testing.T) {
	c := &collector{}
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk), WithOnTransition(c.record))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 首个 2000ms 样本：S=19 → α=0.95，从 0 直接越过 0.9 → 只发 started。
	dec := g.Begin()
	clk.advance(2000 * time.Millisecond)
	dec.Record(nil)
	kinds := c.kinds()
	if len(kinds) != 1 || kinds[0] != TransitionSuppressionStarted {
		t.Fatalf("快速爬坡应跳过 deep，got %v", kinds)
	}
	if to := c.events()[0].To; !almostEqual(to, 0.95) {
		t.Fatalf("To = %v, want 0.95", to)
	}
}

func TestTransitionDriverFlip(t *testing.T) {
	c := &collector{}
	clk := newFakeClock()
	a := &stubSignal{name: "a"}
	b := &stubSignal{name: "b"}
	g, err := New(WithSignals(a, b), WithClock(clk), WithOnTransition(c.record))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 初始无压力：无跃迁。
	dec := g.Begin()
	dec.Record(nil)
	if n := len(c.events()); n != 0 {
		t.Fatalf("无压力不应有跃迁，got %d", n)
	}

	// a 抬压：S=3 → α=0.75 → started，归因 a。
	a.p = 3
	dec = g.Begin()
	dec.Record(nil)
	trs := c.events()
	if len(trs) != 1 || trs[0].Kind != TransitionSuppressionStarted {
		t.Fatalf("want [started], got %v", c.kinds())
	}
	if trs[0].Driver != "a" || !almostEqual(trs[0].DriverPressure, 3) {
		t.Fatalf("归因 = %q/%v, want a/3", trs[0].Driver, trs[0].DriverPressure)
	}

	// 压力来源切换到 b：S=19 → α=0.95（从 0.75 上穿 0.9）→ deep，归因 b。
	a.p = 0.2
	b.p = 19
	dec = g.Begin()
	dec.Record(nil)
	trs = c.events()
	if len(trs) != 2 || trs[1].Kind != TransitionDeepSuppression {
		t.Fatalf("want [started deep], got %v", c.kinds())
	}
	if trs[1].Driver != "b" || !almostEqual(trs[1].DriverPressure, 19) {
		t.Fatalf("归因 = %q/%v, want b/19", trs[1].Driver, trs[1].DriverPressure)
	}
}

// ---- wait 两态跃迁 ----

func TestTransitionWaitKinds(t *testing.T) {
	c := &collector{}
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk),
		WithBaseWait(2*time.Second), WithOnTransition(c.record))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	// α=0：wait=0，无事件。
	g.Wait(ctx)
	if waitKinds(c) != nil {
		t.Fatalf("空压力不应有 wait 跃迁，got %v", waitKinds(c))
	}

	// 一次 300ms 反馈：α=2/3 → wait=T_base×α≈1.333s → engaged。
	dec := g.Begin()
	clk.advance(300 * time.Millisecond)
	dec.Record(nil)
	g.Wait(ctx)
	wk := waitKinds(c)
	if len(wk) != 1 || wk[0] != TransitionWaitEngaged {
		t.Fatalf("want [wait_engaged], got %v", wk)
	}
	eng := waitEvents(c)[0]
	if eng.From != 0 || !closeEnough(eng.To, 4.0/3.0) {
		t.Fatalf("engaged 区间 = [%v, %v], want [0, ≈1.333s]", eng.From, eng.To)
	}

	// α 未变的再次 Wait：值不变不重发。
	g.Wait(ctx)
	if len(waitKinds(c)) != 1 {
		t.Fatalf("wait 值未变不应重发跃迁，got %v", waitKinds(c))
	}

	// 恢复：小样本把 EWMA 拉到目标以下 → α=0 → wait=0 → released。
	for i := 0; i < 8; i++ {
		dec = g.Begin()
		clk.advance(10 * time.Millisecond)
		dec.Record(nil)
		if g.Suppression() == 0 {
			break
		}
	}
	g.Wait(ctx)
	wk = waitKinds(c)
	if len(wk) != 2 || wk[1] != TransitionWaitReleased {
		t.Fatalf("want [wait_engaged wait_released], got %v", wk)
	}
	rel := waitEvents(c)[1]
	if !closeEnough(rel.From, 4.0/3.0) || rel.To != 0 {
		t.Fatalf("released 区间 = [%v, %v], want [≈1.333s, 0]", rel.From, rel.To)
	}
}

func waitKinds(c *collector) []TransitionKind {
	var out []TransitionKind
	for _, k := range c.kinds() {
		switch k {
		case TransitionWaitEngaged, TransitionWaitReleased:
			out = append(out, k)
		}
	}
	return out
}

func waitEvents(c *collector) []Transition {
	var out []Transition
	for _, tr := range c.events() {
		switch tr.Kind {
		case TransitionWaitEngaged, TransitionWaitReleased:
			out = append(out, tr)
		}
	}
	return out
}

// ---- wait 跃迁日志 ----

func TestWaitTransitionLogs(t *testing.T) {
	cl := &capturingLogger{}
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk),
		WithBaseWait(2*time.Second), WithLogger(cl))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	engagedMsg := "governor: wait engaged (wait went above 0)"
	releasedMsg := "governor: wait released (wait reached 0)"

	g.Wait(ctx) // α=0：无跃迁日志
	if n := cl.count(engagedMsg); n != 0 {
		t.Fatalf("空压力不应记 engaged，got %d", n)
	}

	dec := g.Begin()
	clk.advance(300 * time.Millisecond)
	dec.Record(nil)
	g.Wait(ctx)
	if n := cl.count(engagedMsg); n != 1 {
		t.Fatalf("engaged 日志 = %d 条, want 1", n)
	}
	if v, ok := cl.attrOf(engagedMsg, "wait_seconds"); !ok || !closeEnough(v.Float64(), 4.0/3.0) {
		t.Fatalf("engaged 缺少 wait_seconds 归因，got %v", v)
	}
	g.Wait(ctx) // 值未变：不重发
	if n := cl.count(engagedMsg); n != 1 {
		t.Fatalf("engaged 日志应只记一次，got %d", n)
	}

	for i := 0; i < 8; i++ {
		dec = g.Begin()
		clk.advance(10 * time.Millisecond)
		dec.Record(nil)
		if g.Suppression() == 0 {
			break
		}
	}
	g.Wait(ctx)
	if n := cl.count(releasedMsg); n != 1 {
		t.Fatalf("released 日志 = %d 条, want 1", n)
	}
}

// ---- 派发约束 ----

func TestOnTransitionPanicRecovered(t *testing.T) {
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100 * time.Millisecond))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk),
		WithOnTransition(func(Transition) { panic("boom") }))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 触发跃迁：panic 不外溢。
	dec := g.Begin()
	clk.advance(300 * time.Millisecond)
	dec.Record(nil)

	// 回调故障不影响控制路径：α 已抬升，后续反馈照常。
	if a := g.Suppression(); a == 0 {
		t.Fatalf("跃迁后 α 应已抬升")
	}
	dec = g.Begin()
	dec.Record(nil)
}

func TestOnTransitionStructuralRejectedByUpdate(t *testing.T) {
	c := &collector{}
	g, err := New(WithOnTransition(c.record))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Update(WithOnTransition(func(Transition) {})); err == nil {
		t.Fatalf("Update 应拒绝更换跃迁回调")
	}
	if err := g.Update(WithSensitivity(2)); err != nil {
		t.Fatalf("普通参数更新不应受影响: %v", err)
	}
	if _, err := New(WithOnTransition(nil)); err == nil {
		t.Fatalf("nil 回调应报错")
	}
}

func TestOnTransitionSurvivesUpdate(t *testing.T) {
	c := &collector{}
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100 * time.Millisecond))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk), WithOnTransition(c.record))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 运行时调参后回调仍接线：C=0.5，300ms 样本 S=2 → α=0.8 → started。
	if err := g.Update(WithSensitivity(0.5)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	dec := g.Begin()
	clk.advance(300 * time.Millisecond)
	dec.Record(nil)
	kinds := c.kinds()
	if len(kinds) != 1 || kinds[0] != TransitionSuppressionStarted {
		t.Fatalf("Update 后回调应继续生效，got %v", kinds)
	}
}
