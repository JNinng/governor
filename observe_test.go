package governor

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jninng/observ"
)

// ---- 假 Meter / 假 Logger（公共接口 observ.Meter / observ.Logger 上的替身）----

type fakeCounter struct {
	mu sync.Mutex
	v  float64
}

func (c *fakeCounter) Inc() { c.Add(1) }
func (c *fakeCounter) Add(v float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v += v
}
func (c *fakeCounter) value() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}

type fakeGauge struct {
	mu sync.Mutex
	v  float64
}

func (g *fakeGauge) Set(v float64) { g.mu.Lock(); defer g.mu.Unlock(); g.v = v }
func (g *fakeGauge) Add(v float64) { g.mu.Lock(); defer g.mu.Unlock(); g.v += v }
func (g *fakeGauge) value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v
}

type fakeHistogram struct {
	mu   sync.Mutex
	vals []float64
}

func (h *fakeHistogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vals = append(h.vals, v)
}
func (h *fakeHistogram) values() []float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]float64, 0, len(h.vals))
	return append(out, h.vals...)
}

type fakeMeter struct {
	mu        sync.Mutex
	counters  map[string]*fakeCounter
	gauges    map[string]*fakeGauge
	histogram map[string]*fakeHistogram
}

func newFakeMeter() *fakeMeter {
	return &fakeMeter{
		counters:  map[string]*fakeCounter{},
		gauges:    map[string]*fakeGauge{},
		histogram: map[string]*fakeHistogram{},
	}
}

func (m *fakeMeter) NewCounter(name, _ string) observ.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] = &fakeCounter{}
	return m.counters[name]
}

func (m *fakeMeter) NewGauge(name, _ string) observ.Gauge {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = &fakeGauge{}
	return m.gauges[name]
}

func (m *fakeMeter) NewHistogram(name, _ string, _ []float64) observ.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histogram[name] = &fakeHistogram{}
	return m.histogram[name]
}

func (m *fakeMeter) counter(name string) *fakeCounter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

func (m *fakeMeter) gauge(name string) *fakeGauge {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gauges[name]
}

func (m *fakeMeter) histogramValues(name string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h := m.histogram[name]; h != nil {
		return h.values()
	}
	return nil
}

type logEntry struct {
	level slog.Level
	msg   string
	attrs []slog.Attr
}

// attr 返回日志条目中指定 key 的属性值。
func (e logEntry) attr(key string) (slog.Value, bool) {
	for _, a := range e.attrs {
		if a.Key == key {
			return a.Value, true
		}
	}
	return slog.Value{}, false
}

type fakeLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

func (l *fakeLogger) Enabled(slog.Level) bool { return true }
func (l *fakeLogger) Log(level slog.Level, msg string, attrs ...slog.Attr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level, msg, attrs})
}

func (l *fakeLogger) snapshot() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]logEntry(nil), l.entries...)
}

// ---- 指标命名 ----

func TestMetricNames(t *testing.T) {
	m := newFakeMeter()
	if _, err := New(WithMeter(m)); err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{
		"governor_pressure", "governor_suppression",
		"governor_decisions_allow_total", "governor_decisions_drop_total",
		"governor_decisions_probe_total", "governor_batch_size",
		"governor_wait_seconds", "governor_operations_seconds",
	} {
		if m.gauge(name) == nil && m.counter(name) == nil && m.histogramValues(name) == nil {
			t.Errorf("缺少默认前缀指标 %s", name)
		}
	}
}

func TestMetricPrefix(t *testing.T) {
	m := newFakeMeter()
	if _, err := New(WithMeter(m), WithMetricPrefix("redis")); err != nil {
		t.Fatalf("New: %v", err)
	}
	if m.gauge("redis_pressure") == nil {
		t.Fatal("缺少自定义前缀指标 redis_pressure")
	}
	if m.gauge("governor_pressure") != nil {
		t.Fatal("不应再创建默认前缀指标")
	}
}

// ---- 决策计数（枚举拆名，无 label）----

func TestDecisionCounters(t *testing.T) {
	t.Run("正常放行", func(t *testing.T) {
		m := newFakeMeter()
		g, _ := New(WithMeter(m), withRandSource(float64s(0.9, 0.9)))
		g.Allow()
		if got := m.counter("governor_decisions_allow_total").value(); got != 1 {
			t.Fatalf("allow 计数 = %v, want 1", got)
		}
	})
	t.Run("丢弃", func(t *testing.T) {
		m := newFakeMeter()
		clk := newFakeClock()
		sig, _ := NewLatencySignal(WithLatencyTarget(time.Millisecond))
		g, _ := New(WithMeter(m), WithClock(clk), WithSignals(sig), withRandSource(float64s(0, 0.5)))
		dec := g.Begin()
		clk.advance(time.Second)
		dec.Record(nil) // 深度过载
		g.Allow()       // r=0 < α, pd=0.5 ≥ ratio → 丢弃
		if got := m.counter("governor_decisions_drop_total").value(); got != 1 {
			t.Fatalf("drop 计数 = %v, want 1", got)
		}
	})
	t.Run("探测放行", func(t *testing.T) {
		m := newFakeMeter()
		clk := newFakeClock()
		sig, _ := NewLatencySignal(WithLatencyTarget(time.Millisecond))
		g, _ := New(WithMeter(m), WithClock(clk), WithSignals(sig), withRandSource(float64s(0, 0)))
		dec := g.Begin()
		clk.advance(time.Second)
		dec.Record(nil)
		g.Allow() // r=0 < α, pd=0 < ratio → 探测
		if got := m.counter("governor_decisions_probe_total").value(); got != 1 {
			t.Fatalf("probe 计数 = %v, want 1", got)
		}
	})
}

// ---- 状态仪表与耗时直方图 ----

func TestStateGaugesAndHistogram(t *testing.T) {
	m := newFakeMeter()
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(WithMeter(m), WithClock(clk), WithSignals(sig))

	dec := g.Begin()
	clk.advance(150 * time.Millisecond)
	dec.Record(nil)

	if got := m.gauge("governor_pressure").value(); got != 0.5 {
		t.Fatalf("压力仪表 = %v, want 0.5", got)
	}
	want := SuppressionFactor(0.5, 1)
	if got := m.gauge("governor_suppression").value(); got != want {
		t.Fatalf("抑制仪表 = %v, want %v", got, want)
	}
	vals := m.histogramValues("governor_operations_seconds")
	if len(vals) != 1 || vals[0] != 0.15 {
		t.Fatalf("操作耗时直方图 = %v, want [0.15]", vals)
	}
}

// ---- 日志跃迁（热路径不落日志，仅状态跃迁）----

func TestLoggerTransitions(t *testing.T) {
	lg := &fakeLogger{}
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(WithLogger(lg), WithClock(clk), WithSignals(sig))

	// 1000ms 采样：S=9 → α≈0.9，上穿 0.5 → WARN
	dec := g.Begin()
	clk.advance(1000 * time.Millisecond)
	dec.Record(nil)

	// 连续 0 采样把 EWMA 拉回目标以下：500→250→125→62.5，α 归零 → INFO
	for i := 0; i < 4; i++ {
		d := g.Begin()
		d.Record(nil)
	}

	entries := lg.snapshot()
	if len(entries) != 2 {
		t.Fatalf("日志条目 = %d, want 2: %+v", len(entries), entries)
	}
	if entries[0].level != slog.LevelWarn || entries[1].level != slog.LevelInfo {
		t.Fatalf("级别 = [%v %v], want [WARN INFO]", entries[0].level, entries[1].level)
	}
	// 框架日志固定英文，简单词汇并自带判定依据
	if entries[0].msg != "governor: suppression started (alpha went above 0.5)" ||
		entries[1].msg != "governor: suppression stopped (alpha reached 0)" {
		t.Fatalf("日志消息 = [%q %q], want 英文跃迁消息", entries[0].msg, entries[1].msg)
	}
}

// ---- 缺省观测为 Noop ----

func TestDefaultObservabilityIsNoop(t *testing.T) {
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(time.Millisecond))
	g, err := New(WithSignals(sig), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dec := g.Begin()
	clk.advance(time.Second)
	dec.Record(nil) // 不应 panic
	_ = g.BatchSize()
	_ = g.Wait(context.Background())
	g.Allow()
}

// ---- 跃迁日志信号归因与深度抑制档位 ----

func TestLoggerTransitionsDriverAndDeep(t *testing.T) {
	lg := &fakeLogger{}
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(WithLogger(lg), WithClock(clk), WithSignals(sig))

	// 1000ms 采样：S=9 → α=0.9，上穿 0.5 → WARN（started）
	dec := g.Begin()
	clk.advance(1000 * time.Millisecond)
	dec.Record(nil)
	// 第二次采样耗时 1100ms：EWMA=1050 → S=9.5 → α≈0.905，上穿 0.9 → WARN（deep）
	dec = g.Begin()
	clk.advance(1100 * time.Millisecond)
	dec.Record(nil)

	entries := lg.snapshot()
	if len(entries) != 2 {
		t.Fatalf("日志条目 = %d, want 2: %+v", len(entries), entries)
	}
	if entries[0].msg != "governor: suppression started (alpha went above 0.5)" {
		t.Fatalf("首条消息 = %q", entries[0].msg)
	}
	if entries[1].level != slog.LevelWarn ||
		entries[1].msg != "governor: deep suppression (alpha went above 0.9)" {
		t.Fatalf("深度抑制日志 = [%v %q]", entries[1].level, entries[1].msg)
	}
	// 归因：驱动信号应为 latency（唯一信号，也是压力最大者）
	for i, e := range entries {
		if v, ok := e.attr("driver_signal"); !ok || v.String() != "latency" {
			t.Fatalf("日志[%d] 缺少 driver_signal=latency 归因: %+v", i, e.attrs)
		}
		if _, ok := e.attr("driver_pressure"); !ok {
			t.Fatalf("日志[%d] 缺少 driver_pressure: %+v", i, e.attrs)
		}
	}
}
