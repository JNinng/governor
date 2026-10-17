package governor

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ---- 测试替身：真实 provider 上的 Meter（otel 同步仪表接口密封，无法自造假仪表）----

func newTestMeter(t *testing.T) (metric.Meter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider.Meter("governor_test"), reader
}

// recordingMeter 包装真实 Meter 并记录创建的仪表名，验证前缀生成。
type recordingMeter struct {
	metric.Meter
	mu    sync.Mutex
	names []string
}

func newRecordingMeter(t *testing.T) *recordingMeter {
	t.Helper()
	m, _ := newTestMeter(t)
	return &recordingMeter{Meter: m}
}

func (m *recordingMeter) record(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.names = append(m.names, name)
}

func (m *recordingMeter) created() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.names...)
}

func (m *recordingMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	m.record(name)
	return m.Meter.Int64Counter(name, opts...)
}

func (m *recordingMeter) Int64Gauge(name string, opts ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	m.record(name)
	return m.Meter.Int64Gauge(name, opts...)
}

func (m *recordingMeter) Float64Gauge(name string, opts ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	m.record(name)
	return m.Meter.Float64Gauge(name, opts...)
}

func (m *recordingMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	m.record(name)
	return m.Meter.Float64Histogram(name, opts...)
}

// ---- 采集与取值（Sum 取累计和、Gauge 取最新值）----

func collectMetrics(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("采集指标: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func sumOf(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		t.Fatalf("指标 %s 未采集到", name)
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("指标 %s 不是 Int64 Sum", name)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

func gaugeOf(t *testing.T, rm metricdata.ResourceMetrics, name string) float64 {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		t.Fatalf("指标 %s 未采集到", name)
	}
	g, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("指标 %s 不是 Float64 Gauge", name)
	}
	if len(g.DataPoints) == 0 {
		t.Fatalf("指标 %s 无数据点", name)
	}
	return g.DataPoints[len(g.DataPoints)-1].Value
}

func histogramOf(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		t.Fatalf("指标 %s 未采集到", name)
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("指标 %s 不是 Float64 Histogram", name)
	}
	if len(h.DataPoints) == 0 {
		t.Fatalf("指标 %s 无数据点", name)
	}
	return h.DataPoints[0]
}

// ---- 日志测试替身：捕获全部记录的 slog.Handler（不做级别过滤）----

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

type recordHandler struct {
	mu      sync.Mutex
	entries []logEntry
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := logEntry{level: r.Level, msg: r.Message}
	r.Attrs(func(a slog.Attr) bool { e.attrs = append(e.attrs, a); return true })
	h.entries = append(h.entries, e)
	return nil
}
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordHandler) snapshot() []logEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]logEntry(nil), h.entries...)
}

// ---- 指标命名 ----

func TestMetricNames(t *testing.T) {
	m := newRecordingMeter(t)
	if _, err := New(WithMeter(m)); err != nil {
		t.Fatalf("New: %v", err)
	}
	created := map[string]bool{}
	for _, name := range m.created() {
		created[name] = true
	}
	for _, name := range []string{
		"governor_pressure", "governor_suppression",
		"governor_decisions_allow_total", "governor_decisions_drop_total",
		"governor_decisions_probe_total", "governor_batch_size",
		"governor_wait_seconds", "governor_operations_seconds",
	} {
		if !created[name] {
			t.Errorf("缺少默认前缀指标 %s", name)
		}
	}
}

func TestMetricPrefix(t *testing.T) {
	m := newRecordingMeter(t)
	if _, err := New(WithMeter(m), WithMetricPrefix("redis")); err != nil {
		t.Fatalf("New: %v", err)
	}
	created := map[string]bool{}
	for _, name := range m.created() {
		created[name] = true
	}
	if !created["redis_pressure"] {
		t.Fatal("缺少自定义前缀指标 redis_pressure")
	}
	if created["governor_pressure"] {
		t.Fatal("不应再创建默认前缀指标")
	}
}

// ---- 决策计数（枚举拆名，无 label）----

func TestDecisionCounters(t *testing.T) {
	t.Run("正常放行", func(t *testing.T) {
		m, r := newTestMeter(t)
		g, _ := New(WithMeter(m), withRandSource(float64s(0.9, 0.9)))
		g.Allow()
		if got := sumOf(t, collectMetrics(t, r), "governor_decisions_allow_total"); got != 1 {
			t.Fatalf("allow 计数 = %v, want 1", got)
		}
	})
	t.Run("丢弃", func(t *testing.T) {
		m, r := newTestMeter(t)
		clk := newFakeClock()
		sig, _ := NewLatencySignal(WithLatencyTarget(time.Millisecond))
		g, _ := New(WithMeter(m), WithClock(clk), WithSignals(sig), withRandSource(float64s(0, 0.5)))
		dec := g.Begin()
		clk.advance(time.Second)
		dec.Record(nil) // 深度过载
		g.Allow()       // r=0 < α, pd=0.5 ≥ ratio → 丢弃
		if got := sumOf(t, collectMetrics(t, r), "governor_decisions_drop_total"); got != 1 {
			t.Fatalf("drop 计数 = %v, want 1", got)
		}
	})
	t.Run("探测放行", func(t *testing.T) {
		m, r := newTestMeter(t)
		clk := newFakeClock()
		sig, _ := NewLatencySignal(WithLatencyTarget(time.Millisecond))
		g, _ := New(WithMeter(m), WithClock(clk), WithSignals(sig), withRandSource(float64s(0, 0)))
		dec := g.Begin()
		clk.advance(time.Second)
		dec.Record(nil)
		g.Allow() // r=0 < α, pd=0 < ratio → 探测
		if got := sumOf(t, collectMetrics(t, r), "governor_decisions_probe_total"); got != 1 {
			t.Fatalf("probe 计数 = %v, want 1", got)
		}
	})
}

// ---- 状态仪表与耗时直方图 ----

func TestStateGaugesAndHistogram(t *testing.T) {
	m, r := newTestMeter(t)
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(WithMeter(m), WithClock(clk), WithSignals(sig))

	dec := g.Begin()
	clk.advance(150 * time.Millisecond)
	dec.Record(nil)

	rm := collectMetrics(t, r)
	if got := gaugeOf(t, rm, "governor_pressure"); got != 0.5 {
		t.Fatalf("压力仪表 = %v, want 0.5", got)
	}
	want := SuppressionFactor(0.5, 1)
	if got := gaugeOf(t, rm, "governor_suppression"); got != want {
		t.Fatalf("抑制仪表 = %v, want %v", got, want)
	}
	dp := histogramOf(t, rm, "governor_operations_seconds")
	if dp.Count != 1 || dp.Sum != 0.15 {
		t.Fatalf("操作耗时直方图 = count %d / sum %v, want 1 / 0.15", dp.Count, dp.Sum)
	}
}

// ---- 日志跃迁（热路径不落日志，仅状态跃迁）----

func TestLoggerTransitions(t *testing.T) {
	h := &recordHandler{}
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(WithLogger(slog.New(h)), WithClock(clk), WithSignals(sig))

	// 1000ms 采样：S=9 → α≈0.9，上穿 0.5 → WARN
	dec := g.Begin()
	clk.advance(1000 * time.Millisecond)
	dec.Record(nil)

	// 连续 0 采样把 EWMA 拉回目标以下：500→250→125→62.5，α 归零 → INFO
	for i := 0; i < 4; i++ {
		d := g.Begin()
		d.Record(nil)
	}

	entries := h.snapshot()
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

// ---- 缺省观测为静默 ----

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
	h := &recordHandler{}
	clk := newFakeClock()
	sig, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	g, _ := New(WithLogger(slog.New(h)), WithClock(clk), WithSignals(sig))

	// 1000ms 采样：S=9 → α=0.9，上穿 0.5 → WARN（started）
	dec := g.Begin()
	clk.advance(1000 * time.Millisecond)
	dec.Record(nil)
	// 第二次采样耗时 1100ms：EWMA=1050 → S=9.5 → α≈0.905，上穿 0.9 → WARN（deep）
	dec = g.Begin()
	clk.advance(1100 * time.Millisecond)
	dec.Record(nil)

	entries := h.snapshot()
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
