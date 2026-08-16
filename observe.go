package governor

import (
	"log/slog"
	"time"

	"github.com/jninng/observ"
)

// observ 埋点（ADR-0001）：直接依赖 observ 根模块，option 注入；
// 热路径只打指标，日志仅低频状态跃迁；枚举拆名、无 label。

// metrics 持有构造期创建的全部指标（observ 规范：New* 仅构造期调用）。
type metrics struct {
	pressure    observ.Gauge
	suppression observ.Gauge
	allow       observ.Counter
	drop        observ.Counter
	probe       observ.Counter
	batchSize   observ.Gauge
	waitSeconds observ.Gauge
	opSeconds   observ.Histogram
}

// defaultBuckets 覆盖毫秒级接口到十秒级批处理的耗时分布。
var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

func newMetrics(m observ.Meter, prefix string) *metrics {
	return &metrics{
		pressure:    m.NewGauge(prefix+"_pressure", "Current pressure index S (worst signal wins)"),
		suppression: m.NewGauge(prefix+"_suppression", "Current suppression factor alpha"),
		allow:       m.NewCounter(prefix+"_decisions_allow_total", "Decisions allowed"),
		drop:        m.NewCounter(prefix+"_decisions_drop_total", "Decisions dropped on the client"),
		probe:       m.NewCounter(prefix+"_decisions_probe_total", "Decisions let through as probes"),
		batchSize:   m.NewGauge(prefix+"_batch_size", "Current batch size"),
		waitSeconds: m.NewGauge(prefix+"_wait_seconds", "Current wait before the next call, in seconds"),
		opSeconds:   m.NewHistogram(prefix+"_operations_seconds", "Time taken by calls sent to the remote, in seconds", defaultBuckets),
	}
}

// logger 以 observ 规范包装：先 Enabled 门控再构造 attrs（零构造成本）。
type logger struct {
	l observ.Logger
}

func (lg logger) enabled(level slog.Level) bool { return lg.l.Enabled(level) }

func (lg logger) debug(msg string, attrs ...slog.Attr) {
	if lg.enabled(slog.LevelDebug) {
		lg.l.Log(slog.LevelDebug, msg, attrs...)
	}
}

func (lg logger) info(msg string, attrs ...slog.Attr) {
	if lg.enabled(slog.LevelInfo) {
		lg.l.Log(slog.LevelInfo, msg, attrs...)
	}
}

func (lg logger) warn(msg string, attrs ...slog.Attr) {
	if lg.enabled(slog.LevelWarn) {
		lg.l.Log(slog.LevelWarn, msg, attrs...)
	}
}

func slogAttrFloat(k string, v float64) slog.Attr { return slog.Float64(k, v) }
func slogAttrInt(k string, v int) slog.Attr       { return slog.Int(k, v) }

// seconds 把时长换算为秒（observ 命名规范：耗时一律 _seconds）。
func seconds(d time.Duration) float64 { return d.Seconds() }
