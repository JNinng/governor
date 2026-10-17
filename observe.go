package governor

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// 埋点（ADR-0001）：指标依赖 otel/metric，option 注入，缺省取全局
// MeterProvider（未配置即 noop 静默）；日志用 log/slog，缺省 discard 静默。
// 热路径只打指标，日志仅低频状态跃迁；指标无属性 label，枚举拆名、耗时一律 _seconds。

// metrics 持有构造期创建的全部指标（otel 规范：仪表仅构造期创建、记录时复用）。
type metrics struct {
	pressure    metric.Float64Gauge
	suppression metric.Float64Gauge
	allow       metric.Int64Counter
	drop        metric.Int64Counter
	probe       metric.Int64Counter
	batchSize   metric.Int64Gauge
	waitSeconds metric.Float64Gauge
	opSeconds   metric.Float64Histogram
}

// defaultBuckets 覆盖毫秒级接口到十秒级批处理的耗时分布。
var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

func newMetrics(m metric.Meter, prefix string) *metrics {
	// otel SDK 与全局默认 provider 在创建报错时仍返回可用仪表（典型原因是
	// 名称不合规，如前缀不以字母开头），故忽略错误、不打断构造。
	pressure, _ := m.Float64Gauge(prefix+"_pressure",
		metric.WithDescription("Current pressure index S (worst signal wins)"), metric.WithUnit("1"))
	suppression, _ := m.Float64Gauge(prefix+"_suppression",
		metric.WithDescription("Current suppression factor alpha"), metric.WithUnit("1"))
	allow, _ := m.Int64Counter(prefix+"_decisions_allow_total",
		metric.WithDescription("Decisions allowed"), metric.WithUnit("1"))
	drop, _ := m.Int64Counter(prefix+"_decisions_drop_total",
		metric.WithDescription("Decisions dropped on the client"), metric.WithUnit("1"))
	probe, _ := m.Int64Counter(prefix+"_decisions_probe_total",
		metric.WithDescription("Decisions let through as probes"), metric.WithUnit("1"))
	batchSize, _ := m.Int64Gauge(prefix+"_batch_size",
		metric.WithDescription("Current batch size"), metric.WithUnit("1"))
	waitSeconds, _ := m.Float64Gauge(prefix+"_wait_seconds",
		metric.WithDescription("Current wait before the next call, in seconds"), metric.WithUnit("s"))
	opSeconds, _ := m.Float64Histogram(prefix+"_operations_seconds",
		metric.WithDescription("Time taken by calls sent to the remote, in seconds"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(defaultBuckets...))
	return &metrics{
		pressure:    pressure,
		suppression: suppression,
		allow:       allow,
		drop:        drop,
		probe:       probe,
		batchSize:   batchSize,
		waitSeconds: waitSeconds,
		opSeconds:   opSeconds,
	}
}

// discardLogger 是缺省日志：默认静默（零输出），与指标缺省 noop 对齐。
var discardLogger = slog.New(slog.DiscardHandler)

// defaultMeter 取全局 MeterProvider：未配置时为全局默认（noop）静默；
// 首次 otel.SetMeterProvider 后已建实例自动接入（仅首次生效，再次 Set 不追溯）。
func defaultMeter() metric.Meter {
	return otel.GetMeterProvider().Meter("github.com/jninng/governor")
}

// seconds 把时长换算为秒（耗时指标一律以秒计，命名口径 *_seconds）。
func seconds(d time.Duration) float64 { return d.Seconds() }
