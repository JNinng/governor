# 埋点直接依赖 otel 指标 API 与 slog

governor 需要暴露指标与日志。备选是自建一套零依赖的小接口（Meter/Logger 抽象）、再由应用侧桥接到具体实现，保持核心零第三方依赖。我们决定不自建抽象：指标直接依赖 `go.opentelemetry.io/otel/metric`（`WithMeter` 注入 `metric.Meter`，缺省取全局 MeterProvider），日志直接用标准库 `log/slog`（`WithLogger` 注入 `*slog.Logger`，缺省 discard 静默）。

理由：otel 指标 API 是埋点的事实标准，库侧只依赖 API、由应用装配 provider（未配置即全局默认 noop，零打点）；slog 是标准库日志汇流点，`*slog.Logger` 自带级别门控。两者都让 governor 的观测面跟随应用的全局装配自动生效，无须自有抽象层与适配器桥接。

## Consequences

- 公共 option 签名携带 `metric.Meter` / `*slog.Logger`，跟随 otel 与标准库演进。
- 核心运行时依赖限于 `go.opentelemetry.io/otel`（`GetMeterProvider` 缺省联动）与 `go.opentelemetry.io/otel/metric`；sdk（`go.opentelemetry.io/otel/sdk/metric`）仅测试断言引入（ManualReader 收集 metricdata），不出现在库代码 import 中。
- otel 同步仪表接口密封（外部包无法自造假仪表，测试替身须包装真实 provider）；otel 全局 provider 是委托式的：未配置时为全局默认（noop），首次 `SetMeterProvider` 后此前创建的实例（含 Group / 适配器惰性实例）自动接入，之后再次 Set 被忽略。
- 指标无属性 label：枚举拆名（`_allow_total` / `_drop_total`），耗时一律 `_seconds`；多实例以前缀隔离（`WithMetricPrefix`）。
- 锁定的 otel v1.46.0 自身要求 Go 1.25（其 go.mod 声明 `go 1.25.0`，高于 otel API 的 1.23 与 `slog.DiscardHandler` 的 1.24 下限），go directive 随之取 1.25 并在 README 声明。
