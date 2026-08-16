# 直接依赖 observ 根模块做埋点

governor 需要暴露指标与日志。备选是自建一个零依赖的小 Observer 接口、再由适配器子包桥接 observ，保持核心纯净。我们决定核心直接 import `github.com/jninng/observ`（根模块，纯 stdlib、零第三方依赖），通过 `WithMeter` / `WithLogger` option 注入，缺省 `observ.NoopMeter` 与 `observ.DefaultLogger()` 构造期快照。

理由：observ 自身的依赖规则（docs/observability-design.md §3）钦定"业务库 → observ 根模块"为标准姿势；它零第三方依赖，接入成本等同 stdlib；自建接口会分裂生态里唯一的埋点契约。prom/zap 适配器归应用侧组合，governor 不引用。

## Consequences

- governor 的公共 option 签名携带 observ 类型，跟随 observ 接口演进。
- 指标遵守 observ 无 label 约束：枚举拆名（`_allow_total` / `_drop_total`），耗时一律 `_seconds`。
- 不引 `observ/adapters/*`；需要 prom 变长 label 时由应用侧预构建 Meter 注入。
