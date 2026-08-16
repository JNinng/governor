// Package governor 实现通用客户端自适应流控。
//
// 三层结构（理论设计见 docs/desc.md）：
//
//   - 感知层（Signal）：把异构反馈归一化为压力指数 S >= 0。
//     内置 RejectionSignal（成败计数滑窗）与 LatencySignal（延迟 EWMA），
//     均可脱离 Governor 单独复用，也可实现 Signal 接入自定义信号。
//   - 决策层（纯函数）：SuppressionFactor 把 S 映射为抑制因子
//     alpha = clamp(S/(S+C), 0, 1)；多信号取最差值合成（MaxPressure）。
//   - 执行层（Governor）：把 alpha 翻译为三种控制动作——
//     Allow 概率丢弃（含探测放行）、BatchSize 批次调整、Wait 延迟执行。
//
// 反馈通过 Decision 令牌闭环：Begin/Allow/Wait 返回令牌，
// 操作完成后 Record(err) 自动计时、分类并喂入信号；Record 幂等，
// 被本地丢弃的令牌其 Record 为安全 no-op（本地丢弃不污染统计）。
//
// 调节参数（敏感度 C、探测比例、批次上下限、基准等待）可通过
// Governor.Update 在运行时更新：原子生效，任一 option 报错、校验
// 失败或试图变更结构性字段（信号集合、分类器、时钟、Meter、
// Logger、指标前缀、随机源）时当前配置保持不变并返回错误。
// 内置信号自身参数（K、窗口、目标延迟、β）为构造期固定，
// 运行时不可更新；运行中的调节请在 Update 支持的参数内进行。
//
// # 能力边界（ADR-0002）
//
// 本库的全部控制输出即上述三种动作，另有只读观测输出
// （Pressure/Suppression/Signals，观测不是控制）。它是连续、概率式的
// 流量调节器：alpha 严格小于 1；探测开启时（默认 2%，可用
// WithProbeRatio(0) 关闭，关闭后深度抑制可逼近全丢）反馈路径永不归零。
// 它不是熔断器（无全开/全关状态机），不内置降级（被抑制的操作以
// ErrSuppressed 快速失败，降级响应由业务层构造），不是并发信号量，
// 也不是重试引擎（Wait 是延迟执行——延迟而不丢弃——不是失败后的退避重试）。
//
// 一个 Governor 实例对应一个远端目标，无全局单例。
//
// # 埋点（ADR-0001）
//
// 直接依赖 observ 根模块：WithMeter 注入（默认 NoopMeter），
// WithLogger 注入（缺省构造期快照 observ.DefaultLogger()）。
// 指标无 label、枚举拆名，多实例用 WithMetricPrefix 区分。
package governor
