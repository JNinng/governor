# 策略与参数手册

逐项参考：三种控制策略与各自参数的语义、公式、约束、运行时可调性、指标与日志。
策略怎么选见 [README](../README.md) 的速查表；公式推导与特性分析见
[desc.md](desc.md) §3.3、§4。

## 公共决策链（三种策略共享）

三种策略消费同一个抑制因子 α，区别只在"α 怎么翻译成动作"：

```text
信号层    S_rej = max(0, (N_total − K·N_success) / (N_total + 1))   成功率高于 1/K 时恒 0
          S_lat = max(0, (L_ewma − L_target) / L_target)            延迟低于目标时恒 0
合成      S     = max(S_1, …, S_n)                                 最差信号优先
决策层    α     = clamp(S / (S + C), 0, 1)                          S=C 时 α=0.5，渐近 1 不达 1
```

公共参数只有一个：

| 参数 | 默认 | 约束 | 效果 |
|---|---|---|---|
| `WithSensitivity`（C） | 1.0 | > 0 | α=S/(S+C) 的双曲饱和曲线在 S=0 处斜率为 1/C。调大：抑制更保守（S=C 才丢一半）；调小：小压力即强抑制 |

跃迁日志（所有策略共用，由反馈路径 `Decision.Record` 触发）：

| 事件 | 级别 | 消息 | 归因字段 |
|---|---|---|---|
| α 上穿 0.5 | WARN | `governor: suppression started (alpha went above 0.5)` | `driver_signal` / `driver_pressure` |
| α 上穿 0.9 | WARN | `governor: deep suppression (alpha went above 0.9)` | 同上 |
| α 归 0 | INFO | `governor: suppression stopped (alpha reached 0)` | 同上 |

状态跃迁回调（`WithOnTransition`，构造期固定，`Update` 拒绝更换）：跃迁发生时
锁外同步派发 `Transition{Kind, From, To, Driver, DriverPressure}`，与上表日志
同源同频（锁内日志有序；锁外派发的回调间不保证全局到达顺序，顺序以事件的
From/To 为准）。同步调用、必须快速返回、panic 一律 recover。只推跃迁（低频），
DEBUG 级 wait/batch 逐次变化不进回调。Kind 一览：

| Kind | From / To | 触发路径 |
|---|---|---|
| `suppression_started` | 跃迁前后的 α | `Record` |
| `deep_suppression` | 同上 | `Record`；α 从 <0.5 直接越过 0.9 时跳过（只发 started） |
| `suppression_stopped` | 同上 | `Record` |
| `wait_engaged` / `wait_released` | 跃迁前后的等待秒数（wait = T_base×α） | `Wait` |

---

## 策略一：概率丢弃（`Allow` / `Do`）

接口请求类：过载时在发出之前按概率丢弃，被丢请求以 `ErrSuppressed` 快速失败，
降级响应（默认值、兜底缓存）由业务层构造（[ADR-0002](adr/0002-flow-control-only-no-breaker.md)）。

### 语义与公式

- 生成均匀随机数 r：r ≥ α 放行；r < α 丢弃，除非探测抽样 pd < probeRatio，
  此时放行为**探测请求**。
- 实际放行率 = (1−α) + α×probeRatio；探测请求是真实远端调用，结局照常计入反馈，
  保证深度抑制期间反馈不断流、远端恢复后 α 自动回落。
- 有效速率 R_effective = R_origin × (1−α)（推导见 [desc.md](desc.md) §4.1）。

### API

```go
// 一段式：决策 + 执行 + 反馈
v, err := governor.Do(ctx, g, callRemote)

// 两段式：决策与执行之间需要插入逻辑时
dec := g.Allow()
if !dec.Allowed() {
    return fallback, nil // ErrSuppressed 语义等价：本地丢弃，未发出请求
}
v, err := callRemote(ctx)
dec.Record(err)
```

### 参数

| 参数 | 默认 | 约束 | 调大 | 调小 | 运行时 `Update` |
|---|---|---|---|---|---|
| `WithSensitivity`（C） | 1.0 | > 0 | 抑制更保守 | 更激进 | ✅ |
| `WithProbeRatio` | 0.02 | [0, 1)，建议 0.01~0.05 | 反馈更足，穿透流量更多 | 反馈变稀；0=关闭探测（可逼近全丢，慎用） | ✅ |

### 指标与日志

| 名称 | 类型 | 含义 |
|---|---|---|
| `_decisions_allow_total` | counter | 正常放行（与 probe 互斥不叠加） |
| `_decisions_drop_total` | counter | 本地丢弃 |
| `_decisions_probe_total` | counter | 探测放行 |
| `_operations_seconds` | histogram | 实际发往远端的操作耗时（`OutcomeIgnore` 不计入） |

热路径只打指标，不落日志；日志仅上表所列公共跃迁。

### 适用与示例

可容忍部分丢失的在线请求。示例：
[example/http](../example/http/main.go)、[example/minimal](../example/minimal/main.go)、
[example/bff](../example/bff/main.go)（晚高峰降级）。

---

## 策略二：批次调整（`BatchSize` + `Begin`）

批量写入类：不能丢、但可减量。过载时自动缩小单批数据量，降低对下游的单次冲击。

### 语义与公式

- B_current = max(B_min, floor(B_max × (1−α)))。
- α 上升 → 批次线性收缩；下限保证深度抑制时仍持续产生小批量反馈，
  控制回路不因批次为 0 而中断（[desc.md](desc.md) §4.2）。
- `Begin` 只起表不判丢弃：批量场景不做概率丢弃，节奏由批次大小自带的减量承担。

### API

```go
n := g.BatchSize()
dec := g.Begin()                                    // 起表；不做丢弃判定
err := myPipeline(ctx, items[:min(n, len(items))])
dec.Record(err)                                     // 整批耗时与成败自动计入
```

### 参数

| 参数 | 默认 | 约束 | 调大 | 调小 | 运行时 `Update` |
|---|---|---|---|---|---|
| `WithBatch`（max） | 100 | ≥ min | 无压时单批更大、吞吐更高 | 过载前批次更小 |
| `WithBatch`（min） | 1 | ≥ 1 | 深度抑制时反馈样本更足 | 反馈更细；为 1 时仅最小反馈不断流 |

### 指标与日志

| 名称 | 类型 | 含义 |
|---|---|---|
| `_batch_size` | gauge | 当前批次大小 |
| `governor: batch size changed` | DEBUG | 批次变化时记录（from→to） |

### 适用与示例

批量写库、Redis Pipeline 等。完整数值算例见 [desc.md](desc.md) §5.1。

---

## 策略三：延迟执行（`Wait`）

后台任务类：不能丢、可延迟。过载时在每次执行前插入等待，压低单位时间的请求量。
延迟执行不丢弃请求，只延后发出；不是失败后的退避重试
（[ADR-0002](adr/0002-flow-control-only-no-breaker.md)）。

### 语义与公式

- T_wait = T_base × α，线性映射，上限即 T_base；α=0 时不等待。
- ctx 在调用时已取消/超时的，跳过睡眠直接返回令牌。

### API

```go
for {
    dec := g.Wait(ctx) // 睡 T_base×α 后返回令牌
    err := mySync(ctx)
    dec.Record(err)
}
```

### 参数

| 参数 | 默认 | 约束 | 调大 | 调小 | 运行时 `Update` |
|---|---|---|---|---|---|
| `WithBaseWait`（T_base） | 1s | ≥ 0 | 深度抑制时等待上限更高、削峰更狠 | 延迟节奏更快 |

### 指标与日志

| 名称 | 类型 | 含义 |
|---|---|---|
| `_wait_seconds` | gauge | 当前等待时长（秒） |
| `governor: wait engaged (wait went above 0)` | INFO | wait 首次非零（`wait_seconds`、`driver_signal` 归因） |
| `governor: wait released (wait reached 0)` | INFO | wait 归零 |
| `governor: wait changed` | DEBUG | 等待变化时记录（from_seconds→to_seconds） |

### 适用与示例

后台异步任务、非实时同步。示例：[example/wait](../example/wait/main.go)
（下游三档梯度变化，等待阶梯式上升、恢复即归零，全程无丢弃）。

---

## 感知信号参数（策略的输入侧）

信号决定 S 怎么来，是三种策略共享的输入。**信号参数均为构造期固定，
运行时不可调整**；运行中的等价调节请用 `Update` 支持的四个参数
（C / probeRatio / batch / baseWait，见 [README](../README.md) 运行时更新参数一节）。

| 信号 | 参数 | 默认 | 约束 | 效果 |
|---|---|---|---|---|
| `RejectionSignal` | `WithRejectionK`（K） | 2.0 | > 0 | 容忍失败率 ≈ 1/K（K=2 即容忍 50%）；调大更宽容，调小更早感知失败 |
| `RejectionSignal` | `WithRejectionWindow` | 90s | > 0 | 聚合窗口；调大更平滑、恢复更慢，调小更灵敏 |
| `LatencySignal` | `WithLatencyTarget`（L_target） | 200ms | > 0 | 压力=0的分界线；调大对变慢更宽容，调小更早感知劣化 |
| `LatencySignal` | `WithLatencyBeta`（β） | 0.4 | (0, 1)，建议 0.3~0.5 | EWMA 平滑系数；调大更平滑，调小跟手但易抖动 |
| `LatencySignal` | `WithLatencyWindow` | 90s | > 0 | 采样饥饿阈值；窗口内无新采样时沿用上一次计算出的压力 |
| `ConcurrencySignal` | `WithConcurrencyTarget`（N_target） | 100 | > 0 | 压力=0的分界线：n ≤ N_target 恒不抑制，超出线性升压（n = 2×N_target 时 S=1）；按下游并发容量设定 |
| `DirectiveSignal` | `WithDirectiveScale` | 1s | > 0 | 换算基准：服务端要求等待该时长时 S=1（5s 指令 → S=5）；指令压力随自身时长线性衰减，到期自行解除 |

`ConcurrencySignal` 以 `Enter`/`Leave` 显式配对计数（在途 / 队列深度），不参与
上述窗口与饥饿语义：计数恒反映当前状态，不随时间衰减；经传输层适配器使用时
由适配器自动配对（放行后起算，本地丢弃不计入）。

`DirectiveSignal` 以 `Feed`/`FeedAt` 喂入服务端等待指令（HTTP `Retry-After`、
gRPC `RetryInfo`），同样不参与窗口与饥饿语义：喂入时 S = 指令时长 / 换算基准，
随指令自身时长线性衰减至 0——到期自行解除，不产生陈旧压力；喂入 0 等于清除。
经传输层适配器使用时由适配器自动解析；`X-RateLimit` 等无统一格式的头由业务
自行换算后调用同一方法。

## 参数总表

| 参数 | 归属 | 默认 | 约束 | 运行时 `Update` |
|---|---|---|---|---|
| `WithSensitivity`（C） | 决策层（三策略共享） | 1.0 | > 0 | ✅ |
| `WithProbeRatio` | 策略一 | 0.02 | [0, 1) | ✅ |
| `WithBatch`（max/min） | 策略二 | 100 / 1 | min ≥ 1，max ≥ min | ✅ |
| `WithBaseWait` | 策略三 | 1s | ≥ 0 | ✅ |
| `WithRejectionK` | RejectionSignal | 2.0 | > 0 | ❌（构造期） |
| `WithRejectionWindow` | RejectionSignal | 90s | > 0 | ❌（构造期） |
| `WithLatencyTarget` | LatencySignal | 200ms | > 0 | ❌（构造期） |
| `WithLatencyBeta` | LatencySignal | 0.4 | (0, 1) | ❌（构造期） |
| `WithLatencyWindow` | LatencySignal | 90s | > 0 | ❌（构造期） |
| `WithConcurrencyTarget` | ConcurrencySignal | 100 | > 0 | ❌（构造期） |
| `WithDirectiveScale` | DirectiveSignal | 1s | > 0 | ❌（构造期） |
