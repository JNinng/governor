# governor

通用客户端自适应流控库（Go）：感知远端反馈（拒绝计数、响应时长）计算压力指数 S，
输出连续、概率式的抑制因子 α，再翻译为**丢弃 / 缩批 / 延迟**三种控制动作。

**它是流量调节器，不是熔断器**：无全开/全关状态机，抑制连续可调；探测开启
（默认 2%，`WithProbeRatio(0)` 可关闭，关闭后深度抑制可逼近全丢）时放行率恒大于零；
被抑制的请求以 `ErrSuppressed` 快速失败，怎么降级由你的业务决定（[ADR-0002](docs/adr/0002-flow-control-only-no-breaker.md)）。
理论设计见 [docs/desc.md](docs/desc.md)。

## 安装

```bash
go get github.com/jninng/governor
```

## 快速上手

整个库的模型只有三句话：

1. **一个下游目标建一个 Governor**（无全局单例）；
2. **用 `governor.Do` 包住远端调用**——耗时与成败自动反馈，闭环自动形成；
3. **过载时 `Do` 直接返回 `ErrSuppressed`**——降级（兜底值 / 缓存 / 报错）是业务的事。

```text
governor.Do(ctx, g, op)
 ├── 放行 → 执行 op → 自动 Record(成败+耗时) → 更新 S 与 α   ← 反馈闭环
 └── 丢弃 → 返回 ErrSuppressed → 业务自行降级
     α = clamp(S/(S+C), 0, 1)：S 越大丢得越狠，探测放行保证反馈永不归零
```

### 示例一览

四个可运行示例，从最小闭环到真实业务形态：

| 示例 | 演示重点 | 运行 |
|---|---|---|
| [minimal](example/minimal/main.go) | 最小闭环：一个延迟信号 + `Do` + 降级分支 | `go run ./example/minimal` |
| [bff](example/bff/main.go) | 业务场景：晚高峰劣化 → 本地缓存兜底 | `go run ./example/bff` |
| [http](example/http/main.go) | 真实 `http.Client` 调用 + observ 日志与降级统计 | `go run ./example/http` |
| [wait](example/wait/main.go) | 策略三延迟执行：等待随压力阶梯升降，零丢弃 | `go run ./example/wait` |

### 复制即跑的最小示例

上面三句话模型的最短实现——感知、流控器、降级分支各占几行。
模拟下游第 6 次请求起过载（响应 300ms，目标 100ms），完整程序见
[example/minimal/main.go](example/minimal/main.go)，`go run ./example/minimal` 直接运行：

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jninng/governor"
)

func main() {
	// 1) 感知什么：响应时长超过 100ms 视为压力（EWMA 平滑，β 默认 0.4）
	sig, err := governor.NewLatencySignal(governor.WithLatencyTarget(100 * time.Millisecond))
	if err != nil {
		panic(err)
	}

	// 2) 建流控器：一个下游目标一个实例；C=1 表示 S=C 时抑制一半流量
	g, err := governor.New(governor.WithSignals(sig))
	if err != nil {
		panic(err)
	}

	// 模拟一个会过载的下游：第 6 次请求起响应 300ms
	slow := false
	remote := func(ctx context.Context) (string, error) {
		if slow {
			time.Sleep(300 * time.Millisecond)
		}
		return "远端数据", nil
	}

	for i := 1; i <= 12; i++ {
		if i == 6 {
			slow = true // 下游开始过载
		}
		v, err := governor.Do(context.Background(), g, remote)
		switch {
		case errors.Is(err, governor.ErrSuppressed):
			fmt.Printf("请求 %2d  被抑制 → 兜底值   α=%.2f\n", i, g.Suppression())
		case err != nil:
			fmt.Printf("请求 %2d  远端错误: %v\n", i, err)
		default:
			fmt.Printf("请求 %2d  OK  %s   α=%.2f\n", i, v, g.Suppression())
		}
	}
}
```

真实运行输出（概率丢弃，每次略有差异）：

```text
请求  1  OK  远端数据   α=0.00
请求  5  OK  远端数据   α=0.00
请求  6  OK  远端数据   α=0.44      ← 下游开始变慢，首个 300ms 采样抬升压力
请求  7  被抑制 → 兜底值   α=0.44
请求  8  OK  远端数据   α=0.60      ← α 随 EWMA 爬升
请求 10  被抑制 → 兜底值   α=0.60
请求 11  OK  远端数据   α=0.64
请求 12  被抑制 → 兜底值   α=0.64   ← 稳定趋向 2/3（300ms 过载 → S=2 → α≈0.67）
```

*（中间行省略；完整 12 行见实际运行。）*

三个看点：

- **健康期零干扰**：α=0，全部放行，库接近透传；
- **过载即收敛**：无需配置任何阈值，α 跟随反馈爬升到与过载程度匹配的位置；
- **降级在你的代码里**：`ErrSuppressed` 分支返回兜底值——库只管快速失败。

### 业务场景示例：晚高峰的用户服务调用

`go run ./example/bff`（[example/bff/main.go](example/bff/main.go)）——
商品页 BFF 调用户服务取昵称：平时响应 80ms，晚高峰劣化到 500ms（目标 100ms）；
被抑制的请求**不发出**，回本地缓存兜底（概率丢弃，每次运行略有差异）：

```text
请求  1  远端OK  用户服务实时昵称   α=0.00   ← 平峰：α=0，接近透传
请求  6  远端OK  用户服务实时昵称   α=0.70   ← 晚高峰首个 500ms 采样，压力抬升
请求  7  远端OK  用户服务实时昵称   α=0.77
请求  8  降级→本地缓存昵称   α=0.77   ← 概率丢弃开始命中：请求没发出去
请求  9  远端OK  用户服务实时昵称   α=0.79
请求 12  降级→本地缓存昵称   α=0.79   ← 稳定趋向 0.8（500ms = 5×目标 → S=4 → α=4/5）
```

*（中间行省略；完整 12 行见实际运行。）*

对照真实业务：**远端接口变慢而非报错**时（P99 劣化、队列堆积），延迟信号先于
错误发生作用；α 收敛到的位置由过载程度（几倍于目标）决定，无需人工设阈值。

### 真实 HTTP 一键演示

换成真实网络栈跑同一套闭环：内置一个响应 250ms 的本地"过载服务"（目标 100ms），
`governor.Do` 包住 `http.Client` 调用。示例同时演示 observ 接入姿势——
slog 以 Info 阈值接入（只出状态跃迁日志），结束时汇总放行/降级统计
（[example/http/main.go](example/http/main.go)）：

```bash
go run ./example/http
```

```text
time=2026-08-15T23:48:52.437+08:00 level=WARN msg="governor: suppression started (alpha went above 0.5)" pressure=1.520119 suppression=0.6031933412668211
  1  OK  remote-payload   α=0.60
  2  OK  remote-payload   α=0.60
  3  OK  remote-payload   α=0.60
  ...

合计: 放行 22，被抑制（走降级）38，最终 α=0.60
```

## 三种控制动作速查

| 场景 | 动作 | 关键调用 |
|---|---|---|
| 接口请求（可容忍部分丢失） | 概率丢弃 | `Do`（或 `Allow` 两段式） |
| 批量写入（不能丢，可减量） | 缩小批次 | `BatchSize` + `Begin` + `Record` |
| 后台任务（可延迟） | 延迟执行（不丢弃） | `Wait` + `Record` |

各策略的语义、公式、参数约束、指标与日志逐项参考见
[docs/strategies.md](docs/strategies.md)（策略与参数手册）。

#### ① 概率丢弃 — 接口请求

```go
v, err := governor.Do(ctx, g, func(ctx context.Context) (string, error) {
	return callRemote(ctx) // ← 替换为你的远端调用
})
if errors.Is(err, governor.ErrSuppressed) {
	return fallbackValue, nil // 过载被本地丢弃：在此降级
}
```

需要在决策与执行之间插入逻辑时，用两段式（`Do` 内部即此组合）：

```go
dec := g.Allow()
if !dec.Allowed() {
	return fallbackValue, nil
}
v, err := callRemote(ctx)
dec.Record(err) // 耗时自 Allow 起自动计算，幂等
```

#### ② 缩小批次 — 批量写入

```go
n := g.BatchSize()                                 // 深度抑制也不低于 B_min，反馈不断流
dec := g.Begin()                                   // 起表；批量场景不做丢弃判定
err := myPipeline(ctx, items[:min(n, len(items))]) // ← 替换为你的批量写入
dec.Record(err)                                    // 整批耗时与成败自动计入反馈
```

#### ③ 延迟执行 — 后台任务（延时不丢弃）

```go
for {
	dec := g.Wait(ctx) // 睡 T_base×α 后继续；延迟而不丢弃。α=0 时不等待
	err := mySync(ctx) // ← 替换为你的同步操作
	dec.Record(err)
}
```

完整可运行示例：`go run ./example/wait`（[example/wait/main.go](example/wait/main.go)）。
场景是后台同步任务：下游三档梯度变化（600ms 轻度过载 → 1100ms 加深 → 200ms 恢复，
目标 500ms）。两个看点：**等待随压力阶梯式上升，恢复后一两次采样即归零；
全程 12 次执行无一被丢弃**——延迟执行只延后、不丢请求。日志同 http 示例，
以 Info 阈值接入只记状态跃迁（`WARN suppression started` / `INFO suppression
stopped`），DEBUG 级 `wait changed`（带 from→to 明细）静默，排查时调到 Debug 即见：

```text
第  4 次  等待  334ms │ 执行 1100ms │ α=0.44
time=...level=WARN  msg="governor: suppression started (alpha went above 0.5)" pressure=1.04 suppression=0.51
第  5 次  等待  889ms │ 执行 1100ms │ α=0.51   ← α 上穿 0.5，WARN 跃迁
第  6 次  等待 1020ms │ 执行 1100ms │ α=0.53   ← 深度过载：阶梯式收敛
（……中略……）
第  9 次  等待 1087ms │ 执行  200ms │ α=0.10   ← 下游已恢复，EWMA 滞后一次
time=...level=INFO  msg="governor: suppression stopped (alpha reached 0)" pressure=0 suppression=0
第 10 次  等待  209ms │ 执行  200ms │ α=0.00   ← α 归零，INFO 跃迁
第 11 次  等待    0ms │ 执行  200ms │ α=0.00   ← 归零，全速运行
```

## 信号与参数选型速查

动作怎么选看上表（按业务能否容忍丢失 / 减量 / 延迟）；感知什么、参数怎么调看这里。

**选哪个信号（感知什么反馈）：**

| 信号 | 过载怎么看出来 | 适用场景 | 关键参数（默认） |
|---|---|---|---|
| `RejectionSignal`（场景 A） | 滑窗失败率超 1/K | 下游**显式拒绝**：HTTP 429/503、RPC 错误 | `WithRejectionK` 2.0（容忍 50% 失败）、`WithRejectionWindow` 90s |
| `LatencySignal`（场景 B） | EWMA 耗时超目标值 | 下游**变慢但不报错**：P99 劣化、写入堆积 | `WithLatencyTarget` 200ms、`WithLatencyBeta` 0.4、`WithLatencyWindow` 90s |
| 两者组合 | 取最差值，任一维度过载即抑制 | 错误与变慢互为前兆（推荐默认） | — |
| 自定义 `Signal` | 任意口径（队列深度、在途请求数…） | 内置两类的口径都不合适 | 实现 `Name/Observe/Pressure` 三方法 |

**参数往哪调：**

| 参数 | 默认 | 调大 | 调小 |
|---|---|---|---|
| `WithSensitivity`（C） | 1.0 | 抑制更保守（S=C 才丢一半） | 更激进（小压力即强抑制） |
| `WithProbeRatio` | 0.02 | 深度抑制期反馈更足，穿透流量更多 | 反馈变稀；0=关闭探测（可逼近全丢） |
| `WithLatencyTarget` | 200ms | 对变慢更宽容 | 更早感知劣化 |
| `WithRejectionK` | 2.0 | 容忍更高失败率 | 更早感知失败 |
| `WithBatch`（max/min） | 100/1 | 过载时单批冲击更大、吞吐更高 | 反馈更细、对下游更温和 |
| `WithBaseWait` | 1s | 深度抑制时等待上限更高 | 延迟执行节奏更快 |

调参次序：**默认起步 → 观测 `_pressure` / `_suppression` / `_decisions_drop_total` →
只动与现象对应的那一个参数**（如“抑制太晚”先降 C 或降 target，而非同时调多个）。
运行中调参不必重启：C、探测比例、批次、基准等待可用 `Update` 原子生效，见
[运行时更新参数](#运行时更新参数)；表中信号参数（K / target / β / 窗口）
仅构造期可设，运行中的等价调节请用 `Update` 支持的四个参数。
各 option 完整签名与约束见下文[进阶配置](#信号与决策的进阶配置)。

### 为什么我的请求被丢了？

`ErrSuppressed` 意味着 governor 根据反馈判断下游已过载，在**发出之前**就丢弃了请求。
业务应在此返回降级响应；即使深度抑制，探测放行（默认 2%）仍持续发出真实请求，
远端恢复后 α 自动回落、放行率随之恢复，无需人工干预。

### 两种信号与多信号合成

从反馈到抑制的完整公式链（推导与特性分析见 [desc.md](docs/desc.md) §3）：

```text
拒绝信号  S_rej = max(0, (N_total − K·N_success) / (N_total + 1))   成功率高于 1/K 时恒 0
延迟信号  S_lat = max(0, (L_ewma − L_target) / L_target)            延迟低于目标时恒 0
多信号    S     = max(S_1, …, S_n)                                 最差信号优先
抑制因子  α     = clamp(S / (S + C), 0, 1)                          S=C 时 α=0.5，渐近 1 不达 1
```

```go
rej, _ := governor.NewRejectionSignal() // 场景 A：失败率超 1/K（默认容忍 50%）产生压力
g, _ := governor.New(governor.WithSignals(lat, rej)) // 多信号取最差值（最差信号优先）
```

成败语义可自定义（如把业务中性错误排除在统计外）：

```go
g, _ := governor.New(
	governor.WithSignals(rej),
	governor.WithClassifier(func(err error) governor.Outcome {
		if errors.Is(err, context.Canceled) {
			return governor.OutcomeIgnore // 调用方取消：不计入成败统计
		}
		if err == nil {
			return governor.OutcomeSuccess
		}
		return governor.OutcomeFailure
	}),
)
```

## 业务自定义决策（只读暴露）

```go
s := g.Pressure()    // 合成压力指数 S（最差信号优先）
a := g.Suppression() // 抑制因子 α
for _, snap := range g.Signals() {
	log.Printf("signal=%s pressure=%v", snap.Name, snap.Pressure)
}
if s > 2 { /* 业务侧切只读模式等，观测不是控制 */ }
```

## 感知/决策组件单独复用

```go
sig, _ := governor.NewLatencySignal(governor.WithLatencyTarget(100 * time.Millisecond))
sig.Observe(time.Now(), governor.OutcomeSuccess, took)
s := sig.Pressure(time.Now())

alpha := governor.SuppressionFactor(s, 1.0) // 纯函数：clamp(s/(s+c), 0, 1)
```

实现 `Signal` 接口（`Name/Observe/Pressure`）即可接入自定义信号（如基于队列深度）。
`Pressure` 为只读观测；两类内置信号在统计饥饿（窗口/阈值内无新样本）时沿用
上一次非空压力，从未有样本时返回 0。

### 信号与决策的进阶配置

```go
rej, _ := governor.NewRejectionSignal(
	governor.WithRejectionK(3),              // 调节系数 K（默认 2，即容忍约 50% 失败率）
	governor.WithRejectionWindow(time.Minute), // 聚合窗口（默认 90s）
)
lat, _ := governor.NewLatencySignal(
	governor.WithLatencyTarget(100*time.Millisecond),
	governor.WithLatencyBeta(0.3),             // EWMA 平滑系数 β（默认 0.4，建议 0.3~0.5）
	governor.WithLatencyWindow(2*time.Minute), // 采样饥饿阈值（默认 90s）
)
```

其他导出项：

- `Clock` / `WithClock`：可注入时间源（`Now` + 可被 ctx 取消的 `Sleep`），
  供测试模拟收敛过程；默认系统时钟。
- `Decision.Probe()`：报告本次放行是否为探测请求（探测放行仍是真实远端调用，
  其结局照常计入反馈）。
- `Wait` 在 ctx 已取消/超时时不睡眠，直接返回令牌。
- 各 `With*` option 传 `nil`（如 `WithClassifier(nil)`、`WithMeter(nil)`、
  `WithLogger(nil)`）一律构造失败；恢复默认只需不传该 option。

### 运行时更新参数

调节参数（`WithSensitivity` / `WithProbeRatio` / `WithBatch` / `WithBaseWait`）
可在不停机的情况下通过 `Update` 生效——配合配置中心、管理端口或 SRE 预案动态调参，
无需重建 Governor、丢失滑动窗口统计：

```go
// 观测到 "_suppression" 长期高位，把敏感度调保守、批次调小
if err := g.Update(
    governor.WithSensitivity(2.0),
    governor.WithBatch(50, 5),
); err != nil {
    // 任一 option 报错、校验失败或试图变更结构性字段时，
    // 当前配置原样保留，可安全重试
}
```

- **原子生效**：全部 option 应用并校验通过后才整体切换；失败时旧配置仍在用。
- **结构性字段不可变更**：信号集合、分类器、时钟、Meter、Logger、指标前缀、
  随机源传入 `Update` 会被拒绝（信号类型与口径保持稳定，统计连续可比）。
  **信号自身参数**（`WithRejectionK` / `WithRejectionWindow` / `WithLatencyTarget` /
  `WithLatencyBeta` / `WithLatencyWindow`）同为构造期固定，运行时不可调整；
  口径级变更（换 K / target / 窗口 / β）须按新口径重建信号与 Governor（统计清零）。
- 更新成功记一条 INFO 日志（含各参数 from→to），便于审计谁在何时调了什么。

全部参数的归属、默认值、约束与运行时可调性汇总见
[策略与参数手册 · 参数总表](docs/strategies.md#参数总表)。

#### 信号参数类诉求 → 在已支持动态调整的参数中处理

运行中想改信号容忍度时，不必等重建——`Update` 支持的四个参数通常足以达到同等目的：

| 想达到的效果 | 运行时的处理方式 |
|---|---|
| 改容忍度/灵敏度（K、target 的效果） | `Update(WithSensitivity(...))`：α = S/(S+C)，C 是全部信号共享的灵敏度旋钮，即时增减抑制强度（S=0 时调 C 无效果，属正常——无压力即无抑制） |
| 深度抑制期反馈量 | `Update(WithProbeRatio(...))` |
| 过载时单批冲击 | `Update(WithBatch(...))` |
| 延迟执行节奏 | `Update(WithBaseWait(...))` |
| 更换口径本身（K/窗口/target/β） | 不支持运行时调整，需重建信号与 Governor |

## 埋点（observ）

```go
import (
	"log/slog"

	"github.com/jninng/observ"
)

g, _ := governor.New(
	governor.WithSignals(lat),
	governor.WithLogger(observ.NewSlogLogger(slog.Default())), // 缺省快照 observ.DefaultLogger()
	governor.WithMetricPrefix("redis"), // 多实例区分（无 label 约束）；默认 "governor"
)
```

`WithMeter` 接受任意 `observ.Meter` 实现（默认 `observ.NoopMeter`）；
Prometheus / OTel 等适配器以 [observ](https://github.com/jninng/observ)
仓库实际发布的模块为准。

指标一览（前缀默认 `governor`）：`_pressure`、`_suppression`、`_batch_size`、
`_wait_seconds`（gauge），`_decisions_allow_total` / `_decisions_drop_total` /
`_decisions_probe_total`（counter），`_operations_seconds`（histogram）。
放行决策只计 allow、探测放行只计 probe（互斥不叠加）；`_operations_seconds`
只统计实际发往远端的操作耗时，被 `OutcomeIgnore` 分类（如调用方取消）的不计入。

## 性能与并发模型

- **并发安全**：每个 `Governor` 一把锁、每个信号一把锁；无后台协程、
  无 ticker（滑窗惰性过期，读写时清理）。
- **天然分片**：一个下游目标一个实例，锁不跨实例——多目标部署的竞争
  随实例数摊薄，无需额外协调。
- **开销定位**：控制路径是纯内存计算，成本远低于其保护的远端调用；
  量级与扩展性请在自己的目标平台实测——[bench_test.go](bench_test.go)
  头部含基线、并行度扩展、锁竞争（mutex profile）、CPU 热点四组命令。

## 能力边界（ADR-0002）

控制动作仅三种，另有只读观测输出（观测不是控制）。不是熔断器、不内置降级、
不是并发信号量（不限 in-flight）、不是重试引擎（`Wait` 是延迟执行——延时不丢弃——不是失败后退避）。
完整否定清单见 [ADR-0002](docs/adr/0002-flow-control-only-no-breaker.md)。

## 文档

- [CONTEXT.md](CONTEXT.md) — 领域术语表
- [docs/strategies.md](docs/strategies.md) — 策略与参数手册（各策略公式、参数约束、指标与日志）
- [docs/desc.md](docs/desc.md) — 算法理论设计
- [docs/adr/](docs/adr/) — 架构决策记录

## 许可证

[MIT](LICENSE) © JNinng
