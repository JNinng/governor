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

### 复制即跑的最小示例

模拟一个第 6 次请求起开始过载的下游（响应 300ms），完整程序见
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

### 真实 HTTP 一键演示

```bash
go run ./example/http
```

内置一个响应 250ms 的本地"过载服务"（目标 100ms），演示 `http.Client` 场景、
observ 日志（slog 以 Info 阈值接入）与降级统计（[example/http/main.go](example/http/main.go)）：

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
模拟下游三档梯度（600ms 轻度过载 → 1100ms 加深 → 200ms 恢复，目标 500ms），
实测输出——等待随压力**阶梯式上升**，恢复后一两次采样即归零，
**12 次执行无一被丢弃**；示例以 Info 阈值接入 slog，只记状态跃迁
（`WARN suppression started` / `INFO suppression stopped`），DEBUG 级
`wait changed`（带 from→to 明细）静默，需要排查时调到 Debug 即见：

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

### 为什么我的请求被丢了？

`ErrSuppressed` 意味着 governor 根据反馈判断下游已过载，在**发出之前**就丢弃了请求。
业务应在此返回降级响应；即使深度抑制，探测放行（默认 2%）仍持续发出真实请求，
远端恢复后 α 自动回落、放行率随之恢复，无需人工干预。

### 两种信号与多信号合成

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

## 埋点（observ）

```go
import (
	"github.com/jninng/observ"
	obsprom "github.com/jninng/observ/adapters/prom"
)

g, _ := governor.New(
	governor.WithSignals(lat),
	governor.WithMeter(obsprom.New(registry)), // 默认 observ.NoopMeter
	governor.WithLogger(zaplog.New(zapLogger)), // 缺省构造期快照 observ.DefaultLogger()
	governor.WithMetricPrefix("redis"),        // 多实例区分（无 label 约束）
)
```

指标一览（前缀默认 `governor`）：`_pressure`、`_suppression`、`_batch_size`、
`_wait_seconds`（gauge），`_decisions_allow_total` / `_decisions_drop_total` /
`_decisions_probe_total`（counter），`_operations_seconds`（histogram）。
放行决策只计 allow、探测放行只计 probe（互斥不叠加）；`_operations_seconds`
只统计实际发往远端的操作耗时，被 `OutcomeIgnore` 分类（如调用方取消）的不计入。

## 能力边界（ADR-0002）

控制动作仅三种，另有只读观测输出（观测不是控制）。不是熔断器、不内置降级、
不是并发信号量（不限 in-flight）、不是重试引擎（`Wait` 是延迟执行——延时不丢弃——不是失败后退避）。
完整否定清单见 [ADR-0002](docs/adr/0002-flow-control-only-no-breaker.md)。

## 文档

- [CONTEXT.md](CONTEXT.md) — 领域术语表
- [docs/desc.md](docs/desc.md) — 算法理论设计
- [docs/adr/](docs/adr/) — 架构决策记录
