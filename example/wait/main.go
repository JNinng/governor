// 延迟执行（Wait）示例：后台任务在下游过载时"延迟而不丢弃"。
// 模拟下游三档梯度：第 1~3 次轻度过载（执行 600ms，目标 500ms）、
// 4~8 次加深（1100ms）、随后恢复（200ms）。观察等待时长随压力
// 阶梯式上升、恢复后一两次采样即归零——全程没有任何一次执行被丢弃。
// 运行：go run ./example/wait
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jninng/governor"
	"github.com/jninng/observ"
)

func main() {
	// 以 Info 级别阈值接入 slog：只记录状态跃迁
	// （WARN suppression started / INFO suppression stopped），
	// DEBUG 级的 wait changed 明细静默——生产推荐的安静模式。
	// 想看每次等待变化时把 Level 调为 slog.LevelDebug 即可。不设置时缺省为 Noop。
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	observ.SetDefaultLogger(observ.NewSlogLogger(logger))

	// 1) 感知：响应时长超过 500ms 视为压力
	sig, err := governor.NewLatencySignal(governor.WithLatencyTarget(500 * time.Millisecond))
	if err != nil {
		panic(err)
	}

	// 2) 流控器：T_base 是 α=1 时的最大等待；过载时每次执行前等待 T_base×α
	g, err := governor.New(
		governor.WithSignals(sig),
		governor.WithBaseWait(2*time.Second),
	)
	if err != nil {
		panic(err)
	}

	// 模拟下游三档梯度：轻度过载 → 加深 → 恢复
	remoteTook := func(i int) time.Duration {
		switch {
		case i <= 3:
			return 600 * time.Millisecond
		case i <= 8:
			return 1100 * time.Millisecond
		default:
			return 200 * time.Millisecond
		}
	}

	ctx := context.Background()
	for i := 1; i <= 12; i++ {
		begin := time.Now()
		dec := g.Wait(ctx) // 睡 T_base×α 后返回令牌；α=0 时不等待
		waited := time.Since(begin)

		took := remoteTook(i)
		time.Sleep(took) // ← 替换为你的后台操作（如日志同步）
		dec.Record(nil)  // 本次执行耗时自动计入反馈

		fmt.Printf("第 %2d 次  等待 %4.0fms │ 执行 %4.0fms │ α=%.2f\n",
			i, float64(waited.Milliseconds()), float64(took.Milliseconds()), g.Suppression())
	}
	fmt.Println("\n过载加深时等待阶梯式上升（0 → 334 → ~1087ms），无一执行被丢弃；")
	fmt.Println("恢复后因 EWMA 平滑有一两次滞后，随后等待归零、全速运行。")
}
