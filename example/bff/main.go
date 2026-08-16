// 业务场景示例：商品页 BFF 调用用户服务取昵称——晚高峰用户服务过载，
// 响应从 80ms 劣化到 500ms；governor 感知劣化逐步加压，
// 被抑制的请求在发出前被本地丢弃，业务回本地缓存兜底。
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jninng/governor"
)

func main() {
	// 1) 感知什么：用户服务正常响应约 80ms，超过 100ms 视为过载前兆
	//    （EWMA 平滑，β 默认 0.4）
	sig, err := governor.NewLatencySignal(governor.WithLatencyTarget(100 * time.Millisecond))
	if err != nil {
		panic(err)
	}

	// 2) 建流控器：一个下游目标（用户服务）一个实例；C=1 表示 S=C 时抑制一半流量
	g, err := governor.New(governor.WithSignals(sig))
	if err != nil {
		panic(err)
	}

	// 模拟用户服务：平时 80ms；晚高峰（第 6 次请求起）过载劣化到 500ms
	rushHour := false
	userService := func(ctx context.Context) (string, error) {
		if rushHour {
			time.Sleep(500 * time.Millisecond)
			return "用户服务实时昵称", nil
		}
		time.Sleep(80 * time.Millisecond)
		return "用户服务实时昵称", nil
	}

	for i := 1; i <= 12; i++ {
		if i == 6 {
			rushHour = true // 晚高峰开始，用户服务过载
		}
		nick, err := governor.Do(context.Background(), g, userService)
		switch {
		case errors.Is(err, governor.ErrSuppressed):
			// 过载被本地丢弃：降级方式由业务决定，这里回本地缓存
			fmt.Printf("请求 %2d  降级→本地缓存昵称   α=%.2f\n", i, g.Suppression())
		case err != nil:
			fmt.Printf("请求 %2d  远端错误: %v\n", i, err)
		default:
			fmt.Printf("请求 %2d  远端OK  %s   α=%.2f\n", i, nick, g.Suppression())
		}
	}
}
