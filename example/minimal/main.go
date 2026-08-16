// 最小可运行示例：模拟下游在第 6 次请求后开始过载（响应 300ms），
// 观察 α 爬升、概率丢弃与业务降级。README「快速上手」内嵌同一段代码。
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
