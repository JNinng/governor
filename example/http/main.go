// 真实 HTTP 演示：内置一个响应 250ms 的本地"过载服务"（目标 100ms），
// 展示 http.Client 场景下的概率丢弃、业务降级与 observ 日志跃迁。
// 运行：go run ./example/http
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jninng/governor"
	"github.com/jninng/observ"
)

func main() {
	// 以 Info 级别阈值接入 slog：只显示状态跃迁（WARN suppression started /
	// INFO suppression stopped），DEBUG 级调整明细静默。
	// 不设置时缺省为 Noop，指标日志均零开销。
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	observ.SetDefaultLogger(observ.NewSlogLogger(logger))

	// 内置"过载服务"：固定响应 250ms
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(250 * time.Millisecond)
		fmt.Fprint(w, "remote-payload")
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	url := "http://" + ln.Addr().String()

	// 1) 感知：响应时长超过 100ms 视为压力
	sig, err := governor.NewLatencySignal(governor.WithLatencyTarget(100 * time.Millisecond))
	if err != nil {
		log.Fatalf("创建信号失败: %v", err)
	}
	// 2) 流控器：接真实下游时，按目标各建一个实例；
	//    需要指标时再传 WithMeter（见 README「埋点」）。
	g, err := governor.New(governor.WithSignals(sig))
	if err != nil {
		log.Fatalf("创建流控器失败: %v", err)
	}

	client := &http.Client{}
	ok, suppressed := 0, 0
	for i := 1; i <= 60; i++ {
		body, err := governor.Do(context.Background(), g, func(ctx context.Context) (string, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return "", err
			}
			resp, err := client.Do(req)
			if err != nil {
				return "", err
			}
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			return string(b), err
		})
		switch {
		case errors.Is(err, governor.ErrSuppressed):
			suppressed++
			fmt.Printf("%3d  被抑制 → 降级为兜底缓存   α=%.2f\n", i, g.Suppression())
		case err != nil:
			fmt.Printf("%3d  远端错误: %v\n", i, err)
		default:
			ok++
			fmt.Printf("%3d  OK  %-16s α=%.2f\n", i, body, g.Suppression())
		}
	}
	fmt.Printf("\n合计: 放行 %d，被抑制（走降级）%d，最终 α=%.2f\n", ok, suppressed, g.Suppression())
	fmt.Println("远端恢复后 α 会自动回落，放行率随之恢复——无需任何人工干预。")
}
