package governor

import (
	"fmt"
	"sync/atomic"
	"time"
)

// 感知层：在途请求数 / 队列深度信号。
// Signal 接口没有"操作开始"事件（Observe 只在 Record 完成时触发，Allow/Begin
// 不通知信号），统计不出"在途"，故本类型在 Signal 三方法之外另暴露 Enter/Leave
// 显式计数；为 Signal 加开始通知属碰契约，不做。

const defaultConcurrencyTarget = 100

// ConcurrencySignal 以在途操作数或队列深度为压力来源：
//
//	S = max(0, (n − N_target) / N_target)
//
// n 为当前计数，N_target 是压力 0 的分界线；超出目标一倍即 S=1（与 LatencySignal
// 同形），随决策曲线 α = S/(S+C) 照常合成。计数不随时间衰减，只反映当前状态。
//
// 两种口径：
//   - 在途请求数：放行后 Enter、完成后 Leave，本地丢弃不计入（同 desc.md §3.1）。
//     经传输层适配器使用时由适配器自动配对；无适配器时业务手动配对（建议 defer）。
//   - 队列深度：业务在入队处 Enter、出队/完成处 Leave。
//
// 未配对的 Leave 钳制在 0；未配对的 Enter 会永久抬高压力（泄漏），
// Enter/Leave 务必成对。Observe 为空实现——计数只由 Enter/Leave 驱动，
// Record 反馈不改变它。
type ConcurrencySignal struct {
	target float64
	n      atomic.Int64
}

// ConcurrencyOption 配置 ConcurrencySignal。
type ConcurrencyOption func(*ConcurrencySignal) error

// WithConcurrencyTarget 设置压力分界线 N_target（默认 100），须 > 0。
// n <= N_target 时压力恒 0；超出部分线性升压（n = 2×N_target 时 S=1）。
// 按下游实际并发容量设定。
func WithConcurrencyTarget(n int) ConcurrencyOption {
	return func(s *ConcurrencySignal) error {
		if n <= 0 {
			return fmt.Errorf("governor: concurrency target must be positive, got %d", n)
		}
		s.target = float64(n)
		return nil
	}
}

// NewConcurrencySignal 创建在途 / 队列深度信号。
func NewConcurrencySignal(opts ...ConcurrencyOption) (*ConcurrencySignal, error) {
	s := &ConcurrencySignal{target: defaultConcurrencyTarget}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Name 实现 Signal。
func (s *ConcurrencySignal) Name() string { return "concurrency" }

// Observe 实现 Signal（空实现：计数只由 Enter/Leave 驱动）。
func (s *ConcurrencySignal) Observe(time.Time, Outcome, time.Duration) {}

// Pressure 实现 Signal（只读，恒 >= 0）。
func (s *ConcurrencySignal) Pressure(time.Time) float64 {
	n := float64(s.n.Load())
	if n <= s.target {
		return 0
	}
	return (n - s.target) / s.target
}

// Enter 记一个操作进入（在途 +1 / 队列入队 +1）。
func (s *ConcurrencySignal) Enter() { s.n.Add(1) }

// Leave 记一个操作离开（完成 -1 / 队列出队 -1）。计数钳制在 >= 0，
// 多余的 Leave 不产生负值。
func (s *ConcurrencySignal) Leave() {
	for {
		n := s.n.Load()
		if n <= 0 {
			return
		}
		if s.n.CompareAndSwap(n, n-1) {
			return
		}
	}
}

// InFlight 返回当前计数（在途操作数 / 队列深度，只读观测）。
func (s *ConcurrencySignal) InFlight() int { return int(s.n.Load()) }
