package governor

import (
	"fmt"
	"sync"
	"time"
)

// 感知层：服务端指令信号。下游以 Retry-After（HTTP）或 RetryInfo（gRPC）显式
// 下发的等待指令是最权威的压力来源——无需等滑窗 / EWMA 慢慢收敛。
// Signal 接口的 Observe 只承载操作结局，承载不了"服务端要求等待多久"这条指令，
// 且 Record 会把真实耗时喂给所有信号，重载 took 无法区分两条来源——
// 故本类型自带 Feed/FeedAt 喂入方法，适配器与业务调同一方法。

const defaultDirectiveScale = time.Second

// DirectiveSignal 把服务端下发的等待指令归一化为压力：喂入时 S = 指令时长 /
// 换算基准，随后随指令自身时长线性衰减至 0——指令到期即自行解除，不产生
// 陈旧压力（区别于窗口/阈值类信号的饥饿沿用语义），不会钉住 α 妨碍感知恢复。
//
// 与其他信号经 max 合成，无特殊通道。Observe 为空实现：该信号只由 Feed/FeedAt
// 驱动，Record 反馈不改变它。多次喂入以最后一次为准；喂入 0 等于清除当前指令。
//
// 喂入口径：传输层适配器自动解析（httpx 解析 Retry-After 头，grpcx 解析
// RetryInfo detail），或业务手动喂——X-RateLimit 等无统一格式的头由业务自行
// 换算为等待时长后调用同一方法。
type DirectiveSignal struct {
	mu       sync.Mutex
	scale    float64       // 换算基准（秒）：服务端要求等待该时长时 S=1
	fedAt    time.Time     // 当前指令的起算时刻
	duration time.Duration // 当前指令的等待时长
}

// DirectiveOption 配置 DirectiveSignal。
type DirectiveOption func(*DirectiveSignal) error

// WithDirectiveScale 设置换算基准（默认 1s），须 > 0：服务端要求等待该时长时
// S=1（如 5s 指令 → S=5，C=1 时 α≈0.83）。按下游单次限流的典型恢复时长设定。
func WithDirectiveScale(d time.Duration) DirectiveOption {
	return func(s *DirectiveSignal) error {
		if d <= 0 {
			return fmt.Errorf("governor: directive scale must be positive, got %v", d)
		}
		s.scale = d.Seconds()
		return nil
	}
}

// NewDirectiveSignal 创建服务端指令信号。
func NewDirectiveSignal(opts ...DirectiveOption) (*DirectiveSignal, error) {
	s := &DirectiveSignal{scale: defaultDirectiveScale.Seconds()}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Name 实现 Signal。
func (s *DirectiveSignal) Name() string { return "directive" }

// Observe 实现 Signal（空实现：只由 Feed/FeedAt 驱动）。
func (s *DirectiveSignal) Observe(time.Time, Outcome, time.Duration) {}

// Pressure 实现 Signal（只读，恒 >= 0）：剩余等待时长 / 换算基准，
// 指令到期后为 0。
func (s *DirectiveSignal) Pressure(now time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.duration <= 0 {
		return 0
	}
	remaining := s.duration - now.Sub(s.fedAt)
	if remaining <= 0 {
		return 0
	}
	return remaining.Seconds() / s.scale
}

// Feed 喂入一条服务端等待指令（以当前时刻起算）；d 为负按 0 处理，
// 喂入 0 等于清除当前指令。
func (s *DirectiveSignal) Feed(d time.Duration) { s.FeedAt(time.Now(), d) }

// FeedAt 同 Feed，起算时刻由调用方给定（时钟注入的测试用）。
func (s *DirectiveSignal) FeedAt(now time.Time, d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fedAt = now
	s.duration = d
}
