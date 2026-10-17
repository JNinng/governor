package governor

// 状态跃迁的类型化回调出口（观测非控制）：跃迁发生时锁外同步派发 Transition，
// 供告警 / webhook 接入，无需解析日志文本。跃迁判定在持锁段内完成，用户代码
// 经 fireTransition 移到锁外执行——慢回调只拖慢触发它的那次调用，不阻塞热路径。

// TransitionKind 标识一次状态跃迁的种类，取值即对应日志事件名。
type TransitionKind string

const (
	// TransitionSuppressionStarted α 上穿 0.5。
	TransitionSuppressionStarted TransitionKind = "suppression_started"
	// TransitionDeepSuppression α 上穿 0.9。判定沿用既有优先级：α 从 <0.5
	// 直接越过 0.9 时只发 TransitionSuppressionStarted、不发本事件——
	// 快速爬坡会跳过 deep 事件，告警消费方须知。
	TransitionDeepSuppression TransitionKind = "deep_suppression"
	// TransitionSuppressionStopped α 归 0。
	TransitionSuppressionStopped TransitionKind = "suppression_stopped"
	// TransitionWaitEngaged 等待首次非零（wait = T_base×α > 0）。
	TransitionWaitEngaged TransitionKind = "wait_engaged"
	// TransitionWaitReleased 等待归零。运行时把 T_base 调为 0 也会触发本事件，
	// 此时 Driver 等归因字段反映事件发生时的压力，而非压力消失。
	TransitionWaitReleased TransitionKind = "wait_released"
)

// Transition 是一次状态跃迁的只读事件（观测不是控制：回调里做告警/webhook
// 可以，改变配置不行）。From/To 的单位随 Kind：suppression_* 三类为跃迁前后的
// α（0~1），wait_* 两类为跃迁前后的等待时长（秒，wait = T_base × α）。
type Transition struct {
	Kind           TransitionKind
	From           float64
	To             float64
	Driver         string  // 压力最大的信号名（归因；无信号时为空串）
	DriverPressure float64 // 该信号的压力值
}
