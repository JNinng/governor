package governor

// 快照导出：把 Pressure/Suppression/Signals 三个只读出口聚为一个结构体，
// 免去业务逐个拼装；单次加锁采集，字段彼此一致（对应同一时刻的信号状态）。

// Snapshot 是 Governor 当前状态的只读快照（观测非控制）。字段带 JSON 友好标签，
// 可直接挂调试端点或落日志。
type Snapshot struct {
	Pressure    float64          `json:"pressure"`    // 合成压力指数 S（最差信号优先）
	Suppression float64          `json:"suppression"` // 抑制因子 α
	Signals     []SignalSnapshot `json:"signals"`     // 各信号的独立压力
}

// Snapshot 返回当前状态的只读快照：合成压力、抑制因子与各信号压力在单次加锁内
// 采集。反馈路径（Record）驱动的状态严格一致——不会采到反馈中途的拼凑视图；
// 经 Feed/Enter 外部喂入的信号不持该锁，各自为原子读（逐个调用
// Pressure/Suppression/Signals 则可能采到并发反馈的不同侧面）。JSON 友好：
//
//	{"pressure":2,"suppression":0.667,"signals":[{"name":"latency","pressure":2}]}
func (g *Governor) Snapshot() Snapshot {
	cfg := g.cfg.Load()
	g.mu.Lock()
	defer g.mu.Unlock()
	now := cfg.clock.Now()
	s := g.pressureLocked(now)
	snaps := make([]SignalSnapshot, len(g.signals))
	for i, sig := range g.signals {
		snaps[i] = SignalSnapshot{Name: sig.Name(), Pressure: sig.Pressure(now)}
	}
	return Snapshot{
		Pressure:    s,
		Suppression: SuppressionFactor(s, cfg.c),
		Signals:     snaps,
	}
}
