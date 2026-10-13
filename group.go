package governor

import (
	"fmt"
	"sync"
)

// Group 是绑定多个远端目标的多下游管理器：按 key 惰性创建并复用 Governor，
// 统一生成指标前缀，聚合信号快照。构造便利工具，不是全局单例——各实例独立
// 持锁（天然分片语义不变），仍可脱离 Group 单独构造与使用。
//
// 信号持有可变统计状态，不可跨 Governor 共享，故经信号工厂逐实例创建；
// observ 无 label，按 key 生成的指标前缀是唯一合规的实例级隔离手段（ADR-0001）。
type Group struct {
	mu         sync.RWMutex
	governors  map[string]*Governor
	prefixes   map[string]string // 规整后的指标前缀 → key，创建时检测碰撞
	newSignals func() ([]Signal, error)
	prefix     func(key string) string
	govOpts    []Option
}

// GroupOption 配置 Group；除模板 option 外，全部作用于惰性创建的每一个实例。
type GroupOption func(*Group) error

// NewGroup 创建多下游管理器。任一 option 非法时报错（与 New 一致，
// 构造失败不产生半成品）。
func NewGroup(opts ...GroupOption) (*Group, error) {
	g := &Group{
		governors:  make(map[string]*Governor),
		prefixes:   make(map[string]string),
		newSignals: defaultGroupSignals,
		prefix:     defaultGroupPrefix,
	}
	for _, opt := range opts {
		if err := opt(g); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// Get 返回 key 对应的 Governor，惰性创建并按 key 复用；构造失败时报错且不缓存
// （下次调用重试）。空 key 报错；不同 key 经前缀规整后若产生相同指标前缀，
// 创建时报错——key 须在规整后唯一，或经 WithKeyPrefix 自定义消除碰撞。
// 实例用法与单独构造的 Governor 完全一致（Do / Allow / Wait / BatchSize / Update），
// 实例的操作不经过 Group 的锁；Get 本身每次取读锁，建议启动期获取实例并长期复用，
// 而非每个请求调用一次。
func (g *Group) Get(key string) (*Governor, error) {
	if key == "" {
		return nil, fmt.Errorf("governor: group key must not be empty")
	}
	g.mu.RLock()
	gov, ok := g.governors[key]
	g.mu.RUnlock()
	if ok {
		return gov, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if gov, ok := g.governors[key]; ok {
		return gov, nil
	}
	prefix := g.prefix(key)
	if existing, ok := g.prefixes[prefix]; ok {
		// 走到这里说明 key 尚未创建，前缀已被其他 key 占用即碰撞。
		return nil, fmt.Errorf(
			"governor: metric prefix %q of key %q collides with key %q; keys must be unique after prefix sanitization (see WithKeyPrefix)",
			prefix, key, existing)
	}
	gov, err := g.build(key, prefix)
	if err != nil {
		return nil, err
	}
	g.governors[key] = gov
	g.prefixes[prefix] = key
	return gov, nil
}

// Keys 返回已创建实例的 key（无序），不触发创建；供枚举排查使用。
func (g *Group) Keys() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	keys := make([]string, 0, len(g.governors))
	for key := range g.governors {
		keys = append(keys, key)
	}
	return keys
}

// Signals 聚合全部已创建实例的信号快照，按 key 分组（观测非控制）。
// 仅含已创建的实例——尚未经 Get 创建的 key 不出现。
func (g *Group) Signals() map[string][]SignalSnapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make(map[string][]SignalSnapshot, len(g.governors))
	for key, gov := range g.governors {
		out[key] = gov.Signals()
	}
	return out
}

// ---- option ----

// WithSignalFactory 注入信号工厂，每创建一个实例调用一次（信号不可跨实例共享）。
// 返回空切片表示无信号（压力恒 0，全部放行）。nil 报错；
// 不传时默认 RejectionSignal（K=2、窗口 90s）。
func WithSignalFactory(newSignals func() ([]Signal, error)) GroupOption {
	return func(g *Group) error {
		if newSignals == nil {
			return fmt.Errorf("governor: signal factory must not be nil")
		}
		g.newSignals = newSignals
		return nil
	}
}

// WithKeyPrefix 注入指标前缀函数。observ 无 label，前缀是唯一合规的实例级隔离
// 手段（ADR-0001）；nil 报错；不传时默认 "governor_<key>"（key 中除 [a-z0-9_]
// 外的字符统一替换为下划线）。不同 key 规整后产生相同前缀时，后创建的 key 报错
// （如 "user.service" 与 "user_service"），自定义前缀函数的输出同样参与检测。
func WithKeyPrefix(prefix func(key string) string) GroupOption {
	return func(g *Group) error {
		if prefix == nil {
			return fmt.Errorf("governor: key prefix must not be nil")
		}
		g.prefix = prefix
		return nil
	}
}

// WithGovernorOptions 透传核心 option 作为每个实例的构造模板，用于调节敏感度、
// 探测比例、批次、基准等待、分类器、Meter 等。其中的 WithSignals / WithMetricPrefix
// 会被 Group 接管的同名配置覆盖（信号经工厂逐实例创建，前缀按 key 生成）；
// 取值非法（如 C <= 0）在该实例首次创建时报错。
func WithGovernorOptions(govOpts ...Option) GroupOption {
	return func(g *Group) error {
		for _, opt := range govOpts {
			if opt == nil {
				return fmt.Errorf("governor: governor option must not be nil")
			}
		}
		g.govOpts = append(g.govOpts, govOpts...)
		return nil
	}
}

// ---- 内部 ----

// build 构造单个实例，须持有写锁：模板 option 在前，Group 接管的信号与前缀在后
// （后应用的 option 覆盖先应用的）。
func (g *Group) build(key, prefix string) (*Governor, error) {
	sigs, err := g.newSignals()
	if err != nil {
		return nil, fmt.Errorf("governor: build signals for %q: %w", key, err)
	}
	opts := make([]Option, 0, len(g.govOpts)+2)
	opts = append(opts, g.govOpts...)
	opts = append(opts,
		WithSignals(sigs...),
		WithMetricPrefix(prefix),
	)
	gov, err := New(opts...)
	if err != nil {
		return nil, fmt.Errorf("governor: build governor for %q: %w", key, err)
	}
	return gov, nil
}

func defaultGroupSignals() ([]Signal, error) {
	rej, err := NewRejectionSignal()
	if err != nil {
		return nil, err
	}
	return []Signal{rej}, nil
}

// defaultGroupPrefix 默认指标前缀：governor_<key>。
func defaultGroupPrefix(key string) string {
	return "governor_" + sanitizeMetricName(key)
}

// sanitizeMetricName 把字符串规整为合法指标名片段：大写转小写，除 [a-z0-9_] 外
// 的字符统一替换为下划线。
func sanitizeMetricName(s string) string {
	b := []byte(s)
	for i, c := range b {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
		case c >= 'A' && c <= 'Z':
			b[i] = c + ('a' - 'A')
		default:
			b[i] = '_'
		}
	}
	return string(b)
}
