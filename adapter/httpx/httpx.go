// Package httpx 提供标准库 net/http 的传输层适配器：一个实现 http.RoundTripper
// 的包装，按 host 自动创建并复用 Governor，自动完成决策与反馈（成败 + 耗时）。
//
// 语义（纯包装层，不改 Do/Allow 契约）：
//   - 命中抑制时 RoundTrip 直接返回 governor.ErrSuppressed，请求不会发往远端；
//     http.Client 会把错误包装为 *url.Error，调用方以 errors.Is 判别后降级；
//   - 放行的请求结束后自动 Record：结局由分类器归一（默认网络错误与 429/5xx 为失败、
//     调用方取消为忽略、其余为成功），耗时自 Allow 起算，与 Do 一致；
//   - 请求级旁路：ctx 带 WithoutSuppression 标记的请求完全绕过抑制与反馈，
//     不做 Allow 判定（不会 ErrSuppressed）、不 Record（不进任何统计），
//     适用于 health check 等不参与抑制的调用。
//
// 边界：host→Governor 注册表持有在 Transport 实例内，非包级全局；Transport 可独立
// 构造与丢弃，Governor 仍可脱离适配器单独构造与使用。
//
// 用法：
//
//	tr, err := httpx.New(nil) // nil 时底层用 http.DefaultTransport
//	if err != nil { ... }
//	client := &http.Client{Transport: tr}
//	resp, err := client.Get(url)
//	if errors.Is(err, governor.ErrSuppressed) {
//	    return fallback, nil // 过载被本地丢弃：在此降级
//	}
package httpx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/jninng/governor"
	"github.com/jninng/observ"
)

// Transport 是 http.RoundTripper 包装：按 host 惰性创建 Governor（首个请求时构造，
// 失败不缓存、下次请求重试），放行的请求结束后自动 Record。
// 并发安全；多请求共享同一实例（http.Client.Transport 即此用法）。
type Transport struct {
	base http.RoundTripper

	mu         sync.RWMutex
	governors  map[string]*governor.Governor
	newSignals func() ([]governor.Signal, error)
	classify   func(resp *http.Response, err error) governor.Outcome
	prefix     func(host string) string
	meter      observ.Meter
	sensitiv   *float64
	probeRatio *float64
}

// New 创建适配器。base 为 nil 时使用 http.DefaultTransport；
// 任一 option 非法时报错（与 governor.New 一致，构造失败不产生半成品）。
func New(base http.RoundTripper, opts ...Option) (*Transport, error) {
	t := &Transport{
		base:       base,
		governors:  make(map[string]*governor.Governor),
		newSignals: defaultSignals,
		classify:   defaultClassify,
		prefix:     defaultPrefix,
	}
	if t.base == nil {
		t.base = http.DefaultTransport
	}
	for _, opt := range opts {
		if err := opt(t); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// RoundTrip 实现 http.RoundTripper，不应直接调用，经 http.Client 使用。
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if bypassed(req.Context()) {
		return t.base.RoundTrip(req)
	}
	g, err := t.governorFor(req.URL.Host)
	if err != nil {
		return nil, err
	}
	dec := g.Allow()
	if !dec.Allowed() {
		return nil, governor.ErrSuppressed
	}
	resp, err := t.base.RoundTrip(req)
	dec.Record(t.recordInput(resp, err))
	return resp, err
}

// Governor 返回 host 对应的 Governor，不存在则创建（与请求路径共用同一注册表），
// 供业务查询只读状态（Pressure / Suppression / Signals，观测非控制）。
// 构造失败时返回错误且不缓存。
func (t *Transport) Governor(host string) (*governor.Governor, error) {
	return t.governorFor(host)
}

// WithoutSuppression 标记该 ctx 上的请求完全绕过抑制与反馈：
// 不做 Allow 判定（不会 ErrSuppressed）、不 Record（不进任何统计）。
// 适用于 health check 等不应参与抑制的请求。
func WithoutSuppression(ctx context.Context) context.Context {
	return context.WithValue(ctx, bypassKey{}, true)
}

type bypassKey struct{}

func bypassed(ctx context.Context) bool {
	v, _ := ctx.Value(bypassKey{}).(bool)
	return v
}

// ---- option ----

// Option 配置 Transport；全部作用于其惰性创建的每一个 Governor。
type Option func(*Transport) error

// WithSignalFactory 注入信号工厂，每创建一个 Governor 调用一次（信号不可跨实例共享）。
// 返回空切片表示该 host 无信号（压力恒 0，全部放行）。nil 报错；
// 不传时默认 RejectionSignal（K=2、窗口 90s），与 HTTP 显式拒绝（429/5xx）口径配套。
func WithSignalFactory(newSignals func() ([]governor.Signal, error)) Option {
	return func(t *Transport) error {
		if newSignals == nil {
			return fmt.Errorf("governor/httpx: signal factory must not be nil")
		}
		t.newSignals = newSignals
		return nil
	}
}

// WithClassifier 注入结局分类器，整体替换默认分类（网络错误/429/5xx 失败、
// 调用方取消忽略、其余成功）。nil 报错。
func WithClassifier(classify func(resp *http.Response, err error) governor.Outcome) Option {
	return func(t *Transport) error {
		if classify == nil {
			return fmt.Errorf("governor/httpx: classifier must not be nil")
		}
		t.classify = classify
		return nil
	}
}

// WithSensitivity 设置决策层敏感度 C（不传沿用 governor 默认 1.0），须 > 0。
func WithSensitivity(c float64) Option {
	return func(t *Transport) error {
		if c <= 0 {
			return fmt.Errorf("governor/httpx: sensitivity C must be positive, got %v", c)
		}
		v := c
		t.sensitiv = &v
		return nil
	}
}

// WithProbeRatio 设置探测放行比例（不传沿用 governor 默认 0.02），须在 [0,1) 内。
func WithProbeRatio(r float64) Option {
	return func(t *Transport) error {
		if r < 0 || r >= 1 {
			return fmt.Errorf("governor/httpx: probe ratio must be in [0,1), got %v", r)
		}
		v := r
		t.probeRatio = &v
		return nil
	}
}

// WithMetricPrefix 注入指标前缀函数。observ 无 label，前缀是唯一合规的实例级隔离
// 手段（ADR-0001）；nil 报错；不传时默认 "governor_<host>"（host 中除 [a-z0-9_]
// 外的字符替换为下划线）。
func WithMetricPrefix(prefix func(host string) string) Option {
	return func(t *Transport) error {
		if prefix == nil {
			return fmt.Errorf("governor/httpx: metric prefix must not be nil")
		}
		t.prefix = prefix
		return nil
	}
}

// WithMeter 注入指标 Meter（不传为 observ.NoopMeter），不得为 nil。
func WithMeter(m observ.Meter) Option {
	return func(t *Transport) error {
		if m == nil {
			return fmt.Errorf("governor/httpx: meter must not be nil")
		}
		t.meter = m
		return nil
	}
}

// ---- 内部 ----

// 分类结果到 Record 输入的桥接：Record 只接收 error，成功传 nil、忽略与失败传
// 对应哨兵，由 coreClassifier 还原为 Outcome（哨兵不会离开本包，不影响调用方）。
var (
	errClassifiedFailure = errors.New("governor/httpx: classified as failure")
	errClassifiedIgnore  = errors.New("governor/httpx: classified as ignore")
)

// coreClassifier 是注入各 Governor 的分类器，只识别本包哨兵；
// 业务级成败语义应经 WithClassifier 在适配器层定制。
func coreClassifier(err error) governor.Outcome {
	switch {
	case err == nil:
		return governor.OutcomeSuccess
	case errors.Is(err, errClassifiedIgnore):
		return governor.OutcomeIgnore
	default:
		return governor.OutcomeFailure
	}
}

// recordInput 把分类结果映射为 Record 的输入。
func (t *Transport) recordInput(resp *http.Response, err error) error {
	switch t.classify(resp, err) {
	case governor.OutcomeSuccess:
		return nil
	case governor.OutcomeIgnore:
		return errClassifiedIgnore
	default:
		return errClassifiedFailure
	}
}

// defaultClassify 是默认结局分类：网络错误为失败（下游不可达即压力），
// 调用方取消为忽略（不计入统计），HTTP 429 与 5xx 为失败
// （desc.md §3.2 场景 A 的显式拒绝），其余响应为成功。
func defaultClassify(resp *http.Response, err error) governor.Outcome {
	switch {
	case err != nil:
		if errors.Is(err, context.Canceled) {
			return governor.OutcomeIgnore
		}
		return governor.OutcomeFailure
	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= http.StatusInternalServerError:
		return governor.OutcomeFailure
	default:
		return governor.OutcomeSuccess
	}
}

func defaultSignals() ([]governor.Signal, error) {
	rej, err := governor.NewRejectionSignal()
	if err != nil {
		return nil, err
	}
	return []governor.Signal{rej}, nil
}

// defaultPrefix 默认指标前缀：governor_<host>。
func defaultPrefix(host string) string {
	return "governor_" + sanitize(host)
}

// sanitize 把字符串规整为合法指标名片段：大写转小写，除 [a-z0-9_] 外的字符
// 统一替换为下划线。
func sanitize(s string) string {
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

// governorFor 返回 host 对应的 Governor，惰性创建；构造失败返回错误且不缓存。
// 双重检查：读锁命中直返，未命中升级写锁后再查一次（并发首访同一 host 只构造一次）。
func (t *Transport) governorFor(host string) (*governor.Governor, error) {
	t.mu.RLock()
	g, ok := t.governors[host]
	t.mu.RUnlock()
	if ok {
		return g, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if g, ok := t.governors[host]; ok {
		return g, nil
	}
	g, err := t.build(host)
	if err != nil {
		return nil, err
	}
	t.governors[host] = g
	return g, nil
}

func (t *Transport) build(host string) (*governor.Governor, error) {
	sigs, err := t.newSignals()
	if err != nil {
		return nil, fmt.Errorf("governor/httpx: build signals for host %q: %w", host, err)
	}
	opts := []governor.Option{
		governor.WithSignals(sigs...),
		governor.WithMetricPrefix(t.prefix(host)),
		governor.WithClassifier(coreClassifier),
	}
	if t.meter != nil {
		opts = append(opts, governor.WithMeter(t.meter))
	}
	if t.sensitiv != nil {
		opts = append(opts, governor.WithSensitivity(*t.sensitiv))
	}
	if t.probeRatio != nil {
		opts = append(opts, governor.WithProbeRatio(*t.probeRatio))
	}
	g, err := governor.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("governor/httpx: build governor for host %q: %w", host, err)
	}
	return g, nil
}
