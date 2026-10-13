// Package grpcx 提供 gRPC client 的传输层适配器：Unary / Stream client interceptor，
// 按 ClientConn target 自动创建并复用 Governor，自动完成决策与反馈（成败 + 耗时）。
//
// 本包是独立 Go module（不随核心库依赖 grpc-go）：核心库保持零第三方依赖
// （ADR-0001 同款考量），仅使用本适配器的用户自行引入 google.golang.org/grpc。
//
// 语义（纯包装层，不改 Allow 契约）：
//   - 命中抑制时 RPC 直接返回 governor.ErrSuppressed（未发往远端），
//     调用方以 errors.Is 判别后降级；
//   - unary：一次调用为一操作，invoker 返回后自动 Record；
//   - stream：建流成功即放行，以流结束为一操作——RecvMsg 返回 io.EOF 记成功，
//     其余错误（含 SendMsg 因传输写失败返回的 io.EOF，grpc 契约允许）按分类记录；
//     业务未读完流即弃用则不记录（与两段式不 Record 同语义）；
//   - 请求级旁路：ctx 带 WithoutSuppression 标记的调用完全绕过抑制与反馈。
//
// 默认结局分类：ResourceExhausted / Unavailable（429/503 的等价码）与
// DeadlineExceeded（超时 ≈ 下游变慢）记为失败；Canceled（调用方取消）不计入；
// 其余错误码不计入（多与业务语义相关而非过载）。经 WithClassifier 整体替换。
//
// 边界：target→Governor 注册表持有在 Interceptor 实例内，非包级全局；Interceptor
// 可独立构造与丢弃，Governor 仍可脱离适配器单独构造与使用。同一 target 的多个
// ClientConn 共享一个 Governor（一个下游一个实例）。
//
// 用法：
//
//	ix, err := grpcx.New()
//	if err != nil { ... }
//	conn, err := grpc.NewClient(target,
//	    grpc.WithTransportCredentials(insecure.NewCredentials()),
//	    grpc.WithChainUnaryInterceptor(ix.Unary()),
//	    grpc.WithChainStreamInterceptor(ix.Stream()),
//	)
//	err = conn.Invoke(ctx, method, req, reply)
//	if errors.Is(err, governor.ErrSuppressed) {
//	    return fallback, nil // 过载被本地丢弃：在此降级
//	}
package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/jninng/governor"
	"github.com/jninng/observ"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Interceptor 持有按 target 惰性创建的 Governor 注册表：Unary 与 Stream 共用。
// 并发安全；多连接共享同一实例。
type Interceptor struct {
	mu         sync.RWMutex
	governors  map[string]*governor.Governor
	prefixes   map[string]string // 规整后的指标前缀 → target，创建时检测碰撞
	newSignals func() ([]governor.Signal, error)
	classify   func(err error) governor.Outcome
	prefix     func(target string) string
	meter      observ.Meter
	sensitiv   *float64
	probeRatio *float64
}

// New 创建适配器。任一 option 非法时报错（与 governor.New 一致，构造失败不产生
// 半成品）。
func New(opts ...Option) (*Interceptor, error) {
	ix := &Interceptor{
		governors:  make(map[string]*governor.Governor),
		prefixes:   make(map[string]string),
		newSignals: defaultSignals,
		classify:   defaultClassify,
		prefix:     defaultPrefix,
	}
	for _, opt := range opts {
		if err := opt(ix); err != nil {
			return nil, err
		}
	}
	return ix, nil
}

// Unary 返回 unary client interceptor。
func (ix *Interceptor) Unary() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if bypassed(ctx) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		g, err := ix.governorFor(cc.Target())
		if err != nil {
			return err
		}
		dec := g.Allow()
		if !dec.Allowed() {
			return governor.ErrSuppressed
		}
		invokeErr := invoker(ctx, method, req, reply, cc, opts...)
		dec.Record(ix.recordInput(invokeErr))
		return invokeErr
	}
}

// Stream 返回 stream client interceptor。以流结束为一操作：RecvMsg 返回 io.EOF
// 记成功，任一方向出错按分类记录；业务未读完流即弃用则不记录。
func (ix *Interceptor) Stream() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc,
		cc *grpc.ClientConn, method string, streamer grpc.Streamer,
		opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if bypassed(ctx) {
			return streamer(ctx, desc, cc, method, opts...)
		}
		g, err := ix.governorFor(cc.Target())
		if err != nil {
			return nil, err
		}
		dec := g.Allow()
		if !dec.Allowed() {
			return nil, governor.ErrSuppressed
		}
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			dec.Record(ix.recordInput(err))
			return nil, err
		}
		return &recordingStream{
			ClientStream: cs,
			finish: func(done error, fromRecv bool) {
				if fromRecv && errors.Is(done, io.EOF) {
					dec.Record(nil) // RecvMsg 的 io.EOF：流正常读完，记成功
					return
				}
				dec.Record(ix.recordInput(done))
			},
		}, nil
	}
}

// Governor 返回 target 对应的 Governor，不存在则创建（与拦截器路径共用同一注册
// 表），供业务查询只读状态（Pressure / Suppression / Signals，观测非控制）。
// 构造失败时返回错误且不缓存。
func (ix *Interceptor) Governor(target string) (*governor.Governor, error) {
	return ix.governorFor(target)
}

// WithoutSuppression 标记该 ctx 上的调用完全绕过抑制与反馈：
// 不做 Allow 判定（不会 ErrSuppressed）、不 Record（不进任何统计）。
func WithoutSuppression(ctx context.Context) context.Context {
	return context.WithValue(ctx, bypassKey{}, true)
}

type bypassKey struct{}

func bypassed(ctx context.Context) bool {
	v, _ := ctx.Value(bypassKey{}).(bool)
	return v
}

// ---- option ----

// Option 配置 Interceptor；全部作用于其惰性创建的每一个 Governor。
type Option func(*Interceptor) error

// WithSignalFactory 注入信号工厂，每创建一个 Governor 调用一次（信号不可跨实例
// 共享）。返回空切片表示该 target 无信号（压力恒 0，全部放行）。nil 报错；
// 不传时默认 RejectionSignal（K=2、窗口 90s），与显式拒绝类错误码口径配套。
func WithSignalFactory(newSignals func() ([]governor.Signal, error)) Option {
	return func(ix *Interceptor) error {
		if newSignals == nil {
			return fmt.Errorf("governor/grpcx: signal factory must not be nil")
		}
		ix.newSignals = newSignals
		return nil
	}
}

// WithClassifier 注入结局分类器，整体替换默认分类（ResourceExhausted / Unavailable /
// DeadlineExceeded 失败、其余不计入）。nil 报错。
func WithClassifier(classify func(err error) governor.Outcome) Option {
	return func(ix *Interceptor) error {
		if classify == nil {
			return fmt.Errorf("governor/grpcx: classifier must not be nil")
		}
		ix.classify = classify
		return nil
	}
}

// WithSensitivity 设置决策层敏感度 C（不传沿用 governor 默认 1.0），须 > 0。
func WithSensitivity(c float64) Option {
	return func(ix *Interceptor) error {
		if c <= 0 {
			return fmt.Errorf("governor/grpcx: sensitivity C must be positive, got %v", c)
		}
		v := c
		ix.sensitiv = &v
		return nil
	}
}

// WithProbeRatio 设置探测放行比例（不传沿用 governor 默认 0.02），须在 [0,1) 内。
func WithProbeRatio(r float64) Option {
	return func(ix *Interceptor) error {
		if r < 0 || r >= 1 {
			return fmt.Errorf("governor/grpcx: probe ratio must be in [0,1), got %v", r)
		}
		v := r
		ix.probeRatio = &v
		return nil
	}
}

// WithMetricPrefix 注入指标前缀函数。observ 无 label，前缀是唯一合规的实例级隔离
// 手段（ADR-0001）；nil 报错；不传时默认 "governor_<target>"（target 中除
// [a-z0-9_] 外的字符替换为下划线）。不同 target 规整后产生相同前缀时，后创建的
// target 报错，自定义前缀函数的输出同样参与检测。
func WithMetricPrefix(prefix func(target string) string) Option {
	return func(ix *Interceptor) error {
		if prefix == nil {
			return fmt.Errorf("governor/grpcx: metric prefix must not be nil")
		}
		ix.prefix = prefix
		return nil
	}
}

// WithMeter 注入指标 Meter（不传为 observ.NoopMeter），不得为 nil。
func WithMeter(m observ.Meter) Option {
	return func(ix *Interceptor) error {
		if m == nil {
			return fmt.Errorf("governor/grpcx: meter must not be nil")
		}
		ix.meter = m
		return nil
	}
}

// ---- 内部 ----

// 分类结果到 Record 输入的桥接：Record 只接收 error，成功传 nil、忽略与失败传
// 对应哨兵，由 coreClassifier 还原为 Outcome（哨兵不会离开本包，不影响调用方）。
var (
	errClassifiedFailure = errors.New("governor/grpcx: classified as failure")
	errClassifiedIgnore  = errors.New("governor/grpcx: classified as ignore")
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
func (ix *Interceptor) recordInput(err error) error {
	switch ix.classify(err) {
	case governor.OutcomeSuccess:
		return nil
	case governor.OutcomeIgnore:
		return errClassifiedIgnore
	default:
		return errClassifiedFailure
	}
}

// defaultClassify 是默认结局分类：ResourceExhausted / Unavailable（429/503 的
// 等价码）与 DeadlineExceeded（超时 ≈ 下游变慢）为失败（desc.md §3.2 场景 A），
// 其余错误码一律不计入（Canceled 是调用方取消；Unknown / Internal 等多与业务
// 语义相关而非过载，计入会污染压力估计）。
func defaultClassify(err error) governor.Outcome {
	if err == nil {
		return governor.OutcomeSuccess
	}
	switch status.Code(err) {
	case codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded:
		return governor.OutcomeFailure
	default:
		return governor.OutcomeIgnore
	}
}

func defaultSignals() ([]governor.Signal, error) {
	rej, err := governor.NewRejectionSignal()
	if err != nil {
		return nil, err
	}
	return []governor.Signal{rej}, nil
}

// defaultPrefix 默认指标前缀：governor_<target>。
func defaultPrefix(target string) string {
	return "governor_" + sanitize(target)
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

// governorFor 返回 target 对应的 Governor，惰性创建；构造失败返回错误且不缓存。
// 双重检查：读锁命中直返，未命中升级写锁后再查一次（并发首访同一 target 只构造
// 一次）。不同 target 规整后产生相同指标前缀时报错（如 "a.b:1" 与 "a_b:1"）。
func (ix *Interceptor) governorFor(target string) (*governor.Governor, error) {
	ix.mu.RLock()
	g, ok := ix.governors[target]
	ix.mu.RUnlock()
	if ok {
		return g, nil
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if g, ok := ix.governors[target]; ok {
		return g, nil
	}
	prefix := ix.prefix(target)
	if existing, ok := ix.prefixes[prefix]; ok {
		// 走到这里说明 target 尚未创建，前缀已被其他 target 占用即碰撞。
		return nil, fmt.Errorf(
			"governor/grpcx: metric prefix %q of target %q collides with target %q; targets must be unique after prefix sanitization (see WithMetricPrefix)",
			prefix, target, existing)
	}
	g, err := ix.build(target, prefix)
	if err != nil {
		return nil, err
	}
	ix.governors[target] = g
	ix.prefixes[prefix] = target
	return g, nil
}

func (ix *Interceptor) build(target, prefix string) (*governor.Governor, error) {
	sigs, err := ix.newSignals()
	if err != nil {
		return nil, fmt.Errorf("governor/grpcx: build signals for target %q: %w", target, err)
	}
	opts := []governor.Option{
		governor.WithSignals(sigs...),
		governor.WithMetricPrefix(prefix),
		governor.WithClassifier(coreClassifier),
	}
	if ix.meter != nil {
		opts = append(opts, governor.WithMeter(ix.meter))
	}
	if ix.sensitiv != nil {
		opts = append(opts, governor.WithSensitivity(*ix.sensitiv))
	}
	if ix.probeRatio != nil {
		opts = append(opts, governor.WithProbeRatio(*ix.probeRatio))
	}
	g, err := governor.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("governor/grpcx: build governor for target %q: %w", target, err)
	}
	return g, nil
}

// recordingStream 包装 ClientStream，把流结束作为一次操作记录（只记首个终态）：
// RecvMsg 的 io.EOF 是流正常读完，记成功；其余错误（含 SendMsg 因传输写失败
// 返回的 io.EOF，grpc 契约允许）按分类记录，默认不计入。
type recordingStream struct {
	grpc.ClientStream
	finish func(err error, fromRecv bool)
	once   sync.Once
}

func (s *recordingStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		s.once.Do(func() { s.finish(err, false) })
	}
	return err
}

func (s *recordingStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.once.Do(func() { s.finish(err, true) })
	}
	return err
}
