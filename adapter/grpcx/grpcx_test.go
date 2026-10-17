package grpcx

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/governor"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// countingSignal 记录 Observe 喂入的结局分布，Pressure 恒 0（只测分类，不触发抑制）。
type countingSignal struct {
	mu      sync.Mutex
	success int
	failure int
	ignore  int
}

func (s *countingSignal) Name() string { return "counting" }

func (s *countingSignal) Observe(_ time.Time, outcome governor.Outcome, _ time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch outcome {
	case governor.OutcomeSuccess:
		s.success++
	case governor.OutcomeFailure:
		s.failure++
	default:
		s.ignore++
	}
}

func (s *countingSignal) Pressure(time.Time) float64 { return 0 }

func (s *countingSignal) counts() (success, failure, ignore int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.success, s.failure, s.ignore
}

// tightRejectionSignals 返回低容忍的 RejectionSignal（K=1.1），
// 配合 C=0.5 使持续失败的 target 快速进入稳定抑制；全成功时压力恒 0。
func tightRejectionSignals() ([]governor.Signal, error) {
	rej, err := governor.NewRejectionSignal(governor.WithRejectionK(1.1))
	if err != nil {
		return nil, err
	}
	return []governor.Signal{rej}, nil
}

// ---- 测试基建：惰性 ClientConn 与 stub invoker / streamer ----

func newConn(t *testing.T, target string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient(%q): %v", target, err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

type countingInvoker struct {
	calls atomic.Int64
	err   error
}

func (c *countingInvoker) invoke(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, opts ...grpc.CallOption) error {
	c.calls.Add(1)
	return c.err
}

type fakeStream struct {
	grpc.ClientStream
	sendErr error
	recvErr error
}

func (f *fakeStream) SendMsg(any) error { return f.sendErr }
func (f *fakeStream) RecvMsg(any) error { return f.recvErr }

type countingStreamer struct {
	calls atomic.Int64
	cs    grpc.ClientStream
	err   error
}

func (c *countingStreamer) stream(ctx context.Context, desc *grpc.StreamDesc,
	cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	c.calls.Add(1)
	return c.cs, c.err
}

func fixedSignals(sig governor.Signal) func() ([]governor.Signal, error) {
	return func() ([]governor.Signal, error) { return []governor.Signal{sig}, nil }
}

// ---- 构造与校验 ----

func TestNewOptionValidation(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{"nil signal factory", WithSignalFactory(nil)},
		{"nil classifier", WithClassifier(nil)},
		{"nil prefix", WithMetricPrefix(nil)},
		{"nil meter", WithMeter(nil)},
		{"zero sensitivity", WithSensitivity(0)},
		{"negative sensitivity", WithSensitivity(-1)},
		{"negative probe ratio", WithProbeRatio(-0.1)},
		{"probe ratio out of range", WithProbeRatio(1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.opt); err == nil {
				t.Fatalf("New 应报错")
			}
		})
	}
	if _, err := New(); err != nil {
		t.Fatalf("默认构造不应报错: %v", err)
	}
}

// ---- unary ----

func TestUnaryRecordsOutcome(t *testing.T) {
	sig := &countingSignal{}
	ix, err := New(WithSignalFactory(fixedSignals(sig)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	unary := ix.Unary()
	cc := newConn(t, "a.example:1")

	invoke := func(err error) error {
		inv := &countingInvoker{err: err}
		callErr := unary(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{},
			cc, inv.invoke)
		if got := inv.calls.Load(); got != 1 {
			t.Fatalf("invoker 调用次数 = %d, want 1", got)
		}
		return callErr
	}

	if err := invoke(nil); err != nil {
		t.Fatalf("成功调用不应报错: %v", err)
	}
	if err := invoke(status.Error(codes.ResourceExhausted, "overloaded")); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("invoker 错误应原样透出，got %v", err)
	}
	if err := invoke(status.Error(codes.Unavailable, "down")); status.Code(err) != codes.Unavailable {
		t.Fatalf("invoker 错误应原样透出，got %v", err)
	}
	if err := invoke(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消应原样透出，got %v", err)
	}

	success, failure, ignore := sig.counts()
	if success != 1 {
		t.Fatalf("success = %d, want 1", success)
	}
	if failure != 2 { // ResourceExhausted 与 Unavailable：显式拒绝类按失败
		t.Fatalf("failure = %d, want 2", failure)
	}
	if ignore != 1 { // Canceled：调用方取消不计入
		t.Fatalf("ignore = %d, want 1", ignore)
	}
}

func TestUnarySuppressesOnPersistentFailure(t *testing.T) {
	ix, err := New(WithSignalFactory(tightRejectionSignals), WithSensitivity(0.5))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	unary := ix.Unary()
	cc := newConn(t, "a.example:1")
	inv := &countingInvoker{err: status.Error(codes.ResourceExhausted, "overloaded")}

	suppressed, passthrough := 0, 0
	for i := 0; i < 200; i++ {
		err := unary(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{},
			cc, inv.invoke)
		switch {
		case errors.Is(err, governor.ErrSuppressed):
			suppressed++
		case status.Code(err) == codes.ResourceExhausted:
			passthrough++
		default:
			t.Fatalf("意外错误: %v", err)
		}
	}
	if suppressed < 10 || passthrough < 10 {
		t.Fatalf("持续 ResourceExhausted 下抑制与探测放行应并存，suppressed=%d passthrough=%d",
			suppressed, passthrough)
	}
}

func TestUnaryBypass(t *testing.T) {
	ix, err := New(WithSignalFactory(tightRejectionSignals), WithSensitivity(0.5))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	unary := ix.Unary()
	cc := newConn(t, "a.example:1")
	inv := &countingInvoker{err: status.Error(codes.ResourceExhausted, "overloaded")}

	ctx := WithoutSuppression(context.Background())
	for i := 0; i < 30; i++ {
		err := unary(ctx, "/pkg.Svc/Method", struct{}{}, struct{}{}, cc, inv.invoke)
		if errors.Is(err, governor.ErrSuppressed) {
			t.Fatalf("旁路调用不应被抑制")
		}
	}
	// 旁路不 Record，压力不抬升：全部调用都应到达 invoker。
	if got := inv.calls.Load(); got != 30 {
		t.Fatalf("旁路调用应全部到达 invoker，实收 %d/30", got)
	}
}

// ---- 多 target 隔离 ----

func TestPerTargetIsolation(t *testing.T) {
	ix, err := New(WithSignalFactory(tightRejectionSignals), WithSensitivity(0.5))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ccA1 := newConn(t, "a.example:1")
	ccA2 := newConn(t, "a.example:1")
	ccB := newConn(t, "b.example:1")

	// 同一 target 的两个连接共享一个 Governor；不同 target 各自独立。
	gA1, err := ix.Governor(ccA1.Target())
	if err != nil {
		t.Fatalf("Governor: %v", err)
	}
	gA2, err := ix.Governor(ccA2.Target())
	if err != nil {
		t.Fatalf("Governor: %v", err)
	}
	gB, err := ix.Governor(ccB.Target())
	if err != nil {
		t.Fatalf("Governor: %v", err)
	}
	if gA1 != gA2 {
		t.Fatalf("同一 target 应复用同一 Governor")
	}
	if gA1 == gB {
		t.Fatalf("不同 target 不应共享 Governor")
	}

	// 打满 a 后，b 的调用不受影响。
	unary := ix.Unary()
	invA := &countingInvoker{err: status.Error(codes.ResourceExhausted, "overloaded")}
	for i := 0; i < 100; i++ {
		_ = unary(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{}, ccA1, invA.invoke)
	}
	invB := &countingInvoker{err: nil}
	for i := 0; i < 10; i++ {
		if err := unary(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{}, ccB, invB.invoke); err != nil {
			t.Fatalf("b target 不应受 a target 影响: %v", err)
		}
	}
}

// ---- stream ----

func TestStreamRecordsOnEOFAndError(t *testing.T) {
	sig := &countingSignal{}
	ix, err := New(WithSignalFactory(fixedSignals(sig)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	streamIx := ix.Stream()
	cc := newConn(t, "a.example:1")

	open := func(cs grpc.ClientStream, err error) grpc.ClientStream {
		got, serr := streamIx(context.Background(), &grpc.StreamDesc{ServerStreams: true},
			cc, "/pkg.Svc/Method", (&countingStreamer{cs: cs, err: err}).stream)
		if serr != nil {
			t.Fatalf("建流不应失败: %v", serr)
		}
		return got
	}

	// RecvMsg 返回 io.EOF：流正常读完，记成功。
	s := open(&fakeStream{recvErr: io.EOF}, nil)
	if err := s.RecvMsg(struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("RecvMsg 应透出 io.EOF，got %v", err)
	}
	// SendMsg 出错：按分类记录。
	s = open(&fakeStream{sendErr: status.Error(codes.Unavailable, "broken")}, nil)
	if err := s.SendMsg(struct{}{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("SendMsg 应透出错误，got %v", err)
	}
	// RecvMsg 出错：按分类记录。
	s = open(&fakeStream{recvErr: status.Error(codes.ResourceExhausted, "overloaded")}, nil)
	if err := s.RecvMsg(struct{}{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("RecvMsg 应透出错误，got %v", err)
	}
	// SendMsg 返回 io.EOF（传输写失败时 grpc 契约允许）：不是正常读完，按分类
	// 记录——默认 Unknown 不计入。
	s = open(&fakeStream{sendErr: io.EOF}, nil)
	if err := s.SendMsg(struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("SendMsg 应透出 io.EOF，got %v", err)
	}

	success, failure, ignore := sig.counts()
	if success != 1 {
		t.Fatalf("success = %d, want 1", success)
	}
	if failure != 2 {
		t.Fatalf("failure = %d, want 2", failure)
	}
	if ignore != 1 { // SendMsg 的 io.EOF 不计为成功
		t.Fatalf("ignore = %d, want 1", ignore)
	}
}

func TestStreamStreamerError(t *testing.T) {
	sig := &countingSignal{}
	ix, err := New(WithSignalFactory(fixedSignals(sig)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	streamIx := ix.Stream()
	cc := newConn(t, "a.example:1")

	buildErr := status.Error(codes.Unavailable, "connect failed")
	_, got := streamIx(context.Background(), &grpc.StreamDesc{ServerStreams: true},
		cc, "/pkg.Svc/Method", (&countingStreamer{err: buildErr}).stream)
	if status.Code(got) != codes.Unavailable {
		t.Fatalf("建流错误应原样透出，got %v", got)
	}
	if _, failure, _ := sig.counts(); failure != 1 {
		t.Fatalf("建流失败应记失败，failure=%d", failure)
	}
}

func TestStreamSuppresses(t *testing.T) {
	ix, err := New(WithSignalFactory(tightRejectionSignals), WithSensitivity(0.5))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	streamIx := ix.Stream()
	cc := newConn(t, "a.example:1")
	st := &countingStreamer{cs: &fakeStream{recvErr: status.Error(codes.ResourceExhausted, "overloaded")}}

	suppressed := 0
	for i := 0; i < 100; i++ {
		cs, err := streamIx(context.Background(), &grpc.StreamDesc{ServerStreams: true},
			cc, "/pkg.Svc/Method", st.stream)
		if errors.Is(err, governor.ErrSuppressed) {
			suppressed++
			continue
		}
		if err != nil {
			t.Fatalf("建流意外失败: %v", err)
		}
		_ = cs.RecvMsg(struct{}{}) // 消费到终态才触发记录
	}
	if suppressed < 10 {
		t.Fatalf("持续失败下流应进入抑制，suppressed=%d", suppressed)
	}
	// 被抑制的流不应触达 streamer。
	if got, want := st.calls.Load(), int64(100-suppressed); got != want {
		t.Fatalf("streamer 调用次数 = %d, want %d", got, want)
	}
}

func TestStreamBypass(t *testing.T) {
	ix, err := New(WithSignalFactory(tightRejectionSignals), WithSensitivity(0.5))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	streamIx := ix.Stream()
	cc := newConn(t, "a.example:1")
	st := &countingStreamer{cs: &fakeStream{recvErr: status.Error(codes.ResourceExhausted, "overloaded")}}

	ctx := WithoutSuppression(context.Background())
	for i := 0; i < 30; i++ {
		if _, err := streamIx(ctx, &grpc.StreamDesc{ServerStreams: true},
			cc, "/pkg.Svc/Method", st.stream); errors.Is(err, governor.ErrSuppressed) {
			t.Fatalf("旁路建流不应被抑制")
		}
	}
	if got := st.calls.Load(); got != 30 {
		t.Fatalf("旁路建流应全部触达 streamer，实收 %d/30", got)
	}
}

// ---- 构造失败与前缀 ----

func TestSignalFactoryErrorFailsRPC(t *testing.T) {
	ix, err := New(WithSignalFactory(func() ([]governor.Signal, error) {
		return nil, errors.New("boom")
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc := newConn(t, "a.example:1")
	inv := &countingInvoker{}

	err = ix.Unary()(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{}, cc, inv.invoke)
	if err == nil {
		t.Fatalf("构造失败的 Governor 应使 RPC 失败")
	}
	if errors.Is(err, governor.ErrSuppressed) {
		t.Fatalf("构造失败不是抑制，不应返回 ErrSuppressed")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("错误应携带构造失败原因，got %v", err)
	}
	if got := inv.calls.Load(); got != 0 {
		t.Fatalf("invoker 不应被调用，got %d", got)
	}
}

func TestDefaultPrefixSanitize(t *testing.T) {
	tests := []struct{ target, want string }{
		{"dns:///api.example.com:9000", "governor_dns____api_example_com_9000"},
		{"passthrough:///localhost:80", "governor_passthrough____localhost_80"},
		{"API.Example.COM", "governor_api_example_com"},
		{"", "governor_"},
	}
	for _, tt := range tests {
		if got := defaultPrefix(tt.target); got != tt.want {
			t.Fatalf("defaultPrefix(%q) = %q, want %q", tt.target, got, tt.want)
		}
	}
}

// ---- 指标前缀与碰撞检测 ----

// recordingMeter 包装真实 Meter 并记录创建的仪表名，验证前缀生成
// （otel 同步仪表接口密封，无法自造假仪表，故委托真实 provider）。
type recordingMeter struct {
	metric.Meter
	mu    sync.Mutex
	names []string
}

func newRecordingMeter(t *testing.T) *recordingMeter {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return &recordingMeter{Meter: provider.Meter("grpcx_test")}
}

func (m *recordingMeter) record(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.names = append(m.names, name)
}

func (m *recordingMeter) created() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.names...)
}

func (m *recordingMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	m.record(name)
	return m.Meter.Int64Counter(name, opts...)
}

func (m *recordingMeter) Int64Gauge(name string, opts ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	m.record(name)
	return m.Meter.Int64Gauge(name, opts...)
}

func (m *recordingMeter) Float64Gauge(name string, opts ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	m.record(name)
	return m.Meter.Float64Gauge(name, opts...)
}

func (m *recordingMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	m.record(name)
	return m.Meter.Float64Histogram(name, opts...)
}

func TestMetricPrefixPerTarget(t *testing.T) {
	m := newRecordingMeter(t)
	ix, err := New(WithMeter(m))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc := newConn(t, "a.example:1")
	inv := &countingInvoker{}
	unary := ix.Unary()
	if err := unary(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{},
		cc, inv.invoke); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	prefix := "governor_a_example_1_"
	for _, name := range m.created() {
		if len(name) < len(prefix) || name[:len(prefix)] != prefix {
			t.Fatalf("指标名 %q 未使用按 target 生成的前缀 %q", name, prefix)
		}
	}
	found := false
	for _, name := range m.created() {
		if name == prefix+"pressure" {
			found = true
		}
	}
	if !found {
		t.Fatalf("未找到 %spressure，got %v", prefix, m.created())
	}
}

func TestPrefixCollisionBetweenTargets(t *testing.T) {
	ix, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := ix.Governor("a.b:1"); err != nil {
		t.Fatalf("Governor(a.b:1): %v", err)
	}
	// "a.b:1" 与 "a_b:1" 规整后同为 governor_a_b_1。
	if _, err := ix.Governor("a_b:1"); err == nil {
		t.Fatalf("前缀碰撞应报错")
	}
	// 已创建的 target 不受影响。
	if _, err := ix.Governor("a.b:1"); err != nil {
		t.Fatalf("已有 target 应正常复用: %v", err)
	}
}

// ---- ConcurrencySignal 自动接线 ----

// waitUntil 轮询等待条件成立，超时 Fatal（用于在途计数的跨协程同步）。
func waitUntil(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

type gatedInvoker struct {
	release chan struct{}
}

func (g *gatedInvoker) invoke(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, opts ...grpc.CallOption) error {
	<-g.release
	return nil
}

func TestUnaryWiresConcurrencySignal(t *testing.T) {
	conc, err := governor.NewConcurrencySignal(governor.WithConcurrencyTarget(10))
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	ix, err := New(WithSignalFactory(fixedSignals(conc)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc := newConn(t, "a.example:1")
	inv := &gatedInvoker{release: make(chan struct{})}

	const n = 5
	for i := 0; i < n; i++ {
		go func() {
			_ = ix.Unary()(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{}, cc, inv.invoke)
		}()
	}
	waitUntil(t, 5*time.Second, func() bool { return conc.InFlight() == n },
		"在途调用应把计数抬升到 5")
	close(inv.release)
	waitUntil(t, 5*time.Second, func() bool { return conc.InFlight() == 0 },
		"调用完成后在途计数应归零")
}

type gatedStream struct {
	grpc.ClientStream
	release chan struct{}
}

func (g *gatedStream) RecvMsg(any) error {
	<-g.release
	return io.EOF
}

func TestStreamWiresConcurrencySignal(t *testing.T) {
	conc, err := governor.NewConcurrencySignal(governor.WithConcurrencyTarget(10))
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	ix, err := New(WithSignalFactory(fixedSignals(conc)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc := newConn(t, "a.example:1")
	st := &countingStreamer{cs: &gatedStream{release: make(chan struct{})}}

	cs, err := ix.Stream()(context.Background(), &grpc.StreamDesc{ServerStreams: true},
		cc, "/pkg.Svc/Method", st.stream)
	if err != nil {
		t.Fatalf("建流: %v", err)
	}
	waitUntil(t, 5*time.Second, func() bool { return conc.InFlight() == 1 },
		"未结束的流应计为在途")
	go func() { _ = cs.RecvMsg(struct{}{}) }()
	close(st.cs.(*gatedStream).release)
	waitUntil(t, 5*time.Second, func() bool { return conc.InFlight() == 0 },
		"流结束（io.EOF）后在途计数应归零")
}

// ---- OnTransition 透传 ----

type transitionEntry struct {
	key string
	tr  governor.Transition
}

type transitionCollector struct {
	mu      sync.Mutex
	entries []transitionEntry
}

func (c *transitionCollector) record(target string, tr governor.Transition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, transitionEntry{key: target, tr: tr})
}

func (c *transitionCollector) snapshot() []transitionEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]transitionEntry(nil), c.entries...)
}

func TestUnaryOnTransition(t *testing.T) {
	c := &transitionCollector{}
	ix, err := New(
		WithSignalFactory(tightRejectionSignals),
		WithSensitivity(0.5),
		WithOnTransition(c.record),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc := newConn(t, "a.example:1")
	inv := &countingInvoker{err: status.Error(codes.ResourceExhausted, "overloaded")}
	unary := ix.Unary()
	for i := 0; i < 100; i++ {
		_ = unary(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{}, cc, inv.invoke)
	}
	entries := c.snapshot()
	if len(entries) == 0 {
		t.Fatalf("持续失败应产生跃迁事件")
	}
	for _, e := range entries {
		if e.key != cc.Target() {
			t.Fatalf("回调 key = %q, want %q", e.key, cc.Target())
		}
		if e.tr.Kind != governor.TransitionSuppressionStarted {
			t.Fatalf("kind = %v, want suppression_started", e.tr.Kind)
		}
	}
	if _, err := New(WithOnTransition(nil)); err == nil {
		t.Fatalf("nil 回调应报错")
	}
}

// ---- DirectiveSignal 自动接线（RetryInfo 解析） ----

func TestUnaryFeedsRetryInfo(t *testing.T) {
	dir, err := governor.NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	ix, err := New(WithSignalFactory(fixedSignals(dir)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc := newConn(t, "a.example:1")

	// 携带 RetryInfo detail 的 ResourceExhausted：解析出 30s 指令。
	st, err := status.New(codes.ResourceExhausted, "rate limited").WithDetails(
		&errdetails.RetryInfo{RetryDelay: durationpb.New(30 * time.Second)})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	inv := &countingInvoker{err: st.Err()}
	got := ix.Unary()(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{},
		cc, inv.invoke)
	if status.Code(got) != codes.ResourceExhausted {
		t.Fatalf("错误应原样透出，got %v", got)
	}
	if p := dir.Pressure(time.Now()); p <= 29 || p > 30 {
		t.Fatalf("RetryInfo 30s 应喂入压力 (29,30]，got %v", p)
	}

	// 无 detail 的错误：不喂（全新信号保持 0）。
	dir2, err := governor.NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	ix2, err := New(WithSignalFactory(fixedSignals(dir2)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc2 := newConn(t, "b.example:1")
	plain := &countingInvoker{err: status.Error(codes.ResourceExhausted, "no details")}
	if err := ix2.Unary()(context.Background(), "/pkg.Svc/Method", struct{}{}, struct{}{},
		cc2, plain.invoke); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("错误应原样透出，got %v", err)
	}
	if p := dir2.Pressure(time.Now()); p != 0 {
		t.Fatalf("无 RetryInfo 不应喂入，got %v", p)
	}
}

func TestStreamFeedsRetryInfo(t *testing.T) {
	dir, err := governor.NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	ix, err := New(WithSignalFactory(fixedSignals(dir)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cc := newConn(t, "a.example:1")

	st, err := status.New(codes.Unavailable, "overloaded").WithDetails(
		&errdetails.RetryInfo{RetryDelay: durationpb.New(2 * time.Second)})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	_, got := ix.Stream()(context.Background(), &grpc.StreamDesc{ServerStreams: true},
		cc, "/pkg.Svc/Method", (&countingStreamer{err: st.Err()}).stream)
	if status.Code(got) != codes.Unavailable {
		t.Fatalf("建流错误应原样透出，got %v", got)
	}
	if p := dir.Pressure(time.Now()); p <= 1 || p > 2 {
		t.Fatalf("建流失败的 RetryInfo 2s 应喂入压力 (1,2]，got %v", p)
	}
}
