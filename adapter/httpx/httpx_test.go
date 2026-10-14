package httpx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/governor"
	"github.com/jninng/observ"
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

// tightRejectionSignals 返回低容忍的 RejectionSignal（K=1.1，失败率超约 91% 即升压），
// 配合 C=0.5 使持续失败的服务器快速进入稳定抑制；全成功时压力恒 0，健康 host 不受影响。
func tightRejectionSignals() ([]governor.Signal, error) {
	rej, err := governor.NewRejectionSignal(governor.WithRejectionK(1.1))
	if err != nil {
		return nil, err
	}
	return []governor.Signal{rej}, nil
}

func statusServer(code int, hits *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		w.WriteHeader(code)
	}))
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u.Host
}

// ---- 构造与校验 ----

func TestNewOptionValidation(t *testing.T) {
	base := http.DefaultTransport
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
			if _, err := New(base, tt.opt); err == nil {
				t.Fatalf("New 应报错")
			}
		})
	}
	if _, err := New(nil); err != nil {
		t.Fatalf("nil base 应回退 DefaultTransport，got %v", err)
	}
}

// ---- 透传与抑制 ----

func TestHealthyPassthrough(t *testing.T) {
	srv := statusServer(http.StatusOK, nil)
	defer srv.Close()
	tr, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}
	for i := 0; i < 100; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("第%d次请求不应失败: %v", i+1, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("第%d次请求状态 = %d", i+1, resp.StatusCode)
		}
	}
	g, err := tr.Governor(hostOf(srv.URL))
	if err != nil {
		t.Fatalf("Governor: %v", err)
	}
	if a := g.Suppression(); a != 0 {
		t.Fatalf("健康期 α = %v, want 0", a)
	}
}

func TestSuppressesOnPersistentFailure(t *testing.T) {
	var hits atomic.Int64
	srv := statusServer(http.StatusTooManyRequests, &hits)
	defer srv.Close()
	tr, err := New(nil,
		WithSignalFactory(tightRejectionSignals),
		WithSensitivity(0.5),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	suppressed, ok := 0, 0
	for i := 0; i < 100; i++ {
		resp, err := client.Get(srv.URL)
		switch {
		case errors.Is(err, governor.ErrSuppressed):
			suppressed++
		case err != nil:
			t.Fatalf("第%d次请求意外错误: %v", i+1, err)
		default:
			resp.Body.Close()
			if resp.StatusCode != http.StatusTooManyRequests {
				t.Fatalf("放行请求状态 = %d, want 429", resp.StatusCode)
			}
			ok++
		}
	}
	if suppressed < 10 || ok < 10 {
		t.Fatalf("持续 429 下抑制与放行应并存，suppressed=%d ok=%d", suppressed, ok)
	}
	// 被抑制的请求不会发往远端：服务器实收次数必须等于放行次数。
	if got, want := hits.Load(), int64(ok); got != want {
		t.Fatalf("服务器实收 %d 次, want %d（被抑制的请求不应到达远端）", got, want)
	}
}

func TestRequestLevelBypass(t *testing.T) {
	var hits atomic.Int64
	srv := statusServer(http.StatusTooManyRequests, &hits)
	defer srv.Close()
	tr, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	for i := 0; i < 50; i++ {
		req, err := http.NewRequestWithContext(
			WithoutSuppression(context.Background()), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("旁路请求不应被抑制: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("旁路请求状态 = %d", resp.StatusCode)
		}
	}
	if got := hits.Load(); got != 50 {
		t.Fatalf("旁路请求应全部到达远端，实收 %d/50", got)
	}
}

// ---- 结局分类 ----

func TestClassification(t *testing.T) {
	sig := &countingSignal{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusOK)
		case "/notfound":
			w.WriteHeader(http.StatusNotFound)
		case "/rate":
			w.WriteHeader(http.StatusTooManyRequests)
		case "/boom":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	tr, err := New(nil, WithSignalFactory(func() ([]governor.Signal, error) {
		return []governor.Signal{sig}, nil
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	for _, path := range []string{"/ok", "/notfound", "/rate", "/boom"} {
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		resp.Body.Close()
	}

	// 调用方取消：不计入统计。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/ok", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if _, err := client.Do(req); err == nil {
		t.Fatalf("已取消的请求应报错")
	}

	success, failure, ignore := sig.counts()
	if success != 2 { // 200 与 404：非过载类响应按成功
		t.Fatalf("success = %d, want 2", success)
	}
	if failure != 2 { // 429 与 503：显式拒绝与服务器错误按失败
		t.Fatalf("failure = %d, want 2", failure)
	}
	if ignore != 1 { // 调用方取消
		t.Fatalf("ignore = %d, want 1", ignore)
	}
}

// ---- 多 host 隔离 ----

func TestPerHostIsolation(t *testing.T) {
	good := statusServer(http.StatusOK, nil)
	defer good.Close()
	bad := statusServer(http.StatusTooManyRequests, nil)
	defer bad.Close()
	tr, err := New(nil,
		WithSignalFactory(tightRejectionSignals),
		WithSensitivity(0.5),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	suppressed := 0
	for i := 0; i < 50; i++ {
		resp, err := client.Get(bad.URL)
		if errors.Is(err, governor.ErrSuppressed) {
			suppressed++
			continue
		}
		if err != nil {
			t.Fatalf("bad host: %v", err)
		}
		resp.Body.Close()
	}
	if suppressed < 5 {
		t.Fatalf("bad host 应进入抑制，suppressed=%d", suppressed)
	}

	for i := 0; i < 30; i++ {
		resp, err := client.Get(good.URL)
		if err != nil {
			t.Fatalf("good host 不应受 bad host 影响: %v", err)
		}
		resp.Body.Close()
	}
	gg, err := tr.Governor(hostOf(good.URL))
	if err != nil {
		t.Fatalf("Governor(good): %v", err)
	}
	if a := gg.Suppression(); a != 0 {
		t.Fatalf("good host α = %v, want 0（bad host 的压力不应泄漏）", a)
	}

	gb, err := tr.Governor(hostOf(bad.URL))
	if err != nil {
		t.Fatalf("Governor(bad): %v", err)
	}
	if gb == gg {
		t.Fatalf("不同 host 应各自持有 Governor")
	}
}

// ---- 注册表与构造失败 ----

func TestGovernorAccessorReuse(t *testing.T) {
	tr, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g1, err := tr.Governor("a.example:1")
	if err != nil {
		t.Fatalf("Governor: %v", err)
	}
	g2, err := tr.Governor("a.example:1")
	if err != nil {
		t.Fatalf("Governor: %v", err)
	}
	g3, err := tr.Governor("b.example:1")
	if err != nil {
		t.Fatalf("Governor: %v", err)
	}
	if g1 != g2 {
		t.Fatalf("同一 host 应复用同一 Governor")
	}
	if g1 == g3 {
		t.Fatalf("不同 host 不应共享 Governor")
	}
}

func TestSignalFactoryErrorFailsRequest(t *testing.T) {
	srv := statusServer(http.StatusOK, nil)
	defer srv.Close()
	tr, err := New(nil, WithSignalFactory(func() ([]governor.Signal, error) {
		return nil, errors.New("boom")
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	_, err = client.Get(srv.URL)
	if err == nil {
		t.Fatalf("构造失败的 Governor 应使请求失败")
	}
	if errors.Is(err, governor.ErrSuppressed) {
		t.Fatalf("构造失败不是抑制，不应返回 ErrSuppressed")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("错误应携带构造失败原因，got %v", err)
	}
	if _, gerr := tr.Governor(hostOf(srv.URL)); gerr == nil {
		t.Fatalf("Governor 访问器应透出同一构造错误")
	}
}

// ---- 前缀规整 ----

func TestDefaultPrefixSanitize(t *testing.T) {
	tests := []struct{ host, want string }{
		{"user.example.com:8080", "governor_user_example_com_8080"},
		{"API.Example.COM", "governor_api_example_com"},
		{"localhost", "governor_localhost"},
		{"", "governor_"},
		{"a__b..c", "governor_a__b__c"},
	}
	for _, tt := range tests {
		if got := defaultPrefix(tt.host); got != tt.want {
			t.Fatalf("defaultPrefix(%q) = %q, want %q", tt.host, got, tt.want)
		}
	}
}

// ---- 并发 ----

func TestConcurrentRequests(t *testing.T) {
	srv := statusServer(http.StatusOK, nil)
	defer srv.Close()
	tr, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	const workers, per = 25, 20
	var wg sync.WaitGroup
	errs := make(chan error, workers*per)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				resp, err := client.Get(srv.URL)
				if err != nil {
					errs <- fmt.Errorf("请求失败: %w", err)
					continue
				}
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// ---- 指标前缀与碰撞检测 ----

type recordingMeter struct {
	mu    sync.Mutex
	names []string
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

func (m *recordingMeter) NewCounter(name, _ string) observ.Counter {
	m.record(name)
	return nopCounter{}
}
func (m *recordingMeter) NewGauge(name, _ string) observ.Gauge { m.record(name); return nopGauge{} }
func (m *recordingMeter) NewHistogram(name, _ string, _ []float64) observ.Histogram {
	m.record(name)
	return nopHistogram{}
}

type nopCounter struct{}

func (nopCounter) Inc()        {}
func (nopCounter) Add(float64) {}

type nopGauge struct{}

func (nopGauge) Set(float64) {}
func (nopGauge) Add(float64) {}

type nopHistogram struct{}

func (nopHistogram) Observe(float64) {}

func TestMetricPrefixPerHost(t *testing.T) {
	srv := statusServer(http.StatusOK, nil)
	defer srv.Close()
	m := &recordingMeter{}
	tr, err := New(nil, WithMeter(m))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	prefix := "governor_" + sanitize(hostOf(srv.URL)) + "_"
	for _, name := range m.created() {
		if len(name) < len(prefix) || name[:len(prefix)] != prefix {
			t.Fatalf("指标名 %q 未使用按 host 生成的前缀 %q", name, prefix)
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

func TestPrefixCollisionBetweenHosts(t *testing.T) {
	tr, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := tr.Governor("a.b:80"); err != nil {
		t.Fatalf("Governor(a.b:80): %v", err)
	}
	// "a.b:80" 与 "a_b:80" 规整后同为 governor_a_b_80。
	if _, err := tr.Governor("a_b:80"); err == nil {
		t.Fatalf("前缀碰撞应报错")
	}
	// 已创建的 host 不受影响。
	if _, err := tr.Governor("a.b:80"); err != nil {
		t.Fatalf("已有 host 应正常复用: %v", err)
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

func TestRoundTripperWiresConcurrencySignal(t *testing.T) {
	conc, err := governor.NewConcurrencySignal(governor.WithConcurrencyTarget(10))
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	tr, err := New(nil, WithSignalFactory(func() ([]governor.Signal, error) {
		return []governor.Signal{conc}, nil
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	const n = 5
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(srv.URL)
			if err != nil {
				t.Errorf("请求失败: %v", err)
				return
			}
			resp.Body.Close()
		}()
	}
	waitUntil(t, 5*time.Second, func() bool { return conc.InFlight() == n },
		"在途请求应把计数抬升到 5")
	close(release)
	wg.Wait()
	waitUntil(t, 5*time.Second, func() bool { return conc.InFlight() == 0 },
		"请求完成后在途计数应归零")
}

func TestRoundTripperBypassNotCounted(t *testing.T) {
	conc, err := governor.NewConcurrencySignal(governor.WithConcurrencyTarget(10))
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	tr, err := New(nil, WithSignalFactory(func() ([]governor.Signal, error) {
		return []governor.Signal{conc}, nil
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}

	for i := 0; i < 3; i++ {
		req, err := http.NewRequestWithContext(
			WithoutSuppression(context.Background()), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		go func() {
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	time.Sleep(50 * time.Millisecond) // 等旁路请求到达阻塞点
	if got := conc.InFlight(); got != 0 {
		t.Fatalf("旁路请求不应入在途计数，got %d", got)
	}
	close(release)
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

func (c *transitionCollector) record(host string, tr governor.Transition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, transitionEntry{key: host, tr: tr})
}

func (c *transitionCollector) snapshot() []transitionEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]transitionEntry(nil), c.entries...)
}

func TestRoundTripperOnTransition(t *testing.T) {
	c := &transitionCollector{}
	var hits atomic.Int64
	srv := statusServer(http.StatusTooManyRequests, &hits)
	defer srv.Close()
	tr, err := New(nil,
		WithSignalFactory(tightRejectionSignals),
		WithSensitivity(0.5),
		WithOnTransition(c.record),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client := &http.Client{Transport: tr}
	for i := 0; i < 100; i++ {
		resp, err := client.Get(srv.URL)
		if errors.Is(err, governor.ErrSuppressed) {
			continue
		}
		if err != nil {
			t.Fatalf("第%d次请求: %v", i+1, err)
		}
		resp.Body.Close()
	}
	entries := c.snapshot()
	if len(entries) == 0 {
		t.Fatalf("持续失败应产生跃迁事件")
	}
	for _, e := range entries {
		if e.key != hostOf(srv.URL) {
			t.Fatalf("回调 key = %q, want %q", e.key, hostOf(srv.URL))
		}
		if e.tr.Kind != governor.TransitionSuppressionStarted {
			t.Fatalf("kind = %v, want suppression_started", e.tr.Kind)
		}
	}
}

func TestOnTransitionNilRejected(t *testing.T) {
	if _, err := New(nil, WithOnTransition(nil)); err == nil {
		t.Fatalf("nil 回调应报错")
	}
}

// ---- DirectiveSignal 自动接线（Retry-After 解析） ----

func TestRoundTripperFeedsRetryAfter(t *testing.T) {
	var header atomic.Value // string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if v, _ := header.Load().(string); v != "" {
			w.Header().Set("Retry-After", v)
		}
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	newClient := func(t *testing.T) (*governor.DirectiveSignal, *http.Client) {
		t.Helper()
		dir, err := governor.NewDirectiveSignal(governor.WithDirectiveScale(time.Second))
		if err != nil {
			t.Fatalf("NewDirectiveSignal: %v", err)
		}
		tr, err := New(nil, WithSignalFactory(func() ([]governor.Signal, error) {
			return []governor.Signal{dir}, nil
		}))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return dir, &http.Client{Transport: tr}
	}

	t.Run("延迟秒数", func(t *testing.T) {
		dir, client := newClient(t)
		header.Store("5")
		resp, err := client.Get(srv.URL)
		if err != nil || resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("请求应正常返回 429: %v", err)
		}
		resp.Body.Close()
		if p := dir.Pressure(time.Now()); p <= 4 || p > 5 {
			t.Fatalf("Retry-After: 5 应喂入压力 (4,5]，got %v", p)
		}
	})

	t.Run("HTTP日期", func(t *testing.T) {
		dir, client := newClient(t)
		header.Store(time.Now().Add(3 * time.Second).UTC().Format(http.TimeFormat))
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		resp.Body.Close()
		if p := dir.Pressure(time.Now()); p <= 0 || p > 3 {
			t.Fatalf("HTTP 日期应换算为剩余时长 (0,3]，got %v", p)
		}
	})

	t.Run("无头不喂", func(t *testing.T) {
		dir, client := newClient(t)
		header.Store("")
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		resp.Body.Close()
		if p := dir.Pressure(time.Now()); p != 0 {
			t.Fatalf("无 Retry-After 不应喂入，got %v", p)
		}
	})

	t.Run("不可解析不喂", func(t *testing.T) {
		dir, client := newClient(t)
		header.Store("soon-ish")
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("请求失败: %v", err)
		}
		resp.Body.Close()
		if p := dir.Pressure(time.Now()); p != 0 {
			t.Fatalf("不可解析的 Retry-After 不应喂入，got %v", p)
		}
	})
}
