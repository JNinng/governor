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
