package governor

import (
	"math"
	"testing"
	"time"
)

// 期望值取自 docs/desc.md §3.2 公式与 §5 示例的 EWMA 算例（独立真源）。

var t0 = time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

func TestRejectionSignalPressure(t *testing.T) {
	tests := []struct {
		name    string
		k       float64
		total   int
		success int
		want    float64
	}{
		// S = max(0, (total - K·success) / (total + 1))
		{"健康期钳制为零", 2, 100, 100, 0},
		{"成功率恰好50%不抑制", 2, 100, 50, 0},
		{"75%失败", 2, 100, 25, 50.0 / 101.0},
		{"全部失败", 2, 100, 0, 100.0 / 101.0},
		{"K=1时半数失败即产生压力", 1, 100, 50, 50.0 / 101.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig, err := NewRejectionSignal(WithRejectionK(tt.k))
			if err != nil {
				t.Fatalf("NewRejectionSignal: %v", err)
			}
			for i := 0; i < tt.total; i++ {
				out := OutcomeFailure
				if i < tt.success {
					out = OutcomeSuccess
				}
				sig.Observe(t0.Add(time.Duration(i)*time.Millisecond), out, 0)
			}
			if got := sig.Pressure(t0.Add(time.Duration(tt.total) * time.Millisecond)); math.Abs(got-tt.want) > 1e-12 {
				t.Fatalf("Pressure = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRejectionSignalColdStartAndStarvation(t *testing.T) {
	sig, err := NewRejectionSignal()
	if err != nil {
		t.Fatalf("NewRejectionSignal: %v", err)
	}
	if got := sig.Pressure(t0); got != 0 {
		t.Fatalf("冷启动无样本应为 0，got %v", got)
	}
	// 全部失败产生压力后，窗口滑走：沿用上一次压力（区分“无反馈”与“无压力”）
	for i := 0; i < 10; i++ {
		sig.Observe(t0.Add(time.Duration(i)*time.Second), OutcomeFailure, 0)
	}
	afterSample := sig.Pressure(t0.Add(10 * time.Second))
	if afterSample <= 0 {
		t.Fatalf("全失败应产生压力，got %v", afterSample)
	}
	starved := sig.Pressure(t0.Add(10 * time.Second).Add(10 * time.Minute))
	if starved != afterSample {
		t.Fatalf("窗口耗尽应沿用上次压力 %v，got %v", afterSample, starved)
	}
}

func TestRejectionSignalIgnoresNeutralOutcome(t *testing.T) {
	sig, _ := NewRejectionSignal()
	for i := 0; i < 10; i++ {
		sig.Observe(t0, OutcomeIgnore, 0)
	}
	if got := sig.Pressure(t0); got != 0 {
		t.Fatalf("Ignore 不计入统计，压力应为 0，got %v", got)
	}
}

func TestLatencySignalEWMA(t *testing.T) {
	// §5.1：target=100ms，β=0.5，采样 150→110→90→75，EWMA 依次 150/130/110/92.5
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	seq := []struct {
		sample time.Duration
		wantS  float64
	}{
		{150 * time.Millisecond, 0.5},
		{110 * time.Millisecond, 0.3},
		{90 * time.Millisecond, 0.1},
		{75 * time.Millisecond, 0}, // EWMA=92.5ms < 100ms
	}
	now := t0
	for i, step := range seq {
		now = now.Add(time.Second)
		sig.Observe(now, OutcomeSuccess, step.sample)
		if got := sig.Pressure(now); math.Abs(got-step.wantS) > 1e-12 {
			t.Fatalf("第%d次采样后 Pressure = %v, want %v", i+1, got, step.wantS)
		}
	}
}

func TestLatencySignalEWMA568(t *testing.T) {
	// §5.2：target=500ms，β=0.5，采样 1000→600→300→200，EWMA 依次 1000/800/550/375
	sig, err := NewLatencySignal(WithLatencyTarget(500*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	seq := []struct {
		sample time.Duration
		wantS  float64
	}{
		{1000 * time.Millisecond, 1.0},
		{600 * time.Millisecond, 0.6},
		{300 * time.Millisecond, 0.1},
		{200 * time.Millisecond, 0},
	}
	now := t0
	for i, step := range seq {
		now = now.Add(time.Second)
		sig.Observe(now, OutcomeSuccess, step.sample)
		if got := sig.Pressure(now); math.Abs(got-step.wantS) > 1e-12 {
			t.Fatalf("第%d次采样后 Pressure = %v, want %v", i+1, got, step.wantS)
		}
	}
}

func TestLatencySignalColdStartAndIgnore(t *testing.T) {
	sig, _ := NewLatencySignal(WithLatencyTarget(100 * time.Millisecond))
	if got := sig.Pressure(t0); got != 0 {
		t.Fatalf("冷启动无样本应为 0，got %v", got)
	}
	sig.Observe(t0, OutcomeIgnore, 10*time.Second)
	if got := sig.Pressure(t0); got != 0 {
		t.Fatalf("Ignore 不喂入 EWMA，got %v", got)
	}
	// 饥饿时 EWMA 保值（375ms < 500ms target → 0 压力的场景另测）
	sig2, _ := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	sig2.Observe(t0, OutcomeSuccess, 300*time.Millisecond)
	if got := sig2.Pressure(t0.Add(time.Hour)); got != 2.0 {
		t.Fatalf("饥饿时应沿用上次压力 2.0，got %v", got)
	}
}

func TestLatencySignalHungerUsesNow(t *testing.T) {
	// 饥饿沿用语义：超阈值无采样沿用上次压力；饥饿后新采样立即恢复实时计算。
	sig, err := NewLatencySignal(
		WithLatencyTarget(100*time.Millisecond),
		WithLatencyBeta(0.5),
		WithLatencyWindow(time.Minute),
	)
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	sig.Observe(t0, OutcomeSuccess, 300*time.Millisecond) // S = 2.0
	if got := sig.Pressure(t0.Add(time.Minute)); got != 2.0 {
		t.Fatalf("窗口内应实时计算 2.0，got %v", got)
	}
	if got := sig.Pressure(t0.Add(2 * time.Hour)); got != 2.0 {
		t.Fatalf("饥饿时应沿用上次压力 2.0，got %v", got)
	}
	// 饥饿后新采样恢复实时 EWMA：0.5×300 + 0.5×100 = 200ms → S = 1.0
	sig.Observe(t0.Add(2*time.Hour), OutcomeSuccess, 100*time.Millisecond)
	if got := sig.Pressure(t0.Add(2 * time.Hour)); got != 1.0 {
		t.Fatalf("饥饿后新采样应实时计算 1.0，got %v", got)
	}
}

func TestRejectionSignalPressureIsReadOnly(t *testing.T) {
	// Pressure 只读：重复调用及饥饿期调用不得改变内部状态（含沿用值）。
	sig, _ := NewRejectionSignal()
	for i := 0; i < 5; i++ {
		sig.Observe(t0.Add(time.Duration(i)*time.Second), OutcomeFailure, 0)
	}
	first := sig.Pressure(t0.Add(5 * time.Second))
	for i := 0; i < 3; i++ {
		if got := sig.Pressure(t0.Add(5 * time.Second)); got != first {
			t.Fatalf("重复 Pressure 应幂等，第%d次 got %v, want %v", i+1, got, first)
		}
	}
	if got := sig.Pressure(t0.Add(time.Hour)); got != first {
		t.Fatalf("饥饿时应沿用上次压力 %v，got %v", first, got)
	}
}

func TestNilOptionsRejected(t *testing.T) {
	for name, opt := range map[string]Option{
		"WithClassifier": WithClassifier(nil),
		"WithMeter":      WithMeter(nil),
		"WithLogger":     WithLogger(nil),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(opt); err == nil {
				t.Fatal("nil 注入应返回构造错误")
			}
		})
	}
}

func TestSignalNames(t *testing.T) {
	r, _ := NewRejectionSignal()
	l, _ := NewLatencySignal(WithLatencyTarget(time.Second))
	if r.Name() != "rejection" || l.Name() != "latency" {
		t.Fatalf("信号名 = %q, %q", r.Name(), l.Name())
	}
}
