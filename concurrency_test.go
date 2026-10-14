package governor

import (
	"sync"
	"testing"
	"time"
)

// ---- 压力公式 ----

func TestConcurrencyPressureFormula(t *testing.T) {
	sig, err := NewConcurrencySignal(WithConcurrencyTarget(2))
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	if got := sig.Pressure(time.Now()); got != 0 {
		t.Fatalf("空计数压力 = %v, want 0（冷启动）", got)
	}
	for i := 0; i < 2; i++ {
		sig.Enter() // n = N_target：分界线上，压力仍为 0
	}
	if got := sig.Pressure(time.Now()); got != 0 {
		t.Fatalf("n = N_target 压力 = %v, want 0", got)
	}
	for i := 0; i < 2; i++ {
		sig.Enter() // n = 2×N_target：超出目标一倍，S = 1
	}
	if got := sig.Pressure(time.Now()); !almostEqual(got, 1) {
		t.Fatalf("n = 2×N_target 压力 = %v, want 1", got)
	}
	for i := 0; i < 2; i++ {
		sig.Enter() // n = 3×N_target：S = 2
	}
	if got := sig.Pressure(time.Now()); !almostEqual(got, 2) {
		t.Fatalf("n = 3×N_target 压力 = %v, want 2", got)
	}

	// Observe 为空实现：Record 反馈不改变计数。
	sig.Observe(time.Now(), OutcomeFailure, time.Second)
	if got := sig.InFlight(); got != 6 {
		t.Fatalf("Observe 后计数 = %d, want 6", got)
	}
	if got := sig.Pressure(time.Now()); !almostEqual(got, 2) {
		t.Fatalf("Observe 后压力 = %v, want 2", got)
	}

	for i := 0; i < 6; i++ {
		sig.Leave()
	}
	if got := sig.Pressure(time.Now()); got != 0 {
		t.Fatalf("清零后压力 = %v, want 0", got)
	}
}

func TestConcurrencyClampAndInFlight(t *testing.T) {
	sig, err := NewConcurrencySignal()
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	sig.Leave() // 多余的 Leave：钳制在 0，不产生负值
	if got := sig.InFlight(); got != 0 {
		t.Fatalf("多余 Leave 后计数 = %d, want 0", got)
	}
	sig.Enter()
	sig.Enter()
	sig.Enter()
	if got := sig.InFlight(); got != 3 {
		t.Fatalf("计数 = %d, want 3", got)
	}
	sig.Leave()
	if got := sig.InFlight(); got != 2 {
		t.Fatalf("Leave 后计数 = %d, want 2", got)
	}
}

func TestConcurrencyOptionValidation(t *testing.T) {
	for _, n := range []int{0, -5} {
		if _, err := NewConcurrencySignal(WithConcurrencyTarget(n)); err == nil {
			t.Fatalf("WithConcurrencyTarget(%d) 应报错", n)
		}
	}
	if _, err := NewConcurrencySignal(); err != nil {
		t.Fatalf("默认构造不应报错: %v", err)
	}
}

func TestConcurrencyImplementsSignal(t *testing.T) {
	var _ Signal = (*ConcurrencySignal)(nil)
	sig, err := NewConcurrencySignal()
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	if sig.Name() != "concurrency" {
		t.Fatalf("Name = %q, want concurrency", sig.Name())
	}
}

// ---- 与 Governor 集成 ----

func TestConcurrencyDrivesSuppression(t *testing.T) {
	sig, err := NewConcurrencySignal(WithConcurrencyTarget(2))
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	g, err := New(WithSignals(sig))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a := g.Suppression(); a != 0 {
		t.Fatalf("空计数 α = %v, want 0", a)
	}
	for i := 0; i < 5; i++ {
		sig.Enter() // n=5，S=(5-2)/2=1.5，α=1.5/2.5=0.6
	}
	if a := g.Suppression(); !almostEqual(a, 0.6) {
		t.Fatalf("α = %v, want 0.6", a)
	}
	for i := 0; i < 5; i++ {
		sig.Leave()
	}
	if a := g.Suppression(); a != 0 {
		t.Fatalf("清零后 α = %v, want 0", a)
	}
}

// ---- Group 场景：业务手动接线 ----

func TestConcurrencyInGroupManualWiring(t *testing.T) {
	conc, err := NewConcurrencySignal(WithConcurrencyTarget(1))
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	grp, err := NewGroup(WithSignalFactory(func() ([]Signal, error) {
		return []Signal{conc}, nil
	}))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	a, err := grp.Get("a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	conc.Enter()
	conc.Enter() // n=2，S=1，α=0.5
	if got := a.Suppression(); !almostEqual(got, 0.5) {
		t.Fatalf("α = %v, want 0.5", got)
	}
	agg := grp.Signals()
	if len(agg["a"]) != 1 || agg["a"][0].Name != "concurrency" || !almostEqual(agg["a"][0].Pressure, 1) {
		t.Fatalf("聚合快照 = %+v, want concurrency/1", agg["a"])
	}
}

// ---- 并发 ----

func TestConcurrencyConcurrentEnterLeave(t *testing.T) {
	sig, err := NewConcurrencySignal()
	if err != nil {
		t.Fatalf("NewConcurrencySignal: %v", err)
	}
	const workers, per = 50, 20
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				sig.Enter()
				sig.Leave()
			}
		}()
	}
	wg.Wait()
	if got := sig.InFlight(); got != 0 {
		t.Fatalf("并发配对后计数 = %d, want 0", got)
	}
}
