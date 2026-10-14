package governor

import (
	"sync"
	"testing"
	"time"
)

// ---- 压力衰减曲线 ----

func TestDirectivePressureDecay(t *testing.T) {
	sig, err := NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	t0 := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

	// 从未喂入：压力 0（冷启动）。
	if got := sig.Pressure(t0); got != 0 {
		t.Fatalf("冷启动压力 = %v, want 0", got)
	}

	// 喂入 10s 指令（默认基准 1s）：喂入时 S=10。
	sig.FeedAt(t0, 10*time.Second)
	if got := sig.Pressure(t0); !almostEqual(got, 10) {
		t.Fatalf("喂入时压力 = %v, want 10", got)
	}
	// 过半：剩余 5s → S=5。
	if got := sig.Pressure(t0.Add(5 * time.Second)); !almostEqual(got, 5) {
		t.Fatalf("过半时压力 = %v, want 5", got)
	}
	// 到期：自行归零，不产生陈旧压力。
	if got := sig.Pressure(t0.Add(10 * time.Second)); got != 0 {
		t.Fatalf("到期压力 = %v, want 0", got)
	}
	if got := sig.Pressure(t0.Add(11 * time.Second)); got != 0 {
		t.Fatalf("到期后压力 = %v, want 0", got)
	}

	// 再次喂入以最后一次为准，窗口重新起算。
	sig.FeedAt(t0.Add(20*time.Second), 2*time.Second)
	if got := sig.Pressure(t0.Add(20 * time.Second)); !almostEqual(got, 2) {
		t.Fatalf("重喂时压力 = %v, want 2", got)
	}
	if got := sig.Pressure(t0.Add(21 * time.Second)); !almostEqual(got, 1) {
		t.Fatalf("重喂 1s 后压力 = %v, want 1", got)
	}
}

func TestDirectiveClampAndClear(t *testing.T) {
	sig, err := NewDirectiveSignal(WithDirectiveScale(500 * time.Millisecond))
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	t0 := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

	// 自定义基准：2s 指令 / 0.5s 基准 = S=4。
	sig.FeedAt(t0, 2*time.Second)
	if got := sig.Pressure(t0); !almostEqual(got, 4) {
		t.Fatalf("自定义基准压力 = %v, want 4", got)
	}

	// 负值按 0 处理。
	sig.FeedAt(t0.Add(time.Second), -3*time.Second)
	if got := sig.Pressure(t0.Add(time.Second)); got != 0 {
		t.Fatalf("负指令压力 = %v, want 0", got)
	}

	// 喂入 0 = 清除当前指令。
	sig.FeedAt(t0, 10*time.Second)
	sig.FeedAt(t0.Add(time.Second), 0)
	if got := sig.Pressure(t0.Add(time.Second)); got != 0 {
		t.Fatalf("清除后压力 = %v, want 0", got)
	}
}

func TestDirectiveObserveNoop(t *testing.T) {
	sig, err := NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	t0 := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	sig.FeedAt(t0, 10*time.Second)
	// Record 反馈经 Observe 进入：不改变指令状态。
	sig.Observe(t0, OutcomeFailure, time.Second)
	sig.Observe(t0, OutcomeIgnore, 0)
	if got := sig.Pressure(t0); !almostEqual(got, 10) {
		t.Fatalf("Observe 后压力 = %v, want 10", got)
	}
}

func TestDirectiveImplementsSignal(t *testing.T) {
	var _ Signal = (*DirectiveSignal)(nil)
	sig, err := NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	if sig.Name() != "directive" {
		t.Fatalf("Name = %q, want directive", sig.Name())
	}
}

func TestDirectiveOptionValidation(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if _, err := NewDirectiveSignal(WithDirectiveScale(d)); err == nil {
			t.Fatalf("WithDirectiveScale(%v) 应报错", d)
		}
	}
	if _, err := NewDirectiveSignal(); err != nil {
		t.Fatalf("默认构造不应报错: %v", err)
	}
}

// ---- 与 Governor 集成（时钟注入下的确定性衰减） ----

func TestDirectiveDrivesSuppression(t *testing.T) {
	sig, err := NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	clk := newFakeClock()
	g, err := New(WithSignals(sig), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if a := g.Suppression(); a != 0 {
		t.Fatalf("未喂入 α = %v, want 0", a)
	}

	// 服务端要求等 10s：S=10 → α=10/11。
	sig.FeedAt(clk.Now(), 10*time.Second)
	if a := g.Suppression(); !almostEqual(a, 10.0/11.0) {
		t.Fatalf("α = %v, want 10/11", a)
	}

	// 指令过半：S=5 → α=5/6。
	clk.advance(5 * time.Second)
	if a := g.Suppression(); !almostEqual(a, 5.0/6.0) {
		t.Fatalf("α = %v, want 5/6", a)
	}

	// 指令到期：压力自行归零，α 回落。
	clk.advance(5 * time.Second)
	if a := g.Suppression(); a != 0 {
		t.Fatalf("指令到期后 α = %v, want 0", a)
	}
}

// ---- 并发 ----

func TestDirectiveConcurrentFeedPressure(t *testing.T) {
	sig, err := NewDirectiveSignal()
	if err != nil {
		t.Fatalf("NewDirectiveSignal: %v", err)
	}
	base := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < 25; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				sig.FeedAt(base, time.Duration(i+1)*time.Second)
				_ = sig.Pressure(base)
			}
		}()
	}
	wg.Wait()
	if got := sig.Pressure(base); !almostEqual(got, 20) {
		t.Fatalf("最后一次喂入应生效，压力 = %v, want 20", got)
	}
}
