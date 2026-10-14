package governor

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 内容与一致性 ----

func TestSnapshotContents(t *testing.T) {
	clk := newFakeClock()
	a := &stubSignal{name: "a", p: 2}
	b := &stubSignal{name: "b", p: 0.5}
	g, err := New(WithSignals(a, b), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	snap := g.Snapshot()
	// 合成压力取最差信号；α = S/(S+C)。
	if !almostEqual(snap.Pressure, 2) {
		t.Fatalf("Pressure = %v, want 2", snap.Pressure)
	}
	if !almostEqual(snap.Suppression, 2.0/3.0) {
		t.Fatalf("Suppression = %v, want 2/3", snap.Suppression)
	}
	if len(snap.Signals) != 2 {
		t.Fatalf("Signals = %v, want 2 条", snap.Signals)
	}
	if snap.Signals[0].Name != "a" || !almostEqual(snap.Signals[0].Pressure, 2) {
		t.Fatalf("Signals[0] = %+v, want a/2", snap.Signals[0])
	}
	if snap.Signals[1].Name != "b" || !almostEqual(snap.Signals[1].Pressure, 0.5) {
		t.Fatalf("Signals[1] = %+v, want b/0.5", snap.Signals[1])
	}

	// 与逐个只读出口一致。
	if !almostEqual(snap.Pressure, g.Pressure()) {
		t.Fatalf("Snapshot.Pressure 与 Pressure() 不一致")
	}
	if !almostEqual(snap.Suppression, g.Suppression()) {
		t.Fatalf("Snapshot.Suppression 与 Suppression() 不一致")
	}
}

func TestSnapshotEmptySignals(t *testing.T) {
	g, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	snap := g.Snapshot()
	if snap.Pressure != 0 || snap.Suppression != 0 {
		t.Fatalf("无信号快照应全 0，got %+v", snap)
	}
	if len(snap.Signals) != 0 {
		t.Fatalf("Signals 应为空，got %v", snap.Signals)
	}
	// 空切片 JSON 序列化为 [] 而非 null。
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(b), `"signals":[]`) {
		t.Fatalf("空 signals 应序列化为 []，got %s", b)
	}
}

func TestSnapshotJSONShape(t *testing.T) {
	clk := newFakeClock()
	sig := &stubSignal{name: "latency", p: 2}
	g, err := New(WithSignals(sig), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b, err := json.Marshal(g.Snapshot())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, key := range []string{"pressure", "suppression", "signals"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("JSON 缺少字段 %q: %s", key, b)
		}
	}
	signals, ok := m["signals"].([]any)
	if !ok || len(signals) != 1 {
		t.Fatalf("signals 应为 1 元素数组: %s", b)
	}
	first, _ := signals[0].(map[string]any)
	if first["name"] != "latency" {
		t.Fatalf("signal JSON name = %v, want latency", first["name"])
	}
	if _, ok := first["pressure"].(float64); !ok {
		t.Fatalf("signal JSON 缺少 pressure: %s", b)
	}
}

// ---- 反馈后快照随之更新 ----

func TestSnapshotReflectsFeedback(t *testing.T) {
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s := g.Snapshot(); s.Pressure != 0 {
		t.Fatalf("反馈前压力应 0，got %v", s.Pressure)
	}
	dec := g.Begin()
	clk.advance(300 * time.Millisecond)
	dec.Record(nil)
	snap := g.Snapshot()
	if !almostEqual(snap.Pressure, 2) {
		t.Fatalf("反馈后压力 = %v, want 2", snap.Pressure)
	}
	if !almostEqual(snap.Suppression, 2.0/3.0) {
		t.Fatalf("反馈后 α = %v, want 2/3", snap.Suppression)
	}
}

// ---- 并发 ----

func TestSnapshotConcurrentWithFeedback(t *testing.T) {
	clk := newFakeClock()
	sig, err := NewLatencySignal(WithLatencyTarget(100*time.Millisecond), WithLatencyBeta(0.5))
	if err != nil {
		t.Fatalf("NewLatencySignal: %v", err)
	}
	g, err := New(WithSignals(sig), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				dec := g.Begin()
				clk.advance(300 * time.Millisecond)
				dec.Record(nil)
			}
		}()
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				snap := g.Snapshot()
				if snap.Pressure < 0 || snap.Suppression < 0 || snap.Suppression > 1 {
					t.Errorf("快照越界: %+v", snap)
					return
				}
			}
		}()
	}
	wg.Wait()
}
