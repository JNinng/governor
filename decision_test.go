package governor

import (
	"math"
	"testing"
	"time"
)

// 期望值全部取自 docs/desc.md §3.3 与 §5 的手工算例（独立真源）。

func TestSuppressionFactor(t *testing.T) {
	tests := []struct {
		name string
		s, c float64
		want float64
	}{
		// §3.3：S = C 时 α = 0.5
		{"S=C 时抑制一半", 1.0, 1.0, 0.5},
		// §5.1：S=0.5 → α=1/3；S=0.3 → 0.3/1.3；S=0.1 → 0.1/1.1
		{"示例5.1 第一轮", 0.5, 1.0, 1.0 / 3.0},
		{"示例5.1 第二轮", 0.3, 1.0, 0.3 / 1.3},
		{"示例5.1 第三轮", 0.1, 1.0, 0.1 / 1.1},
		// §5.2：S=0.6 → α=0.375
		{"示例5.2 第二轮", 0.6, 1.0, 0.375},
		{"无压力不抑制", 0, 1.0, 0},
		// 异常输入防御性钳制：负压力与超界均收敛到 [0,1)
		{"负压力钳制为零", -0.97, 1.0, 0},
		{"接近负C的奇点钳制为零", -0.999, 1.0, 0},
		{"巨大压力趋近但不到1", 999, 1.0, 999.0 / 1000.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SuppressionFactor(tt.s, tt.c)
			if math.Abs(got-tt.want) > 1e-12 {
				t.Fatalf("SuppressionFactor(%v, %v) = %v, want %v", tt.s, tt.c, got, tt.want)
			}
		})
	}
}

func TestScaleBatch(t *testing.T) {
	tests := []struct {
		name     string
		max, min int
		alpha    float64
		want     int
	}{
		// §5.1：α=1/3 → 666；α≈0.23 → 769；α≈0.09 → 909；α=0 → 满批
		{"示例5.1 第二轮批次", 1000, 1, 1.0 / 3.0, 666},
		{"示例5.1 第三轮批次", 1000, 1, 0.3 / 1.3, 769},
		{"示例5.1 第四轮批次", 1000, 1, 0.1 / 1.1, 909},
		{"无压力满批", 1000, 1, 0, 1000},
		// 批次下限：深度抑制不触底为 0
		{"深度抑制触底保下限", 1000, 1, 0.9999, 1},
		{"完全抑制仍为下限", 1000, 5, 1, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ScaleBatch(tt.max, tt.min, tt.alpha)
			if got != tt.want {
				t.Fatalf("ScaleBatch(%d, %d, %v) = %d, want %d", tt.max, tt.min, tt.alpha, got, tt.want)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	tests := []struct {
		name  string
		base  time.Duration
		alpha float64
		want  time.Duration
	}{
		// §5.2：α=0.5 → 1000ms；α=0.375 → 750ms；α=0 → 0
		{"示例5.2 第二轮等待", 2000 * time.Millisecond, 0.5, 1000 * time.Millisecond},
		{"示例5.2 第三轮等待", 2000 * time.Millisecond, 0.375, 750 * time.Millisecond},
		{"无压力不等待", 2000 * time.Millisecond, 0, 0},
		{"完全抑制等待满基准", 2 * time.Second, 1, 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Backoff(tt.base, tt.alpha)
			if got != tt.want {
				t.Fatalf("Backoff(%v, %v) = %v, want %v", tt.base, tt.alpha, got, tt.want)
			}
		})
	}
}

func TestMaxPressure(t *testing.T) {
	tests := []struct {
		name      string
		pressures []float64
		want      float64
	}{
		// §3.2 多输入合成：最差信号优先
		{"取最大", []float64{0.3, 0.5, 0.1}, 0.5},
		{"单信号", []float64{0.7}, 0.7},
		{"无信号视为无压力", nil, 0},
		{"异常负输入不传播", []float64{-1, 0.2}, 0.2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MaxPressure(tt.pressures...); got != tt.want {
				t.Fatalf("MaxPressure(%v) = %v, want %v", tt.pressures, got, tt.want)
			}
		})
	}
}
