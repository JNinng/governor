package governor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// 压测套件：量化高并发下 governor 各路径的开销，为"是否需要无锁化"提供数据。
//
// 覆盖三层分解：
//   - 决策纯函数（SuppressionFactor）——开销地板，锁/同步成本皆为相对它度量；
//   - 信号组件（Latency/Rejection 的 Observe/Pressure）——组件级锁成本；
//   - Governor 热路径（Allow/Do/Record/Wait/BatchSize）与读路径（Pressure/Signals）；
//   - 多实例分片（DoSharded*）——生产多 Governor 部署下的竞争隔离模型。
//
// 复现命令：
//
//	# 1) 吞吐与分配基线
//	go test -run '^$' -bench . -benchmem
//
//	# 2) 并行度扩展性（ns/op 随 P 上升而退化 = 锁竞争代价）
//	go test -run '^$' -bench 'Do|Allow|Record' -benchmem -cpu 1,4,28
//
//	# 3) 锁竞争画像（等待时间按调用点聚合）
//	go test -run '^$' -bench 'Parallel' -benchtime 2s -mutexprofile mutex.out
//	go tool pprof -top -nodecount 15 mutex.out
//
//	# 4) CPU 热点
//	go test -run '^$' -bench 'DoLatency' -benchtime 2s -cpuprofile cpu.out
//	go tool pprof -top -nodecount 20 cpu.out
//
// 注意：基准使用真实系统时钟与默认随机源；压测机与生产机的 time.Now、
// 调度器行为可能不同（Windows/Linux 差异最大），结论以相对比例为准。

// ---- 测试基建 ----

func benchLatencySignal(b *testing.B) *LatencySignal {
	b.Helper()
	sig, err := NewLatencySignal(WithLatencyTarget(100 * time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	return sig
}

func benchRejectionSignal(b *testing.B) *RejectionSignal {
	b.Helper()
	sig, err := NewRejectionSignal()
	if err != nil {
		b.Fatal(err)
	}
	return sig
}

func benchGovernor(b *testing.B, signals ...Signal) *Governor {
	b.Helper()
	g, err := New(WithSignals(signals...))
	if err != nil {
		b.Fatal(err)
	}
	return g
}

var nopOp = func(context.Context) (struct{}, error) { return struct{}{}, nil }

// ---- 决策纯函数：开销地板 ----

func BenchmarkSuppressionFactor(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = SuppressionFactor(2.5, 1.0)
	}
}

// ---- 构造：多实例部署时每个远端目标一个 Governor ----

func BenchmarkNewGovernor(b *testing.B) {
	sig := benchLatencySignal(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := New(WithSignals(sig)); err != nil {
			b.Fatal(err)
		}
	}
}

// ---- 信号组件级（各自一把 sync.Mutex）----

func BenchmarkLatencyObserveParallel(b *testing.B) {
	sig := benchLatencySignal(b)
	now := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sig.Observe(now, OutcomeSuccess, 5*time.Millisecond)
		}
	})
}

func BenchmarkLatencyPressureParallel(b *testing.B) {
	sig := benchLatencySignal(b)
	now := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = sig.Pressure(now)
		}
	})
}

func BenchmarkRejectionObserveParallel(b *testing.B) {
	sig := benchRejectionSignal(b)
	now := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sig.Observe(now, OutcomeSuccess, 0)
		}
	})
}

func BenchmarkRejectionPressureParallel(b *testing.B) {
	sig := benchRejectionSignal(b)
	now := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = sig.Pressure(now)
		}
	})
}

// ---- Governor 决策热路径 ----

// AllowNoSignals 隔离 Governor 自身开销（g.mu×2 + rand + 指标），
// 与 AllowLatency 相减即得信号聚合的边际成本。
func BenchmarkAllowNoSignalsParallel(b *testing.B) {
	g := benchGovernor(b)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = g.Allow()
		}
	})
}

func BenchmarkAllowLatencyParallel(b *testing.B) {
	g := benchGovernor(b, benchLatencySignal(b))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = g.Allow()
		}
	})
}

// ---- 全闭环（Allow + 业务调用 + Record），最贴近真实接口路径 ----

func BenchmarkDoLatencyParallel(b *testing.B) {
	g := benchGovernor(b, benchLatencySignal(b))
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Do(ctx, g, nopOp)
		}
	})
}

func BenchmarkDoRejectionParallel(b *testing.B) {
	g := benchGovernor(b, benchRejectionSignal(b))
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Do(ctx, g, nopOp)
		}
	})
}

func BenchmarkDoBothSignalsParallel(b *testing.B) {
	g := benchGovernor(b, benchLatencySignal(b), benchRejectionSignal(b))
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Do(ctx, g, nopOp)
		}
	})
}

// Record 单独度量：Begin+Record（批处理模式 Begin/Wait 令牌的反馈成本）。
func BenchmarkRecordParallel(b *testing.B) {
	g := benchGovernor(b, benchLatencySignal(b))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			g.Begin().Record(nil)
		}
	})
}

func BenchmarkBatchSizeParallel(b *testing.B) {
	g := benchGovernor(b, benchLatencySignal(b))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = g.BatchSize()
		}
	})
}

func BenchmarkWaitParallel(b *testing.B) {
	// α=0 时等待为 0（无睡眠），度量的是决策与账目成本。
	g := benchGovernor(b, benchLatencySignal(b))
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			g.Wait(ctx)
		}
	})
}

// ---- 只读观测路径 ----

func BenchmarkPressureParallel(b *testing.B) {
	g := benchGovernor(b, benchLatencySignal(b))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = g.Pressure()
		}
	})
}

func BenchmarkSignalsParallel(b *testing.B) {
	g := benchGovernor(b, benchLatencySignal(b))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = g.Signals()
		}
	})
}

// ---- 多实例分片：生产部署模型（每目标一个 Governor，锁互不相干）----

func benchDoSharded(b *testing.B, shards int) {
	gs := make([]*Governor, shards)
	for i := range gs {
		gs[i] = benchGovernor(b, benchLatencySignal(b))
	}
	var next atomic.Uint64
	ctx := context.Background()
	// 每 goroutine 固定一个 Governor（按序号取模，P>shards 时复用），
	// 模拟业务按目标哈希路由、竞争被实例数摊薄的效果。
	b.RunParallel(func(pb *testing.PB) {
		g := gs[int(next.Add(1)-1)%shards]
		for pb.Next() {
			_, _ = Do(ctx, g, nopOp)
		}
	})
}

func BenchmarkDoSharded4Parallel(b *testing.B)  { benchDoSharded(b, 4) }
func BenchmarkDoSharded8Parallel(b *testing.B)  { benchDoSharded(b, 8) }
func BenchmarkDoSharded28Parallel(b *testing.B) { benchDoSharded(b, 28) }
