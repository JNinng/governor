package governor

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jninng/observ"
)

// 默认参数与 docs/desc.md 的建议一致（K=2、C=1、β=0.4、探测 2%、窗口 90s）。

const (
	defaultSensitivity = 1.0
	defaultProbeRatio  = 0.02
	defaultBatchMax    = 100
	defaultBatchMin    = 1
	defaultBaseWait    = time.Second
	defaultPrefix      = "governor"
)

type config struct {
	signals    []Signal
	c          float64
	probeRatio float64
	batchMax   int
	batchMin   int
	baseWait   time.Duration
	classifier Classifier
	clock      Clock
	meter      observ.Meter
	logger     observ.Logger
	prefix     string
	source     rand.Source
}

func defaultConfig() *config {
	return &config{
		c:          defaultSensitivity,
		probeRatio: defaultProbeRatio,
		batchMax:   defaultBatchMax,
		batchMin:   defaultBatchMin,
		baseWait:   defaultBaseWait,
		classifier: defaultClassifier,
		clock:      systemClock{},
		prefix:     defaultPrefix,
		source:     rand.NewSource(time.Now().UnixNano()),
	}
}

func (c *config) validate() error {
	if c.c <= 0 {
		return fmt.Errorf("governor: sensitivity C must be positive, got %v", c.c)
	}
	if c.probeRatio < 0 || c.probeRatio >= 1 {
		return fmt.Errorf("governor: probe ratio must be in [0,1), got %v", c.probeRatio)
	}
	if c.batchMin < 1 {
		return fmt.Errorf("governor: batch min must be >= 1, got %d", c.batchMin)
	}
	if c.batchMax < c.batchMin {
		return fmt.Errorf("governor: batch max %d must be >= min %d", c.batchMax, c.batchMin)
	}
	if c.baseWait < 0 {
		return fmt.Errorf("governor: base wait must be >= 0, got %v", c.baseWait)
	}
	if c.prefix == "" {
		return fmt.Errorf("governor: metric prefix must not be empty")
	}
	return nil
}

func defaultClassifier(err error) Outcome {
	if err == nil {
		return OutcomeSuccess
	}
	return OutcomeFailure
}

// Option 配置 Governor。
type Option func(*config) error

// WithSignals 设置压力信号集合（多信号取最差值合成）。
func WithSignals(signals ...Signal) Option {
	return func(c *config) error {
		c.signals = signals
		return nil
	}
}

// WithSensitivity 设置决策层敏感度系数 C（默认 1.0），须 > 0（New 时统一校验）。
// S = C 时 α = 0.5。C 越小对压力越敏感。
func WithSensitivity(c float64) Option {
	return func(cfg *config) error {
		cfg.c = c
		return nil
	}
}

// WithProbeRatio 设置探测放行比例（默认 0.02，建议 0.01~0.05）。
// 0 表示关闭探测（抑制可逼近全丢，慎用）；须在 [0,1) 内（New 时统一校验）。
// 探测作用于将被丢弃的请求，保证深度抑制期间仍有新鲜反馈样本。
func WithProbeRatio(r float64) Option {
	return func(c *config) error {
		c.probeRatio = r
		return nil
	}
}

// WithBatch 设置策略二的批次上限与下限（默认 100 / 1）。
// 须满足 min >= 1 且 max >= min（New 时统一校验）。
func WithBatch(max, min int) Option {
	return func(c *config) error {
		c.batchMax, c.batchMin = max, min
		return nil
	}
}

// WithBaseWait 设置策略三的基准等待 T_base（默认 1s），实际等待 = T_base × α。
// 须 >= 0（New 时统一校验）。
func WithBaseWait(d time.Duration) Option {
	return func(c *config) error {
		c.baseWait = d
		return nil
	}
}

// WithClassifier 注入成败分类器（非 nil 错误即失败）。
// nil 一律报错；如需恢复默认行为，不传该 option 即可。
func WithClassifier(fn Classifier) Option {
	return func(c *config) error {
		if fn == nil {
			return fmt.Errorf("governor: classifier must not be nil")
		}
		c.classifier = fn
		return nil
	}
}

// WithClock 注入时间源（测试用）。
func WithClock(clk Clock) Option {
	return func(c *config) error {
		if clk == nil {
			return fmt.Errorf("governor: clock must not be nil")
		}
		c.clock = clk
		return nil
	}
}

// WithMeter 注入指标 Meter（默认 observ.NoopMeter），不得为 nil。
// 指标遵守 observ 无 label 约束：枚举拆名、耗时 _seconds（ADR-0001）。
func WithMeter(m observ.Meter) Option {
	return func(c *config) error {
		if m == nil {
			return fmt.Errorf("governor: meter must not be nil")
		}
		c.meter = m
		return nil
	}
}

// WithLogger 注入日志 Logger（不得为 nil；缺省在构造期快照 observ.DefaultLogger()）。
func WithLogger(l observ.Logger) Option {
	return func(c *config) error {
		if l == nil {
			return fmt.Errorf("governor: logger must not be nil")
		}
		c.logger = l
		return nil
	}
}

// WithMetricPrefix 设置指标名前缀（默认 "governor"），不得为空，用于多实例区分
// （observ 无 label，前缀是唯一合规的实例级隔离手段）。
func WithMetricPrefix(p string) Option {
	return func(c *config) error {
		c.prefix = p
		return nil
	}
}

// withRandSource 注入确定性随机源（测试用，不导出）。
func withRandSource(src rand.Source) Option {
	return func(c *config) error {
		if src == nil {
			return fmt.Errorf("governor: rand source must not be nil")
		}
		c.source = src
		return nil
	}
}
