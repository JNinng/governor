package governor

import "time"

// 决策层：压力指数到抑制因子、再到三种控制动作的纯映射。
// 公式与特性见 docs/desc.md §3.3、§4；全部函数无副作用，可独立复用。

// SuppressionFactor 将压力指数 s 映射为抑制因子 α = clamp(s/(s+c), 0, 1)。
// 感知层保证 s >= 0 时结果天然落在 [0, 1)；s <= 0 或 c <= 0 的输入直接返回 0，
// clamp 仅作为异常输入（负压力、负敏感度）下的防御性边界，正常路径不可达。
func SuppressionFactor(s, c float64) float64 {
	if s <= 0 || c <= 0 {
		return 0
	}
	a := s / (s + c)
	if a < 0 {
		return 0
	}
	if a > 1 {
		return 1
	}
	return a
}

// MaxPressure 是多输入合成规则：取各信号压力的最大值（最差信号优先），
// 负输入按 0 处理，无输入视为无压力。
func MaxPressure(pressures ...float64) float64 {
	var max float64
	for _, p := range pressures {
		if p > max {
			max = p
		}
	}
	return max
}

// ScaleBatch 依据抑制因子计算批次大小：max(min, floor(max×(1-α)))。
// 下限保证深度抑制时仍持续产生小批量反馈，控制回路不因批次为 0 而中断。
func ScaleBatch(max, min int, alpha float64) int {
	if min < 1 {
		min = 1
	}
	if max < min {
		return min
	}
	n := int(float64(max) * (1 - alpha))
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

// Backoff 计算延迟执行的等待时长：base × α。线性映射，上限即 base。
func Backoff(base time.Duration, alpha float64) time.Duration {
	if base <= 0 || alpha <= 0 {
		return 0
	}
	if alpha >= 1 {
		return base
	}
	return time.Duration(float64(base) * alpha)
}
