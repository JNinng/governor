package governor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ---- 惰性创建与复用 ----

func TestGroupLazyCreationAndKeys(t *testing.T) {
	grp, err := NewGroup()
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	if keys := grp.Keys(); len(keys) != 0 {
		t.Fatalf("Keys 不应触发创建，got %v", keys)
	}

	a1, err := grp.Get("a")
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	a2, err := grp.Get("a")
	if err != nil {
		t.Fatalf("Get(a) 二次: %v", err)
	}
	b, err := grp.Get("b")
	if err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	if a1 != a2 {
		t.Fatalf("同一 key 应复用同一实例")
	}
	if a1 == b {
		t.Fatalf("不同 key 不应共享实例")
	}
	if keys := grp.Keys(); len(keys) != 2 {
		t.Fatalf("Keys = %v, want 2 个", keys)
	}
}

func TestGroupEmptyKeyRejected(t *testing.T) {
	grp, err := NewGroup()
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	if _, err := grp.Get(""); err == nil {
		t.Fatalf("空 key 应报错")
	}
	if keys := grp.Keys(); len(keys) != 0 {
		t.Fatalf("报错的 key 不应创建实例，got %v", keys)
	}
}

// ---- 信号逐实例创建（不可共享） ----

func TestGroupSignalIsolation(t *testing.T) {
	grp, err := NewGroup()
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	a, err := grp.Get("a")
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	b, err := grp.Get("b")
	if err != nil {
		t.Fatalf("Get(b): %v", err)
	}

	// 向 a 喂入一次失败：K=2 时 S=(1-0)/2=0.5。
	dec := a.Begin()
	dec.Record(errors.New("boom"))

	if got := a.Pressure(); !almostEqual(got, 0.5) {
		t.Fatalf("a 压力 = %v, want 0.5", got)
	}
	if got := b.Pressure(); got != 0 {
		t.Fatalf("b 压力 = %v, want 0（信号不可跨实例共享）", got)
	}
}

func TestGroupSignalsAggregate(t *testing.T) {
	grp, err := NewGroup()
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	a, err := grp.Get("a")
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if _, err := grp.Get("b"); err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	dec := a.Begin()
	dec.Record(errors.New("boom"))

	agg := grp.Signals()
	if len(agg) != 2 {
		t.Fatalf("聚合快照含 %d 个 key, want 2", len(agg))
	}
	sa := agg["a"]
	if len(sa) != 1 || sa[0].Name != "rejection" || !almostEqual(sa[0].Pressure, 0.5) {
		t.Fatalf("a 的快照 = %+v, want rejection/0.5", sa)
	}
	sb := agg["b"]
	if len(sb) != 1 || sb[0].Pressure != 0 {
		t.Fatalf("b 的快照 = %+v, want rejection/0", sb)
	}
}

// ---- 指标前缀按 key 生成 ----

func TestGroupMetricPrefixPerKey(t *testing.T) {
	m := newRecordingMeter(t)
	grp, err := NewGroup(WithGovernorOptions(WithMeter(m)))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	if _, err := grp.Get("user.service:8080"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	for _, name := range m.created() {
		if len(name) < len("governor_user_service_8080_") || name[:len("governor_user_service_8080_")] != "governor_user_service_8080_" {
			t.Fatalf("指标名 %q 未使用按 key 生成的前缀", name)
		}
	}
	found := false
	for _, name := range m.created() {
		if name == "governor_user_service_8080_pressure" {
			found = true
		}
	}
	if !found {
		t.Fatalf("未找到 governor_user_service_8080_pressure，got %v", m.created())
	}

	// 自定义前缀函数：输出原样使用。
	custom := newRecordingMeter(t)
	grp2, err := NewGroup(
		WithKeyPrefix(func(key string) string { return "svc_" + key }),
		WithGovernorOptions(WithMeter(custom)),
	)
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	if _, err := grp2.Get("a.b"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	for _, name := range custom.created() {
		if len(name) < 5 || name[:5] != "svc_a" {
			t.Fatalf("自定义前缀未生效，指标名 %q", name)
		}
	}
}

// ---- 模板 option ----

func TestGroupTemplateOptions(t *testing.T) {
	grp, err := NewGroup(WithGovernorOptions(WithSensitivity(0.5)))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	a, err := grp.Get("a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// 一次失败 S=0.5；C=0.5 时 α = 0.5/(0.5+0.5) = 0.5，验证模板 C 生效。
	dec := a.Begin()
	dec.Record(errors.New("boom"))
	if got := a.Suppression(); !almostEqual(got, 0.5) {
		t.Fatalf("α = %v, want 0.5（模板 WithSensitivity 未生效）", got)
	}

	// 模板取值非法：延迟到实例创建时报错。
	bad, err := NewGroup(WithGovernorOptions(WithSensitivity(0)))
	if err != nil {
		t.Fatalf("NewGroup 应通过（构造期无法检查模板取值）: %v", err)
	}
	if _, err := bad.Get("a"); err == nil {
		t.Fatalf("非法模板取值应在实例创建时报错")
	}
	if keys := bad.Keys(); len(keys) != 0 {
		t.Fatalf("构造失败的实例不应留存，got %v", keys)
	}
}

// ---- 构造失败与校验 ----

func TestGroupSignalFactoryError(t *testing.T) {
	grp, err := NewGroup(WithSignalFactory(func() ([]Signal, error) {
		return nil, errors.New("boom")
	}))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	// 构造失败不缓存：两次调用都重试并报错，且不留存实例。
	for i := 0; i < 2; i++ {
		if _, err := grp.Get("a"); err == nil {
			t.Fatalf("第%d次 Get 应报错", i+1)
		}
	}
	if keys := grp.Keys(); len(keys) != 0 {
		t.Fatalf("构造失败不应留存实例，got %v", keys)
	}
}

func TestGroupOptionValidation(t *testing.T) {
	tests := []struct {
		name string
		opt  GroupOption
	}{
		{"nil signal factory", WithSignalFactory(nil)},
		{"nil key prefix", WithKeyPrefix(nil)},
		{"nil governor option", WithGovernorOptions(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewGroup(tt.opt); err == nil {
				t.Fatalf("NewGroup 应报错")
			}
		})
	}
	if _, err := NewGroup(); err != nil {
		t.Fatalf("默认构造不应报错: %v", err)
	}
}

// ---- 边界：实例可脱离 Group 单独使用 ----

func TestGroupInstanceStandaloneUse(t *testing.T) {
	grp, err := NewGroup()
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	a, err := grp.Get("a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	v, err := Do(context.Background(), a, func(context.Context) (string, error) {
		return "ok", nil
	})
	if err != nil || v != "ok" {
		t.Fatalf("Do = %q, %v", v, err)
	}
	// 实例自身的能力不受 Group 影响。
	if err := a.Update(WithBaseWait(2 * time.Second)); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// ---- 并发 ----

func TestGroupConcurrentGet(t *testing.T) {
	grp, err := NewGroup()
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	const workers = 25
	var wg sync.WaitGroup
	pointers := make(chan *Governor, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, err := grp.Get("shared")
			if err != nil {
				t.Errorf("Get: %v", err)
				return
			}
			pointers <- g
		}()
	}
	wg.Wait()
	close(pointers)
	var want *Governor
	for g := range pointers {
		if want == nil {
			want = g
		} else if g != want {
			t.Fatalf("并发 Get 应得到同一实例")
		}
	}
}

// ---- 前缀碰撞检测 ----

func TestGroupPrefixCollision(t *testing.T) {
	grp, err := NewGroup()
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	if _, err := grp.Get("user.service"); err != nil {
		t.Fatalf("Get(user.service): %v", err)
	}
	// "user.service" 与 "user_service" 规整后同为 governor_user_service。
	if _, err := grp.Get("user_service"); err == nil {
		t.Fatalf("前缀碰撞应报错")
	}
	if keys := grp.Keys(); len(keys) != 1 {
		t.Fatalf("碰撞的 key 不应创建实例，got %v", keys)
	}
	// 已创建的 key 不受影响。
	if _, err := grp.Get("user.service"); err != nil {
		t.Fatalf("已有 key 应正常复用: %v", err)
	}

	// 自定义前缀函数的输出同样参与碰撞检测。
	grp2, err := NewGroup(WithKeyPrefix(func(string) string { return "fixed" }))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}
	if _, err := grp2.Get("a"); err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if _, err := grp2.Get("b"); err == nil {
		t.Fatalf("自定义前缀的碰撞应报错")
	}
}
