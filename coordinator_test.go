package deskbridge

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingPrep 返回一个准备函数：进入时通过 started 通知，随后阻塞直到
// gate 关闭，返回 result。调用方可用它精确控制迟到结果的到达时机。
func blockingPrep(started chan<- struct{}, gate <-chan struct{}, result bool) PrepareFunc {
	return func() bool {
		if started != nil {
			close(started)
		}
		<-gate
		return result
	}
}

// checkConsistency 从快照中的会话与待决申请逐项重新统计名额，
// 要求与快照给出的 TechUsed 完全一致。
func checkConsistency(t *testing.T, snap Snapshot) {
	t.Helper()
	want := make(map[TechID]int)
	for _, s := range snap.Sessions {
		if s.State == StateEnded {
			continue
		}
		want[s.Owner]++
		if s.Transfer != nil {
			want[s.Transfer.Target]++
		}
	}
	if len(want) != len(snap.TechUsed) {
		t.Fatalf("TechUsed 技师集合不一致: got %v, want %v", snap.TechUsed, want)
	}
	for tech, n := range want {
		if snap.TechUsed[tech] != n {
			t.Fatalf("技师 %s 名额不一致: got %d, want %d (快照 %+v)", tech, snap.TechUsed[tech], n, snap)
		}
	}
}

// establishActive 以立即成功的准备函数建立会话并等待其生效。
func establishActive(t *testing.T, c *Coordinator, id SessionID, tech TechID) {
	t.Helper()
	done := make(chan struct{})
	if !c.Establish(id, tech, func() bool { close(done); return true }) {
		t.Fatalf("建立会话 %s 被拒绝", id)
	}
	<-done
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Snapshot().Sessions[id].State == StateActive {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("会话 %s 未在超时前进入活跃状态", id)
}

// TestRaceForLastSlot 多个建立与转交申请同时争抢某技师最后一个名额，
// 只能受理一个；被拒绝申请的准备函数不得执行。
func TestRaceForLastSlot(t *testing.T) {
	c := NewCoordinator(1)

	// 先让 A、B 各持有一个活跃会话，作为转交来源。
	establishActive(t, c, "sa", "A")
	establishActive(t, c, "sb", "B")

	var prepCalls atomic.Int64
	never := make(chan struct{}) // 永不关闭：被受理的准备函数一直阻塞
	prep := func() bool {
		prepCalls.Add(1)
		<-never
		return true
	}

	const contenders = 16
	type result struct {
		kind     string
		accepted bool
	}
	results := make(chan result, contenders*2)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			id := SessionID("new-" + string(rune('a'+i)))
			results <- result{"establish", c.Establish(id, "T", prep)}
		}(i)
		go func() {
			defer wg.Done()
			_, ok := c.Transfer("sa", "T", prep)
			results <- result{"transfer", ok}
		}()
	}
	wg.Wait()
	close(results)

	accepted := 0
	for r := range results {
		if r.accepted {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("最后一个名额被受理 %d 次，应为 1", accepted)
	}
	// 只会有一个准备函数真正执行（被受理的那个），且它仍在阻塞。
	if n := prepCalls.Load(); n != 1 {
		t.Fatalf("准备函数执行了 %d 次，应为 1（被拒绝的申请不得执行）", n)
	}
	snap := c.Snapshot()
	if snap.TechUsed["T"] != 1 {
		t.Fatalf("T 已用名额 = %d，应为 1", snap.TechUsed["T"])
	}
	checkConsistency(t, snap)
}

// TestCancelThenRetrySameTarget 撤回后立即向同一目标重试；
// 旧准备结果与重复撤回都不得影响新申请。
func TestCancelThenRetrySameTarget(t *testing.T) {
	c := NewCoordinator(2)
	establishActive(t, c, "s1", "A")

	gate1 := make(chan struct{})
	started1 := make(chan struct{})
	tid1, ok := c.Transfer("s1", "B", blockingPrep(started1, gate1, true))
	if !ok {
		t.Fatal("首次转交未受理")
	}
	<-started1 // 确保旧准备函数已在执行，之后才会产生迟到结果

	c.CancelTransfer("s1", tid1)
	c.CancelTransfer("s1", tid1) // 重复撤回无副作用

	gate2 := make(chan struct{})
	started2 := make(chan struct{})
	tid2, ok := c.Transfer("s1", "B", blockingPrep(started2, gate2, true))
	if !ok {
		t.Fatal("撤回后立即向同一目标重试未受理")
	}
	if tid2 == tid1 {
		t.Fatal("两次受理的转交编号必须不同")
	}
	<-started2

	// 旧申请在新申请待决期间返回成功：不得兑现。
	close(gate1)
	time.Sleep(50 * time.Millisecond)
	snap := c.Snapshot()
	s := snap.Sessions["s1"]
	if s.Owner != "A" || s.Transfer == nil || s.Transfer.ID != tid2 {
		t.Fatalf("旧结果影响了新申请: %+v", s)
	}
	if snap.TechUsed["B"] != 1 || snap.TechUsed["A"] != 1 {
		t.Fatalf("名额异常: %+v", snap.TechUsed)
	}

	// 再次重复撤回旧编号：无副作用。
	c.CancelTransfer("s1", tid1)
	if c.Snapshot().Sessions["s1"].Transfer == nil {
		t.Fatal("重复撤回旧编号撤销了新申请")
	}

	// 新申请成功：归属切换、名额同时生效。
	close(gate2)
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap = c.Snapshot()
		if snap.Sessions["s1"].Owner == "B" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("新转交未在超时前生效")
		}
		time.Sleep(time.Millisecond)
	}
	if snap.Sessions["s1"].Transfer != nil {
		t.Fatal("转交完成后待决申请未清除")
	}
	if snap.TechUsed["A"] != 0 || snap.TechUsed["B"] != 1 {
		t.Fatalf("转交完成后名额错误: %+v", snap.TechUsed)
	}
	checkConsistency(t, snap)
}

// TestLateFailureDoesNotAffectRetry 旧申请失败结果在新申请完成后到达，不得改变归属。
func TestLateFailureDoesNotAffectRetry(t *testing.T) {
	c := NewCoordinator(2)
	establishActive(t, c, "s1", "A")

	gate1 := make(chan struct{})
	tid1, ok := c.Transfer("s1", "B", blockingPrep(nil, gate1, false))
	if !ok {
		t.Fatal("首次转交未受理")
	}
	c.CancelTransfer("s1", tid1)

	gate2 := make(chan struct{})
	if _, ok := c.Transfer("s1", "B", blockingPrep(nil, gate2, true)); !ok {
		t.Fatal("重试转交未受理")
	}
	close(gate2) // 新申请先成功
	deadline := time.Now().Add(2 * time.Second)
	for c.Snapshot().Sessions["s1"].Owner != "B" {
		if time.Now().After(deadline) {
			t.Fatal("新转交未生效")
		}
		time.Sleep(time.Millisecond)
	}
	close(gate1) // 旧申请随后返回失败
	time.Sleep(50 * time.Millisecond)
	snap := c.Snapshot()
	if snap.Sessions["s1"].Owner != "B" || snap.TechUsed["B"] != 1 || snap.TechUsed["A"] != 0 {
		t.Fatalf("旧失败结果改变了新状态: %+v", snap)
	}
	checkConsistency(t, snap)
}

// TestEndDuringPending 待决期间结束会话，随后准备成功：
// 会话保持终态，名额不被再次扣减或释放；阻塞的准备函数不拖住任何操作。
func TestEndDuringPending(t *testing.T) {
	c := NewCoordinator(2)

	// 一个永不返回的准备函数，用于验证其他操作不被拖住。
	never := make(chan struct{})
	if !c.Establish("stuck", "A", blockingPrep(nil, never, true)) {
		t.Fatal("建立 stuck 未受理")
	}

	// 待决建立期间结束。
	gateEst := make(chan struct{})
	if !c.Establish("s1", "B", blockingPrep(nil, gateEst, true)) {
		t.Fatal("建立 s1 未受理")
	}
	c.EndSession("s1")
	c.EndSession("s1") // 重复结束无副作用

	// 待决转交期间结束。
	establishActive(t, c, "s2", "A")
	gateTr := make(chan struct{})
	if _, ok := c.Transfer("s2", "B", blockingPrep(nil, gateTr, true)); !ok {
		t.Fatal("转交 s2 未受理")
	}
	c.EndSession("s2")

	// 迟到的成功结果到达。
	close(gateEst)
	close(gateTr)
	time.Sleep(50 * time.Millisecond)

	snap := c.Snapshot()
	for _, id := range []SessionID{"s1", "s2"} {
		if snap.Sessions[id].State != StateEnded {
			t.Fatalf("会话 %s 应保持终态: %+v", id, snap.Sessions[id])
		}
	}
	if snap.TechUsed["B"] != 0 {
		t.Fatalf("B 的名额被迟到结果扣减: %+v", snap.TechUsed)
	}
	if snap.TechUsed["A"] != 1 { // 仅剩 stuck 的预占
		t.Fatalf("A 的名额错误: %+v", snap.TechUsed)
	}
	checkConsistency(t, snap)

	// 结束后的会话编号不能重新建立。
	if c.Establish("s1", "B", nil) {
		t.Fatal("已结束的会话编号不应允许重新建立")
	}

	// 阻塞准备函数存在时，所有操作须在 200ms 内返回。
	assertFast(t, func() { c.Establish("s3", "B", nil) })
	assertFast(t, func() { c.EndSession("s3") })
	assertFast(t, func() { c.CancelTransfer("s2", 999) })
	assertFast(t, func() { c.Snapshot() })
	assertFast(t, func() { c.EndSession("stuck") }) // 对阻塞申请的结束也须及时返回
}

func assertFast(t *testing.T, op func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { op(); close(done) }()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("操作被阻塞的准备函数拖住，超过 200ms")
	}
}

// TestEstablishFailureReleasesSlot 建立失败释放名额，允许重新申请。
func TestEstablishFailureReleasesSlot(t *testing.T) {
	c := NewCoordinator(1)
	done := make(chan struct{})
	if !c.Establish("s1", "A", func() bool { close(done); return false }) {
		t.Fatal("首次建立未受理")
	}
	<-done
	deadline := time.Now().Add(2 * time.Second)
	for c.Snapshot().TechUsed["A"] != 0 {
		if time.Now().After(deadline) {
			t.Fatal("失败后名额未释放")
		}
		time.Sleep(time.Millisecond)
	}
	establishActive(t, c, "s2", "A") // 名额已释放，可重新申请
	checkConsistency(t, c.Snapshot())
}

// TestTransferValidation 转交目标必须不同于当前技师，且同一会话仅允许一个待决转交。
func TestTransferValidation(t *testing.T) {
	c := NewCoordinator(3)
	establishActive(t, c, "s1", "A")

	if _, ok := c.Transfer("s1", "A", nil); ok {
		t.Fatal("转交给当前技师应被拒绝")
	}
	if _, ok := c.Transfer("missing", "B", nil); ok {
		t.Fatal("转交不存在的会话应被拒绝")
	}
	never := make(chan struct{})
	if _, ok := c.Transfer("s1", "B", blockingPrep(nil, never, true)); !ok {
		t.Fatal("首次转交应受理")
	}
	if _, ok := c.Transfer("s1", "C", nil); ok {
		t.Fatal("已存在待决转交时应拒绝第二个申请")
	}
	snap := c.Snapshot()
	if snap.TechUsed["B"] != 1 || snap.TechUsed["A"] != 1 {
		t.Fatalf("待决转交期间名额错误: %+v", snap.TechUsed)
	}
	checkConsistency(t, snap)
}

// TestConcurrentStress 64 个并发调用混合执行，期间每次整体查看都必须自洽。
func TestConcurrentStress(t *testing.T) {
	c := NewCoordinator(3)
	stop := make(chan struct{})
	var checkers sync.WaitGroup

	// 持续整体查看并核对一致性。
	for i := 0; i < 8; i++ {
		checkers.Add(1)
		go func() {
			defer checkers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := c.Snapshot()
				want := make(map[TechID]int)
				for _, s := range snap.Sessions {
					if s.State == StateEnded {
						continue
					}
					want[s.Owner]++
					if s.Transfer != nil {
						want[s.Transfer.Target]++
					}
				}
				for tech, n := range want {
					if snap.TechUsed[tech] != n {
						t.Errorf("快照不自洽: 技师 %s got %d want %d", tech, snap.TechUsed[tech], n)
						return
					}
				}
			}
		}()
	}

	// 混合操作：建立、转交、撤回、结束。
	var workers sync.WaitGroup
	for i := 0; i < 56; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			id := SessionID("sess-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26)))
			tech := TechID("tech-" + string(rune('a'+i%8)))
			gate := make(chan struct{})
			if c.Establish(id, tech, blockingPrep(nil, gate, i%3 != 0)) {
				close(gate)
			}
			tid, ok := c.Transfer(id, "tech-x", nil)
			if ok {
				if i%2 == 0 {
					c.CancelTransfer(id, tid)
				}
			}
			if i%4 == 0 {
				c.EndSession(id)
			}
		}(i)
	}
	workers.Wait()
	close(stop)
	checkers.Wait()
	checkConsistency(t, c.Snapshot())
}
