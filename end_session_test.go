package deskbridge

import (
	"sync/atomic"
	"testing"
	"time"
)

// assertFast 断言操作在 200ms 内返回。
func assertFast(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("%s 未在 200ms 内返回", what)
	}
}

// TestEndDuringPendingEstablish 建立待决期间结束会话，随后准备成功：
// 会话保持终态，名额不被再次扣减或复活。
func TestEndDuringPendingEstablish(t *testing.T) {
	c := NewCoordinator()
	prep, release := gatePrep(nil)
	if err := c.Establish("s1", "techA", prep); err != nil {
		t.Fatalf("建立被拒绝: %v", err)
	}
	if got := c.Snapshot().TechSlots["techA"]; got != 1 {
		t.Fatalf("受理后 techA 占额 = %d，应为 1", got)
	}

	assertFast(t, "待决期间结束会话", func() { c.EndSession("s1") })
	snap := c.Snapshot()
	verifySnapshot(t, snap)
	if _, ok := snap.Sessions["s1"]; ok {
		t.Fatal("结束后会话仍出现在快照中")
	}
	if got := snap.TechSlots["techA"]; got != 0 {
		t.Fatalf("结束后 techA 占额 = %d，应为 0", got)
	}

	// 准备随后成功：不得复活会话或改动名额。
	release <- true
	assertFast(t, "迟到结果后查看", func() { verifySnapshot(t, c.Snapshot()) })
	snap = c.Snapshot()
	if _, ok := snap.Sessions["s1"]; ok {
		t.Fatal("迟到的准备成功复活了已结束的会话")
	}
	if got := snap.TechSlots["techA"]; got != 0 {
		t.Fatalf("迟到结果后 techA 占额 = %d，应为 0", got)
	}

	// 结束后的编号不能重新建立。
	var exec atomic.Int32
	if err := c.Establish("s1", "techA", instantPrep(true, &exec)); err != ErrSessionEnded {
		t.Fatalf("重新建立应返回 ErrSessionEnded，实际: %v", err)
	}
	if n := exec.Load(); n != 0 {
		t.Fatalf("被拒绝的重新建立执行了准备函数 %d 次", n)
	}
	// 重复结束无副作用。
	c.EndSession("s1")
	verifySnapshot(t, c.Snapshot())
}

// TestEndDuringPendingTransfer 转交待决期间结束会话，随后准备成功：
// 会话保持终态，原技师与目标技师名额均正确释放且不被再次释放。
func TestEndDuringPendingTransfer(t *testing.T) {
	c := NewCoordinator()
	establishAndWait(t, c, "s2", "techA")
	establishAndWait(t, c, "other", "techA") // techA 现占 2 席

	prep, release := gatePrep(nil)
	transferID, err := c.Transfer("s2", "techB", prep)
	if err != nil {
		t.Fatalf("转交被拒绝: %v", err)
	}
	if got := c.Snapshot().TechSlots["techB"]; got != 1 {
		t.Fatalf("转交受理后 techB 占额 = %d，应为 1", got)
	}

	assertFast(t, "转交待决期间结束会话", func() { c.EndSession("s2") })
	snap := c.Snapshot()
	verifySnapshot(t, snap)
	if snap.TechSlots["techA"] != 1 || snap.TechSlots["techB"] != 0 {
		t.Fatalf("结束后占额错误: techA=%d(应1) techB=%d(应0)",
			snap.TechSlots["techA"], snap.TechSlots["techB"])
	}

	// 准备随后成功：不得复活会话、不得再次扣减 techA 或 techB。
	release <- true
	snap = c.Snapshot()
	verifySnapshot(t, snap)
	if _, ok := snap.Sessions["s2"]; ok {
		t.Fatal("迟到的转交成功复活了已结束的会话")
	}
	if snap.TechSlots["techA"] != 1 || snap.TechSlots["techB"] != 0 {
		t.Fatalf("迟到结果后占额错误: techA=%d(应1) techB=%d(应0)",
			snap.TechSlots["techA"], snap.TechSlots["techB"])
	}
	// 撤回已随结束失效的申请：无副作用。
	c.CancelTransfer("s2", transferID)
	verifySnapshot(t, c.Snapshot())
}

// TestBlockedPrepDoesNotStall 持续阻塞的准备函数不得拖住其他操作。
func TestBlockedPrepDoesNotStall(t *testing.T) {
	c := NewCoordinator()
	establishAndWait(t, c, "stuck", "techA")

	blocking := func() bool { select {} } // 永不返回
	transferID, err := c.Transfer("stuck", "techB", blocking)
	if err != nil {
		t.Fatalf("转交被拒绝: %v", err)
	}
	stuckEstablish := func() bool { select {} }
	if err := c.Establish("stuck-2", "techC", stuckEstablish); err != nil {
		t.Fatalf("建立被拒绝: %v", err)
	}

	// 对阻塞申请的撤回与结束必须在 200ms 内返回。
	assertFast(t, "撤回阻塞中的转交", func() { c.CancelTransfer("stuck", transferID) })
	assertFast(t, "结束建立阻塞中的会话", func() { c.EndSession("stuck-2") })

	// 其他会话的受理、结束与整体查看同样不受拖累。
	assertFast(t, "其他会话建立受理", func() {
		if err := c.Establish("fine", "techD", instantPrep(true, nil)); err != nil {
			t.Errorf("建立被拒绝: %v", err)
		}
	})
	assertFast(t, "整体查看", func() { verifySnapshot(t, c.Snapshot()) })
	assertFast(t, "结束其他会话", func() { c.EndSession("fine") })

	snap := c.Snapshot()
	verifySnapshot(t, snap)
	if got := snap.TechSlots["techB"]; got != 0 {
		t.Fatalf("撤回后 techB 占额 = %d，应为 0", got)
	}
	if got := snap.TechSlots["techC"]; got != 0 {
		t.Fatalf("结束后 techC 占额 = %d，应为 0", got)
	}
}

// TestEstablishFailureReleasesSlot 建立准备失败释放名额并允许重新申请。
func TestEstablishFailureReleasesSlot(t *testing.T) {
	c := NewCoordinator()
	if err := c.Establish("s3", "techA", instantPrep(false, nil)); err != nil {
		t.Fatalf("建立被拒绝: %v", err)
	}
	waitFor(t, "失败后释放名额", func() bool {
		snap := c.Snapshot()
		_, exists := snap.Sessions["s3"]
		return !exists && snap.TechSlots["techA"] == 0
	})
	// 允许以相同编号重新申请。
	establishAndWait(t, c, "s3", "techA")
	verifySnapshot(t, c.Snapshot())
}
