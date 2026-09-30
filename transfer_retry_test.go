package deskbridge

import (
	"sync/atomic"
	"testing"
)

// TestCancelRetryLateResult 撤回后立即向同一目标重试；
// 旧准备结果在新申请待决期间到达，不得影响新申请。
func TestCancelRetryLateResult(t *testing.T) {
	c := NewCoordinator()
	establishAndWait(t, c, "s1", "techA")

	var oldExec, newExec atomic.Int32
	oldPrep, oldRelease := gatePrep(&oldExec)
	newPrep, newRelease := gatePrep(&newExec)

	oldID, err := c.Transfer("s1", "techB", oldPrep)
	if err != nil {
		t.Fatalf("首次转交被拒绝: %v", err)
	}
	waitFor(t, "旧准备函数开始执行", func() bool { return oldExec.Load() == 1 })

	c.CancelTransfer("s1", oldID)
	// 撤回后目标预占立即释放。
	verifySnapshot(t, c.Snapshot())
	if got := c.Snapshot().TechSlots["techB"]; got != 0 {
		t.Fatalf("撤回后 techB 占额 = %d，应为 0", got)
	}
	// 重复撤回无副作用。
	c.CancelTransfer("s1", oldID)
	verifySnapshot(t, c.Snapshot())

	// 立即向同一目标重试。
	newID, err := c.Transfer("s1", "techB", newPrep)
	if err != nil {
		t.Fatalf("重试转交被拒绝: %v", err)
	}
	waitFor(t, "新准备函数开始执行", func() bool { return newExec.Load() == 1 })

	// 旧准备结果（成功）在新申请待决期间到达：不得兑现。
	oldRelease <- true
	waitFor(t, "旧结果已处理", func() bool {
		snap := c.Snapshot()
		return snap.TechSlots["techA"] == 1 && snap.TechSlots["techB"] == 1
	})
	snap := c.Snapshot()
	verifySnapshot(t, snap)
	s1 := snap.Sessions["s1"]
	if s1.Owner != "techA" {
		t.Fatalf("旧结果兑现了：归属变为 %s", s1.Owner)
	}
	if s1.PendingTransfer == nil || s1.PendingTransfer.ID != newID {
		t.Fatalf("新申请被旧结果破坏: %+v", s1.PendingTransfer)
	}

	// 新申请准备成功：归属切换、名额同步。
	newRelease <- true
	waitFor(t, "归属切换到 techB", func() bool {
		s, ok := c.Snapshot().Sessions["s1"]
		return ok && s.Owner == "techB" && s.State == StateActive
	})
	snap = c.Snapshot()
	verifySnapshot(t, snap)
	if snap.TechSlots["techA"] != 0 || snap.TechSlots["techB"] != 1 {
		t.Fatalf("切换后占额错误: techA=%d techB=%d", snap.TechSlots["techA"], snap.TechSlots["techB"])
	}
	// 撤回已完成的申请无副作用。
	c.CancelTransfer("s1", oldID)
	c.CancelTransfer("s1", newID)
	snap = c.Snapshot()
	verifySnapshot(t, snap)
	if snap.Sessions["s1"].Owner != "techB" {
		t.Fatalf("重复撤回改变了归属: %s", snap.Sessions["s1"].Owner)
	}
}

// TestCancelRetryOldFailsLate 旧准备结果（失败）在新申请完成后到达，不得影响归属。
func TestCancelRetryOldFailsLate(t *testing.T) {
	c := NewCoordinator()
	establishAndWait(t, c, "s2", "techA")

	oldPrep, oldRelease := gatePrep(nil)
	oldID, err := c.Transfer("s2", "techB", oldPrep)
	if err != nil {
		t.Fatalf("首次转交被拒绝: %v", err)
	}
	c.CancelTransfer("s2", oldID)

	newPrep, newRelease := gatePrep(nil)
	if _, err := c.Transfer("s2", "techB", newPrep); err != nil {
		t.Fatalf("重试转交被拒绝: %v", err)
	}
	newRelease <- true
	waitFor(t, "归属切换到 techB", func() bool {
		s, ok := c.Snapshot().Sessions["s2"]
		return ok && s.Owner == "techB"
	})

	// 旧申请迟到失败：不得撤销新归属或改动名额。
	oldRelease <- false
	snap := c.Snapshot()
	verifySnapshot(t, snap)
	if snap.Sessions["s2"].Owner != "techB" {
		t.Fatalf("迟到的旧失败结果改变了归属: %s", snap.Sessions["s2"].Owner)
	}
	if snap.TechSlots["techB"] != 1 || snap.TechSlots["techA"] != 0 {
		t.Fatalf("迟到的旧失败结果改变了名额: techA=%d techB=%d",
			snap.TechSlots["techA"], snap.TechSlots["techB"])
	}
}

// TestSinglePendingTransfer 同一会话同时只允许一个待决转交。
func TestSinglePendingTransfer(t *testing.T) {
	c := NewCoordinator()
	establishAndWait(t, c, "s3", "techA")

	prep, release := gatePrep(nil)
	if _, err := c.Transfer("s3", "techB", prep); err != nil {
		t.Fatalf("首次转交被拒绝: %v", err)
	}
	var secondExec atomic.Int32
	if _, err := c.Transfer("s3", "techC", instantPrep(true, &secondExec)); err != ErrTransferPending {
		t.Fatalf("并发第二个转交应返回 ErrTransferPending，实际: %v", err)
	}
	if n := secondExec.Load(); n != 0 {
		t.Fatalf("被拒绝的转交执行了准备函数 %d 次", n)
	}
	release <- false
	waitFor(t, "转交失败释放预占", func() bool {
		return c.Snapshot().Sessions["s3"].PendingTransfer == nil
	})
	verifySnapshot(t, c.Snapshot())
}
