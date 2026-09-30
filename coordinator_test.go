package deskbridge

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gatePrep 返回一个可控的准备函数：调用次数计入 executed，
// 阻塞直到调用方通过 release 发送结果。
func gatePrep(executed *atomic.Int32) (PrepareFunc, chan<- bool) {
	release := make(chan bool, 1)
	return func() bool {
		if executed != nil {
			executed.Add(1)
		}
		return <-release
	}, release
}

// instantPrep 返回立即出结果的准备函数。
func instantPrep(ok bool, executed *atomic.Int32) PrepareFunc {
	return func() bool {
		if executed != nil {
			executed.Add(1)
		}
		return ok
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// verifySnapshot 独立逐项重统名额并与快照中的名额比对。
func verifySnapshot(t *testing.T, snap Snapshot) {
	t.Helper()
	recount := make(map[string]int)
	for id, s := range snap.Sessions {
		if s.State == StateEnded {
			t.Fatalf("快照中出现终态会话 %s", id)
		}
		recount[s.Owner]++
		if s.PendingTransfer != nil {
			recount[s.PendingTransfer.Target]++
		}
	}
	for tech, used := range snap.TechSlots {
		if used > MaxSessionsPerTech {
			t.Fatalf("技师 %s 占额 %d 超过上限", tech, used)
		}
		if recount[tech] != used {
			t.Fatalf("技师 %s 快照占额 %d 与重统 %d 不一致", tech, used, recount[tech])
		}
	}
	for tech, used := range recount {
		if snap.TechSlots[tech] != used {
			t.Fatalf("技师 %s 重统占额 %d 未出现在快照中", tech, used)
		}
	}
}

// establishAndWait 建立会话并等待其进入 Active。
func establishAndWait(t *testing.T, c *Coordinator, id, tech string) {
	t.Helper()
	if err := c.Establish(id, tech, instantPrep(true, nil)); err != nil {
		t.Fatalf("建立 %s 被拒绝: %v", id, err)
	}
	waitFor(t, "会话 "+id+" 进入 Active", func() bool {
		s, ok := c.Snapshot().Sessions[id]
		return ok && s.State == StateActive
	})
}

// TestContendLastSlot 多个建立与转交申请同时争抢某技师最后一个名额，
// 只能受理一个，被拒绝申请的准备函数不得执行。
func TestContendLastSlot(t *testing.T) {
	c := NewCoordinator()
	establishAndWait(t, c, "seed-1", "techA")
	establishAndWait(t, c, "seed-2", "techA")
	// 另建一个属于 techB 的会话，用于发起向 techA 的转交。
	establishAndWait(t, c, "mover", "techB")

	const contenders = 8
	var executed atomic.Int32
	results := make([]error, contenders)
	var transferErr error
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = c.Establish(fmt.Sprintf("new-%d", i), "techA", instantPrep(true, &executed))
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, transferErr = c.Transfer("mover", "techA", instantPrep(true, &executed))
	}()
	wg.Wait()

	accepted := 0
	for _, err := range results {
		if err == nil {
			accepted++
		} else if err != ErrCapacityFull {
			t.Fatalf("建立申请返回意外错误: %v", err)
		}
	}
	transferAccepted := transferErr == nil
	if transferErr != nil && transferErr != ErrCapacityFull {
		t.Fatalf("转交申请返回意外错误: %v", transferErr)
	}
	if got := accepted + boolToInt(transferAccepted); got != 1 {
		t.Fatalf("受理数 = %d，应为 1", got)
	}
	if n := executed.Load(); n != 1 {
		t.Fatalf("准备函数执行 %d 次，应为 1（被拒绝的申请不得执行）", n)
	}

	waitFor(t, "techA 占满 3 个名额", func() bool {
		return c.Snapshot().TechSlots["techA"] == MaxSessionsPerTech
	})
	snap := c.Snapshot()
	verifySnapshot(t, snap)
	if snap.TechSlots["techA"] != MaxSessionsPerTech {
		t.Fatalf("techA 占额 = %d，应为 %d", snap.TechSlots["techA"], MaxSessionsPerTech)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
