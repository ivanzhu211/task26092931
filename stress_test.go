package deskbridge

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// TestConcurrentStress 64 个并发调用者随机执行建立/转交/撤回/结束/查看，
// 每次快照都必须能独立核对占额。
func TestConcurrentStress(t *testing.T) {
	c := NewCoordinator()
	const workers = 64
	const opsPerWorker = 200
	const techs = 20

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerWorker; i++ {
				id := fmt.Sprintf("s-%d", rng.Intn(400))
				tech := fmt.Sprintf("t-%d", rng.Intn(techs))
				target := fmt.Sprintf("t-%d", rng.Intn(techs))
				switch rng.Intn(5) {
				case 0:
					_ = c.Establish(id, tech, instantPrep(rng.Intn(2) == 0, nil))
				case 1:
					tid, err := c.Transfer(id, target, instantPrep(rng.Intn(2) == 0, nil))
					if err == nil && rng.Intn(2) == 0 {
						c.CancelTransfer(id, tid)
					}
				case 2:
					c.CancelTransfer(id, uint64(rng.Intn(1000)))
				case 3:
					c.EndSession(id)
				case 4:
					verifySnapshot(t, c.Snapshot())
				}
			}
		}(int64(w) + 1)
	}
	wg.Wait()
	verifySnapshot(t, c.Snapshot())
}
