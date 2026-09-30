package deskbridge

import "sync"

// DefaultCapacity 是每名技师最多同时承担的会话名额。
const DefaultCapacity = 3

// Coordinator 协调远程支持会话的建立、转交、撤回与结束。
// 状态全部保存在进程内存中，由上层直接调用，并发安全。
type Coordinator struct {
	mu       sync.Mutex
	capacity int
	sessions map[SessionID]*session
	nextTID  TransferID
}

// session 是会话的内部状态。
type session struct {
	id       SessionID
	owner    TechID
	state    SessionState
	transfer *transfer
}

// transfer 是一次待决转交申请的内部状态。
type transfer struct {
	id     TransferID
	target TechID
}

// NewCoordinator 创建协调器，capacity 为每名技师的最大会话名额。
// capacity <= 0 时使用 DefaultCapacity。
func NewCoordinator(capacity int) *Coordinator {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Coordinator{
		capacity: capacity,
		sessions: make(map[SessionID]*session),
	}
}

// usedLocked 统计技师当前占用的名额：进行中的会话、待决建立与待决转入。
// 调用前必须持有 c.mu。
func (c *Coordinator) usedLocked(tech TechID) int {
	used := 0
	for _, s := range c.sessions {
		if s.state == StateEnded {
			continue
		}
		if s.owner == tech {
			used++
		}
		if s.transfer != nil && s.transfer.target == tech {
			used++
		}
	}
	return used
}

// Establish 申请建立会话。受理时预占技师一个名额并异步执行准备函数；
// 容量不足或会话编号已存在（含已结束）时立即拒绝，且不执行准备函数。
// 返回 true 表示申请已受理，调用方无需等待准备完成。
func (c *Coordinator) Establish(id SessionID, tech TechID, prep PrepareFunc) bool {
	c.mu.Lock()
	if _, exists := c.sessions[id]; exists {
		c.mu.Unlock()
		return false
	}
	if c.usedLocked(tech) >= c.capacity {
		c.mu.Unlock()
		return false
	}
	c.sessions[id] = &session{id: id, owner: tech, state: StatePendingEstablish}
	c.mu.Unlock()

	go func() {
		ok := runPrepare(prep)
		c.mu.Lock()
		defer c.mu.Unlock()
		s, exists := c.sessions[id]
		if !exists || s.state != StatePendingEstablish {
			return // 迟到结果：会话已结束或已归属，忽略
		}
		if ok {
			s.state = StateActive
		} else {
			delete(c.sessions, id) // 释放预占，允许重新申请
		}
	}()
	return true
}

// Transfer 申请将活跃会话转交给目标技师。目标必须不同于当前技师，
// 且同一会话只能有一个待决转交申请。受理时预占目标技师一个名额，
// 原归属保持不变。返回转交编号与是否受理。
func (c *Coordinator) Transfer(id SessionID, target TechID, prep PrepareFunc) (TransferID, bool) {
	c.mu.Lock()
	s, exists := c.sessions[id]
	if !exists || s.state != StateActive || s.transfer != nil || target == s.owner {
		c.mu.Unlock()
		return 0, false
	}
	if c.usedLocked(target) >= c.capacity {
		c.mu.Unlock()
		return 0, false
	}
	c.nextTID++
	tid := c.nextTID
	s.transfer = &transfer{id: tid, target: target}
	c.mu.Unlock()

	go func() {
		ok := runPrepare(prep)
		c.mu.Lock()
		defer c.mu.Unlock()
		cur, exists := c.sessions[id]
		if !exists || cur.state != StateActive || cur.transfer == nil || cur.transfer.id != tid {
			return // 迟到结果：申请已撤回、被取代或会话已结束，忽略
		}
		cur.transfer = nil
		if ok {
			cur.owner = target // 归属切换、原名额释放、目标预占转正同时生效
		}
	}()
	return tid, true
}

// CancelTransfer 撤回一次待决转交，立即释放该次目标预占，原归属不变。
// 重复撤回、撤回已完成或不存在的申请均无副作用。
func (c *Coordinator) CancelTransfer(id SessionID, tid TransferID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, exists := c.sessions[id]
	if !exists || s.transfer == nil || s.transfer.id != tid {
		return
	}
	s.transfer = nil
}

// EndSession 结束会话，立即成为终态并释放其正式占用和全部有效预占。
// 重复结束无副作用；之后到达的准备结果不改变任何状态。
func (c *Coordinator) EndSession(id SessionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, exists := c.sessions[id]
	if !exists || s.state == StateEnded {
		return
	}
	s.state = StateEnded
	s.transfer = nil
}

// Snapshot 返回同一时刻的整体状态视图：会话归属、待决申请与各技师已用名额。
// 名额由会话与有效预占逐项重新统计，与会话视图严格一致。
func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := Snapshot{
		Sessions: make(map[SessionID]SessionView, len(c.sessions)),
		TechUsed: make(map[TechID]int),
	}
	for _, s := range c.sessions {
		view := SessionView{ID: s.id, Owner: s.owner, State: s.state}
		if s.transfer != nil {
			view.Transfer = &TransferView{ID: s.transfer.id, Target: s.transfer.target}
		}
		snap.Sessions[s.id] = view
		if s.state == StateEnded {
			continue
		}
		snap.TechUsed[s.owner]++
		if s.transfer != nil {
			snap.TechUsed[s.transfer.target]++
		}
	}
	return snap
}

// runPrepare 执行准备函数；nil 视为立即成功。
func runPrepare(prep PrepareFunc) bool {
	if prep == nil {
		return true
	}
	return prep()
}
