// Package deskbridge 提供远程支持会话的进程内协调能力。
package deskbridge

import (
	"errors"
	"sync"
)

// MaxSessionsPerTech 是每名技师可同时承担的名额上限。
const MaxSessionsPerTech = 3

var (
	ErrCapacityFull     = errors.New("deskbridge: 技师名额已满")
	ErrSessionExists    = errors.New("deskbridge: 会话编号已存在")
	ErrSessionEnded     = errors.New("deskbridge: 会话已结束，编号不可复用")
	ErrSessionNotFound  = errors.New("deskbridge: 会话不存在")
	ErrSessionNotActive = errors.New("deskbridge: 会话不在可转交状态")
	ErrSameTech         = errors.New("deskbridge: 转交目标不能是当前技师")
	ErrTransferPending  = errors.New("deskbridge: 会话已有待决转交申请")
)

// PrepareFunc 由调用方提供，仅返回成功或失败，可能长期阻塞。
type PrepareFunc func() bool

// SessionState 表示会话生命周期状态。
type SessionState int

const (
	StateEstablishing SessionState = iota // 建立申请已受理，准备中
	StateActive                           // 已正式归属某技师
	StateEnded                            // 终态
)

// TransferView 描述一次待决转交申请。
type TransferView struct {
	ID     uint64
	Target string
}

// SessionView 是单个会话的快照视图。
type SessionView struct {
	ID              string
	Owner           string
	State           SessionState
	PendingTransfer *TransferView
}

// Snapshot 是同一时刻的整体状态视图。
type Snapshot struct {
	Sessions  map[string]SessionView
	TechSlots map[string]int
}

type session struct {
	id       string
	owner    string
	state    SessionState
	transfer *pendingTransfer
}

type pendingTransfer struct {
	id     uint64
	target string
}

// Coordinator 协调会话建立、转交、撤回与结束，状态保存在进程内存中。
type Coordinator struct {
	mu           sync.Mutex
	sessions     map[string]*session
	ended        map[string]struct{}
	slots        map[string]int
	nextTransfer uint64
}

// NewCoordinator 创建一个空的协调器。
func NewCoordinator() *Coordinator {
	return &Coordinator{
		sessions: make(map[string]*session),
		ended:    make(map[string]struct{}),
		slots:    make(map[string]int),
	}
}

// Establish 受理建立申请：预占技师名额并异步执行准备函数。
func (c *Coordinator) Establish(sessionID, techID string, prep PrepareFunc) error {
	c.mu.Lock()
	if _, ok := c.sessions[sessionID]; ok {
		c.mu.Unlock()
		return ErrSessionExists
	}
	if _, ok := c.ended[sessionID]; ok {
		c.mu.Unlock()
		return ErrSessionEnded
	}
	if c.slots[techID] >= MaxSessionsPerTech {
		c.mu.Unlock()
		return ErrCapacityFull
	}
	c.slots[techID]++
	c.sessions[sessionID] = &session{id: sessionID, owner: techID, state: StateEstablishing}
	c.mu.Unlock()

	go c.applyEstablish(sessionID, prep)
	return nil
}

func (c *Coordinator) applyEstablish(sessionID string, prep PrepareFunc) {
	ok := prep()
	c.mu.Lock()
	defer c.mu.Unlock()
	s, found := c.sessions[sessionID]
	if !found || s.state != StateEstablishing {
		return
	}
	if ok {
		s.state = StateActive
		return
	}
	c.slots[s.owner]--
	delete(c.sessions, sessionID)
}

// Transfer 受理转交申请：预占目标技师名额并异步执行准备函数。
// 返回本次转交的申请编号，用于撤回。
func (c *Coordinator) Transfer(sessionID, targetTech string, prep PrepareFunc) (uint64, error) {
	c.mu.Lock()
	s, found := c.sessions[sessionID]
	if !found {
		c.mu.Unlock()
		return 0, ErrSessionNotFound
	}
	if s.state != StateActive {
		c.mu.Unlock()
		return 0, ErrSessionNotActive
	}
	if targetTech == s.owner {
		c.mu.Unlock()
		return 0, ErrSameTech
	}
	if s.transfer != nil {
		c.mu.Unlock()
		return 0, ErrTransferPending
	}
	if c.slots[targetTech] >= MaxSessionsPerTech {
		c.mu.Unlock()
		return 0, ErrCapacityFull
	}
	c.slots[targetTech]++
	c.nextTransfer++
	transferID := c.nextTransfer
	s.transfer = &pendingTransfer{id: transferID, target: targetTech}
	c.mu.Unlock()

	go c.applyTransfer(sessionID, transferID, prep)
	return transferID, nil
}

func (c *Coordinator) applyTransfer(sessionID string, transferID uint64, prep PrepareFunc) {
	ok := prep()
	c.mu.Lock()
	defer c.mu.Unlock()
	s, found := c.sessions[sessionID]
	if !found || s.transfer == nil || s.transfer.id != transferID {
		return
	}
	target := s.transfer.target
	s.transfer = nil
	if ok {
		c.slots[s.owner]--
		s.owner = target
		return
	}
	c.slots[target]--
}

// CancelTransfer 撤回指定转交申请；重复撤回或撤回已完成申请均无副作用。
func (c *Coordinator) CancelTransfer(sessionID string, transferID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, found := c.sessions[sessionID]
	if !found || s.transfer == nil || s.transfer.id != transferID {
		return
	}
	c.slots[s.transfer.target]--
	s.transfer = nil
}

// EndSession 立即结束会话，释放正式占用与全部有效预占。
func (c *Coordinator) EndSession(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, found := c.sessions[sessionID]
	if !found {
		return
	}
	c.slots[s.owner]--
	if s.transfer != nil {
		c.slots[s.transfer.target]--
	}
	delete(c.sessions, sessionID)
	c.ended[sessionID] = struct{}{}
}

// Snapshot 返回同一时刻的整体状态视图。
func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := Snapshot{
		Sessions:  make(map[string]SessionView, len(c.sessions)),
		TechSlots: make(map[string]int),
	}
	for id, s := range c.sessions {
		view := SessionView{ID: id, Owner: s.owner, State: s.state}
		snap.TechSlots[s.owner]++
		if s.transfer != nil {
			view.PendingTransfer = &TransferView{ID: s.transfer.id, Target: s.transfer.target}
			snap.TechSlots[s.transfer.target]++
		}
		snap.Sessions[id] = view
	}
	return snap
}
