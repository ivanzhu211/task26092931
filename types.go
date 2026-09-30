package deskbridge

// TechID 标识一名技师。
type TechID string

// SessionID 标识一个远程支持会话。
type SessionID string

// TransferID 标识一次已受理的转交申请，用于撤回转交。
type TransferID uint64

// PrepareFunc 是调用方提供的同进程准备函数，仅返回成功或失败，
// 不修改库内状态，可能长期不返回。
type PrepareFunc func() bool

// SessionState 表示会话生命周期状态。
type SessionState int

const (
	// StatePendingEstablish 建立申请已受理，名额已预占，准备函数尚未完成。
	StatePendingEstablish SessionState = iota
	// StateActive 会话已正式归属当前技师。
	StateActive
	// StateEnded 会话已结束，终态。
	StateEnded
)

// TransferView 是整体查看中一次待决转交申请的只读视图。
type TransferView struct {
	ID     TransferID
	Target TechID
}

// SessionView 是整体查看中单个会话的只读视图。
type SessionView struct {
	ID       SessionID
	Owner    TechID
	State    SessionState
	Transfer *TransferView // 非空表示存在待决转交申请
}

// Snapshot 是同一时刻的整体状态视图：会话归属、待决申请与各技师已用名额。
// TechUsed 中的名额与 Sessions 中的会话及有效预占逐项统计结果一致。
type Snapshot struct {
	Sessions map[SessionID]SessionView
	TechUsed map[TechID]int
}
