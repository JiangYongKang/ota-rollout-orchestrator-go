// Package ota 提供本地可运行的设备固件灰度升级编排能力。
package ota

import "time"

// TaskStatus 升级任务状态。
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"   // 已创建，尚未开始推送
	TaskRolling   TaskStatus = "rolling"   // 灰度推进中
	TaskPaused    TaskStatus = "paused"    // 人工或健康判定暂停
	TaskAborted   TaskStatus = "aborted"   // 已终止（终态）
	TaskCompleted TaskStatus = "completed" // 全部批次完成（终态）
)

// DeviceStatus 设备在某一任务中的升级状态。
type DeviceStatus string

const (
	DevicePending     DeviceStatus = "pending"     // 等待下发
	DeviceDelivered   DeviceStatus = "delivered"   // 已下发，等待上报
	DeviceSucceeded   DeviceStatus = "succeeded"   // 升级成功（终态，不可回退）
	DeviceFailed      DeviceStatus = "failed"      // 升级失败，待回滚
	DeviceRolledBack  DeviceStatus = "rolled_back" // 已回滚到已知可用版本（终态）
	DeviceQuarantined DeviceStatus = "quarantined" // 异常/离线上报缺失，隔离待人工处理
)

// Terminal 判断设备状态是否为终态（终态结果不得被后续上报改写）。
func (s DeviceStatus) Terminal() bool {
	switch s {
	case DeviceSucceeded, DeviceRolledBack, DeviceQuarantined:
		return true
	}
	return false
}

// ReportType 设备上报类型。
type ReportType string

const (
	ReportAck     ReportType = "ack"     // 收到下发
	ReportSuccess ReportType = "success" // 升级成功
	ReportFailure ReportType = "failure" // 升级失败
	ReportOffline ReportType = "offline" // 设备离线宣告
	ReportOnline  ReportType = "online"  // 设备重连宣告
)

// Device 设备记录。
type Device struct {
	ID             string       `json:"id"`
	CurrentVersion string       `json:"current_version"` // 当前固件版本（已确认）
	Online         bool         `json:"online"`
	Status         DeviceStatus `json:"status"`
	BatchIndex     int          `json:"batch_index"` // 所属批次，-1 表示未分配
	TargetVersion  string       `json:"target_version,omitempty"`
	LastSeq        uint64       `json:"last_seq"` // 已处理的最大上报序号，用于乱序去重
	FailReason     string       `json:"fail_reason,omitempty"`
}

// 判定触发来源。
const (
	TriggerManual = "manual" // 人工（手动 Advance）
	TriggerAuto   = "auto"   // 观察期到点自动调度
)

// Batch 一个灰度批次。
type Batch struct {
	Index     int      `json:"index"`
	DeviceIDs []string `json:"device_ids"`
	Started   bool     `json:"started"`
	Healthy   bool     `json:"healthy"` // 最近一次健康判定结果

	// 观察期与一次性判定保护。零值字段对历史任务天然兼容。
	ObserveDeadline *time.Time `json:"observe_deadline,omitempty"` // 当前观察期到点时刻；nil 表示无观察期或不在观察
	Decided         bool       `json:"decided"`                    // 本批是否已形成确定结论（放量/暂停），仅允许一次
	DecidedAt       *time.Time `json:"decided_at,omitempty"`
	Trigger         string     `json:"trigger,omitempty"` // 形成结论的触发来源：manual / auto
}

// Task 升级任务。
type Task struct {
	ID             string     `json:"id"`
	TargetVersion  string     `json:"target_version"`
	Status         TaskStatus `json:"status"`
	Batches        []Batch    `json:"batches"`
	CurrentBatch   int        `json:"current_batch"`    // 当前推进到的批次
	MinSuccessRate float64    `json:"min_success_rate"` // 健康判定：批次成功率阈值
	CreatedAt      time.Time  `json:"created_at"`

	// ObservePeriod 每批下发后的观察期时长；0 表示不启用自动调度，保持手动推进。
	ObservePeriod time.Duration `json:"observe_period,omitempty"`
}

// Report 设备上报消息。
type Report struct {
	DeviceID string     `json:"device_id"`
	TaskID   string     `json:"task_id"`
	Seq      uint64     `json:"seq"` // 设备侧单调递增序号
	Type     ReportType `json:"type"`
	Version  string     `json:"version,omitempty"` // 上报时设备实际版本
	Reason   string     `json:"reason,omitempty"`  // 失败原因
}

// Event 状态变更事件，用于追溯。
type Event struct {
	At       time.Time `json:"at"`
	TaskID   string    `json:"task_id"`
	DeviceID string    `json:"device_id,omitempty"`
	Kind     string    `json:"kind"`
	Detail   string    `json:"detail"`
}
