package ota

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Orchestrator 灰度升级编排器。所有状态变更串行化并持久化，保证并发安全与可追溯。
type Orchestrator struct {
	mu    sync.Mutex
	store *Store
	now   func() time.Time
}

// NewOrchestrator 创建编排器。
func NewOrchestrator(store *Store) *Orchestrator {
	return &Orchestrator{store: store, now: time.Now}
}

// CreateTask 创建升级任务：注册设备并按 batchSize 划分批次。
// devices 为 设备ID -> 当前已知可用版本。
func (o *Orchestrator) CreateTask(taskID, targetVersion string, devices map[string]string, batchSize int, minSuccessRate float64) (*Task, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if taskID == "" || targetVersion == "" {
		return nil, errors.New("taskID 与 targetVersion 不能为空")
	}
	if _, exists := o.store.Tasks[taskID]; exists {
		return nil, fmt.Errorf("任务 %s 已存在", taskID)
	}
	if batchSize <= 0 {
		return nil, errors.New("batchSize 必须为正")
	}
	if len(devices) == 0 {
		return nil, errors.New("设备列表不能为空")
	}
	ids := make([]string, 0, len(devices))
	for id := range devices {
		ids = append(ids, id)
	}
	sort.Strings(ids) // 确定性的批次划分

	task := &Task{
		ID:             taskID,
		TargetVersion:  targetVersion,
		Status:         TaskPending,
		CurrentBatch:   -1,
		MinSuccessRate: minSuccessRate,
		CreatedAt:      o.now(),
	}
	for i, id := range ids {
		batchIdx := i / batchSize
		for len(task.Batches) <= batchIdx {
			task.Batches = append(task.Batches, Batch{Index: len(task.Batches)})
		}
		task.Batches[batchIdx].DeviceIDs = append(task.Batches[batchIdx].DeviceIDs, id)
		o.store.Devices[id] = &Device{
			ID:             id,
			CurrentVersion: devices[id],
			Online:         true,
			Status:         DevicePending,
			BatchIndex:     batchIdx,
		}
	}
	o.store.Tasks[taskID] = task
	o.eventLocked(taskID, "", "task_created",
		fmt.Sprintf("target=%s devices=%d batches=%d minSuccessRate=%.2f", targetVersion, len(ids), len(task.Batches), minSuccessRate))
	if err := o.store.Save(); err != nil {
		return nil, err
	}
	return task, nil
}

// Start 开始任务，下发第一个批次。
func (o *Orchestrator) Start(taskID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, err := o.taskLocked(taskID)
	if err != nil {
		return err
	}
	if task.Status != TaskPending && task.Status != TaskPaused {
		return fmt.Errorf("任务状态 %s 不允许启动", task.Status)
	}
	task.Status = TaskRolling
	next := task.CurrentBatch + 1
	if next >= len(task.Batches) {
		return errors.New("没有可下发的批次")
	}
	o.deliverBatchLocked(task, next)
	return o.store.Save()
}

// Advance 对当前批次做健康判定：仅 succeeded 计入成功；
// 失败设备回滚、离线上报缺失设备隔离；满足阈值则放量进入下一批，否则暂停任务。
func (o *Orchestrator) Advance(taskID string) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, err := o.taskLocked(taskID)
	if err != nil {
		return false, err
	}
	if task.Status != TaskRolling {
		return false, fmt.Errorf("任务状态 %s 不允许推进", task.Status)
	}
	if task.CurrentBatch < 0 {
		return false, errors.New("尚未下发任何批次")
	}
	batch := &task.Batches[task.CurrentBatch]
	succeeded := 0
	for _, id := range batch.DeviceIDs {
		dev := o.store.Devices[id]
		switch dev.Status {
		case DeviceSucceeded:
			succeeded++
		case DeviceFailed:
			// 确定策略：失败设备回滚到已知可用版本
			o.rollbackLocked(task, dev, "批次健康判定：升级失败，执行回滚")
		default:
			// delivered/pending：离线上报缺失，隔离待人工处理，不计入成功
			dev.Status = DeviceQuarantined
			o.eventLocked(taskID, id, "device_quarantined",
				fmt.Sprintf("batch=%d 上报缺失/异常，不计入成功", batch.Index))
		}
	}
	rate := float64(succeeded) / float64(len(batch.DeviceIDs))
	batch.Healthy = rate >= task.MinSuccessRate
	o.eventLocked(taskID, "", "batch_health_check",
		fmt.Sprintf("batch=%d succeeded=%d/%d rate=%.2f threshold=%.2f healthy=%v",
			batch.Index, succeeded, len(batch.DeviceIDs), rate, task.MinSuccessRate, batch.Healthy))
	if !batch.Healthy {
		task.Status = TaskPaused
		o.eventLocked(taskID, "", "task_paused", "健康判定未通过，自动暂停")
		return false, o.store.Save()
	}
	if task.CurrentBatch+1 >= len(task.Batches) {
		task.Status = TaskCompleted
		o.eventLocked(taskID, "", "task_completed", "全部批次完成")
		return true, o.store.Save()
	}
	o.deliverBatchLocked(task, task.CurrentBatch+1)
	return true, o.store.Save()
}

// Pause 暂停任务推进。
func (o *Orchestrator) Pause(taskID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, err := o.taskLocked(taskID)
	if err != nil {
		return err
	}
	if task.Status != TaskRolling {
		return fmt.Errorf("任务状态 %s 不允许暂停", task.Status)
	}
	task.Status = TaskPaused
	o.eventLocked(taskID, "", "task_paused", "人工暂停")
	return o.store.Save()
}

// Resume 恢复已暂停的任务（不自动放量，需再次 Advance）。
func (o *Orchestrator) Resume(taskID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, err := o.taskLocked(taskID)
	if err != nil {
		return err
	}
	if task.Status != TaskPaused {
		return fmt.Errorf("任务状态 %s 不允许恢复", task.Status)
	}
	task.Status = TaskRolling
	o.eventLocked(taskID, "", "task_resumed", "人工恢复")
	return o.store.Save()
}

// Abort 终止任务（终态）：失败设备回滚，未完成设备隔离。
func (o *Orchestrator) Abort(taskID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, err := o.taskLocked(taskID)
	if err != nil {
		return err
	}
	if task.Status == TaskAborted || task.Status == TaskCompleted {
		return fmt.Errorf("任务已处于终态 %s", task.Status)
	}
	for _, b := range task.Batches {
		for _, id := range b.DeviceIDs {
			dev := o.store.Devices[id]
			switch dev.Status {
			case DeviceFailed:
				o.rollbackLocked(task, dev, "任务终止：回滚失败设备")
			case DevicePending, DeviceDelivered:
				dev.Status = DeviceQuarantined
				o.eventLocked(taskID, id, "device_quarantined", "任务终止：未完成设备隔离待处理")
			}
		}
	}
	task.Status = TaskAborted
	o.eventLocked(taskID, "", "task_aborted", "任务终止")
	return o.store.Save()
}

// HandleReport 处理设备上报：按设备序号去重乱序，终态结果不可改写。
func (o *Orchestrator) HandleReport(r Report) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, err := o.taskLocked(r.TaskID)
	if err != nil {
		return err
	}
	dev, ok := o.store.Devices[r.DeviceID]
	if !ok {
		return fmt.Errorf("未知设备 %s", r.DeviceID)
	}
	// 幂等与乱序：只接受严格递增的序号，重复/旧上报直接丢弃
	if r.Seq <= dev.LastSeq {
		o.eventLocked(task.ID, dev.ID, "report_dropped",
			fmt.Sprintf("seq=%d <= lastSeq=%d type=%s（重复或乱序，丢弃）", r.Seq, dev.LastSeq, r.Type))
		return o.store.Save()
	}
	dev.LastSeq = r.Seq

	switch r.Type {
	case ReportOffline:
		dev.Online = false
		o.eventLocked(task.ID, dev.ID, "device_offline", fmt.Sprintf("seq=%d", r.Seq))
		return o.store.Save()
	case ReportOnline:
		dev.Online = true
		o.eventLocked(task.ID, dev.ID, "device_online", fmt.Sprintf("seq=%d", r.Seq))
		return o.store.Save()
	}

	// 终态保护：已完成/已回滚/已隔离的结果不得被后续上报回退或改写
	if dev.Status.Terminal() {
		o.eventLocked(task.ID, dev.ID, "report_ignored",
			fmt.Sprintf("type=%s 设备已处终态 %s，忽略", r.Type, dev.Status))
		return o.store.Save()
	}

	// 推送保护：批次尚未下发的设备（pending）的升级类上报无效，忽略
	if dev.Status == DevicePending {
		o.eventLocked(task.ID, dev.ID, "report_ignored",
			fmt.Sprintf("type=%s 设备尚未被下发（pending），忽略", r.Type))
		return o.store.Save()
	}

	switch r.Type {
	case ReportAck:
		dev.Status = DeviceDelivered
		o.eventLocked(task.ID, dev.ID, "report_ack", fmt.Sprintf("seq=%d", r.Seq))
	case ReportSuccess:
		// 版本一致性校验：上报版本必须等于任务目标版本
		if r.Version != task.TargetVersion {
			dev.Status = DeviceFailed
			dev.FailReason = fmt.Sprintf("上报版本 %s 与目标 %s 不一致", r.Version, task.TargetVersion)
			o.eventLocked(task.ID, dev.ID, "report_version_mismatch", dev.FailReason)
			break
		}
		dev.Status = DeviceSucceeded
		dev.CurrentVersion = r.Version
		dev.TargetVersion = ""
		o.eventLocked(task.ID, dev.ID, "report_success",
			fmt.Sprintf("seq=%d version=%s", r.Seq, r.Version))
	case ReportFailure:
		dev.Status = DeviceFailed
		dev.FailReason = r.Reason
		o.eventLocked(task.ID, dev.ID, "report_failure",
			fmt.Sprintf("seq=%d reason=%s", r.Seq, r.Reason))
	default:
		return fmt.Errorf("未知上报类型 %s", r.Type)
	}
	return o.store.Save()
}

// RollbackDevice 将失败设备回滚到已知可用版本。
func (o *Orchestrator) RollbackDevice(taskID, deviceID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, err := o.taskLocked(taskID)
	if err != nil {
		return err
	}
	dev, ok := o.store.Devices[deviceID]
	if !ok {
		return fmt.Errorf("未知设备 %s", deviceID)
	}
	if dev.Status != DeviceFailed {
		return fmt.Errorf("设备状态 %s 不需要回滚", dev.Status)
	}
	o.rollbackLocked(task, dev, "人工回滚")
	return o.store.Save()
}

// rollbackLocked 回滚：CurrentVersion 从未被失败路径修改，始终为已知可用版本。
func (o *Orchestrator) rollbackLocked(task *Task, dev *Device, reason string) {
	dev.Status = DeviceRolledBack
	dev.TargetVersion = ""
	o.eventLocked(task.ID, dev.ID, "device_rolled_back",
		fmt.Sprintf("恢复到已知可用版本 %s；%s", dev.CurrentVersion, reason))
}

// deliverBatchLocked 下发一个批次：标记设备为 delivered 并记录目标版本。
func (o *Orchestrator) deliverBatchLocked(task *Task, batchIdx int) {
	batch := &task.Batches[batchIdx]
	batch.Started = true
	task.CurrentBatch = batchIdx
	for _, id := range batch.DeviceIDs {
		dev := o.store.Devices[id]
		dev.Status = DeviceDelivered
		dev.TargetVersion = task.TargetVersion
		o.eventLocked(task.ID, id, "delivered",
			fmt.Sprintf("batch=%d target=%s", batchIdx, task.TargetVersion))
	}
	o.eventLocked(task.ID, "", "batch_delivered",
		fmt.Sprintf("batch=%d devices=%d", batchIdx, len(batch.DeviceIDs)))
}

func (o *Orchestrator) taskLocked(taskID string) (*Task, error) {
	task, ok := o.store.Tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("未知任务 %s", taskID)
	}
	return task, nil
}

func (o *Orchestrator) eventLocked(taskID, deviceID, kind, detail string) {
	o.store.Events = append(o.store.Events, Event{
		At:       o.now(),
		TaskID:   taskID,
		DeviceID: deviceID,
		Kind:     kind,
		Detail:   detail,
	})
}

// Snapshot 返回任务与设备的只读快照（用于测试与 CLI 展示）。
func (o *Orchestrator) Snapshot(taskID string) (*Task, map[string]Device, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	task, ok := o.store.Tasks[taskID]
	if !ok {
		return nil, nil, fmt.Errorf("未知任务 %s", taskID)
	}
	t := *task
	devs := map[string]Device{}
	for _, b := range task.Batches {
		for _, id := range b.DeviceIDs {
			devs[id] = *o.store.Devices[id]
		}
	}
	return &t, devs, nil
}

// Events 返回全部状态变更事件（追溯用）。
func (o *Orchestrator) Events() []Event {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]Event, len(o.store.Events))
	copy(out, o.store.Events)
	return out
}
