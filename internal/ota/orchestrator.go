package ota

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Orchestrator 灰度升级编排器。所有状态变更串行化并持久化，保证并发安全与可追溯。
type Orchestrator struct {
	mu        sync.Mutex
	store     *Store
	now       func() time.Time
	scheduler *Scheduler
}

// NewOrchestrator 创建编排器，并恢复历史任务的观察期定时器（见 Scheduler.restore）。
func NewOrchestrator(store *Store) *Orchestrator {
	o := &Orchestrator{store: store, now: time.Now}
	o.scheduler = newScheduler(o)
	o.scheduler.restore()
	return o
}

// CreateTask 创建升级任务：注册设备并按 batchSize 划分批次。
// devices 为 设备ID -> 当前已知可用版本。
// observePeriod 为每批下发后的观察期时长：>0 时观察期到点自动评估；
// 0 表示不启用自动调度，完全保持手动推进（历史任务同样按此行为）。
func (o *Orchestrator) CreateTask(taskID, targetVersion string, devices map[string]string, batchSize int, minSuccessRate float64, observePeriod time.Duration) (*Task, error) {
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
	if observePeriod < 0 {
		return nil, errors.New("observePeriod 不能为负")
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
		ObservePeriod:  observePeriod,
		CreatedAt:      o.now(),
	}
	for i, id := range ids {
		batchIdx := i / batchSize
		for len(task.Batches) <= batchIdx {
			task.Batches = append(task.Batches, Batch{Index: len(task.Batches)})
		}
		task.Batches[batchIdx].DeviceIDs = append(task.Batches[batchIdx].DeviceIDs, id)
		o.store.addDeviceLocked(&Device{
			ID:             id,
			CurrentVersion: devices[id],
			Online:         true,
			Status:         DevicePending,
			BatchIndex:     batchIdx,
		})
	}
	o.store.Tasks[taskID] = task
	o.eventLocked(taskID, "", "task_created",
		fmt.Sprintf("target=%s devices=%d batches=%d minSuccessRate=%.2f observePeriod=%s",
			targetVersion, len(ids), len(task.Batches), minSuccessRate, observePeriodText(observePeriod)))
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
	if err := o.store.Save(); err != nil {
		return err
	}
	// 观察期持久化成功后再挂定时器，避免定时器先于落盘触发。
	o.scheduleAfterBatchLocked(task)
	return nil
}

// Advance 人工推进：立即按当前设备情况判定当前批次并结束其观察等待。
// 若当前批次已经形成过结论（自动先触发或人工先触发），则直接返回既有结论，
// 不会重复判定、不会改写已确认的设备结果——同一批次至多一次结论。
// 对未设置观察期的历史任务，行为与此前完全一致（手动推进）。
func (o *Orchestrator) Advance(taskID string) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.evaluateTriggerLocked(taskID, TriggerManual)
}

// evaluateTriggerLocked 以指定触发来源（manual/auto）对当前批次做唯一一次健康判定。
// 所有路径（人工 Advance、观察期定时器、重启补偿）都汇聚于此，在编排器锁内串行执行，
// 因而无论并发多少次、谁先谁后，每个批次只会产生一次放量或暂停结论。
// 返回 (healthy, err)：已判定过的批次返回既有 healthy 结论且 err 为 nil。
func (o *Orchestrator) evaluateTriggerLocked(taskID, trigger string) (bool, error) {
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

	// 一次性结论保护：已判定过的批次，任何触发（自动定时器、并发人工、重复调用）
	// 都只回读既有结论，设备终态结果不会被再次评估改写。
	if batch.Decided {
		log.Printf("[ota-orchestrator] 批次已形成结论，跳过重复评估 task=%s batch=%d healthy=%v 本次触发=%s 结论触发=%s",
			taskID, batch.Index, batch.Healthy, trigger, batch.Trigger)
		o.eventLocked(taskID, "", "batch_decision_skipped",
			fmt.Sprintf("batch=%d trigger=%s 该批次已由 %s 形成结论 healthy=%v，跳过重复评估",
				batch.Index, trigger, batch.Trigger, batch.Healthy))
		return batch.Healthy, o.store.Save()
	}

	// 人工提前触发：观察期尚未结束，立即取消定时器并按此刻设备情况判定。
	manualEarly := trigger == TriggerManual && batch.ObserveDeadline != nil &&
		o.now().Before(*batch.ObserveDeadline)
	o.scheduler.disarmLocked(taskID)
	batch.ObserveDeadline = nil

	succeeded := 0
	quarantined := 0
	rolledBack := 0
	for _, id := range batch.DeviceIDs {
		dev := o.store.Devices[id]
		switch dev.Status {
		case DeviceSucceeded:
			succeeded++
		case DeviceFailed:
			// 确定策略：失败设备回滚到已知可用版本
			o.rollbackLocked(task, dev, fmt.Sprintf("批次健康判定（%s）：升级失败，执行回滚", trigger))
			rolledBack++
		default:
			// delivered/pending：离线上报缺失，隔离待人工处理，不计入成功
			dev.Status = DeviceQuarantined
			o.store.touchDevice(dev)
			quarantined++
			o.eventLocked(taskID, id, "device_quarantined",
				fmt.Sprintf("batch=%d trigger=%s 上报缺失/异常，不计入成功", batch.Index, trigger))
		}
	}
	rate := float64(succeeded) / float64(len(batch.DeviceIDs))
	batch.Healthy = rate >= task.MinSuccessRate
	batch.Decided = true
	decidedAt := o.now()
	batch.DecidedAt = &decidedAt
	batch.Trigger = trigger
	o.eventLocked(taskID, "", "batch_health_check",
		fmt.Sprintf("batch=%d trigger=%s succeeded=%d/%d rolledBack=%d quarantined=%d rate=%.2f threshold=%.2f healthy=%v%s",
			batch.Index, trigger, succeeded, len(batch.DeviceIDs), rolledBack, quarantined,
			rate, task.MinSuccessRate, batch.Healthy, earlyText(manualEarly)))

	if !batch.Healthy {
		task.Status = TaskPaused
		reason := "健康判定未通过，自动暂停"
		if trigger == TriggerManual {
			reason = "人工推进：健康判定未通过，暂停"
		}
		o.eventLocked(taskID, "", "task_paused", reason)
		if err := o.store.Save(); err != nil {
			return false, err
		}
		log.Printf("[ota-orchestrator] 批次判定不通过 task=%s batch=%d trigger=%s succeeded=%d/%d rate=%.2f 任务暂停",
			taskID, batch.Index, trigger, succeeded, len(batch.DeviceIDs), rate)
		return false, nil
	}
	if task.CurrentBatch+1 >= len(task.Batches) {
		task.Status = TaskCompleted
		o.eventLocked(taskID, "", "task_completed",
			fmt.Sprintf("全部批次完成 trigger=%s", trigger))
		if err := o.store.Save(); err != nil {
			return false, err
		}
		log.Printf("[ota-orchestrator] 任务完成 task=%s trigger=%s batches=%d", taskID, trigger, len(task.Batches))
		return true, nil
	}
	next := task.CurrentBatch + 1
	o.deliverBatchLocked(task, next)
	if err := o.store.Save(); err != nil {
		return false, err
	}
	// 下一批已随判定放量下发：为新批次安排观察定时器（持久化成功后）。
	o.scheduleAfterBatchLocked(task)
	log.Printf("[ota-orchestrator] 批次判定通过放量 task=%s batch=%d->%d trigger=%s succeeded=%d/%d rate=%.2f",
		taskID, batch.Index, next, trigger, succeeded, len(batch.DeviceIDs), rate)
	return true, nil
}

// scheduleAfterBatchLocked 为当前批次安排观察期定时器；未配置观察期的任务为空操作。
// 调用方须持有编排器锁，且当前批次观察截止时刻已随 deliverBatchLocked 写好。
func (o *Orchestrator) scheduleAfterBatchLocked(task *Task) {
	if task.ObservePeriod <= 0 || task.CurrentBatch < 0 {
		return
	}
	o.scheduler.armLocked(task)
}

func observePeriodText(d time.Duration) string {
	if d <= 0 {
		return "0(手动推进)"
	}
	return d.String()
}

func earlyText(early bool) string {
	if early {
		return " 人工提前于观察期结束触发"
	}
	return ""
}

// Pause 人工暂停：观察期内立即生效——取消待触发的自动评估定时器并清除观察截止，
// 使定时器到点也不会再评估当前批次；批次的已确认结论不受影响。
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
	o.scheduler.disarmLocked(taskID)
	if task.CurrentBatch >= 0 {
		task.Batches[task.CurrentBatch].ObserveDeadline = nil
	}
	task.Status = TaskPaused
	o.eventLocked(taskID, "", "task_paused", "人工暂停（观察期定时器已取消）")
	return o.store.Save()
}

// Resume 恢复已暂停的任务。若当前批次尚未形成结论且配置了观察期，
// 则从恢复时刻重新计算一个完整观察期；已判定过的批次不会被再次自动评估，
// 需要人工再次 Advance 决定是否放量（保持人工可控）。
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
	detail := "人工恢复"
	if task.CurrentBatch >= 0 {
		batch := &task.Batches[task.CurrentBatch]
		if batch.Decided {
			detail = "人工恢复：当前批次已有结论，等待人工推进，不重复评估"
		} else if task.ObservePeriod > 0 {
			deadline := o.now().Add(task.ObservePeriod)
			batch.ObserveDeadline = &deadline
			detail = fmt.Sprintf("人工恢复：重新开始观察期 %s（截止 %s）",
				task.ObservePeriod, deadline.Format("15:04:05.000"))
		}
	}
	o.eventLocked(taskID, "", "task_resumed", detail)
	if err := o.store.Save(); err != nil {
		return err
	}
	// 仅未判定批次需要重新挂定时器；已判定批次等人工 Advance（评估入口会自行拦截）。
	if task.CurrentBatch >= 0 && !task.Batches[task.CurrentBatch].Decided {
		o.scheduleAfterBatchLocked(task)
	}
	return nil
}

// Abort 终止任务（终态）：失败设备回滚，未完成设备隔离。观察期内终止立即生效，
// 取消待触发定时器，之后不会再有自动评估。
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
	o.scheduler.disarmLocked(taskID)
	for _, b := range task.Batches {
		for _, id := range b.DeviceIDs {
			dev := o.store.Devices[id]
			switch dev.Status {
			case DeviceFailed:
				o.rollbackLocked(task, dev, "任务终止：回滚失败设备")
			case DevicePending, DeviceDelivered:
				dev.Status = DeviceQuarantined
				o.store.touchDevice(dev)
				o.eventLocked(taskID, id, "device_quarantined", "任务终止：未完成设备隔离待处理")
			}
		}
	}
	task.Status = TaskAborted
	o.eventLocked(taskID, "", "task_aborted", "任务终止（观察期定时器已取消）")
	return o.store.Save()
}

// Close 停止自动调度器并等待在途回调结束，并关闭事件日志句柄。
func (o *Orchestrator) Close() {
	o.scheduler.Stop()
	_ = o.store.Close()
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
	o.store.touchDevice(dev) // 序号/在线性/升级状态等任意字段变更都落在同一设备分片

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
	o.store.touchDevice(dev)
	o.eventLocked(task.ID, dev.ID, "device_rolled_back",
		fmt.Sprintf("恢复到已知可用版本 %s；%s", dev.CurrentVersion, reason))
}

// deliverBatchLocked 下发一个批次：标记设备为 delivered 并记录目标版本。
// 若任务配置了观察期，同时把观察截止时刻（绝对时间）写入批次，供定时器与重启恢复使用。
func (o *Orchestrator) deliverBatchLocked(task *Task, batchIdx int) {
	batch := &task.Batches[batchIdx]
	batch.Started = true
	batch.Decided = false
	batch.Trigger = ""
	batch.DecidedAt = nil
	task.CurrentBatch = batchIdx
	if task.ObservePeriod > 0 {
		deadline := o.now().Add(task.ObservePeriod)
		batch.ObserveDeadline = &deadline
	} else {
		batch.ObserveDeadline = nil
	}
	for _, id := range batch.DeviceIDs {
		dev := o.store.Devices[id]
		dev.Status = DeviceDelivered
		dev.TargetVersion = task.TargetVersion
		o.store.touchDevice(dev)
		o.eventLocked(task.ID, id, "delivered",
			fmt.Sprintf("batch=%d target=%s", batchIdx, task.TargetVersion))
	}
	o.eventLocked(task.ID, "", "batch_delivered",
		fmt.Sprintf("batch=%d devices=%d observePeriod=%s",
			batchIdx, len(batch.DeviceIDs), observePeriodText(task.ObservePeriod)))
}

func (o *Orchestrator) taskLocked(taskID string) (*Task, error) {
	task, ok := o.store.Tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("未知任务 %s", taskID)
	}
	return task, nil
}

// eventLocked 记录状态变更事件：先追加到内存与 append-only 事件日志并落盘，
// 随后调用方再 Save 状态。该顺序保证崩溃时最多多出事件，而不会出现"状态已变但
// 丢失判定依据"。store 自带互斥锁，此处不会与编排器锁形成锁环。
func (o *Orchestrator) eventLocked(taskID, deviceID, kind, detail string) {
	e := Event{
		At:       o.now(),
		TaskID:   taskID,
		DeviceID: deviceID,
		Kind:     kind,
		Detail:   detail,
	}
	// AppendEvent 同时追加内存与事件日志文件。
	if err := o.store.AppendEvent(e); err != nil {
		panic(fmt.Errorf("事件日志写入失败: %w", err))
	}
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
