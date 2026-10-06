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
	mu        sync.Mutex
	store     *Store
	now       func() time.Time
	timers    map[string]*time.Timer // taskID -> 观察期定时器（每任务同一时刻至多一个在观察的批次）
	closed    bool
	dirty     bool // 有未落盘的上报类变更，由后台协程合并写盘
	stopFlush chan struct{}
	flushWG   sync.WaitGroup
}

// flushInterval 上报类变更的合并落盘间隔。
const flushInterval = 50 * time.Millisecond

// NewOrchestrator 创建编排器，并恢复重启前未到期的观察期调度：
// 已过期未评估的批次立即补评，未到期的按剩余时间重新计时；已形成的结论不重复评估。
func NewOrchestrator(store *Store) *Orchestrator {
	o := &Orchestrator{store: store, now: time.Now, timers: map[string]*time.Timer{}, stopFlush: make(chan struct{})}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, task := range o.store.Tasks {
		if task.Status != TaskRolling || task.ObservePeriod <= 0 || task.CurrentBatch < 0 {
			continue
		}
		batch := &task.Batches[task.CurrentBatch]
		if !batch.Started || batch.Evaluated {
			continue
		}
		if batch.ObserveUntil == nil {
			// 兼容异常状态：已下发但无截止时间，按完整观察期重新计时
			o.armObservationLocked(task)
			continue
		}
		remaining := time.Until(*batch.ObserveUntil)
		if remaining <= 0 {
			o.eventLocked(task.ID, "", "observation_expired_while_down",
				fmt.Sprintf("batch=%d 观察期在停机期间到期，立即补评", batch.Index))
			o.evaluateLocked(task, "auto:重启补评")
		} else {
			o.armTimerLocked(task, remaining)
			o.eventLocked(task.ID, "", "observation_resumed",
				fmt.Sprintf("batch=%d 重启后续跑观察期，剩余 %s", batch.Index, remaining.Round(time.Millisecond)))
		}
	}
	_ = o.flushLocked()
	o.flushWG.Add(1)
	go o.flushLoop()
	return o
}

// flushLoop 周期性把上报类变更合并落盘，避免高频上报时每次全量写盘。
func (o *Orchestrator) flushLoop() {
	defer o.flushWG.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-o.stopFlush:
			return
		case <-ticker.C:
			o.mu.Lock()
			if o.dirty {
				_ = o.store.Save()
				o.dirty = false
			}
			o.mu.Unlock()
		}
	}
}

// markDirtyLocked 标记有变更待落盘（由 flushLoop 或下一次关键转换落盘）。
func (o *Orchestrator) markDirtyLocked() {
	o.dirty = true
}

// flushLocked 立即同步落盘并清除脏标记。任务级状态转换（创建/下发/评估/暂停/
// 恢复/终止/回滚）必须走同步落盘，保证已形成的结论在重启后不丢失。
func (o *Orchestrator) flushLocked() error {
	o.dirty = false
	return o.store.Save()
}

// Close 停止全部观察期定时器与后台落盘协程，并把未落盘变更同步写盘。
// 之后自动调度不再触发，已有状态保持可读。
func (o *Orchestrator) Close() {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.closed = true
	for id, t := range o.timers {
		t.Stop()
		delete(o.timers, id)
	}
	close(o.stopFlush)
	o.mu.Unlock()
	o.flushWG.Wait()
	o.mu.Lock()
	defer o.mu.Unlock()
	_ = o.flushLocked()
}

// armObservationLocked 为当前批次开启一段新的观察期：重置截止时间并武装定时器。
func (o *Orchestrator) armObservationLocked(task *Task) {
	if task.ObservePeriod <= 0 || o.closed {
		return
	}
	batch := &task.Batches[task.CurrentBatch]
	deadline := o.now().Add(task.ObservePeriod)
	batch.ObserveUntil = &deadline
	o.armTimerLocked(task, task.ObservePeriod)
	o.eventLocked(task.ID, "", "observation_scheduled",
		fmt.Sprintf("batch=%d observePeriod=%s deadline=%s",
			batch.Index, task.ObservePeriod, deadline.Format("15:04:05.000")))
}

// armTimerLocked 武装（或重置）任务的观察期定时器，d 后触发自动评估；不修改截止时间。
func (o *Orchestrator) armTimerLocked(task *Task, d time.Duration) {
	o.cancelTimerLocked(task.ID)
	batchIdx := task.CurrentBatch
	taskID := task.ID
	o.timers[taskID] = time.AfterFunc(d, func() { o.autoAdvance(taskID, batchIdx) })
}

// cancelTimerLocked 取消任务的观察期定时器（若存在）。
func (o *Orchestrator) cancelTimerLocked(taskID string) {
	if t, ok := o.timers[taskID]; ok {
		t.Stop()
		delete(o.timers, taskID)
	}
}

// autoAdvance 观察期到点自动评估。与人工操作、设备上报共用同一把锁串行化；
// 批次已评估、任务已离开 rolling 或已推进到其他批次时直接放弃，保证同批只评一次。
func (o *Orchestrator) autoAdvance(taskID string, batchIdx int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	task, ok := o.store.Tasks[taskID]
	if !ok || task.Status != TaskRolling || task.CurrentBatch != batchIdx {
		return
	}
	batch := &task.Batches[batchIdx]
	if !batch.Started || batch.Evaluated {
		return
	}
	delete(o.timers, taskID)
	o.evaluateLocked(task, "auto:观察期到期")
	_ = o.flushLocked()
}

// CreateTask 创建纯手动推进的升级任务（无观察期），行为与历史版本一致。
func (o *Orchestrator) CreateTask(taskID, targetVersion string, devices map[string]string, batchSize int, minSuccessRate float64) (*Task, error) {
	return o.CreateTaskWithObserve(taskID, targetVersion, devices, batchSize, minSuccessRate, 0)
}

// CreateTaskWithObserve 创建升级任务：注册设备并按 batchSize 划分批次。
// observePeriod > 0 时，每批下发后进入观察期，到期按健康判定规则自动放量或暂停；
// observePeriod == 0 时保持手动 Advance 推进。devices 为 设备ID -> 当前已知可用版本。
func (o *Orchestrator) CreateTaskWithObserve(taskID, targetVersion string, devices map[string]string, batchSize int, minSuccessRate float64, observePeriod time.Duration) (*Task, error) {
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
	if observePeriod < 0 {
		return nil, errors.New("observePeriod 不能为负")
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
		ObservePeriod:  observePeriod,
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
		fmt.Sprintf("target=%s devices=%d batches=%d minSuccessRate=%.2f observePeriod=%s",
			targetVersion, len(ids), len(task.Batches), minSuccessRate, observePeriod))
	if err := o.flushLocked(); err != nil {
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
	o.armObservationLocked(task)
	return o.flushLocked()
}

// Advance 人工推进：立即对当前批次做健康判定并结束观察等待。
// 与自动调度并发安全：同一批次只形成一次确定结论，重复触发返回已形成的结论。
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
	o.cancelTimerLocked(taskID) // 人工结束本批等待，定时器不得再次评估同一批
	batch := &task.Batches[task.CurrentBatch]
	if batch.Evaluated {
		// 本批已有确定结论（如自动调度刚评估过）：不重复评估，返回既有结论
		o.eventLocked(taskID, "", "batch_evaluate_skipped",
			fmt.Sprintf("batch=%d 已形成结论 healthy=%v，重复触发忽略", batch.Index, batch.Healthy))
		if !batch.Healthy {
			// 结论为不健康：保持暂停语义（如 Resume 后再次 Advance）
			task.Status = TaskPaused
			o.eventLocked(taskID, "", "task_paused", "批次已有不健康结论，恢复暂停")
		}
		return batch.Healthy, o.flushLocked()
	}
	healthy := o.evaluateLocked(task, "manual")
	return healthy, o.flushLocked()
}

// evaluateLocked 对当前批次做健康判定：仅 succeeded 计入成功；
// 失败设备回滚、离线上报缺失设备隔离；满足阈值则放量进入下一批，否则暂停任务。
// trigger 记录触发来源（manual / auto:观察期到期 / auto:重启补评），写入事件以便追溯。
// 判定完成后批次被标记为 Evaluated，任何后续触发都不会再次评估本批。
func (o *Orchestrator) evaluateLocked(task *Task, trigger string) bool {
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
			o.eventLocked(task.ID, id, "device_quarantined",
				fmt.Sprintf("batch=%d 上报缺失/异常，不计入成功", batch.Index))
		}
	}
	rate := float64(succeeded) / float64(len(batch.DeviceIDs))
	batch.Healthy = rate >= task.MinSuccessRate
	batch.Evaluated = true
	batch.ObserveUntil = nil
	o.eventLocked(task.ID, "", "batch_health_check",
		fmt.Sprintf("batch=%d trigger=%s succeeded=%d/%d rate=%.2f threshold=%.2f healthy=%v",
			batch.Index, trigger, succeeded, len(batch.DeviceIDs), rate, task.MinSuccessRate, batch.Healthy))
	if !batch.Healthy {
		task.Status = TaskPaused
		o.cancelTimerLocked(task.ID)
		o.eventLocked(task.ID, "", "task_paused", fmt.Sprintf("健康判定未通过，自动暂停（trigger=%s）", trigger))
		return false
	}
	if task.CurrentBatch+1 >= len(task.Batches) {
		task.Status = TaskCompleted
		o.cancelTimerLocked(task.ID)
		o.eventLocked(task.ID, "", "task_completed", fmt.Sprintf("全部批次完成（trigger=%s）", trigger))
		return true
	}
	o.deliverBatchLocked(task, task.CurrentBatch+1)
	o.armObservationLocked(task) // 新批次进入下一段观察期
	return true
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
	o.cancelTimerLocked(task.ID) // 观察期内人工暂停立即生效，定时器不再触发
	o.eventLocked(taskID, "", "task_paused", "人工暂停")
	return o.flushLocked()
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
	// 当前批次仍在观察中（已下发未评估）时，恢复后重新计时一段完整观察期
	if task.ObservePeriod > 0 && task.CurrentBatch >= 0 {
		if b := &task.Batches[task.CurrentBatch]; b.Started && !b.Evaluated {
			o.armObservationLocked(task)
		}
	}
	return o.flushLocked()
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
	o.cancelTimerLocked(task.ID) // 终止立即生效，观察期定时器不再触发
	o.eventLocked(taskID, "", "task_aborted", "任务终止")
	return o.flushLocked()
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
		o.markDirtyLocked()
		return nil
	}
	dev.LastSeq = r.Seq

	switch r.Type {
	case ReportOffline:
		dev.Online = false
		o.eventLocked(task.ID, dev.ID, "device_offline", fmt.Sprintf("seq=%d", r.Seq))
		o.markDirtyLocked()
		return nil
	case ReportOnline:
		dev.Online = true
		o.eventLocked(task.ID, dev.ID, "device_online", fmt.Sprintf("seq=%d", r.Seq))
		o.markDirtyLocked()
		return nil
	}

	// 终态保护：已完成/已回滚/已隔离的结果不得被后续上报回退或改写
	if dev.Status.Terminal() {
		o.eventLocked(task.ID, dev.ID, "report_ignored",
			fmt.Sprintf("type=%s 设备已处终态 %s，忽略", r.Type, dev.Status))
		o.markDirtyLocked()
		return nil
	}

	// 推送保护：批次尚未下发的设备（pending）的升级类上报无效，忽略
	if dev.Status == DevicePending {
		o.eventLocked(task.ID, dev.ID, "report_ignored",
			fmt.Sprintf("type=%s 设备尚未被下发（pending），忽略", r.Type))
		o.markDirtyLocked()
		return nil
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
	o.markDirtyLocked()
	return nil
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
	return o.flushLocked()
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
	// 深拷贝批次与设备ID切片：定时器/自动调度可能在 Snapshot 返回后修改批次字段，
	// 快照必须与后续变更隔离，避免调用方读到竞态数据。
	t.Batches = make([]Batch, len(task.Batches))
	for i, b := range task.Batches {
		ids := make([]string, len(b.DeviceIDs))
		copy(ids, b.DeviceIDs)
		b.DeviceIDs = ids
		t.Batches[i] = b
	}
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
