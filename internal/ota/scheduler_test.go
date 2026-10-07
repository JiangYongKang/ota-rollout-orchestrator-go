package ota

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// waitFor 轮询等待条件成立，超时失败。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待条件超时（%s）：%s", timeout, msg)
}

// createAutoTask 新建带观察期的任务并 Start。
func createAutoTask(t *testing.T, taskID string, n, batchSize int, rate float64, observe time.Duration) (*Orchestrator, map[string]*SimDevice) {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o := NewOrchestrator(store)
	t.Cleanup(o.Close)
	devs, sims := makeDevices(n)
	if _, err := o.CreateTask(taskID, "v2.0.0", devs, batchSize, rate, observe); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start(taskID); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return o, sims
}

// reportBatchIDs 对指定批次设备回收上报。
func reportBatchIDs(t *testing.T, o *Orchestrator, taskID, target string, sims map[string]*SimDevice, batchIdx int) {
	t.Helper()
	task, _, err := o.Snapshot(taskID)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for _, id := range task.Batches[batchIdx].DeviceIDs {
		for _, r := range sims[id].Deliver(taskID, target) {
			if err := o.HandleReport(r); err != nil {
				t.Fatalf("HandleReport: %v", err)
			}
		}
	}
}

// countEvents 统计事件数量。
func countEvents(o *Orchestrator, taskID, kind string) int {
	n := 0
	for _, e := range o.Events() {
		if e.TaskID == taskID && e.Kind == kind {
			n++
		}
	}
	return n
}

// TestObservePeriodAutoAdvance 观察期到点：自动评估并连续放量到完成，结论来源标注 auto。
func TestObservePeriodAutoAdvance(t *testing.T) {
	o, sims := createAutoTask(t, "auto1", 6, 2, 0.8, 80*time.Millisecond)
	task, _, _ := o.Snapshot("auto1")
	batches := len(task.Batches)
	// 每批被自动放量下发后，再回收该批设备上报，推动下一批观察。
	for b := 0; b < batches; b++ {
		waitFor(t, time.Second, func() bool {
			tk, _, _ := o.Snapshot("auto1")
			return tk.CurrentBatch >= b || tk.Status != TaskRolling
		}, fmt.Sprintf("批次 %d 下发", b))
		tk, _, _ := o.Snapshot("auto1")
		if tk.Status != TaskRolling && tk.Status != TaskCompleted {
			t.Fatalf("批次 %d 前任务意外离开 rolling：%s", b, tk.Status)
		}
		reportBatchIDs(t, o, "auto1", "v2.0.0", sims, b)
	}
	// 调度器逐批到点自动放量，直到完成。
	waitFor(t, 3*time.Second, func() bool {
		task, _, _ := o.Snapshot("auto1")
		return task.Status == TaskCompleted
	}, "自动放量至完成")

	task, devs, _ := o.Snapshot("auto1")
	logSnapshot(t, o, "auto1", "after-auto-complete")
	if task.CurrentBatch != batches-1 {
		t.Fatalf("currentBatch 应为 %d，实际 %d", batches-1, task.CurrentBatch)
	}
	for _, b := range task.Batches {
		if !b.Decided || b.Trigger != TriggerAuto {
			t.Fatalf("批次 %d 应由自动调度判定，实际 decided=%v trigger=%q", b.Index, b.Decided, b.Trigger)
		}
	}
	if countEvents(o, "auto1", "batch_health_check") != batches {
		t.Fatalf("每批应恰好一次健康判定事件")
	}
	for id, d := range devs {
		if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
			t.Fatalf("设备 %s 未收敛：%s %s", id, d.Status, d.CurrentVersion)
		}
	}
}

// TestManualAdvanceDuringObservation 人工在观察期结束前推进：立即判定放量，
// 之后定时器到点不得对同一批再判一次。
func TestManualAdvanceDuringObservation(t *testing.T) {
	// 观察期设长，保证人工先于定时器。
	o, sims := createAutoTask(t, "early1", 4, 2, 0.8, 5*time.Second)
	reportBatchIDs(t, o, "early1", "v2.0.0", sims, 0)

	healthy, err := o.Advance("early1")
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if !healthy {
		t.Fatal("人工提前推进应判定健康并放量")
	}
	task, _, _ := o.Snapshot("early1")
	if task.CurrentBatch != 1 {
		t.Fatalf("应已放量到批次1，实际 %d", task.CurrentBatch)
	}
	b0 := task.Batches[0]
	if !b0.Decided || b0.Trigger != TriggerManual {
		t.Fatalf("批次0应由人工判定，decided=%v trigger=%q", b0.Decided, b0.Trigger)
	}
	if countEvents(o, "early1", "batch_health_check") != 1 {
		t.Fatal("批次0应只有一次判定事件")
	}
	// 再等待一个远大于观察期精度的窗口，确认旧定时器没有补判批次0。
	time.Sleep(120 * time.Millisecond)
	if got := countEvents(o, "early1", "batch_health_check"); got != 1 {
		t.Fatalf("定时器到点不得重复评估批次0，实际判定事件 %d 次", got)
	}
	// 批次1仍在观察中（尚未回收上报），人工再推进应隔离设备并暂停，结论仅一次。
	healthy, err = o.Advance("early1")
	if err != nil {
		t.Fatalf("Advance batch1: %v", err)
	}
	if healthy {
		t.Fatal("批次1无成功上报，应判定不健康")
	}
	task, _, _ = o.Snapshot("early1")
	if task.Status != TaskPaused {
		t.Fatalf("应暂停，实际 %s", task.Status)
	}
	o.Abort("early1")
}

// sendReport 单条上报便捷封装。
func sendReport(t *testing.T, o *Orchestrator, r Report) {
	t.Helper()
	if err := o.HandleReport(r); err != nil {
		t.Fatalf("HandleReport: %v", err)
	}
}

// TestRepeatedTriggersSingleDecision 定时器、并发人工、重复调用同时轰炸同一批次，
// 只产生一次确定结论，设备结果不被改写。
func TestRepeatedTriggersSingleDecision(t *testing.T) {
	o, sims := createAutoTask(t, "repeat1", 4, 4, 1.0, 60*time.Millisecond)
	reportBatchIDs(t, o, "repeat1", "v2.0.0", sims, 0)

	var wg sync.WaitGroup
	// 观察期到点前后，多个人工 Advance 与重复触发并发进行。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = o.Advance("repeat1")
		}()
	}
	wg.Wait()

	// 等待可能迟到的定时器回调也尝试一遍。
	time.Sleep(150 * time.Millisecond)
	task, devs, _ := o.Snapshot("repeat1")
	logSnapshot(t, o, "repeat1", "after-repeated-triggers")

	if task.Status != TaskCompleted {
		t.Fatalf("唯一批次健康，应直接完成，实际 %s", task.Status)
	}
	b := task.Batches[0]
	if !b.Decided || b.DecidedAt == nil {
		t.Fatal("批次应已形成唯一结论")
	}
	if got := countEvents(o, "repeat1", "batch_health_check"); got != 1 {
		t.Fatalf("健康判定事件必须恰好 1 次，实际 %d", got)
	}
	for id, d := range devs {
		if d.Status != DeviceSucceeded {
			t.Fatalf("设备 %s 终态被改写：%s", id, d.Status)
		}
	}
}

// TestManualAndAutoConcurrent 人工提前推进与自动定时器竞争：谁先谁后都只有一次结论，
// 且结论记录实际触发来源；定时器不得二次评估。
func TestManualAndAutoConcurrent(t *testing.T) {
	for trial := 0; trial < 20; trial++ {
		o, sims := createAutoTask(t, fmt.Sprintf("race%d", trial), 2, 2, 1.0, time.Duration(2+trial%5)*time.Millisecond)
		reportBatchIDs(t, o, fmt.Sprintf("race%d", trial), "v2.0.0", sims, 0)
		// 人工推进与定时器几乎同时发生。
		done := make(chan struct{})
		go func() {
			_, _ = o.Advance(fmt.Sprintf("race%d", trial))
			close(done)
		}()
		<-done
		waitFor(t, time.Second, func() bool {
			tk, _, _ := o.Snapshot(fmt.Sprintf("race%d", trial))
			return tk.Status == TaskCompleted
		}, "竞争后完成")
		time.Sleep(50 * time.Millisecond)
		tk, _, _ := o.Snapshot(fmt.Sprintf("race%d", trial))
		if got := countEvents(o, fmt.Sprintf("race%d", trial), "batch_health_check"); got != 1 {
			t.Fatalf("trial=%d 判定事件应恰好1次，实际 %d（trigger=%s）", trial, got, tk.Batches[0].Trigger)
		}
		if tk.Batches[0].Trigger != TriggerManual && tk.Batches[0].Trigger != TriggerAuto {
			t.Fatalf("trial=%d 非法触发来源 %q", trial, tk.Batches[0].Trigger)
		}
	}
}

// TestAutoPauseOnUnhealthy 观察期到点判定不健康：自动暂停，结论仅一次，
// 之后定时器不再评估；人工 Resume 不自动重判。
func TestAutoPauseOnUnhealthy(t *testing.T) {
	o, sims := createAutoTask(t, "pause1", 4, 2, 0.9, 60*time.Millisecond)
	sims["dev-001"].FailNextUpgrade = true
	reportBatchIDs(t, o, "pause1", "v2.0.0", sims, 0)

	waitFor(t, time.Second, func() bool {
		tk, _, _ := o.Snapshot("pause1")
		return tk.Status == TaskPaused
	}, "自动暂停")
	time.Sleep(120 * time.Millisecond) // 超过观察期，确认没有二次评估
	task, devs, _ := o.Snapshot("pause1")
	logSnapshot(t, o, "pause1", "after-auto-pause")
	if task.CurrentBatch != 0 || !task.Batches[0].Decided || task.Batches[0].Trigger != TriggerAuto {
		t.Fatalf("批次0应自动判定后暂停，current=%d decided=%v trigger=%q",
			task.CurrentBatch, task.Batches[0].Decided, task.Batches[0].Trigger)
	}
	if got := countEvents(o, "pause1", "batch_health_check"); got != 1 {
		t.Fatalf("判定应仅1次，实际 %d", got)
	}
	if devs["dev-001"].Status != DeviceRolledBack || devs["dev-001"].CurrentVersion != "v1.0.0" {
		t.Fatalf("失败设备应已回滚，实际 %s %s", devs["dev-001"].Status, devs["dev-001"].CurrentVersion)
	}
	// Resume 后已判定批次不被自动重新评估，也不自动放量。
	if err := o.Resume("pause1"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	task2, _, _ := o.Snapshot("pause1")
	if task2.CurrentBatch != 0 {
		t.Fatalf("恢复后不得自动放量，current=%d", task2.CurrentBatch)
	}
	if got := countEvents(o, "pause1", "batch_health_check"); got != 1 {
		t.Fatalf("恢复后不得重复判定，实际 %d 次", got)
	}
	o.Abort("pause1")
}

// TestReportBeforeEvaluationBoundary 成功上报在观察期到点"前"到达：计入成功，自动放量。
func TestReportBeforeEvaluationBoundary(t *testing.T) {
	o, sims := createAutoTask(t, "before1", 2, 2, 1.0, 200*time.Millisecond)
	// 先只上报 dev-000，观察期内再补 dev-001，两笔都在判定前，应全部成功。
	task, _, _ := o.Snapshot("before1")
	ids := task.Batches[0].DeviceIDs
	for _, r := range sims[ids[0]].Deliver("before1", "v2.0.0") {
		sendReport(t, o, r)
	}
	time.Sleep(80 * time.Millisecond) // 仍在观察期内
	for _, r := range sims[ids[1]].Deliver("before1", "v2.0.0") {
		sendReport(t, o, r)
	}
	waitFor(t, 2*time.Second, func() bool {
		tk, _, _ := o.Snapshot("before1")
		return tk.Status == TaskCompleted
	}, "边界前上报计入成功并自动完成")
	_, devs, _ := o.Snapshot("before1")
	for _, id := range ids {
		if devs[id].Status != DeviceSucceeded {
			t.Fatalf("设备 %s 应成功，实际 %s", id, devs[id].Status)
		}
	}
}

// TestReportAfterEvaluationBoundary 观察期到点时未上报成功的设备被隔离；
// 判定"后"到达的成功/失败上报不得把结论或版本判反（终态保护）。
func TestReportAfterEvaluationBoundary(t *testing.T) {
	o, sims := createAutoTask(t, "after1", 2, 2, 0.5, 40*time.Millisecond)
	sims["dev-001"].DropReports = true // 观察期内不上报
	task, _, _ := o.Snapshot("after1")
	ids := task.Batches[0].DeviceIDs
	for _, r := range sims[ids[0]].Deliver("after1", "v2.0.0") {
		sendReport(t, o, r)
	}
	// 到点：dev-000 成功、dev-001 上报缺失被隔离，成功率 1/2=0.5 达阈值 → 放量/单批任务完成
	waitFor(t, time.Second, func() bool {
		tk, _, _ := o.Snapshot("after1")
		return tk.Status == TaskCompleted
	}, "观察期到点完成")

	// 判定后 dev-001 重连补发 success（即便带正确版本），终态不得改写、版本不得翻转。
	sims["dev-001"].DropReports = false
	sims["dev-001"].Version = "v2.0.0"
	for _, r := range sims["dev-001"].Deliver("after1", "v2.0.0") {
		if err := o.HandleReport(r); err != nil {
			t.Fatalf("迟到上报处理: %v", err)
		}
	}
	// 再来一个版本错乱的 success 也不能影响结论。
	sendReport(t, o, Report{DeviceID: "dev-001", TaskID: "after1", Seq: 999, Type: ReportSuccess, Version: "v9.9.9"})
	_, devs, _ := o.Snapshot("after1")
	logSnapshot(t, o, "after1", "after-late-reports")
	if devs["dev-001"].Status != DeviceQuarantined {
		t.Fatalf("已隔离结论不得被迟到上报改写，实际 %s", devs["dev-001"].Status)
	}
	if devs["dev-000"].Status != DeviceSucceeded || devs["dev-000"].CurrentVersion != "v2.0.0" {
		t.Fatalf("dev-000 成功结论/版本被扰动：%s %s", devs["dev-000"].Status, devs["dev-000"].CurrentVersion)
	}
	if got := countEvents(o, "after1", "batch_health_check"); got != 1 {
		t.Fatalf("判定必须唯一，实际 %d", got)
	}
}

// TestPauseDuringObservation 观察期内人工暂停立即生效：定时器取消，不再自动评估；
// 恢复后未判定批次重新计时，仍可自动放量。
func TestPauseDuringObservation(t *testing.T) {
	o, sims := createAutoTask(t, "pauseresume1", 4, 2, 1.0, 60*time.Millisecond)
	reportBatchIDs(t, o, "pauseresume1", "v2.0.0", sims, 0)
	if err := o.Pause("pauseresume1"); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // 远超观察期
	task, _, _ := o.Snapshot("pauseresume1")
	if task.Status != TaskPaused || task.Batches[0].Decided {
		t.Fatalf("暂停后不得自动评估：status=%s decided=%v", task.Status, task.Batches[0].Decided)
	}
	if got := countEvents(o, "pauseresume1", "batch_health_check"); got != 0 {
		t.Fatalf("暂停期间不应有判定事件，实际 %d", got)
	}
	// 恢复：批次0重新计时，到点自动放量到批次1。
	if err := o.Resume("pauseresume1"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		tk, _, _ := o.Snapshot("pauseresume1")
		return tk.CurrentBatch >= 1
	}, "恢复后自动放量到批次1")
	reportBatchIDs(t, o, "pauseresume1", "v2.0.0", sims, 1)
	waitFor(t, 2*time.Second, func() bool {
		tk, _, _ := o.Snapshot("pauseresume1")
		return tk.Status == TaskCompleted
	}, "批次1自动完成")
}

// TestAbortDuringObservation 观察期内终止立即生效，定时器取消，之后无自动评估。
func TestAbortDuringObservation(t *testing.T) {
	o, sims := createAutoTask(t, "abortobs1", 4, 2, 1.0, 50*time.Millisecond)
	reportBatchIDs(t, o, "abortobs1", "v2.0.0", sims, 0)
	if err := o.Abort("abortobs1"); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	task, _, _ := o.Snapshot("abortobs1")
	if task.Status != TaskAborted || task.Batches[0].Decided {
		t.Fatalf("终止后不得再自动评估：status=%s decided=%v", task.Status, task.Batches[0].Decided)
	}
}

// addPrefixedTask 在同一个编排器上新建设备 ID 带前缀的任务，模拟单进程多任务并行。
func addPrefixedTask(t *testing.T, o *Orchestrator, prefix, taskID string, n, batchSize int, observe time.Duration) map[string]*SimDevice {
	t.Helper()
	devs := map[string]string{}
	sims := map[string]*SimDevice{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-dev-%03d", prefix, i)
		devs[id] = "v1.0.0"
		sims[id] = NewSimDevice(id, "v1.0.0")
	}
	if _, err := o.CreateTask(taskID, "v2.0.0", devs, batchSize, 0.6, observe); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start(taskID); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return sims
}

// TestMultipleTasksParallel 多任务并行观察/推进，定时器与事件彼此隔离、互不干扰。
func TestMultipleTasksParallel(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o := NewOrchestrator(store)
	t.Cleanup(o.Close)

	const taskN = 4
	type item struct {
		sims map[string]*SimDevice
		seen map[int]bool
	}
	items := map[string]*item{}
	for k := 0; k < taskN; k++ {
		id := fmt.Sprintf("multi-%d", k)
		sims := addPrefixedTask(t, o, id, id, 6, 2, time.Duration(30+20*k)*time.Millisecond)
		items[id] = &item{sims: sims, seen: map[int]bool{}}
	}

	var wg sync.WaitGroup
	for id, it := range items {
		wg.Add(1)
		go func(id string, it *item) {
			defer wg.Done()
			// 后台轮询：哪个批次被下发就回收哪批上报，模拟持续设备上报。
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				tk, _, err := o.Snapshot(id)
				if err != nil || tk.Status == TaskCompleted || tk.Status == TaskAborted {
					return
				}
				b := tk.CurrentBatch
				if b >= 0 && !it.seen[b] {
					it.seen[b] = true
					for _, devID := range tk.Batches[b].DeviceIDs {
						for _, r := range it.sims[devID].Deliver(id, "v2.0.0") {
							_ = o.HandleReport(r)
						}
					}
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(id, it)
	}
	wg.Wait()

	for id := range items {
		tk, devs, _ := o.Snapshot(id)
		if tk.Status != TaskCompleted {
			t.Fatalf("任务 %s 应自动完成，实际 %s", id, tk.Status)
		}
		for _, b := range tk.Batches {
			if !b.Decided || b.Trigger != TriggerAuto {
				t.Fatalf("任务 %s 批次 %d 应自动判定", id, b.Index)
			}
		}
		for devID, d := range devs {
			if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
				t.Fatalf("任务 %s 设备 %s 结论异常：%s %s", id, devID, d.Status, d.CurrentVersion)
			}
		}
		// 事件隔离：该任务的判定事件只属于该任务。
		if got := countEvents(o, id, "batch_health_check"); got != len(tk.Batches) {
			t.Fatalf("任务 %s 判定事件数异常：%d", id, got)
		}
	}
}

// startOrchestratorAt 以可替换时钟的方式不可直接构造，这里通过保存状态 + 等待真实时间模拟重启：
// 重启用例均基于持久化文件重新 NewOrchestrator。

// TestRestartPendingObservation 观察期尚未到点时重启：按持久化的截止时刻继续等待，
// 到期自动放量且不重复判定。
func TestRestartPendingObservation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o1 := NewOrchestrator(store)
	devs, sims := makeDevices(4)
	if _, err := o1.CreateTask("restart1", "v2.0.0", devs, 2, 1.0, 300*time.Millisecond); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o1.Start("restart1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	reportBatchIDs(t, o1, "restart1", "v2.0.0", sims, 0)
	// 重启发生在观察期内（下发后约 300ms 到期，这里立即重启）。
	o1.Close()

	store2, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	o2 := NewOrchestrator(store2)
	t.Cleanup(o2.Close)

	tk, _, _ := o2.Snapshot("restart1")
	if tk.Status != TaskRolling || tk.Batches[0].Decided {
		t.Fatalf("重启后应仍处于观察中：status=%s decided=%v", tk.Status, tk.Batches[0].Decided)
	}
	// 到期后自动放量到批次1（重启不重复、不漏掉）。
	waitFor(t, 2*time.Second, func() bool {
		t2, _, _ := o2.Snapshot("restart1")
		return t2.CurrentBatch >= 1
	}, "重启后观察期到点自动放量")
	if got := countEvents(o2, "restart1", "batch_health_check"); got != 1 {
		t.Fatalf("批次0重启前后合计只应判定1次，实际 %d", got)
	}
	// 继续完成批次1。
	reportBatchIDs(t, o2, "restart1", "v2.0.0", sims, 1)
	waitFor(t, 2*time.Second, func() bool {
		t2, _, _ := o2.Snapshot("restart1")
		return t2.Status == TaskCompleted
	}, "重启后续跑完成")
	task, devs2, _ := o2.Snapshot("restart1")
	logSnapshot(t, o2, "restart1", "after-restart-continue")
	for _, b := range task.Batches {
		if !b.Decided || b.Trigger != TriggerAuto {
			t.Fatalf("批次 %d 应自动判定", b.Index)
		}
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded {
			t.Fatalf("设备 %s 结论丢失：%s", id, d.Status)
		}
	}
}

// TestRestartOverdueObservation 停机跨过观察截止时刻：重启后立即补一次自动评估，
// 不重复放量；已判定批次重启后保持结论。
func TestRestartOverdueObservation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o1 := NewOrchestrator(store)
	devs, sims := makeDevices(2)
	if _, err := o1.CreateTask("restart2", "v2.0.0", devs, 2, 1.0, 30*time.Millisecond); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o1.Start("restart2"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	reportBatchIDs(t, o1, "restart2", "v2.0.0", sims, 0)
	o1.Close()
	time.Sleep(120 * time.Millisecond) // 停机期间观察期已过

	store2, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	o2 := NewOrchestrator(store2) // 构造时应立即补评估 → 单批任务完成
	t.Cleanup(o2.Close)

	waitFor(t, time.Second, func() bool {
		tk, _, _ := o2.Snapshot("restart2")
		return tk.Status == TaskCompleted
	}, "重启后补判完成")
	task, devs2, _ := o2.Snapshot("restart2")
	if !task.Batches[0].Decided || task.Batches[0].Trigger != TriggerAuto {
		t.Fatalf("应由重启补偿自动判定，decided=%v trigger=%q", task.Batches[0].Decided, task.Batches[0].Trigger)
	}
	if got := countEvents(o2, "restart2", "batch_health_check"); got != 1 {
		t.Fatalf("补判只能有一次判定事件，实际 %d", got)
	}
	if countEvents(o2, "restart2", "observe_due_restore") != 1 {
		t.Fatal("应记录重启补偿事件 observe_due_restore")
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded {
			t.Fatalf("设备 %s 结论异常：%s", id, d.Status)
		}
	}
	// 再次重启：已完成任务不重建定时器、不重复评估。
	o2.Close()
	store3, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload2: %v", err)
	}
	o3 := NewOrchestrator(store3)
	t.Cleanup(o3.Close)
	time.Sleep(100 * time.Millisecond)
	if got := countEvents(o3, "restart2", "batch_health_check"); got != 1 {
		t.Fatalf("终态任务重启后不得重复判定，实际 %d", got)
	}
}

// TestScale1200Devices 千台以上、每批百台以上自动放量：无死锁、结论正确且耗时可控。
func TestScale1200Devices(t *testing.T) {
	const total = 1200
	const batchSize = 100
	// 观察期需覆盖每批 100 台并发上报的落盘时间（设备分片后与总量解耦；
	// -race 下仍会放慢，故留足余量），保证到点前本批上报全部回收。
	o, _ := createAutoTask(t, "scale1", total, batchSize, 1.0, 800*time.Millisecond)

	start := time.Now()
	deadline := time.Now().Add(60 * time.Second)
	reported := map[int]bool{}
	for time.Now().Before(deadline) {
		tk, _, err := o.Snapshot("scale1")
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if tk.Status == TaskCompleted {
			break
		}
		if tk.Status != TaskRolling {
			t.Fatalf("大规模任务意外状态 %s", tk.Status)
		}
		b := tk.CurrentBatch
		if b >= 0 && !reported[b] {
			reported[b] = true
			ids := tk.Batches[b].DeviceIDs
			var wg sync.WaitGroup
			for _, id := range ids {
				wg.Add(1)
				go func(id string) {
					defer wg.Done()
					// 直接构造成功+ack上报并发打入，避免创建上千个 SimDevice。
					_ = o.HandleReport(Report{DeviceID: id, TaskID: "scale1", Seq: 1, Type: ReportAck})
					_ = o.HandleReport(Report{DeviceID: id, TaskID: "scale1", Seq: 2, Type: ReportSuccess, Version: "v2.0.0"})
				}(id)
			}
			wg.Wait()
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Now().Sub(start)

	task, devs, _ := o.Snapshot("scale1")
	if task.Status != TaskCompleted {
		t.Fatalf("千台任务应自动完成，实际 %s", task.Status)
	}
	if len(task.Batches) != total/batchSize {
		t.Fatalf("批次数异常：%d", len(task.Batches))
	}
	succeeded := 0
	for _, d := range devs {
		if d.Status == DeviceSucceeded && d.CurrentVersion == "v2.0.0" {
			succeeded++
		}
	}
	if succeeded != total {
		t.Fatalf("应收敛 %d 台成功，实际 %d", total, succeeded)
	}
	for _, b := range task.Batches {
		if !b.Decided || b.Trigger != TriggerAuto {
			t.Fatalf("批次 %d 未自动判定", b.Index)
		}
	}
	t.Logf("千台规模自动放量完成：devices=%d batches=%d 耗时=%s", total, len(task.Batches), elapsed)
	if elapsed > 30*time.Second {
		t.Fatalf("千台规模耗时过长：%s", elapsed)
	}
}
