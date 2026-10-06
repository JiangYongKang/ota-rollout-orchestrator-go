package ota

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// waitFor 轮询等待条件成立，超时则失败。
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// countHealthChecks 统计某批次形成健康判定结论（batch_health_check）的次数。
func countHealthChecks(o *Orchestrator, taskID string, batchIdx int) int {
	n := 0
	for _, e := range o.Events() {
		if e.TaskID == taskID && e.Kind == "batch_health_check" &&
			strings.Contains(e.Detail, fmt.Sprintf("batch=%d ", batchIdx)) {
			n++
		}
	}
	return n
}

// logTaskEvents 打印任务相关事件（含触发来源与判定依据）。
func logTaskEvents(t *testing.T, o *Orchestrator, taskID string) {
	t.Helper()
	for _, e := range o.Events() {
		if e.TaskID == taskID {
			t.Logf("  event [%s] dev=%s %s: %s",
				e.At.Format("15:04:05.000"), e.DeviceID, e.Kind, e.Detail)
		}
	}
}

// TestAutoAdvanceAfterObservePeriod 观察期到点后自动评估并放量，全程无需人工 Advance。
func TestAutoAdvanceAfterObservePeriod(t *testing.T) {
	o, _ := newEnv(t)
	defer o.Close()
	devs, sims := makeDevices(4)
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 2, 0.8, 100*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// 批次设备上报后，等待观察期到点自动放量
	for batch := 0; batch < 2; batch++ {
		deliverAndReport(t, o, "t1", sims, batch)
		waitFor(t, fmt.Sprintf("batch=%d 自动评估", batch), 3*time.Second, func() bool {
			task, _, _ := o.Snapshot("t1")
			return task.Batches[batch].Evaluated
		})
		task, _, _ := o.Snapshot("t1")
		t.Logf("batch=%d 自动评估完成 healthy=%v currentBatch=%d status=%s",
			batch, task.Batches[batch].Healthy, task.CurrentBatch, task.Status)
		if !task.Batches[batch].Healthy {
			t.Fatalf("batch=%d 全部成功，自动判定应健康", batch)
		}
	}
	task, devs2, _ := o.Snapshot("t1")
	if task.Status != TaskCompleted {
		t.Fatalf("任务应自动完成，实际 %s", task.Status)
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
			t.Fatalf("设备 %s 未收敛：status=%s version=%s", id, d.Status, d.CurrentVersion)
		}
	}
	// 每批恰好评估一次，且由自动调度触发
	for batch := 0; batch < 2; batch++ {
		if n := countHealthChecks(o, "t1", batch); n != 1 {
			t.Fatalf("batch=%d 应只评估一次，实际 %d 次", batch, n)
		}
	}
	logTaskEvents(t, o, "t1")
}

// TestManualAdvanceBeforeTimerFires 观察期内人工推进立即生效，定时器到点不得重复评估同一批。
func TestManualAdvanceBeforeTimerFires(t *testing.T) {
	o, _ := newEnv(t)
	defer o.Close()
	devs, sims := makeDevices(4)
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 2, 0.8, 250*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)
	// 观察期远未到期，人工立即推进
	healthy, err := o.Advance("t1")
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if !healthy {
		t.Fatal("全部成功，人工判定应健康")
	}
	task, _, _ := o.Snapshot("t1")
	if task.CurrentBatch != 1 {
		t.Fatalf("人工推进后应进入批次1，实际 %d", task.CurrentBatch)
	}
	// 批次1 进入新观察期；上报后等待原批次0的定时器到点（250ms），确认不会重复评估批次0
	deliverAndReport(t, o, "t1", sims, 1)
	waitFor(t, "批次1 自动评估", 3*time.Second, func() bool {
		task, _, _ := o.Snapshot("t1")
		return task.Batches[1].Evaluated
	})
	time.Sleep(300 * time.Millisecond) // 覆盖批次0原定时器到点时刻
	if n := countHealthChecks(o, "t1", 0); n != 1 {
		t.Fatalf("批次0 应只评估一次，实际 %d 次", n)
	}
	if n := countHealthChecks(o, "t1", 1); n != 1 {
		t.Fatalf("批次1 应只评估一次，实际 %d 次", n)
	}
	task, _, _ = o.Snapshot("t1")
	if task.Status != TaskCompleted {
		t.Fatalf("两批均健康，任务应完成，实际 %s", task.Status)
	}
	logTaskEvents(t, o, "t1")
}

// TestConcurrentManualAndAutoTriggers 人工推进与自动调度并发、同一批次被反复触发，
// 最终只形成一次确定结论，且已确认的设备结果不被改写。
func TestConcurrentManualAndAutoTriggers(t *testing.T) {
	o, _ := newEnv(t)
	defer o.Close()
	devs, sims := makeDevices(4)
	// 单批任务，观察期 150ms：人工并发 Advance 与定时器同时竞争
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 4, 0.8, 150*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = o.Advance("t1") // 反复触发同一批
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	waitFor(t, "批次0 形成结论", 3*time.Second, func() bool {
		task, _, _ := o.Snapshot("t1")
		return task.Batches[0].Evaluated
	})
	// 再多触发几轮（含定时器早已到点），结论次数不得增加
	for i := 0; i < 5; i++ {
		_, _ = o.Advance("t1")
	}
	time.Sleep(300 * time.Millisecond)
	if n := countHealthChecks(o, "t1", 0); n != 1 {
		t.Fatalf("批次0 被反复触发，但只能评估一次，实际 %d 次", n)
	}
	task, devs2, _ := o.Snapshot("t1")
	if task.Status != TaskCompleted {
		t.Fatalf("全部成功，任务应完成，实际 %s", task.Status)
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
			t.Fatalf("设备 %s 结论被改写：status=%s version=%s", id, d.Status, d.CurrentVersion)
		}
	}
	logTaskEvents(t, o, "t1")
}

// TestReportsAroundEvaluationBoundary 评估边界前后到达的上报：
// 评估前到达的成功计入结论；评估后到达的成功/失败不得改写已隔离或已成功的终态。
func TestReportsAroundEvaluationBoundary(t *testing.T) {
	o, _ := newEnv(t)
	defer o.Close()
	devs, sims := makeDevices(2)
	sims["dev-001"].DropReports = true // dev-001 暂不上报
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 2, 0.5, 5*time.Second); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// 评估前：dev-000 上报成功
	for _, r := range sims["dev-000"].Deliver("t1", "v2.0.0") {
		if err := o.HandleReport(r); err != nil {
			t.Fatalf("HandleReport: %v", err)
		}
	}
	// 观察期内人工提前评估：dev-001 上报缺失被隔离
	healthy, err := o.Advance("t1")
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if !healthy {
		t.Fatal("成功率 1/2 = 0.5 达到阈值 0.5，判定应通过")
	}
	_, devs2, _ := o.Snapshot("t1")
	if devs2["dev-001"].Status != DeviceQuarantined {
		t.Fatalf("dev-001 应被隔离，实际 %s", devs2["dev-001"].Status)
	}
	// 评估后：dev-001 迟到的成功上报不得改写隔离结论；dev-000 迟到的失败上报不得改写成功
	sims["dev-001"].DropReports = false
	sims["dev-001"].Version = "v2.0.0"
	sims["dev-001"].pendingTask = "t1"
	sims["dev-001"].pendingTarget = "v2.0.0"
	late := sims["dev-001"].Reconnect("t1")
	late = append(late, Report{DeviceID: "dev-000", TaskID: "t1", Seq: 3, Type: ReportFailure, Reason: "迟到的失败"})
	for _, r := range late {
		if err := o.HandleReport(r); err != nil {
			t.Fatalf("HandleReport: %v", err)
		}
	}
	_, devs3, _ := o.Snapshot("t1")
	logSnapshot(t, o, "t1", "after-late-reports")
	if devs3["dev-001"].Status != DeviceQuarantined {
		t.Fatalf("dev-001 隔离终态不得被迟到成功改写，实际 %s", devs3["dev-001"].Status)
	}
	if devs3["dev-000"].Status != DeviceSucceeded || devs3["dev-000"].CurrentVersion != "v2.0.0" {
		t.Fatalf("dev-000 终态不得被迟到失败改写：status=%s version=%s",
			devs3["dev-000"].Status, devs3["dev-000"].CurrentVersion)
	}
}

// TestPauseResumeAbortDuringObservation 观察期内人工暂停/恢复/终止立即生效：
// 暂停后定时器不触发；恢复后重新计时并自动评估；终止后定时器不再触发。
func TestPauseResumeAbortDuringObservation(t *testing.T) {
	o, _ := newEnv(t)
	defer o.Close()
	devs, sims := makeDevices(2)
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 2, 0.8, 120*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)
	// 观察期内暂停：定时器到点也不得评估
	if err := o.Pause("t1"); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	time.Sleep(250 * time.Millisecond) // 覆盖原定时器到点时刻
	task, _, _ := o.Snapshot("t1")
	if task.Status != TaskPaused || task.Batches[0].Evaluated {
		t.Fatalf("暂停期间定时器不得评估：status=%s evaluated=%v", task.Status, task.Batches[0].Evaluated)
	}
	// 恢复：重新计时，到点自动评估并完成
	if err := o.Resume("t1"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitFor(t, "恢复后自动评估", 3*time.Second, func() bool {
		task, _, _ := o.Snapshot("t1")
		return task.Batches[0].Evaluated
	})
	task, _, _ = o.Snapshot("t1")
	if task.Status != TaskCompleted {
		t.Fatalf("恢复后应自动评估并完成，实际 %s", task.Status)
	}
	if n := countHealthChecks(o, "t1", 0); n != 1 {
		t.Fatalf("批次0 应只评估一次，实际 %d 次", n)
	}

	// 终止场景：新任务观察期内终止，定时器不再触发
	devs2, sims2 := makeDevices(2)
	if _, err := o.CreateTaskWithObserve("t2", "v2.0.0", devs2, 2, 0.8, 100*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve t2: %v", err)
	}
	if err := o.Start("t2"); err != nil {
		t.Fatalf("Start t2: %v", err)
	}
	deliverAndReport(t, o, "t2", sims2, 0)
	if err := o.Abort("t2"); err != nil {
		t.Fatalf("Abort t2: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // 覆盖原定时器到点时刻
	task2, _, _ := o.Snapshot("t2")
	if task2.Status != TaskAborted || task2.Batches[0].Evaluated {
		t.Fatalf("终止后定时器不得评估：status=%s evaluated=%v", task2.Status, task2.Batches[0].Evaluated)
	}
	logTaskEvents(t, o, "t1")
	logTaskEvents(t, o, "t2")
}

// TestMultiTaskParallelObservation 多个任务并行观察/推进，互不干扰；
// 无观察期的手动任务不受自动调度影响。
func TestMultiTaskParallelObservation(t *testing.T) {
	o, _ := newEnv(t)
	defer o.Close()
	const tasks = 3
	simsByTask := map[string]map[string]*SimDevice{}
	for i := 0; i < tasks; i++ {
		taskID := fmt.Sprintf("auto-%d", i)
		// 设备全局唯一：按任务加前缀，避免跨任务设备ID冲突
		devs := map[string]string{}
		sims := map[string]*SimDevice{}
		for j := 0; j < 4; j++ {
			id := fmt.Sprintf("%s-dev-%03d", taskID, j)
			devs[id] = "v1.0.0"
			sims[id] = NewSimDevice(id, "v1.0.0")
		}
		simsByTask[taskID] = sims
		if _, err := o.CreateTaskWithObserve(taskID, "v2.0.0", devs, 2, 0.8,
			time.Duration(80+i*40)*time.Millisecond); err != nil {
			t.Fatalf("CreateTaskWithObserve %s: %v", taskID, err)
		}
		if err := o.Start(taskID); err != nil {
			t.Fatalf("Start %s: %v", taskID, err)
		}
	}
	// 手动任务（无观察期）
	manualDevs, manualSims := makeDevices(2)
	if _, err := o.CreateTask("manual", "v2.0.0", manualDevs, 2, 0.8); err != nil {
		t.Fatalf("CreateTask manual: %v", err)
	}
	if err := o.Start("manual"); err != nil {
		t.Fatalf("Start manual: %v", err)
	}
	deliverAndReport(t, o, "manual", manualSims, 0)

	// 并发向各自动任务批次内设备下发并回收上报
	var wg sync.WaitGroup
	for i := 0; i < tasks; i++ {
		taskID := fmt.Sprintf("auto-%d", i)
		wg.Add(1)
		go func(taskID string) {
			defer wg.Done()
			sims := simsByTask[taskID]
			for batch := 0; batch < 2; batch++ {
				task, _, _ := o.Snapshot(taskID)
				for _, id := range task.Batches[batch].DeviceIDs {
					for _, r := range sims[id].Deliver(taskID, "v2.0.0") {
						_ = o.HandleReport(r)
					}
				}
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					task, _, _ := o.Snapshot(taskID)
					if task.Batches[batch].Evaluated {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(taskID)
	}
	wg.Wait()

	for i := 0; i < tasks; i++ {
		taskID := fmt.Sprintf("auto-%d", i)
		task, devs2, _ := o.Snapshot(taskID)
		if task.Status != TaskCompleted {
			t.Fatalf("%s 应自动完成，实际 %s", taskID, task.Status)
		}
		for id, d := range devs2 {
			if d.Status != DeviceSucceeded {
				t.Fatalf("%s 设备 %s 未成功：%s", taskID, id, d.Status)
			}
		}
		for batch := 0; batch < 2; batch++ {
			if n := countHealthChecks(o, taskID, batch); n != 1 {
				t.Fatalf("%s batch=%d 应只评估一次，实际 %d 次", taskID, batch, n)
			}
		}
		logTaskEvents(t, o, taskID)
	}
	// 手动任务：自动调度不得触碰，仍需人工 Advance
	task, _, _ := o.Snapshot("manual")
	if task.Status != TaskRolling || task.Batches[0].Evaluated {
		t.Fatalf("手动任务不应被自动评估：status=%s evaluated=%v", task.Status, task.Batches[0].Evaluated)
	}
	healthy, err := o.Advance("manual")
	if err != nil || !healthy {
		t.Fatalf("手动任务人工推进失败：healthy=%v err=%v", healthy, err)
	}
}

// reopen 模拟进程重启：关闭旧编排器（停掉定时器），从同一状态文件重新加载。
func reopen(t *testing.T, o *Orchestrator, path string) *Orchestrator {
	t.Helper()
	o.Close()
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	return NewOrchestrator(store)
}

// TestRestartResumesPendingObservation 重启后未到期的观察期按剩余时间续跑，到点自动评估。
func TestRestartResumesPendingObservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o := NewOrchestrator(store)
	devs, sims := makeDevices(2)
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 2, 0.8, 500*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)

	// 观察期未到期时重启
	o = reopen(t, o, path)
	defer o.Close()
	waitFor(t, "重启后续跑观察期并自动评估", 3*time.Second, func() bool {
		task, _, _ := o.Snapshot("t1")
		return task.Batches[0].Evaluated
	})
	task, devs2, _ := o.Snapshot("t1")
	if task.Status != TaskCompleted {
		t.Fatalf("重启后应自动完成，实际 %s", task.Status)
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded {
			t.Fatalf("设备 %s 结论丢失：%s", id, d.Status)
		}
	}
	if n := countHealthChecks(o, "t1", 0); n != 1 {
		t.Fatalf("批次0 应只评估一次（含重启前），实际 %d 次", n)
	}
	logTaskEvents(t, o, "t1")
}

// TestRestartEvaluatesExpiredObservation 停机期间观察期已到期，重启后立即补评，不漏放量。
func TestRestartEvaluatesExpiredObservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o := NewOrchestrator(store)
	devs, sims := makeDevices(2)
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 2, 0.8, 80*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)

	// 停机 200ms（观察期 80ms 在停机期间到期），重启后应立即补评
	o.Close()
	time.Sleep(200 * time.Millisecond)
	store2, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	o2 := NewOrchestrator(store2)
	defer o2.Close()
	task, _, _ := o2.Snapshot("t1")
	if !task.Batches[0].Evaluated {
		t.Fatal("停机期间到期的观察期应在重启时立即补评")
	}
	if task.Status != TaskCompleted {
		t.Fatalf("补评后任务应完成，实际 %s", task.Status)
	}
	if n := countHealthChecks(o2, "t1", 0); n != 1 {
		t.Fatalf("批次0 应只补评一次，实际 %d 次", n)
	}
	logTaskEvents(t, o2, "t1")
}

// TestRestartKeepsFormedConclusions 重启前已形成的判定与终态结论，重启后不重复评估、不重复放量。
func TestRestartKeepsFormedConclusions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o := NewOrchestrator(store)
	devs, sims := makeDevices(4)
	if _, err := o.CreateTaskWithObserve("t1", "v2.0.0", devs, 2, 0.8, 60*time.Millisecond); err != nil {
		t.Fatalf("CreateTaskWithObserve: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for batch := 0; batch < 2; batch++ {
		deliverAndReport(t, o, "t1", sims, batch)
		waitFor(t, fmt.Sprintf("batch=%d 自动评估", batch), 3*time.Second, func() bool {
			task, _, _ := o.Snapshot("t1")
			return task.Batches[batch].Evaluated
		})
	}
	eventsBefore := len(o.Events())

	// 任务已完成时重启：不得产生任何新的评估或下发
	o = reopen(t, o, path)
	defer o.Close()
	time.Sleep(200 * time.Millisecond)
	task, devs2, _ := o.Snapshot("t1")
	if task.Status != TaskCompleted {
		t.Fatalf("重启后任务应保持 completed，实际 %s", task.Status)
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
			t.Fatalf("重启后设备 %s 结论被改写：status=%s version=%s", id, d.Status, d.CurrentVersion)
		}
	}
	if len(o.Events()) != eventsBefore {
		t.Fatalf("重启后不应产生新事件：before=%d after=%d", eventsBefore, len(o.Events()))
	}
	for batch := 0; batch < 2; batch++ {
		if n := countHealthChecks(o, "t1", batch); n != 1 {
			t.Fatalf("batch=%d 应只评估一次，实际 %d 次", batch, n)
		}
	}
}

// TestScaleManyDevicesParallelTasks 规模验收：1200 台设备、每批 150 台、3 个任务并行自动放量，
// 不应出现死锁、状态丢失或明显变慢。
func TestScaleManyDevicesParallelTasks(t *testing.T) {
	o, _ := newEnv(t)
	defer o.Close()
	const (
		numTasks   = 3
		perTask    = 400
		batchSize  = 150
		numBatches = 3 // 150+150+100
	)
	simsByTask := map[string]map[string]*SimDevice{}
	start := time.Now()
	for i := 0; i < numTasks; i++ {
		taskID := fmt.Sprintf("scale-%d", i)
		devs := map[string]string{}
		sims := map[string]*SimDevice{}
		for j := 0; j < perTask; j++ {
			id := fmt.Sprintf("%s-dev-%04d", taskID, j)
			devs[id] = "v1.0.0"
			sims[id] = NewSimDevice(id, "v1.0.0")
		}
		simsByTask[taskID] = sims
		if _, err := o.CreateTaskWithObserve(taskID, "v2.0.0", devs, batchSize, 0.9, 2*time.Second); err != nil {
			t.Fatalf("CreateTaskWithObserve %s: %v", taskID, err)
		}
		if err := o.Start(taskID); err != nil {
			t.Fatalf("Start %s: %v", taskID, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < numTasks; i++ {
		taskID := fmt.Sprintf("scale-%d", i)
		wg.Add(1)
		go func(taskID string) {
			defer wg.Done()
			sims := simsByTask[taskID]
			for batch := 0; batch < numBatches; batch++ {
				task, _, _ := o.Snapshot(taskID)
				for _, id := range task.Batches[batch].DeviceIDs {
					for _, r := range sims[id].Deliver(taskID, "v2.0.0") {
						_ = o.HandleReport(r)
					}
				}
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) {
					task, _, _ := o.Snapshot(taskID)
					if task.Batches[batch].Evaluated {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(taskID)
	}
	wg.Wait()

	for i := 0; i < numTasks; i++ {
		taskID := fmt.Sprintf("scale-%d", i)
		task, devs2, _ := o.Snapshot(taskID)
		if task.Status != TaskCompleted {
			t.Fatalf("%s 应完成，实际 %s", taskID, task.Status)
		}
		succeeded := 0
		for _, d := range devs2 {
			if d.Status == DeviceSucceeded && d.CurrentVersion == "v2.0.0" {
				succeeded++
			}
		}
		if succeeded != perTask {
			t.Fatalf("%s 应全部成功 %d 台，实际 %d 台", taskID, perTask, succeeded)
		}
		for batch := 0; batch < numBatches; batch++ {
			if n := countHealthChecks(o, taskID, batch); n != 1 {
				t.Fatalf("%s batch=%d 应只评估一次，实际 %d 次", taskID, batch, n)
			}
		}
	}
	t.Logf("规模验收：%d 台设备、%d 个任务、每批 %d 台，总耗时 %s",
		numTasks*perTask, numTasks, batchSize, time.Since(start).Round(time.Millisecond))
}
