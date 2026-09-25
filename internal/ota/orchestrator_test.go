package ota

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func newEnv(t *testing.T) (*Orchestrator, *Store) {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return NewOrchestrator(store), store
}

// logSnapshot 打印批次、设备状态与判定依据。
func logSnapshot(t *testing.T, o *Orchestrator, taskID, phase string) {
	t.Helper()
	task, devs, err := o.Snapshot(taskID)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Logf("[%s] task=%s status=%s currentBatch=%d", phase, taskID, task.Status, task.CurrentBatch)
	for _, b := range task.Batches {
		t.Logf("  batch=%d started=%v healthy=%v devices=%v", b.Index, b.Started, b.Healthy, b.DeviceIDs)
	}
	for id, d := range devs {
		t.Logf("  device=%s batch=%d status=%s version=%s target=%s online=%v lastSeq=%d reason=%q",
			id, d.BatchIndex, d.Status, d.CurrentVersion, d.TargetVersion, d.Online, d.LastSeq, d.FailReason)
	}
}

// deliverAndReport 模拟向批次内设备下发并回收上报。
func deliverAndReport(t *testing.T, o *Orchestrator, taskID string, sims map[string]*SimDevice, batchIdx int) {
	t.Helper()
	task, _, err := o.Snapshot(taskID)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for _, id := range task.Batches[batchIdx].DeviceIDs {
		for _, r := range sims[id].Deliver(taskID, task.TargetVersion) {
			if err := o.HandleReport(r); err != nil {
				t.Fatalf("HandleReport: %v", err)
			}
		}
	}
}

func makeDevices(n int) (map[string]string, map[string]*SimDevice) {
	devs := map[string]string{}
	sims := map[string]*SimDevice{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%03d", i)
		devs[id] = "v1.0.0"
		sims[id] = NewSimDevice(id, "v1.0.0")
	}
	return devs, sims
}

// TestHappyPathRollout 分批灰度推进，全部健康，版本收敛到目标版本。
func TestHappyPathRollout(t *testing.T) {
	o, _ := newEnv(t)
	devs, sims := makeDevices(6)
	if _, err := o.CreateTask("t1", "v2.0.0", devs, 2, 0.8); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for batch := 0; batch < 3; batch++ {
		deliverAndReport(t, o, "t1", sims, batch)
		healthy, err := o.Advance("t1")
		if err != nil {
			t.Fatalf("Advance batch=%d: %v", batch, err)
		}
		t.Logf("batch=%d 健康判定 healthy=%v", batch, healthy)
		if !healthy {
			t.Fatalf("batch=%d 应健康", batch)
		}
		logSnapshot(t, o, "t1", fmt.Sprintf("after-batch-%d", batch))
	}
	task, devs2, _ := o.Snapshot("t1")
	if task.Status != TaskCompleted {
		t.Fatalf("任务应完成，实际 %s", task.Status)
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
			t.Fatalf("设备 %s 未收敛：status=%s version=%s", id, d.Status, d.CurrentVersion)
		}
	}
}

// TestOfflineReconnect 设备离线上报缺失 → 健康判定隔离；重连补报不改变已隔离结论。
func TestOfflineReconnect(t *testing.T) {
	o, _ := newEnv(t)
	devs, sims := makeDevices(2)
	sims["dev-001"].DropReports = true // 故障注入：离线上报缺失
	if _, err := o.CreateTask("t1", "v2.0.0", devs, 2, 0.6); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)
	healthy, err := o.Advance("t1")
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	logSnapshot(t, o, "t1", "after-advance")
	if healthy {
		t.Fatal("dev-001 上报缺失不计入成功，成功率 1/2=0.5 < 阈值 0.6，判定不应通过")
	}
	_, devs2, _ := o.Snapshot("t1")
	if devs2["dev-001"].Status != DeviceQuarantined {
		t.Fatalf("dev-001 应被隔离，实际 %s", devs2["dev-001"].Status)
	}
	// 重连补报：已隔离的终态不得被改写
	sims["dev-001"].DropReports = false
	sims["dev-001"].Version = "v2.0.0"
	sims["dev-001"].pendingTask = "t1"
	sims["dev-001"].pendingTarget = "v2.0.0"
	for _, r := range sims["dev-001"].Reconnect("t1") {
		if err := o.HandleReport(r); err != nil {
			t.Fatalf("HandleReport: %v", err)
		}
	}
	_, devs3, _ := o.Snapshot("t1")
	logSnapshot(t, o, "t1", "after-reconnect")
	if devs3["dev-001"].Status != DeviceQuarantined {
		t.Fatalf("终态不得被重连上报改写，实际 %s", devs3["dev-001"].Status)
	}
	if !devs3["dev-001"].Online {
		t.Fatal("重连后设备应在线")
	}
}

// TestDuplicateAndOutOfOrderReports 重复与乱序上报被丢弃，终态不回退。
func TestDuplicateAndOutOfOrderReports(t *testing.T) {
	o, _ := newEnv(t)
	devs, _ := makeDevices(1)
	if _, err := o.CreateTask("t1", "v2.0.0", devs, 1, 1.0); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// 乱序：success(seq=2) 先于 ack(seq=1) 到达
	reports := []Report{
		{DeviceID: "dev-000", TaskID: "t1", Seq: 2, Type: ReportSuccess, Version: "v2.0.0"},
		{DeviceID: "dev-000", TaskID: "t1", Seq: 1, Type: ReportAck},                        // 乱序旧报文
		{DeviceID: "dev-000", TaskID: "t1", Seq: 2, Type: ReportSuccess, Version: "v2.0.0"}, // 重复
		{DeviceID: "dev-000", TaskID: "t1", Seq: 3, Type: ReportFailure, Reason: "迟到的失败报文"}, // 终态后上报
	}
	for _, r := range reports {
		if err := o.HandleReport(r); err != nil {
			t.Fatalf("HandleReport: %v", err)
		}
	}
	_, devs2, _ := o.Snapshot("t1")
	logSnapshot(t, o, "t1", "after-out-of-order")
	d := devs2["dev-000"]
	if d.Status != DeviceSucceeded {
		t.Fatalf("终态应保持 succeeded，实际 %s", d.Status)
	}
	if d.CurrentVersion != "v2.0.0" {
		t.Fatalf("版本不得回退，实际 %s", d.CurrentVersion)
	}
	if d.LastSeq != 3 {
		t.Fatalf("LastSeq 应为 3，实际 %d", d.LastSeq)
	}
}

// TestFailureRollbackAndPause 失败设备回滚到已知可用版本，健康判定不通过则暂停。
func TestFailureRollbackAndPause(t *testing.T) {
	o, _ := newEnv(t)
	devs, sims := makeDevices(4)
	sims["dev-001"].FailNextUpgrade = true // 故障注入
	sims["dev-003"].FailNextUpgrade = true
	if _, err := o.CreateTask("t1", "v2.0.0", devs, 2, 0.9); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)
	healthy, err := o.Advance("t1")
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	logSnapshot(t, o, "t1", "after-batch0-unhealthy")
	if healthy {
		t.Fatal("成功率 1/2 < 0.9，判定不应通过")
	}
	task, devs2, _ := o.Snapshot("t1")
	if task.Status != TaskPaused {
		t.Fatalf("健康判定不通过应自动暂停，实际 %s", task.Status)
	}
	d := devs2["dev-001"]
	if d.Status != DeviceRolledBack || d.CurrentVersion != "v1.0.0" {
		t.Fatalf("dev-001 应回滚到 v1.0.0，实际 status=%s version=%s", d.Status, d.CurrentVersion)
	}
	// 暂停后终止：剩余设备按策略处理
	if err := o.Abort("t1"); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	task, devs3, _ := o.Snapshot("t1")
	logSnapshot(t, o, "t1", "after-abort")
	if task.Status != TaskAborted {
		t.Fatalf("任务应终止，实际 %s", task.Status)
	}
	if devs3["dev-002"].Status != DeviceQuarantined || devs3["dev-003"].Status != DeviceQuarantined {
		t.Fatalf("未下发批次设备应隔离：dev-002=%s dev-003=%s",
			devs3["dev-002"].Status, devs3["dev-003"].Status)
	}
}

// TestConcurrentPushAndReports 多设备并发上报与并发推进，状态不丢失、版本不错乱。
func TestConcurrentPushAndReports(t *testing.T) {
	o, _ := newEnv(t)
	const n = 40
	devs, sims := makeDevices(n)
	if _, err := o.CreateTask("t1", "v2.0.0", devs, 10, 0.8); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%03d", i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for _, r := range sims[id].Deliver("t1", "v2.0.0") {
				_ = o.HandleReport(r)
			}
		}(id)
	}
	wg.Wait()
	healthy, err := o.Advance("t1")
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	t.Logf("并发批次健康判定 healthy=%v", healthy)
	if !healthy {
		t.Fatal("全部成功，判定应通过")
	}
	logSnapshot(t, o, "t1", "after-concurrent-batch0")
	_, devs2, _ := o.Snapshot("t1")
	succeeded := 0
	for _, d := range devs2 {
		if d.Status == DeviceSucceeded && d.CurrentVersion == "v2.0.0" {
			succeeded++
		}
	}
	if succeeded != 10 {
		t.Fatalf("批次0应有 10 台成功，实际 %d", succeeded)
	}
}

// TestPersistenceAcrossRestart 进程重启后已确认结论保持稳定。
func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	o := NewOrchestrator(store)
	devs, sims := makeDevices(2)
	if _, err := o.CreateTask("t1", "v2.0.0", devs, 2, 0.5); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deliverAndReport(t, o, "t1", sims, 0)
	if _, err := o.Advance("t1"); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	// 模拟进程重启：重新加载存储
	store2, err := NewStore(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	o2 := NewOrchestrator(store2)
	task, devs2, err := o2.Snapshot("t1")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	logSnapshot(t, o2, "t1", "after-restart")
	if task.Status != TaskCompleted {
		t.Fatalf("重启后任务状态应为 completed，实际 %s", task.Status)
	}
	for id, d := range devs2 {
		if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
			t.Fatalf("重启后设备 %s 结论丢失：status=%s version=%s", id, d.Status, d.CurrentVersion)
		}
	}
	if len(o2.Events()) == 0 {
		t.Fatal("重启后事件追溯不应丢失")
	}
}

// TestIdempotentRedeliver 同一升级重复下发不造成重复安装。
func TestIdempotentRedeliver(t *testing.T) {
	o, _ := newEnv(t)
	devs, sims := makeDevices(1)
	if _, err := o.CreateTask("t1", "v2.0.0", devs, 1, 1.0); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := o.Start("t1"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	first := sims["dev-000"].Deliver("t1", "v2.0.0")
	second := sims["dev-000"].Deliver("t1", "v2.0.0") // 重复下发
	for _, r := range append(first, second...) {
		if err := o.HandleReport(r); err != nil {
			t.Fatalf("HandleReport: %v", err)
		}
	}
	_, devs2, _ := o.Snapshot("t1")
	logSnapshot(t, o, "t1", "after-redeliver")
	d := devs2["dev-000"]
	if d.Status != DeviceSucceeded || d.CurrentVersion != "v2.0.0" {
		t.Fatalf("重复下发后状态异常：status=%s version=%s", d.Status, d.CurrentVersion)
	}
}
