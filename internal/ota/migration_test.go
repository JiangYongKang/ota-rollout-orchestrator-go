package ota

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestLegacyStoreMigration 旧版（devices/events 内嵌主 JSON、无观察期）状态文件
// 能被新版本加载：设备分片与事件日志自动迁移，历史任务保持手动推进、结论不变。
func TestLegacyStoreMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	legacy := map[string]interface{}{
		"tasks": map[string]interface{}{
			"old": map[string]interface{}{
				"id": "old", "target_version": "v2.0.0", "status": "rolling",
				"current_batch": 0, "min_success_rate": 1.0,
				"batches": []map[string]interface{}{
					{"index": 0, "device_ids": []string{"d1", "d2"}, "started": true, "healthy": true},
				},
			},
		},
		"devices": map[string]interface{}{
			"d1": map[string]interface{}{"id": "d1", "current_version": "v2.0.0", "online": true, "status": "succeeded", "batch_index": 0, "last_seq": 2},
			"d2": map[string]interface{}{"id": "d2", "current_version": "v1.0.0", "online": true, "status": "delivered", "batch_index": 0, "target_version": "v2.0.0", "last_seq": 1},
		},
		"events": []map[string]interface{}{
			{"at": "2026-01-01T00:00:00Z", "task_id": "old", "kind": "task_created", "detail": "legacy"},
		},
	}
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("加载旧文件失败: %v", err)
	}
	o := NewOrchestrator(store)
	t.Cleanup(o.Close)

	task, devs, err := o.Snapshot("old")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if task.Status != TaskRolling || task.CurrentBatch != 0 || task.ObservePeriod != 0 {
		t.Fatalf("历史任务状态/观察期异常：%s cb=%d period=%v", task.Status, task.CurrentBatch, task.ObservePeriod)
	}
	if devs["d1"].Status != DeviceSucceeded || devs["d1"].CurrentVersion != "v2.0.0" {
		t.Fatalf("d1 结论迁移错误：%+v", devs["d1"])
	}
	if len(o.Events()) != 1 || o.Events()[0].Kind != "task_created" {
		t.Fatalf("历史事件迁移错误：%+v", o.Events())
	}
	// 无观察期：不自动推进，保持手动；d2 未上报成功 → 手动判定不健康并暂停。
	healthy, err := o.Advance("old")
	if err != nil {
		t.Fatalf("历史任务手动 Advance: %v", err)
	}
	if healthy {
		t.Fatal("d2 未成功，手动判定应不健康")
	}
	task2, _, _ := o.Snapshot("old")
	if task2.Status != TaskPaused {
		t.Fatalf("应保持手动判定后暂停，实际 %s", task2.Status)
	}
	if _, err := os.Stat(path + ".events.jsonl"); err != nil {
		t.Fatalf("事件日志未迁移: %v", err)
	}
	if shardFiles, _ := filepath.Glob(path + ".d*"); len(shardFiles) == 0 {
		t.Fatal("设备分片未生成")
	}
}
