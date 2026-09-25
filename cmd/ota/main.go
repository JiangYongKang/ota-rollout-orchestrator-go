// ota 是本地灰度升级编排演示入口：用模拟设备群与本地 JSON 状态存储
// 演示分批推送、健康判定、失败回滚与状态追溯。
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/highcumontoa/ota-rollout-orchestrator-go/internal/ota"
)

func main() {
	statePath := flag.String("state", "ota-state.json", "本地状态存储路径")
	target := flag.String("target", "v2.0.0", "目标固件版本")
	numDevices := flag.Int("devices", 6, "模拟设备数量")
	batchSize := flag.Int("batch-size", 2, "每批设备数")
	minRate := flag.Float64("min-success-rate", 0.8, "批次健康判定成功率阈值")
	failDevice := flag.String("fail-device", "", "注入升级失败的设备ID（如 dev-001）")
	flag.Parse()

	store, err := ota.NewStore(*statePath)
	if err != nil {
		log.Fatalf("打开状态存储: %v", err)
	}
	o := ota.NewOrchestrator(store)

	devs := map[string]string{}
	sims := map[string]*ota.SimDevice{}
	for i := 0; i < *numDevices; i++ {
		id := fmt.Sprintf("dev-%03d", i)
		devs[id] = "v1.0.0"
		sims[id] = ota.NewSimDevice(id, "v1.0.0")
	}
	if *failDevice != "" {
		if s, ok := sims[*failDevice]; ok {
			s.FailNextUpgrade = true
		}
	}

	const taskID = "demo-task"
	if _, err := o.CreateTask(taskID, *target, devs, *batchSize, *minRate); err != nil {
		log.Fatalf("创建任务: %v", err)
	}
	if err := o.Start(taskID); err != nil {
		log.Fatalf("启动任务: %v", err)
	}

	task, _, _ := o.Snapshot(taskID)
	for batch := 0; batch < len(task.Batches); batch++ {
		fmt.Printf("== 下发批次 %d ==\n", batch)
		task, _, _ = o.Snapshot(taskID)
		for _, id := range task.Batches[batch].DeviceIDs {
			for _, r := range sims[id].Deliver(taskID, *target) {
				if err := o.HandleReport(r); err != nil {
					log.Fatalf("处理上报: %v", err)
				}
			}
		}
		healthy, err := o.Advance(taskID)
		if err != nil {
			log.Fatalf("推进: %v", err)
		}
		fmt.Printf("批次 %d 健康判定: %v\n", batch, healthy)
		if !healthy {
			fmt.Println("健康判定未通过，任务已暂停；执行终止以回滚/隔离未完成设备")
			if err := o.Abort(taskID); err != nil {
				log.Fatalf("终止: %v", err)
			}
			break
		}
	}

	task, devs2, _ := o.Snapshot(taskID)
	fmt.Printf("\n任务终态: %s\n", task.Status)
	for _, b := range task.Batches {
		for _, id := range b.DeviceIDs {
			d := devs2[id]
			fmt.Printf("  %s batch=%d status=%s version=%s\n", id, d.BatchIndex, d.Status, d.CurrentVersion)
		}
	}
	fmt.Printf("\n状态变更事件（共 %d 条，含判定依据）:\n", len(o.Events()))
	for _, e := range o.Events() {
		fmt.Printf("  [%s] task=%s dev=%s %s: %s\n",
			e.At.Format("15:04:05.000"), e.TaskID, e.DeviceID, e.Kind, e.Detail)
	}
}
