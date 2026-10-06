# OTA Rollout Orchestrator（本地灰度升级编排）

用模拟设备群与本地 JSON 状态存储实现设备固件灰度升级编排：分批推送、批次观察期 +
自动放量调度、健康判定、失败回滚、离线/重连与乱序上报处理，全部状态变化可追溯、
进程重启后结论与未到期的观察期都能续跑。

## 结构

- `internal/ota/types.go` — 任务/批次/设备/上报/事件的领域模型与状态枚举
- `internal/ota/store.go` — 本地 JSON 状态存储（临时文件 + rename 原子写盘）
- `internal/ota/orchestrator.go` — 编排器：创建任务、分批推进、观察期自动调度、健康判定、
  暂停/恢复/终止、上报处理、回滚、重启恢复
- `internal/ota/device.go` — 模拟设备：接收下发、产生上报，支持故障注入（升级失败、离线上报缺失）
- `cmd/ota/main.go` — 本地演示入口

## 灰度与回滚策略

1. `CreateTaskWithObserve` 按设备 ID 排序后确定性划分批次（`batchSize`），任务初始为 `pending`。
   `observePeriod > 0` 时每批下发后进入观察期，到期自动评估；`observePeriod == 0`
   （或用兼容接口 `CreateTask`）时保持纯手动 `Advance` 推进，历史任务读入后行为不变。
2. `Start` 下发第一个批次；每批下发后若设置了观察期，则记录 `observe_until` 并武装定时器。
3. 评估（人工 `Advance` 或观察期到点自动触发）规则一致：
   - **只有 `succeeded` 计入成功**；失败、离线上报缺失、异常设备一律不计入成功。
   - 批次成功率 `succeeded / 批次设备数 >= minSuccessRate` 才允许放量进入下一批；
     否则任务自动转为 `paused`，需人工 `Resume` 或 `Abort`。
4. 判定时的确定处理策略：
   - `failed` 设备 → 自动**回滚**（`rolled_back`）。设备的 `CurrentVersion` 只在成功上报
     且版本校验通过时才更新，因此回滚后必然回到已知可用版本。
   - `delivered/pending`（离线上报缺失）设备 → **隔离**（`quarantined`），待人工处理。
5. `Abort` 终止任务（终态）：失败设备回滚、未完成设备隔离；`completed/aborted` 为任务终态。
6. 全部批次判定通过 → 任务 `completed`。

## 观察期与自动放量

- 每批下发后进入 `observePeriod` 观察期，到点由编排器按上述健康判定规则**自动评估**：
  健康则自动放量下发下一批（并开启下一段观察期），不健康则自动暂停。
- **人工提前推进**：观察期内调用 `Advance` 立即按当时设备情况判定并结束本批等待；
  原定时器被取消，到点不会对同一批再评一次。
- **人工暂停/恢复/终止**：观察期内 `Pause`/`Abort` 立即生效并取消定时器；
  `Resume` 后若当前批次仍在观察中（已下发未评估），重新计时一段完整观察期。
- **同一批次只形成一次确定结论**：批次评估后置位 `Evaluated` 并持久化。自动调度、
  人工操作、设备上报无论如何交错、同一批被触发多少次，后续触发都返回既有结论
  （记录 `batch_evaluate_skipped` 事件），不会重复评估、重复放量或重复暂停。
- **触发来源可追溯**：每次评估的事件 `batch_health_check` 携带
  `trigger=manual` / `auto:观察期到期` / `auto:重启补评`，以及成功数、比率、阈值等判定依据；
  放量（`batch_delivered`）、暂停（`task_paused`）、完成（`task_completed`）同样带触发来源。

## 并发与多任务

- 所有状态变更在编排器内串行化（单互斥锁），自动调度回调、人工 API、设备上报共用
  同一把锁，多任务各自持有独立的观察期定时器，互不干扰。
- 上报类变更采用**合并落盘**（默认 50ms 批量写盘），任务级状态转换（创建/下发/评估/
  暂停/恢复/终止/回滚）**同步落盘**，兼顾千台级吞吐与结论不丢失。
- `Snapshot` 返回深拷贝快照，后台定时器评估不会与读取方产生数据竞争。
- 规模验收：`TestScaleManyDevicesParallelTasks` 覆盖 1200 台设备、每批 150 台、
  3 个任务并行自动放量，无死锁、无状态丢失。

## 设备状态流转

```
pending --下发--> delivered --success(版本校验通过)--> succeeded   [终态]
                   delivered --failure---------------> failed --回滚--> rolled_back [终态]
pending/delivered --判定或终止时上报缺失--------------> quarantined  [终态]
```

- 终态（`succeeded/rolled_back/quarantined`）**不可被后续任何上报改写或回退**：
  评估前后到达的迟到上报不会把版本或成败判反。
- 设备在线性（`Online`）由 `offline/online` 上报维护，与升级状态正交。

## 幂等与重连语义

- **序号去重**：每台设备维护单调递增的 `seq`，编排器只接受严格大于 `LastSeq` 的上报，
  重复与乱序（旧序号）报文直接丢弃并记录 `report_dropped` 事件。
- **推送保护**：批次尚未下发（`pending`）的设备的升级类上报无效，记录 `report_ignored`。
- **版本一致性**：`success` 上报的固件版本必须等于任务目标版本，否则记为失败
  （`report_version_mismatch`），防止版本错乱。
- **重复下发幂等**：模拟设备已处于目标版本时直接回报成功，不重复安装。
- **重连**：设备重连发 `online` 宣告并补发未完成的成功上报；若设备已被隔离（终态），
  补报不会改写结论，只更新在线状态——保证"已确认的结论稳定"。

## 重启恢复

- 每次任务级状态转换原子写盘（tmp + rename），上报类变更由后台协程合并落盘，
  `Close` 时把残余变更同步写盘。
- 进程重启后（`NewOrchestrator` 重新加载存储）：
  - 已形成的评估结论（`Evaluated`）、设备终态与事件追溯完整保留，不重复放量；
  - 观察期**未到期**的批次按剩余时间重新计时（事件 `observation_resumed`）；
  - 观察期在**停机期间已到期**的批次立即补评（事件 `observation_expired_while_down`，
    `trigger=auto:重启补评`），不漏放量；
  - 无观察期的任务不受影响，仍等待人工 `Advance`。

## 本地验证

```bash
# 单元测试（含观察期自动放量、人工/自动并发、同批反复触发、评估边界上报、
# 多任务并行、重启续跑、千台规模验收，以及原有手动流程用例）
go test ./internal/ota/ -v
go test ./internal/ota/ -race -count=3

# 自动放量演示：4 台设备、每批 2 台、观察期 300ms
go run ./cmd/ota -state /tmp/ota-state.json -devices 4 -batch-size 2 -observe 300ms

# 手动模式（兼容原有流程）：6 台设备、每批 2 台，给 dev-003 注入升级失败
go run ./cmd/ota -state /tmp/ota-state.json -devices 6 -batch-size 2 \
    -min-success-rate 0.8 -fail-device dev-003
```

测试日志会打印每个批次的健康判定依据（触发来源/成功数/阈值）与每台设备的状态、
版本、序号与失败原因，便于核对版本收敛与成败判定的一致性。
