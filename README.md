# OTA Rollout Orchestrator（本地灰度升级编排）

用模拟设备群与本地 JSON 状态存储实现设备固件灰度升级编排：分批推送、健康判定放量、
失败回滚、离线/重连与乱序上报处理，全部状态变化可追溯、进程重启后结论稳定。

## 结构

- `internal/ota/types.go` — 任务/批次/设备/上报/事件的领域模型与状态枚举
- `internal/ota/store.go` — 本地 JSON 状态存储（临时文件 + rename 原子写盘）
- `internal/ota/orchestrator.go` — 编排器：创建任务、分批推进、健康判定、暂停/恢复/终止、上报处理、回滚
- `internal/ota/device.go` — 模拟设备：接收下发、产生上报，支持故障注入（升级失败、离线上报缺失）
- `cmd/ota/main.go` — 本地演示入口

## 灰度与回滚策略

1. `CreateTask` 按设备 ID 排序后确定性划分批次（`batchSize`），任务初始为 `pending`。
2. `Start` 下发第一个批次；`Advance` 对当前批次做健康判定：
   - **只有 `succeeded` 计入成功**；失败、离线上报缺失、异常设备一律不计入成功。
   - 批次成功率 `succeeded / 批次设备数 >= minSuccessRate` 才允许放量进入下一批；
     否则任务自动转为 `paused`，需人工 `Resume` 或 `Abort`。
3. 判定时的确定处理策略：
   - `failed` 设备 → 自动**回滚**（`rolled_back`）。设备的 `CurrentVersion` 只在成功上报
     且版本校验通过时才更新，因此回滚后必然回到已知可用版本。
   - `delivered/pending`（离线上报缺失）设备 → **隔离**（`quarantined`），待人工处理。
4. `Abort` 终止任务（终态）：失败设备回滚、未完成设备隔离；`completed/aborted` 为任务终态。
5. 全部批次判定通过 → 任务 `completed`。

## 设备状态流转

```
pending --下发--> delivered --success(版本校验通过)--> succeeded   [终态]
                   delivered --failure---------------> failed --回滚--> rolled_back [终态]
pending/delivered --判定或终止时上报缺失--------------> quarantined  [终态]
```

- 终态（`succeeded/rolled_back/quarantined`）**不可被后续任何上报改写或回退**。
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
- **持久化**：每次状态变更原子写盘（tmp + rename），进程重启后任务状态、设备结论与
  事件追溯完整恢复（见 `TestPersistenceAcrossRestart`）。

## 本地验证

```bash
# 单元测试（并发推送 + 故障注入：离线重连、重复/乱序上报、批次中断回滚、重启持久化）
go test ./internal/ota/ -v
go test ./internal/ota/ -race -count=2

# 本地演示：6 台设备、每批 2 台，给 dev-003 注入升级失败
go run ./cmd/ota -state /tmp/ota-state.json -devices 6 -batch-size 2 \
    -min-success-rate 0.8 -fail-device dev-003
```

测试日志会打印每个批次的健康判定依据（成功数/阈值）与每台设备的状态、版本、
序号与失败原因，便于核对版本收敛与成败判定的一致性。
