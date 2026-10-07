# OTA Rollout Orchestrator（本地灰度升级编排）

用模拟设备群与本地状态存储实现设备固件灰度升级编排：分批推送、**批次观察期 + 到点自动放量**、
健康判定、失败回滚、离线/重连与乱序上报处理；自动调度、人工操作与设备上报并发安全，
全部状态变化可追溯，进程重启后观察期与判定结论可续跑。

## 结构

- `internal/ota/types.go` — 任务/批次/设备/上报/事件领域模型、状态枚举与触发来源（manual/auto）
- `internal/ota/scheduler.go` — 观察期自动调度器：每任务一个到点定时器、重启恢复、停止与生命周期管理
- `internal/ota/orchestrator.go` — 编排器：创建任务、观察期/手动推进、统一触发式健康判定、
  暂停/恢复/终止、上报处理、回滚；所有状态变更在单一互斥锁内串行化
- `internal/ota/store.go` — 本地持久化：任务状态原子 JSON、设备状态按 16 分片原子写、事件 append-only JSONL
- `internal/ota/device.go` — 模拟设备：接收下发、产生上报，支持故障注入（升级失败、离线上报缺失）
- `cmd/ota/main.go` — 本地演示入口（`-observe-period` 切换自动/手动模式）

## 观察期与自动放量

1. `CreateTask(..., observePeriod)` 新增最后一个参数 **观察期时长**：
   - `observePeriod > 0`：每批下发后进入观察期，记录绝对的 `observe_deadline`，调度器挂一个到点定时器；
   - `observePeriod == 0`：**不启用自动调度，完全保持原来的手动 `Advance` 推进方式**
     （历史任务没有该字段，读入后即为 0，行为与升级前一致）。
2. 观察期一到，调度器以触发来源 `auto` 调用统一评估入口，按**现有健康判定规则**得出唯一结论：
   - 仅 `succeeded` 计入成功；`failed` 自动回滚、`delivered/pending`（离线上报缺失）隔离；
   - `成功率 >= minSuccessRate` → 放量下发下一批（最后一批则任务 `completed`），并为新批挂定时器；
   - 否则任务 `paused`（自动暂停），等待人工 `Resume` / `Abort`。
3. **人工提前推进**：观察期结束前调用 `Advance`，立即取消定时器并按"当时"的设备情况判定，
   随后定时器到点不会再对同一批评一次。
4. 观察期内人工 **暂停 / 恢复 / 终止立即生效**：
   - `Pause` 取消待触发定时器并清除截止时刻，到点不再评估；
   - `Resume` 对未判定批次从恢复时刻重新计算一个完整观察期；已判定批次不会被自动重判，需再次人工 `Advance`；
   - `Abort` 取消定时器并进入终态，之后不再有自动评估。

## 并发与"每批唯一结论"

自动调度（定时器）、人工操作、设备上报会同时发生，甚至同一批次被反复触发。保证手段：

- **单一串行入口**：所有状态变更都在 `Orchestrator` 的互斥锁内执行；定时器回调、`Advance`、
  `Pause/Resume/Abort`、`HandleReport` 全部排队进入，不存在并发改状态。
- **每批一次性结论**：批次有 `decided / decided_at / trigger` 字段。评估入口先检查 `decided`，
  已判定的批次对任何重复触发（定时器、并发人工、重复调用）只**回读既有结论**并记录
  `batch_decision_skipped` 事件，不重判、不改写设备结果。
- **设备终态保护**：`succeeded / rolled_back / quarantined` 为终态，评估之后到达的成功/失败/
  乱序上报都不能改写结论或版本；序号严格递增去重，`success` 版本必须等于目标版本，
  防止"评估前后到达的上报把版本或成败判反"。
- **触发来源与依据可追溯**：`batch_health_check` 事件含 `trigger=manual|auto`、成功数/回滚数/
  隔离数、成功率、阈值、是否人工提前；放量（`batch_delivered`）、暂停（`task_paused`）、
  完成（`task_completed`）都带触发来源与原因。运行日志同样打印任务、批次、设备的判定依据。

加锁顺序固定为 `Orchestrator.mu -> Scheduler.mu`（必要时再取 `Store.mu`），避免锁环与死锁。

## 多任务隔离与重启续跑

- **多任务并行**：调度器为每个观察中的任务独立维护一个定时器（`taskID -> timer`），
  任务之间的定时器、批次状态、事件互不影响；统一编排器锁保证跨任务串行，无死锁。
  注意：一个进程使用**一个** `Orchestrator` 管理其全部任务（一个 `Store` 对应一个编排器）。
- **重启恢复**（`NewOrchestrator` 时自动执行）：
  - 任务仍 `rolling`、当前批次未判定且截止时刻未到 → 按持久化的 `observe_deadline` 计算剩余时间，
    重建定时器，到点照常放量；
  - 截止时刻在停机期间已过 → 重启后立即补一次自动评估（事件 `observe_due_restore`），不漏放量；
  - 已判定批次 / 已暂停 / 已终止 / 已完成 / 无观察期任务 → 不重建定时器，结论不重复。
- 观察截止时刻存的是**绝对时间**，因此重启不会让观察期"从头再来"，也不会重复放量。

## 持久化布局（已从单文件演进）

为满足"本地千台以上设备、每批百台以上、多任务并行且不明显变慢"，持久化拆成三类文件：

- `<state>.json`：仅任务状态，原子写（tmp + rename），体积有上界；
- `<state>.d<0..15>`：设备状态 **16 分片**，设备按 ID 的 FNV-1a 哈希落片，单笔上报只原子重写
  它所在的那一个分片（约 1/16 设备），使上报落盘成本与设备总量解耦；
- `<state>.json.events.jsonl`：事件日志，每行一条，**仅追加 O(1)**，不再随每条上报整体重写。

事件在产生时先追加落盘，随后状态再原子写；崩溃时最多多出事件，不会出现"状态已变但丢判定依据"。

**历史文件自动迁移**：旧版本把 `devices` / `events` 内嵌在单个 JSON 中，首次用新版本打开时会
自动迁移到分片 + JSONL，任务与设备结论、事件追溯保持不变（见 `TestLegacyStoreMigration`）。

## 设备状态流转

```
pending --下发--> delivered --success(版本校验通过)--> succeeded   [终态]
                   delivered --failure---------------> failed --回滚--> rolled_back [终态]
pending/delivered --判定或终止时上报缺失--------------> quarantined  [终态]
```

- 终态（`succeeded/rolled_back/quarantined`）不可被后续任何上报改写或回退。
- 设备在线性（`Online`）由 `offline/online` 上报维护，与升级状态正交。
- 序号严格递增去重；批次未下发（pending）设备的升级类上报无效；重复下发在设备侧幂等。

## 本地验证

```bash
# 全部单测（含原有用例 + 观察期/自动调度新用例 + 旧格式迁移 + 千台规模）
go test ./internal/ota/ -v

# 带竞态检测多跑几遍（定时器、人工、上报并发，要求稳定无 DATA RACE）
go test ./... -race -count=3

# 自动放量演示：观察期 300ms，4 台设备每批 2 台，到点自动评估放量直到完成
go run ./cmd/ota -state /tmp/ota-auto.json -devices 4 -batch-size 2 \
    -observe-period 300ms -min-success-rate 0.8

# 自动暂停演示：给 dev-001 注入失败，观察期到点判定不健康自动暂停
go run ./cmd/ota -state /tmp/ota-fail.json -devices 4 -batch-size 2 \
    -observe-period 300ms -min-success-rate 0.9 -fail-device dev-001

# 手动模式（观察期为 0，与历史行为一致）：人工 Advance 逐批推进
go run ./cmd/ota -state /tmp/ota-manual.json -devices 6 -batch-size 2
```

新能力相关用例（`internal/ota/scheduler_test.go` / `migration_test.go`）覆盖：

- 观察期到点自动放量、自动暂停；
- 人工观察期内提前推进、且定时器不再二次评估；
- 人工与自动并发竞争、同一批次被反复触发（结论/事件唯一）；
- 评估边界前后的上报（提前到达计入成功、滞后到达不改写隔离/版本）；
- 观察期内暂停/恢复/终止立即生效；
- 多任务并行隔离；重启时观察期未到（续跑）与已过（补判）、终态不重复；
- 1200 台设备 / 每批 100 台 / 12 批自动放量的规模用例（无死锁、结论正确、耗时可控）；
- 旧版单文件状态的自动迁移与手动流程兼容。

测试与运行日志会打印每个批次的触发来源、成功/回滚/隔离数、成功率与阈值，以及每台设备的
状态、版本、序号与失败原因，便于核对放量依据与版本收敛的一致性。
