# OTA Rollout Orchestrator（本地可运行的固件灰度升级编排器）

一套**本地即可运行**的设备固件灰度升级编排能力。用**模拟设备群**（`internal/fleet`）
和**本地 JSON 快照存储**（`internal/store`）替代真实设备与外部数据库，重点保证：

- 分批灰度推进，只有通过**健康门禁**的批次才能放量到下一批；
- 异常 / 离线 / 上报缺失的设备**绝不计入成功**；
- 推进过程可**暂停、可终止**，且每次状态变化都有**可追溯事件**；
- 设备离线、重连、重复 / 乱序上报下，**最终版本与批次归属确定且一致**；
- 失败按**确定策略**隔离回滚到“已知可用版本（基线）”，回滚不了的进入
  `ROLLBACK_FAILED` 人工待处理队列，不产生版本错乱；
- 多入口（后台决策循环、HTTP 管理面、设备上报面）**并发安全**，进程重启后
  已确认结论保持稳定。

仅依赖 Go 标准库。

---

## 1. 快速开始

```bash
go test ./... -race -count=1          # 全部单测（并发 + 故障注入为主）
./scripts/demo.sh                     # 端到端演示（健康放量 + 失败回滚）

# 手动运行
go run ./cmd/otad -addr :8080 -state ./data/state.json -batch-wait 3s
go run ./cmd/otactl devices           # 另一个终端
```

`otad` 启动后在内存中维护一个模拟设备群，并把全部状态原子写入
`-state` 指定的 JSON 文件；`otactl` 是它的 HTTP 客户端。

---

## 2. 架构与入口

```
                 ┌──────────────────────────── otad (单进程) ───────────────────────────┐
 otactl / 运维 ──▶│ HTTP API (internal/api)                                               │
                 │     设备管理 / 灰度控制(pause,resume,terminate) / 查询 / 模拟故障注入   │
                 │                                                                        │
 模拟设备上报 ───▶│ HandleReport  ◀── fleet.Sink（设备异步上报，天然乱序、可能重复）       │
                 │        │                                                               │
                 │        ▼                                                               │
                 │  Orchestrator 状态机 (internal/orchestrator)                            │
                 │   分批派发 / 健康门禁 / 暂停终止 / 失败隔离回滚 / 上报幂等收敛          │
                 │        │ 每个状态变更都在同一个 store 事务里提交 + 写事件               │
                 │        ▼                                                               │
                 │  Store (internal/store)：RWMutex + JSON 快照 + 临时文件 rename 原子落盘│
                 └────────────────────────────────────────────────────────────────────────┘
```

**三个入口驱动同一套状态**，所有变更都经过同一个写锁串行化：

1. **后台决策循环**（`Start`，间隔 `-poll`）：派发命令、判定门禁、驱动回滚；
2. **HTTP 管理面**（`/v1/rollouts/...`）：创建 / 暂停 / 恢复 / 终止 / 查询；
3. **设备上报面**（`POST /v1/reports` 与模拟设备的 in-process sink）：结果回传。

设备指令（安装 / 回滚）在**事务提交后、锁外**异步下发，慢设备或离线设备
不会阻塞状态收敛，也不会与持锁代码形成死锁。

---

## 3. 灰度策略（分批放量与健康门禁）

- 创建升级时，设备 ID 先**去重并按字典序排序**，再按 `batch_sizes` 确定性切批；
  未给足的数量会自动补一个尾批次，因此“哪些设备属于哪一批”是稳定的。
- 创建时为**每台设备捕获当前确认版本作为回滚基线 `from_version`**，并对设备
  **加锁**（`device_locks`）：同一设备在升级未结束前不能被另一个升级占用，
  从根上杜绝两个任务把同一目标刷成不同版本。
- 批次开始时记录截止时间 `deadline = now + batch-wait`：
  - 窗口内只派发与接收上报，**不提前判定**；
  - 窗口结束统计 `success / failed / offline`：
    - 窗口内始终没有 `SUCCESS`/`FAILED` 上报的设备（`PENDING`/`INSTALLING`）
      归类为 **`OFFLINE`（上报缺失）**，原因写明
      “no report before batch deadline”，**绝不计入成功**。
  - 健康阈值：`required = ceil(batch_size * success_ratio)`（默认 0.8）。
- **门禁判定**（在截止时刻、单次事务内完成）：
  - `success >= required` → 批次 `HEALTHY`：
    - 不是最后一批：`cursor` 推进到下一批继续放量；
    - 是最后一批：升级 `COMPLETED`。窗口内没上报但批次整体已达标的“掉队”
      设备保持 `OFFLINE` 隔离状态（不冒充成功），设备锁随升级完成释放。
  - 否则批次 `FAILING`、升级进入 `FAILING`：**立即隔离该批并回滚，后续批次
    永远不会开始**；更早之前已经健康的批次**保留新版本**（失败只隔离在出问题
    的那一批，不扩大爆炸半径）。

> 判定依据全部落在批次对象与事件里：`success/failed/offline/required/ratio/reason`，
> 单测日志会逐批、逐设备打印（见 `go test -v`）。

---

## 4. 失败隔离与回滚策略（确定性）

健康门禁失败后（或运维 `terminate` 后），状态机按如下确定性规则收敛：

| 情形 | 设备处理 | 最终状态 |
|---|---|---|
| 已刷到目标版本（`SUCCESS`） | 下发回滚到捕获基线的指令，确认后落定 | `ROLLED_BACK`（版本=基线） |
| 安装失败（`FAILED`，没离开基线） | 仍下发一次回滚确认，统一收敛 | `ROLLED_BACK` |
| 窗口内无上报（`OFFLINE`） | 周期性重发回滚指令；重连后可成功 | `ROLLED_BACK` |
| 回滚指令 / 确认在回滚窗口内始终无法完成 | 停止重试，转人工 | `ROLLBACK_FAILED`（版本保持基线） |
| 终止时还没派发到的批次 | 不刷写，直接标记留在基线 | `ROLLED_BACK` / 批次 `SKIPPED` |
| 终止时进行中、未出门禁结论的批次 | 记 `FAILED`（ABORTED，未做过门禁决定） | 设备按上表回滚 |

要点：

- **回滚目标永远是创建时捕获的基线版本**，不是“猜一个稳定版”，因此回滚后
  设备一定回到一个已知可用镜像。
- 回滚是**幂等可重试**的：重试复用原命令序号 `seq`；模拟设备对“已完成的同一
  命令”会重放缓存结果（覆盖“结果上报丢了、服务端重发”的场景）。
- 回滚有**有界等待窗口**（`2 × batch-wait`）：超时仍未确认 → `ROLLBACK_FAILED`，
  不无限挂起；该结论与设备的基线版本一并持久化，作为人工待处理项。
- **失败隔离范围**：健康门禁失败只回滚当前批次，已健康的更早批次保留新版本；
  `terminate` 是级联回滚所有刷到目标版本的设备。

---

## 5. 设备状态流转

```
                         创建升级（捕获基线、加锁）
                                   │
                                   ▼
                              PENDING ──派发安装指令──▶ INSTALLING
                                   │                        │
            ┌──────────────────────┼──────────────┬─────────┴───────────────┐
            │（终止时未派发）       │成功上报       │失败上报                  │窗口内无上报
            ▼                      ▼              ▼                          ▼
       ROLLED_BACK             SUCCESS       FAILED                      OFFLINE
       (留在基线)            (版本=目标)        │                          │
            ▲                    │             │ 门禁失败/终止：下发回滚    │
            │                    │ 门禁失败/    └──────────────┬───────────┘
            │                    │ terminate：回滚到基线        │
            │                    ▼                            ▼
            │               ROLLING_BACK ◀─────────────（周期重发回滚）
            │                    │
            │        ┌───────────┴───────────────┐
            │   确认回到基线                 回滚窗口内未确认
            └──── ROLLED_BACK             ROLLBACK_FAILED（人工待处理）
```

- 设备级**终态**：`SUCCESS`、`ROLLED_BACK`、`ROLLBACK_FAILED`。一旦到达，
  同一升级内**任何后续上报都不能改写**（终态不可变）。
- 升级级阶段：`PENDING → RUNNING ⇄ PAUSED → COMPLETED`；
  异常路径 `RUNNING → FAILING → FAILED`；
  终止路径 `* → TERMINATING → TERMINATED`。

---

## 6. 幂等、乱序与重连语义

### 6.1 命令与上报序号（seq）

- 每条安装 / 回滚指令带一个对 `(设备, 升级)` 单调递增的 `seq`，保存在
  `participation.last_seq`。
- 上报判定：
  - `seq < last_seq` → **陈旧 / 乱序上报，丢弃**，返回 `accepted=false` 及原因；
  - `seq == last_seq` 且设备仍在等待该命令的结果 → 视为**结果重传，应用一次**
    （覆盖“结果上报丢失、服务端用同一条命令重发”的场景）；其他情况判为重复，
    返回冻结结论；
  - `seq > last_seq` 但状态已终态 → 拒绝，已完成结论不被回退或改写。
- **批次判定是权威的**：门禁一旦做出决定（健康 / 失败 / 完成），之后迟到的
  “安装成功”上报一律冻结，不能把已经 `OFFLINE` 隔离或已经判定的批次再翻回去；
  只有回滚阶段的回滚结果在对账窗口内会被接受。

### 6.2 重复下发不重复安装

- 同一升级重复 `create` → `409 ALREADY_EXISTS`。
- 并发创建第二个升级、但设备已被未结束的升级锁定 → `409 DEVICE_BUSY`。
- 设备侧对“完全相同的命令（同 rollout+seq）”幂等：正在执行则忽略重复，
  已完成则重放缓存结果，不会二次刷写。

### 6.3 离线 / 重连

- 设备离线期间：命令不可达，派发返回错误，状态保留在 `INSTALLING/ROLLING_BACK`，
  由后续 tick 的超时 / 重试路径收敛，不静默丢状态。
- 模拟设备若在结果产生后才离线，结果会**缓存**；`online` 恢复（重连）时
  **自动补报**，与在线期间的上报走完全相同的收敛逻辑。
- 重连发生在门禁失败后的回滚窗口内：设备会被正常回滚并 `ROLLED_BACK`；
  重连**不能复活一个已经失败的门禁**（阶段保持 `FAILED`），结论确定可解释。

---

## 7. 并发安全与重启稳定性

- 所有状态变更都在**单个 `store.Lock` 事务**中完成：要么提交并原子落盘，
  要么返回错误且不触碰状态。读接口返回**深拷贝**，HTTP JSON 序列化可在锁外
  安全进行（`-race` 下验证无数据竞争）。
- 持久化：每次提交把整份快照写入临时文件 `fsync` 后 `rename` 原子替换，并
  尽力 `fsync` 目录，保证“已确认即已落盘”。
- `Start()` 时从快照**水合模拟设备群**；重启后决策循环会继续把
  `FAILING/TERMINATING` 中未完成的回滚幂等驱动到终态，已 `COMPLETED/FAILED/
  TERMINATED` 的结论不会再变化（重启测试见 `TestRestart*`）。

---

## 8. HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/devices` | `{"id","version"}` 注册设备（同时加入模拟群） |
| GET | `/v1/devices` | 设备列表（含当前确认版本） |
| DELETE | `/v1/devices/{id}` | 删除（被未结束升级锁定则 409） |
| POST | `/v1/devices/{id}/simulate` | 模拟控制：`{"online":false}` 离线/重连，`{"fault":"INSTALL_FAIL"}` 故障注入 |
| POST | `/v1/reports` | 设备上报入口（幂等/乱序判定） |
| POST | `/v1/rollouts` | 创建灰度：`id,target_version,device_ids,batch_sizes,success_ratio?` |
| GET | `/v1/rollouts` / `/{id}` | 列表 / 详情（批次门禁计数与原因） |
| POST | `/v1/rollouts/{id}/pause` `/resume` `/terminate` | 暂停 / 恢复 / 终止 |
| GET | `/v1/rollouts/{id}/participants` | 每设备状态、基线/目标版本、原因、seq |
| GET | `/v1/rollouts/{id}/events` | 完整审计事件流（全局递增 seq） |
| GET | `/v1/healthz` | 健康检查 |

模拟故障类型：`INSTALL_FAIL`（安装失败）、`DROP_COMMAND`（命令丢失）、
`DROP_REPORT`（结果上报丢失）、`NO_OP`（收到但不动作不上报）、`NONE`（清除）。

---

## 9. 本地验证（测试地图）

单测以**并发推送 + 故障注入**为主，`-v` 日志会打印批次门禁计数、每台设备状态
版本以及判定 / 拒绝原因：

| 测试 | 场景 |
|---|---|
| `TestHappyPathBatchedPromotion` | 两批全部健康、逐批放量、版本全部收敛目标版 |
| `TestUnhealthyBatchRollsBackAndStops` | 安装失败 + 命令丢失，门禁不过 → 当批回滚、下一批不启动、回滚不了的进人工 |
| `TestOfflineReconnectQueuedReport` | 设备离线被记 OFFLINE（不计成功），回滚窗口内重连补报后确定回滚 |
| `TestOfflineLateReportCannotRewriteCompletion` | 完成后的迟到成功上报被冻结，版本不被改写 |
| `TestDuplicateAndOutOfOrderReports` | 同 seq / 更旧 seq / 矛盾失败 / 伪造回滚，终态与版本均不变 |
| `TestDuplicateCreateConflict` | 重复创建与并发占用设备的冲突 |
| `TestPauseResume` / `TestTerminateRollsEveryoneBack` / `TestControlConflicts` | 暂停期间绝不派发；终止级联回滚；非法阶段转换冲突 |
| `TestFleetDuplicateCommandReplaysCachedResult` | 结果上报丢失后用同一命令重发，设备重放缓存结果、不重复刷写 |
| `TestConcurrentTicksAndBurstReports` | 8 个并发 tick + 上报突发竞争，门禁只判定一次、版本恰好收敛一次 |
| `TestConcurrentDuplicateReports` | 50 个 goroutine 并发重发终态上报，0 次被接受 |
| `TestRestartStabilityAfterCompletion` / `TestRestartDuringFailingReconciles` | 重启后结论、版本、事件不变；失败对账在重启后继续完成 |
| `TestHTTP*`（`internal/api`） | 经 HTTP 全链路：完成、404/409/400、并发创建唯一胜出、离线注入 |

```bash
go test ./... -race -count=1     # 竞态
go test ./... -count=10          # 多轮稳定性
go test ./internal/orchestrator/ -v   # 查看批次/设备/判定依据日志
```

---

## 10. 代码结构

```
internal/domain/        核心状态模型：设备、升级、批次、参与记录、事件、哨兵错误
internal/store/         JSON 快照存储：读写锁、原子落盘、深拷贝读视图、设备锁
internal/fleet/         模拟设备群：异步上报、离线缓存补报、故障注入、幂等重放
internal/orchestrator/  状态机：创建/控制/上报收敛/tick 门禁与回滚（+ 测试）
internal/api/           HTTP API（含端到端测试）
cmd/otad/               守护进程：组装 store+fleet+orchestrator+HTTP
cmd/otactl/             命令行客户端
scripts/demo.sh         一键演示
```
