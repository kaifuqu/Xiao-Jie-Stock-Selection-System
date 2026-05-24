# P3/P4 异步扫描部署与运维指南（指挥舱 + auto_sniper_daemon）

> **当前架构**：`ui/app.py` 内 P2–P5 以 **同步** `run_scan_engine` 为主。若仍有代码路径写入 `pending.json`，队列由 **`auto_sniper_daemon.py`** 在启动时独占 `pending_queue_consumer.filelock`，并在主循环中 **每 10 秒** 调用一次 `process_one_pending_scan_job()`（与已删除的独立 `service/scheduler.py` 等价）。**勿**再并行启动第二套队列消费进程。

## 1. 链路总览

| 组件 | 职责 |
|------|------|
| `ui/app.py` | 指挥舱；扫描以同步路径为主；可选异步提交时仅写 JSON。 |
| `service/async_scan_bridge.py` | 队列文件、状态机、底仓加载、`run_scan_engine`、企微 `notify_scan_results_top3_p2p3p4`，并提供队列状态探针与 filelock 互斥。 |
| `auto_sniper_daemon.py` | 启动时 `try_acquire_scheduler_queue_consumer_filelock()`，轮询 `process_one_pending_scan_job()`；同时负责分时快照与 P2/P3/P4/P5 调度。 |
| `service/scan_service.py` | 同步扫描封装；UI 与 daemon 的同步路径统一入口。 |
| `core/scan_engine.py` | P2–P5 主扫描引擎；非交易日走更静态的回放/冻结路径，直通车与底仓来源可区分。 |

## 2. 目录与文件

在 `core.runtime_data_paths.ensure_runtime_data_layout()` 下自动创建：

- `data/runtime/scan_async/pending.json` — 待执行任务（原子 rename 为 `running.json`）。
- `data/runtime/scan_async/running.json` — 执行中。
- `data/runtime/scan_async/status.json` — `state`: `idle` / `queued` / `running` / `done` / `error`。
- `data/runtime/scan_async/latest_result.json` — 最近一次成功结果。
- `data/runtime/scan_async/pending_queue_consumer.filelock` — **filelock** 互斥；**auto_sniper_daemon** 与 Streamlit 内嵌 `ScanAsyncWorker` **二选一**。

## 3. 推荐部署（生产）

1. 使用项目根目录 **`start_server.sh`**（Linux）或 **`start_server.bat`**（Windows）：先后台 **`python auto_sniper_daemon.py`**，再前台 **`streamlit run ui/app.py`**。
2. 守护进程已占队列锁时，Streamlit 侧 `ensure_async_scan_worker_started()`（若被调用）会因拿不到锁而 **不嵌入** UI 内嵌消费者，由守护进程统一消费队列。

## 4. 故障排查

| 现象 | 处理 |
|------|------|
| 异步任务一直 queued / running | 查 `status.json`；确认 **`auto_sniper_daemon.py` 已运行**；查 `data/runtime/daemon.log`。 |
| 提示队列忙碌 | 删除卡死的 `running.json`（确认无进程正在扫盘后）；或等待超时清理逻辑。 |
| DuckDB 锁 | 勿多开第二套日线同步/洗盘守护；仅保留 **UI + daemon** 两进程。 |

## 5. 安全与一致性

- 底仓与同步扫描一致：从池缓存 JSON 读取 **code + hist**，再 `rehydrate_base_items_for_scan_engine`。
- 合并进会话仍走 `dehydrate_*`，表结构不变。
