# 小杰AI选股系统 Pro V26.2 — 全项目文件说明与源代码索引

> **生成说明**：本文件列出仓库内（**不含** `__pycache__`；若存在历史虚拟环境目录 `.venv` 也排除）的全部文件路径、大致行数与功能。  
> **关于「源代码」**：完整程序正文位于各 `.py` / 配置文件中；**不得**将数万行代码与 GB 级数据库合并为单文件。二进制与运行时数据仅记路径与用途。  
> **导出全部文本源码**：见本文末尾「附录：一键导出脚本」。

---

## 1. 根目录

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `auto_sniper_daemon.py` | 917 | 无人值守守护进程：交易日屏障、自动巡航、分时快照、扫描与企微推送 |
| `config.yaml` | 434 | 全局配置（Tushare、数据库、regime、策略阈值、企微、scan_async 等） |
| `constants.py` | 76 | 版本号、路径常量等 |
| `requirements.txt` | 16 | Python 依赖（主环境） |
| `requirements-daemon.txt` | 4 | 守护进程额外依赖 |
| `install_all_deps.bat` | — | Windows：用系统 Python 一键安装上述两份 requirements |
| `start_server.bat` | — | Windows 生产启动：守护进程 + Streamlit |
| `start_server.sh` | — | Linux 生产启动：守护进程 + Streamlit |
| `verify_system.py` | 349 | 轻量自检（配置、DuckDB、scan_async、regime 等） |
| `run_lab.py` | 50 | 离线策略实验室独立 Streamlit 入口（与 `ui/app.py` 进程隔离） |

---

## 2. `core/` — 核心引擎

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `backtest_context.py` | 36 | 回测 legacy 模式上下文（`is_backtest_legacy_mode`） |
| `backtest_painpoint_config.py` | 84 | 痛点回测窗口与参数配置 |
| `backtest_runner.py` | 586 | 单股/批量/痛点回测 CLI 与入口 |
| `config_manager.py` | 436 | YAML 与策略实验室会话合并、缩量三键、通知配置等 |
| `danger_signal_utils.py` | 164 | 斩仓条件、危险信号判定（供 scan_engine） |
| `experiment_db.py` | 262 | 策略实验 SQLite 记录（实验室用） |
| `indicator_calc.py` | 319 | K 线预计算指标 |
| `intraday_snapshot_scheduler.py` | 136 | Streamlit 内分时快照后台调度（多时点 VR；含 P1 复核兼容入口） |
| `master_control.py` | 约 220 | 企微总闸、守护进程巡航、`maintenance_mode` 维护锁等跨进程 JSON（`master_control.json`） |
| `notification_gateway.py` | 908 | 企微 Webhook、P1 高分池摘要、P2–P5 Top 推送 |
| `p1_score_display.py` | 40 | P1 分项 → 满分/最低项展示字符串 |
| `pool_manager.py` | 1847 | P1 洗盘主逻辑、观察池、行业动态贝塔、落库 |
| `regime_analyzer.py` | 233 | 双轨 Regime（DuckDB 聚合 + sentiment_key） |
| `runtime_data_paths.py` | 212 | `data/runtime` 下池缓存、状态、scan_async 路径 |
| `scan_engine.py` | 2166 | P2–P5 扫描主循环、漏斗、观察池、危险表、signal_log |
| `sop_v11.py` | 236 | 指数熔断探测（`evaluate_market_circuit_breaker`） |
| `streamlit_thread_ctx.py` | 28 | Streamlit 线程上下文辅助 |

### `core/strategies/`

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `fund_mv_utils.py` | 696 | 市场缩量语境、换手自适应、筹码等 |
| `p2_auction_screener.py` | 596 | P2 竞价筛选器配置与逻辑 |
| `p3_intraday_screener.py` | 803 | P3 盘中筛选器 |
| `p4_tail_screener.py` | 648 | P4 尾盘筛选器 |
| `p5_postmarket_screener.py` | 656 | P5 盘后筛选器 |
| `risk_control_engine.py` | 820 | 三层风控软降权、标签 |
| `score_calibration.py` | 166 | 爆发分软封顶、乖离压缩等 |
| `strat_base.py` | 616 | 战法基础分、动态分、P1 基因缓存 |
| `strat_golden_10.py` | 228 | 金共振等组合战法 |
| `strat_p2_auction.py` | 266 | P2 引擎入口 |
| `strat_p3_intraday.py` | 390 | P3 引擎入口 |
| `strat_p4_tail.py` | 314 | P4 引擎入口 |
| `strat_p5_postmarket.py` | 298 | P5 引擎入口 |

---

## 3. `data/` — 数据层与运行时

| 文件 | 行数/大小 | 功能 |
|------|-----------|------|
| `api_fetcher.py` | 299 | 实时批量等 API 封装 |
| `data_fetcher.py` | 978 | 数据下载/同步相关逻辑 |
| `db_core.py` | 约 1580+ | DuckDB：全局写/只读单例、`get_read_conn`/`get_write_conn` 短连接、`duckdb_vacuum_silent`、日线、P1 缓存表、行业同步、`get_index_latest_from_daily_data`（指数卡片 DuckDB 兜底） |
| `quant_data.duckdb` | 二进制 | 主行情库（**非文本**） |
| `quant_data.duckdb.wal` | 二进制 | DuckDB 预写日志 |
| `experiments.duckdb` | 二进制 | 实验库（**非文本**） |
| `runtime/pool_cache/p1_cache_*.json` | 极大 | P1 按日落盘缓存（数据文件） |
| `runtime/p1_gene/p1_gene_*.json` | 视情况 | P1 基因分存档 |
| `runtime/state/*.json` | 视情况 | 黑名单、总控、洗盘指标、阵亡缓存、行业历史等 |
| `runtime/scan_async/*` | 视情况 | 异步扫描队列与锁文件 |
| `runtime/daemon.log` | 文本日志 | `start_server*` 重定向的守护进程日志（可选） |

---

## 4. `ui/` — Streamlit 前端

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `app.py` | 约 2280 | 实盘指挥舱单页：洗盘、扫描、结果表、漏斗、大盘熔断 Expander（sop_v11）、侧边栏联动（不含策略实验室 Tab） |
| `display_labels.py` | 126 | 表格列中文名、样式 |
| `session_cache_dehydrate.py` | 222 | 大对象脱水/再水化，降 session 内存 |
| `strategy_lab.py` | 1282 | 策略实验室（参数扫描、会话覆写） |
| `strategy_lab_labels.py` | 588 | 实验室标签与常量 |
| `ui_components.py` | 254 | 指数卡片、池子展示组件 |
| `ui_sidebar.py` | 约 1180+ | 侧边栏：总控台、池子视图、数据底座（含一键维护/压缩/同步）、机构中控等 |

---

## 5. `service/` — 服务与调度

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `async_scan_bridge.py` | 670 | P3/P4 异步队列、UI/daemon 协作、状态文件与 filelock 互斥 |
| `scan_service.py` | 63 | 扫描服务封装 |
| ~~`scheduler.py`~~ | — | **已删除**：职责并入 `auto_sniper_daemon.py` |
| ~~`sync_daemon.py`~~ | — | **已删除**：日线增量由 daemon 晚间/早盘链负责 |

---

## 6. `tools/` — 工具脚本

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `db_inspector.py` | 156 | 数据库巡检 |
| `force_maintenance_vacuum.py` | 约 155 | 维护锁 + 终止守护 + DuckDB VACUUM + 解除锁并重启 `auto_sniper_daemon`（供 UI 或命令行独立进程执行） |
| `SYSTEM_E2E_TEST_GUIDE.md` | 121 | 端到端测试说明 |

---

## 6.1 `offline_tools/` — 离线科研/审计（与实盘隔离）

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `mvp_truth_detector.py` | 817 | 离线测谎/收益检验 |
| `p1_threshold_smoke.py` | 58 | P1 阈值冒烟测试 |
| `audit_indicators.py` | 98 | 指标审计脚本 |

---

## 7. `scripts/`

| 文件 | 行数(约) | 功能 |
|------|----------|------|
| `audit_report.txt` | 1532 | 审计报告输出（文本） |

---

## 8. `docs/`

| 文件 | 功能 |
|------|------|
| `DEPLOY_SCAN_ASYNC.md` | 异步扫描部署说明 |
| `TRADER_DAILY_WORKFLOW.md` | 操盘手单日流程（简版）；含 **DuckDB 维护模式 / 一键维护 / 短读连接** 等 V26.2 运维说明 |
| `PROJECT_FULL_INVENTORY.md` | 本文件：全项目索引 |
| `系统代码及使用说明.html` / `system_full_snapshot.html` | 全量源码快照 HTML；在项目根执行 `python tools/generate_system_full_snapshot.py` 重新生成 |

---

## 9. `xuanguchi/` — 归档导出样例

| 文件 | 功能 |
|------|------|
| `*/P1_底仓.txt` 等 | 某次导出的各池文本占位/样例 |

---

## 10. 源代码在哪里？

- **所有 `.py` 源代码**：以 UTF-8 文本形式保存在上表对应路径。  
- **配置**：`config.yaml`、`constants.py`、`requirements*.txt`。  
- **二进制/数据**：`*.duckdb`、`*.wal`、大型 `*.json` 缓存为**数据**而非可读源码。  
- **第三方库**：安装在当前所用 Python 的 `site-packages`（**本清单不逐文件列出**）。

---

## 附录 A：Python 源码文件一览（可复制到 IDE 搜索）

```
auto_sniper_daemon.py
constants.py
verify_system.py
run_lab.py
core/backtest_context.py
core/backtest_painpoint_config.py
core/backtest_runner.py
core/config_manager.py
core/danger_signal_utils.py
core/experiment_db.py
core/indicator_calc.py
core/intraday_snapshot_scheduler.py
core/master_control.py
core/notification_gateway.py
core/p1_score_display.py
core/pool_manager.py
core/regime_analyzer.py
core/runtime_data_paths.py
core/scan_engine.py
core/sop_v11.py
core/streamlit_thread_ctx.py
core/strategies/fund_mv_utils.py
core/strategies/p2_auction_screener.py
core/strategies/p3_intraday_screener.py
core/strategies/p4_tail_screener.py
core/strategies/p5_postmarket_screener.py
core/strategies/risk_control_engine.py
core/strategies/score_calibration.py
core/strategies/strat_base.py
core/strategies/strat_golden_10.py
core/strategies/strat_p2_auction.py
core/strategies/strat_p3_intraday.py
core/strategies/strat_p4_tail.py
core/strategies/strat_p5_postmarket.py
data/api_fetcher.py
data/data_fetcher.py
data/db_core.py
service/async_scan_bridge.py
service/scan_service.py
start_server.bat
start_server.sh
tools/db_inspector.py
offline_tools/mvp_truth_detector.py
offline_tools/p1_threshold_smoke.py
offline_tools/audit_indicators.py
ui/app.py
ui/display_labels.py
ui/session_cache_dehydrate.py
ui/strategy_lab.py
ui/strategy_lab_labels.py
ui/ui_components.py
ui/ui_sidebar.py
```

---

## 附录 B：一键导出「全部 .py 源码」到单个文本（自行运行）

在项目根目录 PowerShell 中执行（生成 `docs/_all_python_sources_concat.txt`，**体积可能很大**）：

```powershell
Set-Location "d:\xiaojie"
$out = "docs\_all_python_sources_concat.txt"
"" | Set-Content $out -Encoding UTF8
Get-ChildItem -Recurse -Filter *.py | Where-Object {
  $_.FullName -notmatch '\\\.venv\\|\\__pycache__\\'
} | Sort-Object FullName | ForEach-Object {
  "`n`n========== $($_.FullName) ==========`n" | Add-Content $out -Encoding UTF8
  Get-Content $_.FullName -Raw -Encoding UTF8 | Add-Content $out -Encoding UTF8
}
```

说明：该文件**仅作备份/检索**，日常开发请直接编辑各 `.py` 源文件。

---

**文档版本**：与仓库目录快照一致；新增文件请同步更新本表或重新运行行数统计脚本。
