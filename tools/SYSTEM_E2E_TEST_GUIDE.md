# 小杰AI选股系统 Pro V26.2 — 全系统贯通测试指南（交钥匙）

> 目的：按固定顺序验证 **数据增量、引擎稳定性、战法漏斗、SOP 纪律、测谎仪摩擦** 是否协同可用。  
> 环境：Windows / Python 3.11+，已配置 `config.yaml`（Tushare token、DuckDB 路径）。

---

## 一、启动顺序总览

| 顺序 | 组件 | 命令 / 操作 | 预期 |
|------|------|-------------|------|
| 1 | 后台守护（推荐） | `python auto_sniper_daemon.py`（或与 UI 同机用 `start_server.bat` / `start_server.sh`） | 日志出现调度注册与 scan_async 锁；晚间/早盘由守护进程做增量同步，勿再单独起 sync_daemon |
| 2 | 主 UI | `streamlit run ui/app.py` | 浏览器打开指挥舱，无 ImportError |
| 3 | DuckDB | 确认 `data/quant_data.duckdb` 存在且可被读 | 侧边栏能显示 Regime、底仓数量 |

---

## 二、战区 1：数据增量与锁（data_fetcher / db_core / api_fetcher）

### 2.1 验证「按日断点续传」

1. 打开日志（控制台或日志文件），搜索关键字：`【断点续传】`  
2. 对**已有** `daily_data` 的某日再跑同步（见 `data.data_fetcher.sync_single_day` 或守护进程）。  
3. **预期**：日志出现 `需补票 X / Y 只` 或 `跳过重复下载`；不应出现对**已存在 (ts_code, trade_date)** 的重复 HTTP 全量拉取（除 `RECENT_FORCE_RESYNC_TAIL` 近端 N 日有意重扫补洞）。

### 2.2 验证 DuckDB 写避让

1. **终端 A**：启动 `auto_sniper_daemon.py` 或长时间 `sync_history`（勿再使用已删除的 `sync_daemon`）。  
2. **终端 B**：同时打开 Streamlit 并点击 **P1 启动全量洗盘**（触发写 `p1_cache` / 读 DuckDB）。  
3. **预期**：偶发锁冲突时，日志出现 `【写库避让】... 遇锁`，最终成功或明确报错；**进程不因裸异常退出**。

### 2.3 验证 api_fetcher 超时与退避

1. 断网或限速环境下触发 `fetch_realtime_batch`（任意扫描）。  
2. **预期**：请求在有限时间内失败返回空 dict 或部分结果；日志含 `重试`，不应无限挂死（单次 HTTP 有 `TIMEOUT_SECONDS` 与重试上限）。

---

## 三、战区 2：引擎与截面 Rank（scan_engine / score_calibration）

### 3.1 截面 Rank 语义（非未来函数）

1. 阅读 `core/strategies/score_calibration.py` 文档字符串：Rank 在 **当次扫描候选集** 内计算。  
2. 实盘核对：同一交易日多次扫描，候选集不变则秩应稳定（允许实时价微调导致细微差异）。  
3. **预期**：不存在「用未来交易日的全样本」对历史行做标准化；`indicator_calc` 中均线/MACD 均为 **按 ts_code 时间序列滚动**（`groupby('ts_code')`）。

### 3.2 综合分与表格契约

1. 执行 **P2～P5** 任一扫描。  
2. 检查表格列：代码、名称、综合分、涨幅、量比、真换手等仍与改版前一致（见 `scan_engine._ensure_pool_table_row_contract`）。  
3. **预期**：新增 `sop_market_breaker` 仅存在于 `session_state['scan_results']` 元数据，**不改变**单行股票字典的列集合。

---

## 四、战区 3：P1 及格线与 P2～P5 隔离（pool_manager / strat_*）

### 4.1 P1 50 分线与观察池

1. **P1 模式**下完成洗盘，查看底仓列表与 `p1_rejected_cache`（阵亡诊断）。  
2. 切换 **主池 / 主池+观察池**，确认观察池仅在引擎填充时出现。  
3. **预期**：得分低于 `pass_line` 的主路径淘汰有明确理由；观察池使用降档 `pass_line_obs`（见 pool_manager 注释）。

### 4.2 P2～P5 物理隔离

1. 仅点击 **P4 盘尾扫描**，确认日志与结果仅跑 P4 引擎。  
2. **预期**：`target_pools=['p4']` 时不会误跑 P2 战法逻辑（各 `strat_p*.py` 独立 `run_all`）。

---

## 五、战区 4：大盘熔断与测谎仪（sop_v11 / app / mvp_truth_detector）

### 5.1 大盘熔断（指挥舱内 Expander）

1. 启动实盘 `streamlit run ui/app.py`（单页指挥舱），在漏斗卡片附近展开 **「📡 大盘指数熔断（防空洞）· 手动刷新」**（或旧文案「大盘熔断」）。  
2. 点击 **刷新指数熔断状态**，确认无 Python 异常，必要时查看 JSON 明细。

### 5.1.1 策略实验室（V26.2 说明区，独立进程）

1. **另开终端**执行 `streamlit run run_lab.py`（与指挥舱隔离，避免科研内存与盯盘争用）。  
2. 展开 **「V26.2 与系统对齐说明」**。  
3. **预期**：可见会话覆写、缩量三键、`scan_pools` 与指挥舱同源、100 亿过滤、痛点 CLI 等说明；页面无 `NameError`（`logging` 已导入）。

### 5.2 指数防空洞与 P4 拦截

1. 在 `config.yaml` 将 `sop_v11.circuit_breaker.enforce_block_p4` 设为 `true`（仅测试环境）。  
2. 模拟或等待指数跌幅满足阈值（或依赖东财/Tushare 回退读数）。  
3. 点击 **P4 扫描**。  
4. **预期**：出现 **Error/阻断提示**，且**不**抛出 Python 未捕获异常；将 `enforce_block_p4` 改回 `false` 后恢复可扫。

### 5.3 策略实验室「宽松档」

1. 在 `run_lab.py` 打开的页面上选 **P1 · relaxed**，调整阈值后运行。  
2. **预期**：扫描按合并后的会话参数执行；已无单独的 SOP 底线 `st.warning`（原校验已移除）。

### 5.4 会话污染与次日早盘

1. 尾盘在 **实验室**（`run_lab.py`）切换 **relaxed** 并保存。  
2. **不关闭浏览器**，次日早盘再打开。  
3. **预期**：Streamlit **session_state 可能仍保留昨日覆写**（框架行为）。**操作纪律**：手动清空实验室覆写或新开会话，勿依赖自动清零。

### 5.5 测谎仪（mvp_truth_detector）

1. 在项目根执行：  
   `python offline_tools/mvp_truth_detector.py`（若主入口为 `if __name__ == "__main__"`，按文件内说明）。  
2. 检查输出或生成报告中的 **`buy_slippage`: 0.002**、**税费** 与 **一字涨停剔除** 描述。  
3. **预期**：收益计算已体现文档所述 **+0.2% 买入滑点、−0.15% 卖出侧税费**；一字板样本被剔除或标记不可交易。

---

## 六、推荐回归清单（每次发版前）

- [ ] `python -c "from data.db_core import get_read_conn; print(get_read_conn().execute('SELECT 1').fetchone())"`  
- [ ] `streamlit run ui/app.py` 单页指挥舱正常；另开 `streamlit run run_lab.py` 策略实验室可独立打开；指挥舱内可展开大盘熔断 Expander  
- [ ] P1 洗盘 → P4 扫描 各一次  
- [ ] 查看 `scan_results` 含 `funnel`、`observation`、`sop_market_breaker`  
- [ ] 守护进程与 UI 同时压测 5 分钟无崩溃  

---

## 七、声明

本指南验证 **工程稳定性与契约一致性**，**不**保证「印钞」或任意行情下的高胜率；实盘需配合资金管理与 SOP 纪律。未来函数防护以 **按票时序指标 + 当日截面 Rank** 为准；测谎仪内 **按 trade_date 截面 Z-Score** 与训练文档一致，与实盘扫描的 Rank 场景不同，勿混为一谈。
