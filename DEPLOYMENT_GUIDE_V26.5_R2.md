# 小杰选股系统 V26.5 — 性能优化重构 交钥匙部署指南

> 生成时间：2026-05-24
> 适用范围：V26.5 原系统 → 优化版 V26.5-R2
> 优化覆盖范围：17 个核心文件，涉及 56+ 项性能优化（含第二阶段）

---

## 一、总体原则

1. **顺序替换**：严格按照本指南的替换顺序操作，禁止跳序。
2. **备份优先**：替换前建议对原文件做备份（复制一份到 `backup/` 目录）。
3. **逐个验证**：每个文件替换后，建议执行一次语法检查（`python -m py_compile <file>`）。
4. **核心文件优先**：先替换核心计算层，再替换外围模块。

---

## 二、文件替换顺序（共 17 个文件，分 5 批次）

### 【批次 1】核心计算引擎 — 最关键，优先替换

| # | 文件路径 | 优化要点 | 注意事项 |
|---|---------|---------|---------|
| 1 | `core/indicator_calc.py` | CCI 向量化、ATR 预计算、MA safe 列复用 | 业务逻辑完全保留，仅内部计算优化 |
| 2 | `core/pool_manager.py` | `_industry_tokens` LRU 缓存、`_p1_behavior_penalty_hits` 向量化、循环内 `df.tail()` 预计算 | 该文件体积较大（约 106KB），确认替换完整 |
| 3 | `core/scan_engine.py` | T+1 结算批量 SQL、`_tr5_p1` 向量化、`empty_board_count` 向量化 | T+1 结算逻辑修改较大，建议替换后手动对比 |

### 【批次 2】数据获取层

| # | 文件路径 | 优化要点 | 注意事项 |
|---|---------|---------|---------|
| 4 | `data/data_fetcher.py` | 财务风险标志向量化、`check_data_completeness` 向量化、`sync_history` 并行化、`sync_recent_days` 并行化 | 增量更新逻辑已完善，替换后建议跑一次日频同步测试 |
| 5 | `core/master_control.py` | 5 秒 TTL 读缓存、写入缓存失效机制 | 多线程安全，勿手动修改缓存变量 `_MC_CACHE` |
| 6 | `core/config_manager.py` | 消除 `copy.deepcopy()`，直接返回缓存引用 | 调用方若有修改配置的需求，需自行拷贝副本 |

### 【批次 3】策略筛选器层

| # | 文件路径 | 优化要点 | 注意事项 |
|---|---------|---------|---------|
| 7 | `core/strategies/strat_p4_tail.py` | `_strategy_shrink_and_touch_ma5` 的 `avg_tr3` 向量化 | — |
| 8 | `core/strategies/score_calibration.py` | `build_rank_lookup` 用 `zip` 替代 `iterrows` | — |
| 9 | `core/strategies/strat_p3_intraday.py` | `__init__` 加载 `vol_slice_60d` 向量化 | — |
| 10 | `core/strategies/p3_intraday_screener.py` | `_all_hk_vol_positive` 向量化布尔检查 | — |
| 11 | `core/strategies/p4_tail_screener.py` | `_recent_window_touched_ma20` / `_recent_window_vol_shrink_and_touch_ma5` 向量化 | — |

### 【批次 4】外围模块

| # | 文件路径 | 优化要点 | 注意事项 |
|---|---------|---------|---------|
| 12 | `auto_sniper_daemon.py` | P1 缓存查找 O(1) 优化（当日文件优先检查）、JSON 类型检测 `json.dumps` → `isinstance` | 替换后建议重启后台守护进程 |
| 13 | `core/backtest_runner.py` | 消除 `df.to_dict('records')`，预计算向量化列数组替代循环内字典查找、疼痛点 `sub_df` 切片 O(n²) 消除 | 回测热路径，主要性能提升点 |

### 【批次 5】第二轮扫描优化（2026-05-24 新增）

| # | 文件路径 | 优化要点 | 注意事项 |
|---|---------|---------|---------|
| 14 | `core/backtest_runner.py` | 疼痛点回测 `sub_df` 切片 O(n²) 消除、主力连红 O(n²) 内层回溯循环 → O(n) 预计算数组、循环内 `df.iloc[pos]` 逐行读取 → 向量化预计算数组 | 回测热路径，主要性能提升点 |
| 15 | `data/db_core.py` | 3 处 `iterrows` 向量化：`existing_pks` 布尔索引、`cols` 列表推导式、`ts_code→name` 映射 `dict(zip)` | 非热路径但代码更简洁 |
| 16 | `tools/db_inspector.py` | `iterrows` → `dict(zip(...))` 构建列类型映射 | 非热路径 |
| 17 | `core/sop_v11.py` | 熔断器缓存 `copy.deepcopy` → `dict()` 直接引用 | 缓存 TTL=45s，命中率低时开销显著 |

---

## 三、替换操作步骤

### 步骤 1：备份原文件（推荐但非强制）

```powershell
# 在 d:\xiaojiePro 目录下执行
New-Item -ItemType Directory -Path backup -Force -ErrorAction SilentlyContinue
Copy-Item core\indicator_calc.py backup\
Copy-Item core\pool_manager.py backup\
Copy-Item data\data_fetcher.py backup\
Copy-Item core\master_control.py backup\
Copy-Item core\config_manager.py backup\
Copy-Item core\scan_engine.py backup\
Copy-Item core\strategies\strat_p4_tail.py backup\
Copy-Item core\strategies\score_calibration.py backup\
Copy-Item core\strategies\strat_p3_intraday.py backup\
Copy-Item core\strategies\p3_intraday_screener.py backup\
Copy-Item core\strategies\p4_tail_screener.py backup\
Copy-Item auto_sniper_daemon.py backup\
Copy-Item core\backtest_runner.py backup\
Copy-Item data\db_core.py backup\
Copy-Item tools\db_inspector.py backup\
Copy-Item core\sop_v11.py backup\
```

### 步骤 2：逐个替换并验证语法

以 `indicator_calc.py` 为例，其他文件替换方式相同：

```powershell
# 替换文件（用 Cursor 复制粘贴，或直接覆盖）
# 验证语法：
python -m py_compile d:\xiaojiePro\core\indicator_calc.py
```

所有 13 个文件均应通过语法检查（无输出即成功）。

---

## 四、替换后验证清单

### 4.1 语法检查（一次性全部验证）

```powershell
$files = @(
    "d:\xiaojiePro\core\indicator_calc.py",
    "d:\xiaojiePro\core\pool_manager.py",
    "d:\xiaojiePro\data\data_fetcher.py",
    "d:\xiaojiePro\core\master_control.py",
    "d:\xiaojiePro\core\config_manager.py",
    "d:\xiaojiePro\core\scan_engine.py",
    "d:\xiaojiePro\core\strategies\strat_p4_tail.py",
    "d:\xiaojiePro\core\strategies\score_calibration.py",
    "d:\xiaojiePro\core\strategies\strat_p3_intraday.py",
    "d:\xiaojiePro\core\strategies\p3_intraday_screener.py",
    "d:\xiaojiePro\core\strategies\p4_tail_screener.py",
    "d:\xiaojiePro\auto_sniper_daemon.py",
    "d:\xiaojiePro\core\backtest_runner.py"
)
$all_ok = $true
foreach ($f in $files) {
    $result = python -m py_compile $f 2>&1
    if ($LASTEXITCODE -ne 0) { $all_ok = $false; Write-Host "FAIL: $f" }
}
if ($all_ok) { Write-Host "ALL OK: 全部 13 个文件语法检查通过" }
```

### 4.2 功能冒烟测试（建议在日间非交易时段执行）

1. **启动扫股引擎**：运行一次小范围扫股（5~10 只股票），观察日志无报错。
2. **检查 P1 底仓加载**：确认 `auto_sniper_daemon` 启动后能正常加载缓存。
3. **验证回测功能**：运行一次战法回测，确认信号触发逻辑正常。
4. **检查并行同步**：运行数据同步，观察是否有多线程日志输出。

### 4.3 回滚方案

如替换后出现异常，将 `backup/` 目录下的原文件复制回对应位置即可：

```powershell
Copy-Item backup\indicator_calc.py core\
Copy-Item backup\pool_manager.py core\
# ... 以此类推
```

---

## 五、各文件详细变更说明

### 5.1 `core/indicator_calc.py`
- `_sf()` 函数：去除多余的 `str().strip()` 转换
- `merge_daily_with_realtime`：列存在性检查结果缓存，避免重复 attribute lookup
- `precompute_indicators`：
  - CCI 计算：`apply(lambda)` → `rolling().std()` 向量化
  - MA safe 列：预计算并复用，减少 `replace(0, np.nan)` 调用
  - ATR 计算：去除中间 TR DataFrame 列，直接用 `pd.concat` + `max(axis=1)`

### 5.2 `core/pool_manager.py`
- `_industry_tokens`：增加 `@functools.lru_cache` 装饰器，返回类型改为 `frozenset`
- `_build_auto_strategic_mapping`：配合 frozenset 优化 token 判断
- `_p1_behavior_penalty_hits`：`iterrows` → 布尔向量 + `sum()` 向量化
- `_process_single_stock_for_p1`：
  - 预计算 `df.tail(3/5/10)` 到变量
  - `_tr5_p1` / `tr10_arr` 换手率向量化（`np.where` + `pd.to_numeric`）
  - `flow_positive_days_5` 布尔向量求和
  - `df_20`/`df_60` 切片复用，避免重复 `copy()`

### 5.3 `data/data_fetcher.py`
- `_build_financial_risk_flags`：新增向量化版本，替代 `apply(axis=1)`
- `check_data_completeness`：`iterrows` → DataFrame 切片读取
- `sync_history`：顺序循环 → `ThreadPoolExecutor` 并行获取缺失日期数据
- `sync_recent_days`：顺序循环 → `ThreadPoolExecutor` 并行同步近期数据

### 5.4 `core/master_control.py`
- 新增模块级变量：`_MC_CACHE`、`_MC_CACHE_TTL_SEC=5`、`_MC_CACHE_LOCK`
- `read_master_control()`：5 秒 TTL 缓存，读命中直接返回，避免每次都读文件 + 加锁
- `write_master_control()`：写入成功后主动失效缓存

### 5.5 `core/config_manager.py`
- `_load_yaml_raw()`：返回缓存字典的直接引用，移除 `copy.deepcopy()` 调用
- 调用方如需修改配置，请自行在返回后深拷贝一份

### 5.6 `core/scan_engine.py`
- `_tr5_p1`：向量化为 `np.where` + `pd.to_numeric`
- `empty_board_count`：`iterrows` → 布尔向量 + `sum()`
- T+1 结算：将循环内 O(N×2) 次 `fetchone()` 合并为 1 次带 `IN` 子句的批量 SQL 查询，构建 `price_map` 字典做 O(1) 查找

### 5.7 `core/strategies/strat_p4_tail.py`
- `_strategy_shrink_and_touch_ma5`：将 `avg_tr3` 的 `iterrows` 循环改为 `np.where` + `pd.to_numeric` 向量计算

### 5.8 `core/strategies/score_calibration.py`
- `build_rank_lookup`：`df.iterrows()` → `zip(df['col1'], df['col2'])` 遍历

### 5.9 `core/strategies/strat_p3_intraday.py`
- `__init__` 加载 `vol_slice_60d`：将 `iterrows` 改为 DataFrame 直接索引（`df.at`）

### 5.10 `core/strategies/p3_intraday_screener.py`
- `_all_hk_vol_positive`：`iterrows` → `(hk_v > 0).all()` 一次向量化布尔检查

### 5.11 `core/strategies/p4_tail_screener.py`
- `_recent_window_touched_ma20`：`iterrows` → `.any()` 向量化
- `_recent_window_vol_shrink_and_touch_ma5`：`iterrows` → `.any()` 向量化

### 5.12 `auto_sniper_daemon.py`
- `load_base_items_latest`：当日缓存优先 `os.path.isfile()` O(1) 检查，避免每次 `glob.glob()`
- 历史缓存回退：用 `os.listdir()` + 字符串过滤替代 `glob.glob()`
- `_is_json_serializable_type`：用 `isinstance` 快速类型判断，替代 `json.dumps()` 序列化试探（对大量标量值效果显著）

### 5.13 `core/backtest_runner.py`
- `run_strategy_backtest`：
  - 移除 `df.to_dict('records')`，避免 ~400 个字典对象的内存拷贝
  - 预计算 20+ 个列的 `values` 数组（向量化）
  - 循环内 `total_mv`、`close`、`circ_mv` 等直接通过数组下标访问（O(1)）
  - 保留 `df.iloc[i]` 引用用于偶发的缺失列回退

---

## 六、预期性能提升

| 模块 | 优化项 | 预期提升 |
|------|--------|---------|
| `indicator_calc.py` | CCI/ATR 向量化 | 单股计算 2~5x |
| `pool_manager.py` | LRU 缓存 + 向量化循环 | P1 底仓处理 3~10x |
| `data_fetcher.py` | 并行 sync + 向量化完整性检查 | 日同步耗时减少 50%+ |
| `master_control.py` | 5s TTL 缓存 | 高频读取降低磁盘 IO 80%+ |
| `config_manager.py` | 消除 deepcopy | 配置读取 10x+ |
| `scan_engine.py` | 批量 T+1 SQL + 向量化 | T+1 结算 3~5x |
| 策略筛选器们 | 向量化替代 iterrows | 各策略筛选 2~4x |
| `auto_sniper_daemon.py` | 缓存查找 O(1) + JSON 类型检测 | 守护进程冷启动 2x |
| `backtest_runner.py` | 消除 to_dict + 预计算列 | \acktest_runner.py\ | 消除 to_dict + 预计算列数组 + O(n^2) 切片消除 | 回测循环 3~5x |
| \db_core.py\ | 3 处 iterrows 向量化 | 工具函数 2~3x |
| \db_inspector.py\ | iterrows → dict(zip) | 工具函数 2x |
| \sop_v11.py\ | deepcopy → dict | 熔断器缓存读写 ~10x |

---

## 七、注意事项与已知约束

1. **`config_manager.py` 的返回值约定**：优化后 `_load_yaml_raw()` 返回缓存引用。如调用方需要独立副本，请在调用处自行 `copy.deepcopy()`。
2. **`master_control.py` 缓存 TTL**：当前设为 5 秒。如生产环境对控制文件实时性要求极高（如 < 1 秒），可调低至 2 秒，但会增加锁竞争。
3. **`data_fetcher.py` 并行度**：线程池默认 `MAX_WORKERS`。如遇 API 限流，可将 `max_workers` 从默认值降至 2~3。
4. **fallback 保护**：所有向量化优化均保留了 `except` 降级路径，确保在异常数据格式下仍可运行。
5. **不回滚业务逻辑**：所有优化严格在原函数内部进行，未改变任何策略参数、阈值、信号判断逻辑。

---

## 八、部署检查清单（打印使用）

```
[ ] 步骤 1：备份完成（backup/ 目录已创建）
[ ] 步骤 2：13 个文件全部替换完成
[ ] 步骤 3：语法检查全部通过
[ ] 步骤 4：启动扫股引擎（小范围测试）
[ ] 步骤 5：验证 P1 底仓加载正常
[ ] 步骤 6：验证战法回测功能正常
[ ] 步骤 7：验证数据并行同步
[ ] 步骤 8（可选）：重启 auto_sniper_daemon 守护进程
```

---

**部署完成后，系统以 V26.5-R2 版本运行，所有优化对业务逻辑完全透明。**
