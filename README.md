# 小杰AI选股系统 Pro V26.6

利用完整数据和量化策略进行选股，并通过企业微信推送信号，辅以 DeepSeek AI 深度分析建议。

---

## 功能特性

- **5 大战法引擎**：竞价（P2）、盘中（P3）、尾盘（P4）、盘后（P5）、共振（GoldenTen）
- **全市场秒级回测**：支持痛点时段专项回测、战法胜率分析
- **智能扫股推送**：P1 底仓筛选 + P2~P5 信号推送，全程企业微信通知
- **DeepSeek AI 辅助**：选股结果追加 AI 分析建议
- **7×24 守护进程**：`auto_sniper_daemon` 后台运行，定时推送
- **双轨行情雷达**：自动识别大盘环境（主升浪/震荡市/情绪退潮市）
- **三层分级全局风控**：死亡红线 + 右侧攻击红线 + 雷达降权
- **资金共振体系**：capital_resonance_score（0~100）+ fund_memory_score（0~200）

---

## 系统架构全景图

```
┌─────────────────────────────────────────────────────────────┐
│                       UI 层 (Streamlit)                        │
│          app.py · strategy_lab.py · ui_components.py             │
└───────────────────────────┬─────────────────────────────────────┘
                             │  run_scan_engine()
┌───────────────────────────▼─────────────────────────────────────┐
│                       核心引擎层 (core/)                            │
│    scan_engine.py  ←── 扫股总调度                                    │
│    pool_manager.py  ←── P1 底仓管理与十一维打分                      │
│    indicator_calc.py←── 技术指标预处理（向量化优化 V2）               │
│    regime_analyzer.py←── 双轨行情雷达（大盘状态判断）                │
│    sop_v11.py       ←── 系统熔断与缩量观察池                         │
│    config_manager.py ←── YAML 热重载 + 策略实验室参数覆写            │
│    notification_gateway.py←── 企业微信推送中枢（双 webhook）         │
│    master_control.py  ←── 物理总控台（UI ↔ Daemon 跨进程状态）       │
└────────┬──────────────────────────────────────────────────────┬───┘
         │                                                      │
┌────────▼──────────────────┐    ┌────────────────────────────▼────────────┐
│  策略层 core/strategies/   │    │  数据层 (data/)                          │
│  ├── strat_base.py          │    │  ├── data_fetcher.py                     │
│  │   (打分基类/黄金门禁)   │    │  │   Tushare 增量同步 + 55 维落库       │
│  ├── strat_p2_auction.py   │    │  ├── db_core.py                          │
│  ├── strat_p3_intraday.py  │    │  │   DuckDB 数据库管理                    │
│  ├── strat_p4_tail.py      │    │  ├── api_fetcher.py                      │
│  ├── strat_p5_postmarket.py│    │  │   腾讯/新浪 实时行情瀑布式获取        │
│  ├── risk_control_engine.py │    │  ├── capital_resonance_features.py      │
│  │   三层分级全局风控      │    │  │   资金共振分（筹码底座50 + 主力底座30   │
│  ├── score_calibration.py   │    │  │   + 两融加分20）                     │
│  │   综合分稳定化          │    │  └── fund_memory_score.py               │
│  └── fund_mv_utils.py     │    │      股性记忆分（21交易日半衰期指数衰减）  │
└─────────────────────────────┘    └─────────────────────────────────────────┘
```

---

## 五档股票池体系

| 档位 | 名称 | 执行时间 | 核心策略逻辑 | 策略数量 |
|------|------|---------|-------------|---------|
| **P0** | 自选池 | 手动管理 | 用户自定义关注标的 | — |
| **P1** | 底仓池 | 每日 19:55 重建 | 十一维平滑打分 + 资金共振 + 市场环境自适应三档（严格/中性/放松） | 1 主引擎 |
| **P2** | 竞价池 | 每日 09:26 | 竞价量比 + 涨幅区间 + 龙虎榜机构连续净买 | 4 子策略 |
| **P3** | 盘中池 | 盘中 150s 轮询 | 均线低吸 + MACD 背离 + 动能共振 + 黄金门禁四阶段动态门槛 | 8+ 子策略 |
| **P4** | 盘尾池 | 14:31~14:55 五枪 | 光头阳 + 筹码锁死 + 机构尾盘潜伏 + 均线缩量低吸 | 11+ 子策略 |
| **P5** | 盘后池 | 每日 20:05 | 资金/趋势/结构/VWAP 防伪 + 爆发分均线动能补偿 + 板块内部分化修正 | 14+ 子策略 |

---

## 核心技术亮点

### 1. P1 十一维打分体系

全面评估个股质量，覆盖以下维度：

| 维度 | 评估内容 | 数据来源 |
|------|---------|---------|
| 筹码真空 | 真实换手率、量价背离检测 | turnover_rate_f, vol |
| 趋势距离 | MA20/MA60/MA120 多头排列状态 | ma20, ma60, ma120 |
| 均线成熟度 | MA20 斜率、五日均线方向 | ma20_slope_5 |
| 资金攻击 | 北向资金、主力净流入、连日正流入天数 | hk_vol, net_main_amount |
| 波段涨幅 | 近 60 日最大涨幅 | max_60d_pct |
| 黄金起爆 | 当日涨幅 + 量比 + MACD 动能柱 | pct_chg, vol_ratio, macd_bar |
| 趋势健康 | BIAS20 乖离率、高位冷却期检测 | bias_20 |
| 动态 PE | 个股 PE 相对行业分位 | pe_ttm, 行业 PE 分位统计 |
| 市值加分 | 流通市值档位（大蓝筹/中盘/核心中盘） | circ_mv |
| 行业/板块 | 战略行业加分、动态行业贝塔 | 申万行业映射、板块排名 |
| 股性记忆 | fund_memory_score（21日半衰期） | fund_memory_score |

**市场自适应三档**：根据双轨行情雷达判定的大盘环境，自动切换严格/中性/放松档位：

- **严格档（主升浪）**：MA120 粘合度要求高、MACD 柱斩杀线严、量价背离比例严 → 底仓精选
- **中性档（震荡市）**：各阈值居中 → 稳健防守
- **放松档（情绪退潮）**：适度放宽通过条件 → 提高生存率

**容错补偿网**：在资金强抢筹、筹码单峰密集、MACD 拐头改善等信号出现时，自动升高容错档（0/1/2），同步放宽 MA120 粘合度、MACD 绿柱斩杀线、量价背离比例等约束。

### 2. 双轨行情雷达（Regime Analyzer）

- **主 Regime**：近 20 日 A 股上涨家数占比均值，刻画战略环境（主升浪 / 震荡市 / 情绪退潮市）
- **副 Regime**：当日宽度 + 均涨，刻画短期情绪（高潮 / 冰点 / 回暖 / 平稳）
- SQL 层净化：剔除北交所、常见指数代码、ST 股、涨跌幅极端脏样本
- 下游直接影响：P1 档位切换、P2-P5 战法动态权重、惩罚乘子

### 3. 三层分级全局风控

| 层级 | 名称 | 触发动作 | 适用场景 |
|------|------|---------|---------|
| **第一层** | 死亡红线 | 触碰即否决出票 | 全策略 P2/P3/P4/P5 统一执行 |
| **第二层** | 右侧攻击红线 | 触碰即否决 | 仅右侧突破/主升浪类策略生效 |
| **第三层** | 雷达降权 | 累计扣分（5~30分），打标签 | 所有策略，提示建议最低买入分 |

**第一层否决项**：趋势破位（现价跌破 MA20 且斜率为负）、恶性派发（巨量收跌）、危险高开（高开 >6% 非涨停）、长上影诱多、主力资金出逃、极端 PE 泡沫

### 4. 资金共振体系

**capital_resonance_score（0~100 分）**：

- 筹码单峰底座：50 分（cyq_concentration ≥65% 起评，线性映射）
- 主力资金底座：30 分（近 5 日主力净流入 / 流通市值截面 Rank，MAD 截断防极值）
- 两融加分项：20 分（融资买入近 5 日环比增速截面 Rank）

**fund_memory_score（0~200 分）**：

- 21 交易日半衰期指数衰减
- 充值触发：涨停（limit_times≥1 或 pct_chg≥9.8%）或天量换手（turnover_rate_f≥15% 或 vol_ratio≥3.0）→ +100 分，上限 200
- 双重过滤：当日流通市值 <100 亿或近 60 日无放量异动 → 输出强制为 0

### 5. 黄金门禁四阶段动态门槛

根据 pool_key 固定门禁口径，避免时钟误伤：

| 阶段 | 时段 | 核心门槛 |
|------|------|---------|
| Stage1 | 09:30-09:45 早盘确认 | pct>2.0, vr≥3.0, 获利盘>85, 现价>筹码中枢 |
| Stage2 | 09:45-11:20 趋势确立 | pct>3.0, vr≥1.8, 现价>VWAP, MACD 柱>0 |
| Stage3 | 10:30-14:00 控盘洗盘 | pct>4.0, vr≥1.5, MA20 斜率>1.0, 近5日阳量>阴量 |
| Stage4 | 14:00后/盘后 | 2.0<pct<8.0, vr≥1.1, 尾盘增量比>1.5, 上影<2.5%, 主力净流入>0 |

### 6. 实时行情瀑布式获取

数据源优先级（顺序执行，任一足够好即返回）：

1. **腾讯行情** → 主力字段最全（price/pre_close/open/high/low/vol/amount/vol_ratio/turnover_rate_f/limit_up/down/amplitude_pct）
2. **新浪行情** → 补充腾讯失败的标的（基础字段）
3. **Tushare** → 补充 PE_TTM / PB / circ_mv（历史日线数据）
4. **本地推算** → pct_chg = (price - pre_close) / pre_close，涨跌停从昨收推算

---

## 7×24 守护进程调度表

| 时间 | 任务 | 说明 |
|------|------|------|
| 03:30 | 每周 DuckDB VACUUM | 独立子进程，ISO 周去重，交易时段跳过 |
| 08:50 | 早盘编排入口 | 清企微防刷 + 条件性同步 + P1 补建 |
| 09:18 | 早盘合并简报 | 企微一条（含 DB 探活 + 风控模式说明） |
| 09:20 | 每周心跳 | ISO 周去重，每周一执行 |
| 09:26 | P2 竞价扫描 | 与 09:18 早盘简报错峰 |
| 09:35/10:30/11:25 | 分时快照 | 记录量能 + VR 锚点 |
| 13:25 | 分时快照 | — |
| 14:25/14:39 | 分时快照 | 14:39 错峰 14:40 密集扫描 |
| 14:31~14:55 | P4 尾盘密集扫描 | 每 150s 一枪，共 5 枪，核心重仓窗 14:40~14:50 |
| 盘中持续 | P3 盘中巡逻 | 每 150s 一枪，14:31 起让路 P4 |
| 19:45 | 增量数据同步 | 含补洞 + 最新交易日半残救场（最多 5 轮） |
| 19:55 | P1 全量重建 | 含高分池企微推送（≥75 分，最多 8 只） |
| 20:05 | P5 盘后扫描 | 依赖当日同步成功且 P1 已落盘 |

---

## 目录结构

```
xiaojiePro/
├── ui/                         # Streamlit 图形界面
│   ├── app.py                  # 主界面（指挥舱大屏）
│   ├── strategy_lab.py          # 策略实验室（参数调优）
│   └── ui_components.py          # UI 组件
├── core/                       # 核心引擎
│   ├── scan_engine.py           # 扫股总调度
│   ├── pool_manager.py          # P1 底仓管理与十一维打分
│   ├── indicator_calc.py        # 技术指标预处理（向量化优化 V2）
│   ├── backtest_runner.py       # 回测引擎
│   ├── regime_analyzer.py       # 双轨行情雷达
│   ├── risk_control_engine.py   # 三层分级全局风控
│   ├── notification_gateway.py  # 企业微信推送中枢
│   ├── master_control.py        # 物理总控台（跨进程状态）
│   ├── sop_v11.py              # 系统熔断与缩量观察池
│   ├── config_manager.py         # YAML 配置热重载
│   ├── p5_morning_validation.py  # P5 次日早盘验证
│   ├── intraday_snapshot_scheduler.py  # 分时快照调度
│   ├── log_config.py            # 日志轮转配置
│   ├── runtime_data_paths.py    # 运行时数据路径管理
│   └── strategies/              # 五档策略
│       ├── strat_base.py         # 策略基类 + 黄金门禁
│       ├── strat_p2_auction.py   # P2 竞价策略
│       ├── strat_p3_intraday.py  # P3 盘中策略
│       ├── strat_p4_tail.py      # P4 尾盘策略
│       ├── strat_p5_postmarket.py# P5 盘后策略
│       ├── score_calibration.py  # 综合分稳定化
│       ├── risk_control_engine.py # 三层风控
│       └── fund_mv_utils.py     # 资金/市值工具函数
├── data/                       # 数据层
│   ├── data_fetcher.py         # Tushare 数据获取（增量同步 + 55 维落库）
│   ├── db_core.py              # DuckDB 数据库管理
│   ├── api_fetcher.py          # 实时行情获取（腾讯/新浪瀑布式）
│   ├── capital_resonance_features.py  # 资金共振分算法
│   └── fund_memory_score.py    # 股性记忆分算法
├── service/                    # 服务层
│   ├── async_scan_bridge.py     # 异步扫描队列桥接
│   └── scan_service.py          # 扫描服务
├── tools/                      # 运维工具
│   ├── weekly_db_maintenance_orchestrated.py  # 每周数据库维护
│   ├── generate_system_full_snapshot.py       # 系统全量快照
│   └── maintenance_process_control.py       # 维护进程控制
├── docs/                       # 文档
├── tests/                      # 测试
├── offline_tools/              # 离线工具
├── scripts/                   # 脚本
├── auto_sniper_daemon.py       # 7×24 守护进程
├── config.yaml                 # 系统配置（策略阈值/风控参数）
├── constants.py                # 全局常量
├── .env                       # 敏感配置（Tushare/DeepSeek/企微 Webhook）
├── .env.example               # 环境变量模板
└── requirements.txt           # Python 依赖
```

---

## 55 维日线字段契约

日线宽表（落库 59 列 = ts_code + trade_date + 55 基础字段 + capital_resonance_score + fund_memory_score）：

| 类别 | 字段 |
|------|------|
| **行情** | open, high, low, close, pre_close, pct_chg, vol, amount, turnover_rate_f, vol_ratio |
| **估值** | pe_ttm, pb, ps_ttm, dv_ratio, total_mv, circ_mv, adj_factor |
| **均线** | ma5, ma10, ma20, ma60, ma120, ma250 |
| **量能** | vol_ma5, vol_ma10, vol_ma20 |
| **动能** | ma20_slope_5, high_20, low_60, macd, macd_signal, macd_hist, rsi_14, kdj_k, kdj_d, boll_upper, boll_lower, cci, bias_20, atr_pct |
| **资金** | net_elg_amount, net_main_amount, inst_net_buy, hk_vol, rz_net_buy |
| **筹码** | cost_5th, cost_50th, cost_95th, avg_cost, winner_rate, cyq_concentration |
| **特殊** | nineturn_signal, limit_times, strth, forecast_type |
| **评分** | capital_resonance_score（0~100）, fund_memory_score（0~200） |

---

## 配置说明

所有量化参数集中在 `config.yaml`，修改后随文件 mtime 自动重载，无需重启进程。

### 关键配置区域

| 配置段 | 说明 |
|--------|------|
| `strategies.p1` | P1 底仓池：市值下限、三档 profile（strict/neutral/relaxed）、容错补偿参数 |
| `strategies.p2` | P2 竞价池：流通市值门槛、竞价涨幅区间、量比下限、机构资金门槛 |
| `strategies.p3` | P3 盘中池：乖离率上限、量比下限、MACD 动能要求 |
| `strategies.p4` | P4 尾盘池：涨幅区间、换手率门槛、筹码集中度要求 |
| `strategies.p5` | P5 盘后池：VWAP 防伪参数、均线动能补偿、板块分化修正 |
| `strategies.golden_burst` | 黄金起爆门槛：四阶段动态涨幅/量比/主力净额下限 |
| `risk_control` | 三层风控：死亡红线阈值、攻击红线乘子、雷达降权参数 |
| `regime` | 双轨雷达：回望天数、主副 Regime 阈值、SQL 净化参数 |
| `deepseek_analysis` | DeepSeek API：模型选择、超时时间、最大 token 数 |

敏感信息（Token / API Key / Webhook URL）通过 `.env` 管理，不写入 `config.yaml`：

```bash
TUSHARE_TOKEN=你的TushareToken
DEEPSEEK_API_KEY=你的DeepSeekKey
WEIXIN_WEBHOOK_URL=企微主Webhook地址
WEIXIN_WEBHOOK_URL_SECONDARY=企微次Webhook地址
```

---

## 快速开始

### 第一步：安装依赖

```bash
pip install -r requirements.txt
```

### 第二步：配置环境变量

复制 `.env.example` 为 `.env`，填入你的 Tushare Token、DeepSeek API Key 和企业微信 Webhook URL。

### 第三步：启动系统

```bash
# UI 界面
streamlit run ui/app.py

# 7×24 后台守护进程
python auto_sniper_daemon.py
```

---

## 数据来源

- 行情数据：[Tushare Pro](https://tushare.pro)
- 实时行情：腾讯行情 API / 新浪行情 API
- AI 分析：[DeepSeek](https://platform.deepseek.com)
- 推送通知：企业微信自定义机器人

---

## 版本历史

- **V26.6**：资金共振体系 + 股性记忆体系全面上线，P5 爆发分均线动能补偿优化，板块内部分化修正
- **V26.5**：P1 十一维打分 + 容错补偿网，三层风控体系重构
- **V26.2**：袖珍盘漏洞封堵（强制 100 亿流通市值门槛），涨跌停宽容逻辑增强
- 更早版本见 DEPLOYMENT_GUIDE 文档
