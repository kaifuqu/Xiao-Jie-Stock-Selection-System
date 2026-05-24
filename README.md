# 小杰AI选股系统 Pro V26.6

利用完整数据和量化策略进行选股与企微推送。

---

## 功能特性

- **5 大战法引擎**：竞价（P2）、盘中（P3）、尾盘（P4）、盘后（P5）、共振（GoldenTen）
- **全市场秒级回测**：支持痛点时段专项回测、战法胜率分析
- **智能扫股推送**：P1 底仓筛选 + P2~P5 信号推送，全程企微通知
- **DeepSeek AI 辅助**：选股结果追加 AI 分析建议
- **7×24 守护进程**：`auto_sniper_daemon` 后台运行，定时推送

---

## 快速开始

### 第一步：克隆项目

```bash
git clone https://github.com/kaifuqu/Xiao-Jie-Stock-Selection-System.git
cd Xiao-Jie-Stock-Selection-System
```

### 第二步：安装依赖

```bash
# Python 依赖（推荐使用 conda 或 venv）
pip install -r requirements.txt

# 安装 Tushare（若未安装）
pip install tushare
```

### 第三步：启动系统（首次运行会自动引导配置）

```bash
streamlit run ui/app.py
```

> 首次运行时会自动弹出配置向导，引导你填写：
>
> - **Tushare Pro Token**（必填）：从 [tushare.pro](https://tushare.pro/register) 注册获取
> - **DeepSeek API Key**（可选）：从 [platform.deepseek.com](https://platform.deepseek.com) 获取，用于 AI 分析
> - **企微 Webhook URL**（可选）：用于接收推送通知

配置自动保存到 `.env` 文件，后续运行无需再输入。

> 后台守护进程（7×24 自动推送）请运行：`python auto_sniper_daemon.py`

---

## 目录结构

```
Xiao-Jie-Stock-Selection-System/
├── ui/                      # Streamlit 图形界面
│   ├── app.py               # 主界面
│   ├── strategy_lab.py      # 策略实验室
│   └── ui_components.py     # UI 组件
├── core/                    # 核心引擎
│   ├── scan_engine.py       # 扫股引擎
│   ├── pool_manager.py      # P1 底仓管理
│   ├── indicator_calc.py    # 指标计算
│   ├── backtest_runner.py   # 回测引擎
│   ├── strategies/          # 5 大战法
│   └── sop_v11.py           # 系统熔断控制
├── data/                    # 数据层
│   ├── data_fetcher.py      # Tushare 数据获取
│   └── db_core.py           # DuckDB 数据库
├── .env.example             # 环境变量模板（复制为 .env 使用）
├── config.yaml              # 系统配置
├── auto_sniper_daemon.py   # 后台守护进程
└── requirements.txt         # Python 依赖
```

---

## 配置文件说明

所有敏感配置通过 `.env` 文件管理（**不要将 .env 提交到 Git**）：

```bash
# .env 文件（从 .env.example 复制后填写）
TUSHARE_TOKEN=你的TushareToken
DEEPSEEK_API_KEY=你的DeepSeekKey
WEIXIN_WEBHOOK_URL=企微主Webhook地址
WEIXIN_WEBHOOK_URL_SECONDARY=企微次Webhook地址
```

---

## 常见问题

**Q: 推送没有收到？**
- 检查企微 Webhook URL 是否正确
- 检查 `config.yaml` 中 `notification.enabled` 是否为 `true`

**Q: 数据下载失败？**
- 确认 Tushare Token 有效且积分充足
- 检查网络是否能访问 `api.tushare.pro`

**Q: 如何查看运行日志？**
- 实时日志：`data/runtime/sniper.log`
- Streamlit 日志在终端直接输出

---

## 致谢

- 数据来源：[Tushare](https://tushare.pro)
- AI 分析：[DeepSeek](https://platform.deepseek.com)
- 推送通知：企业微信自定义机器人
- 如果对此系统有什么想法或问题交流，欢迎加入QQ群1090875939交流，备注：小杰选股
