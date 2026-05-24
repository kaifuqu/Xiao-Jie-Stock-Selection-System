# -*- coding: utf-8 -*-
"""
一次性 / 可重复生成全量 HTML 底稿（与磁盘源码同步）：
- docs/系统代码及使用说明.html 的中文交付名
- docs/system_full_snapshot.html 的英文档名（内容相同）

勿将输出当作「源码真相」——始终以各源文件为准；HTML 仅作迁移与审计底稿。
"""
from __future__ import annotations

import html
import os
import re
from datetime import datetime
from typing import List, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 英文档名（历史）与中文档名（交付）同步写入，内容一致
OUT_SNAPSHOT = os.path.join(ROOT, "docs", "system_full_snapshot.html")
OUT_ZH = os.path.join(ROOT, "docs", "系统代码及使用说明.html")

# 参与快照的相对路径（有序：根 → core → data → service → ui → tools → offline）
MANIFEST: List[str] = [
    "constants.py",
    "config.yaml",
    "requirements.txt",
    "requirements-daemon.txt",
    "auto_sniper_daemon.py",
    "verify_system.py",
    "UPGRADE_LOG.md",
    "run_lab.py",
    "start_server.bat",
    "install_all_deps.bat",
    "start_server.sh",
]


def _collect_core_data_service_ui() -> List[str]:
    out: List[str] = []
    for sub, ext in (
        ("core", ".py"),
        ("data", ".py"),
        ("service", ".py"),
        ("ui", ".py"),
    ):
        d = os.path.join(ROOT, sub)
        if not os.path.isdir(d):
            continue
        for dirpath, _, files in os.walk(d):
            for f in sorted(files):
                if f.endswith(ext) and not f.startswith("."):
                    rel = os.path.join(dirpath, f)[len(ROOT) + 1 :].replace("\\", "/")
                    out.append(rel)
    return sorted(out)


MANIFEST.extend(_collect_core_data_service_ui())
MANIFEST.append("tools/db_inspector.py")
MANIFEST.append("tools/force_maintenance_vacuum.py")
MANIFEST.append("offline_tools/mvp_truth_detector.py")


# 人工补强：闭环角色说明（与自动 docstring 合并展示）
ROLE_NOTES: dict[str, str] = {
    "constants.py": (
        "集中 APP_VERSION、日线候选市值门槛（DAILY_BASIC_MIN_MV_WAN=100 亿量级，单位万元）、"
        "P1 流通市值下限（默认 60 亿，万元）、日线同步门槛（DAILY_BASIC_MIN_MV_WAN 等）、策略阈值等。"
        "data_fetcher 与 pool_manager 均依赖此处，避免魔法数散落；P1 下限可由 config.yaml strategies.p1.select_min_circ_mv_wan 覆写。"
    ),
    "data/data_fetcher.py": (
        "数据获取闭环上游：Tushare 拉取、按交易日断点续传、近端齐套可跳过重复下载；"
        "日线宽表含 55 个基础字段 + capital_resonance_score + fund_memory_score（常量名 ALL_55_COLS 为历史习惯）；"
        "夜间尾部 _sync_daily_features() 零 API 重算共振与资金记忆列；熔断时 raise_data_fetch_critical 联动企微。"
    ),
    "data/fund_memory_score.py": (
        "【V26.5 资金记忆】21 交易日半衰期指数衰减状态机，输出 0~200；双重过滤（流通≥100 亿且 60 日内放量异动）后才非零落库；"
        "P1 路径由 score_calibration 按 fund_memory_weight_p1 可选凸入平滑分，不进入 P4/P5 硬闸。"
    ),
    "data/capital_resonance_features.py": (
        "【V26.5】日线 capital_resonance_score（0~100）向量化：80 分底座 +20 分两融加分；"
        "与 fund_memory 并列由 _sync_daily_features 维护；P1 排序/分层闸与 P3–P5 低权重动态分使用。"
    ),
    "data/db_core.py": (
        "DuckDB：进程内写/只读单例与 get_read_conn/get_write_conn 短连接（维护/VACUUM 走独占写）、"
        "表重建、日线合并、p1_cache 表、行业 stock_basic、duckdb_vacuum_silent 等；"
        "scan_engine / pool_manager / data_fetcher 的持久化中枢。"
    ),
    "data/api_fetcher.py": (
        "实时与批量接口薄封装，供 UI 或扫描链路按需调用。"
    ),
    "core/pool_manager.py": (
        "P1 洗盘主引擎：候选码、行业贝塔、委托 score_calibration 多维分项与可选 fund_memory 凸组合、打分落盘 JSON + DuckDB；"
        "量化闭环中 P2–P5 的「底仓」来源。"
    ),
    "core/scan_engine.py": (
        "P2–P5 扫描总控：漏斗、观察池、危险表、signal_log；读取 DuckDB 与 P1 缓存，驱动各 strat_*；"
        "run_scan_engine 内 signal_log 聚合等已用 get_read_conn 短读连接，降低持锁时间。"
    ),
    "core/notification_gateway.py": (
        "企微 Webhook、异步线程池、防刷 dedup；守护进程与 UI 共用推送出口。"
    ),
    "core/regime_analyzer.py": (
        "双轨市场状态（DuckDB 聚合 + 配置/情绪键），供扫描与仓位语境使用。"
    ),
    "core/master_control.py": (
        "跨进程 JSON：企微总闸、守护进程自动巡航、maintenance_mode 维护锁等，daemon 与 Streamlit 共读。"
    ),
    "core/intraday_snapshot_scheduler.py": (
        "分时多时点 VR / 成交量锚点调度；与 scan_engine 内六槽字段（935…1440）对齐，服务量能锚点。"
    ),
    "service/async_scan_bridge.py": (
        "P3/P4 异步扫描 pending 队列、filelock 与 daemon 内 process_one_pending_scan_job 对接。"
    ),
    "auto_sniper_daemon.py": (
        "24h 调度器：交易日屏障、08:50 早安例行+早盘补位、晚间 19:05 增量→19:15 P1→19:30 P5、分时六槽（14:39 执行 slot 1440 错峰）、"
        "P3 盘中降频整分对齐且仅非阻塞抢锁，自 14:35 起让路 P4 不派线程；"
        "P4 于 14:35/40/45/50 四枪，子线程内等锁最长 900s；09:35 快照后 early_morning_p5_validation；"
        "_SCAN_BUSY 串行化扫描与快照；maintenance_mode 为真时主循环避让并跳过写库定时任务。"
    ),
    "ui/app.py": (
        "Streamlit 指挥舱：洗盘、扫描、结果展示；与 daemon 进程隔离，通过文件/DB 共享状态。"
    ),
    "tools/force_maintenance_vacuum.py": (
        "独立运维进程：开启 maintenance_mode → 缓刑等待 → 终止 auto_sniper_daemon → "
        "duckdb_vacuum_silent（短时写连接）→ 解除维护锁 → 跨平台重启守护；可由 UI 数据底座按钮投递。"
    ),
}


def _read_text(rel: str) -> Tuple[str, str]:
    p = os.path.join(ROOT, rel.replace("/", os.sep))
    if not os.path.isfile(p):
        return "", f"【缺失】路径不存在: {rel}"
    for enc in ("utf-8", "utf-8-sig", "gbk"):
        try:
            with open(p, "r", encoding=enc) as f:
                return f.read(), ""
        except UnicodeDecodeError:
            continue
    with open(p, "r", encoding="utf-8", errors="replace") as f:
        return f.read(), ""


def _extract_doc_blurb(content: str, max_len: int = 1200) -> str:
    if not content.strip():
        return "（空文件）"
    lines_all = content.splitlines()
    i = 0
    while i < len(lines_all):
        t = lines_all[i].strip()
        if t.startswith("#") and "coding" in t:
            i += 1
            continue
        if t.startswith("#!"):
            i += 1
            continue
        if t == "":
            i += 1
            continue
        break
    rest = "\n".join(lines_all[i:]).lstrip()
    if rest.startswith('"""'):
        end = rest.find('"""', 3)
        if end != -1:
            return rest[3:end].strip()[:max_len]
    if rest.startswith("'''"):
        end = rest.find("'''", 3)
        if end != -1:
            return rest[3:end].strip()[:max_len]
    comment_lines: List[str] = []
    for line in lines_all[i : i + 35]:
        stripped = line.strip()
        if stripped.startswith("#"):
            comment_lines.append(stripped.lstrip("#").strip())
        elif stripped == "" and comment_lines:
            comment_lines.append("")
        elif comment_lines:
            break
    if comment_lines:
        return "\n".join(comment_lines).strip()[:max_len]
    tail = lines_all[i : i + 22]
    if tail:
        return "\n".join(tail).strip()[:max_len]
    return "（无模块说明）"


def _anchor_id(rel: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "-", rel.replace("/", "-"))
    return f"f-{safe}"


def _default_pipeline_role(rel: str) -> str:
    """未单独撰写 ROLE_NOTES 时的兜底说明（自然语言，贴近运维阅读）。"""
    if rel == "config.yaml":
        return "全局配置中枢：接口 Token、数据库路径、regime、策略阈值、企微与 scan_async 等。"
    if rel.endswith("requirements.txt") or rel.endswith("requirements-daemon.txt"):
        return "依赖声明：pip install -r 两份文件完成部署（Windows 可用 install_all_deps.bat）；daemon 额外依赖见 requirements-daemon。"
    if rel == "install_all_deps.bat":
        return "Windows 一键安装：将 requirements.txt 与 requirements-daemon.txt 装入当前系统 Python（优先 py / python，无需 .venv）。"
    if rel.startswith("core/strategies/"):
        return "战法与筛选器实现：由 scan_engine 经各 strat_* 入口调用，常结合 fund_mv_utils 做流通市值与换手语境。"
    if rel.startswith("core/"):
        return "核心引擎：指标、扫描、回测、通知、路径与总控等，被 daemon 与 UI 共同 import。"
    if rel.startswith("ui/"):
        return "Streamlit 前端：与 auto_sniper_daemon 进程隔离，通过共享目录与 DuckDB 协作（注意并发与缓存）。"
    if rel.startswith("tools/"):
        return "运维/巡检工具：按需手动执行，不纳入 daemon 默认定时链。"
    if rel.startswith("offline_tools/"):
        return "离线科研或审计脚本：默认不参与实盘调度。"
    if rel == "verify_system.py":
        return (
            "轻量自检：配置、DuckDB、异步扫描目录、regime；"
            "V26.5 起校验 capital_resonance_score 在最新日流通≥100 亿样本上覆盖率≥85%，及 fund_memory_score 列浮点结构。"
        )
    if rel == "UPGRADE_LOG.md":
        return "V26.5 交钥匙升级说明：资金记忆、共振列、增量管道、verify 验收步骤与排障（根目录 Markdown）。"
    if rel == "run_lab.py":
        return "策略实验室独立进程入口，与主 UI 分离。"
    if rel.endswith(".bat") or rel.endswith(".sh"):
        return "启动编排：常见为守护进程与 Streamlit 并行拉起。"
    return "仓库组成部分：具体职责以本页源码及 docstring 为准。"


def _build_html() -> str:
    parts: List[str] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    parts.append("<!DOCTYPE html>")
    parts.append('<html lang="zh-CN"><head><meta charset="utf-8">')
    parts.append(
        "<title>小杰AI选股系统 Pro V26.5 — 系统代码及使用说明（全量底稿）</title>"
        "<style>"
        ":root{--bg:#0f1419;--fg:#e7e9ea;--muted:#8b98a5;--acc:#1d9bf0;--card:#192734;--border:#38444d}"
        "body{margin:0;font-family:system-ui,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--fg);"
        "line-height:1.55;font-size:15px;}"
        "nav#toc{position:sticky;top:0;max-height:100vh;overflow:auto;padding:1rem 1.25rem;background:var(--card);"
        "border-right:1px solid var(--border);min-width:280px;max-width:min(380px,40vw);}"
        "nav#toc a{color:var(--acc);text-decoration:none;display:block;padding:0.15rem 0;font-size:13px;}"
        "nav#toc a:hover{text-decoration:underline;}"
        "main{flex:1;padding:1.5rem 2rem;max-width:1200px;}"
        ".layout{display:flex;align-items:flex-start;}"
        "section{margin-bottom:2.5rem;border-bottom:1px solid var(--border);padding-bottom:2rem;}"
        "h1{font-size:1.65rem;margin-top:0}"
        "h2{font-size:1.2rem;color:var(--acc);margin-top:2rem}"
        ".meta{color:var(--muted);font-size:13px;margin:0.5rem 0 1rem}"
        ".guide{background:var(--card);border-left:4px solid var(--acc);padding:1rem 1.25rem;margin:1rem 0;"
        "white-space:pre-wrap;font-size:14px;}"
        ".role{color:#aad;}"
        "pre.source{margin:0;padding:1rem;background:#010409;border:1px solid var(--border);"
        "overflow-x:auto;font-size:12px;line-height:1.45;white-space:pre;}"
        "code{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;}"
        ".warn{border-left:4px solid #f4212e;padding-left:1rem;margin:1rem 0;}"
        ".ok{border-left:4px solid #00ba7c;padding-left:1rem;margin:1rem 0;}"
        "</style></head><body><div class='layout'>"
    )
    parts.append("<nav id='toc'><strong>目录</strong><br><br>")
    parts.append("<a href='#executive'>0. 执行摘要与版本语境</a>")
    parts.append("<a href='#compliance'>1. 运维与合规检查结论</a>")
    parts.append("<a href='#architecture'>2. 闭环架构速览</a>")
    parts.append("<a href='#usage'>3. 使用说明摘要</a>")
    for rel in MANIFEST:
        if not os.path.isfile(os.path.join(ROOT, rel.replace("/", os.sep))):
            continue
        parts.append(f"<a href='#{_anchor_id(rel)}'>{html.escape(rel)}</a>")
    parts.append("</nav><main>")

    # --- Executive ---
    parts.append("<section id='executive'>")
    parts.append("<h1>小杰AI选股系统 Pro V26.5 — 系统代码及使用说明（全量底稿）</h1>")
    parts.append(
        f"<p class='meta'>生成时间（机器本地）：{html.escape(now)} · 工作区根：<code>{html.escape(ROOT)}</code> · "
        f"同步输出：<code>docs/系统代码及使用说明.html</code> 与 <code>docs/system_full_snapshot.html</code></p>"
    )
    parts.append(
        "<div class='guide'><strong>版本语境：</strong>当前主线为 <strong>V26.5</strong>，与 <code>constants.APP_VERSION</code> 一致；"
        "含资金共振列、资金活跃度记忆（fund_memory_score）、Daemon 调度优化、P5 次日早盘验证与企微剔除闸、"
        "DuckDB maintenance_mode 与短读连接运维路径等。"
        "\n\n<strong>本 HTML 约定：</strong>下文「导读」为自然语言说明 + 自动摘录的模块 docstring；"
        "「完整源代码」区块为逐字符转义后的磁盘原文，便于审计与 diff。"
        "\n更新方式：在源文件中维护代码后，于项目根执行 <code>py tools/generate_system_full_snapshot.py</code>（或 <code>python tools/generate_system_full_snapshot.py</code>）重新生成本文档。"
        "\n部署与增量数据验收详见仓库根目录 <code>UPGRADE_LOG.md</code>。</div>"
    )
    parts.append("</section>")

    # --- Compliance ---
    parts.append("<section id='compliance'>")
    parts.append("<h2>1. 运维与合规检查结论（深度检查摘要）</h2>")

    parts.append("<h3>1.1 增量更新与重复下载</h3>")
    parts.append(
        "<div class='ok'><strong>结论：机制健全。</strong><code>data_fetcher.py</code> 明确仅同步缺失交易日，"
        "近端交易日已在库则提示跳过重复下载；日线写入走 DuckDB 合并。守护进程 <code>_sync_daily_incremental_core</code> "
        "封装 sync_recent_days 并带重试，且晚间链要求同步成功后才 P1，避免脏数据。</div>"
    )

    parts.append("<h3>1.2 分时量能锚点（六槽快照）</h3>")
    parts.append(
        "<div class='ok'><strong>结论：业务链依赖「分时六槽快照」作 VR/量能锚点；</strong>"
        "日线宽表与 DuckDB 表名按 data_fetcher 契约维护。再生成快照前建议以仓库 <code>rg</code> 自检关键词为准。</div>"
    )

    parts.append("<h3>1.3 14:40 调度与 I/O 阻塞</h3>")
    parts.append(
        "<div class='warn'><strong>结论：<code>service/scheduler.py</code> 已删除</strong>（职责并入 "
        "<code>auto_sniper_daemon.py</code>，见 <code>docs/PROJECT_FULL_INVENTORY.md</code>）。"
        "因此不存在「两个进程在 14:40 争抢」的旧模型；单一 daemon 内：</div>"
    )
    parts.append(
        "<ul><li><strong>错峰：</strong>slot <code>1440</code> 在 <strong>14:39</strong> 触发快照，"
        "与 <strong>14:40</strong> 的 P4 tick 错开一分钟，降低同一分钟内的锁竞争概率。</li>"
        "<li><strong>互斥锁：</strong>所有 P3/P4/P5、分时快照、晚间/早盘链共享 <code>_SCAN_BUSY</code>；"
        "P3 仅非阻塞抢锁；自 <strong>14:35</strong> 起进入 P4 优先走廊，<strong>不再派发 P3 线程</strong>（主循环 O(1) 跳过，不阻塞）。"
        "P4 四枪（14:35–14:50）子线程内等锁最长 <strong>900s</strong>（15 分钟），保证尾盘任务相对快照/长尾仍有机会执行；"
        "分时快照等任务另设较长超时。主线程 <code>schedule.run_pending</code> 仅投递线程 + <code>sleep(1)</code>，"
        "不被扫描 CPU 阻塞；风险主要在于串行排队与 DuckDB 写锁。</li>"
        "<li><strong>异步队列：</strong>每 10 秒 <code>process_one_pending_scan_job</code> 亦在同进程内，"
        "仍受 filelock 与业务逻辑约束，需避免与 UI 进程双消费（已由 filelock 互斥）。</li></ul>"
    )

    parts.append("<h3>1.4 市值与多维架构</h3>")
    parts.append(
        "<ul><li><strong>100 亿门槛：</strong><code>constants.DAILY_BASIC_MIN_MV_WAN</code> 与 "
        "<code>data_fetcher</code> 中 <code>_mv_sync_min</code> 对齐，单位为万元，对应总市值/流通市值侧过滤逻辑（见源码）。</li>"
        "<li><strong>500 亿：</strong>本仓库常量以 100 亿日线同步门槛与 P1 的 60 亿流通下限为主；"
        "若业务文档中的 500 亿指策略分层或回测 KPI，请见 <code>core/backtest_runner.py</code>、"
        "<code>offline_tools/mvp_truth_detector.py</code> 等处的分段标签。</li>"
        "<li><strong>55+2 列 / 52 维表述：</strong><code>data_fetcher</code> 在 55 个基础行情/指标字段之上增加 "
        "<code>capital_resonance_score</code>、<code>fund_memory_score</code>（常量列表名 ALL_55_COLS 为历史习惯）；"
        "离线工具中「52 维」属科研语境，与实盘宽表口径不同。</li></ul>"
    )

    parts.append("<h3>1.5 V26.5 资金记忆、企微闸与 P5 早盘验证</h3>")
    parts.append(
        "<div class='ok'><strong>资金记忆：</strong><code>data/fund_memory_score.py</code> 计算日线记忆分；"
        "<code>score_calibration.compute_p1_multi_dim_smooth_score</code> 按 <code>config.yaml</code> / "
        "<code>FUND_MEMORY_WEIGHT_P1</code>（约 15%–20%，权重为 0 则纯十一维）凸组合融入 P1 平滑分；"
        "<strong>不</strong>进入 P4/P5 右侧量价硬闸。"
        "</div>"
    )
    parts.append(
        "<div class='ok'><strong>P5 次日闭环：</strong>盘后 P5 写入 <code>data/runtime/state/p5_last_session.json</code>；"
        "交易日 09:35 分时快照后 <code>early_morning_p5_validation</code> 写 "
        "<code>p5_yesterday_validated.json</code>；<code>notification_gateway</code> 对「已剔除」代码跳过企微推送；"
        "侧边栏核心底仓区展示验证统计。</div>"
    )

    parts.append("<h3>1.6 DuckDB 维护模式与短读连接（V26.5）</h3>")
    parts.append(
        "<div class='ok'><strong>maintenance_mode：</strong>由 <code>core/master_control.py</code> 写入 "
        "<code>data/runtime/state/master_control.json</code>；为真时守护进程避让并在进入 "
        "<code>schedule.run_pending()</code> 前休眠轮询，同步/P1–P5/快照/异步队列等写路径跳过，避免与 "
        "<code>duckdb_vacuum_silent()</code> 或离线压库争锁。"
        "<strong>一键维护：</strong><code>tools/force_maintenance_vacuum.py</code> 或指挥舱数据底座按钮投递独立进程执行。"
        "<code>data/db_core.py</code> 提供 <code>get_read_conn</code>/<code>get_write_conn</code> 短连接；"
        "UI 与 <code>scan_engine</code> 中部分只读查询已改为短时连接。维护窗口建议关闭多余 Streamlit 标签页。</div>"
    )
    parts.append("</section>")

    # --- Architecture mermaid-style text ---
    parts.append("<section id='architecture'>")
    parts.append("<h2>2. 闭环架构速览（文字拓扑）</h2>")
    parts.append(
        "<pre class='source' style='white-space:pre-wrap'>"
        "Tushare / 配置 (config.yaml)\n"
        "    → data_fetcher（增量、断点、55基础列+crs+fund_memory）→ DuckDB (db_core)\n"
        "    → pool_manager（P1）→ p1_cache JSON + DuckDB p1_cache 表\n"
        "    → scan_engine（P2–P5；P3/P4 14:31–14:55）→ strat_* + fund_mv_utils（流通市值锚定、动态阈值）\n"
        "    → notification_gateway → 企微异步推送\n"
        "并行：auto_sniper_daemon（调度+快照槽位+队列消费） / Streamlit ui（人机操作）\n"
        "总控：master_control.json（含 maintenance_mode）+ runtime 状态目录（含 p5_*.json 等）\n"
        "</pre>"
    )
    parts.append("</section>")

    # --- Usage summary ---
    parts.append("<section id='usage'>")
    parts.append("<h2>3. 使用说明摘要</h2>")
    parts.append(
        "<ul>"
        "<li><strong>环境：</strong>项目根 <code>install_all_deps.bat</code>（Windows）或 <code>pip install -r requirements.txt -r requirements-daemon.txt</code>（系统 Python，无需 <code>.venv</code>）。</li>"
        "<li><strong>数据：</strong>侧边栏「💾 数据底座」内全量/补缺下载与压缩；或调用 <code>data_fetcher.sync_history</code> / "
        "<code>sync_recent_days</code>；同步成功后自动或手动触发特征列修补（共振+资金记忆）。"
        "<strong>运维：</strong><code>python tools/force_maintenance_vacuum.py</code> 或界面「一键强制数据库维护」；"
        "维护锁开启时勿并行大数据下载/压缩。</li>"
        "<li><strong>自检：</strong>项目根 <code>python verify_system.py</code>（含 DuckDB 列与健康度）。</li>"
        "<li><strong>Web 指挥舱：</strong>按 <code>start_server.bat</code> / <code>start_server.sh</code>（优先 <code>py</code>/<code>python</code>）或 <code>streamlit run ui/app.py</code>。</li>"
        "<li><strong>24h 调度：</strong><code>python auto_sniper_daemon.py</code>（工作目录为项目根）；与 UI 通过 master_control（含 maintenance_mode）、DuckDB、runtime 协作；P3 14:31 起让路，P4 14:31–14:55 轮询；操盘手单日清单见 <code>docs/TRADER_DAILY_WORKFLOW.md</code>。</li>"
        "<li><strong>升级交付：</strong>根目录 <code>UPGRADE_LOG.md</code>。</li>"
        "</ul>"
    )
    parts.append("</section>")

    # --- Per-file chapters ---
    for rel in MANIFEST:
        ap = os.path.join(ROOT, rel.replace("/", os.sep))
        if not os.path.isfile(ap):
            continue
        raw, err = _read_text(rel)
        if err:
            raw = err
        blurb = _extract_doc_blurb(raw)
        role = ROLE_NOTES.get(rel) or _default_pipeline_role(rel)
        parts.append(f"<section id='{_anchor_id(rel)}'>")
        parts.append(f"<h2>{html.escape(rel)}</h2>")
        parts.append("<p class='meta'>路径用途：量化闭环中的角色见下栏；源码为仓库当前快照，未作删节。</p>")
        parts.append(f"<p class='role'><strong>闭环职能说明：</strong>{html.escape(role)}</p>")
        parts.append("<div class='guide'><strong>模块导读（摘录 docstring / 头部注释，自然语言）：</strong>\n")
        parts.append(html.escape(blurb))
        parts.append("</div>")
        parts.append("<p><strong>完整源代码（100% 原文，HTML 转义显示；行号未注入以保持与磁盘一致）：</strong></p>")
        parts.append("<pre class='source'><code>")
        parts.append(html.escape(raw, quote=False))
        parts.append("</code></pre>")
        parts.append("</section>")

    parts.append("</main></div></body></html>")
    return "".join(parts)


def main() -> None:
    os.makedirs(os.path.dirname(OUT_ZH), exist_ok=True)
    doc = _build_html()
    raw = doc.encode("utf-8")
    for path in (OUT_ZH, OUT_SNAPSHOT):
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(doc)
        print("Wrote", path, "bytes", len(raw))


if __name__ == "__main__":
    main()
