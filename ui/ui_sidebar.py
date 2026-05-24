# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 - 侧边栏控制中枢（宽域八极雷达版）
【UI 修复与升级】：
1. 📊 雷达扩容：将领跌板块从 Bottom 5 扩容至 Bottom 8，与进攻区 Top 8 形成绝对对称，全景监控资金退潮方向。
2. 🧹 视觉精简：彻底拔除冗余的手动“市场环境预判(Regime)”下拉框，100% 交由顶部双轨雷达自动驾驶。
3. ✨ 排版优化：精简 P1/P0 底仓模式的标题文案，彻底解决侧边栏文字换行导致的视觉臃肿。
4. 强制目录收纳：P0 自选股上传文件强制写入 data/ 目录。
"""
import json
import os
import locale
import subprocess
import sys
import streamlit as st
import time
from datetime import datetime
import constants


def _project_root_ui() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _is_maintenance_mode_ui() -> bool:
    """与守护进程共用 master_control.json；维护中为 True 时应避免 UI 侧大事务写库或与 VACUUM 争用。"""
    try:
        from core.master_control import is_maintenance_mode_enabled

        return bool(is_maintenance_mode_enabled())
    except Exception:
        return False


def _load_sniper_pipeline_state_for_ui() -> dict:
    """与 auto_sniper_daemon 共用落盘文件，用于网页端观测定时增量是否曾成功。"""
    p = os.path.join(_project_root_ui(), "data", "runtime", "state", "sniper_pipeline_state.json")
    if not os.path.isfile(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            o = json.load(f)
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}


def _is_duckdb_windows_file_lock_error(exc: Exception) -> bool:
    """识别 Windows 下 DuckDB 主库被其它进程占用的典型报错。"""
    msg = str(exc or "").lower()
    if not msg:
        return False
    return (
        ("quant_data.duckdb" in msg and "cannot open file" in msg)
        or ("file is already open in" in msg)
        or ("另一个程序正在使用此文件" in msg)
    )


def _run_compact_db_offline() -> tuple[bool, str]:
    """
    调用 tools/compact_db_offline.py 执行离线压缩。
    返回 (ok, output_text)。
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = os.path.join(root, "tools", "compact_db_offline.py")
    if not os.path.isfile(script):
        return False, f"压缩脚本不存在: {script}"
    py = sys.executable or "python"
    try:
        cp = subprocess.run(
            [py, script],
            cwd=root,
            capture_output=True,
            timeout=1800,
        )
    except subprocess.TimeoutExpired:
        return False, "压缩超时（>30 分钟），请检查是否有进程占用 quant_data.duckdb。"
    except Exception as e:
        return False, f"启动压缩脚本失败: {e}"
    def _decode_out(b: bytes) -> str:
        if not b:
            return ""
        for enc in (
            locale.getpreferredencoding(False),
            "gbk",
            "cp936",
            "utf-8",
        ):
            try:
                return b.decode(enc)
            except Exception:
                continue
        return b.decode("utf-8", errors="replace")

    stdout_txt = _decode_out(cp.stdout or b"")
    stderr_txt = _decode_out(cp.stderr or b"")
    out = (stdout_txt + ("\n" + stderr_txt if stderr_txt else "")).strip()
    ok = (cp.returncode == 0)
    if not out:
        out = f"压缩脚本已结束，exit_code={cp.returncode}"
    return ok, out


def _precheck_db_file_lock_for_compact() -> tuple[bool, str]:
    """
    压缩前轻量探测主库是否被占用。
    返回 (ok_to_continue, message)。
    """
    try:
        from data.db_core import get_duckdb_path, get_read_conn

        dbp = get_duckdb_path()
    except Exception:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        dbp = os.path.join(root, "data", "quant_data.duckdb")

    if not os.path.isfile(dbp):
        return False, f"未检测到主库文件: {dbp}"

    try:
        # 短时只读连接：with 结束即 close，避免与守护进程长期争用同一读句柄。
        with get_read_conn(read_only=True) as con:
            con.execute("SELECT 1").fetchone()
        return True, f"预检测通过：主库可访问（{dbp}）"
    except Exception as e:
        if _is_duckdb_windows_file_lock_error(e):
            return (
                False,
                "预检测失败：quant_data.duckdb 正被其它 Python 进程占用。"
                "请先停止 daemon/其它写库进程后再压缩。",
            )
        return False, f"预检测失败：{e}"


def _list_daemon_pids_windows() -> list[int]:
    """枚举命令行包含 auto_sniper_daemon.py 的进程 PID（Windows）。"""
    ps_cmd = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -and $_.CommandLine -match 'auto_sniper_daemon.py' } | "
        "Select-Object -ExpandProperty ProcessId"
    )
    cp = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_cmd],
        capture_output=True,
        timeout=8,
    )
    out = (cp.stdout or b"").decode(errors="ignore")
    pids: list[int] = []
    for line in out.splitlines():
        s = line.strip()
        if s.isdigit():
            try:
                pids.append(int(s))
            except Exception:
                pass
    return sorted(set(pids))


def _stop_daemon_pids(pids: list[int]) -> tuple[bool, str]:
    if not pids:
        return True, "未检测到 daemon 进程。"
    logs: list[str] = []
    ok = True
    for pid in pids:
        killed = False
        # 通道1：taskkill（兼容性最好）
        cp = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            timeout=10,
        )
        one = ((cp.stdout or b"") + b"\n" + (cp.stderr or b"")).decode(errors="ignore").strip()
        logs.append(f"[taskkill PID {pid}] rc={cp.returncode} {one}")
        txt = one.lower()
        if cp.returncode == 0 or ("not found" in txt) or ("没有运行的任务" in one):
            killed = True
        # 通道2：PowerShell Stop-Process（taskkill 失败时兜底）
        if not killed:
            ps = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    f"Stop-Process -Id {pid} -Force -ErrorAction SilentlyContinue; "
                    f"if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 2 }} else {{ exit 0 }}",
                ],
                capture_output=True,
                timeout=10,
            )
            two = ((ps.stdout or b"") + b"\n" + (ps.stderr or b"")).decode(errors="ignore").strip()
            logs.append(f"[Stop-Process PID {pid}] rc={ps.returncode} {two}")
            if ps.returncode == 0:
                killed = True
        if not killed:
            ok = False
    return ok, "\n".join(logs)


def _start_daemon_background() -> tuple[bool, str]:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    daemon_py = os.path.join(root, "auto_sniper_daemon.py")
    py = sys.executable or "python"
    try:
        flags = 0
        if os.name == "nt":
            flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
                subprocess, "CREATE_NEW_PROCESS_GROUP", 0
            )
        # 守护进程自身已 RotatingFileHandler 写 data/runtime/sniper.log；勿再 shell 重定向以免双写撑盘
        subprocess.Popen(
            [py, daemon_py],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=flags,
            close_fds=False,
        )
        return True, f"已启动 daemon: {daemon_py}（日志见 data/runtime/sniper.log）"
    except Exception as e:
        return False, f"重启 daemon 失败: {e}"


def _run_tushare_api_probe(data_fetcher_mod) -> tuple[bool, str]:
    """
    运行高阶接口可用性探针，返回 (ok, log_text)。
    ok=True 表示至少核心接口有可用返回（不要求每个接口都有数据）。
    """
    lines: list[str] = []
    try:
        pro = getattr(data_fetcher_mod, "pro", None)
        if pro is None:
            return False, "pro 未初始化：请先检查 Tushare Token。"
        retry_api = getattr(data_fetcher_mod, "retry_api")
        norm_date = getattr(data_fetcher_mod, "_norm_cal_date_8")
        fetch_chunked = getattr(data_fetcher_mod, "fetch_chunked")
    except Exception as e:
        return False, f"探针初始化失败: {e}"

    try:
        from data.db_core import get_latest_daily_data_trade_date_yyyymmdd

        td = norm_date(get_latest_daily_data_trade_date_yyyymmdd() or "")
    except Exception:
        td = ""
    if not td:
        td = datetime.now().strftime("%Y%m%d")

    lines.append(f"[INFO] probe trade_date={td}")

    # 1) 交易日历基本探活
    try:
        cal = retry_api(pro.trade_cal)(exchange="SSE", start_date=td, end_date=td)
        n = 0 if cal is None else len(cal)
        lines.append(f"[OK] trade_cal rows={n}")
    except Exception as e:
        lines.append(f"[FAIL] trade_cal: {e}")

    # 2) 高阶接口（整日回退语义）
    apis = [
        ("cyq_perf", pro.cyq_perf),
        ("hk_hold", pro.hk_hold),
        ("margin_detail", pro.margin_detail),
        ("top_inst", pro.top_inst),
    ]
    any_ok = False
    for name, fn in apis:
        try:
            df_all = retry_api(fn)(trade_date=td)
            n_all = 0 if df_all is None else len(df_all)
            lines.append(f"[OK] {name:<13} trade_date rows={n_all}")
            if n_all > 0:
                any_ok = True
        except Exception as e:
            lines.append(f"[FAIL] {name:<13} trade_date: {e}")

    # 涨跌停新接口（科室6主数据源）
    if hasattr(pro, "limit_list_d"):
        try:
            df_ld = retry_api(pro.limit_list_d)(trade_date=td, limit_type="U")
            n_ld = 0 if df_ld is None else len(df_ld)
            lines.append(f"[OK] {'limit_list_d(U)':<13} trade_date rows={n_ld}")
            if n_ld > 0:
                any_ok = True
        except Exception as e:
            lines.append(f"[FAIL] {'limit_list_d(U)':<13} trade_date: {e}")
    try:
        df_ll = retry_api(pro.limit_list)(trade_date=td)
        n_ll = 0 if df_ll is None else len(df_ll)
        lines.append(f"[OK] {'limit_list':<13} trade_date rows={n_ll}")
        if n_ll > 0:
            any_ok = True
    except Exception as e:
        lines.append(f"[FAIL] {'limit_list':<13} trade_date: {e}")

    # 3) ts_code 分块语义探针（模拟生产链路）
    try:
        from data.db_core import get_all_stock_codes

        codes = list(get_all_stock_codes() or [])[:800]
    except Exception:
        codes = []
    if codes:
        for name, fn in [("cyq_perf", pro.cyq_perf), ("hk_hold", pro.hk_hold), ("margin_detail", pro.margin_detail)]:
            try:
                df_chunk = fetch_chunked(fn, codes, td, chunk_size=400)
                n_chunk = 0 if df_chunk is None else len(df_chunk)
                lines.append(f"[OK] {name:<13} ts_code_chunk rows={n_chunk} (codes={len(codes)})")
                if n_chunk > 0:
                    any_ok = True
            except Exception as e:
                lines.append(f"[FAIL] {name:<13} ts_code_chunk: {e}")
    else:
        lines.append("[WARN] ts_code_chunk probe skipped: no local codes")

    return any_ok, "\n".join(lines)


def render_master_control_sidebar() -> None:
    """
    🎛️ 物理总控台：企微推送总闸 + 全自动巡航总闸。
    状态持久化至 data/runtime/state/master_control.json，与 auto_sniper_daemon 共享。

    【多标签/手工改 JSON】每次 Streamlit 重跑比较磁盘 mtime，有变化则从文件重载开关；
    另提供「从磁盘刷新」按钮强制对齐。
    """
    from core.master_control import read_master_control, write_master_control, get_master_control_state_path

    mc_path = get_master_control_state_path()
    try:
        disk_mtime = os.path.getmtime(mc_path) if os.path.isfile(mc_path) else 0.0
    except OSError:
        disk_mtime = 0.0
    prev_mtime = st.session_state.get("mc_disk_mtime")
    if prev_mtime is None or float(prev_mtime) != float(disk_mtime):
        mc0 = read_master_control()
        st.session_state["mc_wechat"] = bool(mc0.get("wechat_push_enabled", True))
        st.session_state["mc_daemon"] = bool(mc0.get("daemon_auto_cruise_enabled", True))
        # 与 master_control.json 对齐；缺省 False，避免误推送
        st.session_state["push_p1_high_score_enabled"] = bool(mc0.get("push_p1_high_score_enabled", False))
        st.session_state["mc_disk_mtime"] = disk_mtime
    # 升级后旧会话可能缺少该键：补一次缺省，避免 toggle 报错
    if "push_p1_high_score_enabled" not in st.session_state:
        _mc_fill = read_master_control()
        st.session_state["push_p1_high_score_enabled"] = bool(_mc_fill.get("push_p1_high_score_enabled", False))

    st.sidebar.markdown(
        """
<div style="text-align:center;font-weight:700;font-size:1.06rem;margin:0 0 0.25rem 0;line-height:1.35;">
🎛️ 系统总控中心<br/>
<span style="font-size:0.78rem;font-weight:500;color:#64748b;">(Master Control)</span>
</div>
""",
        unsafe_allow_html=True,
    )
    st.sidebar.divider()
    st.sidebar.caption("跨进程总闸 · 与后台 Daemon 共用同一状态文件")

    def _persist_master_control() -> None:
        write_master_control(
            wechat_push_enabled=bool(st.session_state.get("mc_wechat", True)),
            daemon_auto_cruise_enabled=bool(st.session_state.get("mc_daemon", True)),
            push_p1_high_score_enabled=bool(st.session_state.get("push_p1_high_score_enabled", False)),
        )
        try:
            st.session_state["mc_disk_mtime"] = os.path.getmtime(mc_path)
        except OSError:
            st.session_state["mc_disk_mtime"] = time.time()
        try:
            st.toast("总控状态已写入 master_control.json", icon="🎛️")
        except Exception:
            try:
                st.sidebar.success("已同步总控状态")
            except Exception:
                pass

    _toggle = getattr(st.sidebar, "toggle", None)
    if _toggle is not None:
        _toggle(
            "🔔 企微实盘推送",
            key="mc_wechat",
            on_change=_persist_master_control,
            help="总闸关闭后，本机任何进程均不会向企微 Webhook 发出消息。",
        )
    else:
        st.sidebar.checkbox(
            "🔔 企微实盘推送",
            key="mc_wechat",
            on_change=_persist_master_control,
            help="总闸关闭后，本机任何进程均不会向企微 Webhook 发出消息。",
        )
    st.sidebar.markdown(
        "<div style='font-size:12px;color:#64748b;margin:-6px 0 10px 0;'>开启后允许发送信号至手机</div>",
        unsafe_allow_html=True,
    )

    if _toggle is not None:
        _toggle(
            "🤖 24H 全自动巡航",
            key="mc_daemon",
            on_change=_persist_master_control,
            help="关闭后，独立守护进程将跳过下载、P1 重建与 P3/P4/P5 扫描。开启时：休市日（周末/法定节假日）自动跳过同步与企微推送。",
        )
    else:
        st.sidebar.checkbox(
            "🤖 24H 全自动巡航",
            key="mc_daemon",
            on_change=_persist_master_control,
            help="关闭后，独立守护进程将跳过下载、P1 重建与 P3/P4/P5 扫描。开启时：休市日（周末/法定节假日）自动跳过同步与企微推送。",
        )
    st.sidebar.markdown(
        "<div style='font-size:12px;color:#64748b;margin:-6px 0 8px 0;'>"
        "开启后台 Daemon 的自动下载与扫描。<br/>休市日自动跳过同步与推送"
        "</div>",
        unsafe_allow_html=True,
    )

    if _toggle is not None:
        _toggle(
            "🌟 推送 P1 高分池 (≥75分)",
            key="push_p1_high_score_enabled",
            on_change=_persist_master_control,
            help="开启后，在全量洗盘结束后自动将 75 分以上的底仓股票推送至企业微信。",
        )
    else:
        st.sidebar.checkbox(
            "🌟 推送 P1 高分池 (≥75分)",
            key="push_p1_high_score_enabled",
            on_change=_persist_master_control,
            help="开启后，在全量洗盘结束后自动将 75 分以上的底仓股票推送至企业微信。",
        )
    st.sidebar.markdown(
        "<div style='font-size:12px;color:#64748b;margin:-6px 0 8px 0;'>"
        "开启后，在全量洗盘结束后自动将 75 分以上的底仓股票推送至企业微信。"
        "</div>",
        unsafe_allow_html=True,
    )

    c1, c2 = st.sidebar.columns(2)
    with c1:
        if st.button("🔄 磁盘刷新", key="mc_btn_refresh_disk", help="从 master_control.json 强制重载（多标签/手工改文件）"):
            st.session_state.pop("mc_disk_mtime", None)
            try:
                m = read_master_control()
                st.session_state["mc_wechat"] = bool(m.get("wechat_push_enabled", True))
                st.session_state["mc_daemon"] = bool(m.get("daemon_auto_cruise_enabled", True))
                st.session_state["push_p1_high_score_enabled"] = bool(m.get("push_p1_high_score_enabled", False))
                if os.path.isfile(mc_path):
                    st.session_state["mc_disk_mtime"] = os.path.getmtime(mc_path)
            except Exception as ex:
                st.sidebar.error(f"刷新失败: {ex}")
            try:
                st.toast("已从磁盘重载总控状态", icon="🔄")
            except Exception:
                pass
            # st.button 本身已触发一次重跑；此处再手动 rerun 在部分前端会出现重复卸载节点异常。
    with c2:
        st.caption(f"修改时间：`{disk_mtime:.0f}`" if disk_mtime else "`（无文件·缺省全开）`")

    # 与 app.py 扫描推送开关对齐（notify_scan_results_top3_p2p3p4 仍读此键）
    st.session_state["wechat_notify_enabled"] = bool(st.session_state.get("mc_wechat", True))

    # ---------- 风控模式：config.yaml risk_control.ui_alert_only（与引擎 get_risk_control_config 联动）----------
    try:
        from core.config_manager import (
            get_config_yaml_path,
            get_ui_alert_only,
            write_ui_alert_only_to_config_yaml,
            invalidate_config_cache,
        )

        _cfg_yaml_path = get_config_yaml_path()
        try:
            _mt_risk_cfg = os.path.getmtime(_cfg_yaml_path) if os.path.isfile(_cfg_yaml_path) else 0.0
        except OSError:
            _mt_risk_cfg = 0.0
        _prev_risk_mt = st.session_state.get("cfg_yaml_mtime_ui_alert")
        if _prev_risk_mt is None or float(_prev_risk_mt) != float(_mt_risk_cfg):
            st.session_state["rc_ui_alert_only"] = bool(get_ui_alert_only(force_reload=True))
            st.session_state["cfg_yaml_mtime_ui_alert"] = _mt_risk_cfg
        if "rc_ui_alert_only" not in st.session_state:
            st.session_state["rc_ui_alert_only"] = bool(get_ui_alert_only(force_reload=True))

        st.sidebar.markdown(
            '<div style="text-align:center;font-weight:600;font-size:0.95rem;margin:0.4rem 0 0.15rem 0;">'
            "⚙️ 风控模式</div>",
            unsafe_allow_html=True,
        )
        st.sidebar.caption("写入配置文件中的风控开关，下一轮扫描生效")

        def _persist_risk_ui_alert_only() -> None:
            v = bool(st.session_state.get("rc_ui_alert_only", True))
            if write_ui_alert_only_to_config_yaml(v):
                invalidate_config_cache()
                try:
                    st.session_state["cfg_yaml_mtime_ui_alert"] = os.path.getmtime(_cfg_yaml_path)
                except OSError:
                    st.session_state["cfg_yaml_mtime_ui_alert"] = time.time()
                try:
                    st.toast("风控配置已切换", icon="✅")
                except Exception:
                    try:
                        st.sidebar.success("风控配置已切换")
                    except Exception:
                        pass
                # toggle 的 on_change 本轮已触发重绘，无需再手动 rerun（可减少前端重复卸载节点风险）。
            else:
                try:
                    st.sidebar.error("写入 config.yaml 失败（请检查文件权限与磁盘）")
                except Exception:
                    pass

        _rc_toggle = getattr(st.sidebar, "toggle", None)
        _rc_label = "⚠️ 仅界面预警（不拦出票）"
        _rc_help = "开：红线只打预警标签、不拦出票；关：触线即硬否决。会写入配置文件。"
        if _rc_toggle is not None:
            _rc_toggle(
                _rc_label,
                key="rc_ui_alert_only",
                on_change=_persist_risk_ui_alert_only,
                help=_rc_help,
            )
        else:
            st.sidebar.checkbox(
                _rc_label,
                key="rc_ui_alert_only",
                on_change=_persist_risk_ui_alert_only,
                help=_rc_help,
            )
        st.sidebar.markdown(
            "<div style='font-size:12px;color:#64748b;margin:-6px 0 10px 0;'>"
            "开＝仅预警　关＝触线拦票</div>",
            unsafe_allow_html=True,
        )
    except Exception as _risk_ex:
        st.sidebar.caption(f"风控开关不可用: {_risk_ex}")

    st.sidebar.divider()


def _fmt_size_mb(byte_size):
    try:
        return f"{(float(byte_size) / (1024 * 1024)):.2f} MB"
    except Exception:
        return "--"


@st.cache_data(ttl=60)
def _cached_sidebar_daily_data_rowcounts() -> tuple[int, int]:
    """
    高频大表统计：仅缓存 (总行数, 代码数) 元组，绝不缓存数据库连接。
    返回 (-1, -1) 表示缺表或查询失败。
    """
    try:
        from data.db_core import get_read_conn

        with get_read_conn(read_only=True) as con:
            row = con.execute("SELECT COUNT(*) FROM daily_data").fetchone()
            codes = con.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_data").fetchone()
        tr = int(row[0]) if row and row[0] is not None else 0
        tc = int(codes[0]) if codes and codes[0] is not None else 0
        return (tr, tc)
    except Exception:
        return (-1, -1)


def _get_db_snapshot_cached(ttl_sec=300):
    """
    读取 DuckDB 健康快照（带 TTL 缓存），避免每次 UI 重绘都触发重查询。
    返回：
    {
      ok, db_size_bytes, total_rows, total_codes, quality_tag, quality_hint, error
    }
    """
    now_ts = time.time()
    cache = st.session_state.get("sidebar_db_snapshot_cache")
    if cache and (now_ts - cache.get("ts", 0) < ttl_sec):
        return cache.get("data", {})

    snapshot = {
        "ok": False,
        "db_size_bytes": 0,
        "total_rows": 0,
        "total_codes": 0,
        "quality_tag": "未知",
        "quality_hint": "未完成检测",
        "error": "",
    }
    try:
        from data.db_core import duckdb_disk_bytes_total, get_duckdb_path, table_exists

        db_file = get_duckdb_path()
        if os.path.exists(db_file):
            snapshot["db_size_bytes"] = duckdb_disk_bytes_total()

        tr, tc = _cached_sidebar_daily_data_rowcounts()
        if tr < 0 or tc < 0:
            if not table_exists("daily_data"):
                snapshot["quality_tag"] = "缺表"
                snapshot["quality_hint"] = "未找到 daily_data"
            else:
                snapshot["quality_tag"] = "异常"
                snapshot["quality_hint"] = "读取失败，请检查连接"
                snapshot["error"] = "daily_data 统计失败"
        else:
            snapshot["total_rows"] = tr
            snapshot["total_codes"] = tc
            snapshot["ok"] = True

            if snapshot["total_rows"] >= 200000 and snapshot["total_codes"] >= 1500:
                snapshot["quality_tag"] = "优"
                snapshot["quality_hint"] = "覆盖充足，可直接洗盘"
            elif snapshot["total_rows"] >= 80000 and snapshot["total_codes"] >= 800:
                snapshot["quality_tag"] = "中"
                snapshot["quality_hint"] = "可运行，建议盘后补全"
            else:
                snapshot["quality_tag"] = "低"
                snapshot["quality_hint"] = "样本偏少，建议先补数"
    except Exception as e:
        snapshot["quality_tag"] = "异常"
        snapshot["quality_hint"] = "读取失败，请检查连接"
        snapshot["error"] = str(e)

    st.session_state["sidebar_db_snapshot_cache"] = {"ts": now_ts, "data": snapshot}
    return snapshot


def render_data_foundation_expander():
    """数据底座手动工具区；定时链由 auto_sniper_daemon 执行（见 data/runtime/state/daemon_public_meta.json）。"""
    try:
        from data import data_fetcher
        from data.data_fetcher import DataFetchCriticalError
    except Exception as e:
        st.sidebar.error(f"❌ 数据模块加载失败: {e}")
        return

    with st.expander("💾 数据底座", expanded=True):
        _stp = _load_sniper_pipeline_state_for_ui()
        _sync_ok = str(_stp.get("last_sync_ok_bj_date") or "").strip()
        _p1_ok = str(_stp.get("last_p1_ok_bj_date") or "").strip()
        _sync_fail = str(_stp.get("last_sync_fail_bj_date") or "").strip()
        if _sync_ok or _p1_ok or _sync_fail:
            st.caption(
                f"链：同步 {_sync_ok or '—'} · P1 {_p1_ok or '—'}"
                + (f" · 同步失败 {_sync_fail}" if _sync_fail else "")
            )
        else:
            st.caption("⚠ 无守护落盘（请运行 auto_sniper_daemon）")

        db_snap = _get_db_snapshot_cached(ttl_sec=300)
        db_ok = bool(db_snap.get("ok"))
        size_text = _fmt_size_mb(db_snap.get("db_size_bytes", 0)) if db_ok else "--"
        rows_text = f"{db_snap.get('total_rows', 0):,}" if db_ok else "--"
        codes_text = f"{db_snap.get('total_codes', 0):,}" if db_ok else "--"
        tag_text = db_snap.get("quality_tag", "暂不可用") if db_ok else "暂不可用"
        hint_text = db_snap.get("quality_hint", "") if db_ok else ""
        st.markdown(
            f"""
<div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:8px 10px;line-height:1.45;font-size:12px;'>
  <div style='margin:2px 0;'>📁 {size_text} · 📊 {rows_text} · 🎯 {codes_text} · 🧪 {tag_text}{(' · ' + hint_text) if hint_text else ''}</div>
</div>
""",
            unsafe_allow_html=True,
        )
        if db_snap.get("error"):
            st.caption("数据库快照暂不可用，请检查 daemon 或维护状态")

        if _is_maintenance_mode_ui():
            st.error(
                "🚨 **维护模式已开启**：后台守护已暂停写库与同步 / P1–P5 链。"
                "请勿执行下载或压缩，直至运维控制台提示维护完成。"
            )
            if st.button(
                "🔓 强制解除维护锁（仅当维护脚本已退出仍误锁时）",
                key="sidebar_force_clear_maintenance",
                help="写入 maintenance_mode=false；误点可能导致与正在运行的 VACUUM 争锁，请确认维护窗口已结束。",
            ):
                try:
                    from core.master_control import write_master_control

                    write_master_control(maintenance_mode=False)
                    st.session_state.pop("force_maintenance_dispatched", None)
                    st.success("已解除维护锁，请刷新本页后重试。")
                except Exception as _e_clr:
                    st.error(f"解除失败: {_e_clr}")
            st.divider()
            return

        if st.session_state.get("force_maintenance_dispatched"):
            st.warning("⚠️ 维护程序已投递，请等待脚本结束后再操作压缩与同步。")

        def _render_panel_header(title: str, hint: str) -> None:
            st.markdown(
                f"<div style='font-weight:700;font-size:0.98rem;margin:0.35rem 0 0.15rem 0;'>{title}</div>",
                unsafe_allow_html=True,
            )
            st.caption(hint)

        _render_panel_header("🧪 1. 数据健康检查", "只读诊断，不改库；优先判断是否真的需要同步或维护。")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔍 缺失查询", width="stretch", key="sidebar_btn_missing_check"):
                status_box = st.empty()
                status_box.markdown(
                    "<div style='background-color:#f8f9fa;padding:12px;border-radius:6px;margin:10px 0;color:#6c757d;font-size:14px;'>🔄 正在比对交易日历与本地库...</div>",
                    unsafe_allow_html=True,
                )
                try:
                    success, missing_dates = data_fetcher.check_data_completeness(days=150)
                    if success:
                        st.session_state["missing_dates"] = missing_dates
                        st.session_state["query_done"] = True
                    else:
                        st.error("❌ 无法连接 Tushare，请检查网络或 Token。")
                        st.session_state["missing_dates"] = []
                        st.session_state["query_done"] = False
                except DataFetchCriticalError as e:
                    st.error(f"🚨 实盘熔断：无法比对交易日历（企微已告警）。{e}")
                except Exception as e:
                    st.error(f"检查异常: {e}")
                status_box.empty()
        with c2:
            if st.button("🧪 接口探针", width="stretch", key="sidebar_btn_api_probe"):
                with st.spinner("正在执行 Tushare 接口探针..."):
                    ok_probe, probe_log = _run_tushare_api_probe(data_fetcher)
                st.code((probe_log or "")[-7000:], language="log")
                if ok_probe:
                    st.success("✅ 接口探针完成：至少一条高阶接口返回了有效数据。")
                else:
                    st.warning("💡 探针未拿到高阶有效数据，请检查 Token 权限/额度/时间窗。")

        missing_dates = st.session_state.get("missing_dates", [])
        query_done = st.session_state.get("query_done", False)
        if query_done:
            if not missing_dates:
                st.markdown(
                    "<div style='background-color:#e6f4ea;padding:12px;border-radius:6px;margin:10px 0;color:#10b981;font-size:14px;font-weight:bold;'>✅ 150天数据完备，滴水不漏！</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div style='background-color:#fdfadd;border:1px solid #ffeeba;padding:12px;border-radius:6px;margin:10px 0;color:#856404;font-size:14px;'>⚠️ 发现缺失 <span style='font-weight:bold;'>{len(missing_dates)}</span> 天：</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<div style='background-color:#e2e8f0;padding:12px;border-radius:6px;margin:10px 0;color:#004085;font-size:14px;'>{', '.join(missing_dates[:5])}{'...' if len(missing_dates) > 5 else ''}</div>",
                    unsafe_allow_html=True,
                )

        _render_panel_header("📥 2. 数据同步", "空库走全量历史；已有库优先做增量补缺。全量历史会触发重铸。")
        sync_choice = st.radio(
            "同步模式",
            ["轻量补缺", "全量重建"],
            horizontal=True,
            index=0,
            key="sidebar_sync_choice",
            label_visibility="collapsed",
        )
        if sync_choice == "轻量补缺":
            st.caption("轻量补缺：优先只补缺失交易日；若本地已完整，则仅做特征修补，不重铸全库。")
        else:
            st.warning("全量重建：会重新拉取历史并重铸 daily_data，可能放大 DuckDB 体积；仅在空库或大版本修复时使用。")

        if st.button("📥 执行同步", width="stretch", key="sidebar_btn_sync_entry", type="primary"):
            if _is_maintenance_mode_ui():
                st.error("维护模式中：已暂停 UI 侧同步，请待维护结束后再试。")
            else:
                log_container = st.empty()
                log_history = []

                def update_log(msg):
                    log_history.append(msg)
                    log_container.code("\n".join(log_history[-15:]), language="log")

                try:
                    if sync_choice == "全量重建":
                        update_log("🚧 已选择全量重建：将执行 150 日历史下载并重铸 daily_data。")
                        data_fetcher.sync_history(
                            days=150,
                            status_callback=update_log,
                            progress_callback=None,
                        )
                        st.success("✅ 全量重建已完成。")
                    else:
                        lookback_days = int(getattr(constants, "MAX_DAYS", 150) or 150)
                        ok, missing = data_fetcher.check_data_completeness(days=lookback_days)
                        missing = [str(d) for d in (missing or []) if str(d)]
                        if not ok:
                            st.warning("⚠️ 交易日历校验未通过，已跳过同步。")
                        elif not missing:
                            update_log("✨ 本地数据已完整，仅执行特征修补。")
                            data_fetcher._sync_daily_features(update_log)
                            st.success("✅ 本地已完整，已执行轻量特征修补。")
                        else:
                            update_log(f"检测到缺失 {len(missing)} 个交易日，开始批量自动补缺。")
                            total_missing = len(missing)
                            failed_days = data_fetcher.sync_missing_days_batch(missing, status_callback=update_log)
                            if failed_days:
                                st.warning(
                                    f"⚠️ 自动补缺完成，但仍有 {len(failed_days)} 天失败："
                                    + ",".join(failed_days[:8])
                                    + ("..." if len(failed_days) > 8 else "")
                                )
                            else:
                                st.success(f"✅ 批量自动补缺完成：共补齐 {total_missing} 个交易日，单次重铸。")
                except DataFetchCriticalError as e:
                    st.error(f"🚨 实盘熔断：行情拉取不可用，本轮已终止（企微已告警）。{e}")
                except Exception as e:
                    if _is_duckdb_windows_file_lock_error(e):
                        st.error(
                            "❌ 主库 quant_data.duckdb 正被其它 Python 进程占用（通常是 daemon）。"
                            "请先停掉并发写库进程后再同步，或改用 daemon 定时晚间链自动同步。"
                        )
                    else:
                        st.error(f"同步失败: {e}")
                    try:
                        from core.notification_gateway import notify_wechat_system_alert

                        notify_wechat_system_alert(
                            title="UI 数据同步异常",
                            detail=str(e)[:900],
                            category="data_sync",
                            dedup_key="ui_sidebar_sync_entry_exc",
                        )
                    except Exception:
                        pass

        _render_panel_header("🧹 3. 数据维护", "维护类操作集中收口；建议先停 daemon，再执行压缩。")
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("🧹 仅压缩", width="stretch", key="sidebar_btn_compact"):
                if _is_maintenance_mode_ui():
                    st.error("维护模式中：请等待 force_maintenance_vacuum 结束后再压缩。")
                else:
                    ok_precheck, precheck_msg = _precheck_db_file_lock_for_compact()
                    if ok_precheck:
                        st.info(precheck_msg)
                    else:
                        st.error(precheck_msg)
                    if ok_precheck:
                        st.caption("建议先停 daemon 再压库。")
                        log_container = st.empty()
                        with st.spinner("正在执行数据库压缩（可能需要数分钟）..."):
                            ok, output_text = _run_compact_db_offline()
                        log_container.code(output_text[-6000:], language="log")
                        if ok:
                            st.success("✅ 数据库压缩完成。")
                            st.session_state.pop("sidebar_db_snapshot_cache", None)
                        else:
                            st.warning("💡 本次压缩未完成，可能仍有进程占用主库。")
        with col_b:
            if st.button("🛠 停Daemon→压缩→重启", width="stretch", key="sidebar_btn_compact_restart"):
                if _is_maintenance_mode_ui():
                    st.error("维护模式中：请等待运维脚本结束后再执行。")
                else:
                    flow_logs: list[str] = []
                    if os.name != "nt":
                        st.error("该一键流程目前仅支持 Windows。")
                    else:
                        try:
                            from data.db_core import close_db

                            close_db()
                            flow_logs.append("[STEP] 已释放当前 UI 进程 DuckDB 连接")
                        except Exception as e:
                            flow_logs.append(f"[WARN] 释放 UI 连接失败（继续执行）: {e}")

                        with st.spinner("正在执行：停 daemon -> 压缩 -> 重启 daemon ..."):
                            pids = _list_daemon_pids_windows()
                            flow_logs.append(f"[STEP] 检测到 daemon PID: {pids or '无'}")
                            ok_stop, stop_msg = _stop_daemon_pids(pids)
                            flow_logs.append(stop_msg)
                            if not ok_stop:
                                st.error("❌ 停止 daemon 失败，请手工关闭后再试。")
                                st.code("\n".join(flow_logs)[-7000:], language="log")
                            else:
                                ok_comp, comp_msg = _run_compact_db_offline()
                                flow_logs.append(comp_msg)
                                if not ok_comp:
                                    st.error("❌ 压缩失败，已跳过 daemon 重启。")
                                    st.code("\n".join(flow_logs)[-7000:], language="log")
                                else:
                                    ok_start, start_msg = _start_daemon_background()
                                    flow_logs.append(start_msg)
                                    st.code("\n".join(flow_logs)[-7000:], language="log")
                                    if ok_start:
                                        st.success("✅ 一键流程完成：daemon 已重启，数据库已压缩。")
                                        st.session_state.pop("sidebar_db_snapshot_cache", None)
                                    else:
                                        st.warning("⚠️ 压缩已完成，但 daemon 重启失败，请手工启动。")

        if st.session_state.get("force_maintenance_dispatched"):
            st.warning("⚠️ 维护程序已投递，请等待脚本结束后再操作。")
        st.warning("🚨 深度维护将切断扫描引擎，执行底层清理后自动恢复。")
        if st.button("🚨 一键强制数据库维护", type="primary", width="stretch", key="sidebar_btn_force_maintenance"):
            _root = _project_root_ui()
            _script = os.path.join(_root, "tools", "force_maintenance_vacuum.py")
            if not os.path.isfile(_script):
                st.error(f"未找到运维脚本: {_script}")
            else:
                _cmd = [sys.executable, _script]
                try:
                    from data.db_core import close_db

                    try:
                        close_db()
                    except Exception:
                        pass
                except Exception as e:
                    st.warning(f"未能释放本进程 DuckDB 连接（仍将投递脚本）: {e}")
                try:
                    if sys.platform == "win32":
                        subprocess.Popen(_cmd, cwd=_root, creationflags=subprocess.CREATE_NEW_CONSOLE)
                    else:
                        subprocess.Popen(
                            _cmd,
                            cwd=_root,
                            start_new_session=True,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                    st.session_state["force_maintenance_dispatched"] = True
                    st.success("清理任务已投递至独立进程，请留意弹出的控制台！")
                except Exception as e:
                    st.error(f"投递失败: {e}")

        if query_done:
            st.divider()
            st.caption("缺失查询结果")
            if not missing_dates:
                st.markdown(
                    "<div style='background-color:#e6f4ea;padding:12px;border-radius:6px;margin:10px 0;color:#10b981;font-size:14px;font-weight:bold;'>✅ 150天数据完备，滴水不漏！</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div style='background-color:#fdfadd;border:1px solid #ffeeba;padding:12px;border-radius:6px;margin:10px 0;color:#856404;font-size:14px;'>⚠️ 发现缺失 <span style='font-weight:bold;'>{len(missing_dates)}</span> 天：</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<div style='background-color:#e2e8f0;padding:12px;border-radius:6px;margin:10px 0;color:#004085;font-size:14px;'>{', '.join(missing_dates[:5])}{'...' if len(missing_dates) > 5 else ''}</div>",
                    unsafe_allow_html=True,
                )


def render_sidebar(p1_size="--", latest_trade_date="未同步", sector_ranks=None, regime_name="震荡市"):
    need_refresh_radar = False

    with st.sidebar:
        # 与上方「池子视图 + 数据底座」区块分隔（二者由 app.py 先行渲染）
        st.markdown("---")
        st.markdown("### 🛡️ 机构实盘中控舱")
        st.markdown(
            f"<b>📅 本地数据基准</b>: <code>{latest_trade_date}</code>",
            unsafe_allow_html=True,
        )
        db_snap = _get_db_snapshot_cached(ttl_sec=300)
        size_text = _fmt_size_mb(db_snap.get("db_size_bytes", 0))
        rows_text = f"{db_snap.get('total_rows', 0):,}"
        codes_text = f"{db_snap.get('total_codes', 0):,}"
        tag = db_snap.get("quality_tag", "未知")
        hint = db_snap.get("quality_hint", "")
        tag_color = "#10b981" if tag == "优" else ("#f59e0b" if tag in ["中", "锁冲突"] else "#ef4444")
        st.markdown(
            f"""
<div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:8px 10px;line-height:1.45;font-size:12px;'>
  <div style='margin:2px 0;'>📁 物理体积: <b>{size_text}</b> <span style='color:#64748b;'>(防暴涨锁死)</span></div>
  <div style='margin:2px 0;'>📊 载弹总数: <b>{rows_text}</b> 条 K 线记录</div>
  <div style='margin:2px 0;'>🎯 标的数量: <b>{codes_text}</b> 只 <span style='color:#64748b;'>(全市场样本覆盖)</span></div>
  <div style='margin:2px 0;'>🧪 健康评级: <span style='color:{tag_color};font-weight:700;'>{tag}</span> · <span style='color:#475569;'>{hint}</span></div>
</div>
""",
            unsafe_allow_html=True
        )
        if db_snap.get("error"):
            st.caption(f"数据库快照提示：{db_snap.get('error')}")
        
        st.markdown("---")
        
        st.markdown("### 🎯 P1 战略底仓模式（参与门槛：流通市值）")
        # 与 Daemon 09:35 early_morning_p5_validation 写入的 p5_yesterday_validated.json 对齐（当日口径）
        try:
            from core.p5_morning_validation import read_p5_validation_summary_for_ui

            _pv_c, _pv_r, _ = read_p5_validation_summary_for_ui()
            st.caption(f"P5 次日早盘已验证 {_pv_c} 只（剔除 {_pv_r} 只）")
        except Exception:
            st.caption("P5 次日早盘已验证 0 只（剔除 0 只）")
        st.markdown(
            """
<style>
section[data-testid="stSidebar"] div[data-baseweb="radio"] label {
  font-size: 0.86rem !important;
  line-height: 1.35 !important;
}
</style>
""",
            unsafe_allow_html=True,
        )
        pool_mode = st.radio(
            "模式选定",
            ["🔴 P1底仓池", "⭐ 直通车: 专属自选"],
            index=0 if st.session_state.get('pool_mode', 'P1') == 'P1' else 1,
            label_visibility="collapsed"
        )
        st.session_state["pool_mode"] = "P0" if "直通车" in pool_mode else "P1"

        st.markdown("---")
        if st.session_state['pool_mode'] == 'P0':
            uploaded_file = st.file_uploader("📥 导入自选股 (TXT格式)", type=['txt'])
            if uploaded_file is not None:
                os.makedirs('data', exist_ok=True)
                file_path = getattr(constants, 'P0_FILE_PATH', 'data/p0_custom.txt')
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                st.toast("✅ 自选股弹药已上膛入库 (data/p0_custom.txt)！")
                
        st.markdown("---")
        st.markdown("### 📡 实时板块雷达")
        if st.button("🔄 刷新实时板块", width="stretch"):
            need_refresh_radar = True
            
        # ================= 🚀 涨跌双向宽域雷达渲染 =================
        _sr_fs = "14.5px"
        _sr_lh = "1.45"
        if sector_ranks:
            rank_list = list(sector_ranks.items())
            total_sectors = len(rank_list)

            st.markdown(
                f"<div style='font-size:{_sr_fs};line-height:{_sr_lh};'>🔥 <b>领涨板块 (进攻区 Top 8)</b></div>",
                unsafe_allow_html=True,
            )
            # 展示前 8 名
            for i, (sec, pct) in enumerate(rank_list[:8]):
                if i < 3:
                    st.markdown(
                        f"<div style='font-size:{_sr_fs};line-height:{_sr_lh};margin:2px 0;'>"
                        f"<b>Top {i+1}</b>: 👑 <b>{sec}</b> <span style='color:#ef4444;font-weight:bold;'>{pct:.2f}%</span></div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f"<div style='font-size:{_sr_fs};line-height:{_sr_lh};margin:2px 0;'>"
                        f"<b>Top {i+1}</b>: {sec} <span style='color:#ef4444;'>{pct:.2f}%</span></div>",
                        unsafe_allow_html=True,
                    )

            # 🚀 修改点：将领跌板块扩容至 Bottom 8
            if total_sectors > 16:
                st.markdown(
                    f"<div style='font-size:{_sr_fs};line-height:{_sr_lh};margin-top:8px;'>"
                    f"🧊 <b>领跌板块 (绞肉机 Bottom 8)</b></div>",
                    unsafe_allow_html=True,
                )
                # 提取倒数 8 名，并反转顺序让跌得最惨的排在“倒数第1”
                bottom_list = rank_list[-8:]
                bottom_list.reverse()
                for i, (sec, pct) in enumerate(bottom_list):
                    if i < 3:
                        st.markdown(
                            f"<div style='font-size:{_sr_fs};line-height:{_sr_lh};margin:2px 0;'>"
                            f"<b>倒数 {i+1}</b>: 💀 <b>{sec}</b> <span style='color:#10b981;font-weight:bold;'>{pct:.2f}%</span></div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f"<div style='font-size:{_sr_fs};line-height:{_sr_lh};margin:2px 0;'>"
                            f"<b>倒数 {i+1}</b>: 🩸 {sec} <span style='color:#10b981;'>{pct:.2f}%</span></div>",
                            unsafe_allow_html=True,
                        )
        else:
            st.info("大盘数据不足或尚未收盘。")

        st.markdown("---")
        st.markdown("### ⚠️ 机构实盘铁纪律")
        st.markdown("""
        <div style='background-color: #fce8e8; padding: 18px; border-radius: 8px; color: #a94442; line-height: 1.8; font-size: 14px;'>
            <span style='color:#d9534f; font-weight:bold;'>1. 仓位底线：</span>单票≤8%，总仓≤40%。<br>
            <span style='color:#d9534f; font-weight:bold;'>2. 止损铁律：</span>浮亏5%无条件斩仓。<br>
            <span style='color:#d9534f; font-weight:bold;'>3. 情绪过滤：</span>大盘处于【冰点/退潮】时，P3 全忽略。<br>
            <span style='color:#d9534f; font-weight:bold;'>4. 确认机制：</span>看到 P3 底牌，必须等量比 > 2.0 且站上 MA5。<br>
            <span style='color:#d9534f; font-weight:bold;'>5. 敬畏市场：</span>谋定后动，严禁盘中冲动追高！
        </div>
        """, unsafe_allow_html=True)

        # 1档阈值固定在侧边栏最底部，便于与上方雷达/纪律区分
        st.markdown("---")
        st.markdown("### 📏 P1底仓池阈值（当前生效）")
        try:
            from core.pool_manager import get_p1_threshold_summary
            summ = get_p1_threshold_summary(regime_name)
            t = summ.get("thresholds") or {}
            st.markdown("""
<style>
/* 仅压缩侧边栏阈值表格显示 */
section[data-testid="stSidebar"] table {
  font-size: 12px !important;
}
section[data-testid="stSidebar"] th,
section[data-testid="stSidebar"] td {
  padding: 0.28rem 0.45rem !important;
  line-height: 1.25 !important;
}
</style>
""", unsafe_allow_html=True)
            st.caption(
                f"大盘环境：**{summ.get('regime_input', '')}** → **{summ.get('profile_label', '')}**"
            )
            st.markdown(
                f"""
| 项 | 数值 |
| --- | --- |
| MA60/MA120 粘合 | ≥ **{t.get('trend_ma120_min_ratio', 0):.3f}** |
| MA20 斜率快通道 | ≥ **{t.get('trend_slope_fastpass', 0):.2f}** |
| 贴近 MA20 | 收盘 ≥ MA20×**{t.get('near_ma20_min_ratio', 0):.3f}** |
| MACD 淘汰线 | ≤ **{t.get('macd_bar_kill', 0):.2f}** |
| 量价背离 | 阳量 弱于 阴量×**{t.get('vol_divergence_ratio', 0):.2f}** 视为背离 |
| 第五层及格线 | **{t.get('pass_line', 0):.0f}** 分 |
"""
            )
            st.caption(
                "多维分项（含筹码、趋势距离、均线成熟、资金攻击、启动势能、波段涨幅、黄金起爆、趋势健康、"
                "主升斜率、动态PE、高位熔断及假突破惩罚、行业/板块/市值附加等）在主页「阵亡基因追溯」或入选提示的展开表中查看。"
            )
            st.caption("说明：仅 1档 全市场洗盘使用；直通车模式不走该套前置打分。及格线可在 config.yaml → strategies.p1.profiles 修改。")
        except Exception as e:
            st.warning(f"阈值摘要加载失败：{e}")

    return need_refresh_radar