# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 — 家用服务器 24h 无人值守守护进程（调度 + 交易日屏障 + 企微推送）
【V26.6 新增资金记忆体系 · 第三阶段】：日线 fund_memory_score 由 data_fetcher 夜间/早盘增量管道 `_sync_daily_features()` 维护；本进程仅调度同步/P1，不直接计算该列。

职责概览：
- 仅交易日执行：数据同步、P1 重建、分时六槽快照、2档·竞价池、3档·盘中池、4档·盘尾池扫描与企微 Markdown 推送（与 Streamlit 解耦）。
- 分时快照与 **3档·盘中池/4档·盘尾池异步扫描队列**（`process_one_pending_scan_job`）均在本进程内调度；勿再并行启动其它日线/快照/队列守护。
- 【时序铁律】**19:45** 仅日线增量（已最新则跳过）；**19:55** P1 全量重建 + ≥65 推送；**20:05** P5 盘后扫描（独立定时，依赖当日同步成功且 P1 已落盘）。
- 【资源档】`config.yaml` → `server.daemon_profile`：`standard`（默认）| `low`（老至强/小内存：3档·盘中池降频、异步扫描队列轮询拉长，见 `_apply_daemon_resource_profile`）。
- 【维护避让】`master_control.json` → `maintenance_mode: true` 时：主循环休眠轮询；增量同步/P1/扫描/快照/异步队列/stock_basic/每周 VACUUM 等核心写路径由 `_maintenance_mode_skip` 主动跳过（见 `core.master_control.is_maintenance_mode_enabled`）。
- 【早盘编排】交易日 08:50 采用单一编排入口：先清空企微防刷字典，再执行早盘补位链（同步 / 条件性补 P1）。若当日 p1_cache_YYYYMMDD.json 尚不存在，则同步成功后立即补建 P1，避免周一早盘仍只挂载周五 JSON 却无「当日文件名」的认知与数据断层（K 线仍以 DB 最新交易日为准）。
- 【早盘合并简报】交易日 **09:18** 仅由 `daily_open_heartbeat_routine()` 统一发送一条 Markdown：原早安问候（推送/巡航开关）+ 数据库探活 + ``risk_control.ui_alert_only`` 风控说明 + 巡航待命；启动补发与定时任务共用同一入口，并通过进程内锁 + pipeline_state 日标记双重去重，保证同日仅一条。预检失败则跳过企微简报并尝试一条运维告警（按日去重）。
- 【时区锚定】模块首部设置 TZ=Asia/Shanghai 并在 Unix 上 tzset，与 ZoneInfo(BJ) 双保险。

【调度哲学：尾盘重仓、盘中降频 · V26.6 第二阶段】
- P4 尾盘五拍（14:31 / 14:36 / 14:41 / 14:46 / 14:51）一律 **wait_for_lock_sec=900**（15 分钟）：工作线程内等锁，
  主循环 `schedule.run_pending` **永不阻塞**。其中 **14:40–14:55** 为「重仓核心窗」——与盘中降频的 P3 绝对互斥。
- P3 **大幅降频**（默认每 5 分钟整分对齐一拍，可由 ``server.daemon_profile`` / ``daemon_p3_poll_interval_minutes`` 调整）；仅非阻塞抢锁。
  自 **14:31** 起（含）进入「P4 优先走廊」：**禁止**再派生 P3 线程（O(1) return），杜绝与 P4 争 _SCAN_BUSY。
- 19:45 仅日线增量（可跳过）→ 19:55 P1+推送 → 20:05 P5

【分时快照】仅调度六槽（935/1030/1125/1325/1425/1440）量能与 VR 锚点写入 JSON，供 P3/P4 等链路读取。

【不死性说明·锁与 finally】
- 子线程内对 _SCAN_BUSY 采用「acquired 标志 + finally 内安全 release」。
- P4 尾盘使用 **最长 900s** 等锁；P5/晚间/早盘仍用较长超时；P3 **仅非阻塞抢锁**，且尾盘重叠窗内不调度。

依赖：pip install schedule requests
运行前请将工作目录设为项目根目录，或从项目根执行：python auto_sniper_daemon.py
"""
from __future__ import annotations

# ---------- 时区强锚定：须在依赖本地时间语义的逻辑之前（Linux/Unix 上 tzset 刷新 libc 时区表）----------
import os

os.environ["TZ"] = "Asia/Shanghai"
os.environ["XIAOJIE_DAEMON_MODE"] = "1"
import time

if hasattr(time, "tzset"):
    try:
        time.tzset()  # type: ignore[attr-defined]
    except Exception:
        pass

import gc
import html
import io
import json
import logging
import re
import subprocess
import sys
import tempfile
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# ---------- 项目根路径 ----------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)


def _force_bj_system_timezone() -> None:
    """尽量把进程级系统时区锚定到北京时间，避免日志/本地时间显示混乱。"""
    os.environ["TZ"] = "Asia/Shanghai"
    if os.name == "nt":
        # Windows 下标准库通常不提供稳定的 set tz 语义；至少统一环境变量与进程内显示口径。
        os.environ["TZ"] = "Asia/Shanghai"
    if hasattr(time, "tzset"):
        try:
            time.tzset()  # type: ignore[attr-defined]
        except Exception:
            pass


_force_bj_system_timezone()

import pandas as pd  # noqa: E402

# 分时快照统一入口（实现位于 scan_engine，此处 re-export 避免循环 import）
from core.intraday_snapshot_scheduler import capture_intraday_snapshots  # noqa: E402

try:
    import schedule
except ImportError as e:
    print("请先安装: pip install schedule", file=sys.stderr)
    raise e

from core.log_config import setup_sniper_rotating_file_logging  # noqa: E402

# 开启日志轮转（sniper.log 50MB×10），防止 7x24 纯后台模式下日志无限膨胀；详见 core/log_config.py
setup_sniper_rotating_file_logging(level=logging.INFO)
logger = logging.getLogger("auto_sniper_daemon")


class _SuppressStreamlitContextWarnings(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(getattr(record, "msg", "") or "")
        if "missing ScriptRunContext" in msg:
            return False
        if "Session state does not function when running a script without `streamlit run`" in msg:
            return False
        return True


for _name in (
    "streamlit.runtime.scriptrunner_utils.script_run_context",
    "streamlit.runtime.state.session_state_proxy",
):
    logging.getLogger(_name).addFilter(_SuppressStreamlitContextWarnings())

BJ_TZ = ZoneInfo("Asia/Shanghai")
_BJ_SCHEDULE_LOCK = threading.Lock()
_BJ_DAILY_JOBS: List[Tuple[str, Callable[[], None], str]] = []
_BJ_LAST_TICK_MINUTE_KEY = ""
# 早盘合并简报去重锁：防止 09:18 定时任务与启动补发并发重入导致重复发送。
_OPEN_HEARTBEAT_LOCK = threading.Lock()
_DUCKDB_LOCK_ALERT_TS: Dict[str, float] = {}
# 进程级单实例锁：防止 Windows 任务计划 / 手工启动 / 看门狗重复拉起多个 daemon 实例。
_DAEMON_INSTANCE_LOCK_PATH = os.path.join(_PROJECT_ROOT, "data", "runtime", "state", "auto_sniper_daemon.lock")
_DAEMON_INSTANCE_LOCK_FD: Optional[int] = None
# 维护锁开启时主循环每 10s 休眠一轮；日志降为「进入时 + 至多每 5 分钟一条」，避免 7x24 误开维护把磁盘刷满
_MAINT_LOOP_LOG_LAST_MONO: Optional[float] = None
# 运行时 JSON / 缓存清理节拍
_RUNTIME_CLEANUP_LAST_HOUR_KEY = ""


def _daemon_cruise_off_skip(task_label: str) -> bool:
    """
    物理总控「全自动巡航」关闭时返回 True，调用方应立即 return，不得下载/扫描/重建。
    读取失败时视为开启（与 master_control 缺省 True 一致），避免误伤生产。
    """
    try:
        from core.master_control import is_daemon_auto_cruise_enabled

        if not is_daemon_auto_cruise_enabled():
            logger.info(
                "[Daemon 休眠中] 总控开关已关闭 (master_control.json)，跳过: %s",
                task_label,
            )
            return True
    except Exception as e:
        logger.debug("master_control 读取失败，Daemon 继续运行: %s", e)
    return False


def _maintenance_mode_skip(task_label: str) -> bool:
    """
    维护模式「主动避让」刹车：总控 maintenance_mode 为 True 时，跳过本任务对应的核心写/扫路径。
    返回 True 表示应跳过；False 表示可继续（再经巡航/交易日等闸门）。
    """
    try:
        from core.master_control import is_maintenance_mode_enabled

        if is_maintenance_mode_enabled():
            logger.warning(
                f"⚠️ 发现全局维护锁，守护进程主动让路暂停 | task={task_label}"
            )
            return True
    except Exception as e:
        logger.debug("维护模式检测失败（视为非维护）: %s", e)
    return False


def _notify_daemon_alert(
    title: str,
    detail: str,
    *,
    category: str = "daemon",
    dedup_key: Optional[str] = None,
) -> None:
    """守护进程内数据/扫描异常 → 企微运维提示（与 UI 共用网关）。"""
    try:
        from core.config_manager import get_daemon_alert_silence_config

        cfg = get_daemon_alert_silence_config()
        title_s = str(title or "")
        detail_s = str(detail or "")
        if bool(cfg.get("enabled", True)):
            whitelist = cfg.get("whitelist_keywords") or []
            if any(k and (k in title_s or k in detail_s) for k in whitelist):
                logger.info("已按白名单放行守护告警 | category=%s title=%s", category, title_s)
            else:
                if category in set(cfg.get("blacklist_categories") or []):
                    logger.info("已按类别静默守护告警 | category=%s title=%s detail=%s", category, title_s, detail_s)
                    return
                if any(k and (k in title_s or k in detail_s) for k in (cfg.get("silence_keywords") or [])):
                    logger.info("已按关键字静默守护告警 | category=%s title=%s detail=%s", category, title_s, detail_s)
                    return
        from core.notification_gateway import notify_wechat_system_alert

        notify_wechat_system_alert(title=title, detail=detail, category=category, dedup_key=dedup_key)
    except Exception as e:
        logger.debug("企微守护进程告警发送失败（已忽略）: %s", e)


def _acquire_single_instance_lock() -> bool:
    """获取进程级单实例锁，失败则说明已有 daemon 在运行。"""
    global _DAEMON_INSTANCE_LOCK_FD
    try:
        os.makedirs(os.path.dirname(_DAEMON_INSTANCE_LOCK_PATH), exist_ok=True)
        fd = os.open(_DAEMON_INSTANCE_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o666)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            os.close(fd)
            return False
        _DAEMON_INSTANCE_LOCK_FD = fd
        try:
            os.ftruncate(fd, 0)
            os.write(fd, str(os.getpid()).encode("utf-8"))
            os.fsync(fd)
        except Exception:
            pass
        return True
    except Exception as e:
        logger.warning("单实例锁初始化失败，仍继续启动以免误伤: %s", e)
        return True


def _release_single_instance_lock() -> None:
    global _DAEMON_INSTANCE_LOCK_FD
    fd = _DAEMON_INSTANCE_LOCK_FD
    _DAEMON_INSTANCE_LOCK_FD = None
    if fd is None:
        return
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(fd)
    except Exception:
        pass


def _cleanup_runtime_caches() -> None:
    """定期清理运行时 JSON / 账本 / 临时快照，避免 24/7 长跑堆积。"""
    global _RUNTIME_CLEANUP_LAST_HOUR_KEY
    now = _now_bj()
    hour_key = now.strftime("%Y%m%d%H")
    if hour_key == _RUNTIME_CLEANUP_LAST_HOUR_KEY:
        return
    _RUNTIME_CLEANUP_LAST_HOUR_KEY = hour_key
    try:
        from core.runtime_data_paths import (
            path_alert_dedup_cache_json,
            path_intraday_snapshots_json,
            path_scan_async_pending_json,
            path_scan_async_running_json,
            path_scan_async_status_json,
            path_scan_async_latest_result_json,
            path_wechat_signal_dedup_cache_json,
            path_wash_metrics_json,
        )
    except Exception:
        return
    try:
        import glob

        from core.runtime_data_paths import POOL_CACHE_DIR

        keep_days = 14
        cutoff = int((now - pd.Timedelta(days=keep_days)).strftime("%Y%m%d"))
        for fp in glob.glob(os.path.join(POOL_CACHE_DIR, "p[01]_cache_*.json")):
            base = os.path.basename(fp)
            m = re.search(r"_(\d{8})\.json$", base)
            if not m:
                continue
            try:
                if int(m.group(1)) < cutoff:
                    os.remove(fp)
            except Exception:
                pass
        for fp in glob.glob(os.path.join(POOL_CACHE_DIR, "p[01]_cache_*.pkl")):
            base = os.path.basename(fp)
            m = re.search(r"_(\d{8})\.pkl$", base)
            if not m:
                continue
            try:
                if int(m.group(1)) < cutoff:
                    os.remove(fp)
            except Exception:
                pass

        for p in (path_alert_dedup_cache_json(), path_wechat_signal_dedup_cache_json()):
            if os.path.isfile(p) and os.path.getsize(p) > 5 * 1024 * 1024:
                bak = f"{p}.bak_{now.strftime('%Y%m%d%H%M')}"
                try:
                    os.replace(p, bak)
                except Exception:
                    pass

        for p in (path_scan_async_pending_json(), path_scan_async_running_json(), path_scan_async_status_json(), path_scan_async_latest_result_json()):
            if os.path.isfile(p) and os.path.getsize(p) > 2 * 1024 * 1024:
                try:
                    os.remove(p)
                except Exception:
                    pass

        wm = path_wash_metrics_json()
        if os.path.isfile(wm) and os.path.getsize(wm) > 10 * 1024 * 1024:
            bak = f"{wm}.bak_{now.strftime('%Y%m%d%H%M')}"
            try:
                os.replace(wm, bak)
            except Exception:
                pass
    except Exception as e:
        logger.debug("运行时缓存清理失败(忽略): %s", e)


# 全任务互斥（扫描 / 晚间链 / 早盘链串行，避免 DuckDB 与底仓逻辑并发踩踏）
_SCAN_BUSY = threading.Lock()
_CAL_MEM_LOCK = threading.Lock()

# ---------- 尾盘优先走廊：14:35 起 P3 彻底让路；P4 在 14:40~14:50 为业务意义上的重仓核心窗 ----------
# 单位：自当日 00:00 起的分钟数（北京时间）。该半开区间内 _job_p3_tick_clock_aligned 直接 return，不创建线程。
P4_TAIL_PRIORITY_START_MIN = 14 * 60 + 31  # 14:31（P4 从此时开始接管股票池扫描）
P4_TAIL_PRIORITY_END_MIN = 14 * 60 + 55  # 14:55（含末枪 P4）
# 文档锚点：14:40~14:50 三枪为「核心重仓」调度，仍统一使用下方 900s 等锁，不在代码里拆更短超时。
P4_CORE_TAIL_START_MIN = 14 * 60 + 40  # 14:40（仅用于日志/注释对齐产品话术）
# P4 每一枪均最多等锁 15 分钟：快照或长尾任务占锁时，尾盘线程仍能挤入执行，满足「必定执行」容错。
P4_TAIL_WAIT_LOCK_SEC = 900.0

# P3 / P4 轮询：按秒级节拍对齐；默认 150 秒（2分30秒）一拍。
P3_POLL_INTERVAL_SECONDS = 150
P4_POLL_INTERVAL_SECONDS = 150

# 主循环 sleep(1s)；累计满 N 次 tick 后轮询异步扫描队列（默认 10s）
_ASYNC_QUEUE_POLL_TICKS = 10

# 异步扫描队列（原独立 scheduler 进程）：本进程启动时独占 filelock，主循环按 _ASYNC_QUEUE_POLL_TICKS 轮询
_async_scan_queue_lock_ok = False


def _apply_daemon_resource_profile() -> None:
    """
    从 config.yaml server.* 读取资源档，减轻老 CPU / 小内存主机上盘中扫描与轮询压力。
    - daemon_profile: standard | low（low：P3 约 5 分钟一拍、异步队列约 15s 轮询；如无显式覆盖，P3 仍保持 5 分钟）
    - 可选覆盖：daemon_p3_poll_interval_minutes、daemon_async_queue_poll_seconds
    """
    global P3_POLL_INTERVAL_SECONDS, P4_POLL_INTERVAL_SECONDS, _ASYNC_QUEUE_POLL_TICKS
    std = {"p3_sec": 150, "p4_sec": 150, "async_sec": 10}
    low = {"p3_sec": 150, "p4_sec": 150, "async_sec": 15}
    prof = "standard"
    p3_override: Optional[int] = None
    p4_override: Optional[int] = None
    async_override: Optional[int] = None
    try:
        import yaml

        cfg_path = os.path.join(_PROJECT_ROOT, "config.yaml")
        if os.path.isfile(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            if isinstance(cfg, dict):
                srv = cfg.get("server") or {}
                if isinstance(srv, dict):
                    raw = str(srv.get("daemon_profile") or srv.get("daemon_resource_profile") or "").strip().lower()
                    if raw:
                        prof = raw
                    if srv.get("daemon_p3_poll_interval_seconds") is not None:
                        p3_override = int(srv["daemon_p3_poll_interval_seconds"])
                    elif srv.get("daemon_p3_poll_interval_minutes") is not None:
                        p3_override = int(srv["daemon_p3_poll_interval_minutes"]) * 60
                    if srv.get("daemon_p4_poll_interval_seconds") is not None:
                        p4_override = int(srv["daemon_p4_poll_interval_seconds"])
                    elif srv.get("daemon_p4_poll_interval_minutes") is not None:
                        p4_override = int(srv["daemon_p4_poll_interval_minutes"]) * 60
                    if srv.get("daemon_async_queue_poll_seconds") is not None:
                        async_override = int(srv["daemon_async_queue_poll_seconds"])
    except Exception as e:
        logger.debug("读取 server.daemon_profile 失败，使用内置默认: %s", e)

    # 保险：避免任何分支下 p4_override 未绑定
    if "p4_override" not in locals():
        p4_override = None
    base = low if prof in ("low", "weak", "minimal", "e5", "light") else std
    p3 = p3_override if p3_override is not None else base["p3_sec"]
    p4 = p4_override if p4_override is not None else base["p4_sec"]
    asec = async_override if async_override is not None else base["async_sec"]
    try:
        p3 = max(60, min(600, int(p3)))
    except (TypeError, ValueError):
        p3 = std["p3_sec"]
    try:
        p4 = max(60, min(600, int(p4)))
    except (TypeError, ValueError):
        p4 = std["p4_sec"]
    try:
        asec = max(5, min(120, int(asec)))
    except (TypeError, ValueError):
        asec = std["async_sec"]
    P3_POLL_INTERVAL_SECONDS = p3
    P4_POLL_INTERVAL_SECONDS = p4
    _ASYNC_QUEUE_POLL_TICKS = max(1, asec)
    logger.info(
        "守护资源档 profile=%s | P3 每 %ss 一拍 | P4 每 %ss 一拍 | 异步队列每 %ss 轮询（可改 config.yaml server）",
        prof or "standard",
        P3_POLL_INTERVAL_SECONDS,
        P4_POLL_INTERVAL_SECONDS,
        _ASYNC_QUEUE_POLL_TICKS,
    )


# ---------- 晚间链状态落盘（可观测 + 重启后可读）----------
_PIPELINE_STATE_REL = os.path.join("data", "runtime", "state", "sniper_pipeline_state.json")
_DAEMON_PUBLIC_META_REL = os.path.join("data", "runtime", "state", "daemon_public_meta.json")


def _write_daemon_public_meta() -> None:
    """
    启动时落盘简明运维说明（晚间链时间表、关键路径），避免在网页堆长文。
    见 data/runtime/state/daemon_public_meta.json
    """
    try:
        from core.runtime_data_paths import ensure_runtime_data_layout

        ensure_runtime_data_layout()
        p = os.path.join(_PROJECT_ROOT, _DAEMON_PUBLIC_META_REL.replace("/", os.sep))
        payload = {
            "timezone": "Asia/Shanghai",
            "evening_chain": "19:45 incremental | 19:55 P1+push | 20:05 P5",
            "morning": "08:50 clear push cache + catchup; 09:18 merged wechat brief (preflight); 09:26 P2; 09:35 P5 validation snapshot",
            "files": {
                "pipeline_state": "data/runtime/state/sniper_pipeline_state.json",
                "daemon_log": "data/runtime/sniper.log",
                "daemon_log_note": "RotatingFileHandler 50MB x10 UTF-8; see core/log_config.py",
            },
            "note": "Scheduled jobs run only in this process; Streamlit does not run the scheduler.",
            "started_at_bj": _now_bj().isoformat(),
        }
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info("运维说明已写入 %s", _DAEMON_PUBLIC_META_REL)
    except Exception as e:
        logger.debug("daemon_public_meta 写入失败: %s", e)


def _pipeline_state_path() -> str:
    from core.runtime_data_paths import ensure_runtime_data_layout

    ensure_runtime_data_layout()
    return os.path.join(_PROJECT_ROOT, _PIPELINE_STATE_REL.replace("/", os.sep))


def _read_pipeline_state() -> Dict[str, Any]:
    p = _pipeline_state_path()
    if not os.path.isfile(p):
        logger.debug("读取 pipeline 状态：文件不存在 | path=%s", p)
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            o = json.load(f)
        if isinstance(o, dict):
            return o
        logger.warning("读取 pipeline 状态：内容不是 dict，已忽略 | path=%s type=%s", p, type(o).__name__)
        return {}
    except json.JSONDecodeError as e:
        logger.warning("读取 pipeline 状态：JSON 解析失败 | path=%s err=%s", p, e)
        return {}
    except Exception as e:
        logger.warning("读取 pipeline 状态失败 | path=%s err=%s", p, e)
        return {}


def _write_pipeline_state_patch(**kwargs: Any) -> bool:
    """合并写入 last_sync_ok_bj_date / last_p1_ok_bj_date 等，成功返回 True。"""
    p = _pipeline_state_path()
    try:
        cur = _read_pipeline_state()
        cur.update({k: v for k, v in kwargs.items() if v is not None})
        cur["updated_at"] = _now_bj().isoformat()
        dir_name = os.path.dirname(p) or "."
        os.makedirs(dir_name, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix="pipeline_state_", suffix=".json", dir=dir_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(cur, f, ensure_ascii=False, indent=0)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, p)
            return True
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass
    except Exception as e:
        logger.warning("写入 pipeline 状态失败: %s", e)
        return False


# ---------- 交易日历：内存 TTL + 本地 JSON 雪崩兜底 ----------
_CAL_TTL_SEC = 120.0
_cal_mem: Dict[str, Any] = {
    "date": "",
    "is_open": False,
    "mono": 0.0,
    "source": "",
}


def _now_bj() -> datetime:
    return datetime.now(BJ_TZ)


def _trade_cal_cache_path() -> str:
    from core.runtime_data_paths import STATE_DIR, ensure_runtime_data_layout

    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "sniper_trade_cal_cache.json")


def _load_cal_file_for_today(today_yyyymmdd: str) -> Optional[bool]:
    p = _trade_cal_cache_path()
    if not os.path.isfile(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return None
        if str(raw.get("date") or "") != today_yyyymmdd:
            return None
        return bool(raw.get("is_open"))
    except Exception as e:
        logger.debug("读取交易日历本地缓存失败: %s", e)
        return None


def _save_cal_file(today_yyyymmdd: str, is_open: bool) -> None:
    try:
        p = _trade_cal_cache_path()
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        payload = {
            "date": today_yyyymmdd,
            "is_open": is_open,
            "updated_at": _now_bj().isoformat(),
        }
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=0)
    except Exception as e:
        logger.debug("写入交易日历本地缓存失败: %s", e)


def is_today_trading_day_sse() -> bool:
    global _cal_mem
    today = _now_bj().strftime("%Y%m%d")
    now_m = time.monotonic()

    with _CAL_MEM_LOCK:
        if (
            _cal_mem.get("date") == today
            and (now_m - float(_cal_mem.get("mono") or 0.0)) < _CAL_TTL_SEC
        ):
            return bool(_cal_mem.get("is_open"))

    is_open: bool = False
    source = "api"
    try:
        from data import data_fetcher
        from data.data_fetcher import DataFetchCriticalError

        if getattr(data_fetcher, "pro", None) is None:
            logger.warning("Tushare 未初始化，尝试本地日历缓存")
            cached = _load_cal_file_for_today(today)
            if cached is not None:
                is_open = bool(cached)
                source = "file"
                logger.warning(
                    "日历 pro 未初始化，已使用本地缓存 sniper_trade_cal_cache.json 当日结果 is_open=%s",
                    is_open,
                )
            else:
                # 无缓存时不能猜「非交易日」——与行情拉取同一套熔断语义
                data_fetcher.raise_data_fetch_critical(
                    "is_today_trading_day_sse：pro 未初始化且无当日本地缓存，无法判定交易日"
                )
        else:
            cal = data_fetcher.retry_api(data_fetcher.pro.trade_cal)(
                exchange="SSE",
                is_open="1",
                start_date=today,
                end_date=today,
            )
            is_open = cal is not None and not cal.empty
            source = "api"
    except DataFetchCriticalError:
        raise
    except Exception as e:
        logger.warning("trade_cal 查询异常，尝试本地回退: %s", e)
        cached = _load_cal_file_for_today(today)
        if cached is not None:
            is_open = cached
            source = "file"
            logger.warning(
                "日历 API 不可用，已使用本地缓存 sniper_trade_cal_cache.json 当日结果 is_open=%s",
                is_open,
            )
        else:
            is_open = False
            source = "fail"
            logger.warning("日历 API 失败且无当日本地缓存 → 按非交易日处理（跳过任务）")

    with _CAL_MEM_LOCK:
        _cal_mem = {
            "date": today,
            "is_open": is_open,
            "mono": time.monotonic(),
            "source": source,
        }

    if source == "api":
        _save_cal_file(today, is_open)

    return is_open


def _daemon_is_trading_day_safe() -> bool:
    """
    交易日 True / 非交易日 False。
    DataFetchCriticalError（企微已告警）时记 ERROR 并返回 False，避免守护线程因日历熔断而整体退出。
    """
    try:
        from data.data_fetcher import DataFetchCriticalError

        return is_today_trading_day_sse()
    except DataFetchCriticalError as e:
        logger.error("🚨 交易日历不可用（行情底座熔断），本轮按非交易日跳过: %s", e)
        return False


def _barrier_trading_day_or_skip(task_name: str) -> bool:
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过: %s", task_name)
        return False
    return True


def _resolve_regime_name() -> str:
    try:
        from core.regime_analyzer import get_market_regime

        raw = str((get_market_regime() or {}).get("primary", {}).get("status", "") or "")
        if "主升" in raw:
            return "主升浪"
        if "退潮" in raw or "防守" in raw:
            return "情绪退潮市"
        return "震荡市"
    except Exception as e:
        logger.debug("regime 回退震荡市: %s", e)
        return "震荡市"


# ---------- JSON 缓存读写 ----------

def _is_json_serializable_type(v) -> bool:
    """
    【性能优化 V2】替代 json.dumps() 做类型检查。
    原逻辑：对每个标量值调用 json.dumps(v) 来判断是否可序列化（慢，每次都构造 JSON 编码器）。
    新逻辑：用 isinstance 快速判断基本可序列化类型，跳过对 np.float64/datetime 等特殊类型的繁重序列化尝试。
    """
    if v is None:
        return True
    if isinstance(v, (bool, int, float, str)):
        return True
    if isinstance(v, (list, tuple)):
        return all(_is_json_serializable_type(x) for x in v)
    if isinstance(v, dict):
        return all(_is_json_serializable_type(k) and _is_json_serializable_type(val) for k, val in v.items())
    # numpy/pandas 类型
    if isinstance(v, (int, float)) and hasattr(v, 'item'):
        return True
    if hasattr(v, 'dtype'):  # numpy scalar or array
        return True
    if isinstance(v, (pd.Timestamp, pd.Timedelta)):
        return True
    return False


def _json_sanitize_scalar(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", errors="replace")
    # 【性能优化 V2】用 isinstance 替代 json.dumps() 类型检测
    if _is_json_serializable_type(v):
        return v
    return str(v)


def _json_safe_dict(d: Any) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in d.items():
        ks = str(k)
        try:
            if isinstance(v, dict):
                out[ks] = _json_safe_dict(v)
            elif isinstance(v, (list, tuple)):
                out[ks] = [
                    _json_safe_dict(x) if isinstance(x, dict) else _json_sanitize_scalar(x)
                    for x in v
                ]
            else:
                out[ks] = _json_sanitize_scalar(v)
        except Exception:
            out[ks] = str(v)
    return out


def _save_base_items_json(
    path: str,
    items: List[Dict[str, Any]],
    *,
    p1_envelope_source: Optional[str] = None,
) -> None:
    rows: List[Dict[str, Any]] = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        row: Dict[str, Any] = {
            "code": it.get("code"),
            "p1_score": float(it.get("p1_score", 0) or 0),
            "hist": _json_safe_dict(it.get("hist") or {}),
        }
        df = it.get("df")
        if isinstance(df, pd.DataFrame) and not df.empty:
            row["df_split"] = json.loads(df.to_json(orient="split", date_format="iso"))
        else:
            row["df_split"] = None
        rows.append(row)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        if p1_envelope_source in ("UI_MANUAL", "DAEMON_AUTO"):
            # P1 底仓 JSON：顶层主权元数据，与 core.pool_manager.p1_cache_json_should_skip_daemon_overwrite 对齐
            _ts = _now_bj().isoformat()
            json.dump(
                {
                    "_source": str(p1_envelope_source),
                    "_timestamp": _ts,
                    "items": rows,
                },
                f,
                ensure_ascii=False,
                indent=0,
            )
        else:
            json.dump(rows, f, ensure_ascii=False, indent=0)


def _load_base_items_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    # 兼容新版带主权封套 {"_source","_timestamp","items"} 与旧版顶层数组
    if isinstance(raw, dict) and isinstance(raw.get("items"), list):
        rows = raw["items"]
    elif isinstance(raw, list):
        rows = raw
    else:
        rows = []
    out: List[Dict[str, Any]] = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        hist = row.get("hist") or {}
        if not isinstance(hist, dict):
            hist = {}
        sp = row.get("df_split")
        if sp:
            df = pd.read_json(io.StringIO(json.dumps(sp)), orient="split")
        else:
            df = pd.DataFrame()
        out.append(
            {
                "code": row.get("code"),
                "p1_score": float(row.get("p1_score", 0) or 0),
                "df": df,
                "hist": hist,
            }
        )
    return out


def load_base_items_latest() -> List[Dict[str, Any]]:
    """优先当日 p1_cache JSON；否则取 runtime 目录下最新一份（周末后周一早盘常见）。"""
    try:
        from core.runtime_data_paths import POOL_CACHE_DIR, ensure_runtime_data_layout
        import glob

        ensure_runtime_data_layout()
        today = _now_bj().strftime("%Y%m%d")
        from core.runtime_data_paths import path_p1_cache_json

        # 【性能优化 V2】优化缓存查找：先检查当日缓存（O(1)），仅当日不存在时才 glob 搜索
        # 原逻辑：先 glob 所有 p1_cache_*.json 再排序，每次都执行文件系统搜索
        # 优化：直接检查当日文件是否存在（O(1)），避免不必要的 glob 开销
        p_today = path_p1_cache_json(today)
        if os.path.isfile(p_today):
            try:
                items = _load_base_items_json(p_today)
                if items:
                    logger.info("底仓载入: 当日缓存 %s (%s只)", p_today, len(items))
                    return items
            except Exception as e:
                logger.warning("读取当日底仓 JSON 失败: %s", e)
        # 仅当日不存在时才 glob 回退查找最近缓存
        # 【优化V2】使用 os.listdir 替代 glob.glob，减少正则开销
        try:
            all_files = os.listdir(POOL_CACHE_DIR)
            candidates = [f for f in all_files if f.startswith("p1_cache_") and f.endswith(".json")]
            candidates.sort(reverse=True)
            for fname in candidates:
                fp = os.path.join(POOL_CACHE_DIR, fname)
                try:
                    items = _load_base_items_json(fp)
                    if items:
                        logger.info("底仓载入: 最近缓存 %s (%s只)", fp, len(items))
                        return items
                except Exception as e:
                    logger.warning("读取底仓 JSON 失败 %s: %s", fp, e)
        except OSError:
            pass
    except Exception as e:
        logger.exception("load_base_items_latest: %s", e)
    return []


def _p1_cache_path_today() -> str:
    from core.runtime_data_paths import path_p1_cache_json, ensure_runtime_data_layout

    ensure_runtime_data_layout()
    return path_p1_cache_json(_now_bj().strftime("%Y%m%d"))


def _notification_url_and_enabled() -> Tuple[bool, str]:
    try:
        from core.config_manager import get_notification_config

        cfg = get_notification_config()
        en = bool(cfg.get("enabled"))
        url = str(cfg.get("wechat_webhook_url") or "").strip()
        return en, url
    except Exception as e:
        logger.debug("get_notification_config: %s", e)
        return False, ""


def daily_morning_routine() -> None:
    """
    每日 08:50 由独立守护线程执行（见 _job_daily_morning_routine，不占用 schedule 主循环）：
    1) 调用 clear_wechat_push_cache_global()，丢弃昨日可能残留的防刷键，防止字典长尾；
    2) 企微文案已与 09:18「早盘合并简报」合并为一条发送，此处不再单独推送（避免同日两条早间消息）。
    """
    from core.notification_gateway import clear_wechat_push_cache_global

    clear_wechat_push_cache_global()
    logger.info("早安例行：企微防刷缓存已清空；交易日完整早间企微简报于 09:18 预检通过后合并发送")


def _probe_duckdb_for_open_heartbeat() -> Tuple[bool, str]:
    """
    为早盘报备探活 DuckDB：能执行 SELECT 1 即视为正常。
    异常时返回 (False, 简短原因)，不向外抛，避免干扰守护进程主循环。
    """
    try:
        from data.db_core import get_read_conn_singleton

        con = get_read_conn_singleton()
        con.execute("SELECT 1").fetchone()
        return True, "正常"
    except Exception as e:
        return False, f"异常：{str(e)[:120]}"


def _morning_briefing_readiness() -> Tuple[bool, str, bool, str, Dict[str, Any]]:
    """
    合并早报发送前自检：DuckDB、master_control、风控配置可读、当日 P1 底仓已就绪（pipeline 或当日 JSON）。
    返回 (ok, fail_reason, db_ok, db_msg, mc)；失败时 fail_reason 供日志与运维告警，mc 可能为 {}。
    """
    db_ok, db_msg = _probe_duckdb_for_open_heartbeat()
    if not db_ok:
        return False, f"DuckDB不可连接: {str(db_msg)[:220]}", db_ok, db_msg, {}
    try:
        from core.master_control import read_master_control

        mc = read_master_control()
        if not isinstance(mc, dict):
            mc = {}
    except Exception as e:
        return False, f"master_control不可读: {e}", db_ok, db_msg, {}
    try:
        from core.config_manager import get_ui_alert_only

        get_ui_alert_only(force_reload=True)
    except Exception as e:
        return False, f"config/ui_alert_only不可读: {e}", db_ok, db_msg, mc
    today = _now_bj().strftime("%Y%m%d")
    st = _read_pipeline_state()
    if str(st.get("last_p1_ok_bj_date", "") or "").strip() != today:
        if not os.path.isfile(_p1_cache_path_today()):
            return (
                False,
                "P1底仓未就绪：当日 last_p1_ok_bj_date 非今日且无 p1_cache 当日 JSON（请查 08:50 早盘补位或晚间 P1）",
                db_ok,
                db_msg,
                mc,
            )
    return True, "", db_ok, db_msg, mc


def daily_open_heartbeat_routine() -> None:
    """
    交易日早盘 **合并简报**（原 08:50 早安 + 原核心引擎就绪）企微一条 Markdown：
    - 预检通过后才发送（见 ``_morning_briefing_readiness``）；
    - 早安问候、侧栏企微推送/自动巡航开关展示；
    - 数据库连接探活；
    - 当前风控模式：``get_ui_alert_only()`` → UI 预警 / 硬否决；
    - 盘中巡航待命提示。

    与 09:18 定时任务及启动补发共用同一入口：由本函数内的进程级锁 + pipeline_state 去重保证同日只发送一次。
    全函数外层由调用方线程再包 try-except；本函数内部对企微发送亦单独兜底，网络超时不得顶翻进程。
    """
    if not _OPEN_HEARTBEAT_LOCK.acquire(blocking=False):
        logger.debug("早盘就绪报备：已有线程在执行，跳过并发重入")
        return
    try:
        if not _daemon_is_trading_day_safe():
            logger.info("早盘就绪报备：非交易日，跳过")
            return

        today = _now_bj().strftime("%Y%m%d")
        st = _read_pipeline_state()
        if str(st.get("last_daemon_open_heartbeat_bj_date", "") or "").strip() == today:
            logger.debug("早盘就绪报备：本日已发送过，跳过")
            return

        en, url = _notification_url_and_enabled()
        if not en or not url:
            logger.info("早盘就绪报备：notification 未启用或未配置 webhook，跳过")
            return

        try:
            from core.master_control import is_wechat_push_master_enabled

            if not is_wechat_push_master_enabled():
                logger.info("早盘就绪报备：总控「企微推送」关闭，跳过")
                return
        except Exception as e:
            logger.debug("早盘就绪报备：读取 master_control 失败，继续尝试: %s", e)

        ok_r, why_r, db_ok, db_msg, mc = _morning_briefing_readiness()
        if not ok_r:
            logger.warning("早盘合并简报：预检未通过，跳过企微 | %s", why_r)
            _notify_daemon_alert(
                title="早盘合并简报已跳过（预检未过）",
                detail=f"{why_r}\n日期：{today}",
                category="daemon",
                dedup_key=f"daemon_morning_brief_preflight_skip_{today}",
            )
            return

        wechat_label = "开启" if mc.get("wechat_push_enabled") else "关闭"
        cruise_label = "开启" if mc.get("daemon_auto_cruise_enabled") else "关闭"

        try:
            from core.config_manager import get_ui_alert_only

            ui_alert_only = bool(get_ui_alert_only(force_reload=True))
            if ui_alert_only:
                risk_line = "纯界面预警：红线仅标签提示，不拦截出票"
            else:
                risk_line = "硬否决：触碰风控红线即拦截出票"
        except Exception as e:
            logger.debug("早盘就绪报备：读取 ui_alert_only 失败: %s", e)
            risk_line = "读取失败（已兜底）"

        ts = _now_bj().strftime("%Y-%m-%d %H:%M:%S")
        ts_short = _now_bj().strftime("%H:%M:%S")
        db_line = "正常" if db_ok else html.escape(str(db_msg), quote=False)
        risk_safe = html.escape(str(risk_line), quote=False)
        lines = [
            f"🟢 **【小杰AI选股系统 Pro V26.6 V26.6】早安 · 交易日简报** <font color=\"comment\">[{ts_short}]</font>",
            "",
            "今日为交易日，各项数据已就绪。",
            "",
            f"- 🎛️ 企微推送：{html.escape(str(wechat_label), quote=False)}",
            f"- 🤖 自动巡航：{html.escape(str(cruise_label), quote=False)}",
            "",
            "---",
            "",
            "✅ **小杰AI选股系统 Pro V26.6 核心引擎已就绪**",
            "",
            f"- 数据库连接：{db_line}",
            f"- 当前风控模式：{risk_safe}",
            "- 守护进程已进入盘中巡航待命",
            "",
            f'<font color="comment">时间：{ts}</font>',
        ]
        body = "\n".join(lines)
        try:
            from core.notification_gateway import get_wechat_gateway

            gw = get_wechat_gateway(url)
            gw.push_markdown_async(body)
        except Exception as e:
            logger.warning("早盘就绪报备：企微投递失败（已吞，不中断守护进程）: %s", e, exc_info=True)
            return

        _write_pipeline_state_patch(
            last_daemon_open_heartbeat_bj_date=today,
            last_daemon_open_heartbeat_at=_now_bj().isoformat(),
        )
        logger.info("早盘合并简报：已提交企微异步发送 | date=%s", today)
    except Exception as e:
        logger.exception("早盘就绪报备：未预期异常（已吞，不中断守护进程）: %s", e)
    finally:
        _OPEN_HEARTBEAT_LOCK.release()


def _maybe_open_heartbeat_on_startup() -> None:
    """
    09:18 启动补发入口：
    - 仅在交易日且北京时间落在 09:00~09:18（含）时启用；
    - 作用只是补发早盘合并简报，不改变 08:50 早盘编排链；
    - 最终仍统一落到 ``daily_open_heartbeat_routine()``，由其内部锁 + pipeline_state 去重。
    """
    try:
        now_bj = _now_bj()
        curr_min = now_bj.hour * 60 + now_bj.minute
        if curr_min < (9 * 60 + 0) or curr_min > (9 * 60 + 18):
            return
        if not _daemon_is_trading_day_safe():
            return

        def _runner() -> None:
            try:
                # 启动补发时，先尽量补齐 08:50 早盘补位链，避免 09:18 还处于 "--" 空状态。
                _morning_trading_catchup()
            except Exception as e:
                logger.exception("早盘补位(启动补发) 未捕获异常: %s", e)
            try:
                daily_open_heartbeat_routine()
            except Exception as e:
                logger.exception("早盘就绪报备(启动补发) 未捕获异常: %s", e)

        threading.Thread(target=_runner, name="daemon-open-hb-startup", daemon=True).start()
    except Exception as e:
        logger.exception("早盘就绪报备启动补发线程派生失败（已吞）: %s", e)


def _job_daily_open_heartbeat_routine() -> None:
    """09:18：调度入口只负责起线程；真正发送逻辑统一由 `daily_open_heartbeat_routine()` 执行。"""

    def _runner() -> None:
        try:
            daily_open_heartbeat_routine()
        except Exception as e:
            logger.exception("daily_open_heartbeat_routine 未捕获异常: %s", e)

    try:
        threading.Thread(target=_runner, name="daemon-open-hb-0918", daemon=True).start()
    except Exception as e:
        logger.exception("早盘就绪报备线程派生失败: %s", e)


def _push_top_scores(
    pool_key: str,
    rows: List[Dict[str, Any]],
    top_n: int,
    res: Optional[Dict[str, Any]] = None,
) -> None:
    en, url = _notification_url_and_enabled()
    if not en or not url:
        return
    obs_rows: List[Dict[str, Any]] = []
    if res is not None and isinstance(res, dict):
        obs = res.get("observation") or {}
        if isinstance(obs, dict):
            o = obs.get(pool_key)
            if isinstance(o, list):
                obs_rows = o
    if not rows and not obs_rows:
        return
    try:
        from core.notification_gateway import get_wechat_gateway

        gw = get_wechat_gateway(url)

        def _sc(r: Dict[str, Any]) -> float:
            try:
                return float(r.get("综合分", 0.0) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        for row in sorted(rows, key=_sc, reverse=True)[: max(1, top_n)]:
            if isinstance(row, dict):
                gw.push_stock_if_allowed(pool_key, row)
        for row in sorted(obs_rows, key=_sc, reverse=True)[: max(1, top_n)]:
            if isinstance(row, dict):
                gw.push_stock_if_allowed(pool_key, row, pool_key_for_dedup=f"{pool_key}_obs")
    except Exception as e:
        logger.debug("企微推送失败(已吞): %s", e, exc_info=True)


def _scrub_base_items_inplace(items: List[Dict[str, Any]]) -> None:
    for it in items or []:
        if isinstance(it, dict):
            it.pop("df", None)
    try:
        items.clear()
    except Exception:
        pass


def _gc_after_scan(*, base: Optional[List[Dict[str, Any]]], res: Any) -> None:
    try:
        if isinstance(res, dict):
            res.clear()
    except Exception:
        pass
    if base is not None:
        _scrub_base_items_inplace(base)
    try:
        del res
    except Exception:
        pass
    try:
        del base
    except Exception:
        pass
    gc.collect()


def _bj_minutes_of_day() -> int:
    """北京时间当前时刻在当日 00:00 起的分钟数，用于时间窗比较。"""
    n = _now_bj()
    return int(n.hour) * 60 + int(n.minute)


def _in_p4_tail_priority_window() -> bool:
    """
    P4 优先走廊：14:31–14:55（含边界）。14:40–14:55 内 P4 五拍中的后四拍为「重仓核心」时段。
    此间 **严禁** 派发 P3（见 _job_p3_tick_clock_aligned）：不抢锁、不排队、不占主循环。
    """
    m = _bj_minutes_of_day()
    return P4_TAIL_PRIORITY_START_MIN <= m <= P4_TAIL_PRIORITY_END_MIN


def _run_scan_push_p2() -> None:
    """2 档竞价池：与 UI「竞价突袭」同源；守护进程固定在 09:26 触发，与 09:18 早盘合并简报错峰。"""
    if _maintenance_mode_skip("P2竞价"):
        return
    if _daemon_cruise_off_skip("P2竞价"):
        return
    if not _barrier_trading_day_or_skip("P2竞价"):
        return
    base = load_base_items_latest()
    if not base:
        logger.warning("P2：底仓为空，跳过扫描")
        return
    res: Any = None
    try:
        from core.scan_engine import run_scan_engine

        regime = _resolve_regime_name()
        res = run_scan_engine(["p2"], base, regime=regime, progress_callback=None)
        rows = res.get("p2") or []
        _push_top_scores("p2", rows if isinstance(rows, list) else [], 3, res if isinstance(res, dict) else None)
    except Exception as e:
        logger.exception("P2 扫描异常: %s", e)
        _notify_daemon_alert(
            "P2 竞价扫描异常",
            str(e)[:900],
            category="scan_p2",
            dedup_key=f"daemon_p2_scan_{_now_bj().strftime('%Y%m%d')}",
        )
    finally:
        _gc_after_scan(base=base, res=res)


def _job_p2_tick() -> None:
    """09:26 竞价扫描：子线程内最多等锁 10 分钟（早盘链或快照占锁时可排队）。"""
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过: P2竞价")
        return
    _spawn("P2", _run_scan_push_p2, wait_for_lock_sec=600.0)


def _in_p3_patrol_window() -> bool:
    """
    P3 允许跑扫描的盘中时段（与策略一致）：早盘 09:30–11:30、午盘 13:00–14:30。
    注意：14:30 之后即使调度器仍 tick，此处也会挡掉；与尾盘窗互斥策略叠加后，14:35 起仅 P4 活跃。
    """
    m = _bj_minutes_of_day()
    if 570 <= m <= 690:
        return True
    if 780 <= m <= 870:
        return True
    return False


def _run_scan_push_p3() -> None:
    if _maintenance_mode_skip("P3巡逻"):
        return
    if _daemon_cruise_off_skip("P3巡逻"):
        return
    if not _barrier_trading_day_or_skip("P3巡逻"):
        return
    if not _in_p3_patrol_window():
        return
    base = load_base_items_latest()
    if not base:
        logger.warning("P3：底仓为空，跳过扫描")
        return
    res: Any = None
    try:
        from core.scan_engine import run_scan_engine

        regime = _resolve_regime_name()
        res = run_scan_engine(["p3"], base, regime=regime, progress_callback=None)
        rows = res.get("p3") or []
        _push_top_scores("p3", rows if isinstance(rows, list) else [], 3, res if isinstance(res, dict) else None)
    except Exception as e:
        logger.exception("P3 扫描异常: %s", e)
        _notify_daemon_alert(
            "P3 盘中扫描异常",
            str(e)[:900],
            category="scan_p3",
            dedup_key=f"daemon_p3_scan_{_now_bj().strftime('%Y%m%d%H')}",
        )
    finally:
        _gc_after_scan(base=base, res=res)


def _run_scan_push_p4() -> None:
    if _maintenance_mode_skip("P4尾盘"):
        return
    if _daemon_cruise_off_skip("P4尾盘"):
        return
    if not _barrier_trading_day_or_skip("P4尾盘"):
        return
    base = load_base_items_latest()
    if not base:
        logger.warning("P4：底仓为空，跳过扫描")
        return
    res: Any = None
    try:
        from core.scan_engine import run_scan_engine

        regime = _resolve_regime_name()
        res = run_scan_engine(["p4"], base, regime=regime, progress_callback=None)
        rows = res.get("p4") or []
        _push_top_scores("p4", rows if isinstance(rows, list) else [], 3, res if isinstance(res, dict) else None)
    except Exception as e:
        logger.exception("P4 扫描异常: %s", e)
        _notify_daemon_alert(
            "P4 尾盘扫描异常",
            str(e)[:900],
            category="scan_p4",
            dedup_key=f"daemon_p4_scan_{_now_bj().strftime('%Y%m%d%H')}",
        )
    finally:
        _gc_after_scan(base=base, res=res)


def _run_scan_push_p5() -> None:
    if _maintenance_mode_skip("P5盘后"):
        return
    if _daemon_cruise_off_skip("P5盘后"):
        return
    if not _barrier_trading_day_or_skip("P5盘后"):
        return
    base = load_base_items_latest()
    if not base:
        logger.warning("P5：底仓为空，跳过扫描")
        return
    res: Any = None
    # P5 盘后仅做扫描与推送；不在这里再写入会话表/维护表，避免与晚间同步链抢写 DuckDB。
    try:
        from core.scan_engine import run_scan_engine

        regime = _resolve_regime_name()
        res = run_scan_engine(["p5"], base, regime=regime, progress_callback=None)
        rows = res.get("p5") or []
        filtered_rows = []
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    score = float(row.get("综合分", 0.0) or 0.0)
                except (TypeError, ValueError):
                    score = 0.0
                if score >= 60.0:
                    filtered_rows.append(row)
        _push_top_scores("p5", filtered_rows, 10, res if isinstance(res, dict) else None)
    except Exception as e:
        logger.exception("P5 扫描异常: %s", e)
        _notify_daemon_alert(
            "P5 盘后扫描异常",
            str(e)[:900],
            category="scan_p5",
            dedup_key=f"daemon_p5_scan_{_now_bj().strftime('%Y%m%d%H')}",
        )
    finally:
        _gc_after_scan(base=base, res=res)


# ---------- 同步核心：返回 bool，供晚间链/早盘链强依赖 ----------

# 最新交易日（MAX(trade_date)）相对次新日行数明显偏少时，对当日再执行 sync_single_day 的总次数
_LATEST_TRADE_DAY_RESYNC_ATTEMPTS = 5


def _is_duckdb_lock_error(err: BaseException) -> bool:
    """
    识别 DuckDB 文件被其他进程占用导致的锁冲突异常。
    """
    raw = str(err or "")
    msg = raw.lower()
    if "database is locked" in msg or "could not set lock" in msg:
        return True
    if "quant_data.duckdb" not in msg:
        return False
    if "cannot open file" in msg or "io error" in msg:
        return True
    if "进程无法访问" in raw or "being used by another" in msg:
        return True
    return False


def _extract_lock_holder_pid(err: BaseException) -> Optional[int]:
    """从 DuckDB 锁异常文本中提取占锁 PID（如：PID 22072）。"""
    m = re.search(r"PID\s*(\d+)", str(err or ""), flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _resolve_pid_brief(pid: int) -> str:
    """查询 PID 对应进程名，失败时回退为 unknown。"""
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
            text=True,
            encoding="utf-8",
            errors="ignore",
        ).strip()
        if not out:
            return "unknown"
        first = out.splitlines()[0].strip()
        if first.upper().startswith("INFO:"):
            return "unknown"
        if first.startswith('"') and "," in first:
            parts = [x.strip().strip('"') for x in first.split('","')]
            if parts and parts[0]:
                return parts[0]
    except Exception:
        pass
    return "unknown"


def _notify_duckdb_lock_holder_once(err: BaseException, *, phase: str, attempt: int) -> None:
    """
    DuckDB 占锁告警（同一 PID + phase + 10 分钟窗口内去重），避免告警风暴。
    """
    pid = _extract_lock_holder_pid(err)
    pid_key = str(pid) if pid is not None else "unknown"
    dedup = f"duckdb_lock_{phase}_{pid_key}"
    now_ts = time.time()
    last_ts = float(_DUCKDB_LOCK_ALERT_TS.get(dedup, 0.0))
    if now_ts - last_ts < 600:
        return
    _DUCKDB_LOCK_ALERT_TS[dedup] = now_ts
    proc_name = _resolve_pid_brief(pid) if pid is not None else "unknown"
    _notify_daemon_alert(
        "DuckDB 文件占锁告警",
        (
            f"阶段={phase} attempt={attempt} 检测到 quant_data.duckdb 被占用；"
            f"holder_pid={pid_key} holder_proc={proc_name}。"
            "守护进程已进入指数退避等待并自动重试。"
        ),
        category="data_sync",
        dedup_key=f"daemon_{dedup}_{_now_bj().strftime('%Y%m%d%H')}",
    )


def _ensure_stock_basic_table_ready() -> bool:
    """
    同步链成功后刷新 stock_basic（行业/简称维表）：
    - 每次成功跑完日线增量后执行全量 sync_stock_basic（L+D+P + daily 缺码补行），
      避免「表已存在但从不刷新」导致新股/映射过期、P1 推送简称仍缺。
    - 失败不阻断主链，但会告警提示后续行业/简称相关能力可能降级。
    """
    if _maintenance_mode_skip("stock_basic维表刷新"):
        return False
    try:
        from data.db_core import table_exists, sync_stock_basic
    except Exception as e:
        logger.warning("stock_basic 兜底检查导入失败（降级忽略）: %s", e)
        return False

    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            ok = bool(sync_stock_basic())
            if ok and bool(table_exists("stock_basic")):
                logger.info("stock_basic 维表已刷新 (attempt %s/3)", attempt + 1)
                return True
            raise RuntimeError("sync_stock_basic 返回失败或表仍不存在")
        except Exception as e:
            last_err = e
            if _is_duckdb_lock_error(e):
                _notify_duckdb_lock_holder_once(e, phase="stock_basic", attempt=attempt + 1)
                backoff = min(120.0, 20.0 * (2 ** attempt))
                logger.warning(
                    "stock_basic 自动补齐失败 attempt %s/3（DuckDB占锁）: %s | 退避 %.0fs",
                    attempt + 1,
                    e,
                    backoff,
                )
                time.sleep(backoff)
            else:
                logger.warning("stock_basic 自动补齐失败 attempt %s/3: %s", attempt + 1, e)
                time.sleep(8.0 * (attempt + 1))

    detail = str(last_err)[:900] if last_err is not None else "unknown"
    _notify_daemon_alert(
        "行业维表刷新失败（stock_basic）",
        "自动重试后仍未写入 stock_basic；P1 推送简称、P2~P5 板块/行业相关能力可能降级。"
        f"最后错误：{detail}",
        category="data_sync",
        dedup_key=f"daemon_stock_basic_missing_{_now_bj().strftime('%Y%m%d')}",
    )
    return False


def _resync_max_trade_day_if_sparse() -> bool:
    """
    在 sync_recent_days + 缺失补洞完成后调用：若库内「最新交易日」行数相对「次新日」半残
    （与 P1 日线锚定同一口径：n < max(500, 85%×次新)），则对该日 sync_single_day，
    最多 _LATEST_TRADE_DAY_RESYNC_ATTEMPTS 次；任一次成功后若不再半残即返回 True。
    仍半残则返回 False（晚间/早盘链将不继续 P1，避免在错数据上全量洗盘）。
    """
    if _maintenance_mode_skip("最新交易日半残救场"):
        # 维护模式下不执行 sync_single_day 写库救场；返回 True 以免误将整轮增量同步判失败。
        return True
    from data import data_fetcher
    from data.db_core import is_max_trade_date_daily_rows_sparse

    sparse, ymd, n0, n1 = is_max_trade_date_daily_rows_sparse()
    if not sparse or not ymd:
        return True

    logger.warning(
        "【最新日救场】疑似半残 trade_date=%s 行数=%s vs 次新=%s，将对当日重拉最多 %s 次",
        ymd,
        n0,
        n1,
        _LATEST_TRADE_DAY_RESYNC_ATTEMPTS,
    )

    def _cb(msg: str) -> None:
        logger.info("[sync-rescue] %s", msg)

    # 数据源常在收盘后 15:30～20:30 间分批落库；锚定日为当日时先等待，减少「半残→救场仍失败」误报
    if ymd == _now_bj().strftime("%Y%m%d"):
        logger.info("【最新日救场】锚定日为当日，先等待 90s 以利日线接口收全")
        time.sleep(90.0)

    for attempt in range(_LATEST_TRADE_DAY_RESYNC_ATTEMPTS):
        ok_pull = bool(data_fetcher.sync_single_day(ymd, status_callback=_cb))
        sparse2, _, n2, n3 = is_max_trade_date_daily_rows_sparse()
        if not sparse2:
            logger.info(
                "【最新日救场】成功 attempt=%s/%s sync_single_day_ok=%s 最新日 n=%s",
                attempt + 1,
                _LATEST_TRADE_DAY_RESYNC_ATTEMPTS,
                ok_pull,
                n2,
            )
            return True
        logger.warning(
            "【最新日救场】仍半残 attempt=%s/%s sync_single_day_ok=%s 最新日 n=%s vs 次新 n=%s",
            attempt + 1,
            _LATEST_TRADE_DAY_RESYNC_ATTEMPTS,
            ok_pull,
            n2,
            n3,
        )
        if attempt < _LATEST_TRADE_DAY_RESYNC_ATTEMPTS - 1:
            time.sleep(45.0 * (attempt + 1))
    logger.error(
        "【最新日救场】已达最大次数仍半残 trade_date=%s，本轮同步判失败（不继续 P1）",
        ymd,
    )
    return False


def _sync_daily_incremental_core() -> bool:
    """
    先执行近端交易日同步，再做缺失交易日补洞（基于交易日历，不依赖自然日）。
    返回 True 表示本轮同步与补洞均成功；False 表示失败，禁止继续 P1。
    """
    if _maintenance_mode_skip("增量同步 sync_recent_days"):
        return False
    if _daemon_cruise_off_skip("增量同步 sync_recent_days"):
        return False
    import constants
    from data import data_fetcher

    last_err: Optional[Exception] = None
    for attempt in range(5):
        try:

            def _cb(msg: str) -> None:
                logger.info("[sync] %s", msg)

            data_fetcher.sync_recent_days(
                days=5,
                status_callback=_cb,
                progress_callback=None,
                raise_on_all_days_failed=True,
            )
            # 第二阶段：补洞模式（按全局窗口自动补齐缺口天数：缺3补3、缺10补10）
            # 与 UI「自动补缺到最新交易日」保持一致，避免前后台口径不一致。
            lookback_days = int(getattr(constants, "MAX_DAYS", 150) or 150)
            ok_comp, missing_dates = data_fetcher.check_data_completeness(days=lookback_days)
            if not ok_comp:
                raise RuntimeError("check_data_completeness 未成功，无法确认缺失交易日")
            if missing_dates:
                logger.warning(
                    "检测到缺失交易日 %s 天（lookback=%s），开始批量补洞: %s",
                    len(missing_dates),
                    lookback_days,
                    missing_dates,
                )
                failed_days = data_fetcher.sync_missing_days_batch(
                    missing_days=missing_dates,
                    status_callback=_cb,
                )
                if failed_days:
                    raise RuntimeError(f"批量补洞有 {len(failed_days)} 天失败: {failed_days}")
            logger.info("增量同步+缺失补洞完成 (attempt %s)", attempt + 1)
            if not _resync_max_trade_day_if_sparse():
                logger.error("最新交易日救场失败（已对当日重拉多轮），本轮同步判失败")
                return False
            # 非阻断兜底：补齐行业维表，避免后续 P2~P5 因 stock_basic 缺失长期降级。
            _ensure_stock_basic_table_ready()
            return True
        except data_fetcher.DataFetchCriticalError as e:
            # 企微已告警；网络/Token 级问题重试无意义，直接结束本轮
            logger.error("增量同步熔断（Tushare/网络不可用），不重试: %s", e)
            return False
        except Exception as e:
            last_err = e
            if _is_duckdb_lock_error(e):
                _notify_duckdb_lock_holder_once(e, phase="sync_recent", attempt=attempt + 1)
                backoff = min(300.0, 30.0 * (2 ** attempt))
                logger.warning(
                    "增量同步失败 attempt %s（DuckDB占锁）: %s | 退避 %.0fs 后重试",
                    attempt + 1,
                    e,
                    backoff,
                )
                time.sleep(backoff)
            else:
                logger.warning("增量同步失败 attempt %s: %s", attempt + 1, e)
                time.sleep(20.0 * (attempt + 1))
    if last_err is not None:
        logger.error("增量同步最终失败: %s", last_err)
    gc.collect()
    return False


def _evening_incremental_sync_can_skip() -> bool:
    """
    交易日：lookback 内无缺失、最新交易日非半残时，无需再跑 _sync_daily_incremental_core（省流量）。
    """
    try:
        import constants
        from data import data_fetcher
        from data.db_core import is_max_trade_date_daily_rows_sparse

        if getattr(data_fetcher, "pro", None) is None:
            return False
        lookback_days = int(getattr(constants, "MAX_DAYS", 150) or 150)
        ok_comp, missing_dates = data_fetcher.check_data_completeness(days=lookback_days)
        if not ok_comp or missing_dates:
            return False
        sparse, ymd, _, _ = is_max_trade_date_daily_rows_sparse()
        if sparse or not ymd:
            return False
        return True
    except Exception as e:
        logger.debug("晚间增量跳过检查: %s", e)
        return False


def _push_p1_ge75_after_daemon_wash() -> None:
    """P1 JSON 落盘后，按 master_control 推送综合分≥65 底仓，最多 12 只（与 UI 洗盘后推送同源）。"""
    if _maintenance_mode_skip("P1高分池企微推送"):
        return
    try:
        from core.notification_gateway import notify_p1_high_score_pool_after_wash

        p = _p1_cache_path_today()
        if not os.path.isfile(p):
            return
        items = _load_base_items_json(p)
        if items:
            notify_p1_high_score_pool_after_wash(items, min_score=60.0, main_score=75.0, max_items=8)
    except Exception as e:
        logger.exception("P1≥75 分企微推送失败: %s", e)


def _evening_sync_only_pipeline() -> None:
    """
    交易日 19:45：仅日线增量同步（含补洞、半残救场）；不执行 P1/P5。
    若库内已覆盖最新交易日且无缺失、非半残，则跳过拉取，仅写入 last_sync_ok 供 19:55/20:05 链使用。
    """
    if _maintenance_mode_skip("晚间数据链(仅同步)"):
        return
    if _daemon_cruise_off_skip("晚间数据链(仅同步)"):
        return
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过晚间增量同步")
        return
    today = _now_bj().strftime("%Y%m%d")
    if _evening_incremental_sync_can_skip():
        logger.info("晚间增量同步：库内已为最新且无半残，跳过拉取 | BJ日期=%s", today)
        if not _write_pipeline_state_patch(last_sync_ok_bj_date=today, last_sync_fail_bj_date=""):
            _notify_daemon_alert(
                "晚间链：状态位写入失败",
                f"日期 {today}：晚间增量同步已跳过拉取，但 last_sync_ok_bj_date 写盘失败。",
                category="data_sync",
                dedup_key=f"daemon_pipeline_state_write_fail_syncskip_{today}",
            )
        return
    logger.info("晚间增量同步开始 | BJ日期=%s", today)
    ok = _sync_daily_incremental_core()
    if not ok:
        logger.error("晚间同步失败（19:55/20:05 将跳过 P1/P5 链）")
        if not _write_pipeline_state_patch(last_sync_ok_bj_date="", last_sync_fail_bj_date=today):
            _notify_daemon_alert(
                "晚间链：状态位写入失败",
                f"日期 {today}：晚间增量同步失败后，last_sync_fail_bj_date 写盘失败。",
                category="data_sync",
                dedup_key=f"daemon_pipeline_state_write_fail_syncfail_{today}",
            )
        _notify_daemon_alert(
            "晚间链：日线增量同步失败",
            (
                f"计划触发：北京时间 **19:45**（仅增量，不含 P1/P5）\n"
                f"告警时间：{_now_bj().strftime('%Y-%m-%d %H:%M:%S')}（常为整轮重试结束时刻，非整点触发）\n"
                f"日期 {today}：增量同步（含 sync_recent_days 与补洞）多轮重试仍失败，或最新交易日半残救场仍失败，已跳过当日同步；**19:55** P1 与 **20:05** P5 将不跑。\n"
                "若失败总出现在旧整点，多半是守护进程未重启、仍在跑旧时间表，请重启 auto_sniper_daemon。\n"
                "详情见 data/runtime/sniper.log 末条异常。"
            ),
            category="data_sync",
            dedup_key=f"daemon_evening_sync_fail_{today}",
        )
        return
    if not _write_pipeline_state_patch(last_sync_ok_bj_date=today, last_sync_fail_bj_date=""):
        _notify_daemon_alert(
            "晚间链：状态位写入失败",
            f"日期 {today}：晚间增量同步成功，但 last_sync_ok_bj_date 写盘失败。",
            category="data_sync",
            dedup_key=f"daemon_pipeline_state_write_fail_syncok_{today}",
        )
    logger.info("晚间增量同步完成 | BJ日期=%s（19:55 将执行 P1+推送；20:05 将执行 P5）", today)
    _notify_daemon_alert(
        "晚间链：日线增量同步已完成",
        f"日期 {today}（北京时间）：自动增量同步成功；**19:55** P1 与 ≥75 推送；**20:05** P5 盘后扫描。", 
        category="data_sync",
        dedup_key=f"daemon_evening_sync_ok_{today}",
    )


def _evening_p1_rebuild_pipeline() -> None:
    """
    交易日 19:55：P1 全量重建 → 高分池推送（不含 P5；P5 由 20:05 独立任务执行）。
    仅当当日 19:45 已标记同步成功（pipeline_state.last_sync_ok_bj_date==今日，含「已最新跳过」路径）。
    """
    if _maintenance_mode_skip("晚间P1重建"):
        return
    if _daemon_cruise_off_skip("晚间P1重建"):
        return
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过晚间 P1 重建")
        return
    today = _now_bj().strftime("%Y%m%d")
    st = _read_pipeline_state()
    last_sync_ok = str(st.get("last_sync_ok_bj_date", "") or "")
    if last_sync_ok != today:
        logger.warning(
            "晚间P1：当日尚未标记增量同步成功(last_sync_ok=%s)，跳过 P1 重建",
            last_sync_ok or "(空)",
        )
        return
    logger.info("晚间 P1 重建开始 | BJ日期=%s", today)
    p1_ok = False
    for _p1_try in range(3):
        p1_ok = _rebuild_p1_cache_job(require_trading_day_barrier=False)
        if p1_ok:
            break
        logger.warning("P1 重建未成功，%.0fs 后重试 (%s/3)", 45.0 * (_p1_try + 1), _p1_try + 2)
        time.sleep(45.0 * (_p1_try + 1))
    if p1_ok:
        if not _write_pipeline_state_patch(last_p1_ok_bj_date=today):
            _notify_daemon_alert(
                "晚间链：状态位写入失败",
                f"日期 {today}：晚间 P1 重建成功，但 last_p1_ok_bj_date 写盘失败。",
                category="data_sync",
                dedup_key=f"daemon_pipeline_state_write_fail_p1ok_{today}",
            )
        logger.info("晚间 P1 重建完成 | BJ日期=%s", today)
        _notify_daemon_alert(
            "晚间链：P1 底仓重建已完成",
            f"日期 {today}（北京时间）：P1 落盘完成；若开启总控「推送 P1 高分池」，已尝试推送≥65 分；**20:05** 将执行 P5 盘后扫描。", 
            category="data_sync",
            dedup_key=f"daemon_evening_p1_chain_ok_{today}",
        )
        _push_p1_ge75_after_daemon_wash()
        logger.info("晚间链：P1 与高分推送完成，P5 由 20:05 定时任务执行")
    else:
        logger.error("晚间链：P1 重建未成功，请检查日志")
        _notify_daemon_alert(
            "晚间链：P1 底仓重建失败",
            f"日期 {today}：当日增量已同步成功，但 P1 重建未落盘，请查守护进程日志。",
            category="scan_p1",
            dedup_key=f"daemon_evening_p1_fail_{today}",
        )


def _evening_p5_only_pipeline() -> None:
    """
    交易日 20:05：仅 P5 盘后扫描与企微推送。
    要求当日 19:45 已标记同步成功，且 19:55 P1 已成功落盘（last_p1_ok_bj_date==今日）。
    """
    if _maintenance_mode_skip("晚间P5盘后"):
        return
    if _daemon_cruise_off_skip("晚间P5盘后"):
        return
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过晚间 P5 盘后")
        return
    today = _now_bj().strftime("%Y%m%d")
    st = _read_pipeline_state()
    last_sync_ok = str(st.get("last_sync_ok_bj_date", "") or "")
    last_p1_ok = str(st.get("last_p1_ok_bj_date", "") or "")
    if last_sync_ok != today:
        logger.warning(
            "晚间P5：等待落库失败——未找到当日增量同步成功标记(last_sync_ok_bj_date=%s, today=%s)，跳过 P5",
            last_sync_ok or "(空)",
            today,
        )
        return
    if last_p1_ok != today:
        logger.warning(
            "晚间P5：等待落库失败——未找到当日 P1 成功落盘标记(last_p1_ok_bj_date=%s, today=%s)，跳过 P5",
            last_p1_ok or "(空)",
            today,
        )
        return
    logger.info("晚间 P5 盘后扫描开始 | BJ日期=%s | last_sync_ok_bj_date=%s | last_p1_ok_bj_date=%s", today, last_sync_ok, last_p1_ok)
    _run_scan_push_p5()


def _morning_trading_catchup() -> None:
    """
    【周末穿越补丁】交易日早盘 08:50：
    - 以晚间 19:45 同步（或已最新跳过）为准：若 pipeline_state 显示「当日晚间同步已成功」且当日 P1 文件存在，则直接跳过。
    - 仅在晚间同步未成功，或当日 P1 文件缺失时，执行早盘兜底链。
    """
    if _maintenance_mode_skip("早盘补位(同步+条件P1)"):
        return
    if _daemon_cruise_off_skip("早盘补位(同步+条件P1)"):
        return
    if not _daemon_is_trading_day_safe():
        return
    today = _now_bj().strftime("%Y%m%d")
    p_today = _p1_cache_path_today()
    need_p1 = not os.path.isfile(p_today)
    st = _read_pipeline_state()
    last_sync_ok = str(st.get("last_sync_ok_bj_date", "") or "")
    evening_sync_ok_today = last_sync_ok == today
    logger.info("早盘补位 | 当日P1缓存存在=%s | path=%s", not need_p1, p_today)
    if evening_sync_ok_today and not need_p1:
        logger.info("早盘补位跳过：已确认晚间同步成功且当日 P1 缓存已存在（以晚间结果为准）")
        return
    if evening_sync_ok_today and need_p1:
        logger.warning("早盘补位：晚间同步已成功，但当日 P1 缓存缺失，执行仅补建 P1（跳过增量同步）")
        for _p1_try in range(3):
            if _rebuild_p1_cache_job(require_trading_day_barrier=False):
                if not _write_pipeline_state_patch(last_p1_ok_bj_date=today):
                    _notify_daemon_alert(
                        "早盘链：状态位写入失败",
                        f"日期 {today}：早盘仅补建 P1 成功，但 last_p1_ok_bj_date 写盘失败。",
                        category="data_sync",
                        dedup_key=f"daemon_pipeline_state_write_fail_morning_p1only_{today}",
                    )
                gc.collect()
                return
            logger.warning("早盘 P1 补建失败，%.0fs 后重试 (%s/3)", 45.0 * (_p1_try + 1), _p1_try + 2)
            time.sleep(45.0 * (_p1_try + 1))
        logger.error("早盘仅补建 P1 未成功，请检查日志")
        _notify_daemon_alert(
            "早盘链：P1 补建失败",
            f"日期 {today}：晚间同步已成功，但当日 P1 文件缺失且早盘补建失败。",
            category="scan_p1",
            dedup_key=f"daemon_morning_p1_only_fail_{today}",
        )
        return
    ok = _sync_daily_incremental_core()
    if not ok:
        logger.warning("早盘同步失败；若缺当日P1文件，扫描仍将回退到最近 JSON")
        if need_p1:
            logger.warning("早盘同步失败且当日 P1 缓存缺失，尝试应急补建 P1（最多 3 轮）")
            emergency_p1_ok = False
            for _p1_try in range(3):
                if _rebuild_p1_cache_job(require_trading_day_barrier=False):
                    emergency_p1_ok = True
                    break
                logger.warning(
                    "早盘应急 P1 补建失败，%.0fs 后重试 (%s/3)",
                    45.0 * (_p1_try + 1),
                    _p1_try + 2,
                )
                time.sleep(45.0 * (_p1_try + 1))
            if emergency_p1_ok:
                if not _write_pipeline_state_patch(last_p1_ok_bj_date=today):
                    _notify_daemon_alert(
                        "早盘链：状态位写入失败",
                        f"日期 {today}：早盘应急 P1 补建成功，但 last_p1_ok_bj_date 写盘失败。",
                        category="data_sync",
                        dedup_key=f"daemon_pipeline_state_write_fail_morning_emergency_{today}",
                    )
                logger.info("早盘同步失败后应急 P1 补建成功，继续使用当日底仓文件")
                return
        _notify_daemon_alert(
            "早盘链：增量同步失败",
            f"日期 {today}：早盘补位 sync_recent_days 未成功；若缺当日 P1 文件，盘中可能回退到最近 JSON。",
            category="data_sync",
            dedup_key=f"daemon_morning_sync_fail_{today}",
        )
        return
    if not _write_pipeline_state_patch(last_morning_sync_ok_bj_date=today):
        _notify_daemon_alert(
            "早盘链：状态位写入失败",
            f"日期 {today}：早盘增量同步成功，但 last_morning_sync_ok_bj_date 写盘失败。",
            category="data_sync",
            dedup_key=f"daemon_pipeline_state_write_fail_morning_syncok_{today}",
        )
    if need_p1:
        logger.warning("当日 P1 缓存缺失，早盘同步后触发补建")
        for _p1_try in range(3):
            if _rebuild_p1_cache_job(require_trading_day_barrier=False):
                break
            logger.warning("早盘 P1 补建失败，%.0fs 后重试 (%s/3)", 45.0 * (_p1_try + 1), _p1_try + 2)
            time.sleep(45.0 * (_p1_try + 1))
    gc.collect()


def _rebuild_p1_cache_job(*, require_trading_day_barrier: bool = True) -> bool:
    """
    重建 P1 并写入当日 JSON + DuckDB p1_cache。
    require_trading_day_barrier=False：由晚间链/早盘链外层已判定交易日时传入，避免重复 trade_cal。
    返回是否完整跑通未抛致命错误；若当日 JSON 已带 UI_MANUAL 主权则跳过落盘但仍返回 True（以人工底仓为准）。
    """
    if _maintenance_mode_skip("P1重建"):
        return False
    if _daemon_cruise_off_skip("P1重建"):
        return False
    if require_trading_day_barrier:
        if not _barrier_trading_day_or_skip("P1重建"):
            return False
    mock_raw: List[Dict[str, Any]] = []
    rejected: List[Any] = []
    ok_out = False
    try:
        from data.db_core import get_all_stock_codes, get_p1_candidate_codes, get_stock_data_qfq
        from core.pool_manager import build_p1_pool_and_cache
        from core.runtime_data_paths import path_p1_cache_json, ensure_runtime_data_layout

        ensure_runtime_data_layout()
        target_codes: List[str] = []
        for wait_round in range(4):
            target_codes = get_p1_candidate_codes() or []
            if not target_codes:
                target_codes = get_all_stock_codes() or []
            target_codes = list(dict.fromkeys(target_codes))
            if target_codes:
                break
            if wait_round < 3:
                delay = 30.0 * (wait_round + 1)
                logger.warning(
                    "P1 重建：候选代码为空（常见于另一进程占锁 DuckDB），%.0fs 后重试 (%s/4)",
                    delay,
                    wait_round + 2,
                )
                time.sleep(delay)
        if not target_codes:
            logger.error("P1 重建：无候选代码")
            _notify_daemon_alert(
                "P1 重建：无候选代码",
                "get_p1_candidate_codes / get_all_stock_codes 在多次重试后仍为空。"
                "请确认 quant_data.duckdb 未被 Streamlit/其它 python 长期占写锁，且 daily_data 已有数据。",
                category="scan_p1",
                dedup_key=f"daemon_p1_no_codes_{_now_bj().strftime('%Y%m%d')}",
            )
            return False

        for c in target_codes:
            try:
                df = get_stock_data_qfq(c, limit=120)
                if df is not None and not df.empty:
                    mock_raw.append({"code": c, "df": df, "hist": df.iloc[-1].to_dict()})
            except Exception as e:
                logger.debug("P1 单票跳过 %s: %s", c, e)

        if not mock_raw:
            logger.error("P1 重建：mock_raw 为空")
            _notify_daemon_alert(
                "P1 重建：K 线数据全空",
                f"候选 {len(target_codes)} 只均未读到有效 K 线，请检查 DuckDB 与占用。",
                category="scan_p1",
                dedup_key=f"daemon_p1_mock_empty_{_now_bj().strftime('%Y%m%d')}",
            )
            return False

        regime_name = _resolve_regime_name()
        base_items, rejected = build_p1_pool_and_cache(
            mock_raw,
            progress_callback=None,
            regime_name=regime_name,
        )
        today = _now_bj().strftime("%Y%m%d")
        out_path = path_p1_cache_json(today)
        try:
            from core.pool_manager import p1_cache_json_should_skip_daemon_overwrite

            if p1_cache_json_should_skip_daemon_overwrite(out_path):
                logger.info(
                    "P1 重建：已跳过 JSON 与 DuckDB p1_cache 覆写（保留 UI_MANUAL 人工底仓）| %s",
                    out_path,
                )
                ok_out = True
            else:
                _save_base_items_json(out_path, base_items, p1_envelope_source="DAEMON_AUTO")
                try:
                    from data.db_core import save_p1_cache

                    save_p1_cache(today, base_items)
                except Exception as e:
                    logger.warning("save_p1_cache DuckDB 侧失败(可忽略): %s", e)
                logger.info("P1 重建完成：入池 %s 拒绝 %s → %s", len(base_items), len(rejected), out_path)
                ok_out = True
        except Exception as e:
            logger.warning("P1 重建主权预检异常，按原路径尝试落盘: %s", e)
            _save_base_items_json(out_path, base_items, p1_envelope_source="DAEMON_AUTO")
            try:
                from data.db_core import save_p1_cache

                save_p1_cache(today, base_items)
            except Exception as e2:
                logger.warning("save_p1_cache DuckDB 侧失败(可忽略): %s", e2)
            logger.info("P1 重建完成：入池 %s 拒绝 %s → %s", len(base_items), len(rejected), out_path)
            ok_out = True
    except Exception as e:
        logger.exception("P1 重建异常: %s", e)
        _notify_daemon_alert(
            "P1 重建过程异常",
            str(e)[:900],
            category="scan_p1",
            dedup_key=f"daemon_p1_exc_{_now_bj().strftime('%Y%m%d')}",
        )
        ok_out = False
    finally:
        for it in mock_raw:
            if isinstance(it, dict):
                it.pop("df", None)
        mock_raw.clear()
        try:
            del rejected
        except Exception:
            pass
        gc.collect()
    return ok_out


def _spawn(
    name: str,
    fn: Callable[[], None],
    *,
    wait_for_lock_sec: Optional[float] = None,
) -> None:
    """
    后台线程执行任务；主循环永不在此阻塞（仅 start 线程，瞬时返回）。

    wait_for_lock_sec=None：非阻塞抢锁。
    wait_for_lock_sec>0：子线程内限时阻塞等锁。
    这是全局串行闸门，避免晚间同步 / P1 / P5 / 快照 / 盘中扫描同时写 DuckDB。
    """

    def _runner() -> None:
        acquired = False
        try:
            if wait_for_lock_sec is None:
                got = _SCAN_BUSY.acquire(blocking=False)
            else:
                got = _SCAN_BUSY.acquire(timeout=float(wait_for_lock_sec))
            if not got:
                logger.warning("[%s] 未获取全局串行锁 _SCAN_BUSY（%s），跳过本次", name, wait_for_lock_sec)
                if name in ("EVENING_SYNC", "EVENING_P1", "EVENING_P5"):
                    _notify_daemon_alert(
                        "晚间链：未获得任务锁（已跳过）",
                        f"任务={name} 北京时间 {_now_bj().strftime('%Y-%m-%d %H:%M:%S')} 起 {float(wait_for_lock_sec):.0f}s 内未能占用同步锁，"
                        "本分钟晚间定时任务未执行。请确认无其它进程长时间占用 DuckDB（如 Streamlit 大数据操作），或查看 data/runtime/sniper.log。",
                        category="data_sync",
                        dedup_key=f"daemon_evening_lock_skip_{name}_{_now_bj().strftime('%Y%m%d%H%M')}",
                    )
                return
            acquired = True
            fn()
        except Exception as e:
            logger.exception("[%s] 任务异常(已记录): %s", name, e)
        except BaseException as e:
            logger.critical("[%s] 致命基类异常: %s", name, e, exc_info=True)
            raise
        finally:
            if acquired:
                try:
                    _SCAN_BUSY.release()
                except RuntimeError as re:
                    logger.error("[%s] 释放全局串行锁 _SCAN_BUSY 异常(忽略): %s", name, re)

    try:
        threading.Thread(target=_runner, name=name, daemon=True).start()
    except Exception as e:
        logger.exception("线程派生失败 %s: %s", name, e)


def _job_p3_tick() -> None:
    """P3：仅非阻塞抢锁；与 P4 尾盘窗互斥逻辑在 clock_aligned 层处理。"""
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过: P3巡逻")
        return
    _spawn("P3", _run_scan_push_p3, wait_for_lock_sec=None)


def _job_p3_tick_clock_aligned() -> None:
    """
    盘中降频：每 P3_POLL_INTERVAL_SECONDS 秒对齐才考虑 P3。
    容错铁律：自 14:31 起进入 P4 走廊则 **立即 return**（不与 P4 14:31/36/41/46/51 任一拍冲突）。
    主线程在此只做时间判断，**绝不** wait 锁；P3 子线程侧也仅非阻塞抢锁。
    """
    if _in_p4_tail_priority_window():
        logger.debug(
            "[P3] P4优先走廊 %02d:%02d–%02d:%02d 内跳过（核心重仓 14:40–14:55 含于内）",
            P4_TAIL_PRIORITY_START_MIN // 60,
            P4_TAIL_PRIORITY_START_MIN % 60,
            P4_TAIL_PRIORITY_END_MIN // 60,
            P4_TAIL_PRIORITY_END_MIN % 60,
        )
        return
    n = _now_bj()
    sec_of_day = int(n.hour) * 3600 + int(n.minute) * 60 + int(n.second)
    if sec_of_day % int(P3_POLL_INTERVAL_SECONDS) != 0:
        return
    _job_p3_tick()


def _job_p4_tick() -> None:
    # 尾盘最高优先级：P4 在尾盘窗内按节拍触发；子线程内最多等锁 900s
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过: P4尾盘")
        return
    _spawn("P4", _run_scan_push_p4, wait_for_lock_sec=P4_TAIL_WAIT_LOCK_SEC)


def _job_p4_tick_clock_aligned() -> None:
    """
    尾盘降频：每 P4_POLL_INTERVAL_SECONDS 秒对齐才考虑 P4。
    自 14:31 起进入 P4 走廊后才允许派生；其余时段直接 return。
    主线程只做时间判断，**绝不** wait 锁；P4 子线程侧仅按 _spawn 的非阻塞/限时等锁执行。
    """
    if not _in_p4_tail_priority_window():
        return
    n = _now_bj()
    sec_of_day = int(n.hour) * 3600 + int(n.minute) * 60 + int(n.second)
    if sec_of_day % int(P4_POLL_INTERVAL_SECONDS) != 0:
        return
    _job_p4_tick()


def _job_p5_tick() -> None:
    """
    保留：手工/扩展用；正常晚间 P5 由 20:05 _job_evening_p5 调度。
    """
    if not _daemon_is_trading_day_safe():
        logger.info("非交易日，跳过: P5盘后")
        return
    _spawn("P5", _run_scan_push_p5, wait_for_lock_sec=7200.0)


def _job_evening_sync_only() -> None:
    _spawn("EVENING_SYNC", _evening_sync_only_pipeline, wait_for_lock_sec=600.0)


def _job_evening_p1_rebuild() -> None:
    _spawn("EVENING_P1", _evening_p1_rebuild_pipeline, wait_for_lock_sec=600.0)


def _job_evening_p5() -> None:
    _spawn("EVENING_P5", _evening_p5_only_pipeline, wait_for_lock_sec=7200.0)


def _run_morning_0850_orchestration() -> None:
    """
    08:50 早盘编排入口：
    - 先清空企微防刷缓存，避免旧日状态影响当天早盘消息；
    - 再执行早盘补位链（同步 / 条件性补 P1）。

    这是唯一的 08:50 调度入口，便于后续维护时一眼看清两步动作的顺序与职责。
    """
    try:
        daily_morning_routine()
    except Exception as e:
        logger.exception("daily_morning_routine 未捕获异常: %s", e)
    try:
        _morning_trading_catchup()
    except Exception as e:
        logger.exception("_morning_trading_catchup 未捕获异常: %s", e)


def _run_intraday_snapshot_body(slot_id: str, label: str) -> None:
    """
    持 _SCAN_BUSY 时在子线程内执行；与扫描任务串行，避免 DuckDB 并发踩踏。
    说明：此为「分时多时点 VR/量能锚点」快照（slot_id 如 935…1440）。
    """
    if _maintenance_mode_skip(f"分时快照 capture_intraday_snapshots {label}"):
        return
    if _daemon_cruise_off_skip(f"分时快照{label}"):
        return
    if not _barrier_trading_day_or_skip(f"分时快照{label}"):
        return
    if _now_bj().weekday() >= 5:
        logger.info("分时快照跳过(周末): %s", label)
        return
    try:
        msg = capture_intraday_snapshots(slot_id=slot_id, force=True)
        logger.info("【分时快照】%s slot_id=%s — %s", label, slot_id, msg)
        if slot_id == "935":
            try:
                from core.p5_morning_validation import early_morning_p5_validation

                early_morning_p5_validation(log=logger)
            except Exception as e:
                logger.exception("P5 次日早盘验证异常: %s", e)
    except Exception as e:
        logger.exception("【分时快照】%s slot_id=%s 异常: %s", label, slot_id, e)


def _job_intraday_snapshot(slot_id: str, label: str) -> None:
    """分时快照：与 P3/P4/P5 同锁序列化；等待至多 30min 以免被长尾扫描误跳过。"""
    _spawn(
        f"SNAP_{slot_id}",
        lambda: _run_intraday_snapshot_body(slot_id, label),
        wait_for_lock_sec=1800.0,
    )


def _job_daily_morning_routine() -> None:
    """08:50：立即返回；`_run_morning_0850_orchestration` 在独立线程执行，保证 `schedule.run_pending` 毫秒级。"""

    def _runner() -> None:
        try:
            _run_morning_0850_orchestration()
        except Exception as e:
            logger.exception("08:50 早盘编排未捕获异常: %s", e)

    try:
        threading.Thread(target=_runner, name="daemon-morning-routine", daemon=True).start()
    except Exception as e:
        logger.exception("早安例行线程派生失败: %s", e)


def _weekly_alive_heartbeat() -> None:
    """
    每周一次「系统运行正常」心跳：
    - 仅交易日发送（防止周末/节假日误触）；
    - 通过 pipeline_state 持久化去重，跨重启仍保证每 ISO 周仅 1 条。
    """
    if _daemon_cruise_off_skip("每周运行心跳"):
        return
    if not _daemon_is_trading_day_safe():
        logger.info("每周心跳：非交易日，跳过")
        return

    now = _now_bj()
    week_key = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
    st = _read_pipeline_state()
    if str(st.get("last_weekly_alive_week", "")).strip() == week_key:
        logger.info("每周心跳：本周已发送 (%s)，跳过", week_key)
        return

    last_sync = str(st.get("last_sync_ok_bj_date", "") or "--")
    last_p1 = str(st.get("last_p1_ok_bj_date", "") or "--")
    detail = (
        f"时间：{now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"周标识：{week_key}\n"
        f"状态：守护进程运行正常\n"
        f"最近同步成功日：{last_sync}\n"
        f"最近P1成功日：{last_p1}"
    )
    _notify_daemon_alert(
        title="【每周心跳】系统运行正常",
        detail=detail,
        category="daemon",
        dedup_key=f"daemon_weekly_alive_{week_key}",
    )
    _write_pipeline_state_patch(last_weekly_alive_week=week_key, last_weekly_alive_at=now.isoformat())
    logger.info("每周心跳：已发送 (%s)", week_key)


def _job_weekly_alive_heartbeat() -> None:
    _spawn("WEEKLY_HEARTBEAT", _weekly_alive_heartbeat, wait_for_lock_sec=5.0)


def _weekly_db_maintenance_vacuum() -> None:
    """
    每周一次 DuckDB 维护（方案二）：投递独立进程 tools/weekly_db_maintenance_orchestrated.py。
    先设 maintenance_mode，再结束本守护与 Streamlit 占用，独占 CHECKPOINT+VACUUM；子进程不再拉起守护，
    由外层 start_daemon_24x7.bat 看门狗（进程退出后约 60s）自动重启，避免与脚本内 Popen 双重唤醒抢锁。
    避免进程内直接 VACUUM 与 UI 只读连接在 Windows 上争用同一 .duckdb 文件。

    - ISO 周去重、交易时段跳过仍在本函数判定；真正维护与企微通知在子进程内完成。
    """
    if _maintenance_mode_skip("每周数据库维护 CHECKPOINT+VACUUM"):
        return
    if _daemon_cruise_off_skip("每周数据库维护"):
        return

    now = _now_bj()
    week_key = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
    st = _read_pipeline_state()
    if str(st.get("last_weekly_vacuum_week", "")).strip() == week_key:
        logger.info("每周数据库维护：本周已执行 (%s)，跳过", week_key)
        return

    curr_min = now.hour * 60 + now.minute
    # 交易窗口保护（09:00~15:30），即便调度误触发也不在盘中维护。
    if (9 * 60) <= curr_min <= (15 * 60 + 30):
        logger.info("每周数据库维护：当前处于交易时段，跳过")
        return

    script = os.path.join(_PROJECT_ROOT, "tools", "weekly_db_maintenance_orchestrated.py")
    if not os.path.isfile(script):
        logger.error("每周数据库维护：未找到编排脚本 %s", script)
        return

    try:
        popen_kw: Dict[str, Any] = {
            "cwd": _PROJECT_ROOT,
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if sys.platform == "win32":
            # 独立进程组，避免维护子进程随守护退出而被误杀
            popen_kw["creationflags"] = int(subprocess.DETACHED_PROCESS) | int(
                subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            popen_kw["start_new_session"] = True
        subprocess.Popen(
            [sys.executable, script, "--no-pause"],
            **popen_kw,
        )
        logger.info(
            "每周数据库维护：已投递独立编排进程（方案二：释放守护/UI 后 VACUUM；守护重启依赖 7x24 外壳看门狗）| week=%s | 详见 data/runtime/weekly_maintenance.log",
            week_key,
        )
    except Exception as e:
        logger.exception("每周数据库维护：投递编排进程失败: %s", e)
        _notify_daemon_alert(
            title="【每周数据库维护】投递失败",
            detail=(
                f"时间：{now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"周标识：{week_key}\n"
                f"说明：无法启动 weekly_db_maintenance_orchestrated.py\n"
                f"异常：{str(e)[:900]}"
            ),
            category="data_sync",
            dedup_key=f"daemon_weekly_vacuum_spawn_fail_{week_key}",
        )


def _job_weekly_db_maintenance_vacuum() -> None:
    _spawn("WEEKLY_DB_MAINT", _weekly_db_maintenance_vacuum, wait_for_lock_sec=60.0)


def _init_async_scan_queue_consumer() -> None:
    """
    独占 pending_queue_consumer.filelock，由本进程消费 data/runtime/scan_async/pending.json。
    替代已删除的 service/scheduler.py 中的队列循环；与 Streamlit 内嵌 ScanAsyncWorker 仍互斥（二选一）。
    """
    global _async_scan_queue_lock_ok
    try:
        from service.async_scan_bridge import try_acquire_scheduler_queue_consumer_filelock

        _async_scan_queue_lock_ok = bool(try_acquire_scheduler_queue_consumer_filelock())
        if _async_scan_queue_lock_ok:
            logger.info("scan_async: 本进程已占用 pending 队列消费锁")
        else:
            logger.warning(
                "scan_async: 未获得 pending 队列锁（他进程/UI 已占用），本进程不消费异步扫描队列"
            )
    except Exception as e:
        logger.warning("scan_async 队列锁初始化失败: %s", e)
        _async_scan_queue_lock_ok = False


def _tick_async_scan_queue() -> None:
    """
    主循环轮询异步 pending 队列（约每 _ASYNC_QUEUE_POLL_TICKS 秒一次）。
    纯 Headless 7x24 守护：无 pending 时 ``process_one_pending_scan_job`` 静默 return，
    不在此处打 info，避免与 UI 已剥离后仍产生海量空轮询日志。
    """
    if _maintenance_mode_skip("异步扫描队列 process_one_pending_scan_job"):
        return
    if not _async_scan_queue_lock_ok:
        return
    try:
        from service.async_scan_bridge import process_one_pending_scan_job

        process_one_pending_scan_job()
    except Exception as e:
        logger.debug("async_scan 队列处理: %s", e)


def _register_bj_daily(hhmm: str, fn: Callable[[], None], label: str) -> None:
    """注册北京时间日任务（分钟粒度，忽略系统本地时区）。"""
    t = str(hhmm or "").strip()
    if len(t) != 5 or t[2] != ":":
        raise ValueError(f"非法 HH:MM: {hhmm!r}")
    try:
        h = int(t[:2])
        m = int(t[3:5])
    except Exception as e:
        raise ValueError(f"非法 HH:MM: {hhmm!r}") from e
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"非法 HH:MM: {hhmm!r}")
    with _BJ_SCHEDULE_LOCK:
        _BJ_DAILY_JOBS.append((t, fn, label))


def _tick_bj_daily_scheduler() -> None:
    """每秒轮询一次；命中北京时间 HH:MM 时仅触发一轮对应任务。"""
    global _BJ_LAST_TICK_MINUTE_KEY
    now = _now_bj()
    minute_key = now.strftime("%Y%m%d%H%M")
    if minute_key == _BJ_LAST_TICK_MINUTE_KEY:
        return
    _BJ_LAST_TICK_MINUTE_KEY = minute_key
    now_hhmm = now.strftime("%H:%M")
    with _BJ_SCHEDULE_LOCK:
        due_jobs = [(fn, label) for hhmm, fn, label in _BJ_DAILY_JOBS if hhmm == now_hhmm]
    for fn, label in due_jobs:
        try:
            fn()
        except Exception as e:
            logger.exception("BJ定时任务触发异常 [%s @ %s]: %s", label, now_hhmm, e)


def register_schedules() -> None:
    # 北京时间任务分发器（与系统本地时区解耦）：每秒 tick，内部按北京时间 HH:MM 触发。
    schedule.every(1).seconds.do(_tick_bj_daily_scheduler)
    # P2：竞价日各 1 次（09:26，与 09:18 早盘合并简报错峰）；与 UI 按钮「2档·竞价突袭」同源引擎
    _register_bj_daily("09:26", _job_p2_tick, "P2竞价")
    # P3 / P4：每秒仅跑时钟对齐函数（O(1)）；真正扫描各自每 150 秒一拍
    schedule.every(1).seconds.do(_job_p3_tick_clock_aligned)
    schedule.every(1).seconds.do(_job_p4_tick_clock_aligned)
    # 【时序咬合】19:45 仅日线增量（可跳过）→ 19:55 P1+推送 → 20:05 P5
    _register_bj_daily("19:45", _job_evening_sync_only, "晚间增量同步")
    _register_bj_daily("19:55", _job_evening_p1_rebuild, "晚间P1")
    _register_bj_daily("20:05", _job_evening_p5, "晚间P5")
    # 08:50：统一编排入口——先清企微防刷缓存，再执行早盘补位链（同步 / 条件性补 P1）
    _register_bj_daily("08:50", _job_daily_morning_routine, "早盘编排入口")
    # 09:18：交易日早盘合并简报企微一条（预检通过后发送），独立线程
    _register_bj_daily("09:18", _job_daily_open_heartbeat_routine, "早盘合并简报")
    # 每周心跳：固定 09:20 触发；内部按 ISO 周去重，跨重启仍每周仅发一次。
    _register_bj_daily("09:20", _job_weekly_alive_heartbeat, "每周心跳")
    # 每周数据库维护：固定 03:30 触发；内部按 ISO 周去重，且只做 CHECKPOINT+VACUUM（不删业务数据）。
    _register_bj_daily("03:30", _job_weekly_db_maintenance_vacuum, "每周库维护")
    # 分时六槽快照；slot_id=1440 提前至 14:39，错峰 14:40 密集扫描
    for hhmm, sid, lbl in (
        ("09:35", "935", "09:35"),
        ("10:30", "1030", "10:30"),
        ("11:25", "1125", "11:25"),
        ("13:25", "1325", "13:25"),
        ("14:25", "1425", "14:25"),
        ("14:39", "1440", "14:39→1440错峰"),
    ):
        _register_bj_daily(
            hhmm,
            lambda sid=sid, lbl=lbl: _job_intraday_snapshot(sid, lbl),
            f"分时快照{lbl}",
        )


def main() -> None:
    if not _acquire_single_instance_lock():
        logger.error("检测到已有 auto_sniper_daemon 进程在运行，当前实例退出以避免双实例抢锁")
        return
    logger.info("auto_sniper_daemon 启动 | 根目录=%s | 时区=Asia/Shanghai", _PROJECT_ROOT)
    # 调度触发统一按 _now_bj() 判定；即使系统时区错误也按北京时间执行。
    try:
        # 统一以北京时间口径记录启动时钟，避免宿主机时区为 UTC 时把日志误读成“23:10”之类的本地错觉。
        _bj = datetime.now(BJ_TZ)
        _utc_aw = datetime.now().astimezone()
        skew_sec = abs(_bj.timestamp() - _utc_aw.timestamp())
        logger.info(
            "时钟自检：北京时间=%s | 系统本地(aware)=%s | |Δepoch|=%.3fs",
            _bj.strftime("%Y-%m-%d %H:%M:%S %Z"),
            _utc_aw.strftime("%Y-%m-%d %H:%M:%S %z"),
            skew_sec,
        )
        if skew_sec >= 3600.0:
            logger.warning(
                "检测到系统时间与标准时偏差较大（|BJ_epoch−本地_epoch|≥3600s）；本守护调度仍按 Asia/Shanghai 触发，"
                "请检查 BIOS/系统时间或 NTP；若仅时区显示不同而本行 Δepoch 接近 0，可忽略。"
            )
    except Exception as e:
        logger.debug("时钟自检跳过: %s", e)
    try:
        import data.data_fetcher  # noqa: F401
    except Exception as e:
        logger.warning("data_fetcher 预热失败: %s", e)

    _apply_daemon_resource_profile()
    _init_async_scan_queue_consumer()
    register_schedules()
    _write_daemon_public_meta()
    _maybe_open_heartbeat_on_startup()
    _cleanup_runtime_caches()
    logger.info(
        "已注册：08:50清企微防刷+早盘补位 | 09:18早盘合并简报企微(独立线程,预检) | 09:26 P2竞价 | 分时快照六槽(14:39=slot1440 错峰) | "
        "P3×%ss降频(14:31起停派让路P4) | 每周心跳 09:20(每周1条) | 每周库维护 03:30(独立编排VACUUM,见weekly_maintenance.log) | "
        "P4×%ss降频(14:31起停派让路P3) | P4每拍等锁%ss | 重仓核心窗 %02d:%02d~%02d:%02d | "
        "19:45晚间增量 | 19:55晚间P1 | 20:05晚间P5 | 09:35 P5早盘验证 | 非交易日跳过", 
        P3_POLL_INTERVAL_SECONDS,
        P4_POLL_INTERVAL_SECONDS,
        int(P4_TAIL_WAIT_LOCK_SEC),
        P4_CORE_TAIL_START_MIN // 60,
        P4_CORE_TAIL_START_MIN % 60,
        P4_TAIL_PRIORITY_END_MIN // 60,
        P4_TAIL_PRIORITY_END_MIN % 60,
    )
    if _async_scan_queue_lock_ok:
        logger.info(
            "scan_async: 主循环将每 %ss 轮询 process_one_pending_scan_job",
            _ASYNC_QUEUE_POLL_TICKS,
        )
    _async_poll_i = 0
    gc_tick_counter = 0
    global _MAINT_LOOP_LOG_LAST_MONO
    while True:
        # 维护锁优先：在 schedule.run_pending() 之前短路，避免定时任务与运维 VACUUM/离线压库争用 DuckDB。
        try:
            from core.master_control import is_maintenance_mode_enabled

            _maint_on = bool(is_maintenance_mode_enabled())
        except Exception as e:
            logger.debug("主循环维护模式检测异常(忽略): %s", e)
            _maint_on = False
        if _maint_on:
            _now_mono = time.monotonic()
            if _MAINT_LOOP_LOG_LAST_MONO is None or (_now_mono - _MAINT_LOOP_LOG_LAST_MONO) >= 300.0:
                _MAINT_LOOP_LOG_LAST_MONO = _now_mono
                logger.error(
                    "🚨 [维护锁] 全局 maintenance_mode=ON，主循环休眠 10s 等待运维释放（本周期不执行 schedule.run_pending）；"
                    "后续同状态每 5 分钟记一条，避免日志刷屏"
                )
            time.sleep(10.0)
            continue
        _MAINT_LOOP_LOG_LAST_MONO = None
        _cleanup_runtime_caches()

        try:
            schedule.run_pending()
        except Exception as e:
            logger.exception("schedule.run_pending 异常(继续运行): %s", e)
        _async_poll_i += 1
        if _async_poll_i >= _ASYNC_QUEUE_POLL_TICKS:
            _async_poll_i = 0
            # 与 run_pending 同级兜底：未来若 _tick_async_scan_queue 内部漏捕异常，仍不得拖死 7x24 主循环
            try:
                _tick_async_scan_queue()
            except Exception as e:
                logger.exception("async_scan 队列轮询外层异常(继续运行): %s", e)
        gc_tick_counter += 1
        if gc_tick_counter >= 3600:
            # 主动触发垃圾回收。防范 Pandas 矩阵运算在 7x24 连续运转下产生的底层 C 句柄内存碎片堆积（GC 疲劳）
            gc.collect()
            gc_tick_counter = 0
        time.sleep(1.0)


if __name__ == "__main__":
    main()

#
# =============================================================================
# 交钥匙：pm2 长期部署 + 日志切割（Linux / 家用小主机）
# =============================================================================
#
# --- 环境与目录 ---
#   sudo timedatectl set-timezone Asia/Shanghai
#   cd /path/to/xiaozhu
#   pip3 install -r requirements.txt -r requirements-daemon.txt   # 系统 Python；至少含 schedule 等
#
# --- pm2 启动（推荐 ecosystem 文件，便于合并日志与轮转参数）---
#   新建 ecosystem.sniper.config.cjs 内容示例：
#   ---------------------------------------------------------------------------
#   module.exports = {
#     apps: [{
#       name: 'xiaozhu-sniper',
#       script: 'auto_sniper_daemon.py',
#       interpreter: '/usr/bin/python3',   # 或 `which python3` 的绝对路径
#       cwd: '/path/to/xiaozhu',
#       instances: 1,
#       autorestart: true,
#       max_restarts: 50,
#       min_uptime: '10s',
#       error_file: '/path/to/xiaozhu/logs/sniper-err.log',
#       out_file: '/path/to/xiaozhu/logs/sniper-out.log',
#       merge_logs: true,
#       time: true,
#     }]
#   };
#   ---------------------------------------------------------------------------
#   mkdir -p logs
#   pm2 start ecosystem.sniper.config.cjs
#   pm2 save
#   pm2 startup systemd -u $USER --hp $HOME   # 按提示执行一条 sudo 命令
#
# --- 日志切割（二选一）---
#   (A) pm2 模块 logrotate（简单）:
#       pm2 install pm2-logrotate
#       pm2 set pm2-logrotate:max_size 50M
#       pm2 set pm2-logrotate:retain 14
#       pm2 set pm2-logrotate:compress true
#
#   (B) 系统 logrotate（示例 /etc/logrotate.d/xiaozhu-sniper）:
#       /path/to/xiaozhu/logs/*.log {
#         daily
#         rotate 14
#         compress
#         missingok
#         notifempty
#         copytruncate
#       }
#
# --- 运维命令 ---
#   pm2 status
#   pm2 logs xiaozhu-sniper --lines 200
#   pm2 restart xiaozhu-sniper
#   pm2 stop xiaozhu-sniper
#
# =============================================================================
