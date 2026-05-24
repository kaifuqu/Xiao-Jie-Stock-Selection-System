# -*- coding: utf-8 -*-
"""
P3 / P4 异步扫描桥接（指挥舱 UI ↔ auto_sniper_daemon 主循环 / Streamlit 内嵌 worker）

- UI 仅写入 pending.json 与状态；耗时 run_scan_engine 在 auto_sniper_daemon 进程轮询 process_one_pending_scan_job 或 UI 内嵌线程中执行。
- 结果写入 latest_result.json，UI 通过轮询 status.json 消费，避免 st.spinner 长时间阻塞主线程。
- 企微 notify_scan_results_top3_p2p3p4 与 logging 进度回调在后台任务内调用，与同步 execute_scan 语义对齐。
- 队列消费互斥：filelock.FileLock(path_scan_async_queue_consumer_filelock)，OS 级锁；守护进程与 Streamlit 内嵌 worker 二选一，进程异常退出后锁自动释放。
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import pickle
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from core.file_utils import is_safe_pickle_path

from filelock import FileLock, Timeout

BJ_TZ = timezone(timedelta(hours=8))

_worker_started = False
_worker_lock = threading.Lock()

# Streamlit 进程内成功抢到 OS 文件锁后常驻持有，直至进程退出（atexit release）
_ui_queue_consumer_filelock: Optional[FileLock] = None
_ui_queue_lock_atexit_registered = False

# ---------------------------------------------------------------------------
# 部署开关：避免 Streamlit 内嵌 ScanAsyncWorker 与 auto_sniper_daemon 双消费 pending.json
# 互斥实现：filelock.FileLock(path_scan_async_queue_consumer_filelock)，进程死亡锁自动释放。
# 优先级：环境变量 XIAOJIE_EMBED_UI_SCAN_WORKER > config.yaml scan_async.embed_worker_in_streamlit
#         auto：瞬时 try acquire；失败则说明守护进程（或其它进程）已占用 → 不嵌入 UI
# ---------------------------------------------------------------------------
_CONFIG_ROOT_CACHE: Optional[str] = None


def _project_root_for_config() -> str:
    global _CONFIG_ROOT_CACHE
    if _CONFIG_ROOT_CACHE is None:
        try:
            from core.runtime_data_paths import project_root

            _CONFIG_ROOT_CACHE = project_root()
        except Exception:
            _CONFIG_ROOT_CACHE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return _CONFIG_ROOT_CACHE


def _load_yaml_scan_async_embed_mode() -> str:
    """
    返回 'true' | 'false' | 'auto' 小写字符串（缺省 auto：无外部守护进程锁则嵌入 UI）。
    """
    cfg_path = os.path.join(_project_root_for_config(), "config.yaml")
    try:
        import yaml

        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        sa = cfg.get("scan_async") or {}
        raw = sa.get("embed_worker_in_streamlit", "auto")
        if isinstance(raw, bool):
            return "true" if raw else "false"
        s = str(raw).strip().lower()
        if s in ("auto", "detect", "scheduler"):
            return "auto"
        if s in ("0", "false", "no", "off", "external", "none"):
            return "false"
        if s in ("1", "true", "yes", "on", "ui", "embedded"):
            return "true"
        return "auto"
    except Exception:
        return "auto"


def _env_embed_ui_scan_worker_override() -> Optional[str]:
    """
    若环境变量已显式设置，返回 'true' | 'false' | 'auto'；未设置返回 None。
    """
    raw = os.environ.get("XIAOJIE_EMBED_UI_SCAN_WORKER")
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s in ("", "inherit", "config"):
        return None
    if s in ("0", "false", "no", "off", "external", "scheduler"):
        return "false"
    if s in ("1", "true", "yes", "on", "ui", "embedded"):
        return "true"
    if s in ("auto", "detect"):
        return "auto"
    return None


def _path_queue_consumer_filelock() -> str:
    from core.runtime_data_paths import path_scan_async_queue_consumer_filelock, ensure_runtime_data_layout

    ensure_runtime_data_layout()
    return path_scan_async_queue_consumer_filelock()


def probe_queue_consumer_lock_available() -> bool:
    """
    auto 模式探测：若能瞬时获得 OS 文件锁则说明当前无其它进程占用，探测结束后立即释放。
    """
    p = _path_queue_consumer_filelock()
    fl = FileLock(p)
    try:
        fl.acquire(timeout=0)
    except Timeout:
        return False
    try:
        fl.release()
    except Exception:
        pass
    return True


def should_embed_ui_scan_worker() -> bool:
    """
    True  → Streamlit 可启动内嵌 ScanAsyncWorker（单进程部署）。
    False → 必须由 auto_sniper_daemon.py（或其它独占进程）调用 process_one_pending_scan_job。
    """
    env_m = _env_embed_ui_scan_worker_override()
    if env_m is not None:
        mode = env_m
    else:
        mode = _load_yaml_scan_async_embed_mode()

    if mode == "false":
        return False
    if mode == "true":
        return True
    if not probe_queue_consumer_lock_available():
        logging.info(
            "scan_async: embed_worker_in_streamlit=auto → OS 队列锁已被占用，禁用 UI 内嵌消费者",
        )
        return False
    return True


def _release_ui_queue_consumer_filelock_at_exit() -> None:
    global _ui_queue_consumer_filelock
    try:
        if _ui_queue_consumer_filelock is not None:
            _ui_queue_consumer_filelock.release()
            _ui_queue_consumer_filelock = None
    except Exception:
        pass


_scheduler_filelock_holder: Optional[FileLock] = None
_scheduler_atexit_registered = False
_scheduler_queue_lock_acquired: bool = False


def _release_scheduler_queue_filelock_at_exit() -> None:
    global _scheduler_filelock_holder, _scheduler_queue_lock_acquired
    try:
        if _scheduler_filelock_holder is not None:
            _scheduler_filelock_holder.release()
            _scheduler_filelock_holder = None
    except Exception:
        pass
    _scheduler_queue_lock_acquired = False


def try_acquire_scheduler_queue_consumer_filelock() -> bool:
    """
    非阻塞获取 pending 队列 OS 互斥锁；成功则在本进程内保持持有直至退出。
    失败返回 False（例如 Streamlit 已嵌入消费者），调用方应跳过 process_one_pending_scan_job。
    """
    global _scheduler_filelock_holder, _scheduler_atexit_registered, _scheduler_queue_lock_acquired
    p = _path_queue_consumer_filelock()
    fl = FileLock(p)
    try:
        fl.acquire(timeout=0)
    except Timeout:
        logging.info("scan_async: 非阻塞获取 pending 队列锁失败（他进程/UI 已占用）: %s", p)
        return False
    _scheduler_filelock_holder = fl
    _scheduler_queue_lock_acquired = True
    logging.info("scan_async: 已占用 pending 队列互斥锁（非阻塞成功，与 UI 内嵌 worker 互斥）")
    if not _scheduler_atexit_registered:
        atexit.register(_release_scheduler_queue_filelock_at_exit)
        _scheduler_atexit_registered = True
    return True


def acquire_scheduler_queue_consumer_filelock_blocking() -> None:
    """
    阻塞直到获得 pending 队列 OS 互斥锁（旧版入口）；进程被 kill / 蓝屏时内核释放锁。
    新部署推荐 auto_sniper_daemon 使用 try_acquire_scheduler_queue_consumer_filelock()。
    """
    global _scheduler_filelock_holder, _scheduler_atexit_registered, _scheduler_queue_lock_acquired
    p = _path_queue_consumer_filelock()
    fl = FileLock(p)
    logging.info("scan_async: 正在等待 OS 级 pending 队列互斥锁: %s", p)
    fl.acquire()
    _scheduler_filelock_holder = fl
    _scheduler_queue_lock_acquired = True
    logging.info("scan_async: 已占用 pending 队列互斥锁（与 Streamlit 内嵌 worker 互斥）")
    if not _scheduler_atexit_registered:
        atexit.register(_release_scheduler_queue_filelock_at_exit)
        _scheduler_atexit_registered = True


def scheduler_holds_queue_consumer_filelock() -> bool:
    """供诊断：当前进程是否已成功占用队列 consumer 锁（历史函数名保留）。"""
    return bool(_scheduler_queue_lock_acquired)


def register_scheduler_consumer_lock() -> None:
    """兼容旧名：等价于 acquire_scheduler_queue_consumer_filelock_blocking()。"""
    acquire_scheduler_queue_consumer_filelock_blocking()


def get_scan_async_queue_probe_status() -> Dict[str, Any]:
    """
    指挥舱探针：队列消费侧 filelock 与 UI worker 状态。
    若本进程已持有锁，不再调用 probe（同进程二次 flock 探测不可靠），直接标为 ui 独占。
    """
    p = _path_queue_consumer_filelock()
    ui_holds = _ui_queue_consumer_filelock is not None
    if ui_holds:
        ext_busy = False
        summary = "本进程(UI)已占用 filelock"
    else:
        ext_busy = not probe_queue_consumer_lock_available()
        summary = "外部进程已占用 filelock" if ext_busy else "filelock 空闲（无独占持有者）"
    return {
        "queue_filelock_path": p,
        "external_process_holds_queue_lock": ext_busy,
        "ui_holds_queue_filelock": ui_holds,
        "summary": summary,
        "ui_embed_worker_started": bool(_worker_started),
        "should_embed_intent": should_embed_ui_scan_worker(),
    }


def _atomic_write_json(path: str, obj: Any) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=0)
    os.replace(tmp, path)


def read_scan_async_status() -> Dict[str, Any]:
    from core.runtime_data_paths import path_scan_async_status_json

    p = path_scan_async_status_json()
    if not os.path.isfile(p):
        return {
            "state": "idle",
            "job_id": None,
            "message": "",
            "error": None,
            "updated_at": None,
        }
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "state": "idle",
            "job_id": None,
            "message": "status_corrupt",
            "error": "status_corrupt",
            "updated_at": None,
        }


def write_scan_async_status(
    state: str,
    job_id: Optional[str],
    message: str = "",
    error: Optional[str] = None,
) -> None:
    from core.runtime_data_paths import path_scan_async_status_json

    payload = {
        "state": state,
        "job_id": job_id,
        "message": message,
        "error": error,
        "updated_at": datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }
    _atomic_write_json(path_scan_async_status_json(), payload)


def _recover_stale_running(max_age_sec: float = 900.0) -> None:
    from core.runtime_data_paths import path_scan_async_running_json

    rp = path_scan_async_running_json()
    if not os.path.isfile(rp):
        return
    try:
        if time.time() - os.path.getmtime(rp) > max_age_sec:
            os.remove(rp)
            logging.warning("async_scan_bridge: 已清理超时 running.json（可能上次进程异常退出）")
    except OSError as e:
        logging.debug("async_scan_bridge: 清理 running 跳过: %s", e)


def load_dehydrated_base_items_from_disk(anchor: str, pool_mode: str) -> List[Dict[str, Any]]:
    """
    仅从池缓存 JSON 读取 code / p1_score / hist，不展开 df_split，降低后台进程峰值内存；
    再水化由 rehydrate_base_items_for_scan_engine 负责。
    """
    from core.runtime_data_paths import (
        path_p0_cache_json,
        path_p0_cache_pkl,
        path_p1_cache_json,
        path_p1_cache_pkl,
    )

    anchor = str(anchor or "").strip()
    pm = str(pool_mode or "P1").upper()
    if pm == "P0":
        json_path, pkl_path = path_p0_cache_json(anchor), path_p0_cache_pkl(anchor)
    else:
        json_path, pkl_path = path_p1_cache_json(anchor), path_p1_cache_pkl(anchor)

    if os.path.isfile(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception as e:
            logging.warning("async_scan_bridge: 读取底仓 JSON 失败 %s: %s", json_path, e)
            raw = []
        if isinstance(raw, dict) and isinstance(raw.get("items"), list):
            rows = raw["items"]
        elif isinstance(raw, list):
            rows = raw
        else:
            rows = []
        if isinstance(rows, list) and rows:
            out: List[Dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                hist = row.get("hist")
                if not isinstance(hist, dict):
                    hist = {}
                code = row.get("code")
                if not code:
                    continue
                out.append(
                    {
                        "code": code,
                        "p1_score": float(row.get("p1_score", 0) or 0),
                        "hist": hist,
                    }
                )
            if out:
                return out

    if os.path.isfile(pkl_path):
        if not is_safe_pickle_path(pkl_path):
            logging.warning("async_scan_bridge: 拒绝读取非白名单 pickle 路径: %s", pkl_path)
            return []
        try:
            with open(pkl_path, "rb") as f:
                data = pickle.load(f)
        except Exception as e:
            logging.warning("async_scan_bridge: 读取底仓 pickle 失败: %s", e)
            return []
        if isinstance(data, list):
            try:
                from ui.session_cache_dehydrate import dehydrate_base_items_list

                return dehydrate_base_items_list(data)
            except Exception as e:
                logging.warning("async_scan_bridge: pickle 脱水失败: %s", e)
    return []


def submit_p3_p4_scan_job(
    target_pools: List[str],
    regime: str,
    pool_mode: str,
    anchor_yyyymmdd: str,
    wechat_notify: bool,
) -> Optional[str]:
    """
    【V26.6 架构废弃/封印】当前生产环境已剥离内嵌 Worker，调用此函数将触发异常拦截。

    提交仅含 p3/p4 的扫描任务。若已有 running.json 则返回 None（避免并发双跑）。
    返回 job_id 供 UI 与 status / result 对齐。
    """
    assert int(os.environ.get("XIAOJIE_EMBED_UI_SCAN_WORKER", 0)) == 1, (
        "❌ 系统架构规范：禁止在当前主进程/UI中内嵌启动异步 Worker，请遵守单例守护进程规范！"
    )
    raw = [str(p).strip().lower() for p in (target_pools or []) if p]
    if any(x not in ("p3", "p4") for x in raw):
        return None
    pools = [x for x in raw if x in ("p3", "p4")]
    if not pools:
        return None

    from core.runtime_data_paths import (
        path_scan_async_pending_json,
        path_scan_async_running_json,
    )

    _recover_stale_running()
    if os.path.isfile(path_scan_async_running_json()):
        logging.warning("async_scan_bridge: 已有任务执行中(running.json)，跳过提交")
        return None

    job_id = str(uuid.uuid4())
    job: Dict[str, Any] = {
        "job_id": job_id,
        "target_pools": pools,
        "regime": str(regime or ""),
        "pool_mode": str(pool_mode or "P1"),
        "anchor_yyyymmdd": str(anchor_yyyymmdd or "").strip(),
        "wechat_notify": bool(wechat_notify),
        "submitted_at": datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }
    _atomic_write_json(path_scan_async_pending_json(), job)
    write_scan_async_status("queued", job_id, message="已入队，等待后台执行")
    logging.info("async_scan_bridge: 已提交 job_id=%s pools=%s", job_id, pools)
    return job_id


def _dehydrate_engine_output_for_json(
    _eng_res: Dict[str, Any],
    target_pools: List[str],
) -> Dict[str, Any]:
    from ui.session_cache_dehydrate import (
        dehydrate_scan_nested_fragment,
        dehydrate_scan_results_list,
    )

    pools_out: Dict[str, Any] = {}
    for k in target_pools:
        pools_out[k] = dehydrate_scan_results_list(_eng_res.get(k, []))
    return {
        "pools": pools_out,
        "danger_buy": dehydrate_scan_results_list(_eng_res.get("danger_buy", [])),
        "danger_sell": dehydrate_scan_results_list(_eng_res.get("danger_sell", [])),
        "funnel": dehydrate_scan_nested_fragment(_eng_res.get("funnel", {})),
        "observation": dehydrate_scan_nested_fragment(_eng_res.get("observation") or {}),
        "adaptive_reason": str(_eng_res.get("adaptive_reason", "") or ""),
        "adaptive_sample_count": int(_eng_res.get("adaptive_sample_count", 0) or 0),
        "market_contraction_score": float(_eng_res.get("market_contraction_score", 0.0) or 0.0),
        "sop_market_breaker": dehydrate_scan_nested_fragment(_eng_res.get("sop_market_breaker") or {}),
    }


def _run_engine_for_job(job: Dict[str, Any]) -> Dict[str, Any]:
    target_pools = list(job.get("target_pools") or [])
    regime = str(job.get("regime") or "震荡市")
    pool_mode = str(job.get("pool_mode") or "P1")
    anchor = str(job.get("anchor_yyyymmdd") or "").strip()
    wechat = bool(job.get("wechat_notify"))
    job_id = str(job.get("job_id") or "")

    try:
        from data.db_core import probe_duckdb_lock

        lock_state = probe_duckdb_lock()
        if not lock_state.get("ok", False):
            pid = lock_state.get("pid")
            msg = lock_state.get("msg", "未知锁冲突")
            raise RuntimeError(
                f"DuckDB 不可用（PID={pid}）：{msg}" if pid else f"DuckDB 不可用：{msg}"
            )
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"启动前锁检测异常: {e}") from e

    if "p4" in target_pools:
        try:
            from core.sop_v11 import evaluate_market_circuit_breaker, load_sop_v11_config

            brk = evaluate_market_circuit_breaker()
            cb_cfg = load_sop_v11_config().get("circuit_breaker") or {}
            if brk.get("active") and bool(cb_cfg.get("enforce_block_p4")):
                raise RuntimeError(
                    f"SOP 指数防空洞已触发，已按配置阻止本次 4档 扫描：{brk.get('message', '')}"
                )
        except RuntimeError:
            raise
        except Exception as e:
            logging.debug("async_scan_bridge: SOP 预检异常（不拦截）: %s", e)

    base_light = load_dehydrated_base_items_from_disk(anchor, pool_mode)
    if not base_light:
        raise RuntimeError("底仓缓存为空：请先执行 1 档/直通车洗盘并确认当日池缓存已落盘")

    from ui.session_cache_dehydrate import rehydrate_base_items_for_scan_engine

    base_for_scan = rehydrate_base_items_for_scan_engine(base_light)
    if not base_for_scan:
        raise RuntimeError("再水化底仓失败：请检查数据库与网络")

    from core.scan_engine import run_scan_engine, get_realtime_sector_ranking

    def _progress_cb(msg: Any) -> None:
        # 纯守护 7x24：进度回调极高频，用 debug 避免 sniper.log 被扫描进度淹没（非「业务完成」事件）
        logging.debug("[async_scan] %s", str(msg)[:900])

    _eng_res = run_scan_engine(
        target_pools=target_pools,
        base_items=base_for_scan,
        regime=regime,
        progress_callback=_progress_cb,
    )

    scan_results_update: Dict[str, Any] = {k: _eng_res.get(k, []) for k in target_pools}
    scan_results_update["danger_buy"] = _eng_res.get("danger_buy", [])
    scan_results_update["danger_sell"] = _eng_res.get("danger_sell", [])
    scan_results_update["funnel"] = _eng_res.get("funnel", {})
    scan_results_update["observation"] = _eng_res.get("observation") or {}

    try:
        from core.notification_gateway import notify_scan_results_top3_p2p3p4

        notify_scan_results_top3_p2p3p4(
            target_pools,
            scan_results_update,
            wechat,
        )
    except Exception:
        logging.debug("async_scan_bridge: 企微推送旁路异常", exc_info=True)

    sector_rank: Dict[str, Any] = {}
    try:
        raw_sr = get_realtime_sector_ranking() or {}
        for _k, _v in raw_sr.items():
            try:
                fv = float(_v)
                if fv == fv and fv != float("inf") and fv != float("-inf"):
                    sector_rank[str(_k)] = fv
            except (TypeError, ValueError):
                continue
    except Exception as e:
        logging.warning("async_scan_bridge: sector_rank 拉取失败: %s", e)

    dehydrated = _dehydrate_engine_output_for_json(_eng_res, target_pools)

    return {
        "job_id": job_id,
        "target_pools": target_pools,
        "completed_at": datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "dehydrated": dehydrated,
        "sector_rank": sector_rank,
    }


def process_one_pending_scan_job() -> bool:
    """
    尝试将 pending.json 原子迁移为 running.json 并执行一轮扫描。
    返回 True 表示本 tick 处理了一条队列（含失败落盘）。

    纯 Headless 守护轮询：无 pending / 有 running 占用 / replace 失败等「空队列」路径一律静默
    return，禁止在此处使用 info 刷屏（进度条回调已降级为 debug，见 _run_engine_for_job）。
    """
    from core.runtime_data_paths import ensure_runtime_data_layout

    ensure_runtime_data_layout()
    from core.runtime_data_paths import (
        path_scan_async_latest_result_json,
        path_scan_async_pending_json,
        path_scan_async_running_json,
    )

    pending = path_scan_async_pending_json()
    running = path_scan_async_running_json()
    if not os.path.isfile(pending):
        return False
    if os.path.isfile(running):
        return False
    try:
        os.replace(pending, running)
    except OSError:
        return False

    job: Dict[str, Any] = {}
    try:
        with open(running, "r", encoding="utf-8") as f:
            job = json.load(f)
    except Exception as e:
        logging.error("async_scan_bridge: 读取 running 任务失败: %s", e)
        try:
            os.remove(running)
        except OSError:
            pass
        write_scan_async_status("error", None, message="任务文件损坏", error=str(e))
        return True

    jid = job.get("job_id")
    write_scan_async_status("running", str(jid) if jid else None, message="扫描执行中")
    try:
        payload = _run_engine_for_job(job)
        _atomic_write_json(path_scan_async_latest_result_json(), payload)
        write_scan_async_status("done", str(jid) if jid else None, message="完成", error=None)
        logging.info("async_scan_bridge: job_id=%s 扫描完成", jid)
    except Exception as e:
        logging.exception("async_scan_bridge: job_id=%s 执行失败", jid)
        write_scan_async_status(
            "error",
            str(jid) if jid else None,
            message=str(e),
            error=str(e),
        )
    finally:
        try:
            os.remove(running)
        except OSError:
            pass
    return True


def read_latest_result_payload() -> Optional[Dict[str, Any]]:
    from core.runtime_data_paths import path_scan_async_latest_result_json

    p = path_scan_async_latest_result_json()
    if not os.path.isfile(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _background_worker_loop() -> None:
    logging.info("async_scan_bridge: 后台扫描线程 ScanAsyncWorker 已启动")
    while True:
        try:
            process_one_pending_scan_job()
        except Exception:
            logging.exception("async_scan_bridge: worker tick 未捕获异常")
        time.sleep(0.85)


def ensure_async_scan_worker_started() -> None:
    """
    【V26.6 架构废弃/封印】当前生产环境已剥离内嵌 Worker，调用此函数将触发异常拦截。

    在 Streamlit 进程内启动唯一 Daemon 线程（幂等）。
    若 config/环境要求外部 auto_sniper_daemon 独占 pending.json，则直接跳过，避免双进程抢队列。
    成功启动前必须非阻塞获得 OS 级 FileLock，与守护进程互斥。
    """
    assert int(os.environ.get("XIAOJIE_EMBED_UI_SCAN_WORKER", 0)) == 1, (
        "❌ 系统架构规范：禁止在当前主进程/UI中内嵌启动异步 Worker，请遵守单例守护进程规范！"
    )
    global _worker_started, _ui_queue_consumer_filelock, _ui_queue_lock_atexit_registered
    if not should_embed_ui_scan_worker():
        logging.info(
            "async_scan_bridge: 已跳过 UI 内嵌 ScanAsyncWorker（embed_worker_in_streamlit 关闭或 auto 检测到 OS 队列锁被占）。"
            " P3/P4 异步队列由 auto_sniper_daemon.py 等外部进程消费。"
        )
        return
    with _worker_lock:
        if _worker_started:
            return
        p = _path_queue_consumer_filelock()
        fl = FileLock(p)
        try:
            fl.acquire(timeout=0)
        except Timeout:
            logging.warning(
                "async_scan_bridge: 无法获得 pending 队列 OS 锁（已被其它进程占用），跳过 UI 内嵌 ScanAsyncWorker: %s",
                p,
            )
            return
        _ui_queue_consumer_filelock = fl
        if not _ui_queue_lock_atexit_registered:
            atexit.register(_release_ui_queue_consumer_filelock_at_exit)
            _ui_queue_lock_atexit_registered = True
        t = threading.Thread(
            target=_background_worker_loop,
            name="ScanAsyncWorker",
            daemon=True,
        )
        if os.environ.get("XIAOJIE_DAEMON_MODE", "").strip().lower() not in ("1", "true", "yes", "on"):
            try:
                from core.streamlit_thread_ctx import attach_script_run_ctx

                attach_script_run_ctx(t)
            except Exception:
                pass
        t.start()
        _worker_started = True
