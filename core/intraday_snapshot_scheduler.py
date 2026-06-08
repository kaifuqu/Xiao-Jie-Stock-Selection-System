# -*- coding: utf-8 -*-
"""
Streamlit 常驻进程内的日内快照后台调度：按北京时间在 6 个独立槽位调用
capture_intraday_snapshots(slot_id=..., force=True)，各时点写入独立键（vr_935、
vr_1125、vol_1435、vol_1440 等），并与 legacy 字段（vr_1030、pre_tail_*）对齐。

本模块仅为多时点量能/VR 锚点调度，与日线因子工程解耦。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date, datetime, time as dt_time, timedelta, timezone

BJ_TZ = timezone(timedelta(hours=8))


def _load_p1_codes_from_runtime_json_or_db(load_p1_cache_func=None) -> list[str]:
    """优先读取 runtime P1 JSON，失败或为空时回退 DuckDB p1_cache。"""
    try:
        from core.runtime_data_paths import path_p1_cache_json, POOL_CACHE_DIR
    except Exception as e:
        logging.warning("读取 P1 路径工具失败: %s", e)
        return []

    today = datetime.now(BJ_TZ).strftime("%Y%m%d")
    candidates = [path_p1_cache_json(today)]
    try:
        if os.path.isdir(POOL_CACHE_DIR):
            for name in sorted(os.listdir(POOL_CACHE_DIR), reverse=True):
                if name.startswith("p1_cache_") and name.endswith(".json"):
                    fp = os.path.join(POOL_CACHE_DIR, name)
                    if fp not in candidates:
                        candidates.append(fp)
    except Exception as e:
        logging.debug("枚举 P1 runtime 缓存失败: %s", e)

    for p in candidates:
        try:
            if not os.path.exists(p):
                continue
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            rows = data if isinstance(data, list) else (data.get("items") or data.get("rows") or data.get("stocks") or [])
            out: list[str] = []
            for it in rows:
                if not isinstance(it, dict):
                    continue
                code = str(it.get("ts_code") or it.get("code") or it.get("代码") or "").strip()
                if code:
                    out.append(code)
            out = [c for c in dict.fromkeys(out) if c]
            if out:
                return out
        except Exception as e:
            logging.warning("读取 P1 runtime JSON 失败 %s: %s", p, e)

    if load_p1_cache_func is None:
        return []
    try:
        p1_cache = load_p1_cache_func(today)
        if isinstance(p1_cache, list):
            codes = [str(x.get("ts_code") or x.get("code") or "").strip() for x in p1_cache if isinstance(x, dict)]
        elif isinstance(p1_cache, dict):
            codes = [str(k).strip() for k in p1_cache.keys()]
        else:
            codes = []
        return [c for c in dict.fromkeys(codes) if c]
    except Exception as e:
        logging.warning("读取 DuckDB p1_cache 失败: %s", e)
        return []


# (时, 分, slot_id) — 与 capture_intraday_snapshots(slot_id=...) 一致
_INTRADAY_SNAPSHOT_SLOTS: tuple[tuple[int, int, str], ...] = (
    (9, 35, "935"),
    (10, 30, "1030"),
    (11, 25, "1125"),
    (13, 25, "1325"),
    (14, 25, "1425"),
    (14, 40, "1440"),
)

_started = False
_start_lock = threading.Lock()
_P1_SCAN_PERIOD_SEC = 180


def _slot_key(d: str, h: int, m: int, slot_id: str) -> str:
    return f"{d}|{h:02d}:{m:02d}|{slot_id}"


def _next_weekday_on_or_after(d: date) -> date:
    cur = d
    while cur.weekday() >= 5:
        cur += timedelta(days=1)
    return cur


def _next_fire_after(now: datetime) -> tuple[datetime, int, int, str]:
    """返回 strictly > now 的下一档（仅工作日；同日优先，否则下一工作日首档）。第四元为 slot_id。"""
    day = now.date()
    if day.weekday() >= 5:
        nd = _next_weekday_on_or_after(day)
        h0, m0, sid0 = _INTRADAY_SNAPSHOT_SLOTS[0]
        t0 = datetime.combine(nd, dt_time(h0, m0, 0), tzinfo=BJ_TZ)
        while t0 <= now:
            nd = _next_weekday_on_or_after(nd + timedelta(days=1))
            t0 = datetime.combine(nd, dt_time(h0, m0, 0), tzinfo=BJ_TZ)
        return (t0, h0, m0, sid0)

    best: tuple[datetime, int, int, str] | None = None
    for h, mi, sid in _INTRADAY_SNAPSHOT_SLOTS:
        t = datetime.combine(day, dt_time(h, mi, 0), tzinfo=BJ_TZ)
        if t <= now:
            continue
        if best is None or t < best[0]:
            best = (t, h, mi, sid)
    if best is not None:
        return best
    h0, m0, sid0 = _INTRADAY_SNAPSHOT_SLOTS[0]
    nd = _next_weekday_on_or_after(day + timedelta(days=1))
    t0 = datetime.combine(nd, dt_time(h0, m0, 0), tzinfo=BJ_TZ)
    return (t0, h0, m0, sid0)


def _load_p1_bottom_pool_codes() -> list[str]:
    """读取 P1 底仓池代码列表，供后台 3 分钟复核使用。"""
    return _load_p1_codes_from_runtime_json_or_db()


def _run_p1_bottom_hold_recheck() -> str:
    """每 3 分钟复核 P1 底仓池，并把命中的 P4-07 结果写回当天快照。"""
    try:
        from core.scan_engine import capture_intraday_snapshots, fetch_realtime_batch, run_scan_engine, load_p1_cache
    except Exception as e:
        return f"P1复核入口导入失败: {e}"

    codes = _load_p1_codes_from_runtime_json_or_db(load_p1_cache)
    codes = [c for c in dict.fromkeys(codes) if c]
    if not codes:
        return "P1底仓池为空"

    rt_map = fetch_realtime_batch(codes)
    if not rt_map:
        return "P1复核获取实时行情失败"

    try:
        base_items = []
        for ts_code in codes:
            rt = rt_map.get(ts_code)
            if not isinstance(rt, dict):
                continue
            base_items.append({"ts_code": ts_code, **rt})
        if not base_items:
            return "P1复核无有效行情"

        eng_res = run_scan_engine(target_pools=["p4"], base_items=base_items, regime="震荡市")
        p4_rows = eng_res.get("p4") if isinstance(eng_res, dict) else None
        if not p4_rows:
            return "P1复核未命中P4-07"

        hit_cnt = 0
        for row in p4_rows:
            if not isinstance(row, dict):
                continue
            if any("P4-07" in str(x) for x in row.get("strategies", []) if x is not None):
                hit_cnt += 1
        if hit_cnt <= 0:
            return "P1复核未命中P4-07"

        try:
            capture_intraday_snapshots(codes=codes, capture_mode="auto", force=True)
        except Exception:
            pass
        return f"P1复核完成，P4-07 命中 {hit_cnt} 只"
    except Exception as e:
        return f"P1复核异常: {e}"


def _scheduler_loop() -> None:
    from core.scan_engine import capture_intraday_snapshots

    fired: set[str] = set()
    last_clear_date = ""
    last_p1_run_ts = 0.0

    while True:
        try:
            now = datetime.now(BJ_TZ)
            d = now.strftime("%Y%m%d")
            if d != last_clear_date:
                fired.clear()
                last_clear_date = d
                last_p1_run_ts = 0.0

            fire_at, fh, fm, slot_id = _next_fire_after(now)
            delay = (fire_at - now).total_seconds()
            if delay > 0:
                sleep_s = min(delay, 300.0)
            else:
                sleep_s = 1.5

            if now.weekday() < 5 and now.hour >= 9 and now.hour < 15:
                if now.timestamp() - last_p1_run_ts >= _P1_SCAN_PERIOD_SEC:
                    try:
                        msg = _run_p1_bottom_hold_recheck()
                        last_p1_run_ts = now.timestamp()
                        logging.info("【P1 3分钟复核】%s", msg)
                    except Exception as e:
                        logging.warning("【P1 3分钟复核】失败: %s", e)
                        last_p1_run_ts = now.timestamp()

            if delay > 0:
                time.sleep(sleep_s)
                continue

            key = _slot_key(d, fh, fm, slot_id)
            if key not in fired:
                try:
                    msg = capture_intraday_snapshots(
                        slot_id=slot_id, force=True
                    )
                    fired.add(key)
                    logging.info(
                        "【后台快照】%02d:%02d slot=%s — %s", fh, fm, slot_id, msg
                    )
                except Exception as e:
                    logging.warning(
                        "【后台快照】%02d:%02d slot=%s 失败: %s",
                        fh,
                        fm,
                        slot_id,
                        e,
                    )
                    time.sleep(20.0)

            time.sleep(1.5)
        except Exception as e:
            logging.warning("【后台快照】调度循环异常: %s", e)
            time.sleep(30)


def start_intraday_snapshot_background_scheduler() -> None:
    """进程内单例：启动守护线程。Streamlit 多次重跑脚本时模块级 _started 仍为 True。"""
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
    t = threading.Thread(
        target=_scheduler_loop,
        name="intraday_snapshot_scheduler",
        daemon=True,
    )
    try:
        from core.streamlit_thread_ctx import attach_script_run_ctx

        attach_script_run_ctx(t)
    except Exception:
        pass
    t.start()
    logging.info(
        "日内快照后台调度已启动（北京时间 %s）",
        ", ".join(f"{h:02d}:{m:02d}→{sid}" for h, m, sid in _INTRADAY_SNAPSHOT_SLOTS),
    )


def capture_intraday_snapshots(*args, **kwargs):
    """
    对外统一入口：转发至 `core.scan_engine.capture_intraday_snapshots`。
    供 `auto_sniper_daemon` 等进程顶层 `from core.intraday_snapshot_scheduler import capture_intraday_snapshots` 使用，
    避免与 scan_engine 模块级循环依赖。
    """
    from core.scan_engine import capture_intraday_snapshots as _impl

    return _impl(*args, **kwargs)
