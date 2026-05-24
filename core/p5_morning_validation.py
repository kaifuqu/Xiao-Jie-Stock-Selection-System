# -*- coding: utf-8 -*-
"""
P5 真龙底仓：前一交易日名单落盘 + 次日 09:35 快照二次形态验证（跨进程 JSON 状态）。

【V26.5 第二阶段】与 auto_sniper_daemon 09:35 槽位咬合：early_morning_p5_validation
拉取实时快照二次闸；结果落盘 data/runtime/state/p5_yesterday_validated.json；
notification_gateway 按「已剔除」拦截企微；ui_sidebar 核心底仓区展示验证统计。

- p5_last_session.json：每晚 P5 扫描后写入（供次日早盘读取）。
- p5_yesterday_validated.json：09:35 验证后写入（企微网关读剔除名单拦截推送）。
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from core.stock_name_utils import normalize_stock_display_name

logger = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_VALIDATION_CACHE: Dict[str, Any] = {
    "mtime": 0.0,
    "calendar": "",
    "file_validation_cal": "",
    "rejected": frozenset(),
}


def _state_path(name: str) -> str:
    from core.runtime_data_paths import STATE_DIR, ensure_runtime_data_layout

    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, name)


P5_LAST_SESSION_JSON = "p5_last_session.json"
P5_YESTERDAY_VALIDATED_JSON = "p5_yesterday_validated.json"


def _norm_cal_date_8(d: Any) -> str:
    s = str(d or "").strip().replace("-", "")[:8]
    return s if len(s) == 8 and s.isdigit() else ""


def _norm_ts_code(code: Any) -> str:
    s = str(code or "").strip()
    if not s:
        return ""
    s = s.split(".")[0]
    return s[:6] if len(s) >= 6 else s.zfill(6)[:6]


def _full_ts_from_row(code_raw: Any) -> str:
    s = str(code_raw or "").strip()
    if not s:
        return ""
    if "." in s:
        return s
    d6 = s.zfill(6)[:6]
    if d6.startswith("6"):
        return f"{d6}.SH"
    return f"{d6}.SZ"


def sse_prev_open_trade_date_before(before_yyyymmdd: str) -> Optional[str]:
    """
    严格早于 before_yyyymmdd 的最近一个 SSE 开市日（YYYYMMDD）。
    """
    b8 = _norm_cal_date_8(before_yyyymmdd)
    if len(b8) != 8:
        return None
    try:
        from data import data_fetcher

        if getattr(data_fetcher, "pro", None) is None:
            return None
        end_dt = b8
        start_dt = (datetime.strptime(b8, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
        cal = data_fetcher.retry_api(data_fetcher.pro.trade_cal)(
            exchange="SSE", is_open="1", start_date=start_dt, end_date=end_dt
        )
        if cal is None or getattr(cal, "empty", True):
            return None
        col = "cal_date" if "cal_date" in cal.columns else cal.columns[0]
        days = sorted({_norm_cal_date_8(x) for x in cal[col].tolist() if _norm_cal_date_8(x)})
        prev = [d for d in days if d < b8]
        return prev[-1] if prev else None
    except Exception as e:
        logging.getLogger(__name__).debug("sse_prev_open_trade_date_before: %s", e)
        return None


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if x != x:  # nan
            return default
        return x
    except (TypeError, ValueError):
        return default


def write_p5_last_session_from_rows(
    rows: List[Dict[str, Any]],
    *,
    p5_trade_date: str,
    regime: str = "",
) -> None:
    """
    P5 扫描成功后调用：写入「当日 P5 真龙底仓候选」快照（供次日早盘二次验证）。
    """
    p8 = _norm_cal_date_8(p5_trade_date)
    if len(p8) != 8:
        return
    items: List[Dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        code = r.get("代码")
        if not code:
            continue
        items.append(
            {
                "代码": code,
                "名称": normalize_stock_display_name(r.get("名称", "")),
                "综合分": _safe_float(r.get("综合分"), 0.0),
                "涨幅": r.get("涨幅", ""),
                "现价": r.get("现价", ""),
            }
        )
    items.sort(key=lambda x: x.get("综合分", 0.0), reverse=True)
    path = _state_path(P5_LAST_SESSION_JSON)
    payload = {
        "p5_trade_date": p8,
        "regime": regime or "",
        "written_at_bj": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "items": items,
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as e:
        logging.getLogger(__name__).warning("写入 %s 失败: %s", P5_LAST_SESSION_JSON, e)


def _validate_one_morning_shape(rt: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    基于 09:35 附近实时快照的轻量形态闸：跌幅过大或无报价则剔除。
    【V26.7 修复】补全低开判断逻辑：
    - 条件1：早盘跌幅 > 5.5% → 直接剔除（深度破位不符合 P5 强势心智）
    - 条件2：现价 < 昨收94% → 直接剔除（形态支撑破位）
    - 条件3：开盘 < 昨收92% → 直接剔除（P5 强势逻辑不允许极端低开）
    - 条件4：低开后未完全修复（开盘 < 92% 且 现价 < 96%）→ 剔除
      注意：条件4 中现价 96% 门槛是为了区分"低开后小幅反弹（可观察）"vs"低开后无实质修复（剔除）"，
      不应与条件2（94%）重叠判断，以免遗漏开盘极端低开但当前已反弹至 94~96% 的股票。
    """
    if not rt or not isinstance(rt, dict):
        return False, "无实时快照"
    price = _safe_float(rt.get("price"), 0.0)
    pre = _safe_float(rt.get("pre_close"), 0.0)
    if price <= 0 or pre <= 0:
        return False, "报价无效"
    pct = (price - pre) / max(pre, 1e-9) * 100.0
    # 条件1：早盘跌幅过深（与P5强势逻辑不符）
    if pct < -5.5:
        return False, f"早盘跌幅过大(pct={pct:.2f}%)"
    # 条件2：现价跌破昨收支撑位（形态破位）
    if price < pre * 0.94:
        return False, "现价低于昨收阈值(形态破位)"
    open_px = _safe_float(rt.get("open"), 0.0)
    # 条件3：极端低开（不符合P5强势逻辑，直接剔除）
    if open_px > 0 and open_px < pre * 0.92:
        return False, f"低开过深(开盘={open_px/pre*100:.1f}%),不符合P5强势逻辑"
    # 条件4：低开后未实质修复（开盘<92% 且 现价<96%，即反弹幅度不足）
    if open_px > 0 and open_px < pre * 0.92 and price < pre * 0.96:
        return False, "低开后反弹不足，形态仍弱"
    return True, ""


def early_morning_p5_validation(log: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    读取前一交易日 P5 名单，拉取最新快照二次验证，写入 p5_yesterday_validated.json。
    供 09:35 分时快照同线程或独立任务调用。
    """
    lg = log or logger
    from zoneinfo import ZoneInfo

    bj = ZoneInfo("Asia/Shanghai")
    now = datetime.now(bj)
    today_cal = now.strftime("%Y%m%d")
    prev_td = sse_prev_open_trade_date_before(today_cal)

    out_path = _state_path(P5_YESTERDAY_VALIDATED_JSON)
    empty: Dict[str, Any] = {
        "validation_calendar_date": today_cal,
        "source_p5_trade_date": None,
        "validated_at_bj": now.isoformat(),
        "items": [],
        "confirmed_count": 0,
        "rejected_count": 0,
        "rejected_codes": [],
        "note": "",
    }

    session_path = _state_path(P5_LAST_SESSION_JSON)
    if not os.path.isfile(session_path):
        empty["note"] = "无 p5_last_session.json，跳过验证"
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(empty, f, ensure_ascii=False, indent=2)
        except OSError as e:
            lg.warning("写入 %s 失败: %s", P5_YESTERDAY_VALIDATED_JSON, e)
        return empty

    try:
        with open(session_path, "r", encoding="utf-8") as f:
            sess = json.load(f)
    except Exception as e:
        lg.warning("读取 p5_last_session 失败: %s", e)
        empty["note"] = f"读取会话失败: {e}"
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(empty, f, ensure_ascii=False, indent=2)
        except OSError:
            pass
        return empty

    if not isinstance(sess, dict):
        empty["note"] = "会话 JSON 格式异常"
        return empty

    # 【V26.7 修复】解析会话中的 trade_date；支持 YYYYMMDD / YYYY-MM-DD / 空值等多种异常情况
    sess_date_raw = sess.get("p5_trade_date")
    sess_date = _norm_cal_date_8(sess_date_raw) if sess_date_raw else ""

    items = sess.get("items")
    if not isinstance(items, list):
        items = []

    if not prev_td or sess_date != prev_td:
        empty["source_p5_trade_date"] = sess_date or None
        empty["note"] = (
            f"会话日期 {sess_date} 与前一交易日 {prev_td} 不一致，跳过二次验证"
            if prev_td
            else "无法解析前一交易日"
        )
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(empty, f, ensure_ascii=False, indent=2)
        except OSError as e:
            lg.warning("写入 %s 失败: %s", P5_YESTERDAY_VALIDATED_JSON, e)
        return empty

    codes_full = [_full_ts_from_row(it.get("代码")) for it in items if isinstance(it, dict) and it.get("代码")]
    codes_full = [c for c in codes_full if c]

    from data.api_fetcher import fetch_realtime_batch

    rt_map = fetch_realtime_batch(codes_full) or {}

    validated_items: List[Dict[str, Any]] = []
    rejected_codes: List[str] = []

    for it in items:
        if not isinstance(it, dict):
            continue
        raw_code = it.get("代码")
        full = _full_ts_from_row(raw_code)
        d6 = _norm_ts_code(raw_code)
        if not d6:
            continue
        rt = rt_map.get(d6)
        ok, reason = _validate_one_morning_shape(rt)
        status = "已确认" if ok else "已剔除"
        if not ok:
            rejected_codes.append(d6)
        validated_items.append(
            {
                "code": d6,
                "ts_code": full or d6,
                "name": normalize_stock_display_name(it.get("名称", "")),
                "status": status,
                "reason": reason if not ok else "",
                "overnight_score": _safe_float(it.get("综合分"), 0.0),
            }
        )

    payload = {
        "validation_calendar_date": today_cal,
        "source_p5_trade_date": sess_date,
        "validated_at_bj": now.isoformat(),
        "items": validated_items,
        "confirmed_count": sum(1 for x in validated_items if x.get("status") == "已确认"),
        "rejected_count": sum(1 for x in validated_items if x.get("status") == "已剔除"),
        "rejected_codes": rejected_codes,
        "note": "",
    }
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as e:
        lg.warning("写入 %s 失败: %s", P5_YESTERDAY_VALIDATED_JSON, e)

    lg.info(
        "P5 次日早盘验证完成: 源交易日=%s 验证日=%s 已确认=%s 已剔除=%s",
        sess_date,
        today_cal,
        payload["confirmed_count"],
        payload["rejected_count"],
    )
    _refresh_rejection_cache(out_path, today_cal)
    return payload


def _refresh_rejection_cache(path: str, today_cal: str) -> None:
    try:
        st = os.stat(path)
        mtime = float(st.st_mtime)
    except OSError:
        return
    rejected: set = set()
    file_val_cal = ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            file_val_cal = _norm_cal_date_8(data.get("validation_calendar_date"))
            if file_val_cal == today_cal:
                for c in data.get("rejected_codes") or []:
                    s = _norm_ts_code(c)
                    if s:
                        rejected.add(s)
    except Exception:
        pass
    with _CACHE_LOCK:
        _VALIDATION_CACHE["mtime"] = mtime
        _VALIDATION_CACHE["calendar"] = today_cal
        _VALIDATION_CACHE["file_validation_cal"] = file_val_cal
        _VALIDATION_CACHE["rejected"] = frozenset(rejected)


def is_code_blocked_by_morning_p5_validation(ts_code_6_or_full: str) -> bool:
    """
    企微推送前调用：若当日验证 JSON 中该代码为「已剔除」，返回 True（应拦截）。
    """
    from zoneinfo import ZoneInfo

    today_cal = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    d6 = _norm_ts_code(ts_code_6_or_full)
    if not d6:
        return False
    path = _state_path(P5_YESTERDAY_VALIDATED_JSON)
    try:
        st = os.stat(path)
        mtime = float(st.st_mtime)
    except OSError:
        return False

    with _CACHE_LOCK:
        cached_m = float(_VALIDATION_CACHE.get("mtime") or 0.0)
        if (
            abs(mtime - cached_m) < 1e-6
            and _VALIDATION_CACHE.get("calendar") == today_cal
            and _VALIDATION_CACHE.get("file_validation_cal") == today_cal
        ):
            return d6 in (_VALIDATION_CACHE.get("rejected") or frozenset())

    _refresh_rejection_cache(path, today_cal)
    with _CACHE_LOCK:
        if (
            _VALIDATION_CACHE.get("calendar") == today_cal
            and _VALIDATION_CACHE.get("file_validation_cal") == today_cal
        ):
            return d6 in (_VALIDATION_CACHE.get("rejected") or frozenset())
    return False


def read_p5_validation_summary_for_ui() -> Tuple[int, int, str]:
    """
    侧边栏展示：返回 (已确认数, 已剔除数, 提示文案后缀)。
    文件缺失或日期非当日返回 (0, 0, "").
    """
    from zoneinfo import ZoneInfo

    today_cal = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    path = _state_path(P5_YESTERDAY_VALIDATED_JSON)
    if not os.path.isfile(path):
        return 0, 0, ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return 0, 0, ""
        if _norm_cal_date_8(data.get("validation_calendar_date")) != today_cal:
            return 0, 0, ""
        c = int(data.get("confirmed_count") or 0)
        r = int(data.get("rejected_count") or 0)
        return c, r, ""
    except Exception:
        return 0, 0, ""
