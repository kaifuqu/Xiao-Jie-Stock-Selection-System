# -*- coding: utf-8 -*-
"""
Session 缓存「脱水 / 再水化」：写入 st.session_state 前剥离 DataFrame 与 numpy 标量，
扫描或策略实验室计算前按需从 DuckDB 拉回 K 线并补算指标。
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core.stock_name_utils import normalize_stock_display_name


def json_sanitize_scalar(v: Any) -> Any:
    if isinstance(v, (np.integer, np.int64, np.int32)):
        return int(v)
    if isinstance(v, (np.floating, np.float64, np.float32)):
        fv = float(v)
        if fv != fv:
            return None
        return fv
    if isinstance(v, np.bool_):
        return bool(v)
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if isinstance(v, np.datetime64):
        try:
            return str(pd.Timestamp(v))
        except Exception:
            return str(v)
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", errors="replace")
    try:
        json.dumps(v)
    except (TypeError, ValueError):
        return str(v)
    return v


def json_safe_dict(d: Any) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in d.items():
        ks = str(k)
        try:
            if isinstance(v, dict):
                out[ks] = json_safe_dict(v)
            elif isinstance(v, (list, tuple)):
                out[ks] = [
                    json_safe_dict(x) if isinstance(x, dict) else json_sanitize_scalar(x)
                    for x in v
                ]
            else:
                out[ks] = json_sanitize_scalar(v)
        except Exception as e:
            logging.warning("session_cache_dehydrate json_safe_dict key=%s err=%s", ks, e)
            out[ks] = str(v)
    return out


def dehydrate_base_item(item: Any) -> Dict[str, Any]:
    """剥离 df，hist 转为 JSON 安全标量；保留其余轻量键（code、p1_score 等）。"""
    if not isinstance(item, dict):
        return {}
    out = {k: v for k, v in item.items() if k != "df"}
    h = out.get("hist")
    if isinstance(h, dict):
        out["hist"] = json_safe_dict(h)
    elif h is None:
        out["hist"] = {}
    return out


def dehydrate_base_items_list(items: Any) -> List[Dict[str, Any]]:
    if not items:
        return []
    return [dehydrate_base_item(x) for x in items if isinstance(x, dict)]


def rehydrate_base_items_for_scan_engine(
    items: Any,
    limit: int = 120,
) -> List[Dict[str, Any]]:
    """
    为 run_scan_engine 准备带 df 的列表；不修改入参中的 dict。
    缺 df 或空表时按 code 拉 QFQ + precompute_indicators。
    """
    from core.indicator_calc import precompute_indicators
    from data.db_core import get_stock_data_qfq

    out: List[Dict[str, Any]] = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        code = it.get("code")
        if not code:
            continue
        df = it.get("df")
        if isinstance(df, pd.DataFrame) and not df.empty:
            row = dict(it)
            if not isinstance(row.get("hist"), dict):
                row["hist"] = {}
            out.append(row)
            continue
        try:
            df2 = get_stock_data_qfq(code, limit=limit)
            if df2 is None or df2.empty:
                continue
            df2 = precompute_indicators(df2)
            hist2 = json_safe_dict(df2.iloc[-1].to_dict())
            merged = {**it, "df": df2, "hist": hist2}
            out.append(merged)
        except Exception as e:
            logging.debug("rehydrate_base_items_for_scan_engine skip %s: %s", code, e)
    return out


def dehydrate_lab_mock_raw_for_session(mock_raw: Any) -> List[Dict[str, Any]]:
    """与底仓项同形：{code, df, hist} → 无 df。"""
    return dehydrate_base_items_list(mock_raw)


def rehydrate_lab_mock_raw_for_compute(
    entries: Any,
    progress_callback=None,
) -> List[Dict[str, Any]]:
    """实验室缓存命中或 legacy 脱水项：补全 df 供 build_p1_pool_and_cache。"""
    from core.indicator_calc import precompute_indicators
    from data.api_fetcher import fetch_realtime_batch
    from data.db_core import get_stock_data_qfq

    if not entries:
        return []
    codes = [e.get("code") for e in entries if isinstance(e, dict) and e.get("code")]
    codes = list(dict.fromkeys(codes))
    try:
        rt_map = fetch_realtime_batch(codes) or {}
    except Exception:
        rt_map = {}
    out: List[Dict[str, Any]] = []
    n = len(entries)
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            continue
        c = e.get("code")
        if not c:
            continue
        df = e.get("df")
        if isinstance(df, pd.DataFrame) and not df.empty:
            out.append(dict(e))
            continue
        if progress_callback and n > 0 and (i % max(1, n // 25) == 0 or i == n - 1):
            try:
                progress_callback(f"1档·实验室回放加载 K 线… ({i + 1}/{n})")
            except Exception:
                pass
        try:
            df2 = get_stock_data_qfq(c, limit=120)
            if df2 is None or df2.empty:
                continue
            df2 = precompute_indicators(df2)
            hist = json_safe_dict(df2.iloc[-1].to_dict())
            s_code = str(c).split(".")[0][:6]
            hist["name"] = normalize_stock_display_name(
                rt_map.get(s_code, {}).get("name", s_code)
            )
            prev_h = e.get("hist")
            if isinstance(prev_h, dict) and prev_h.get("name"):
                hist["name"] = normalize_stock_display_name(str(prev_h.get("name")))
            out.append({"code": c, "df": df2, "hist": hist})
        except Exception as ex:
            logging.debug("rehydrate_lab_mock_raw_for_compute skip %s: %s", c, ex)
    return out


def dehydrate_scan_result_row(r: Any) -> Any:
    if not isinstance(r, dict):
        return json_sanitize_scalar(r)
    out: Dict[str, Any] = {}
    for k, v in r.items():
        if isinstance(v, pd.DataFrame):
            continue
        if isinstance(v, dict):
            out[k] = dehydrate_scan_result_row(v)
        elif isinstance(v, list):
            out[k] = [
                dehydrate_scan_result_row(x) if isinstance(x, dict) else json_sanitize_scalar(x)
                for x in v
            ]
        else:
            out[k] = json_sanitize_scalar(v)
    return out


def dehydrate_scan_results_list(rows: Any) -> List[Any]:
    if not isinstance(rows, list):
        return []
    return [dehydrate_scan_result_row(x) for x in rows]


def dehydrate_scan_nested_fragment(obj: Any) -> Any:
    """funnel / observation / sop_market_breaker 等嵌套 dict-list 结构。"""
    if isinstance(obj, pd.DataFrame):
        return {}
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if isinstance(v, pd.DataFrame):
                continue
            out[str(k)] = dehydrate_scan_nested_fragment(v)
        return out
    if isinstance(obj, list):
        return [
            dehydrate_scan_result_row(x) if isinstance(x, dict) else json_sanitize_scalar(x)
            for x in obj
        ]
    return json_sanitize_scalar(obj)
