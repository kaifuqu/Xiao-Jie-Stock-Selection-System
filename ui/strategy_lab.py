# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 策略实验室：P1–P5 会话覆写 + 当日缓存回放 + 实验落库/回档。
"""
from __future__ import annotations

import copy
import html as html_escape
import json
import logging
from collections import Counter
from dataclasses import fields, is_dataclass
from typing import Any, Dict, List, Set, Tuple, Type

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from core.stock_name_utils import normalize_stock_display_name
from core.config_manager import (
    get_observation_pool_relax_settings,
    get_p1_profiles_merged,
    get_p1_thresholds_for_profile,
    get_p2_screener_config,
    get_p3_intraday_screener_config,
    get_p4_tail_screener_config,
    get_p5_postmarket_config,
    get_strategies_dict,
)
from core.experiment_db import get_experiment_history, init_experiment_table, save_experiment_record
from core.p1_score_display import p1_score_details_to_extreme_labels
from core.pool_manager import build_p1_pool_and_cache
from data.api_fetcher import fetch_realtime_batch
from data.db_core import get_stock_data_qfq
from service.scan_service import scan_pools
from ui.session_cache_dehydrate import (
    dehydrate_lab_mock_raw_for_session,
    rehydrate_base_items_for_scan_engine,
    rehydrate_lab_mock_raw_for_compute,
)
from ui.display_labels import (
    LAB_SWEEP_RESULT_COLUMNS,
    POOL_KEY_CN,
    pool_cn,
    p1_profile_cn,
    rename_columns_for_display,
    style_dataframe_center,
)
from ui.strategy_lab_labels import bounds_for, build_field_label, clamp_value, lab_input_step, label_zh

try:
    from core.runtime_data_paths import path_p1_last_wash_input_codes_json
except ImportError:
    def path_p1_last_wash_input_codes_json():
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data",
            "p1_last_wash_input_codes.json",
        )

_STRATEGY_LAB_SNAPSHOT_KEY = "_strategy_lab_last_snapshot"
_STRATEGY_LAB_POOL_KEYS = frozenset({"p1", "p2", "p3", "p4", "p5", "golden_burst"})
_P1_PROFILE_PARAM_KEYS_FALLBACK = frozenset(
    {
        "trend_ma120_min_ratio",
        "trend_slope_fastpass",
        "near_ma20_min_ratio",
        "macd_bar_kill",
        "vol_divergence_ratio",
        "pass_line",
    }
)
_GOLDEN_BURST_KEYS_FALLBACK = frozenset(
    {
        "golden_burst_pct_low",
        "golden_burst_pct_high",
        "golden_burst_vr_low",
        "golden_burst_vr_high",
        "p5_golden_vr_min",
        "p5_golden_pct_low",
        "p5_golden_pct_high",
    }
)

POOL_LABELS = POOL_KEY_CN


def _lab_json_to_dict(val: Any) -> Dict[str, Any]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return {}
    if isinstance(val, dict):
        return dict(val)
    s = str(val).strip()
    if not s:
        return {}
    try:
        out = json.loads(s)
        return out if isinstance(out, dict) else {}
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def _summarize_params_cell(raw_json: Any) -> str:
    d = _lab_json_to_dict(raw_json)
    if not d:
        return "—"
    parts: List[str] = []
    for k in ("pass_line", "_lab_profile_choice", "trend_ma120_min_ratio", "s1_vol_ratio_min", "s3_vol_ratio_min"):
        if k not in d:
            continue
        v = d[k]
        if k == "_lab_profile_choice":
            parts.append(p1_profile_cn(str(v)))
        else:
            try:
                parts.append(f"{label_zh(k)}={float(v):g}")
            except (TypeError, ValueError):
                parts.append(f"{label_zh(k)}={v}")
        if len(parts) >= 3:
            break
    return "；".join(parts) if parts else "—"


def _summarize_top_reasons_cell(raw_json: Any, max_items: int = 3, reason_max_len: int = 14) -> str:
    d = _lab_json_to_dict(raw_json)
    if not d:
        return "—"
    try:
        items = sorted(d.items(), key=lambda kv: -float(kv[1]))
    except (TypeError, ValueError):
        items = list(d.items())
    chunks: List[str] = []
    for k, v in items[:max_items]:
        rk = str(k).strip() or "—"
        if len(rk) > reason_max_len:
            rk = rk[: reason_max_len - 1] + "…"
        chunks.append(f"{rk}×{v}")
    return "；".join(chunks) if chunks else "—"


def _format_experiment_time_series(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, errors="coerce")
    return ts.dt.strftime("%Y-%m-%d %H:%M").fillna("—")


def _history_display_df(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()

    def col(name: str, default: Any = None) -> pd.Series:
        if name not in raw.columns:
            return pd.Series([default] * len(raw), index=raw.index)
        return raw[name]

    eid = col("exp_id").astype(str)
    short_id = eid.apply(lambda x: (x[:8] + "…") if len(x) > 10 else x)
    pk_series = col("pool_key").fillna("").astype(str).str.lower()
    return pd.DataFrame(
        {
            "实验编号": short_id,
            "实验时间": _format_experiment_time_series(col("created_at")),
            "池子": pk_series.map(lambda x: pool_cn(x) if x else ""),
            "环境": col("regime").fillna("").astype(str),
            "入池数": pd.to_numeric(col("pass_count"), errors="coerce").fillna(0).astype(int),
            "最高分": pd.to_numeric(col("max_score"), errors="coerce").fillna(0.0).astype(float).round(2),
            "参数摘要": col("parameters").map(_summarize_params_cell),
            "死因摘要": col("top_reasons").map(_summarize_top_reasons_cell),
            "备注": col("note").fillna("").astype(str),
        },
        index=raw.index,
    )


def _ensure_lab_state() -> None:
    if "strategy_lab_overrides" not in st.session_state:
        st.session_state["strategy_lab_overrides"] = {}
    # 【会话覆写】缩量观察池放宽三键；供 get_observation_pool_relax_settings 与 fund_mv_utils 读取
    if "lab_opr_overrides" not in st.session_state:
        st.session_state["lab_opr_overrides"] = {}
    elif not isinstance(st.session_state.get("lab_opr_overrides"), dict):
        st.session_state["lab_opr_overrides"] = {}
    if "_strategy_lab_prod_baseline" not in st.session_state:
        st.session_state["_strategy_lab_prod_baseline"] = {}
    init_experiment_table()


def _flatten_p1_active_profile(regime_name: str) -> Tuple[str, Dict[str, Any]]:
    merged = get_p1_profiles_merged(regime_name=regime_name)
    key = str(merged.pop("_active_key", "neutral"))
    prof = dict(merged.get(key) or {})
    return key, prof


def _p1_profile_keys() -> List[str]:
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    profs = p1.get("profiles") if isinstance(p1.get("profiles"), dict) else {}
    return [k for k in ("strict", "neutral", "relaxed") if k in profs] or ["strict", "neutral", "relaxed"]


def _yaml_flat_scalar_keys_under_pool(pool_key: str) -> Set[str]:
    if pool_key == "p1":
        return set()
    s = get_strategies_dict()
    node = s.get(pool_key)
    if not isinstance(node, dict):
        return set()
    return {k for k, v in node.items() if not isinstance(v, dict)}


def _dataclass_field_names_for_flat_pool(pool_key: str) -> Set[str]:
    try:
        if pool_key == "p2":
            from core.strategies.p2_auction_screener import P2ScreenerConfig

            return {f.name for f in fields(P2ScreenerConfig)}
        if pool_key == "p3":
            from core.strategies.p3_intraday_screener import P3IntradayScreenerConfig

            return {f.name for f in fields(P3IntradayScreenerConfig)}
        if pool_key == "p4":
            from core.strategies.p4_tail_screener import P4TailScreenerConfig

            return {f.name for f in fields(P4TailScreenerConfig)}
        if pool_key == "p5":
            from core.strategies.p5_postmarket_screener import P5PostmarketConfig

            return {f.name for f in fields(P5PostmarketConfig)}
    except Exception:
        return set()
    return set()


def _allowed_flat_pool_override_keys(pool_key: str) -> Set[str]:
    keys = _yaml_flat_scalar_keys_under_pool(pool_key)
    if keys:
        return keys
    if pool_key == "golden_burst":
        return set(_GOLDEN_BURST_KEYS_FALLBACK)
    return _dataclass_field_names_for_flat_pool(pool_key)


def _allowed_p1_profile_names() -> Set[str]:
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    profs = p1.get("profiles") if isinstance(p1.get("profiles"), dict) else {}
    if profs:
        return set(profs.keys())
    return {"strict", "neutral", "relaxed"}


def _allowed_p1_profile_param_keys(profile_name: str) -> Set[str]:
    s = get_strategies_dict()
    p1 = s.get("p1") if isinstance(s.get("p1"), dict) else {}
    profs = p1.get("profiles") if isinstance(p1.get("profiles"), dict) else {}
    ref = profs.get(profile_name)
    if isinstance(ref, dict):
        kset = {k for k, v in ref.items() if not isinstance(v, dict)}
        if kset:
            return kset
    return set(_P1_PROFILE_PARAM_KEYS_FALLBACK)


def _sanitize_lab_overrides_core_locked() -> None:
    root = st.session_state.get("strategy_lab_overrides")
    if not isinstance(root, dict):
        st.session_state["strategy_lab_overrides"] = {}
        return

    for tk in list(root.keys()):
        if tk not in _STRATEGY_LAB_POOL_KEYS:
            root.pop(tk, None)

    for pool_key in ("p2", "p3", "p4", "p5", "golden_burst"):
        block = root.get(pool_key)
        if block is None:
            continue
        if not isinstance(block, dict):
            root.pop(pool_key, None)
            continue
        allowed = _allowed_flat_pool_override_keys(pool_key)
        cleaned = {k: v for k, v in block.items() if k in allowed}
        if cleaned:
            root[pool_key] = cleaned
        else:
            root.pop(pool_key, None)

    p1_block = root.get("p1")
    if p1_block is None:
        st.session_state["strategy_lab_overrides"] = root
        return
    if not isinstance(p1_block, dict):
        root.pop("p1", None)
        st.session_state["strategy_lab_overrides"] = root
        return

    cleaned_p1: Dict[str, Any] = {}
    profs_o = p1_block.get("profiles")
    if isinstance(profs_o, dict):
        name_ok = _allowed_p1_profile_names()
        new_profiles: Dict[str, Dict[str, Any]] = {}
        for pk, pv in list(profs_o.items()):
            if pk not in name_ok or not isinstance(pv, dict):
                continue
            allowed_params = _allowed_p1_profile_param_keys(pk)
            filtered = {k: v for k, v in pv.items() if k in allowed_params}
            if filtered:
                new_profiles[pk] = filtered
        if new_profiles:
            cleaned_p1["profiles"] = new_profiles
    if cleaned_p1:
        root["p1"] = cleaned_p1
    else:
        root.pop("p1", None)

    st.session_state["strategy_lab_overrides"] = root


def _merge_p1_lab_profile(profile_key: str, flat: Dict[str, Any]) -> None:
    _ensure_lab_state()
    root = st.session_state["strategy_lab_overrides"]
    p1 = dict(root.get("p1") or {})
    profiles = dict(p1.get("profiles") or {})
    cur = dict(profiles.get(profile_key) or {})
    cur.update(flat)
    profiles[profile_key] = cur
    p1["profiles"] = profiles
    root["p1"] = p1
    st.session_state["strategy_lab_overrides"] = root


def _merge_flat_lab_pool(pool_key: str, flat: Dict[str, Any]) -> None:
    _ensure_lab_state()
    root = st.session_state["strategy_lab_overrides"]
    cur = dict(root.get(pool_key) or {})
    cur.update(flat)
    root[pool_key] = cur
    st.session_state["strategy_lab_overrides"] = root


def _lab_widget_key(pool: str, p1_profile: str, field: str) -> str:
    if pool == "p1":
        return f"lw_{pool}_{p1_profile}_{field}"
    return f"lw_{pool}__{field}"


def _field_is_int_for_dataclass(cls: Type[Any], name: str) -> bool:
    if not is_dataclass(cls):
        return False
    for f in fields(cls):
        if f.name != name:
            continue
        return f.type is int or getattr(f.type, "__name__", "") == "int"
    return False


def _get_config_cls(pool: str) -> Type[Any]:
    if pool == "p2":
        from core.strategies.p2_auction_screener import P2ScreenerConfig

        return P2ScreenerConfig
    if pool == "p3":
        from core.strategies.p3_intraday_screener import P3IntradayScreenerConfig

        return P3IntradayScreenerConfig
    if pool == "p4":
        from core.strategies.p4_tail_screener import P4TailScreenerConfig

        return P4TailScreenerConfig
    if pool == "p5":
        from core.strategies.p5_postmarket_screener import P5PostmarketConfig

        return P5PostmarketConfig
    raise ValueError(pool)


def _merged_flat_pool_values(pool: str) -> Dict[str, Any]:
    if pool == "p2":
        cfg = get_p2_screener_config()
    elif pool == "p3":
        cfg = get_p3_intraday_screener_config()
    elif pool == "p4":
        cfg = get_p4_tail_screener_config()
    elif pool == "p5":
        cfg = get_p5_postmarket_config()
    else:
        return {}
    return {f.name: getattr(cfg, f.name) for f in fields(cfg)}


def _init_lab_widget_scalars(
    pool: str,
    p1_prof: str,
    field_names: List[str],
    defaults: Dict[str, Any],
    types_int: Dict[str, bool],
) -> None:
    for fn in field_names:
        wk = _lab_widget_key(pool, p1_prof, fn)
        if wk in st.session_state:
            continue
        base = defaults.get(fn)
        is_int = types_int.get(fn, False)
        try:
            if is_int:
                st.session_state[wk] = int(round(float(base)))
            else:
                st.session_state[wk] = float(base)
        except (TypeError, ValueError):
            st.session_state[wk] = 0 if is_int else 0.0


def _sync_widgets_from_flat(pool: str, p1_prof: str, flat: Dict[str, Any], yaml_defaults: Dict[str, Any], types_int: Dict[str, bool]) -> None:
    for kk, vv in flat.items():
        if kk == "_lab_profile_choice":
            continue
        wk = _lab_widget_key(pool, p1_prof if pool == "p1" else "", kk)
        is_int = types_int.get(kk, False)
        ref = float(yaml_defaults.get(kk, vv) or 0.0)
        lo, hi = bounds_for(str(kk), ref, is_int)
        try:
            v = float(vv)
            v = max(float(lo), min(float(hi), v))
        except (TypeError, ValueError):
            v = float(lo)
        st.session_state[wk] = int(round(v)) if is_int else v


def _render_number_input_bounded(
    *,
    pool: str,
    p1_prof: str,
    field_key: str,
    yaml_default: Any,
    is_int: bool,
) -> None:
    wk = _lab_widget_key(pool, p1_prof, field_key)
    ref = float(yaml_default) if yaml_default is not None else 0.0
    lo, hi = bounds_for(field_key, ref, is_int)
    cur = float(st.session_state[wk])
    cur = max(lo, min(hi, cur))
    st.session_state[wk] = int(round(cur)) if is_int else cur
    step = lab_input_step(ref, float(lo), float(hi), is_int)
    label = build_field_label(field_key, yaml_default)
    if is_int:
        st.number_input(
            label,
            min_value=int(round(lo)),
            max_value=int(round(hi)),
            step=max(1, int(step)),
            key=wk,
            help=field_key,
        )
    else:
        st.number_input(
            label,
            min_value=float(lo),
            max_value=float(hi),
            step=float(step),
            format="%.6g",
            key=wk,
            help=field_key,
        )


def _lab_markdown_centered_table(df: pd.DataFrame) -> None:
    """与指挥舱漏斗一致：HTML 表格居中，长文案可换行。"""
    if df is None or df.empty:
        return
    cols = list(df.columns)
    th = "<tr>" + "".join(
        f"<th style='text-align:center;padding:8px 10px;border-bottom:1px solid #e5e7eb;font-weight:600;vertical-align:middle'>{html_escape.escape(str(c))}</th>"
        for c in cols
    ) + "</tr>"
    trs = []
    for _, row in df.iterrows():
        tds = []
        for c in cols:
            v = row[c]
            s = html_escape.escape(str(v))
            tds.append(
                f"<td style='text-align:center;padding:8px 10px;max-width:280px;word-break:break-word;font-size:13px;vertical-align:middle'>{s}</td>"
            )
        trs.append("<tr>" + "".join(tds) + "</tr>")
    tbl = (
        "<div style='display:flex;justify-content:center;width:100%;margin:0.35rem 0 0.5rem 0;'>"
        "<table style='border-collapse:collapse;color:#1e293b'>"
        f"<thead>{th}</thead><tbody>{''.join(trs)}</tbody></table></div>"
    )
    st.markdown(tbl, unsafe_allow_html=True)


def _history_row_label(hist_df: pd.DataFrame, row_index: int) -> str:
    r = hist_df.iloc[int(row_index)]
    try:
        tss = pd.to_datetime(r.get("created_at")).strftime("%Y-%m-%d %H:%M")
    except Exception:
        tss = str(r.get("created_at"))[:16]
    try:
        pc = int(pd.to_numeric(r.get("pass_count"), errors="coerce") or 0)
    except Exception:
        pc = 0
    note = str(r.get("note") or "").strip().replace("\n", " ")[:24]
    pk = str(r.get("pool_key") or "")
    return f"[{pool_cn(pk)}] {tss} 入池{pc} {note}"


def _render_lab_report(
    *,
    pass_count: int,
    max_score: float,
    prod: Dict[str, Any],
    top5: List[Tuple[str, int]],
    preview_rows: List[Dict[str, Any]],
) -> None:
    pc = int((prod or {}).get("count", 0))
    ps = float((prod or {}).get("max_score", 0.0))
    st.metric("入池", pass_count, delta=f"{pass_count - pc}" if prod else None)
    st.metric("最高分", round(max_score, 2), delta=f"{round(max_score - ps, 2)}" if prod else None)
    if top5:
        st.markdown(
            "<div style='text-align:center;color:#64748b;font-size:0.9rem;margin:0.5rem 0 0.15rem 0;'>"
            "拦截 / 淘汰原因 Top（横向）</div>",
            unsafe_allow_html=True,
        )
        row_tbl: Dict[str, List[Any]] = {"项目": ["死因", "数量"]}
        for i, (reason, cnt) in enumerate(top5[:5], start=1):
            row_tbl[f"第{i}项"] = [str(reason), str(int(cnt))]
        _lab_markdown_centered_table(pd.DataFrame(row_tbl))
    if preview_rows:
        st.dataframe(style_dataframe_center(pd.DataFrame(preview_rows)), width="stretch", hide_index=True)


def _max_score_scan_rows(rows: List[dict]) -> float:
    best = 0.0
    for r in rows or []:
        try:
            v = float(r.get("综合分", 0.0))
            if v > best:
                best = v
        except (TypeError, ValueError):
            continue
    return best


_MAX_SWEEP_POINTS = 2000

_P1_LAST_WASH_CODES_JSON = path_p1_last_wash_input_codes_json()


def _load_p1_last_wash_codes_meta_from_disk() -> Tuple[List[str], int]:
    if not os.path.exists(_P1_LAST_WASH_CODES_JSON):
        return [], 0
    try:
        with open(_P1_LAST_WASH_CODES_JSON, "r", encoding="utf-8") as f:
            d = json.load(f)
        if not isinstance(d, dict):
            return [], 0
        codes = d.get("codes") or []
        if not isinstance(codes, list):
            return [], 0
        rev = int(d.get("revision", 0) or 0)
        return [str(c) for c in codes if c], rev
    except Exception:
        return [], 0


def _hydrate_p1_mock_raw_from_codes(
    codes: List[str],
    progress_callback=None,
) -> List[Dict[str, Any]]:
    if not codes:
        return []
    try:
        rt_map = fetch_realtime_batch(list(codes)) or {}
    except Exception:
        rt_map = {}
    out: List[Dict[str, Any]] = []
    n = len(codes)
    for i, c in enumerate(codes):
        if progress_callback and n > 0 and (i % max(1, n // 25) == 0 or i == n - 1):
            try:
                progress_callback(f"1档·实验室加载候选 K 线… ({i + 1}/{n})")
            except Exception:
                pass
        try:
            df = get_stock_data_qfq(c, limit=120)
        except Exception:
            df = None
        if df is None or getattr(df, "empty", True):
            continue
        hist = df.iloc[-1].to_dict()
        s_code = str(c).split(".")[0][:6]
        hist["name"] = normalize_stock_display_name(rt_map.get(s_code, {}).get("name", s_code))
        out.append({"code": c, "df": df, "hist": hist})
    return out


def _resolve_p1_lab_mock_raw(
    base_items: List[Any],
    progress_callback=None,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    与指挥舱洗盘对齐：优先使用「上次洗盘写入的候选全集」；勿仅用已入池底仓，否则缩量样本池与实盘不一致。
    返回 (mock_raw, hint)，hint 供 UI 提示。
    """
    codes = st.session_state.get("p1_last_wash_input_codes") or []
    rev = st.session_state.get("p1_last_wash_input_revision")
    if not codes:
        dc, dr = _load_p1_last_wash_codes_meta_from_disk()
        codes = dc
        if rev is None and dr:
            st.session_state["p1_last_wash_input_revision"] = dr
            rev = dr
        if codes and not st.session_state.get("p1_last_wash_input_codes"):
            st.session_state["p1_last_wash_input_codes"] = list(codes)
    try:
        rev_i = int(rev) if rev is not None else 0
    except (TypeError, ValueError):
        rev_i = 0
    cache = st.session_state.get("_p1_lab_mock_raw_cache")
    if (
        isinstance(cache, dict)
        and int(cache.get("revision", -1)) == rev_i
        and isinstance(cache.get("mock_raw"), list)
        and len(cache.get("mock_raw") or []) > 0
    ):
        _cached_entries = list(cache["mock_raw"])
        return (
            rehydrate_lab_mock_raw_for_compute(_cached_entries, progress_callback=progress_callback),
            "replay_full_candidates_cached",
        )

    if codes:
        mock_raw = _hydrate_p1_mock_raw_from_codes(codes, progress_callback=progress_callback)
        st.session_state["_p1_lab_mock_raw_cache"] = {
            "revision": rev_i,
            "mock_raw": dehydrate_lab_mock_raw_for_session(mock_raw),
        }
        return mock_raw, "replay_full_candidates_fresh"

    mock_raw: List[Dict[str, Any]] = []
    for item in rehydrate_base_items_for_scan_engine(base_items or []):
        if not isinstance(item, dict):
            continue
        df = item.get("df")
        if df is None or getattr(df, "empty", True):
            continue
        hist = item.get("hist")
        if not isinstance(hist, dict):
            hist = df.iloc[-1].to_dict()
        mock_raw.append({"code": item.get("code"), "df": df, "hist": hist})
    return mock_raw, "legacy_in_pool_only_mismatch_risk"


def _lab_collect_widget_merged(
    pool: str,
    wprof: str,
    field_names: List[str],
    yaml_defaults: Dict[str, Any],
    types_int: Dict[str, bool],
) -> Dict[str, float]:
    merged_out: Dict[str, float] = {}
    for fn in field_names:
        wk = _lab_widget_key(pool, wprof, fn)
        raw_v = st.session_state.get(wk, yaml_defaults.get(fn, 0.0))
        yd = yaml_defaults.get(fn, 0.0)
        is_int = types_int.get(fn, False)
        merged_out[fn] = clamp_value(fn, float(raw_v), float(yd), is_int)
    return merged_out


def _lab_build_sweep_values(start: float, end: float, step: float, is_int: bool) -> Tuple[List[float], bool]:
    truncated = False
    if is_int:
        a, b = int(round(start)), int(round(end))
        if b < a:
            a, b = b, a
        stp = max(1, int(round(abs(step))) or 1)
        vals = [float(x) for x in range(a, b + 1, stp)]
    else:
        lo, hi = float(start), float(end)
        if hi < lo:
            lo, hi = hi, lo
        stp = float(abs(step))
        if stp < 1e-12:
            return [], False
        vals = list(np.arange(lo, hi + stp * 0.5, stp))
    if len(vals) > _MAX_SWEEP_POINTS:
        vals = vals[:_MAX_SWEEP_POINTS]
        truncated = True
    return vals, truncated


def _lab_run_scan_numbers(
    *,
    pool: str,
    p1_prof: str,
    merged_out: Dict[str, float],
    curr_regime: str,
    base_items: List[Any],
    progress_callback,
) -> Tuple[int, float, List[Tuple[str, int]], List[Dict[str, Any]]]:
    """
    假定调用方已写入 strategy_lab_overrides（P1 profiles / P2–P5 平面）。
    只做测算，不修改覆写字典。
    """
    top5: List[Tuple[str, int]] = []
    preview_rows: List[Dict[str, Any]] = []
    n_pass = 0
    mx = 0.0

    if pool == "p1":
        full_t = get_p1_thresholds_for_profile(curr_regime, p1_prof)
        for k, v in merged_out.items():
            full_t[k] = float(v)
        mock_raw, _hint = _resolve_p1_lab_mock_raw(base_items, progress_callback=progress_callback)
        st.session_state["_p1_lab_last_resolve_hint"] = _hint
        pool_items, rejected = build_p1_pool_and_cache(
            mock_raw,
            progress_callback=None,
            regime_name=curr_regime,
            p1_threshold_override=full_t,
        )
        n_pass = len(pool_items)
        sc = [float(x.get("p1_score", 0.0)) for x in pool_items]
        mx = max(sc) if sc else 0.0
        reasons = [str(x.get("淘汰死因", "")) for x in (rejected or []) if x.get("淘汰死因")]
        top5 = Counter(reasons).most_common(5)
        for x in pool_items[:20]:
            c = str(x.get("code", "")).split(".")[0][:6]
            nm = normalize_stock_display_name((x.get("hist") or {}).get("name", c))
            sc = float(x.get("p1_score", 0.0))
            t1, t2, w1, w2 = p1_score_details_to_extreme_labels(x.get("score_details") or {}, sc)
            preview_rows.append(
                {
                    "代码": c,
                    "名称": nm,
                    "分": round(sc, 2),
                    "满分项1": t1,
                    "满分项2": t2,
                    "最低项1": w1,
                    "最低项2": w2,
                }
            )
    else:
        res = scan_pools(
            target_pools=[pool],
            base_items=base_items,
            regime=curr_regime,
            progress_callback=progress_callback,
        )
        lst = res.get(pool) or []
        n_pass = len(lst)
        mx = _max_score_scan_rows(lst)
        gr = ((res.get("funnel") or {}).get(pool) or {}).get("gate_block_reasons") or {}
        if gr:
            top5 = sorted(gr.items(), key=lambda x: -x[1])[:5]
        for x in lst[:20]:
            code = str(x.get("代码", x.get("code", "")))[:8]
            try:
                zf = round(float(x.get("综合分", 0.0) or 0.0), 2)
            except (TypeError, ValueError):
                zf = 0.0
            preview_rows.append(
                {
                    "代码": code,
                    "综合分": zf,
                    "满分项1": "--",
                    "满分项2": "--",
                    "最低项1": "--",
                    "最低项2": "--",
                }
            )

    return n_pass, mx, top5, preview_rows


def update_strategy_lab_prod_baseline() -> None:
    _ensure_lab_state()
    b = st.session_state.get("_strategy_lab_prod_baseline") or {}
    sr = st.session_state.get("scan_results") or {}
    for k in ("p2", "p3", "p4", "p5"):
        lst = sr.get(k) or []
        b[k] = {"count": len(lst), "max_score": _max_score_scan_rows(lst)}
    p1k = "p0_base_items_cache" if st.session_state.get("pool_mode") == "P0" else "p1_base_items_cache"
    p1_items = st.session_state.get(p1k) or []
    scores = [float(x.get("p1_score", 0.0)) for x in p1_items if isinstance(x, dict)]
    b["p1"] = {"count": len(p1_items), "max_score": max(scores) if scores else 0.0}
    st.session_state["_strategy_lab_prod_baseline"] = b


def render_strategy_lab(*, curr_regime: str, progress_placeholder, progress_bar) -> None:
    _ensure_lab_state()
    _sanitize_lab_overrides_core_locked()

    st.markdown("### 🧪 策略实验室 · V26.5")
    with st.expander("📘 V26.5 与系统对齐说明（本页不修改磁盘 config.yaml）", expanded=False):
        st.markdown(
            """
**会话覆写**  
仅写入 `session_state`（`strategy_lab_overrides` / `lab_opr_overrides`），**不**直接改 `config.yaml`。  
合并规则见 `core.config_manager`：与 YAML、侧边栏 Regime 一并生效。

**缩量观察池三键**（上方「缩量观察池放宽」）  
对应 `config.yaml` → `sop_v11.observation_pool_relax`：`vr_shrink_gate`、`large_cap_yi_min`、`turnover_floor_pct`。  
与全市场 `market_contraction_score`、`adaptive_relaxed_golden_gate_ok`、观察池降档入表线联动。

**P2–P5 立即测试 / 平原扫描**  
调用 `service.scan_service.scan_pools`，与**航母指挥舱**同一扫描链。结果侧可对照指挥舱中的：  
`funnel`（战法→黄金门禁→命中→及格分）、`observation`（缩量期备选）、`sop_market_breaker`（指数熔断元数据）。

**市值与风控**  
全市场扫描层：**流通市值不足约 60 亿**标的不会进入战法结果表（与 `pool_manager` 宪法一致）。  
各池输出可含 **风险标签、建议最低综合分**（三层风控软降权 + 引擎 penalty）；**硬否决**在策略/风控引擎内完成，本页滑块对应 **P1 Profile 与 P2–P5 `*ScreenerConfig` / `PostmarketConfig`**，不含 `RiskControlConfig` 全量字段（避免与引擎内嵌阈值重复维护）。

**危险清单**  
指挥舱另表展示 **danger_sell / danger_buy**，与 `core.danger_signal_utils.would_trigger_danger_sell` 及黑名单逻辑对齐；实验室报表**不**单独列出该表。

**离线痛点回测**  
命令行 `python -m core.backtest_runner --mode=painpoint ...` 为 **离线统计**，与 Streamlit 会话无关；用于 AB（`--legacy_mode`）对比，**不**在本页配置。

**1 档回放候选**  
优先使用「上次全量 1 档洗盘候选全集」存档；若仅用已入池底仓，界面会提示与指挥舱缩量上下文可能不一致。
"""
        )

    # 【策略实验室·会话覆写】仅写 session_state，不修改磁盘 config.yaml；核心合并见 config_manager.get_observation_pool_relax_settings
    _opr_yaml = get_observation_pool_relax_settings(ignore_session_overrides=True)
    with st.expander("🔭 缩量观察池放宽（会话覆写，不写盘）", expanded=False):
        st.caption(
            "以下三项对应 sop_v11.observation_pool_relax；调节后立即写入 lab_opr_overrides，"
            "扫描/洗盘路径中 compute_market_contraction_context、adaptive_turnover_kill_threshold_relaxed 等将使用合并后的有效值。"
        )
        c_opr_a, c_opr_b, c_opr_c = st.columns(3)
        with c_opr_a:
            _vr = st.number_input(
                "缩量量比上限",
                min_value=0.51,
                max_value=3.0,
                value=float(_opr_yaml["vr_shrink_gate"]),
                step=0.05,
                format="%.2f",
                key="lab_opr_ni_vr_shrink_gate",
                help="全市场量比中位低于该阈值时视为极端缩量上下文（vr_shrink_gate）。默认来自 YAML，如 0.95。",
            )
        with c_opr_b:
            _cap = st.number_input(
                "豁免大盘市值下限（亿元）",
                min_value=10.0,
                max_value=20000.0,
                value=float(_opr_yaml["large_cap_yi_min"]),
                step=10.0,
                format="%.0f",
                key="lab_opr_ni_large_cap_yi_min",
                help="流通市值达到该下限（亿）的大中盘在资金为正时可走放宽换手逻辑（large_cap_yi_min）。",
            )
        with c_opr_c:
            _tf = st.number_input(
                "资金正向下换手底线（%）",
                min_value=0.05,
                max_value=5.0,
                value=float(_opr_yaml["turnover_floor_pct"]),
                step=0.05,
                format="%.2f",
                key="lab_opr_ni_turnover_floor_pct",
                help="缩量环境下 adaptive 换手阈值的下限（turnover_floor_pct），百分比口径：0.56 即 0.56%。",
            )
        st.session_state["lab_opr_overrides"] = {
            "vr_shrink_gate": float(_vr),
            "large_cap_yi_min": float(_cap),
            "turnover_floor_pct": float(_tf),
        }
        _opr_eff = get_observation_pool_relax_settings()
        st.caption(
            f"**当前生效（YAML+会话合并后）**：量比门 {_opr_eff['vr_shrink_gate']:.2f}，"
            f"大盘亿下限 {_opr_eff['large_cap_yi_min']:.0f}，换手底线 {_opr_eff['turnover_floor_pct']:.2f}"
        )

    pool_list = ["p1", "p2", "p3", "p4", "p5"]
    pool = st.selectbox("池", pool_list, format_func=lambda k: POOL_LABELS.get(k, k), key="strategy_lab_active_pool")

    prev_pool = st.session_state.get("_strategy_lab_prev_pool_sel")
    if prev_pool != pool:
        st.session_state.pop(_STRATEGY_LAB_SNAPSHOT_KEY, None)
    st.session_state["_strategy_lab_prev_pool_sel"] = pool

    base_key = "p0_base_items_cache" if st.session_state.get("pool_mode") == "P0" else "p1_base_items_cache"
    base_items = st.session_state.get(base_key) or []

    pk_active, prof_defaults = _flatten_p1_active_profile(curr_regime)
    p1_keys = _p1_profile_keys()
    p1_idx = p1_keys.index(pk_active) if pk_active in p1_keys else min(1, len(p1_keys) - 1) if p1_keys else 0

    p1_prof = "neutral"
    if pool == "p1":
        if "strategy_lab_p1_profile" not in st.session_state:
            st.session_state["strategy_lab_p1_profile"] = p1_keys[p1_idx] if p1_keys else "neutral"
        elif st.session_state["strategy_lab_p1_profile"] not in p1_keys:
            st.session_state["strategy_lab_p1_profile"] = p1_keys[0] if p1_keys else "neutral"
        p1_prof = st.selectbox("1档·档位（Profile）", p1_keys, format_func=p1_profile_cn, key="strategy_lab_p1_profile")

    s = get_strategies_dict()
    field_names: List[str] = []
    yaml_defaults: Dict[str, Any] = {}
    types_int: Dict[str, bool] = {}

    if pool == "p1":
        yaml_prof = ((s.get("p1") or {}).get("profiles") or {}).get(p1_prof) or {}
        lab_root = (st.session_state.get("strategy_lab_overrides") or {}).get("p1") or {}
        lab_prof = ((lab_root.get("profiles") or {}).get(p1_prof)) or {}
        field_names = sorted(_allowed_p1_profile_param_keys(p1_prof))
        for fn in field_names:
            yaml_defaults[fn] = yaml_prof.get(fn, prof_defaults.get(fn, 0.0))
            types_int[fn] = False
        for fn in field_names:
            if fn in lab_prof:
                yaml_defaults[fn] = lab_prof[fn]
    else:
        cls = _get_config_cls(pool)
        merged = _merged_flat_pool_values(pool)
        field_names = [f.name for f in fields(cls)]
        for fn in field_names:
            yaml_defaults[fn] = merged.get(fn, 0.0)
            types_int[fn] = _field_is_int_for_dataclass(cls, fn)

    wprof = p1_prof if pool == "p1" else ""
    _init_lab_widget_scalars(pool, wprof, field_names, yaml_defaults, types_int)

    with st.form(f"lab_run_{pool}"):
        for fn in field_names:
            yd = yaml_defaults.get(fn, 0.0)
            _render_number_input_bounded(
                pool=pool,
                p1_prof=wprof,
                field_key=fn,
                yaml_default=yd,
                is_int=types_int.get(fn, False),
            )
        go = st.form_submit_button("🚀 立即测试")

    with st.expander("📊 智能参数平原扫描 (防过拟合雷达)", expanded=False):
        if not field_names:
            st.caption("当前池无可扫参数")
        else:
            scan_param = st.selectbox(
                "扫描变量",
                field_names,
                format_func=label_zh,
                key=f"lab_sweep_sel_{pool}",
            )
            prev_sel_k = f"_lab_sweep_prev_sel_{pool}"
            if (
                st.session_state.get(prev_sel_k) != scan_param
                or f"lab_sweep_start_{pool}" not in st.session_state
            ):
                yd_sp = yaml_defaults.get(scan_param, 0.0)
                is_sp = types_int.get(scan_param, False)
                lo_b, hi_b = bounds_for(scan_param, float(yd_sp), is_sp)
                if is_sp:
                    ia, ib = int(round(lo_b)), int(round(hi_b))
                    if ib < ia:
                        ia, ib = ib, ia
                    st.session_state[f"lab_sweep_start_{pool}"] = ia
                    st.session_state[f"lab_sweep_end_{pool}"] = ib
                    span = ib - ia
                    st.session_state[f"lab_sweep_step_{pool}"] = max(1, max(1, span // 10) if span else 1)
                else:
                    st.session_state[f"lab_sweep_start_{pool}"] = float(lo_b)
                    st.session_state[f"lab_sweep_end_{pool}"] = float(hi_b)
                    st.session_state[f"lab_sweep_step_{pool}"] = lab_input_step(
                        float(yd_sp), float(lo_b), float(hi_b), False
                    )
                st.session_state[prev_sel_k] = scan_param

            is_sp_in = types_int.get(scan_param, False)
            yd_sw = float(yaml_defaults.get(scan_param, 0.0) or 0.0)
            lo_sw, hi_sw = bounds_for(scan_param, yd_sw, is_sp_in)
            fi_adj_step = (
                1.0
                if is_sp_in
                else float(lab_input_step(yd_sw, float(lo_sw), float(hi_sw), False))
            )
            c1, c2, c3 = st.columns(3)
            with c1:
                if is_sp_in:
                    st.number_input(
                        "扫描起始值",
                        min_value=-10**9,
                        max_value=10**9,
                        step=1,
                        key=f"lab_sweep_start_{pool}",
                    )
                else:
                    st.number_input(
                        "扫描起始值",
                        min_value=-1e15,
                        max_value=1e15,
                        step=fi_adj_step,
                        format="%.6g",
                        key=f"lab_sweep_start_{pool}",
                    )
            with c2:
                if is_sp_in:
                    st.number_input(
                        "扫描结束值",
                        min_value=-10**9,
                        max_value=10**9,
                        step=1,
                        key=f"lab_sweep_end_{pool}",
                    )
                else:
                    st.number_input(
                        "扫描结束值",
                        min_value=-1e15,
                        max_value=1e15,
                        step=fi_adj_step,
                        format="%.6g",
                        key=f"lab_sweep_end_{pool}",
                    )
            with c3:
                if is_sp_in:
                    st.number_input(
                        "扫描步长",
                        min_value=1,
                        max_value=10**9,
                        step=1,
                        key=f"lab_sweep_step_{pool}",
                    )
                else:
                    st.number_input(
                        "扫描步长",
                        min_value=1e-12,
                        max_value=1e15,
                        step=max(fi_adj_step * 0.1, 1e-9),
                        format="%.6g",
                        key=f"lab_sweep_step_{pool}",
                    )

            run_sweep = st.button("🧬 开始全景扫描", key=f"lab_sweep_btn_{pool}", width="stretch")

            if run_sweep:
                _wc, _ = _load_p1_last_wash_codes_meta_from_disk()
                _sess_c = st.session_state.get("p1_last_wash_input_codes") or []
                if pool == "p1" and not _sess_c and not _wc and not base_items:
                    st.error("请先执行全量 1档·洗盘以生成候选存档，或保留底仓缓存后再扫描。")
                elif pool != "p1" and not base_items:
                    st.error("底仓缓存空，无法扫描")
                else:
                    s0 = float(st.session_state[f"lab_sweep_start_{pool}"])
                    e0 = float(st.session_state[f"lab_sweep_end_{pool}"])
                    stp0 = float(st.session_state[f"lab_sweep_step_{pool}"])
                    seq, truncated = _lab_build_sweep_values(s0, e0, stp0, is_sp_in)
                    if truncated:
                        st.warning(f"扫描点数超过上限 {_MAX_SWEEP_POINTS}，已截断")
                    if not seq:
                        st.warning("扫描序列为空，请检查起止与步长")
                    else:
                        backup = copy.deepcopy(st.session_state.get("strategy_lab_overrides") or {})
                        try:
                            baseline = _lab_collect_widget_merged(
                                pool, wprof, field_names, yaml_defaults, types_int
                            )
                            rows_out: List[Dict[str, Any]] = []
                            prog_bar = st.progress(0)
                            ntot = len(seq)
                            yd0 = yaml_defaults.get(scan_param, 0.0)
                            for i, val in enumerate(seq):
                                merged_i = dict(baseline)
                                merged_i[scan_param] = clamp_value(
                                    scan_param,
                                    float(val),
                                    float(yd0),
                                    is_sp_in,
                                )
                                if pool == "p1":
                                    _merge_p1_lab_profile(p1_prof, merged_i)
                                else:
                                    _merge_flat_lab_pool(pool, merged_i)
                                n_pass_i, mx_i, _, _ = _lab_run_scan_numbers(
                                    pool=pool,
                                    p1_prof=p1_prof,
                                    merged_out=merged_i,
                                    curr_regime=curr_regime,
                                    base_items=base_items,
                                    progress_callback=None,
                                )
                                rows_out.append(
                                    {
                                        "param_val": float(merged_i[scan_param]),
                                        "pass_count": int(n_pass_i),
                                        "max_score": round(float(mx_i), 4),
                                    }
                                )
                                prog_bar.progress(min(1.0, (i + 1) / max(ntot, 1)))
                            prog_bar.progress(1.0)
                            st.session_state["_strategy_lab_sweep_cache"] = {
                                "pool": pool,
                                "param": scan_param,
                                "rows": rows_out,
                            }
                        finally:
                            st.session_state["strategy_lab_overrides"] = backup
                            _sanitize_lab_overrides_core_locked()

            cache = st.session_state.get("_strategy_lab_sweep_cache")
            if cache and str(cache.get("pool")) == str(pool) and cache.get("rows"):
                st.markdown(
                    "💡 **架构师提示**：寻找【入池数】与【最高分】变化平缓的连续区间（稳健平原）。"
                    "若数据呈断崖式突变，即为过拟合悬崖，实盘禁用！"
                )
                df_sw = pd.DataFrame(cache["rows"])
                df_sw_disp = rename_columns_for_display(df_sw, LAB_SWEEP_RESULT_COLUMNS)
                xp = str(cache.get("param") or "")
                x_title = label_zh(xp) if xp else "参数值"
                base_ch = alt.Chart(df_sw_disp).encode(
                    x=alt.X("参数值:Q", title=x_title),
                    tooltip=["参数值", "入池数", "最高分"],
                )
                bar_ch = base_ch.mark_bar(opacity=0.38, color="#4c78a8").encode(
                    y=alt.Y("入池数:Q", title="入池数"),
                )
                line_ch = base_ch.mark_line(color="#f58518", strokeWidth=2).encode(
                    y=alt.Y("最高分:Q", title="最高分"),
                )
                st.altair_chart(
                    alt.layer(bar_ch, line_ch).resolve_scale(y="independent").properties(height=320),
                    width="stretch",
                )
                st.dataframe(style_dataframe_center(df_sw_disp), width="stretch", hide_index=True)

    if go:
        merged_out = _lab_collect_widget_merged(pool, wprof, field_names, yaml_defaults, types_int)
        if pool == "p1":
            _merge_p1_lab_profile(p1_prof, merged_out)
        else:
            _merge_flat_lab_pool(pool, merged_out)

        _wc_go, _ = _load_p1_last_wash_codes_meta_from_disk()
        _sess_c_go = st.session_state.get("p1_last_wash_input_codes") or []
        if pool == "p1" and not _sess_c_go and not _wc_go and not base_items:
            st.error("请先执行全量 1档·洗盘以生成候选存档，或保留底仓缓存。")
            st.session_state.pop(_STRATEGY_LAB_SNAPSHOT_KEY, None)
        elif pool != "p1" and not base_items:
            st.error("底仓缓存空")
            st.session_state.pop(_STRATEGY_LAB_SNAPSHOT_KEY, None)
        else:
            prod: Dict[str, Any] = (st.session_state.get("_strategy_lab_prod_baseline") or {}).get(pool) or {}
            merged_cfg: Dict[str, Any] = dict(merged_out)
            spinner_msg = "1档…" if pool == "p1" else f"{pool_cn(pool)}…"
            with st.spinner(spinner_msg):
                n_pass, mx, top5, preview_rows = _lab_run_scan_numbers(
                    pool=pool,
                    p1_prof=p1_prof,
                    merged_out=merged_out,
                    curr_regime=curr_regime,
                    base_items=base_items,
                    progress_callback=progress_placeholder.info if progress_placeholder else None,
                )
            if pool == "p1":
                full_t = get_p1_thresholds_for_profile(curr_regime, p1_prof)
                for k, v in merged_out.items():
                    full_t[k] = float(v)
                merged_cfg = {**{k: float(v) for k, v in full_t.items()}, "_lab_profile_choice": p1_prof}

            st.session_state[_STRATEGY_LAB_SNAPSHOT_KEY] = {
                "pool_key": pool,
                "regime": str(curr_regime or ""),
                "merged_cfg": merged_cfg,
                "pass_count": n_pass,
                "max_score": float(mx),
                "top_reasons": dict(top5),
                "top5_ordered": list(top5),
                "preview_rows": preview_rows,
                "prod": prod,
            }
            _render_lab_report(pass_count=n_pass, max_score=mx, prod=prod, top5=top5, preview_rows=preview_rows)
            if pool == "p1" and st.session_state.get("_p1_lab_last_resolve_hint") == "legacy_in_pool_only_mismatch_risk":
                st.warning(
                    "未找到「上次 1档·洗盘候选全集」存档（请先执行一次全量洗盘）。"
                    "当前实验室仅用已入池底仓近似回放，缩量上下文与入池数可能与指挥舱不一致。"
                )

    elif st.session_state.get(_STRATEGY_LAB_SNAPSHOT_KEY):
        snap = st.session_state[_STRATEGY_LAB_SNAPSHOT_KEY]
        if str(snap.get("pool_key")) == str(pool):
            _render_lab_report(
                pass_count=int(snap.get("pass_count", 0)),
                max_score=float(snap.get("max_score", 0.0)),
                prod=dict(snap.get("prod") or {}),
                top5=list(snap.get("top5_ordered") or []),
                preview_rows=list(snap.get("preview_rows") or []),
            )

    snap = st.session_state.get(_STRATEGY_LAB_SNAPSHOT_KEY)
    if snap and str(snap.get("pool_key")) == str(pool):
        save_note = st.text_input("备注", value="", key="lab_experiment_note_input")
        if st.button("💾 存档", key="lab_save_experiment_btn", width="stretch"):
            eid = save_experiment_record(
                pool_key=str(pool),
                regime=str(snap.get("regime") or curr_regime or ""),
                parameters=dict(snap.get("merged_cfg") or {}),
                pass_count=int(snap.get("pass_count", 0)),
                max_score=float(snap.get("max_score", 0.0)),
                top_reasons=dict(snap.get("top_reasons") or {}),
                note=str(save_note or "").strip(),
            )
            if eid:
                st.success("已保存")
            else:
                st.error("保存失败")

    with st.expander("📜 历史", expanded=False):
        hist_pool = st.selectbox(
            "筛选档位",
            ["全部", "p1", "p2", "p3", "p4", "p5"],
            format_func=lambda k: "全部" if k == "全部" else pool_cn(k),
            key="lab_hist_filter_pool",
        )
        hp = None if hist_pool == "全部" else hist_pool
        hist_df = get_experiment_history(pool_key=hp)
        if hist_df is None or hist_df.empty:
            st.caption("无记录")
        else:
            st.dataframe(style_dataframe_center(_history_display_df(hist_df)), width="stretch", hide_index=True)
            opts = list(range(len(hist_df)))
            pick = st.selectbox("选行", opts, format_func=lambda i: _history_row_label(hist_df, int(i)), key="lab_hist_pick")
            if st.button("⚡ 加载参数", key="lab_hist_load_btn", width="stretch"):
                row = hist_df.iloc[int(pick)]
                row_pool = str(row.get("pool_key") or "p1").strip().lower()
                d = _lab_json_to_dict(row.get("parameters"))
                if not d:
                    st.warning("参数无效")
                else:
                    st.session_state["strategy_lab_active_pool"] = row_pool if row_pool in pool_list else "p1"
                    yaml_d: Dict[str, Any] = {}
                    ti: Dict[str, bool] = {}
                    p1p = str(d.get("_lab_profile_choice") or st.session_state.get("strategy_lab_p1_profile") or "neutral")

                    if row_pool == "p1":
                        if p1p not in _p1_profile_keys():
                            p1p = _p1_profile_keys()[0]
                        st.session_state["strategy_lab_p1_profile"] = p1p
                        allowed = _allowed_p1_profile_param_keys(p1p)
                        yp = ((get_strategies_dict().get("p1") or {}).get("profiles") or {}).get(p1p) or {}
                        flat: Dict[str, float] = {}
                        for kk, vv in d.items():
                            if kk in ("_lab_profile_choice",) or kk not in allowed:
                                continue
                            try:
                                flat[str(kk)] = float(vv)
                            except (TypeError, ValueError):
                                continue
                        for kk in allowed:
                            yaml_d[kk] = yp.get(kk, flat.get(kk, 0.0))
                            ti[kk] = False
                        _merge_p1_lab_profile(p1p, flat)
                        _sync_widgets_from_flat("p1", p1p, flat, yaml_d, ti)
                    elif row_pool in ("p2", "p3", "p4", "p5"):
                        cls = _get_config_cls(row_pool)
                        allowed = _allowed_flat_pool_override_keys(row_pool)
                        flat = {}
                        for kk, vv in d.items():
                            if kk == "_lab_profile_choice" or kk not in allowed:
                                continue
                            try:
                                flat[str(kk)] = float(vv)
                            except (TypeError, ValueError):
                                continue
                        mv = _merged_flat_pool_values(row_pool)
                        for kk in fields(cls):
                            fn = kk.name
                            yaml_d[fn] = mv.get(fn, 0.0)
                            ti[fn] = _field_is_int_for_dataclass(cls, fn)
                        _merge_flat_lab_pool(row_pool, flat)
                        _sync_widgets_from_flat(row_pool, "", flat, yaml_d, ti)
                    st.rerun()
