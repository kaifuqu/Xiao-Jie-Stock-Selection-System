# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 - 扫描服务层：把 UI 中的「战区扫描逻辑」抽离出来。

目标：
1) UI 只负责状态管理与渲染；同步编排仍走本模块 scan_pools。
2) 指挥舱 **仅 P3/P4** 可走 `service.async_scan_bridge`（pending JSON + auto_sniper_daemon 或 UI 内嵌 worker），
   其它档位（P2/P5）或与 P3/P4 混扫时仍调用本模块同步路径，避免结果割裂。
3) 扫描引擎可在 CLI/定时任务/单测中复用同一套函数。
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Any, Optional

from core.scan_engine import run_scan_engine, get_realtime_sector_ranking


def _empty_scan_result_shape(target_pools: List[str]) -> Dict[str, Any]:
    """
    与 run_scan_engine 成功返回的顶层键结构对齐（无底仓/引擎异常时仍安全解构）。
    """
    pools = list(target_pools or [])
    out: Dict[str, Any] = {k: [] for k in pools}
    out["danger_buy"] = []
    out["danger_sell"] = []
    out["funnel"] = {
        k: {
            "total_candidates": 0,
            "enter_strategy_check": 0,
            "pass_golden_gate": 0,
            "hit_strategy": 0,
            "pass_score": 0,
            "gate_block_reasons": {},
        }
        for k in pools
    }
    out["p1_prescreen"] = {
        "pass_line": 50.0,
        "smooth_blocked": 0,
        "mv_skipped_rollbacks": 0,
        "gate_reasons": {},
    }
    out["observation"] = {k: [] for k in pools}
    out["sector_rank"] = {}
    out["sop_market_breaker"] = {}
    return out


def scan_pools(
    target_pools: List[str],
    base_items: List[Dict[str, Any]],
    regime: str,
    progress_callback: Optional[Callable[[Any], Any]] = None,
) -> Dict[str, Any]:
    """
    执行指定战区扫描，并把返回数据“对齐到 UI 期望的 key 结构”。
    """
    empty = _empty_scan_result_shape(target_pools)

    if not base_items:
        return empty

    try:
        results = run_scan_engine(
            target_pools=target_pools,
            base_items=base_items,
            regime=regime,
            progress_callback=progress_callback,
        )
        out = {k: results.get(k, []) for k in target_pools}
        out["danger_buy"] = results.get("danger_buy", [])
        out["danger_sell"] = results.get("danger_sell", [])
        out["funnel"] = results.get("funnel", {}) or empty["funnel"]
        out["observation"] = results.get("observation") or empty["observation"]
        out["sop_market_breaker"] = results.get("sop_market_breaker") or {}
        out["p1_prescreen"] = results.get("p1_prescreen") or empty["p1_prescreen"]
        out["sector_rank"] = results.get("sector_rank") or {}
        return out
    except Exception:
        logging.exception("scan_pools: run_scan_engine 失败，返回空扫描结果")
        return _empty_scan_result_shape(target_pools)

