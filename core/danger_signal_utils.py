# -*- coding: utf-8 -*-
"""
高危斩仓（danger_sell）几何条件统一出口。

设计目标：
- 实盘 scan_engine 与专项回测共用同一套函数，禁止在多处复制止损公式导致分叉。
- 本模块仅依赖 pandas 与标量运算，不访问 DuckDB、不修改全局状态。
- 触发结果仅进入 UI「高危斩仓」列表与标签展示；**不**再写入黑名单或触发自动下单（底层零干预）。

对外 API：
- check_stop_loss：原 scan_engine._check_stop_loss 的完整迁移；
- would_trigger_danger_sell：check_stop_loss 为真，或「现价 < MA20 且当日跌幅 < -4%」。
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Tuple

import pandas as pd

_logger = logging.getLogger(__name__)


def _ds_safe_float(val: Any, default: float = 0.0) -> float:
    """
    标量安全转 float：非有限数、脏字符串一律回退 default。
    【V26.6 优化】添加 float/int 的 isinstance 快速路径，
    避免对已经是数值类型的值也走 pd.to_numeric 构造路径。
    """
    # 【V26.6 优化】快速通道：Python 内置数值类型直接返回
    if isinstance(val, (int, float)):
        x = float(val)
        if x != x or x in (float("inf"), float("-inf")):
            return float(default)
        return x
    # 兜底：对字符串等其他类型走 pd.to_numeric 路径
    try:
        x = float(pd.to_numeric(val, errors="coerce"))
        if x != x or x in (float("inf"), float("-inf")):
            return float(default)
        return x
    except Exception:
        return float(default)


def check_stop_loss(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    size_emoji: str,
    holding_days: int,
    vol_z: float,
    ind_rank: int,
) -> Tuple[bool, str]:
    """
    分层止损与结构止损（与历史 scan_engine 逻辑逐行对齐）。

    参数与 scan_engine 原 _check_stop_loss 一致。
    """
    try:
        if df is None or len(df) < 20:
            return False, ""

        curr = df.iloc[-1]
        now_price = _ds_safe_float(rt.get("price", 0.0))
        if now_price <= 0:
            now_price = _ds_safe_float(curr.get("close", 0.0))

        if holding_days >= 2:
            ma5 = _ds_safe_float(curr.get("ma5", 0.0))
            macd_bar = _ds_safe_float(curr.get("macd_bar", curr.get("macd_hist", 0.0)))
            if ma5 > 0 and now_price < ma5 and macd_bar < 0:
                return True, "⏳ 趋势走弱(破MA5+绿柱)无条件离场"

        # 强势反攻日豁免（仅跳过 ATR/MA20「结构斩仓」，不覆盖上方 MA5 离场）：
        # 当日涨幅与「是否仍低于 20 日高点回撤线 / MA20」无关，易出现「涨幅列很红仍进斩仓区」的错觉。
        # 若当日已属明显反弹且实时价突破昨日最高价，视为资金主动修复，不再提示结构破位斩仓。
        pre_day = _ds_safe_float(rt.get("pre_close"), 0.0)
        if pre_day <= 0 and len(df) >= 2:
            pre_day = _ds_safe_float(df.iloc[-2].get("close"), 0.0)
        if pre_day <= 0:
            pre_day = _ds_safe_float(curr.get("pre_close"), 0.0)
        if pre_day > 0 and len(df) >= 2:
            day_pct = (now_price - pre_day) / pre_day * 100.0
            prev_high = _ds_safe_float(df.iloc[-2].get("high"), 0.0)
            if day_pct >= 5.0 and prev_high > 0 and now_price >= prev_high * 1.001:
                return False, ""

        atr20 = _ds_safe_float(
            curr.get("atr20", curr.get("atr", now_price * 0.03)),
            default=now_price * 0.03,
        )
        ma20 = _ds_safe_float(curr.get("ma20", 0.0))

        df_tail_20 = df.tail(20).copy()
        recent_high = _ds_safe_float(df_tail_20["high"].max())

        if recent_high <= 0:
            return False, ""

        # 近 20 日最高价回撤：止损比例随 ATR 放大，且不低于 12%（与原 scan 一致）
        stop_loss_pct = max(0.12, (2.5 * atr20) / recent_high)
        stop_price = recent_high * (1.0 - stop_loss_pct)

        is_atr_broken = now_price < stop_price
        is_ma20_broken = now_price < ma20

        if size_emoji in ["🦍", "🐘"]:
            if is_atr_broken and is_ma20_broken:
                return True, "🚨 双重破位 (破ATR极值 + 破MA20)"
            if is_atr_broken:
                return True, "🚨 跌穿ATR防线 (高位回撤过大)"
            if is_ma20_broken:
                return True, "🩸 跌破MA20生命线 (趋势走坏)"
            return False, ""

        if size_emoji == "🐎":
            if is_atr_broken and is_ma20_broken:
                return True, "🚨 极危双破 (破ATR + 破MA20)"
            if holding_days >= 3:
                is_structure_strong = (vol_z >= 2.0) or (ind_rank <= 3)
                cost_line = df["close"].tail(3).mean() if len(df) >= 3 else ma20
                is_escaped_cost = now_price > (cost_line * 1.02)
                if not is_escaped_cost and not is_structure_strong:
                    return True, "⏳ 3日未脱离成本且无结构强化 (时间止损)"
            if is_ma20_broken:
                return True, "🩸 跌破MA20生命线 (短线走弱)"

        return False, ""
    except Exception as e:
        _logger.debug("check_stop_loss 异常: %s", e, exc_info=True)
        return False, ""


def would_trigger_danger_sell(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    size_emoji: str,
    holding_days: int,
    vol_z: float,
    ind_rank: int,
) -> Tuple[bool, str]:
    """
    是否应进入 UI「无条件斩仓区」danger_sell 列表。

    逻辑 = check_stop_loss 为真，或（现价 < MA20 且当日相对昨收跌幅 < -4%）。
    第二分支与 scan_engine 原条件一致，用于捕获单日暴跌破位。
    check_stop_loss 内对「当日强反弹且突破昨日高」有豁免，避免与涨幅列语义冲突（见函数内注释）。
    """
    is_sl, reason_sl = check_stop_loss(df, rt, size_emoji, holding_days, vol_z, ind_rank)
    if is_sl:
        return True, reason_sl

    if df is None or df.empty:
        return False, ""

    curr = df.iloc[-1]
    now_price = _ds_safe_float(rt.get("price", 0.0))
    if now_price <= 0:
        now_price = _ds_safe_float(curr.get("close", 0.0))

    pre_price = _ds_safe_float(rt.get("pre_close"), 0.0)
    if pre_price <= 0:
        pre_price = _ds_safe_float(
            curr.get("pre_close"),
            _ds_safe_float(curr.get("close"), 0.0),
        )
    if pre_price <= 0:
        return False, ""

    pct = (now_price - pre_price) / pre_price * 100.0
    ma20_line = _ds_safe_float(curr.get("ma20", 0.0))
    if ma20_line > 0 and now_price < ma20_line and pct < -4.0:
        return True, "🩸 严重破位MA20强支撑 (单日暴跌)"
    return False, ""


def size_emoji_from_circ_mv_wan(circ_mv_wan: float) -> str:
    """
    与 scan_engine 市值分档一致，用于回测构造 size_emoji（止损分层依赖）。
    circ_mv：万元；circ_mv_yi = circ_mv_wan / 10000 = 亿元。
    """
    circ_mv_yi = float(circ_mv_wan) / 10000.0
    if circ_mv_yi >= 2000.0:
        return "🦍"
    if circ_mv_yi >= 1000.0:
        return "🐘+"
    if circ_mv_yi >= 500.0:
        return "🐘"
    if circ_mv_yi >= 100.0:
        return "🐎"
    return "🐥"
