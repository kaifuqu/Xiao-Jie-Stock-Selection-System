# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.7 — P4 尾盘选股池「物理胸甲」硬阈值筛选模块（建议运行窗口 14:30–15:00）
================================================================================
【V26.5 资金口径】主力/特大单/机构净买门槛 = max(流通市值(元)×比例, 市值阶梯地板)，禁止单一绝对金额一刀切。
【V26.5 换手口径】100% 使用真实自由换手 turnover_rate_f；缺失时用 vol(手)×close/circ_mv(万) 反算，禁止 turnover_rate。

【V26.7 核心优化】
1. 流通市值下限强制提升至 100 亿(1,000,000 万元)，彻底封死袖珍盘漏洞。
2. 新增尾盘 VWAP 量纲安全计算: _safe_vwap_from_y / _safe_vwap_from_rt_dimension_check，
   对 TuShare 日线 amt(千元)/vol(手) 量纲错位做双重校验，偏离昨收超过 20% 时降级到昨收价，
   防止"尾盘偷袭骗线否决"防线因量纲错误而失效。

【V26.5 A股特殊场景处理】
- 涨跌停宽容：涨跌停时量比萎缩/上影过长属于正常现象，涨跌停标记（_is_limit=UP/DOWN）由
  scan_engine._build_rt_entry 写入，各策略据此放宽否决条件。
- 尾盘增量比基准优化：14:30之后用"尾盘前段(13:00-14:30)均量"作为基准，而非全日均量，
  更精准识别尾盘异动与真实承接。
- 自适应换手率：根据流通市值档位动态调整换手率合理区间（大蓝筹更宽松）。
- 集合竞价修正：9:25-9:30 竞价期用昨日均量替代5日均量作为量比基准。
- 量纲安全 VWAP：日线 amt/vol 量纲需 amt*10/vol 转换，偏离昨收 20% 时降级。

对齐 data_fetcher.ALL_55_COLS；全局一票否决 + 十一大客观策略（满足任一即打标签）。
================================================================================

【策略清单 P4-01 ~ P4-11】
P4-01 ·★ 光头阳线抢筹     — 昨日强势光头阳 + 今日延续 + 尾盘仍在高位
P4-02 ·★ 筹码锁死创新高   — 突破20日高点 + 缩量整理
P4-03 ·★ 机构尾盘潜伏     — 主力净买 + 机构净买
P4-04 ·★ 均线缩量低吸     — 多头排列 + 回踩均线 + 极致缩量
P4-05 ·★ 强势洗盘承接     — 主力净流入 + 回踩MA5 强势洗盘后再次承接
P4-06 ·★ 动能突破共振     — 突破20日高 + 主力确认 + 多头排列
P4-07 ·★ 底仓不破均线     — P1底仓持续稳定 + 尾盘仍强
P4-08 ·★ 温和均线修复     — 量价齐升 + MA20收复 + MACD回暖
P4-09 ·★ 均线多头回踩企稳  — 四线多头(5/10/20/60) + 近窗回踩MA20 + 量能回暖
P4-10 ·★ 沿5日线主升缩量  — 四线多头 + 近窗缩量踩MA5 + 今日再放量上攻
P4-11 ·★ P1底仓不破均线   — P1底仓稳定性验证 + 回踩不破 + 尾盘稳在均线上
================================================================================
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.strategies.fund_mv_utils import (
    adaptive_turnover_f_range,
    circ_mv_wan_from,
    dynamic_inst_single_threshold_yuan,
    dynamic_net_main_threshold_yuan,
    effective_turnover_rate_f,
)

logger = logging.getLogger(__name__)

try:
    import constants as _C
    _P1_MIN_CIRC_MV_WAN = float(getattr(_C, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000))
except Exception:
    _P1_MIN_CIRC_MV_WAN = 1_000_000.0


# =============================================================================
# 策略配置脚手架
# =============================================================================
@dataclass
class P4TailScreenerConfig:
    """
    P4 尾盘阈值脚手架。
    资金类：*_ratio_of_float_mv 表示占流通市值(元)的比例，与阶梯地板取 max。
    换手类：阈值针对 turnover_rate_f（真实换手，通常高于总股本换手）。
    """

    # ---------- 全局否决（p4_global_risk_veto）----------
    circ_mv_min_wan: float = _P1_MIN_CIRC_MV_WAN
    global_fund_reflow_pct_max: float = 0.8
    global_upper_shadow_ratio_max: float = 0.028
    global_upper_turnover_f_min: float = 14.0
    global_upper_shadow_limit_tolerance_pct: float = 3.5

    ma60_downtrend_rel: float = 0.98

    # ---------- P4-01 光头阳线抢筹 ----------
    s1_close_to_high_min: float = 0.99
    s1_pct_low: float = 3.0
    s1_pct_high: float = 6.0
    s1_prev_pct_low: float = 3.0
    s1_prev_pct_high: float = 6.0
    s1_vol_ratio_min: float = 1.5
    s1_winner_min: float = 85.0
    s1_ma20_slope_min: float = 0.5

    # ---------- P4-02 筹码锁死创新高 ----------
    s2_winner_min: float = 90.0
    s2_turnover_f_low: float = 1.5
    s2_turnover_f_high: float = 10.0
    s2_prev_pct_low: float = 3.0
    s2_prev_pct_high: float = 6.0

    # ---------- P4-03 机构尾盘潜伏 ----------
    s3_net_main_ratio_of_float_mv: float = 0.0004
    s3_inst_net_buy_ratio_of_float_mv: float = 0.00008
    s3_pct_low: float = 0.5
    s3_pct_high: float = 3.0

    # ---------- P4-04 均线缩量低吸 ----------
    s4_vol_shrink_ratio: float = 0.8
    s4_pct_low: float = -1.0
    s4_pct_high: float = 2.0
    s4_touch_ma_eps: float = 1.01
    s4_winner_fly_lt: float = 80.0
    s4_prev_pct_low: float = 3.0
    s4_prev_pct_high: float = 6.0

    # ---------- P4-05 强势洗盘承接 ----------
    s7_net_main_ratio_of_float_mv: float = 0.00025
    s7_ma5_touch_eps: float = 1.005
    s7_pct_low: float = 1.0
    s7_pct_high: float = 4.0
    s7_vol_ratio_min: float = 1.2
    s7_turnover_f_fly_gt: float = 18.0

    # ---------- P4-06 动能突破共振 ----------
    s6_momentum_prev_pct_low: float = 3.0
    s6_momentum_prev_pct_high: float = 6.0
    s6_momentum_pct_low: float = 1.0
    s6_momentum_pct_high: float = 5.0
    s6_momentum_atr_pct_fly_gt: float = 6.0
    s6_momentum_bias_fly_gt: float = 12.0
    s6_momentum_net_main_ratio_of_float_mv: float = 0.00022
    s6_momentum_min_vol_vs_vma5: float = 0.45

    # ---------- P4-07 底仓不破均线（复用 P4-11 参数） ----------
    s11_p1_score_min: float = 76.0
    s11_vwap_hold_ratio_min: float = 0.78
    s11_ma5_hold_ratio_min: float = 0.72
    s11_ma20_hold_ratio_min: float = 0.65
    s11_reclaim_max: int = 1
    s11_pullback_depth_max_pct: float = 1.4
    s11_intraday_dd_max_pct: float = 2.2
    s11_vol_ratio_low: float = 1.0
    s11_vol_ratio_high: float = 2.6
    s11_sector_strength_min: float = 1.0
    s11_mainline_score_min: float = 0.0
    s11_tail_vwap_dev_min: float = 0.0
    s11_tail_vwap_dev_max: float = 2.0
    s11_upper_shadow_max_pct: float = 2.0
    s11_hold_bars_min_ratio: float = 0.62

    # ---------- P4-08 温和均线修复（量价齐升 + MA20收复 + MACD回暖） ----------
    s8_pct_low: float = 1.0
    s8_pct_high: float = 3.0
    s8_mhist_expand_min_ratio: float = 1.05
    s8_winner_min: float = 80.0
    s8_turnover_f_min: float = 0.8
    s8_prev_pct_low: float = 1.0
    s8_prev_pct_high: float = 5.0

    # ---------- P4-09 均线多头回踩企稳 ----------
    s9_touch_lookback_bars: int = 6
    s9_touch_ma20_max_mult: float = 1.018
    s9_pct_low: float = 0.3
    s9_pct_high: float = 5.0
    s9_vr_min: float = 1.05
    s9_winner_min: float = 78.0
    s9_ma20_slope_min: float = 0.12
    s9_vol_confirm_vr_extra: float = 0.08
    s9_prev_pct_low: float = 1.0
    s9_prev_pct_high: float = 5.0

    # ---------- P4-10 沿5日线主升缩量再攻 ----------
    s10_ma5_touch_eps: float = 1.014
    s10_shrink_vs_vma5_max: float = 0.88
    s10_pullback_lookback: int = 5
    s10_pct_low: float = 0.35
    s10_pct_high: float = 5.8
    s10_vr_min: float = 1.03
    s10_winner_min: float = 76.0
    s10_ma20_slope_min: float = 0.08
    s10_prev_pct_low: float = 1.0
    s10_prev_pct_high: float = 5.0

    # ---------- P4-11 P1底仓不破均线 ----------
    # 复用 P4-07 的 s11_* 配置（P4-07 与 P4-11 本质相同，P4-07 强调底仓稳定，P4-11 强调不破均线）
    # 为清晰区分，P4-11 使用独立的 mainline_score 门限
    s11_mainline_score_required: float = 3.0
    s11_reclaim_allow_max: int = 1


# =============================================================================
# 工具函数
# =============================================================================
def _resolve_p4_cfg(cfg: Optional[P4TailScreenerConfig]) -> P4TailScreenerConfig:
    if cfg is not None:
        return cfg
    from core.config_manager import get_p4_tail_screener_config
    return get_p4_tail_screener_config()


DEFAULT_P4_CONFIG = P4TailScreenerConfig()


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        if isinstance(val, (float, np.floating)) and pd.isna(val):
            return default
        s = str(val).strip()
        if s in ("", "-", "nan", "None"):
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


# =============================================================================
# A 股量纲安全 VWAP 计算（适用于尾盘全局否决与统计计算）
# =============================================================================

def _safe_vwap_from_y(
    amt: float,
    vol: float,
    ref_price: float,
    fallback_price: float,
) -> float:
    """
    【V26.7 新增】从日线 amount/vol 字段安全计算 VWAP，量纲异常时降级到 fallback。

    A 股日线数据量纲问题:
    - TuShare 日线 amount 单位是"千元"（即 1000 元），不是"元"。
    - vol 在日线中是"成交量（手）"，1手=100股。
    - 因此正确的 VWAP（日线均价）计算公式为:
        amt(千元) * 1000 / (vol(手) * 100) = amt * 10 / vol（元/股）

    常见错误用法:
    - 直接用 amt/vol 计算（得到的是千元/手，单位完全错误）
    - 没有考虑量纲不一致的情况

    量纲异常检测:
    - 用 ref_price（即昨收价）作为基准，计算出的 VWAP 偏离 ref_price 超过 20% 时，
      说明量纲有误，降级使用 fallback_price（通常为昨收价）。

    参数:
        amt: 日线 amount 字段（千元）
        vol: 日线 vol 字段（手）
        ref_price: 参考价格（昨收价 close，用于量纲合理性判断）
        fallback_price: 降级后的 VWAP 替代价格（通常与 ref_price 相同）

    返回: 安全的 VWAP 值（元/股），量纲异常时返回 fallback_price。
    """
    if amt <= 0 or vol <= 0:
        return fallback_price

    # 正确的日线 VWAP 计算：amt(千元)*1000 / vol(手)*100 = amt * 10 / vol
    # 其中 amt 来自日线（千元），vol 来自日线（手=100股）
    tentative = amt * 10.0 / max(vol, 1e-9)

    # 量纲合理性校验：VWAP 偏离昨收价超过 20%，判定为量纲异常
    if ref_price > 0 and abs(tentative - ref_price) / ref_price > 0.20:
        # 异常降级：返回 fallback_price，不参与 VWAP 相关判断
        return fallback_price

    return tentative


def _safe_vwap_from_rt_dimension_check(
    vwap_raw: Any,
    now_px: float,
    fallback_price: float,
) -> float:
    """
    【V26.7 新增】对 rt 中已有的 VWAP 值做量纲安全校验，异常时降级到 fallback。

    当 vwap_full 已从 rt['vwap'] 或 rt['vwap_price'] 提取出来，但无法确定其量纲时，
    用此函数做最后的校验。若 VWAP 偏离参考价（昨收）超过 20%，说明量纲异常，
    直接降级使用 fallback_price。

    参数:
        vwap_raw: rt 中提取的原始 vwap 值（可能为空/None/NaN）
        now_px: 当前价格，用于判断 vwap 是否为空
        fallback_price: 降级后的替代价格（通常为昨收价）

    返回: 量纲安全的 VWAP 值；若 vwap_raw 为空或量纲异常，返回 fallback_price。
    """
    if vwap_raw is None or (isinstance(vwap_raw, float) and pd.isna(vwap_raw)):
        return fallback_price
    try:
        vwap_val = float(vwap_raw)
    except (TypeError, ValueError):
        return fallback_price
    if vwap_val <= 0:
        return fallback_price
    # 量纲校验：VWAP 偏离昨收价超过 20% 时降级
    if fallback_price > 0 and abs(vwap_val - fallback_price) / fallback_price > 0.20:
        return fallback_price
    return vwap_val


def _macd_hist_row(row: pd.Series) -> float:
    v = _safe_float(row.get("macd_hist"), 0.0)
    if v == 0.0 and "macd_bar" in row.index:
        v = _safe_float(row.get("macd_bar"), 0.0)
    return v


def _field_rt_or_y(rt: Dict[str, Any], y: pd.Series, key: str, default: float = 0.0) -> float:
    raw = rt.get(key)
    if raw is not None and not (isinstance(raw, float) and pd.isna(raw)):
        return _safe_float(raw, default)
    return _safe_float(y.get(key), default)


def _today_vol_hand(rt: Dict[str, Any], y: pd.Series) -> float:
    v = _safe_float(rt.get("volume"), 0.0)
    if v <= 0:
        return float("nan")
    return float(v / 100.0)


def _trf(rt: Dict[str, Any], y: pd.Series, close_live: float) -> float:
    return effective_turnover_rate_f(rt, y, close_live)


def _pct_chg_today(rt: Dict[str, Any], y: pd.Series, close_live: float) -> float:
    pre = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    if pre <= 0:
        return 0.0
    return (close_live - pre) / pre * 100.0


def _prev_pct_chg(df: pd.DataFrame, y: pd.Series) -> float:
    """获取昨日涨幅（df.iloc[-2] 的 pct_chg）。"""
    if len(df) < 2:
        return 0.0
    return _safe_float(df.iloc[-2].get("pct_chg"), 0.0)


def _vol_ma_cross_up_last_bar(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False
    if "vol_ma5" not in df.columns or "vol_ma20" not in df.columns:
        return False
    a = df.iloc[-2]
    b = df.iloc[-1]
    v5a, v20a = _safe_float(a.get("vol_ma5")), _safe_float(a.get("vol_ma20"))
    v5b, v20b = _safe_float(b.get("vol_ma5")), _safe_float(b.get("vol_ma20"))
    if min(v5a, v20a, v5b, v20b) <= 0:
        return False
    return (v5a <= v20a) and (v5b > v20b)


def _ma60_downtrend(df: pd.DataFrame, cfg: P4TailScreenerConfig) -> bool:
    if len(df) < 6 or "ma60" not in df.columns:
        return False
    m0 = _safe_float(df.iloc[-1].get("ma60"), 0.0)
    m5 = _safe_float(df.iloc[-6].get("ma60"), 0.0)
    if m5 <= 0:
        return False
    return m0 < m5 * cfg.ma60_downtrend_rel


def _is_limit_up(rt: Dict[str, Any]) -> bool:
    return str(rt.get("_is_limit", "")) == "UP"


def _is_limit_down(rt: Dict[str, Any]) -> bool:
    return str(rt.get("_is_limit", "")) == "DOWN"

def _detect_limit_up(
    ts_code: str,
    now_px: float,
    pre_close: float,
    rt: Dict[str, Any],
) -> str:
    """
    【V26.7 新增】尾盘动态涨跌停判定: 按 A 股板块规则精确计算涨停价.
    - 60/00 开头: 主板，涨停幅度 10%
    - 688 开头: 科创板，涨停幅度 20%
    - 300 开头: 创业板，涨停幅度 20%
    - 北交所(83/87/43/4 开头): 涨停幅度 30%
    - 若 rt['_is_limit'] 已存在则直接返回
    - 若 pre_close 与日线 close 差距 > 20%(疑似除权除息)，跳过涨跌停判定
    """
    if str(rt.get("_is_limit", "")) in ("UP", "DOWN"):
        return str(rt["_is_limit"])
    if not ts_code or now_px <= 0 or pre_close <= 0:
        return ""

    code_base = ts_code.split(".")[0] if "." in ts_code else str(ts_code).strip()
    if code_base.startswith("688"):
        limit_pct = 0.20
    elif code_base.startswith("300"):
        limit_pct = 0.20
    elif code_base.startswith("4") and len(code_base) == 4:
        limit_pct = 0.30
    elif code_base.startswith(("83", "87", "43")):
        limit_pct = 0.30
    elif code_base.startswith(("60", "00")):
        limit_pct = 0.10
    else:
        limit_pct = 0.10

    y_close = _safe_float(rt.get("_y_close"), 0.0)
    if y_close > 0 and pre_close > 0:
        ratio_diff = abs(pre_close - y_close) / y_close
        if ratio_diff > 0.20:
            return ""
    elif y_close <= 0:
        return ""

    limit_price = pre_close * (1.0 + limit_pct)
    if now_px >= limit_price:
        return "UP"
    return ""




# =============================================================================
# 全局否决
# =============================================================================
def p4_global_risk_veto(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: Optional[P4TailScreenerConfig] = None,
) -> Tuple[bool, str]:
    cfg = _resolve_p4_cfg(cfg)
    if df is None or df.empty or len(df) < 2:
        return False, "历史K线不足"

    y = df.iloc[-1]

    close_live = _safe_float(rt.get("price"), 0.0)
    if close_live <= 0:
        close_live = _safe_float(rt.get("close"), 0.0)
    if close_live <= 0:
        return False, "缺少尾盘有效收盘价(现价)"

    # 【V26.7 新增】动态涨跌停检测
    ts_code = str(rt.get("ts_code", y.get("ts_code", "")) or "")
    pre_close_veto = _safe_float(rt.get("pre_close"), _safe_float(y.get("pre_close"), _safe_float(y.get("close"), 0.0)))
    rt["_y_close"] = _safe_float(y.get("close"), 0.0)
    rt["_is_limit"] = _detect_limit_up(ts_code, close_live, pre_close_veto, rt)

    # 【V26.7 重构】全天VWAP重心校验: 使用量纲安全的 VWAP 计算，防止千元/手量纲错位导致判断失效
    # 优先级: rt.vwap > rt.vwap_price > 日线 amt/vol 安全计算 > fallback到昨收价
    y_close_ref = _safe_float(y.get("close"), 0.0)
    vwap_full = rt.get("vwap")
    if vwap_full is None or (isinstance(vwap_full, float) and pd.isna(vwap_full)):
        vwap_full = rt.get("vwap_price")
    if vwap_full is None or (isinstance(vwap_full, float) and pd.isna(vwap_full)):
        # 日线 amt(千元)/vol(手) 量纲需转换: amt*10/vol = 元/股
        amt_y = _safe_float(y.get("amount"), 0.0)
        vol_y = _safe_float(y.get("vol"), 0.0)
        vwap_full = _safe_vwap_from_y(amt_y, vol_y, y_close_ref, y_close_ref)
    else:
        # rt 中的 vwap 也需要量纲校验（实时行情的 amount 可能为元或千元）
        vwap_full = _safe_vwap_from_rt_dimension_check(vwap_full, close_live, y_close_ref)
    if vwap_full > 0:
        try:
            vwap_dev = (close_live - vwap_full) / vwap_full * 100.0
            if vwap_dev < -0.5:
                # 尾盘价格显著低于全天VWAP: 尾盘偷袭骗线，一票否决
                return False, f"尾盘偷袭骗线否决: 现价偏离VWAP {vwap_dev:.2f}% < -0.5%"
        except (TypeError, ValueError):
            pass

    ma20 = _safe_float(y.get("ma20"), 0.0)
    if ma20 > 0 and close_live < ma20:
        return False, "短期破位: 收盘 < ma20"

    ma60 = _safe_float(y.get("ma60"), 0.0)
    if ma60 > 0 and close_live < ma60:
        return False, "中期破位: 收盘 < ma60(非右侧趋势)"

    pct = _pct_chg_today(rt, y, close_live)
    vol_hand = _today_vol_hand(rt, y)
    vma5 = _safe_float(y.get("vol_ma5"), 0.0)
    if not np.isnan(vol_hand) and vol_hand > 0 and vma5 > 0 and vol_hand < vma5 * 0.38:
        if not (_is_limit_up(rt) or _is_limit_down(rt)):
            return False, "尾盘偷袭: 全日量不足5日均量38%"

    sig = _safe_float(y.get("macd_signal"), 0.0)
    m = _safe_float(y.get("macd"), 0.0)
    m_prev = _safe_float(df.iloc[-2].get("macd"), 0.0) if len(df) >= 2 else m
    if sig < 0 and m < m_prev:
        return False, "动能死叉预警: macd_signal<0 且 macd 向下发散"

    nm_rt = _safe_float(rt.get("net_main_amount"), float("nan"))
    if not pd.isna(nm_rt):
        if nm_rt < 0 and pct <= cfg.global_fund_reflow_pct_max:
            return False, f"资金暗流: 主力为负且尾盘涨幅未超 cfg.global_fund_reflow_pct_max({cfg.global_fund_reflow_pct_max}%)"

    high_d = _safe_float(rt.get("high"), close_live)
    turnover_f = _trf(rt, y, close_live)
    if high_d > 0 and close_live > 0:
        upper_ratio = (high_d - close_live) / close_live
        if upper_ratio > cfg.global_upper_shadow_ratio_max and turnover_f > cfg.global_upper_turnover_f_min:
            if not (_is_limit_up(rt) or _is_limit_down(rt)):
                return False, "诱多长上影: 上影/收盘与真实换手超全局阈值"

    cm = _field_rt_or_y(rt, y, "circ_mv")
    if cm < cfg.circ_mv_min_wan:
        return False, f"规模过小 circ_mv={cm:.0f}万 < {cfg.circ_mv_min_wan:.0f}万"

    return True, ""


# =============================================================================
# P4-01 光头阳线抢筹
# 昨日强势光头阳 + 今日延续 + 尾盘仍在高位
# =============================================================================
def _strategy_bald_bull(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P4TailScreenerConfig,
    close_live: float,
    pct: float,
) -> Tuple[bool, str]:
    is_limit = _is_limit_up(rt)

    # 【BugFix V26.5】昨日涨幅必须从 df.iloc[-2] 读取，而非 df.iloc[-1]（当日）
    y_prev = df.iloc[-2] if len(df) >= 2 else y
    y_prev_pct = _safe_float(y_prev.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit else cfg.s1_prev_pct_high
    if not (cfg.s1_prev_pct_low <= y_prev_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s1_prev_pct_low}%~{pct_high_prev}%"

    mh = _macd_hist_row(y)
    if mh <= 0:
        return False, "昨日 macd_hist 未为正"

    high_d = _safe_float(rt.get("high"), close_live)
    if high_d <= 0:
        return False, "最高价缺失"
    if close_live < high_d * cfg.s1_close_to_high_min:
        return False, "未收在近高点(低于 cfg.s1_close_to_high_min*high)"

    vr = _field_rt_or_y(rt, y, "vol_ratio")
    pct_high_s1 = 9.5 if is_limit else cfg.s1_pct_high
    if not (cfg.s1_pct_low <= pct <= pct_high_s1):
        return False, f"涨幅不在 {cfg.s1_pct_low}%~{pct_high_s1}%"
    if not is_limit and vr < cfg.s1_vol_ratio_min:
        return False, "量比低于 cfg.s1_vol_ratio_min"

    wr = _field_rt_or_y(rt, y, "winner_rate")
    if wr <= cfg.s1_winner_min:
        return False, f"winner_rate {wr:.1f} 低于 {cfg.s1_winner_min}"

    slope = _safe_float(y.get("ma20_slope_5"), 0.0)
    if slope <= cfg.s1_ma20_slope_min:
        return False, f"防飞刀: ma20_slope_5 未高于 {cfg.s1_ma20_slope_min}"

    return True, "OK"


# =============================================================================
# P4-02 筹码锁死创新高
# 突破20日高点 + 缩量整理
# =============================================================================
def _strategy_chip_lock_high(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    close_live: float,
    vol_hand: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    wr = _field_rt_or_y(rt, y, "winner_rate")
    if wr <= cfg.s2_winner_min:
        return False, f"winner_rate {wr:.1f} 低于 {cfg.s2_winner_min}"

    c95 = _field_rt_or_y(rt, y, "cost_95th")
    h20 = _safe_float(y.get("high_20"), 0.0)
    if c95 <= 0 or close_live <= c95:
        return False, "未突破 cost_95th"
    if h20 <= 0 or close_live <= h20:
        return False, "未突破 high_20"

    vma5 = _safe_float(y.get("vol_ma5"), 0.0)
    if np.isnan(vol_hand) or vma5 <= 0:
        return False, "成交量或 vol_ma5 缺失"
    if vol_hand > vma5:
        return False, "未满足缩量(vol<=vol_ma5)"

    turnover_f = _trf(rt, y, close_live)
    cmw = circ_mv_wan_from(y, rt)
    tf_low, tf_high = adaptive_turnover_f_range(cmw, cfg.s2_turnover_f_low, cfg.s2_turnover_f_high)
    if not (tf_low <= turnover_f <= tf_high):
        return False, f"真实换手{turnover_f:.2f}%不在市值自适应区间[{tf_low:.2f}%,{tf_high:.2f}%]"

    return True, "OK"


# =============================================================================
# P4-03 机构尾盘潜伏
# 主力净买 + 机构净买 + 站稳均线
# =============================================================================
def _strategy_inst_tail_lurk(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P4TailScreenerConfig,
    close_live: float,
    pct: float,
) -> Tuple[bool, str]:
    cmw = circ_mv_wan_from(y, rt)
    nm = _field_rt_or_y(rt, y, "net_main_amount")
    thr_nm = dynamic_net_main_threshold_yuan(cmw, cfg.s3_net_main_ratio_of_float_mv)
    if nm <= thr_nm:
        return False, "主力净额未达市值动态门槛(比例+阶梯)"

    inst = _safe_float(y.get("inst_net_buy"), 0.0)
    thr_inst = dynamic_inst_single_threshold_yuan(cmw, cfg.s3_inst_net_buy_ratio_of_float_mv)
    if inst <= thr_inst:
        return False, "inst_net_buy 未达市值动态门槛(日线已收盘)"

    hk_positive_count = 0
    if "hk_vol" in df.columns:
        for idx in (-2, -1):
            if len(df) >= abs(idx):
                hk_val = _safe_float(df.iloc[idx].get("hk_vol"), 0.0)
                if hk_val > 0:
                    hk_positive_count += 1
    rt["_hk_vol_positive_days"] = hk_positive_count

    is_limit = _is_limit_up(rt)
    pct_high_s3 = 9.5 if is_limit else cfg.s3_pct_high
    if not (cfg.s3_pct_low <= pct <= pct_high_s3):
        return False, f"涨幅不在 {cfg.s3_pct_low}%~{pct_high_s3}%"

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if not (close_live > ma5 and close_live > ma20 and close_live > ma60):
        return False, "未站稳 ma5/ma20/ma60 之上"

    c50 = _field_rt_or_y(rt, y, "cost_50th")
    if c50 > 0 and c50 > close_live:
        return False, "防飞刀: cost_50th 高于收盘(套牢区)"

    return True, "OK"


# =============================================================================
# P4-04 均线缩量低吸
# 多头排列 + 回踩均线 + 极致缩量
# =============================================================================
def _strategy_ma_shrink_dip(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    close_live: float,
    low_d: float,
    pct: float,
    vol_hand: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    is_limit = _is_limit_up(rt)

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if not (ma5 > ma20 > ma60):
        return False, "非多头 ma5>ma20>ma60"

    # 【BugFix V26.5】昨日涨幅从 df.iloc[-2] 读取（当日为 df.iloc[-1]）
    y_prev = df.iloc[-2] if len(df) >= 2 else y
    y_prev_pct = _safe_float(y_prev.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit else cfg.s4_prev_pct_high
    if not (cfg.s4_prev_pct_low <= y_prev_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s4_prev_pct_low}%~{pct_high_prev}%"

    if not (cfg.s4_pct_low <= pct <= cfg.s4_pct_high):
        return False, f"涨幅不在策略四 cfg 区间[{cfg.s4_pct_low}%, {cfg.s4_pct_high}%]"

    touch_m20 = ma20 > 0 and low_d <= ma20 * cfg.s4_touch_ma_eps
    touch_m5 = ma5 > 0 and low_d <= ma5 * cfg.s4_touch_ma_eps
    if not (touch_m20 or touch_m5):
        return False, "盘中未触及 ma20 或 ma5 回踩带"
    if not (close_live > ma20):
        return False, "尾盘未站稳 ma20 之上"

    vma5 = _safe_float(y.get("vol_ma5"), 0.0)
    if np.isnan(vol_hand) or vma5 <= 0:
        return False, "量数据缺失"
    if vol_hand >= vma5 * cfg.s4_vol_shrink_ratio:
        return False, f"未极致缩量(vol >= vol_ma5*{cfg.s4_vol_shrink_ratio})"

    if _macd_hist_row(y) <= 0:
        return False, "macd_hist 未为正"

    wr = _field_rt_or_y(rt, y, "winner_rate")
    if wr < cfg.s4_winner_fly_lt:
        return False, f"防飞刀: winner_rate {wr:.1f} 低于 {cfg.s4_winner_fly_lt}"

    return True, "OK"


# =============================================================================
# P4-05 强势洗盘承接
# 主力净流入 + 回踩MA5 强势洗盘后再次承接
# =============================================================================
def _strategy_wash_rebound(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    close_live: float,
    low_d: float,
    pct: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    if not (ma5 > ma20):
        return False, "非多头 ma5>ma20"
    if ma5 <= 0:
        return False, "ma5 缺失"
    if not (low_d <= ma5 * cfg.s7_ma5_touch_eps):
        return False, "盘中未触及 ma5 回踩带"
    if not (close_live > ma5):
        return False, "尾盘未收在 ma5 之上"

    cmw = circ_mv_wan_from(y, rt)
    nm = _field_rt_or_y(rt, y, "net_main_amount")
    thr_nm = dynamic_net_main_threshold_yuan(cmw, cfg.s7_net_main_ratio_of_float_mv)
    if nm <= thr_nm:
        return False, "主力净额未达市值动态门槛"

    if not (cfg.s7_pct_low <= pct <= cfg.s7_pct_high):
        return False, f"涨幅不在 {cfg.s7_pct_low}%~{cfg.s7_pct_high}%"

    vr = _field_rt_or_y(rt, y, "vol_ratio")
    if not _is_limit_up(rt) and vr < cfg.s7_vol_ratio_min:
        return False, f"量比 {vr:.2f} 低于 {cfg.s7_vol_ratio_min}"

    turnover_f = _trf(rt, y, close_live)
    cmw2 = circ_mv_wan_from(y, rt)
    tf_high = adaptive_turnover_f_range(cmw2, cfg.s7_turnover_f_fly_gt, cfg.s7_turnover_f_fly_gt)[1]
    if turnover_f > tf_high:
        return False, f"防飞刀: 真实换手{turnover_f:.2f}%超过自适应上限{tf_high:.2f}%"

    sig = _safe_float(y.get("macd_signal"), 0.0)
    sig_p = _safe_float(df.iloc[-2].get("macd_signal"), 0.0) if len(df) >= 2 else sig
    m = _safe_float(y.get("macd"), 0.0)
    if sig < sig_p and m < sig:
        return False, "防飞刀: macd_signal 拐头且弱于快线(死叉前兆)"

    return True, "OK"


# =============================================================================
# P4-06 动能突破共振
# 突破20日高 + 主力确认 + 多头排列
# =============================================================================
def _strategy_momentum_break(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    close_live: float,
    pct: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    is_limit = _is_limit_up(rt)

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if not (ma5 > ma20 > ma60):
        return False, "非绝对多头 ma5>ma20>ma60"

    # 【BugFix V26.5】昨日涨幅从 df.iloc[-2] 读取
    y_prev = df.iloc[-2] if len(df) >= 2 else y
    y_prev_pct = _safe_float(y_prev.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit else cfg.s6_momentum_prev_pct_high
    if not (cfg.s6_momentum_prev_pct_low <= y_prev_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s6_momentum_prev_pct_low}%~{pct_high_prev}%"

    mh = _macd_hist_row(y)
    if mh <= 0:
        return False, "昨日 macd_hist 未为正"

    pct_high_s6 = 9.5 if is_limit else cfg.s6_momentum_pct_high
    if not (cfg.s6_momentum_pct_low <= pct <= pct_high_s6):
        return False, f"涨幅不在 {cfg.s6_momentum_pct_low}%~{pct_high_s6}%"

    h20 = _safe_float(y.get("high_20"), 0.0)
    if h20 <= 0 or close_live <= h20:
        return False, "收盘未站上 high_20"

    cmw = circ_mv_wan_from(y, rt)
    nm = _field_rt_or_y(rt, y, "net_main_amount")
    thr_nm = dynamic_net_main_threshold_yuan(cmw, cfg.s6_momentum_net_main_ratio_of_float_mv)
    if nm <= thr_nm:
        return False, "主力净额未达动态门槛(动能突破)"

    vol_hand = _today_vol_hand(rt, y)
    vma5 = _safe_float(y.get("vol_ma5"), 0.0)
    if not np.isnan(vol_hand) and vma5 > 0 and vol_hand < vma5 * cfg.s6_momentum_min_vol_vs_vma5:
        return False, f"全日量能不足: 累计量未达5日均量{int(cfg.s6_momentum_min_vol_vs_vma5*100)}%"

    c95 = _field_rt_or_y(rt, y, "cost_95th")
    wr = _field_rt_or_y(rt, y, "winner_rate")
    if c95 > 0 and close_live < c95 * 0.995 and wr < 78.0:
        return False, "筹码重压: 未有效突破成本上沿且获利盘不足"

    atrp = _safe_float(y.get("atr_pct"), 0.0)
    atrp_max = cfg.s6_momentum_atr_pct_fly_gt * 1.25 if is_limit else cfg.s6_momentum_atr_pct_fly_gt
    if atrp > atrp_max:
        return False, f"防飞刀: atr_pct={atrp:.2f}% > {atrp_max:.2f}%"

    ma20v = _safe_float(y.get("ma20"), 0.0)
    if ma20v > 0:
        bias = (close_live - ma20v) / ma20v * 100.0
        bias_max = cfg.s6_momentum_bias_fly_gt * 1.4 if is_limit else cfg.s6_momentum_bias_fly_gt
        if bias > bias_max:
            return False, f"防飞刀: bias={bias:.1f}% > {bias_max:.1f}%"

    return True, "OK"


# =============================================================================
# P4-07 / P4-11 底仓不破均线（共用统计函数）
# =============================================================================
def _p1_bottom_hold_stats(df: pd.DataFrame, rt: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "vwap_hold_ratio": 0.0,
        "ma5_hold_ratio": 0.0,
        "ma20_hold_ratio": 0.0,
        "reclaim_count": 0,
        "pullback_depth_pct": 0.0,
        "intraday_dd_pct": 0.0,
        "upper_shadow_pct": 0.0,
        "close_vwap_dev_pct": 0.0,
        "hold_bars": 0,
    }
    if df is None or df.empty:
        return out
    last = df.iloc[-1]
    price = _safe_float(rt.get("price"), _safe_float(last.get("close"), 0.0))
    if price <= 0:
        return out
    vwap = _safe_float(rt.get("vwap"), _safe_float(last.get("vwap"), 0.0))
    ma5 = _safe_float(last.get("ma5"), 0.0)
    ma20 = _safe_float(last.get("ma20"), 0.0)
    high = _safe_float(rt.get("high"), price)
    low = _safe_float(rt.get("low"), price)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(last.get("pre_close"), 0.0))
    if pre_close > 0:
        out["pullback_depth_pct"] = max(0.0, (high - low) / pre_close * 100.0)
        out["intraday_dd_pct"] = max(0.0, (high - price) / pre_close * 100.0)
        out["upper_shadow_pct"] = max(
            0.0, (high - max(price, _safe_float(rt.get("open"), price))) / pre_close * 100.0
        )
        out["close_vwap_dev_pct"] = ((price - vwap) / vwap * 100.0) if vwap > 0 else 0.0
    if vwap > 0 and "close" in df.columns:
        close_s = pd.to_numeric(df["close"], errors="coerce")
        out["vwap_hold_ratio"] = float((close_s > vwap).mean())
        out["hold_bars"] = int((close_s > vwap).sum())
    if ma5 > 0 and "close" in df.columns:
        out["ma5_hold_ratio"] = float((pd.to_numeric(df["close"], errors="coerce") > ma5).mean())
    if ma20 > 0 and "close" in df.columns:
        out["ma20_hold_ratio"] = float((pd.to_numeric(df["close"], errors="coerce") > ma20).mean())
    if vwap > 0 and "close" in df.columns:
        prev = pd.to_numeric(df["close"], errors="coerce").shift(1)
        now = pd.to_numeric(df["close"], errors="coerce")
        out["reclaim_count"] = int((((prev < vwap) & (now >= vwap))).sum()) if len(df) > 1 else 0
    return out


def _strategy_p1_bottom_hold_ma(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    close_live: float,
    pct: float,
    cfg: P4TailScreenerConfig,
    strategy_tag: str = "P4-07",
) -> Tuple[bool, str]:
    """
    P4-07 / P4-11 底仓不破均线。
    P4-07：强调底仓稳定运行，宽松的主线评分
    P4-11：强调均线不破 + 主线确认，严格的主线评分要求
    """
    p1_score = _safe_float(rt.get("p1_score"), _safe_float(y.get("p1_score"), 0.0))
    if p1_score < cfg.s11_p1_score_min:
        return False, f"P1分 {p1_score:.1f} 低于底仓门槛 {cfg.s11_p1_score_min}"

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if min(ma5, ma20, ma60) <= 0:
        return False, "均线缺失"

    stats = _p1_bottom_hold_stats(df, rt)
    if stats["vwap_hold_ratio"] < cfg.s11_vwap_hold_ratio_min:
        return False, f"VWAP 上方运行不足 {stats['vwap_hold_ratio']:.2f} < {cfg.s11_vwap_hold_ratio_min}"
    if stats["ma5_hold_ratio"] < cfg.s11_ma5_hold_ratio_min:
        return False, f"ma5 上方运行不足 {stats['ma5_hold_ratio']:.2f} < {cfg.s11_ma5_hold_ratio_min}"
    if stats["ma20_hold_ratio"] < cfg.s11_ma20_hold_ratio_min:
        return False, f"ma20 上方运行不足 {stats['ma20_hold_ratio']:.2f} < {cfg.s11_ma20_hold_ratio_min}"

    # 回收次数限制（回踩VWAP后重新站上）
    reclaim_max = cfg.s11_reclaim_allow_max if strategy_tag == "P4-11" else cfg.s11_reclaim_max
    if stats["reclaim_count"] > reclaim_max:
        return False, f"回收次数过多 {stats['reclaim_count']} > {reclaim_max}"

    if stats["pullback_depth_pct"] > cfg.s11_pullback_depth_max_pct:
        return False, f"回踩过深 {stats['pullback_depth_pct']:.2f}% > {cfg.s11_pullback_depth_max_pct}%"
    if stats["intraday_dd_pct"] > cfg.s11_intraday_dd_max_pct:
        return False, f"盘中回撤过大 {stats['intraday_dd_pct']:.2f}% > {cfg.s11_intraday_dd_max_pct}%"
    if stats["upper_shadow_pct"] > cfg.s11_upper_shadow_max_pct:
        return False, f"上影过长 {stats['upper_shadow_pct']:.2f}% > {cfg.s11_upper_shadow_max_pct}%"
    if (
        stats["close_vwap_dev_pct"] < cfg.s11_tail_vwap_dev_min
        or stats["close_vwap_dev_pct"] > cfg.s11_tail_vwap_dev_max
    ):
        return False, f"收盘偏离VWAP异常 {stats['close_vwap_dev_pct']:.2f}%"

    vr = _field_rt_or_y(rt, y, "vol_ratio")
    if vr < cfg.s11_vol_ratio_low or vr > cfg.s11_vol_ratio_high:
        return False, f"量比不在健康区间 {vr:.2f} not in [{cfg.s11_vol_ratio_low}, {cfg.s11_vol_ratio_high}]"

    # 主线评分（仅 P4-11 要求严格主线确认）
    if strategy_tag == "P4-11":
        sector_strength = _safe_float(
            rt.get("sector_strength"), _safe_float(rt.get("industry_beta"), 1.0)
        )
        mainline_score = _safe_float(rt.get("mainline_score"), 0.0)
        if sector_strength < cfg.s11_sector_strength_min:
            return False, f"板块强度不足 {sector_strength:.2f} < {cfg.s11_sector_strength_min}"
        if mainline_score < cfg.s11_mainline_score_required:
            return False, f"主线评分不足 {mainline_score:.2f} < {cfg.s11_mainline_score_required}"

    if not (close_live > ma5 and close_live > ma20 and close_live > ma60):
        return False, "尾盘未稳在关键均线上方"
    if pct < 0.0 or pct > 6.5:
        return False, f"涨幅不在底仓延续区间 {pct:.2f}% not in [0%, 6.5%]"

    mh = _macd_hist_row(y)
    if mh <= 0:
        return False, "macd_hist 未为正"
    slope = _safe_float(y.get("ma20_slope_5"), 0.0)
    if slope <= 0:
        return False, "ma20 斜率未转正"

    return True, "OK"


# =============================================================================
# P4-08 温和均线修复（量价齐升 + MA20收复 + MACD回暖）
# =============================================================================
def _strategy_gentle_ma_repair(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    y_prev: pd.Series,
    close_live: float,
    pct: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    is_limit = _is_limit_up(rt)

    # 昨日需在 MA20 下方（有「收复」的空间）
    cls_prev = _safe_float(y_prev.get("close"), 0.0)
    ma20_prev = _safe_float(y_prev.get("ma20"), 0.0)
    if cls_prev > 0 and ma20_prev > 0 and cls_prev >= ma20_prev:
        return False, "昨日已站上 ma20，非「收复」形态"

    ma20 = _safe_float(y.get("ma20"), 0.0)
    if ma20 <= 0:
        return False, "ma20 缺失"
    if not (close_live > ma20):
        return False, "收盘未站上 ma20"

    # 昨日涨幅约束（涨跌停放宽）
    y_prev_pct = _safe_float(y_prev.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit else cfg.s8_prev_pct_high
    if not (cfg.s8_prev_pct_low <= y_prev_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s8_prev_pct_low}%~{pct_high_prev}%"

    # 今日涨幅区间
    if not (cfg.s8_pct_low <= pct <= cfg.s8_pct_high):
        return False, f"涨幅不在 {cfg.s8_pct_low}%~{cfg.s8_pct_high}%"

    if not _vol_ma_cross_up_last_bar(df):
        return False, "vol_ma5 未在上一交易日完成上穿 vol_ma20"

    mh = _macd_hist_row(y)
    mh_p = _macd_hist_row(y_prev)
    if mh <= 0 or mh <= mh_p * cfg.s8_mhist_expand_min_ratio:
        return False, f"动能回暖不足: macd_hist 未为正或未达 {cfg.s8_mhist_expand_min_ratio}x 于前一日"

    wr = _field_rt_or_y(rt, y, "winner_rate")
    if wr <= cfg.s8_winner_min:
        return False, f"winner_rate {wr:.1f} 低于 {cfg.s8_winner_min}"

    c50 = _field_rt_or_y(rt, y, "cost_50th")
    if c50 > 0 and close_live < c50:
        return False, "防飞刀: 收盘 < cost_50th"

    turnover_f = _trf(rt, y, close_live)
    cmw = circ_mv_wan_from(y, rt)
    tf_low = adaptive_turnover_f_range(cmw, cfg.s8_turnover_f_min, cfg.s8_turnover_f_min)[0]
    if turnover_f < tf_low:
        return False, f"防飞刀: 真实换手{turnover_f:.2f}%低于市值自适应下限{tf_low:.2f}%"

    return True, "OK"


# =============================================================================
# P4-09 均线多头回踩企稳
# 四线多头(5/10/20/60) + 近窗回踩MA20 + 量能回暖
# =============================================================================
def _recent_window_touched_ma20(df: pd.DataFrame, cfg: P4TailScreenerConfig) -> bool:
    n = int(max(3, min(cfg.s9_touch_lookback_bars, 20)))
    # 【性能优化 V2】向量化替代 iterrows：使用 any() 向量化布尔搜索
    if len(df) < n + 1:
        return False
    seg = df.iloc[-(n + 1) : -1]
    eps = float(cfg.s9_touch_ma20_max_mult)
    if "ma20" not in seg.columns or "low" not in seg.columns:
        return False
    ma20_seg = pd.to_numeric(seg["ma20"], errors="coerce").fillna(0)
    low_seg = pd.to_numeric(seg["low"], errors="coerce").fillna(0)
    valid_mask = (ma20_seg > 0) & (low_seg > 0)
    if not valid_mask.any():
        return False
    return bool(((low_seg <= ma20_seg * eps) & valid_mask).any())


def _strategy_ma_bull_pullback_ma20(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    y_prev: pd.Series,
    close_live: float,
    pct: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    is_limit = _is_limit_up(rt)

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma10 = _safe_float(y.get("ma10"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if min(ma5, ma10, ma20, ma60) <= 0:
        return False, "均线缺失"
    if not (ma5 > ma10 > ma20 > ma60):
        return False, "非四线多头排列 ma5>ma10>ma20>ma60"

    slope = _safe_float(y.get("ma20_slope_5"), 0.0)
    if slope < cfg.s9_ma20_slope_min:
        return False, f"ma20_slope_5 未达多头斜率下限 {cfg.s9_ma20_slope_min}"

    if not (close_live > ma5):
        return False, "尾盘未站稳 ma5 上方"

    # 昨日涨幅约束（涨跌停放宽）
    y_prev_pct = _safe_float(y_prev.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit else cfg.s9_prev_pct_high
    if not (cfg.s9_prev_pct_low <= y_prev_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s9_prev_pct_low}%~{pct_high_prev}%"

    # 今日涨幅区间（涨跌停放宽）
    pct_high_s9 = 9.5 if is_limit else cfg.s9_pct_high
    if not (cfg.s9_pct_low <= pct <= pct_high_s9):
        return False, f"涨幅不在 {cfg.s9_pct_low}%~{pct_high_s9}%"

    if not _recent_window_touched_ma20(df, cfg):
        return False, "近窗未见回踩 ma20 形态"

    vr = _field_rt_or_y(rt, y, "vol_ratio")
    vr_min_s9 = cfg.s9_vr_min * 0.7 if is_limit else cfg.s9_vr_min
    if vr < vr_min_s9:
        return False, f"量比 {vr:.2f} < {vr_min_s9:.2f}"

    wr = _field_rt_or_y(rt, y, "winner_rate")
    wr_min_s9 = cfg.s9_winner_min * 0.95 if is_limit else cfg.s9_winner_min
    if wr < wr_min_s9:
        return False, f"winner_rate {wr:.1f} < {wr_min_s9:.1f}"

    mh = _macd_hist_row(y)
    if mh <= 0:
        return False, "macd_hist 未为正"

    if not (
        _vol_ma_cross_up_last_bar(df)
        or vr >= cfg.s9_vr_min + float(cfg.s9_vol_confirm_vr_extra)
    ):
        return False, "量能未确认(未 vol_ma5 上穿 vol_ma20 且量比未额外放大)"

    return True, "OK"


# =============================================================================
# P4-10 沿5日线主升缩量再攻
# 四线多头 + 近窗缩量踩MA5 + 今日再放量上攻
# =============================================================================
def _recent_ma5_shrink_pullback(df: pd.DataFrame, cfg: P4TailScreenerConfig) -> bool:
    n = int(max(2, min(cfg.s10_pullback_lookback, 15)))
    if len(df) < n + 1:
        return False
    seg = df.iloc[-(n + 1) : -1]
    eps = float(cfg.s10_ma5_touch_eps)
    # 【性能优化 V2】向量化替代 iterrows：使用 any() 向量化布尔搜索
    shr = float(cfg.s10_shrink_vs_vma5_max)
    if "ma5" not in seg.columns or "low" not in seg.columns or "vol" not in seg.columns or "vol_ma5" not in seg.columns:
        return False
    ma5_seg = pd.to_numeric(seg["ma5"], errors="coerce").fillna(0)
    low_seg = pd.to_numeric(seg["low"], errors="coerce").fillna(0)
    vol_seg = pd.to_numeric(seg["vol"], errors="coerce").fillna(0)
    vma5_seg = pd.to_numeric(seg["vol_ma5"], errors="coerce").fillna(0)
    valid_mask = (ma5_seg > 0) & (vma5_seg > 0) & (low_seg > 0)
    if not valid_mask.any():
        return False
    return bool(((low_seg <= ma5_seg * eps) & (vol_seg < vma5_seg * shr) & valid_mask).any())


def _strategy_ma5_trend_reattack(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    y_prev: pd.Series,
    close_live: float,
    pct: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    is_limit = _is_limit_up(rt)

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma10 = _safe_float(y.get("ma10"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if min(ma5, ma10, ma20, ma60) <= 0:
        return False, "均线缺失"
    if not (ma5 > ma10 > ma20 > ma60):
        return False, "非四线多头排列 ma5>ma10>ma20>ma60"

    slope = _safe_float(y.get("ma20_slope_5"), 0.0)
    if slope < cfg.s10_ma20_slope_min:
        return False, f"ma20_slope_5 未达策略十下限 {cfg.s10_ma20_slope_min}"

    if not (close_live > ma5):
        return False, "尾盘未沿 ma5 上方"

    # 昨日涨幅约束（涨跌停放宽）
    y_prev_pct = _safe_float(y_prev.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit else cfg.s10_prev_pct_high
    if not (cfg.s10_prev_pct_low <= y_prev_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s10_prev_pct_low}%~{pct_high_prev}%"

    # 今日涨幅区间（涨跌停放宽）
    pct_high_s10 = 9.5 if is_limit else cfg.s10_pct_high
    if not (cfg.s10_pct_low <= pct <= pct_high_s10):
        return False, f"涨幅不在 {cfg.s10_pct_low}%~{pct_high_s10}%"

    if not _recent_ma5_shrink_pullback(df, cfg):
        return False, "近窗未见缩量踩 ma5"

    vr = _field_rt_or_y(rt, y, "vol_ratio")
    vr_min_s10 = cfg.s10_vr_min * 0.7 if is_limit else cfg.s10_vr_min
    if vr < vr_min_s10:
        return False, f"量比 {vr:.2f} < {vr_min_s10:.2f}"

    wr = _field_rt_or_y(rt, y, "winner_rate")
    wr_min_s10 = cfg.s10_winner_min * 0.95 if is_limit else cfg.s10_winner_min
    if wr < wr_min_s10:
        return False, f"winner_rate {wr:.1f} < {wr_min_s10:.1f}"

    mh = _macd_hist_row(y)
    if mh <= 0:
        return False, "macd_hist 未为正"

    return True, "OK"


# =============================================================================
# 策略五（质量趋势底仓）— 兼容保留
# =============================================================================
def _strategy_value_div_bottom(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    close_live: float,
    pct: float,
    cfg: P4TailScreenerConfig,
) -> Tuple[bool, str]:
    """
    策略五：质量趋势底仓（兼容配置 s5_*，实际复用 s5_pe_max 等参数）。
    注意：s5_* 配置保留在 dataclass 中（向后兼容），实现放在此处供参考。
    """
    pe = _safe_float(y.get("pe_ttm"), 999.0)
    if pe >= cfg.s5_pe_max:
        return False, f"PE {pe:.1f} 未达策略五区间（< {cfg.s5_pe_max}）"

    ma60 = _safe_float(y.get("ma60"), 0.0)
    c50 = _field_rt_or_y(rt, y, "cost_50th")
    if ma60 <= 0 or close_live <= ma60:
        return False, "未在 ma60 之上"
    if c50 <= 0 or close_live <= c50:
        return False, "未在 cost_50th 之上"

    turnover_f = _trf(rt, y, close_live)
    cmw = circ_mv_wan_from(y, rt)
    tf_high = adaptive_turnover_f_range(cmw, cfg.s5_turnover_f_max_lt, cfg.s5_turnover_f_max_lt)[1]
    if turnover_f >= tf_high:
        return False, f"真实换手{turnover_f:.2f}%未低于自适应上限{tf_high:.2f}%"

    is_limit = _is_limit_up(rt)
    pct_high_s5 = 9.5 if is_limit else cfg.s5_pct_high
    if not (cfg.s5_pct_low <= pct <= pct_high_s5):
        return False, f"涨幅不在 {cfg.s5_pct_low}%~{pct_high_s5}%"

    if _ma60_downtrend(df, cfg):
        return False, "防飞刀: ma60 明显下降通道"

    return True, "OK"


# =============================================================================
# 主评估函数
# =============================================================================
def evaluate_p4_tail_screener(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: Optional[P4TailScreenerConfig] = None,
) -> Dict[str, Any]:
    cfg = _resolve_p4_cfg(cfg)
    out: Dict[str, Any] = {
        "veto_pass": False,
        "veto_reason": "",
        "strategies": [],
        "strategy_checks": {},
        "p4_core_screener_pass": False,
        "detail": {},
    }

    ok, reason = p4_global_risk_veto(df, rt, cfg)
    out["veto_pass"] = ok
    out["veto_reason"] = reason
    if not ok:
        return out

    y = df.iloc[-1]
    y_prev = df.iloc[-2] if len(df) >= 2 else y

    close_live = _safe_float(rt.get("price"), 0.0)
    if close_live <= 0:
        close_live = _safe_float(rt.get("close"), 0.0)
    low_d = _safe_float(rt.get("low"), close_live)
    pct = _pct_chg_today(rt, y, close_live)
    vol_hand = _today_vol_hand(rt, y)

    # 获取主线评分（用于 P4-11 严格约束）
    mainline_score = _safe_float(rt.get("mainline_score"), 0.0)
    sector_strength = _safe_float(rt.get("sector_strength"), _safe_float(rt.get("industry_beta"), 1.0))

    # 全部 11 个策略的评估（按编号顺序）
    strats = [
        # P4-01 ~ P4-07（核心主战法）
        (
            "P4-01·★光头阳线抢筹",
            lambda: _strategy_bald_bull(df, rt, y, cfg, close_live, pct),
        ),
        (
            "P4-02·★筹码锁死创新高",
            lambda: _strategy_chip_lock_high(df, rt, y, close_live, vol_hand, cfg),
        ),
        (
            "P4-03·★机构尾盘潜伏",
            lambda: _strategy_inst_tail_lurk(df, rt, y, cfg, close_live, pct),
        ),
        (
            "P4-04·★均线缩量低吸",
            lambda: _strategy_ma_shrink_dip(df, rt, y, close_live, low_d, pct, vol_hand, cfg),
        ),
        (
            "P4-05·★强势洗盘承接",
            lambda: _strategy_wash_rebound(df, rt, y, close_live, low_d, pct, cfg),
        ),
        (
            "P4-06·★动能突破共振",
            lambda: _strategy_momentum_break(df, rt, y, close_live, pct, cfg),
        ),
        (
            "P4-07·★底仓不破均线",
            lambda: _strategy_p1_bottom_hold_ma(
                df, rt, y, close_live, pct, cfg, strategy_tag="P4-07"
            ),
        ),
        # P4-08 ~ P4-10（辅助标签战法）
        (
            "P4-08·★温和均线修复",
            lambda: _strategy_gentle_ma_repair(df, rt, y, y_prev, close_live, pct, cfg),
        ),
        (
            "P4-09·★均线多头回踩企稳",
            lambda: _strategy_ma_bull_pullback_ma20(
                df, rt, y, y_prev, close_live, pct, cfg
            ),
        ),
        (
            "P4-10·★沿5日线主升缩量",
            lambda: _strategy_ma5_trend_reattack(
                df, rt, y, y_prev, close_live, pct, cfg
            ),
        ),
        # P4-11（P1底仓严格版：要求主线确认）
        (
            "P4-11·★底仓主线共振",
            lambda: _strategy_p1_bottom_hold_ma(
                df, rt, y, close_live, pct, cfg, strategy_tag="P4-11"
            ),
        ),
    ]

    hits: List[str] = []
    checks: Dict[str, str] = {}
    for name, fn in strats:
        try:
            passed, msg = fn()
        except Exception as ex:
            logger.debug("P4 策略 %s 异常: %s", name, ex)
            passed, msg = False, f"异常:{ex}"
        checks[name] = msg
        if passed:
            hits.append(name)

    # 北向数据滞后提示
    hk_vol_warn = ""
    if rt.get("_hk_vol_positive_days", 0) >= 2:
        hk_vol_warn = "近2日北向正(昨)"
    elif rt.get("_hk_vol_positive_days", 0) >= 1:
        hk_vol_warn = "部分北向数据(昨)"
    else:
        hk_vol_warn = "北向数据(昨-滞后)"

    out["strategies"] = hits
    out["strategy_checks"] = checks
    out["p4_core_screener_pass"] = len(hits) > 0
    cmw = circ_mv_wan_from(y, rt)
    out["detail"] = {
        "close_live": round(close_live, 4),
        "pct_chg_day": round(pct, 3),
        "vol_hand_day": round(vol_hand, 2) if not np.isnan(vol_hand) else None,
        "circ_mv_wan": round(cmw, 2),
        "turnover_f_eff_pct": round(_trf(rt, y, close_live), 3),
        "hk_vol_data_note": hk_vol_warn,
        "mainline_score": round(mainline_score, 2),
        "sector_strength": round(sector_strength, 3),
    }
    return out


def screen_p4_tail_universe(
    rows: List[Tuple[str, pd.DataFrame, Dict[str, Any]]],
    cfg: Optional[P4TailScreenerConfig] = None,
) -> pd.DataFrame:
    cfg = _resolve_p4_cfg(cfg)
    rec: List[Dict[str, Any]] = []
    for ts_code, df, rt in rows:
        r = evaluate_p4_tail_screener(df, rt, cfg)
        rec.append(
            {
                "ts_code": ts_code,
                "veto_pass": r["veto_pass"],
                "veto_reason": r["veto_reason"],
                "strategies": "|".join(r["strategies"]),
                "p4_core_screener_pass": r["p4_core_screener_pass"],
            }
        )
    return pd.DataFrame(rec)
