# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.7 - P3 盘中选股池「物理胸甲」硬阈值筛选模块
================================================================================
对齐 data_fetcher.ALL_55_COLS; 全局一票否决 + 八大客观策略(满足任一即命中).

【V26.7 核心优化】
1. 流通市值下限强制提升至 100 亿(1,000,000 万元)，彻底封死袖珍盘漏洞。
2. 新增盘中动态涨跌停判定_detect_limit_up:按代码前缀精确计算(主板10%/科创创业20%/北交所30%)。
3. 新增盘中 VWAP 脉冲诱多硬否决:当前价向上偏离 VWAP 超过 4% 则一票否决，防止 T+1 诱多陷阱。
4. 增强预估全天成交量逻辑:基于 curr_min 分钟数放大 + 早盘保守系数，防止量比失真。

【增量 / 流式约定】
1. 单票单次调用复杂度 O(1): 仅访问 df 尾部固定窗口(如 tail(5))，不扫描全表，不聚合全市场.
2. 每次盘中快照只需传入「当前 rt」+「已缓存的昨日及以前日线 df」; 禁止在模块内做 DuckDB 全表扫描.
3. 预估全日成交量: 用 rt['volume'](股)换算为「手」后按已交易分钟数线性外推; 若 rt 提供
   elapsed_mins / projected_vol_hand 则优先使用(流式任务可写入，避免重复计算时钟).
4. 资金, 筹码字段: 优先读 rt(实时/快照)，缺失时回退到 df 最后一根日线(昨收)上的同名列.

【V26.7 A股特殊场景处理】
- 动态涨跌停: _detect_limit_up 按代码前缀自动判定(主板10%/科创创业20%/北交所30%);
  各策略据此放宽否决条件(涨跌停时量比萎缩/上影过长属于正常现象).
- 除权除息日容错: 若 pre_close 与日线 close 差距 > 20%，跳过涨跌停判定，避免假暴跌误杀.
- 集合竞价修正: 9:25-9:30 竞价期用昨日均量替代5日均量作为量比基准.
- 自适应换手率: 根据流通市值档位动态调整防飞刀换手上限(大蓝筹更宽松).
- 午休陷阱剔除: _curr_min_lunch_cleaned 剔除 A 股午间休市(11:30-13:00共90分钟)，
  保证下午已交易分钟数计算准确，预估全天成交量不会出现断崖式下跌。
- 量纲安全 VWAP: _safe_vwap_from_rt 对 TuShare 日线(千元)与实时行情(元)的量纲错位
  做双重校验，偏离昨收超过 20% 时自动降级到昨收价，防止 4% 诱多防线失效。
- VWAP换手率: 实时换手率估算优先使用成交额/成交量计算的VWAP均价.

【部署摘要】(完整步骤见模块末尾注释与项目集成说明)
- 定时任务 / 行情推送: 每只股票在回调里调用 evaluate_p3_intraday_screener(df, rt, cfg).
- 批量: screen_p3_universe(rows) 仅对内存列表循环，适合「当前这一批快照」增量过滤.

================================================================================
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from core.strategies.fund_mv_utils import (
        adaptive_turnover_f_range,
        circ_mv_wan_from,
        dynamic_inst_single_threshold_yuan,
        dynamic_net_main_threshold_yuan,
        effective_turnover_rate_f,
        series_effective_turnover_f_daily,
    )
except ImportError:
    from strategies.fund_mv_utils import (  # type: ignore
        adaptive_turnover_f_range,
        circ_mv_wan_from,
        dynamic_inst_single_threshold_yuan,
        dynamic_net_main_threshold_yuan,
        effective_turnover_rate_f,
        series_effective_turnover_f_daily,
    )

logger = logging.getLogger(__name__)

try:
    import constants as _C
    _P1_MIN_CIRC_MV_WAN = float(getattr(_C, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000))
except Exception:
    _P1_MIN_CIRC_MV_WAN = 1_000_000.0

BJ_TZ = timezone(timedelta(hours=8))


def _detect_limit_up(
    ts_code: str,
    now_px: float,
    pre_close: float,
    rt: Dict[str, Any],
) -> str:
    """
    【V26.7 新增】盘中动态涨跌停判定: 按 A 股板块规则精确计算涨停价.
    - 60/00 开头: 主板，涨停幅度 10%(部分 ST 为 5%)
    - 688 开头: 科创板，涨停幅度 20%
    - 300 开头: 创业板，涨停幅度 20%
    - 北交所(83/87/43/4 开头): 涨停幅度 30%
    - 若 rt['_is_limit'] 已存在则直接返回(上层已计算则不重复)
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


@dataclass
class P3IntradayScreenerConfig:
    """P3 硬阈值: 资金类为流通市值比例+阶梯地板; 换手一律真实换手 turnover_rate_f(可反算)."""

    # ---------- 全局 ----------
    # 【V26.7 修复】流通市值下限强制提升至 100 亿(1,000,000 万元)，彻底封死袖珍盘漏洞.
    circ_mv_min_wan: float = _P1_MIN_CIRC_MV_WAN
    # 【V26.7 新增】盘中 VWAP 脉冲诱多否决阈值: 当前价向上偏离实时 VWAP 超过此比例，判定为脉冲诱多，直接一票否决
    vwap_pulse_veto_pct: float = 4.0
    bias_20_max_pct: float = 8.0
    strong_stock_bias_20_max_pct: float = 12.0
    leader_bias_20_max_pct: float = 16.0
    strong_stock_circ_mv_mult: float = 1.4
    leader_sector_strength_min: float = 1.12
    leader_sector_rank_max: int = 2
    leader_sector_beta_min: float = 1.08
    leader_crowding_vr_floor: float = 1.55

    # ---------- 策略一 ----------
    s1_pct_low: float = 3.0
    s1_pct_high: float = 6.0
    s1_vol_ma5_mult: float = 1.5
    s1_winner_min: float = 85.0
    # 【V26.5 新增】P3-01 昨日涨幅区间(涨跌停时放宽到9.5%)
    s1_prev_pct_low: float = 3.0
    s1_prev_pct_high: float = 6.0
    # 【V26.5 新增】P3-01 ATR上限防飞刀
    s1_atr_pct_max: float = 8.0

    # ---------- 策略二 ----------
    s2_pct_low: float = -1.0
    s2_pct_high: float = 3.0
    s2_ma20_touch_ratio: float = 1.01
    s2_winner_min: float = 80.0
    s2_turnover_fly_max: float = 20.0
    s2_pb_fly_max: float = 10.0
    s2_pullback_bonus_bias_min: float = -2.5
    s2_pullback_bonus_bias_max: float = 2.5
    s2_pullback_close_to_ma20_pct: float = 1.5

    # ---------- 策略三 ----------
    s3_winner_min: float = 90.0
    s3_pct_low: float = 2.0
    s3_pct_high: float = 5.0
    s3_vol_ratio_min: float = 1.5
    s3_rsi_fly: float = 80.0
    # 【V26.5 新增】P3-03 昨日涨幅区间(涨跌停时放宽)
    s3_prev_pct_low: float = 3.0
    s3_prev_pct_high: float = 6.0
    # 【V26.5 新增】P3-03 长上影容忍(涨停日上影正常存在)
    s3_upper_shadow_max_pct: float = 2.5

    # ---------- 策略四 ----------
    s4_circ_mv_min_wan: float = 3_000_000.0
    s4_net_main_ratio_of_float_mv: float = 0.00035
    s4_inst_net_buy_ratio_of_float_mv: float = 0.00006

    # ---------- 策略五 ----------
    s5_vol_ratio_min: float = 1.8
    s5_cost50_mult: float = 1.05
    s5_atr_fly_max: float = 6.0
    # 【V26.5 新增】P3-05 MA20斜率下限(须有上攻动能)
    s5_ma20_slope_min: float = 0.5

    # ---------- 策略六 ----------
    s6_pct_low: float = 2.0
    s6_pct_high: float = 5.0
    s6_winner_min: float = 80.0
    s6_bias_fly: float = 10.0
    # 【V26.5 新增】P3-06 ATR上限防飞刀
    s6_atr_pct_max: float = 8.0

    # ---------- 策略七 ----------
    s7_pct_low: float = 1.0
    s7_pct_high: float = 3.0
    s7_net_main_ratio_of_float_mv: float = 0.00025
    s7_pre_close_low_mult: float = 0.99
    s7_vol_ratio_low: float = 1.2
    s7_vol_ratio_high: float = 2.5
    s7_turnover_fly: float = 30.0

    # ---------- 策略八: 倍量启动延续(昨倍量阳 + 沿 5/10 多头) ----------
    s8_vol_vs_vma5_mult: float = 1.85
    s8_pct_low: float = 0.8
    s8_pct_high: float = 5.5
    s8_vr_min: float = 1.15
    s8_winner_min: float = 78.0
    s8_ma20_slope_min: float = 0.15
    s8_yang_body_min: float = 0.998
    # 【V26.5 新增】P3-08 昨日涨幅区间(涨跌停时放宽)
    s8_prev_pct_low: float = 3.0
    s8_prev_pct_high: float = 6.0

    # ---------- 策略九: 质量趋势底仓(从 P4-05 迁入，归 P3 稳健池) ----------
    s9_pe_max: float = 20.0
    s9_turnover_f_max_lt: float = 4.0
    s9_pct_low: float = -1.0
    s9_pct_high: float = 1.5
    s9_ma60_downtrend_rel: float = 0.98
    s9_cost50_mult: float = 1.05
    s9_winner_min: float = 78.0
    s9_enabled: bool = True

    # ---------- 策略十: 缩量分歧低吸 ----------
    s10_touch_bias_ma20_low: float = -8.0
    s10_touch_bias_ma20_high: float = 5.0
    s10_touch_bias_ma5_abs: float = 8.0
    s10_tail_window_min: int = 30
    s10_need_market_contraction: float = 0.7

    # ---------- 环境与记忆增强 ----------
    regime_breaker_veto_score: float = 0.35
    regime_breaker_safety_mult: float = 0.82
    regime_boost_score: float = 1.08
    vwap_support_eps: float = 0.004
    t1_memory_weight: float = 0.22
    t1_memory_min_samples: int = 6
    t1_memory_score_boost: float = 8.0

    # ---------- 盘中成交量预测 ----------
    intraday_profile_lookback_days: int = 20
    intraday_anomaly_open_gap_pct: float = 2.2
    intraday_anomaly_limit_move_pct: float = 9.4
    intraday_anomaly_spike_ratio: float = 2.2
    intraday_profile_min_weight: float = 0.35
    intraday_profile_max_weight: float = 1.35
    intraday_profile_floor_ratio: float = 0.20
    intraday_profile_ceiling_ratio: float = 0.95

    # ---------- 突破类共性(防量价背离 / 假突破) ----------
    breakout_yang_vol_ratio_min: float = 1.05  # 近5日阳量均值 >= 阴量*该系数
    breakout_vwap_eps: float = 0.004  # 现价不得低于分时 VWAP 超过该比例(有额量时生效)
    giant_min_vol_ratio: float = 1.2  # 巨头连贯策略最低量比


def _resolve_p3_cfg(cfg: Optional[P3IntradayScreenerConfig]) -> P3IntradayScreenerConfig:
    if cfg is not None:
        return cfg
    from core.config_manager import get_p3_intraday_screener_config

    return get_p3_intraday_screener_config()


DEFAULT_P3_CONFIG = P3IntradayScreenerConfig()


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


def _macd_hist_series(row: pd.Series) -> float:
    """MACD 柱: 优先 macd_hist, 其次 macd_bar."""
    v = _safe_float(row.get("macd_hist"), 0.0)
    if v == 0.0 and "macd_bar" in row.index:
        v = _safe_float(row.get("macd_bar"), 0.0)
    return v


def _recent_yang_yin_vol_balance(df: pd.DataFrame, n: int = 5, ratio_min: float = 1.05) -> bool:
    """
    近 n 根已收盘 K: 上涨日成交量均值 vs 下跌日成交量均值(右侧放量须阳量占优).
    分母为 0 时返回 False，避免除零.
    """
    if df is None or len(df) < max(3, n):
        return False
    tail = df.tail(n)
    vc = "vol" if "vol" in tail.columns else ("volume" if "volume" in tail.columns else None)
    if vc is None:
        return False
    if "pct_chg" in tail.columns:
        pch = pd.to_numeric(tail["pct_chg"], errors="coerce").fillna(0.0)
    else:
        pch = pd.Series(0.0, index=tail.index)
    up = tail[(tail["close"] > tail["open"]) | (pch > 0)]
    dn = tail[(tail["close"] < tail["open"]) | (pch < 0)]
    av_u = float(pd.to_numeric(up[vc], errors="coerce").mean()) if not up.empty else 0.0
    av_d = float(pd.to_numeric(dn[vc], errors="coerce").mean()) if not dn.empty else 0.0
    if vc == "volume":
        av_u /= 100.0
        av_d /= 100.0
    if av_d <= 0:
        return av_u > 0
    return av_u >= av_d * ratio_min


# =============================================================================
# A 股时间体系工具（午休陷阱剔除 + 安全的 VWAP 量纲对齐）
# =============================================================================

def _curr_min_lunch_cleaned(rt: Dict[str, Any]) -> float:
    """
    【V26.7 新增】A股已交易分钟数（剔除午休时间）。

    A 股交易时间表:
      上午盘: 09:30 - 11:30  -> 120 分钟
      午休  : 11:30 - 13:00  -> 90 分钟（休市，不报价）
      下午盘: 13:00 - 15:00  -> 120 分钟
      全天合计: 240 分钟

    常见的 curr_min 是从 09:25 集合竞价或 09:30 开市累加的绝对分钟数，
    例如 09:31 对应 571（即 570 + 1），13:01 对应 631（即 570 + 61），
    其中 11:30=630, 13:00=720, 13:01=721。

    核心问题: 若直接用 curr_min - 570，则 13:01 的已交易分钟数被错误计为
    721-570=151 分钟（比实际多了 30 分钟），导致下午的预估全天成交量
    出现断崖式下跌，进而扭曲量比判断。

    本函数剔除 11:30(630) 到 13:00(720) 之间的 90 分钟午休时间，
    保证任意时刻的 elapsed_mins 都是真实已交易分钟数。

    返回值: 从 09:30 起算的纯交易分钟数（不含午休）。
    """
    curr_min = _safe_float(rt.get("curr_min"), 0.0)
    # 09:30 对应基准点 570（09:25=565, 09:30=570, 11:30=630, 13:00=720, 15:00=810）
    MORNING_END = 630     # 11:30 的绝对分钟数
    AFTERNOON_START = 720 # 13:00 的绝对分钟数
    DAY_START = 570      # 09:30 的绝对分钟数（基准点）

    if curr_min <= DAY_START:
        # 集合竞价阶段(09:25-09:30): 视为已交易 0 分钟
        return 0.0

    if curr_min <= MORNING_END:
        # 上午盘: 直接减去基准点
        return max(0.0, curr_min - DAY_START)

    # 下午盘: 先计满上午 120 分钟，再剔除午休，最后加上午后的分钟数
    # 午后分钟数 = curr_min - AFTERNOON_START
    afternoon_mins = max(0.0, curr_min - AFTERNOON_START)
    return 120.0 + afternoon_mins


def _safe_vwap_from_rt(
    rt: Dict[str, Any],
    ref_price: float,
    fallback_price: float,
) -> float:
    """
    【V26.7 新增】安全的盘中分时均价线（VWAP）计算。

    量纲错位问题（A股数据常见顽疾）:
    - TuShare 日线: amount 字段单位为"千元"（即 1000 元）
    - 实时行情快照: amount 字段单位为"元"（即元/股）
    - volume 字段有时为"手"（100股），有时为"股"（1股）

    上述组合会导致 amount/volume 计算出荒谬的 VWAP 值：
    - 例如: amount=5_000_000(元), vol=10_000(手=1_000_000股) -> VWAP=5元（正常）
    - 但若 vol 其实是"股"而误当"手": amount=5_000_000, vol=1_000_000 -> VWAP=5元（正常）
    - 量纲异常时: amount=5_000(千元=5_000_000元), vol=10_000(股) -> VWAP=500元（远超股价，异常！）

    判断量纲异常的核心依据:
    - 计算出的 VWAP 偏离参考价（通常为昨收价）超过 20%，基本可以判定量纲错位。
    - 此时降级使用 fallback_price（通常为昨收价）作为 VWAP 基准，保证后续判断不失效。

    参数:
        rt: 实时行情快照（包含 amount / volume 字段）
        ref_price: 参考价格（昨收价），用于量纲合理性判断
        fallback_price: 降级后的 VWAP 替代价格（通常与 ref_price 相同）

    返回: 安全的 VWAP 值（float），量纲异常时返回 fallback_price。
    """
    amt = _safe_float(rt.get("amount"), 0.0)
    vol = _safe_float(rt.get("volume"), 0.0)
    if amt <= 0 or vol <= 0:
        # 数据缺失时降级到 fallback
        return fallback_price

    # 第一步：假设 volume 单位为"股"，计算 tentative VWAP（元/股）
    tentative = amt / max(vol, 1e-9)

    # 量纲合理性校验：VWAP 偏离 ref_price 超过 20%，说明量纲可能错位
    if ref_price > 0 and abs(tentative - ref_price) / ref_price > 0.20:
        # 尝试修正：若 volume 实际上是"手"（1手=100股），则需要 /100
        vol_as_hand = vol * 100.0
        corrected = amt / max(vol_as_hand, 1e-9)
        if ref_price > 0 and abs(corrected - ref_price) / ref_price <= 0.20:
            # 修正后量纲合理，使用修正值
            return corrected
        # 修正后仍不合理，降级到 fallback
        return fallback_price

    return tentative


def _estimate_vwap_from_rt(rt: Dict[str, Any], ref_price: float) -> float:
    """【V26.7 重构】使用安全的 VWAP 计算，量纲异常时降级到 ref_price。"""
    return _safe_vwap_from_rt(rt, ref_price, ref_price)


def _price_on_vwap_ok(rt: Dict[str, Any], now_px: float, eps: float) -> bool:
    """有分时额量时要求现价站在 VWAP 之上(或贴邻)，否则不判定(缺数据不硬杀)."""
    vw = _estimate_vwap_from_rt(rt, now_px)
    if vw <= 0 or now_px <= 0:
        return True
    return now_px >= vw * (1.0 - eps)


def _field_rt_or_bar(rt: Dict[str, Any], y: pd.Series, key: str, default: float = 0.0) -> float:
    """优先实时 rt，其次昨日日线 y."""
    raw = rt.get(key)
    if raw is not None and not (isinstance(raw, float) and pd.isna(raw)):
        return _safe_float(raw, default)
    return _safe_float(y.get(key), default)


def _p3_mainline_score(rt: Dict[str, Any], y: pd.Series) -> Tuple[float, str]:
    """主线识别: 偏向强板块，板块前排，分时站稳的票."""
    sector_strength = _safe_float(rt.get("sector_strength", rt.get("industry_strength", rt.get("sector_beta", 1.0))), 1.0)
    sector_rank = _safe_float(rt.get("sector_rank"), 999.0)
    sector_total = _safe_float(rt.get("sector_total"), 0.0)
    board_beta = _safe_float(rt.get("sector_beta", rt.get("industry_beta", rt.get("sector_mult", 1.0))), 1.0)
    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    now_px = _safe_float(rt.get("price"), 0.0)
    vwap = _safe_float(rt.get("vwap"), now_px)
    pct_chg = 0.0
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    if now_px > 0 and pre_close > 0:
        pct_chg = (now_px - pre_close) / pre_close * 100.0

    score = 0.0
    reasons = []
    if sector_strength >= 1.12:
        score += 3.0
        reasons.append("强板块")
    elif sector_strength >= 1.05:
        score += 1.8
        reasons.append("中强板块")
    elif sector_strength <= 0.95:
        score -= 2.2
        reasons.append("板块偏弱")

    if board_beta >= 1.10:
        score += 1.0
        reasons.append("板块扩散")
    elif board_beta <= 0.92:
        score -= 1.0
        reasons.append("板块退潮")

    if sector_total >= 3 and sector_rank > 0:
        if sector_rank <= 2:
            score += 2.0
            reasons.append("板块前排")
        elif sector_rank <= 5:
            score += 1.0
            reasons.append("板块前列")
        elif sector_rank > sector_total - 3:
            score -= 2.0
            reasons.append("板块后排")

    if now_px > 0 and vwap > 0:
        if now_px >= vwap:
            score += 1.0
            reasons.append("站稳VWAP")
        else:
            score -= 1.5
            reasons.append("VWAP下方")

    if pct_chg >= 2.5 and vr < 1.2:
        score -= 1.5
        reasons.append("冲高无量")
    if pct_chg >= 3.0 and now_px > 0 and vwap > 0 and now_px < vwap * 1.002:
        score -= 2.0
        reasons.append("假突破回落")
    if pct_chg >= 4.0 and sector_strength < 1.05:
        score -= 1.2
        reasons.append("强度不匹配")

    if vr >= 1.2:
        score += 0.8
    if pct_chg >= 1.0:
        score += 0.8
    elif pct_chg < 0:
        score -= 0.8

    return float(score), "/".join(reasons) if reasons else "中性"


def _fund_flow_signal(rt: Dict[str, Any], y: pd.Series, now_px: float) -> Tuple[float, str, Dict[str, float]]:
    """资金流评分: 不做硬门禁，仅提供加分，背离和风险提示."""
    cm = _safe_float(rt.get("circ_mv"), _safe_float(y.get("circ_mv"), 0.0))
    if cm <= 0:
        cm = _safe_float(y.get("circ_mv"), 0.0)
    nm_rt = _safe_float(rt.get("net_main_amount"), np.nan)
    nm_y = _safe_float(y.get("net_main_amount"), 0.0)
    nm = nm_rt if not pd.isna(nm_rt) else nm_y
    inst = _safe_float(rt.get("inst_net_buy"), _safe_float(y.get("inst_net_buy"), 0.0))
    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    pct = (now_px - pre_close) / pre_close * 100.0 if now_px > 0 and pre_close > 0 else 0.0
    ma20 = _safe_float(y.get("ma20"), 0.0)
    bias20 = (now_px - ma20) / ma20 * 100.0 if now_px > 0 and ma20 > 0 else 0.0

    score = 0.0
    notes = []
    metrics = {"net_main_amount": nm, "inst_net_buy": inst, "fund_bias_20": bias20}

    if cm > 0:
        nm_ratio = nm / cm
        inst_ratio = inst / cm
        if nm > 0:
            score += 1.2
            notes.append("主力净流入")
            if nm_ratio >= 0.00025:
                score += 0.8
                notes.append("主力强度足")
        else:
            score -= 0.4
            notes.append("主力净流出")
            if pct > 0.8 and vr >= 1.2 and bias20 >= -1.5:
                score += 0.6
                notes.append("价强量配合")
            if pct > 2.0 and vr < 1.2:
                score -= 0.8
                notes.append("冲高无量背离")

        if inst > 0:
            score += 0.6
            notes.append("机构净买入")
            if inst_ratio >= 0.00006:
                score += 0.4
                notes.append("机构强度足")
        else:
            score -= 0.2
            notes.append("机构净卖出")

        if nm > 0 and inst > 0:
            score += 0.4
            notes.append("资金同向")
        elif nm < 0 and inst < 0:
            score -= 0.4
            notes.append("资金同向流出")

    if pct >= 2.0 and nm > 0:
        score += 0.4
    if pct <= 0 and nm > 0:
        notes.append("资金领先但价格未证实")
        score -= 0.2

    return float(score), "/".join(notes) if notes else "资金中性", metrics


def _intraday_elapsed_minutes(rt: Dict[str, Any]) -> int:
    """
    已交易分钟数(与 scan_engine 主会话 9:30~15:00 对齐，中间非连续简化为 240 分钟满额日).
    流式场景可在 rt['elapsed_mins'] 注入，避免多股重复调用系统时钟.
    """
    em = rt.get("elapsed_mins")
    if em is not None:
        return max(1, int(_safe_float(em, 1.0)))
    try:
        now = datetime.now(BJ_TZ)
        cm = now.hour * 60 + now.minute
        if cm < 565 or cm > 900:
            return 240
        if 565 <= cm < 570:
            return 1
        return max(1, cm - 570)
    except Exception as _e:
        logging.getLogger(__name__).debug("_intraday_elapsed_minutes 异常回退 120: %s", _e, exc_info=True)
        return 120


def _intraday_time_ratio(rt: Dict[str, Any]) -> float:
    """按交易时段粗略估计当日进度(9:30~15:00 记为 1.0，午间不连续时段按满额修正)."""
    em = max(1, _intraday_elapsed_minutes(rt))
    return float(min(1.0, max(0.01, em / 240.0)))


def _intraday_profile_bucket(rt: Dict[str, Any]) -> str:
    em = max(1, _intraday_elapsed_minutes(rt))
    if em <= 30:
        return "open"
    if em <= 90:
        return "morning"
    if em <= 160:
        return "midday"
    if em <= 210:
        return "afternoon"
    return "close"


def _intraday_profile_base_share(bucket: str) -> float:
    profile = {
        "open": 0.28,
        "morning": 0.50,
        "midday": 0.70,
        "afternoon": 0.84,
        "close": 0.96,
    }
    return float(profile.get(bucket, 0.70))


def _intraday_anomaly_weight(rt: Dict[str, Any], y: pd.Series) -> float:
    """异常日权重: 开盘跳空，涨停/跌停，极端放量时，降低历史模板权重."""
    now_px = _safe_float(rt.get("price"), 0.0)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    open_px = _safe_float(rt.get("open"), now_px)
    high_px = _safe_float(rt.get("high"), now_px)
    low_px = _safe_float(rt.get("low"), now_px)
    pct = (now_px - pre_close) / pre_close * 100.0 if now_px > 0 and pre_close > 0 else 0.0
    gap_pct = (open_px - pre_close) / pre_close * 100.0 if open_px > 0 and pre_close > 0 else 0.0
    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))

    weight = 1.0
    if abs(gap_pct) >= 2.2:
        weight *= 0.78
    if pct >= 9.4 or high_px >= pre_close * 1.095:
        weight *= 0.62
    if pct <= -9.4 or low_px <= pre_close * 0.905:
        weight *= 0.62
    if vr >= 2.2:
        weight *= 0.82
    return float(max(0.35, min(1.0, weight)))


def _hist_intraday_volume_share(df: pd.DataFrame, bucket: str, lookback_days: int = 20) -> float:
    """用历史日线的量能特征估算当前时点已完成全天量的比例，缺少字段时退回时段基准."""
    if df is None or df.empty:
        return _intraday_profile_base_share(bucket)
    hist = df.tail(max(5, lookback_days))
    cols = [c for c in ["vol_ratio", "turnover_rate_f", "turnover_f", "volume", "vol"] if c in hist.columns]
    if not cols:
        return _intraday_profile_base_share(bucket)
    base = _intraday_profile_base_share(bucket)
    metric = pd.to_numeric(hist[cols[0]], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if metric.empty:
        return base
    med = float(metric.median())
    last = float(metric.iloc[-1])
    delta = 0.0 if med <= 0 else (last - med) / max(abs(med), 1e-9)
    return float(np.clip(base + delta * 0.04, 0.18, 0.98))


def projected_vol_hand_today(rt: Dict[str, Any], y: pd.Series) -> float:
    """
    【V26.7 重构】预估当日总成交量(手): 历史分时占比模板 + 异常权重 + 午休清洗后的分钟数放大.

    盘中量比失真问题:
    - 早盘09:30-10:00: 前5分钟量能可能极大(集合竞价余波 + 开盘惯性)，
      导致"量比"虚高，用此时段的量估算全天会产生严重高估。
    - 本函数通过午休清洗后的 elapsed_mins 计算已交易分钟数，在早盘时段施加保守系数，
      避免量比失真导致的误判。

    A 股午休陷阱:
    - A 股每日 11:30-13:00 为午间休市（90分钟），此期间行情数据为空。
    - 若 curr_min 是从 09:25 累加的绝对分钟数（如13:01=721），直接减570会导致
      下午的 elapsed_mins 虚增30分钟，使下午的预估全天成交量出现断崖式下跌。
    - 本函数通过 _curr_min_lunch_cleaned() 剔除午休，保证任意时刻的时间进度计算正确。

    A 股 T+1 制度下，全天成交量应随时间线性增长。
    若当前已交易 60 分钟(占全天 25%)，则已成交量的 4 倍是全天预估。
    早盘时段(<120 分钟)施加保守系数防止异常放大。
    """
    pv = rt.get("projected_vol_hand")
    if pv is not None and _safe_float(pv, 0.0) > 0:
        return _safe_float(pv, 0.0)
    vol_shares = _safe_float(rt.get("volume"), 0.0)
    if vol_shares <= 0:
        return 0.0
    vol_hand = vol_shares / 100.0

    # 【V26.7 重构】使用午休时间清洗函数剔除 A 股午休时段（11:30-13:00，共90分钟）
    # 保证下午 13:00 之后的已交易分钟数计算准确，不会出现断崖式下跌
    elapsed_mins = _curr_min_lunch_cleaned(rt)

    # A 股每日交易约 240 分钟(09:30-15:00)，已交易分钟数 / 240 = 时间进度
    trade_mins_per_day = 240.0
    time_progress = min(1.0, elapsed_mins / trade_mins_per_day)
    time_progress = max(0.01, time_progress)  # 防止除零

    # 早盘时段(<120 分钟)保守系数: 此时段量比波动大，防止预估被极端值带偏
    early_session_weight = 1.0
    if elapsed_mins < 120.0:
        # 越早越保守: 09:30 时权重约 0.55，随时间逐渐回归 1.0
        early_session_weight = max(0.45, elapsed_mins / 120.0)

    # 基于时间的全天预估(线性放大)
    time_based_projection = vol_hand / time_progress if time_progress > 0 else vol_hand * 100.0

    # 历史分时模板预估(原有逻辑)
    bucket = _intraday_profile_bucket(rt)
    elapsed_ratio = _intraday_time_ratio(rt)
    hist_share = _hist_intraday_volume_share(
        y.to_frame().T if isinstance(y, pd.Series) else pd.DataFrame(), bucket, 20
    )
    anomaly_w = _intraday_anomaly_weight(rt, y)
    effective_share = max(0.12, min(0.98, hist_share * anomaly_w))
    if elapsed_ratio > 0:
        effective_share = min(effective_share, max(0.12, elapsed_ratio))

    # 历史模板预估
    hist_based_projection = vol_hand / max(effective_share, 1e-6)
    hist_based_projection *= float(np.clip((0.72 + 0.28 * anomaly_w), 0.7, 1.05))

    # 【V26.7 融合策略】早盘时段用时间放大，晚盘时段用历史模板
    if elapsed_mins < 120.0:
        # 早盘: 时间预估权重更高(因为历史模板在早盘失真更严重)
        projected = time_based_projection * 0.6 + hist_based_projection * 0.4
        projected *= early_session_weight  # 施加早盘保守系数
    else:
        # 中后盘: 两者接近，加权平均
        projected = (time_based_projection + hist_based_projection) * 0.5

    return float(max(vol_hand, projected))


def _resolve_risk_mode(rt: Dict[str, Any], y: pd.Series, cfg: P3IntradayScreenerConfig) -> str:
    """普通股 / 强势股 / 主线龙头 三层风控分级."""
    sector_strength, _ = _p3_mainline_score(rt, y)
    sector_rank = _safe_float(rt.get("sector_rank"), 999.0)
    sector_beta = _safe_float(rt.get("sector_beta", rt.get("industry_beta", 1.0)), 1.0)
    cm = _safe_float(rt.get("circ_mv"), _safe_float(y.get("circ_mv"), 0.0))
    base_cmw = max(cfg.circ_mv_min_wan, 1.0)

    is_leader = (
        sector_strength >= 2.8
        and sector_rank > 0
        and sector_rank <= cfg.leader_sector_rank_max
        and sector_beta >= cfg.leader_sector_beta_min
    )
    if is_leader:
        return "leader"

    is_strong = (
        sector_strength >= 1.4
        or cm >= base_cmw * cfg.strong_stock_circ_mv_mult
        or sector_beta >= 1.03
    )
    if is_strong:
        return "strong"

    return "ordinary"


def p3_global_risk_veto(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: Optional[P3IntradayScreenerConfig] = None,
) -> Tuple[bool, str]:
    """
    P3 全局风控(一票否决).

    使用「实时价」= rt['price'](缺失则无法用乖离/破位逻辑，直接否决).
    资金流不再作为硬门禁，仅作为后续评分与背离提示.
    """
    cfg = _resolve_p3_cfg(cfg)
    if df is None or df.empty or len(df) < 2:
        return False, "历史K线不足"

    y = df.iloc[-1]
    now_px = _safe_float(rt.get("price"), 0.0)
    if now_px <= 0:
        return False, "缺少有效现价"

    # 【V26.7 新增】动态涨跌停检测: 按代码前缀判断(主板10%，科创/创业20%，北交所30%)
    ts_code = str(rt.get("ts_code", y.get("ts_code", "")) or "")
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("pre_close"), _safe_float(y.get("close"), 0.0)))
    rt["_y_close"] = _safe_float(y.get("close"), 0.0)
    rt["_is_limit"] = _detect_limit_up(ts_code, now_px, pre_close, rt)
    is_limit_up = rt["_is_limit"] == "UP"

    mode = _resolve_risk_mode(rt, y, cfg)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    slope = _safe_float(y.get("ma20_slope_5"), 0.0)
    bias_live = ((now_px - ma20) / ma20 * 100.0) if ma20 > 0 else 0.0

    # 【V26.7 新增】盘中 VWAP 脉冲诱多硬否决:
    # A 股 T+1 制度下，尾盘/盘中脉冲拉升后资金无法当日卖出，是最常见的诱多陷阱。
    # 判定条件: 当前价向上偏离实时 VWAP 超过阈值(如 4%)，且涨幅明显。
    # 涨停时不触发(涨停价本身高于 VWAP 是正常的).
    # 量纲安全: VWAP 可能缺失，若缺失则跳过此否决，不全灭。
    if not is_limit_up:
        vwap_raw = rt.get("vwap")
        if vwap_raw is not None and not (isinstance(vwap_raw, float) and pd.isna(vwap_raw)):
            try:
                vwap = float(vwap_raw)
                if vwap > 0:
                    vwap_gap = (now_px - vwap) / vwap * 100.0
                    if vwap_gap > float(cfg.vwap_pulse_veto_pct):
                        return False, f"脉冲诱多否决: 现价偏离VWAP {vwap_gap:.2f}% > {float(cfg.vwap_pulse_veto_pct):.1f}%"
            except (TypeError, ValueError):
                pass  # VWAP 数据异常，跳过此否决

    # 1) 趋势向下规避: 普通股与强势股都不能逆势，龙头可保留更宽松的均线回踩窗口
    if ma60 > 0 and now_px <= ma60 and mode != "leader":
        return False, "趋势规避: 现价 <= ma60"
    if slope <= 0 and mode != "leader":
        return False, f"趋势规避: ma20_slope_5={slope:.4f} <= 0"

    # 2) 破位飞刀: 普通股严格，强势股适度放宽，龙头允许回踩但不允许深度失守
    if ma20 > 0:
        if mode == "ordinary" and now_px < ma20:
            return False, "破位飞刀: 现价 < ma20"
        if mode == "strong" and now_px < ma20 * 0.985:
            return False, "强势股回踩失守: 现价 < ma20*0.985"
        if mode == "leader" and now_px < ma20 * 0.975:
            return False, "龙头回踩失守: 现价 < ma20*0.975"

    # 3) 盘子过小: circ_mv 万元
    cm = _safe_float(rt.get("circ_mv"), _safe_float(y.get("circ_mv"), 0.0))
    if cm < cfg.circ_mv_min_wan:
        return False, f"流通市值过小 circ_mv={cm:.0f}万 < {cfg.circ_mv_min_wan:.0f}万"

    # 4) 过度乖离: 普通股严格，强势股放宽，龙头使用更高阈值但仍保留极端约束
    bias_limit = cfg.bias_20_max_pct
    if mode == "strong":
        bias_limit = cfg.strong_stock_bias_20_max_pct
    elif mode == "leader":
        bias_limit = cfg.leader_bias_20_max_pct

    if bias_live > bias_limit:
        return False, f"过度乖离: bias_20={bias_live:.2f}% > {bias_limit:.2f}%({mode})"

    # 5) 强势/龙头的拥挤保护: 仅在极端加速时才做更严格保护
    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    if mode == "ordinary" and vr >= 3.0 and bias_live >= 6.0:
        return False, "普通股拥挤过热"
    if mode == "strong" and vr >= 4.0 and bias_live >= 8.0:
        return False, "强势股拥挤过热"
    if mode == "leader" and vr >= 5.0 and bias_live >= 10.0:
        return False, "龙头极端拥挤过热"
    if mode == "leader" and vr < cfg.leader_crowding_vr_floor and bias_live > 8.0:
        return False, "龙头高位无承接"

    return True, ""


def p3_legacy_hard_veto(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: Optional[P3IntradayScreenerConfig] = None,
) -> Tuple[bool, str]:
    """
    专项回测 legacy 模式: 恢复近似旧版「CCI 动能下限，九转见顶」硬否决.

    仅在 is_backtest_legacy_mode() 为 True 时生效; 实盘默认关闭，不改变物理胸甲剥离版行为.
    """
    try:
        from core.backtest_context import is_backtest_legacy_mode
    except ImportError:
        return True, ""
    if not is_backtest_legacy_mode():
        return True, ""
    if df is None or df.empty:
        return False, "历史K线不足"
    y = df.iloc[-1]
    cci = _safe_float(y.get("cci"), np.nan)
    if np.isfinite(cci) and cci < 100.0:
        return False, "legacy: CCI<100 动能不足"
    nt = _safe_float(y.get("nineturn_signal"), 0.0)
    if nt >= 9.0:
        return False, "legacy: 九转见顶(nineturn>=9)"
    return True, ""


# =============================================================================
# 策略一: 右侧起爆
# =============================================================================
def _strategy_right_side_ignite(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    prev: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
    proj_vol_hand: float,
) -> Tuple[bool, str]:
    """
    策略一: 右侧起爆(保留为辅助确认，不再作为唯一强进攻入口).

    昨日:
        pct_chg 3%~6%(涨停时放宽到9.5%)
        MACD 柱刚翻红(金叉)
    今日:
        pct 3%~6%(涨停时放宽到9.5%)
        预估全日量 > vol_ma5 * 1.5
        vol_ma5 > vol_ma20
        winner_rate > 85%
    防飞刀:
        突破布林上轨，阳量未压过阴量，未站稳VWAP，ATR超限
    """
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    # 昨日涨幅区间(涨跌停时放宽)
    y_pct = _safe_float(y.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit_up else cfg.s1_prev_pct_high
    if not (cfg.s1_prev_pct_low <= y_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s1_prev_pct_low}%~{pct_high_prev}%"

    mh_now = _macd_hist_series(y)
    mh_prev = _macd_hist_series(prev)
    if not (mh_now > 0 and mh_prev <= 0):
        return False, "MACD 柱未呈现刚翻红"

    vma5 = _safe_float(y.get("vol_ma5"), 0.0)
    vma20 = _safe_float(y.get("vol_ma20"), 0.0)
    if vma5 <= 0 or vma20 <= 0:
        return False, "量能均线缺失"
    if not (proj_vol_hand > vma5 * cfg.s1_vol_ma5_mult):
        return False, "预估全日量未达 vol_ma5*1.5"
    if not (vma5 > vma20):
        return False, "vol_ma5 未大于 vol_ma20"

    wr = _field_rt_or_bar(rt, y, "winner_rate")
    if wr <= cfg.s1_winner_min:
        return False, "winner_rate 不足"

    boll_u = _safe_float(y.get("boll_upper"), 0.0)
    if boll_u > 0 and now_px > boll_u:
        return False, "防飞刀: 突破布林上轨"

    if not _recent_yang_yin_vol_balance(df, 5, cfg.breakout_yang_vol_ratio_min):
        return False, "量价结构: 近5日阳量未压过阴量"
    if not _price_on_vwap_ok(rt, now_px, cfg.breakout_vwap_eps):
        return False, "分时均价: 现价未站稳VWAP"

    # 涨停时涨幅上限放宽
    pct_high_s1 = 9.5 if is_limit_up else cfg.s1_pct_high
    if not (cfg.s1_pct_low <= pct <= pct_high_s1):
        return False, f"涨幅不在 {cfg.s1_pct_low}%~{pct_high_s1}%"

    # ATR波动率防飞刀(涨停时跳过)
    if not is_limit_up:
        atrp = _safe_float(y.get("atr_pct"), 0.0)
        if atrp > cfg.s1_atr_pct_max:
            return False, f"防飞刀: atr_pct={atrp:.2f}% > {cfg.s1_atr_pct_max}%"

    return True, "OK"


# =============================================================================
# 策略二: 均线回踩低吸
# =============================================================================
def _strategy_ma20_pullback(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
    low_px: float,
    proj_vol_hand: float,
) -> Tuple[bool, str]:
    """
    策略二: 均线回踩低吸(优先趋势内回踩，兼容突破确认).

    条件:
        pct 在昨日涨幅区间(涨跌停时放宽到9.5%)
        现价站回 ma20 上方
        ma5 > ma20(均线多头)
        回踩深度合理(涨停时放宽)
        预估全日量未明显放大(缩量确认)
        MACD 柱为正
        winner_rate, 换手率, PB 在合理范围
    """
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"
    pct_low = cfg.s2_pct_low
    pct_high = 9.5 if is_limit_up else cfg.s2_pct_high
    if not (pct_low <= pct <= pct_high):
        return False, f"涨幅不在 {pct_low}%~{pct_high}%"

    ma20_pb = _safe_float(y.get("ma20"), 0.0)
    ma5 = _safe_float(y.get("ma5"), 0.0)
    if ma20_pb <= 0 or ma5 <= 0:
        return False, "ma20 或 ma5 缺失"

    touch_ma20 = low_px <= ma20_pb * cfg.s2_ma20_touch_ratio
    near_ma20 = abs(now_px - ma20_pb) / ma20_pb * 100.0 <= cfg.s2_pullback_close_to_ma20_pct if ma20_pb > 0 else False
    above_ma20 = now_px >= ma20_pb
    pullback_bias = (now_px - ma20_pb) / ma20_pb * 100.0 if ma20_pb > 0 else 0.0

    if not (touch_ma20 or near_ma20):
        return False, "未形成 ma20 回踩/贴线"
    if not above_ma20:
        return False, "现价未站回 ma20 上方"
    if ma5 < ma20_pb:
        return False, "ma5 未强于 ma20"

    vma5 = _safe_float(y.get("vol_ma5"), 0.0)
    if vma5 <= 0:
        return False, "vol_ma5 缺失"
    if not (proj_vol_hand <= vma5 * 1.05):
        return False, "预估全日量未体现缩量"

    if _macd_hist_series(y) < 0:
        return False, "macd_hist 未转强"
    wr = _field_rt_or_bar(rt, y, "winner_rate")
    if wr <= cfg.s2_winner_min:
        return False, "winner_rate 不足"

    turnover_f = effective_turnover_rate_f(rt, y, now_px if now_px > 0 else _safe_float(y.get("close"), 0.0))
    cmw = _field_rt_or_bar(rt, y, "circ_mv", default=0.0)
    tf_high = adaptive_turnover_f_range(cmw, cfg.s2_turnover_fly_max, cfg.s2_turnover_fly_max)[1]
    if turnover_f > tf_high:
        return False, f"防飞刀: 真实换手{turnover_f:.2f}%超过市值自适应上限{tf_high:.2f}%"
    pb = _safe_float(y.get("pb"), 0.0)
    if pb > cfg.s2_pb_fly_max:
        return False, "防飞刀: pb>10"

    # 涨停时回踩过浅不否决(涨停时价格已大幅偏离均线)
    if not is_limit_up and pullback_bias > cfg.s2_pullback_bonus_bias_max:
        return False, "回踩过浅"

    return True, "OK"


# =============================================================================
# 策略三: 单峰跃迁
# =============================================================================
def _strategy_single_peak_break(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
) -> Tuple[bool, str]:
    """
    策略三: 单峰跃迁(筹码密集区突破).

    条件:
        昨日涨幅在区间内(涨跌停时放宽)
        昨日 MACD 柱为正
        昨收站稳 MA5(确保前日处于上升趋势)
        昨日主力净额为正(资金持续流入确认)
        今日涨幅在区间内(涨跌停时放宽到9.5%)
        量比, RSI, 高乖离缩量，阳量VWAP 均有涨跌停宽容
        长上影防飞刀(涨停时容忍)
    """
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    # 昨日涨幅区间(涨跌停时放宽)
    y_pct = _safe_float(y.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit_up else cfg.s3_prev_pct_high
    if not (cfg.s3_prev_pct_low <= y_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s3_prev_pct_low}%~{pct_high_prev}%"

    # 昨日 MACD 柱须为正(昨日要有上涨动能)
    mh = _macd_hist_series(y)
    if mh <= 0:
        return False, "昨日 macd_hist 未为正"

    # 昨收须站稳 MA5(确保竞价前日处于健康上升趋势)
    y_close_local = _safe_float(y.get("close"), 0.0)
    ma5_y = _safe_float(y.get("ma5"), 0.0)
    if ma5_y > 0 and y_close_local <= ma5_y:
        return False, "昨收未站稳 ma5"

    # 昨日主力净额须为正(资金持续流入确认，防单日异动)
    nm_y = _safe_float(y.get("net_main_amount"), 0.0)
    if nm_y <= 0:
        return False, "昨日主力净额非正"

    wr = _field_rt_or_bar(rt, y, "winner_rate")
    if wr <= cfg.s3_winner_min:
        return False, "winner_rate 不足"

    c95 = _field_rt_or_bar(rt, y, "cost_95th", default=-1.0)
    if c95 <= 0 or now_px <= c95:
        return False, "未突破 cost_95th"

    # 涨幅上限放宽
    pct_high_s3 = 9.5 if is_limit_up else cfg.s3_pct_high
    if not (cfg.s3_pct_low <= pct <= pct_high_s3):
        return False, f"涨幅不在 {cfg.s3_pct_low}%~{pct_high_s3}%"

    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    # 涨停时量比萎缩不构成否决
    if not is_limit_up and vr < cfg.s3_vol_ratio_min:
        return False, "量比不足"

    h20 = _safe_float(y.get("high_20"), 0.0)
    if h20 > 0 and now_px <= h20:
        return False, "防飞刀: 未突破昨日 high_20(假突破)"

    rsi = _safe_float(y.get("rsi_14"), 0.0)
    if rsi > cfg.s3_rsi_fly:
        return False, "防飞刀: RSI14 超买"

    # 高位乖离 + 缩量: 易为假突破/骗线，收紧过滤(涨停时放宽)
    b20 = _safe_float(y.get("bias_20"), 0.0)
    if not is_limit_up and b20 > 16.0 and pct > 2.0 and vr < 1.8:
        return False, "防飞刀: 高乖离缩量上攻"

    if not _recent_yang_yin_vol_balance(df, 5, cfg.breakout_yang_vol_ratio_min):
        return False, "量价结构: 阳量未压过阴量"
    if not _price_on_vwap_ok(rt, now_px, cfg.breakout_vwap_eps):
        return False, "分时均价: 未站稳VWAP"

    # 长上影防飞刀(涨停时容忍)
    y_high = _safe_float(y.get("high"), 0.0)
    y_close = _safe_float(y.get("close"), 0.0)
    if y_close > 0:
        upper = (y_high - y_close) / y_close * 100.0
        if not is_limit_up and upper > cfg.s3_upper_shadow_max_pct:
            return False, f"防飞刀 长上影 {upper:.2f}% > {cfg.s3_upper_shadow_max_pct}%"

    return True, "OK"


def _hk_vol_positive_last_n(df: pd.DataFrame, n: int = 3) -> bool:
    """近 n 根日线 hk_vol 是否均 > 0."""
    if df is None or len(df) < n:
        return False
    tail = df.tail(n)
    # 【性能优化 V2】向量化替代 iterrows：一行布尔 all() 检查
    if "hk_vol" not in tail.columns:
        return False
    hk_v = pd.to_numeric(tail["hk_vol"], errors="coerce").fillna(0)
    return bool((hk_v > 0).all())


# =============================================================================
# 策略四: 巨头连贯发力
# =============================================================================
def _strategy_giant_continuous(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    prev: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
) -> Tuple[bool, str]:
    """
    策略四: 巨头连贯发力(连续2日机构净买 + 主力持续流入).

    条件:
        cmw >= 300 亿
        主力净额超过动态门槛
        近2日机构净买达到动态门槛
        近3日北向数据辅助观察
        均线多头排列 ma5 > ma20 > ma60
        涨幅在区间(涨停时放宽到9.5%)
        量比足够(涨停时放宽)
    """
    cmw = circ_mv_wan_from(y, rt)
    if cmw <= cfg.s4_circ_mv_min_wan:
        return False, "流通市值未达 300 亿"

    nm_gc = _safe_float(rt.get("net_main_amount"), np.nan)
    nm_gc = nm_gc if not pd.isna(nm_gc) else _safe_float(y.get("net_main_amount"), 0.0)
    thr_yuan = dynamic_net_main_threshold_yuan(cmw, cfg.s4_net_main_ratio_of_float_mv)
    if nm_gc <= thr_yuan:
        return False, "主力净额不足(动态门槛)"

    # 昨日龙虎榜机构净买须达动态门槛
    inst_y = _safe_float(y.get("inst_net_buy"), 0.0)
    inst_thr = dynamic_inst_single_threshold_yuan(cmw, cfg.s4_inst_net_buy_ratio_of_float_mv)
    if inst_y <= inst_thr:
        return False, "昨日 inst_net_buy 未达动态门槛"

    # 前天机构净买也须达动态门槛(连贯确认: 至少连续2日机构买入)
    inst_prev = _safe_float(prev.get("inst_net_buy"), 0.0) if len(df) >= 2 else 0.0
    if inst_prev <= inst_thr:
        return False, "前日 inst_net_buy 未达动态门槛(需连贯)"

    # hk_vol 为日线结算数据，盘中无法获取当日值，近3日 hk_vol > 0 仅作为辅助观察，不阻断命中
    hk_positive_count = 0
    if "hk_vol" in df.columns:
        for idx in range(-3, 0):
            if len(df) >= abs(idx):
                hk_val = _safe_float(df.iloc[idx].get("hk_vol"), 0.0)
                if hk_val > 0:
                    hk_positive_count += 1
    rt["_hk_vol_positive_days"] = hk_positive_count

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if not (ma5 > ma20 > ma60):
        return False, "均线未呈 ma5>ma20>ma60"

    # 涨停时涨幅上限可放宽到9.5%
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"
    pct_high_s4 = 9.5 if is_limit_up else 4.5
    if not (1.5 <= pct <= pct_high_s4):
        return False, f"涨幅不在 1.5%~{pct_high_s4}%"

    vr_gc = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    # 涨停时量比萎缩属于正常现象
    if not is_limit_up and vr_gc < cfg.giant_min_vol_ratio:
        return False, "量比不足(巨头连贯)"

    return True, "OK"


# =============================================================================
# 策略五: 平台二次确认
# =============================================================================
def _strategy_platform_reattack(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
) -> Tuple[bool, str]:
    """
    策略五: 平台二次确认.

    条件:
        突破 high_20
        MA20 斜率 > 下限(须有上攻动能，涨停时放宽)
        量比 > 下限(涨停时放宽)
        突破 cost_50th
        主力净额为正
        ATR 在合理范围
        MACD 柱为正 + 阳量结构良好
    """
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    h20 = _safe_float(y.get("high_20"), 0.0)
    if h20 <= 0 or now_px <= h20:
        return False, "未站上 high_20"

    slope = _safe_float(y.get("ma20_slope_5"), 0.0)
    slope_min = cfg.s5_ma20_slope_min * 0.8 if is_limit_up else cfg.s5_ma20_slope_min
    if slope <= slope_min:
        return False, f"ma20_slope_5 {slope:.2f} 未 > {slope_min:.2f}"

    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    # 涨停时量比下限放宽
    vr_min = cfg.s5_vol_ratio_min * 0.7 if is_limit_up else cfg.s5_vol_ratio_min
    if vr < vr_min:
        return False, f"量比 {vr:.2f} < {vr_min:.2f}"

    c50 = _field_rt_or_bar(rt, y, "cost_50th")
    if c50 <= 0 or now_px <= c50 * cfg.s5_cost50_mult:
        return False, "未显著高于 cost_50th"

    nm_pf = _safe_float(rt.get("net_main_amount"), _safe_float(y.get("net_main_amount"), 0.0))
    if nm_pf < 0:
        return False, "防飞刀: 主力净额为负"

    atrp = _safe_float(y.get("atr_pct"), 0.0)
    if atrp > cfg.s5_atr_fly_max:
        return False, "防飞刀: atr_pct 过高"

    if _macd_hist_series(y) <= 0:
        return False, "macd_hist 未为正(平台突破)"
    if not _recent_yang_yin_vol_balance(df, 5, cfg.breakout_yang_vol_ratio_min):
        return False, "量价结构: 阳量未压过阴量"
    if not _price_on_vwap_ok(rt, now_px, cfg.breakout_vwap_eps):
        return False, "分时均价: 未站稳VWAP"

    return True, "OK"


# =============================================================================
# 策略六: 水上金叉
# =============================================================================
def _strategy_macd_golden_water(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    prev: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
) -> Tuple[bool, str]:
    """
    策略六: 水上金叉(日线 MACD 柱 + 现价乖离; 若有 rt 覆盖 macd 则优先).

    条件:
        MACD 在零轴上方 + 刚上穿 Signal
        涨幅在区间(涨停时放宽到9.5%)
        量能多头排列
        winner_rate 在下限之上
        乖离率在合理范围(涨停时放宽)
        ATR 上限防飞刀(涨停时跳过)
    """
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    m = _safe_float(rt.get("macd"), _safe_float(y.get("macd"), 0.0))
    sig = _safe_float(rt.get("macd_signal"), _safe_float(y.get("macd_signal"), 0.0))
    m_prev = _safe_float(prev.get("macd"), 0.0)
    sig_prev = _safe_float(prev.get("macd_signal"), 0.0)

    if m <= 0:
        return False, "macd 未在水上"

    if not (m > sig and m_prev <= sig_prev):
        return False, "未检测到 macd 刚上穿 signal"

    # 涨幅上限放宽
    pct_high_s6 = 9.5 if is_limit_up else cfg.s6_pct_high
    if not (cfg.s6_pct_low <= pct <= pct_high_s6):
        return False, f"涨幅不在 {cfg.s6_pct_low}%~{pct_high_s6}%"

    vma5 = _safe_float(y.get("vol_ma5"), 0.0)
    vma20 = _safe_float(y.get("vol_ma20"), 0.0)
    if vma5 <= vma20:
        return False, "vol_ma5 未大于 vol_ma20"

    wr = _field_rt_or_bar(rt, y, "winner_rate")
    if wr < cfg.s6_winner_min:
        return False, "winner_rate 不足"

    ma20 = _safe_float(y.get("ma20"), 0.0)
    if ma20 > 0:
        bias = (now_px - ma20) / ma20 * 100.0
        # 乖离率容忍
        bias_max = cfg.s6_bias_fly * 1.5 if is_limit_up else cfg.s6_bias_fly
        if bias > bias_max:
            return False, f"防飞刀: bias>{bias_max:.1f}%"

    # ATR上限防飞刀(涨停时跳过)
    if not is_limit_up:
        atrp = _safe_float(y.get("atr_pct"), 0.0)
        if atrp > cfg.s6_atr_pct_max:
            return False, f"防飞刀: atr_pct={atrp:.2f}% > {cfg.s6_atr_pct_max}%"

    return True, "OK"


# =============================================================================
# 策略七: 资金逆势托底
# =============================================================================
def _strategy_counter_trend_bid(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
    low_px: float,
) -> Tuple[bool, str]:
    """
    策略七: 资金逆势托底.

    条件:
        涨幅在区间(涨停时放宽到9.5%)
        主力净额超过动态门槛
        盘中有一定支撑(涨停时放宽)
        量比在区间(涨停时放宽上下限)
        换手率在合理范围
    """
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    # 涨跌停宽容
    pct_low_s7 = cfg.s7_pct_low
    pct_high_s7 = 9.5 if is_limit_up else cfg.s7_pct_high
    if not (pct_low_s7 <= pct <= pct_high_s7):
        return False, f"涨幅不在 {pct_low_s7}%~{pct_high_s7}%"

    cmw = circ_mv_wan_from(y, rt)
    nm = _safe_float(rt.get("net_main_amount"), _safe_float(y.get("net_main_amount"), 0.0))
    thr_m = dynamic_net_main_threshold_yuan(cmw, cfg.s7_net_main_ratio_of_float_mv)
    if nm <= thr_m:
        return False, "主力净额不足(动态门槛)"

    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    if pre_close <= 0:
        return False, "昨收缺失"
    # 涨停时允许更大回踩深度
    pre_close_mult = 0.985 if is_limit_up else cfg.s7_pre_close_low_mult
    if low_px < pre_close * pre_close_mult:
        return False, "盘中出现过深回踩"

    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    # 涨跌停宽容
    vr_low = 0.0 if is_limit_up else cfg.s7_vol_ratio_low
    vr_high = 99.0 if is_limit_up else cfg.s7_vol_ratio_high
    if not (vr_low <= vr <= vr_high):
        return False, f"量比不在 {vr_low}~{vr_high}"

    turnover_f = effective_turnover_rate_f(rt, y, now_px if now_px > 0 else pre_close)
    # 涨跌停宽容
    tf_fly = cfg.s7_turnover_fly * 1.5 if is_limit_up else cfg.s7_turnover_fly
    if turnover_f > tf_fly:
        return False, f"防飞刀: 真实换手 {turnover_f:.2f}% > {tf_fly:.2f}%"

    return True, "OK"


# =============================================================================
# 策略九: 质量趋势底仓
# =============================================================================
def _strategy_quality_trend_bottom(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    prev: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
) -> Tuple[bool, str]:
    """
    策略九: 质量趋势底仓(从 P4-05 迁入 P3).

    条件:
        ma60 存在且现价站稳
        cost_50th 存在且现价站稳
        ma60 不在快速下行
        PE 在底仓区间
        真实换手处于低位
        winner_rate 在下限之上
        昨日主力净额须为正
        涨幅在底仓区间
    """
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if ma60 <= 0:
        return False, "ma60 缺失"
    if now_px <= ma60:
        return False, "未站稳 ma60"

    c50 = _field_rt_or_bar(rt, y, "cost_50th")
    if c50 <= 0:
        return False, "cost_50th 缺失"
    if now_px <= c50:
        return False, "未站稳 cost_50th"

    ma60_prev = _safe_float(prev.get("ma60"), 0.0)
    if ma60_prev > 0 and ma60 < ma60_prev * cfg.s9_ma60_downtrend_rel:
        return False, "ma60 下行过快"

    pe = _safe_float(y.get("pe_ttm"), 999.0)
    if pe >= cfg.s9_pe_max:
        return False, "PE 未达底仓区间"

    turnover_f = float(effective_turnover_rate_f(rt, y, now_px))
    if turnover_f >= cfg.s9_turnover_f_max_lt:
        return False, "真实换手过高"

    wr = _field_rt_or_bar(rt, y, "winner_rate")
    if wr < cfg.s9_winner_min:
        return False, "winner_rate 不足"

    # 昨日主力净额须为正(质量底仓要求资金面健康)
    nm_y = _safe_float(y.get("net_main_amount"), 0.0)
    if nm_y <= 0:
        return False, "昨日主力净额非正"

    if not (cfg.s9_pct_low <= pct <= cfg.s9_pct_high):
        return False, "涨幅不在底仓区间"

    return True, "OK"


# =============================================================================
# 策略十: 缩量分歧低吸
# =============================================================================
def _strategy_shrink_divergence_dip(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: P3IntradayScreenerConfig,
) -> Tuple[bool, str]:
    """
    策略十: 缩量分歧低吸(辅助 mask).

    仅当 rt['_market_contraction_score'] >= 0.7 时参与判定;
    不修改原七大策略公式，仅追加命中项与后续引擎中的低权重加分/仓位提示.
    """
    _ = cfg
    mcs = _safe_float(rt.get("_market_contraction_score", 0.0), 0.0)
    if mcs < 0.7:
        return False, "缩量分歧低吸未激活(mcs<0.7)"

    if df is None or len(df) < 30:
        return False, "历史不足(换手分位需窗口)"

    y = df.iloc[-1]
    now_px = _safe_float(rt.get("price"), 0.0)
    if now_px <= 0:
        now_px = _safe_float(y.get("close"), 0.0)
    if now_px <= 0:
        return False, "无效现价"

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    if min(ma5, ma20) <= 0:
        return False, "均线缺失"

    # 贴近攻击线与生命线
    b5 = (now_px - ma5) / ma5 * 100.0
    b20 = (now_px - ma20) / ma20 * 100.0
    if not (-8.0 <= b20 <= 5.0):
        return False, f"bias_ma20不在[-8,5]%: {b20:.2f}"
    if abs(b5) > 8.0:
        return False, "未贴近ma5(|bias|>8%)"

    hist = df.tail(60)
    s_tr = series_effective_turnover_f_daily(hist)
    s_tr = pd.to_numeric(s_tr, errors="coerce").replace(0.0, np.nan).dropna()
    if len(s_tr) < 20:
        return False, "有效换手序列过短"
    p20 = float(s_tr.quantile(0.20))
    p60 = float(s_tr.quantile(0.60))
    if p60 <= p20 or p20 <= 0:
        return False, "换手分位退化"

    # 近8根日K(落在5~10日量级)真实换手持续处于历史偏低但非地量: >(20%分位)且<(60%分位)
    tail_n = min(10, max(5, len(df) - 1))
    recent_tr = series_effective_turnover_f_daily(df.tail(tail_n))
    recent_tr = pd.to_numeric(recent_tr, errors="coerce")
    ok_band = 0
    for v in recent_tr.values:
        fv = float(v) if pd.notna(v) else 0.0
        if fv <= 0:
            continue
        if p20 < fv < p60:
            ok_band += 1
    need = max(5, tail_n - 2)
    if ok_band < need:
        return False, f"近{tail_n}日换手未持续处于20~60%分位带({ok_band}/{tail_n})"

    wr = _field_rt_or_bar(rt, y, "winner_rate")
    wr_hist = pd.to_numeric(hist.get("winner_rate"), errors="coerce").dropna()
    if wr_hist.empty:
        return False, "winner_rate历史缺失"
    wr_med = float(wr_hist.median())
    if wr <= wr_med:
        return False, f"winner_rate未高于60日中位({wr:.1f}<={wr_med:.1f})"

    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    # 涨停时量比萎缩不构成否决
    is_limit = str(rt.get("_is_limit", "")) == "UP"
    if not is_limit and not (1.0 <= vr <= 1.8):
        return False, f"量比不在1.0~1.8: {vr:.2f}"

    # 筹码未松动: cyq 有效且较低表示相对锁定; 缺失不硬杀
    cyq = _field_rt_or_bar(rt, y, "cyq_concentration", default=999.0)
    if cyq < 900.0 and cyq >= 28.0:
        return False, f"筹码偏松动(cyq={cyq:.1f}>=28)"

    return True, "OK"


# =============================================================================
# 策略八: 倍量启动延续
# =============================================================================
def _strategy_volume_breakout_follow(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    prev: pd.Series,
    cfg: P3IntradayScreenerConfig,
    pct: float,
    now_px: float,
) -> Tuple[bool, str]:
    """
    策略八: 昨日倍量阳线启动，今日沿 5/10 多头延续(A 股量价惯性).

    条件:
        昨日涨幅在区间(涨跌停时放宽到9.5%)
        昨日 MACD 柱须为正
        昨收站稳 MA5(确保前日处于上升趋势)
        昨日主力净额为正(资金流入确认)
        昨倍量阳线 + 5/10/20 多头排列
        MA20 斜率达标
        现价在 5/10 均线上方
        今日涨幅在区间(涨跌停时放宽到9.5%)
        量比, winner_rate 在下限之上(涨跌停时放宽)
    """
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    # 昨日涨幅区间(涨跌停时放宽)
    y_pct = _safe_float(prev.get("pct_chg"), 0.0)
    pct_high_prev = 9.5 if is_limit_up else cfg.s8_prev_pct_high
    if not (cfg.s8_prev_pct_low <= y_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s8_prev_pct_low}%~{pct_high_prev}%"

    # 昨日 MACD 柱须为正(昨日要有上涨动能)
    mh = _macd_hist_series(prev)
    if mh <= 0:
        return False, "昨日 macd_hist 未为正"

    # 昨收须站稳 MA5(确保竞价前日处于上升趋势)
    y_close = _safe_float(prev.get("close"), 0.0)
    ma5_prev = _safe_float(prev.get("ma5"), 0.0)
    if ma5_prev > 0 and y_close <= ma5_prev:
        return False, "昨收未站稳 ma5"

    # 昨日主力净额须为正(资金持续流入确认)
    nm_prev = _safe_float(prev.get("net_main_amount"), 0.0)
    if nm_prev <= 0:
        return False, "昨日主力净额非正"

    if len(df) < 3:
        return False, "历史不足"

    vol_prev = _safe_float(prev.get("vol"), 0.0)
    vma5_prev = _safe_float(prev.get("vol_ma5"), 0.0)
    if vma5_prev <= 0 or vol_prev <= 0:
        return False, "缺少昨日成交量"
    if vol_prev < vma5_prev * cfg.s8_vol_vs_vma5_mult:
        return False, "昨日未达倍量(vs vol_ma5)"

    o_prev = _safe_float(prev.get("open"), 0.0)
    c_prev = _safe_float(prev.get("close"), 0.0)
    if o_prev <= 0 or c_prev < o_prev * cfg.s8_yang_body_min:
        return False, "昨日非阳线启动"

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma10 = _safe_float(y.get("ma10"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    if min(ma5, ma10, ma20) <= 0:
        return False, "均线缺失"
    if not (ma5 > ma10 > ma20):
        return False, "非多头 ma5>ma10>ma20"

    slope = _safe_float(y.get("ma20_slope_5"), 0.0)
    if slope < cfg.s8_ma20_slope_min:
        return False, "ma20_slope_5 不足"

    if not (now_px > ma5 and now_px > ma10):
        return False, "现价未沿 5/10 上方"

    # 涨幅上限放宽
    pct_high_s8 = 9.5 if is_limit_up else cfg.s8_pct_high
    if not (cfg.s8_pct_low <= pct <= pct_high_s8):
        return False, f"涨幅不在 {cfg.s8_pct_low}%~{pct_high_s8}%"

    vr = _safe_float(rt.get("vol_ratio"), _safe_float(y.get("vol_ratio"), 0.0))
    # 涨停时量比下限放宽
    vr_min_s8 = cfg.s8_vr_min * 0.6 if is_limit_up else cfg.s8_vr_min
    if vr < vr_min_s8:
        return False, f"量比 {vr:.2f} < {vr_min_s8:.2f}"

    wr = _field_rt_or_bar(rt, y, "winner_rate")
    # 涨停时 winner_rate 下限适当放宽
    wr_min_s8 = cfg.s8_winner_min * 0.95 if is_limit_up else cfg.s8_winner_min
    if wr < wr_min_s8:
        return False, f"winner_rate {wr:.1f} < {wr_min_s8:.1f}"

    return True, "OK"


# =============================================================================
# 主入口: evaluate_p3_intraday_screener
# =============================================================================
def evaluate_p3_intraday_screener(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: Optional[P3IntradayScreenerConfig] = None,
) -> Dict[str, Any]:
    """
    单票单次评估: 全局否决 + 八大客观策略; 仅使用 O(1) 尾部字段与 rt 快照.

    返回:
        veto_pass, veto_reason, strategies, strategy_checks,
        p3_core_screener_pass, detail
    """
    cfg = _resolve_p3_cfg(cfg)
    out: Dict[str, Any] = {
        "veto_pass": False,
        "veto_reason": "",
        "strategies": [],
        "strategy_checks": {},
        "p3_core_screener_pass": False,
        "detail": {},
    }

    ok, reason = p3_global_risk_veto(df, rt, cfg)
    out["veto_pass"] = ok
    out["veto_reason"] = reason
    if not ok:
        return out

    ok_l, reason_l = p3_legacy_hard_veto(df, rt, cfg)
    if not ok_l:
        out["veto_pass"] = False
        out["veto_reason"] = reason_l
        return out

    y = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else y
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("pre_close"), _safe_float(y.get("close"), 0.0)))
    now_px = _safe_float(rt.get("price"), 0.0)
    low_px = _safe_float(rt.get("low"), now_px)
    high_px = _safe_float(rt.get("high"), now_px)

    sector_strength, mainline_reason = _p3_mainline_score(rt, y)
    pct = (now_px - pre_close) / pre_close * 100.0 if pre_close > 0 else 0.0
    vr = _safe_float(rt.get("vol_ratio"), 0.0)
    fund_score, fund_reason, fund_metrics = _fund_flow_signal(rt, y, now_px)
    if sector_strength <= -1.0 and vr >= 2.0 and pct >= 3.0:
        out["veto_pass"] = False
        out["veto_reason"] = "假突破过滤: 强拉但板块不配合"
        return out
    proj = projected_vol_hand_today(rt, y)

    # 序号 P3-01~P3-10: 盘中主战法 + 稳健底仓 + 辅助观察项
    strats: List[Tuple[str, Any]] = [
        ("P3-01*右侧起爆", lambda: _strategy_right_side_ignite(df, rt, y, prev, cfg, pct, now_px, proj)),
        ("P3-02*均线回踩低吸", lambda: _strategy_ma20_pullback(df, rt, y, cfg, pct, now_px, low_px, proj)),
        ("P3-03*单峰跃迁", lambda: _strategy_single_peak_break(df, rt, y, cfg, pct, now_px)),
        ("P3-04*巨头连贯发力", lambda: _strategy_giant_continuous(df, rt, y, prev, cfg, pct, now_px)),
        ("P3-05*平台二次确认", lambda: _strategy_platform_reattack(df, rt, y, cfg, pct, now_px)),
        ("P3-06*水上金叉", lambda: _strategy_macd_golden_water(df, rt, y, prev, cfg, pct, now_px)),
        ("P3-07*资金逆势托底", lambda: _strategy_counter_trend_bid(df, rt, y, cfg, pct, now_px, low_px)),
        ("P3-08*倍量启动延续", lambda: _strategy_volume_breakout_follow(df, rt, y, prev, cfg, pct, now_px)),
        ("P3-09*质量趋势底仓", lambda: _strategy_quality_trend_bottom(df, rt, y, prev, cfg, pct, now_px)),
        ("P3-10*缩量分歧低吸", lambda: _strategy_shrink_divergence_dip(df, rt, cfg)),
    ]

    hits: List[str] = []
    checks: Dict[str, str] = {}
    for name, fn in strats:
        try:
            passed, msg = fn()
        except Exception as ex:
            logger.debug("P3 策略 %s 异常: %s", name, ex)
            passed, msg = False, f"异常:{ex}"
        checks[name] = msg
        if passed:
            hits.append(name)

    # hk_vol / net_main_amount / inst_net_buy 为日线结算数据，
    # 盘中显示"昨"标注，不作为盘中判断依据，仅供参考.
    hk_vol_warn = ""
    if rt.get("_hk_vol_positive_days", 0) >= 3:
        hk_vol_warn = "近3日北向正(昨)"
    elif rt.get("_hk_vol_positive_days", 0) >= 1:
        hk_vol_warn = "部分北向数据(昨)"
    else:
        hk_vol_warn = "北向数据(昨-滞后)"

    out["strategies"] = hits
    out["strategy_checks"] = checks
    out["p3_core_screener_pass"] = len(hits) > 0
    out["detail"] = {
        "pct_chg_live": round(pct, 3),
        "projected_vol_hand": round(proj, 2),
        "elapsed_mins": _intraday_elapsed_minutes(rt),
        "sector_strength": round(sector_strength, 3),
        "mainline_reason": mainline_reason,
        "fund_flow_score": round(fund_score, 3),
        "fund_flow_reason": fund_reason,
        "fund_flow_metrics": {k: round(v, 3) if isinstance(v, (int, float, np.floating)) else v for k, v in fund_metrics.items()},
        "hk_vol_data_note": hk_vol_warn,
    }
    return out


def screen_p3_universe(
    rows: List[Tuple[str, pd.DataFrame, Dict[str, Any]]],
    cfg: Optional[P3IntradayScreenerConfig] = None,
) -> pd.DataFrame:
    """批量增量: 仅遍历当日快照列表，不做全表计算."""
    cfg = _resolve_p3_cfg(cfg)
    rec: List[Dict[str, Any]] = []
    for ts_code, df, rt in rows:
        r = evaluate_p3_intraday_screener(df, rt, cfg)
        rec.append(
            {
                "ts_code": ts_code,
                "veto_pass": r["veto_pass"],
                "veto_reason": r["veto_reason"],
                "strategies": "|".join(r["strategies"]),
                "p3_core_screener_pass": r["p3_core_screener_pass"],
            }
        )
    return pd.DataFrame(rec)
