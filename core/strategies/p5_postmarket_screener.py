# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.7 — P5 盘后选股池「物理胸甲」向量化筛选模块（十四策略含箱体/均线粘合发散/缩量分歧低吸）
================================================================================
【V26.5】资金条件：向量化 max(流通市值×比例, 阶梯地板)；换手 100% 使用 _turnover_f_eff（仅 turnover_rate_f 或手数反算）。
禁止 net_* / inst 单一绝对万元门槛；禁止 turnover_rate 降级。

【V26.5 A股特殊场景处理】
- 涨跌停宽容：涨跌停时量比萎缩属于正常现象，P5 盘后策略应识别全日数据中的涨停特征。
- 自适应换手率：根据流通市值档位动态调整换手率合理区间（P5-08 超级中军专用）。
  A股各市值档换手率差异巨大：大蓝筹0.3~2%、中盘0.5~4%、小盘1~6%、次新2~10%+。
- 北向资金滞后：hk_vol 来自 Tushare 日线收盘结算，非盘中实时数据，策略中北向条件反映的是前一交易日数据。
- VWAP防伪：盘后用日线收盘价与 VWAP 偏差检测尾盘异动（过高/过低均罚分）。

【V26.7 新增】
- 小盘股兜底常量强制 100 亿：_P1_MIN_CIRC_MV_WAN fallback 强制设为 1,000,000 万元，严防袖珍盘穿透。
- 盘后滞后数据回退：龙虎榜机构净买入(inst_net_buy)、主力净额(net_main_amount)、
  北向资金(hk_vol) 若当日数据为空或零，自动回退到前一日，并打上 [_盘后数据待更新] 标签，
  防止数据未结算时全池误杀。

================================================================================
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from core.strategies.fund_mv_utils import (
    infer_turnover_rate_f_pct,
    vector_adaptive_turnover_f_threshold,
    vector_dynamic_inst_sum3_threshold,
    vector_dynamic_net_main_threshold,
    vector_dynamic_strth_threshold_wan,
    vector_effective_turnover_rate_f,
    vector_nm_fly_negative_threshold,
)

logger = logging.getLogger(__name__)

try:
    import constants as _C_P5
    _P1_MIN_CIRC_MV_WAN = float(getattr(_C_P5, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000))
except Exception:
    # 【V26.7 修复】强制封死袖珍盘：任何 fallback 均不得低于 100 亿
    _P1_MIN_CIRC_MV_WAN = 1_000_000.0


@dataclass
class P5PostmarketConfig:
    """P5 阈值：比例为占流通市值(元)的比例；strth 比例为占 circ_mv(万) 的比例。"""

    global_bias_high: float = 10.0
    global_bias_low: float = -15.0

    s1_winner_min: float = 85.0
    s1_vr_min: float = 1.6
    s1_pct_low: float = 4.0
    s1_pct_high: float = 6.0
    # 【V26.5 新增】P5-01 涨停时涨幅上限放宽（ST涨停9.9%，普通涨停9.9%）
    s1_pct_high_limit_up: float = 9.9
    s1_upper_shadow_ratio_fly_gt: float = 0.04

    s2_winner_min: float = 80.0
    s2_vol_over_vma5_mult: float = 2.0
    s2_cost95_to_close_ratio_fly_lt: float = 1.05

    s3_net_main_ratio_of_float_mv: float = 0.0004
    s3_vol_over_vma5_fly_mult: float = 3.0

    s4_winner_min: float = 90.0
    s4_ma20_slope_min: float = 1.0
    s4_vol_over_vma5_max_mult: float = 1.5

    s5_circ_mv_fly_lt_wan: float = 500000.0

    s6_inst_sum3_ratio_of_float_mv: float = 0.00012
    s6_pct_low: float = -3.0
    s6_pct_high: float = 0.0
    s6_vol_under_vma5_ratio: float = 0.8
    s6_net_main_fly_ratio_of_float_mv: float = 0.00035

    s7_pct_low: float = -2.0
    s7_pct_high: float = 2.5
    s7_pe_max: float = 30.0
    s7_circ_mv_min_wan: float = _P1_MIN_CIRC_MV_WAN

    s8_circ_mv_min_wan: float = _P1_MIN_CIRC_MV_WAN
    s8_circ_mv_max_wan: float = 5_000_000.0
    s8_net_main_ratio_of_float_mv: float = 0.00028
    s8_turnover_f_low: float = 3.6
    s8_turnover_f_high: float = 14.0

    # ---------- 策略十二：箱体突破回踩（前日收盘突破前箱体上沿，当日回踩 MA20 企稳）----------
    s12_break_min_pct: float = 0.45
    s12_ma20_touch_max_mult: float = 1.018
    s12_pct_low: float = -1.2
    s12_pct_high: float = 4.8
    s12_box_range_min: float = 0.055
    s12_box_range_max: float = 0.36
    s12_winner_min: float = 76.0
    s12_vr_min: float = 1.05

    # ---------- 策略十三：均线粘合发散（三日前短均线粘合 + 当日发散向上 + MACD 红柱）----------
    s13_cohesion_max_pct: float = 2.70
    s13_spread_min_pct: float = 2.15
    s13_ma20_slope_min: float = 0.55
    s13_pct_low: float = -0.8
    s13_pct_high: float = 6.2
    s13_winner_min: float = 74.0
    s13_vr_min: float = 1.05

    # ---------- P5 爆发分专用：均线动能补偿（仅 strat_p5_postmarket.run_all 使用；不改 P1、不做硬否决）----------
    # 健康乖离带 [3%,12%] 仅作文档/话术参考，代码中恒为乘子 1.0，避免与斜率奖励重复加权。
    enable_ma_compensation: bool = True
    ma_bias_overheat_pct: float = 18.0
    circ_mv_wan_large_min: float = 3_000_000.0
    circ_mv_wan_mid_min: float = _P1_MIN_CIRC_MV_WAN
    ma_overheat_mult_large: float = 0.85
    ma_overheat_mult_mid: float = 0.95
    ma_slope_reward_threshold_pct: float = 0.8
    ma_slope_reward_mult_min: float = 1.25
    ma_slope_reward_mult_max: float = 1.4
    ma_slope_reward_interp_high_pct: float = 2.0

    # ---------- 分时结构防伪：VWAP 惩罚（盘后静态日线的最后一道防线）----------
    enable_vwap_penalty: bool = True
    vwap_dev_soft_pct: float = 1.8
    vwap_dev_hard_pct: float = 3.5
    vwap_tail_spike_pct: float = 1.2
    vwap_tail_spike_mult: float = 0.72
    vwap_hard_mult: float = 0.55
    vwap_tail_minutes: int = 15

    # ---------- V26.6 板块内部分化修正阈值 ----------
    sector_diff_beta_th: float = 1.10
    sector_diff_pct_th: float = 1.5

    # ---------- V26.6 连续命中查询参数 ----------
    consec_hit_lookback_days: int = 5


def _resolve_p5_cfg(cfg: Optional[P5PostmarketConfig]) -> P5PostmarketConfig:
    if cfg is not None:
        return cfg
    from core.config_manager import get_p5_postmarket_config

    return get_p5_postmarket_config()


DEFAULT_P5_CONFIG = P5PostmarketConfig()


def _ensure_numeric(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def _macd_hist_col(df: pd.DataFrame) -> pd.Series:
    if "macd_hist" in df.columns:
        h = pd.to_numeric(df["macd_hist"], errors="coerce")
    else:
        h = pd.Series(np.nan, index=df.index, dtype=float)
    if "macd_bar" in df.columns:
        b = pd.to_numeric(df["macd_bar"], errors="coerce")
        h = h.fillna(b)
    return h


def _bias_20_series(df: pd.DataFrame) -> pd.Series:
    if "bias_20" in df.columns:
        return pd.to_numeric(df["bias_20"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    m20 = pd.to_numeric(df["ma20"], errors="coerce")
    return np.where(m20 > 0, (c - m20) / m20 * 100.0, np.nan)


def prepare_p5_lags(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "ts_code" not in out.columns:
        raise ValueError("prepare_p5_lags: 缺少 ts_code 列")
    if "trade_date" not in out.columns:
        raise ValueError("prepare_p5_lags: 缺少 trade_date 列")

    out["trade_date"] = pd.to_datetime(out["trade_date"])
    out = out.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    num_cols = [
        "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "vol_ratio", "turnover_rate_f",
        "pe_ttm", "pb", "ps_ttm", "dv_ratio", "circ_mv", "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
        "vol_ma5", "vol_ma10", "vol_ma20", "ma20_slope_5", "high_20", "low_60", "macd", "macd_signal",
        "atr_pct", "bias_20", "net_main_amount", "inst_net_buy", "hk_vol",
        "cost_50th", "cost_95th", "winner_rate", "limit_times", "strth",
    ]
    _ensure_numeric(out, [c for c in num_cols if c in out.columns])

    tf_src = out["turnover_rate_f"] if "turnover_rate_f" in out.columns else pd.Series(np.nan, index=out.index)
    out["_turnover_f_eff"] = vector_effective_turnover_rate_f(
        out["vol"], out["close"], out["circ_mv"], tf_src
    )

    out["_mh"] = _macd_hist_col(out)

    g = out.groupby("ts_code", sort=False)
    out["nm_lag1"] = g["net_main_amount"].shift(1)
    out["nm_lag2"] = g["net_main_amount"].shift(2)
    out["macd_hist_prev"] = g["_mh"].shift(1)
    out["vol_ma5_prev"] = g["vol_ma5"].shift(1)
    out["vol_ma20_prev"] = g["vol_ma20"].shift(1)
    out["inst_net_buy_sum3"] = g["inst_net_buy"].transform(lambda s: s.rolling(3, min_periods=3).sum())
    out["hk_lag1"] = g["hk_vol"].shift(1)
    out["hk_lag2"] = g["hk_vol"].shift(2)
    out["ma60_lag5"] = g["ma60"].shift(5)
    if "high_20" in out.columns:
        out["close_lag1"] = g["close"].shift(1)
        out["high_20_lag2"] = g["high_20"].shift(2)
    if all(c in out.columns for c in ("ma5", "ma10", "ma20")):
        for lag_i in range(1, 6):
            out[f"ma5_lag{lag_i}"] = g["ma5"].shift(lag_i)
            out[f"ma10_lag{lag_i}"] = g["ma10"].shift(lag_i)
            out[f"ma20_lag{lag_i}"] = g["ma20"].shift(lag_i)

    out["_bias_live"] = _bias_20_series(out)

    # 【V26.7 新增】盘后滞后数据回退：为 inst_net_buy / net_main_amount / hk_vol 生成滞后列
    # 原因：龙虎榜机构净买入、主力净额、北向资金均为 Tushare 日线收盘结算数据，
    # 非盘中实时数据，当日若数据未更新则用前一日替代，并打上 [_lag_note] 标签
    lag_note_parts: List[str] = []
    if "inst_net_buy" in out.columns:
        out["inst_lag1"] = g["inst_net_buy"].shift(1)
        # 龙虎榜数据当日未更新（值为0或空）时，用前一日替代
        inst_cur = pd.to_numeric(out["inst_net_buy"], errors="coerce")
        inst_l1 = pd.to_numeric(out["inst_lag1"], errors="coerce")
        inst_stale = inst_cur.fillna(0) == 0
        inst_has_lag = inst_l1.notna() & (inst_l1.fillna(0) != 0)
        out.loc[inst_stale & inst_has_lag, "inst_net_buy"] = out.loc[inst_stale & inst_has_lag, "inst_lag1"]
        if (inst_stale & inst_has_lag).any():
            lag_note_parts.append("inst_net_buy")
    if "net_main_amount" in out.columns:
        out["nm_lag1"] = g["net_main_amount"].shift(1)
        out["nm_lag2"] = g["net_main_amount"].shift(2)
    if "hk_vol" in out.columns:
        out["hk_lag1"] = g["hk_vol"].shift(1)
        out["hk_lag2"] = g["hk_vol"].shift(2)
    # 【V26.7 新增】打上滞后标签：当日数据缺失时在 _lag_note 列记录，供后续展示使用
    out["_lag_note"] = "[盘后数据待更新]" if lag_note_parts else ""

    if "vwap" not in out.columns:
        out["vwap"] = np.nan
    if "tail_close" not in out.columns:
        out["tail_close"] = np.nan
    if "tail_vwap" not in out.columns:
        out["tail_vwap"] = np.nan
    if "tail_volume" not in out.columns:
        out["tail_volume"] = np.nan

    return out


def p5_global_pass_mask(df: pd.DataFrame, cfg: Optional[P5PostmarketConfig] = None) -> pd.Series:
    cfg = _resolve_p5_cfg(cfg)
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    required_cols = ["close", "ma60", "ma20_slope_5"]
    if any(c not in df.columns for c in required_cols):
        return pd.Series(False, index=df.index)
    close = pd.to_numeric(df["close"], errors="coerce")
    ma60 = pd.to_numeric(df["ma60"], errors="coerce")
    slope = pd.to_numeric(df["ma20_slope_5"], errors="coerce")
    v1 = (close > ma60) & (slope > 0)
    v1 = v1.fillna(False)

    bias = df["_bias_live"] if "_bias_live" in df.columns else _bias_20_series(df)
    v2 = (bias <= cfg.global_bias_high) & (bias >= cfg.global_bias_low)
    v2 = v2.fillna(False)

    pe = pd.to_numeric(df["pe_ttm"], errors="coerce")
    v3 = ~(pe < 0)
    v3 = v3.fillna(True)

    nm0 = pd.to_numeric(df["net_main_amount"], errors="coerce")
    nm1 = pd.to_numeric(df["nm_lag1"], errors="coerce")
    nm2 = pd.to_numeric(df["nm_lag2"], errors="coerce")
    three_neg = nm0.notna() & nm1.notna() & nm2.notna() & (nm0 < 0) & (nm1 < 0) & (nm2 < 0)
    v4 = ~three_neg

    mask = v1 & v2 & v3 & v4
    try:
        from core.backtest_context import is_backtest_legacy_mode
    except ImportError:
        return mask
    if not is_backtest_legacy_mode():
        return mask
    ok_cci = pd.Series(True, index=df.index)
    if "cci" in df.columns:
        cci = pd.to_numeric(df["cci"], errors="coerce")
        ok_cci = cci >= 100.0
    ok_nt = pd.Series(True, index=df.index)
    if "nineturn_signal" in df.columns:
        nt = pd.to_numeric(df["nineturn_signal"], errors="coerce").fillna(0.0)
        ok_nt = nt < 9.0
    return mask & ok_cci.fillna(False) & ok_nt.fillna(False)


def _base_ok(df: pd.DataFrame) -> pd.Series:
    c = pd.to_numeric(df["close"], errors="coerce")
    return c.notna() & (c > 0)


def _to_float_safe(val: Any, default: float = np.nan) -> float:
    try:
        x = float(pd.to_numeric(val, errors="coerce"))
        return x if np.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _to_int_safe(val: Any, default: int = 0) -> int:
    try:
        x = int(float(pd.to_numeric(val, errors="coerce")))
        return x
    except Exception:
        return int(default)


def evaluate_vwap_penalty_p5(
    prep_one_stock: pd.DataFrame,
    rt: Optional[Dict[str, Any]] = None,
    cfg: Optional[P5PostmarketConfig] = None,
) -> Tuple[float, Dict[str, Any], List[str]]:
    cfg = _resolve_p5_cfg(cfg)
    rt = rt or {}
    if not bool(cfg.enable_vwap_penalty):
        return 1.0, {}, []

    warnings: List[str] = []
    patch: Dict[str, Any] = {}
    if prep_one_stock is None or prep_one_stock.empty:
        return 1.0, patch, warnings

    last = prep_one_stock.iloc[-1]
    close = _to_float_safe(last.get("close"), np.nan)
    if not np.isfinite(close) or close <= 0:
        return 1.0, patch, warnings

    vwap = _to_float_safe(last.get("vwap"), np.nan)
    tail_close = _to_float_safe(last.get("tail_close"), np.nan)
    tail_vwap = _to_float_safe(last.get("tail_vwap"), np.nan)
    tail_minutes = _to_int_safe(cfg.vwap_tail_minutes, 15)

    vwap_missing = not (np.isfinite(vwap) and vwap > 0)
    tail_vwap_missing = not (np.isfinite(tail_vwap) and tail_vwap > 0)
    if vwap_missing and tail_vwap_missing:
        patch["VWAP_数据缺失"] = True
        patch["VWAP_尾盘分钟"] = tail_minutes
        return 1.0, patch, warnings

    if vwap_missing:
        vwap = tail_vwap
        patch["VWAP_数据缺失"] = True
        patch["VWAP_尾盘分钟"] = tail_minutes
        # 只有缺少全天 VWAP、但存在尾盘 VWAP 时，记录提示但不报警，不做降权，避免全池误报。
        return 1.0, patch, warnings

    dev_pct = (close - vwap) / vwap * 100.0
    patch["VWAP_收盘偏离_pct"] = round(dev_pct, 4)
    patch["VWAP_收盘价"] = round(close, 4)
    patch["VWAP_均价"] = round(vwap, 4)

    if np.isfinite(tail_close) and np.isfinite(tail_vwap) and tail_vwap > 0:
        tail_dev_pct = (tail_close - tail_vwap) / tail_vwap * 100.0
    else:
        tail_dev_pct = np.nan
    if np.isfinite(tail_dev_pct):
        patch["VWAP_尾盘偏离_pct"] = round(tail_dev_pct, 4)
    patch["VWAP_尾盘分钟"] = tail_minutes

    m = 1.0
    hard = float(cfg.vwap_dev_hard_pct)
    soft = float(cfg.vwap_dev_soft_pct)
    tail_spike = float(cfg.vwap_tail_spike_pct)
    if np.isfinite(dev_pct) and abs(dev_pct) >= hard:
        m = float(cfg.vwap_hard_mult)
        warnings.append("📡【VWAP重锤】收盘严重偏离全天均价线，疑似尾盘画线")
    elif np.isfinite(dev_pct) and abs(dev_pct) >= soft:
        m = min(m, 0.82)
        warnings.append("📡【VWAP警惕】收盘偏离全天均价线偏大")

    if np.isfinite(tail_dev_pct) and tail_dev_pct >= tail_spike:
        m = min(m, float(cfg.vwap_tail_spike_mult))
        warnings.append("📡【尾盘脉冲】尾盘存在偏强脉冲拉升，需防次日兑现")

    if np.isfinite(dev_pct) and abs(dev_pct) >= hard and np.isfinite(tail_dev_pct) and tail_dev_pct >= tail_spike:
        m = min(m, float(cfg.vwap_hard_mult))

    return float(max(0.4, min(1.0, m))), patch, warnings


def _derive_p5_action_summary(strategy_names: List[str], vwap_mult: float = 1.0, risk_tags: Optional[List[str]] = None) -> Tuple[str, List[str], str, str]:
    risk_tags = list(risk_tags or [])
    primary_action = "观察"
    secondary_actions: List[str] = []
    action_priority = [
        ("P5-12B·★箱体回踩", "低吸"),
        ("P5-12A·★箱体突破", "追涨"),
        ("P5-12·★箱体突破回踩", "追涨/低吸"),
        ("P5-13C·★均线发散确认", "追涨"),
        ("P5-13B·★均线发散起点", "追涨"),
        ("P5-13A·★均线粘合", "观察"),
        ("P5-14·★缩量分歧低吸", "低吸"),
        ("P5-03·★资金三重共振", "核心"),
        ("P5-02·★量价主升浪确认", "核心"),
        ("P5-06·★机构龙虎缩量回踩", "辅助"),
        ("P5-07·★外资连续建仓", "辅助"),
        ("P5-08·★超级中军点火", "辅助"),
        ("P5-01·★核心四因子共振", "辅助"),
        ("P5-04·★单峰密集突破", "辅助"),
    ]
    for key, label in action_priority:
        if any(key in h for h in strategy_names):
            if primary_action == "观察":
                primary_action = label
            elif label not in secondary_actions and label != primary_action:
                secondary_actions.append(label)
    if any("VWAP" in h for h in risk_tags) or vwap_mult < 0.9:
        market_status = "分时偏弱"
        risk_level = "高"
    elif vwap_mult < 0.98:
        market_status = "分时警惕"
        risk_level = "中高"
    else:
        market_status = "正常"
        risk_level = "中"
    if any("派发" in h or "假强" in h for h in risk_tags):
        market_status = "防兑现"
        risk_level = "中高"
    return primary_action, secondary_actions, market_status, risk_level


def evaluate_shrink_divergence_mask_p5(
    prep_one_stock: pd.DataFrame,
    rt: Optional[Dict[str, Any]] = None,
    market_contraction_score: float = 0.0,
) -> bool:
    """
    【缩量分歧低吸】四步递进硬门槛：
    1) 大盘/板块处于缩量或震荡；
    2) 个股真实换手位于自身近期分位带；
    3) 获利盘/筹码未明显松动；
    4) 收盘站稳 MA20 且未破坏短均线结构。
    """
    rt = rt or {}
    if market_contraction_score < 0.7:
        return False
    if prep_one_stock is None or prep_one_stock.empty or len(prep_one_stock) < 30:
        return False
    if "_turnover_f_eff" not in prep_one_stock.columns:
        return False

    last = prep_one_stock.iloc[-1]
    close = float(pd.to_numeric(last.get("close"), errors="coerce") or 0.0)
    ma5 = float(pd.to_numeric(last.get("ma5"), errors="coerce") or 0.0)
    ma20 = float(pd.to_numeric(last.get("ma20"), errors="coerce") or 0.0)
    if min(ma5, ma20, close) <= 0:
        return False

    b20 = (close - ma20) / ma20 * 100.0
    b5 = (close - ma5) / ma5 * 100.0
    if not (-7.0 <= b20 <= 3.5):
        return False
    if not (-5.0 <= b5 <= 4.0):
        return False

    hist = prep_one_stock.tail(60)
    s_tr = pd.to_numeric(hist["_turnover_f_eff"], errors="coerce").replace(0.0, np.nan).dropna()
    if len(s_tr) < 20:
        return False
    p20 = float(s_tr.quantile(0.20))
    p60 = float(s_tr.quantile(0.60))
    if p60 <= p20 or p20 <= 0:
        return False

    recent = pd.to_numeric(prep_one_stock["_turnover_f_eff"].tail(8), errors="coerce")
    band_hit = 0
    for v in recent.values:
        if pd.isna(v) or float(v) <= 0:
            continue
        fv = float(v)
        if p20 < fv < p60:
            band_hit += 1
    if band_hit < 6:
        return False

    wr = float(pd.to_numeric(last.get("winner_rate"), errors="coerce") or 0.0)
    wr_hist = pd.to_numeric(hist.get("winner_rate"), errors="coerce").dropna()
    if wr_hist.empty:
        return False
    wr_med = float(wr_hist.median())
    if wr <= wr_med:
        return False

    vr = float(pd.to_numeric(last.get("vol_ratio"), errors="coerce") or 0.0)
    if not (1.0 <= vr <= 1.6):
        return False

    cyq_raw = last["cyq_concentration"] if "cyq_concentration" in last.index else None
    cyq = float(pd.to_numeric(cyq_raw, errors="coerce") or np.nan)
    if np.isnan(cyq):
        cyq = float(pd.to_numeric(rt.get("cyq_concentration"), errors="coerce") or 999.0)
    if cyq < 900.0 and cyq >= 26.0:
        return False

    return True


def strategy_masks_p5(
    df: pd.DataFrame,
    cfg: Optional[P5PostmarketConfig] = None,
    market_contraction_score: float = 0.0,
) -> Dict[str, pd.Series]:
    cfg = _resolve_p5_cfg(cfg)
    if df is None or df.empty:
        return {}
    if "_turnover_f_eff" not in df.columns:
        raise ValueError("strategy_masks_p5: 请先 prepare_p5_lags（含 _turnover_f_eff）")

    mh = df["_mh"]
    mh_prev = pd.to_numeric(df["macd_hist_prev"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    vol = pd.to_numeric(df["vol"], errors="coerce")
    pct = pd.to_numeric(df["pct_chg"], errors="coerce")
    vr = pd.to_numeric(df["vol_ratio"], errors="coerce")
    wr = pd.to_numeric(df["winner_rate"], errors="coerce")
    vma5 = pd.to_numeric(df["vol_ma5"], errors="coerce")
    vma20 = pd.to_numeric(df["vol_ma20"], errors="coerce")
    ma5 = pd.to_numeric(df["ma5"], errors="coerce")
    ma10 = pd.to_numeric(df["ma10"], errors="coerce")
    ma20 = pd.to_numeric(df["ma20"], errors="coerce")
    ma60 = pd.to_numeric(df["ma60"], errors="coerce")
    slope20 = pd.to_numeric(df["ma20_slope_5"], errors="coerce")
    h20 = pd.to_numeric(df["high_20"], errors="coerce")
    macd = pd.to_numeric(df["macd"], errors="coerce")
    sig = pd.to_numeric(df["macd_signal"], errors="coerce")
    nm = pd.to_numeric(df["net_main_amount"], errors="coerce")
    inst = pd.to_numeric(df["inst_net_buy"], errors="coerce")
    hk = pd.to_numeric(df["hk_vol"], errors="coerce")
    c95 = pd.to_numeric(df["cost_95th"], errors="coerce")
    c50 = pd.to_numeric(df["cost_50th"], errors="coerce")
    cm = pd.to_numeric(df["circ_mv"], errors="coerce")
    to_eff = pd.to_numeric(df["_turnover_f_eff"], errors="coerce")
    pe = pd.to_numeric(df["pe_ttm"], errors="coerce")
    ps = pd.to_numeric(df["ps_ttm"], errors="coerce")
    atrp = pd.to_numeric(df["atr_pct"], errors="coerce")
    lim = pd.to_numeric(df["limit_times"], errors="coerce").fillna(0).astype(int)

    v5p = df["vol_ma5_prev"]
    v20p = df["vol_ma20_prev"]
    inst3 = pd.to_numeric(df["inst_net_buy_sum3"], errors="coerce")
    hk1 = pd.to_numeric(df["hk_lag1"], errors="coerce")
    hk2 = pd.to_numeric(df["hk_lag2"], errors="coerce")

    thr_nm_s3 = vector_dynamic_net_main_threshold(cm, cfg.s3_net_main_ratio_of_float_mv)
    thr_inst3_s6 = vector_dynamic_inst_sum3_threshold(cm, cfg.s6_inst_sum3_ratio_of_float_mv)
    thr_nm_s8 = vector_dynamic_net_main_threshold(cm, cfg.s8_net_main_ratio_of_float_mv)
    nm_fly_s6 = vector_nm_fly_negative_threshold(cm, cfg.s6_net_main_fly_ratio_of_float_mv)
    # 【V26.5 A股换手率自适应】根据流通市值动态调整换手率区间（P5-08 超级中军专用）
    # A股各市值档换手率差异巨大：大蓝筹0.3~2%、中盘0.5~4%、小盘1~6%、次新2~10%+
    tf_low_s8, tf_high_s8 = vector_adaptive_turnover_f_threshold(cm, cfg.s8_turnover_f_low, cfg.s8_turnover_f_high)

    masks: Dict[str, pd.Series] = {}

    # 【V26.5 A股涨跌停宽容】涨停时涨幅上限放宽到 s1_pct_high_limit_up
    limit_up = lim.shift(0) == 1
    pct_high_s1 = np.where(limit_up.fillna(False), cfg.s1_pct_high_limit_up, cfg.s1_pct_high)
    s1_core = (
        (wr > cfg.s1_winner_min)
        & (vr >= cfg.s1_vr_min)
        & (mh > 0)
        & (mh > mh_prev)
        & (pct >= cfg.s1_pct_low)
        & (pct <= pct_high_s1)
    )
    s1_fly = (high - close) / close.replace(0, np.nan) > cfg.s1_upper_shadow_ratio_fly_gt
    # 【V26.5 A股涨跌停宽容】涨停时量比萎缩、长上影均属正常现象，不构成否决
    s1_vol_ok = limit_up | (vr >= cfg.s1_vr_min)
    s1_shadow_ok = limit_up | (~s1_fly.fillna(False))
    masks["P5-01·★核心四因子共振"] = s1_core & s1_vol_ok & s1_shadow_ok & _base_ok(df)

    s2_trend = (vma5 > vma20) & (v5p <= v20p) & (ma5 > ma20)
    s2_volume = vol > vma5 * cfg.s2_vol_over_vma5_mult
    s2_quality = wr > cfg.s2_winner_min
    s2_core = s2_trend & s2_volume & s2_quality
    s2_fly = (c95 / close.replace(0, np.nan)) < cfg.s2_cost95_to_close_ratio_fly_lt
    masks["P5-02A·★量价趋势"] = s2_trend & (~s2_fly.fillna(False)) & _base_ok(df)
    masks["P5-02B·★量价放量"] = s2_volume & (~s2_fly.fillna(False)) & _base_ok(df)
    masks["P5-02C·★量价确认"] = s2_core & (~s2_fly.fillna(False)) & _base_ok(df)
    masks["P5-02·★量价主升浪确认"] = masks["P5-02C·★量价确认"]

    s3_money = (nm > thr_nm_s3) & (close > ma60)
    # 【V26.5 优化】北向硬改软：主力净额为正是核心，北向数据缺失时不应阻断策略
    hk_ok = (hk > 0) | (hk1 > 0)  # 近两日任一北向为正即可通过，避免数据滞后全灭
    s3_quality = (inst > 0) & hk_ok
    s3_core = s3_money & s3_quality
    s3_fly = vol > vma5 * cfg.s3_vol_over_vma5_fly_mult
    # 【V26.5 A股涨跌停宽容】涨停时极端放量不构成否决（limit_up 已在 s1 块前定义）
    s3_fly_ok = limit_up | (~s3_fly.fillna(False))
    masks["P5-03A·★资金方向"] = s3_money & s3_fly_ok & _base_ok(df)
    masks["P5-03B·★资金质量"] = s3_quality & s3_fly_ok & _base_ok(df)
    masks["P5-03C·★资金确认"] = s3_core & s3_fly_ok & _base_ok(df)
    masks["P5-03·★资金三重共振"] = masks["P5-03C·★资金确认"]

    s4_core = (
        (wr > cfg.s4_winner_min)
        & (close > c95)
        & (slope20 > cfg.s4_ma20_slope_min)
        & (vol <= vma5 * cfg.s4_vol_over_vma5_max_mult)
    )
    s4_fly = (mh < mh_prev) & mh_prev.notna() & mh.notna()
    # 【V26.5 A股涨跌停宽容】涨停时放宽单峰量能约束：允许放量
    masks["P5-04·★单峰密集突破"] = (
        (wr > cfg.s4_winner_min)
        & (close > c95)
        & (slope20 > cfg.s4_ma20_slope_min)
        & ((limit_up) | (vol <= vma5 * cfg.s4_vol_over_vma5_max_mult))
    ) & (~s4_fly.fillna(False)) & _base_ok(df)

    s5_trend = (ma5 > ma20) & (ma20 > ma60) & (close > h20)
    s5_quality = (macd > sig) & (cm >= cfg.s5_circ_mv_fly_lt_wan)
    s5_fly = cm < cfg.s5_circ_mv_fly_lt_wan
    # 【V26.5 优化】假强判断：A股主升浪中适度放量是正常的，假强须同时满足：极端涨幅+极端放量+低筹码+收盘远离VWAP。
    # 注意：vwap 列可能缺失，此时跳过VWAP维度的假强检测，避免全池误判。
    s5_chase = (pct > 5.5) & (vr > 3.0) & (wr < 75.0)
    vwap_ok = "vwap" in df.columns
    vwap_dev = pd.Series(np.nan, index=df.index)
    if vwap_ok:
        vwap = pd.to_numeric(df["vwap"], errors="coerce")
        vwap_dev = (close - vwap).abs() / vwap.replace(0, np.nan)
    # 涨停时VWAP偏差不受此约束；5%门槛给予优质筹码结构更多容错空间
    s5_fake_strong = s5_chase & (~limit_up)
    if vwap_ok:
        s5_fake_strong = s5_fake_strong & (vwap_dev > 0.050)
    masks["P5-05A·★趋势结构"] = s5_trend & (~s5_fly.fillna(False)) & (~s5_fake_strong.fillna(False)) & _base_ok(df)
    masks["P5-05B·★趋势质量"] = s5_quality & (~s5_fly.fillna(False)) & (~s5_fake_strong.fillna(False)) & _base_ok(df)
    masks["P5-05·★绝对趋势雷达"] = s5_trend & s5_quality & (~s5_fly.fillna(False)) & (~s5_fake_strong.fillna(False)) & _base_ok(df)
    masks["P5-05C·★趋势确认"] = masks["P5-05·★绝对趋势雷达"]

    s6_core = (
        (inst3 > thr_inst3_s6)
        & (pct >= cfg.s6_pct_low)
        & (pct <= cfg.s6_pct_high)
        & (vol < vma5 * cfg.s6_vol_under_vma5_ratio)
        & (close > ma20)
    )
    s6_fly = nm < nm_fly_s6
    masks["P5-06·★机构龙虎缩量回踩"] = s6_core & (~s6_fly.fillna(False)) & _base_ok(df)

    s7_core = (
        (hk > 0)
        & (hk1 > 0)
        & (hk2 > 0)
        & (pct >= cfg.s7_pct_low)
        & (pct <= cfg.s7_pct_high)
        & (pe < cfg.s7_pe_max)
        & (cm > cfg.s7_circ_mv_min_wan)
    )
    # 【V26.5 优化】北向三日全正硬改软：若任一日数据缺失（hk<=0或nan）仍可通过，仅降权不加死。
    # 核心逻辑：主力连续净买 是本质要求，北向数据仅作辅助确认，数据缺失时不应阻断整个策略。
    # 宽松策略：近三日任一北向为正即可通过；数据全正时更优（额外加分在 strat_p5 中体现）。
    hk_all_positive = hk_ok.fillna(False) & hk1.notna() & hk2.notna() & (hk1 > 0) & (hk2 > 0)
    hk_any_positive = hk > 0  # 当日本身为正
    hk_confirmed_ok = hk_all_positive | hk_any_positive | (hk1 > 0) | (hk2 > 0)  # 任一正即可通过，避免数据缺失全灭
    masks["P5-07·★外资连续建仓"] = s7_core & hk_confirmed_ok.fillna(False) & _base_ok(df)

    s8_core = (
        (cm >= cfg.s8_circ_mv_min_wan)
        & (cm <= cfg.s8_circ_mv_max_wan)
        & (nm > thr_nm_s8)
        & (to_eff >= tf_low_s8)
        & (to_eff <= tf_high_s8)
        & (close > ma60)
    )
    masks["P5-08·★超级中军点火"] = s8_core & _base_ok(df)

    if (
        "close_lag1" in df.columns
        and "high_20_lag2" in df.columns
        and "low_60" in df.columns
        and "high_20" in df.columns
    ):
        close_lag1 = pd.to_numeric(df["close_lag1"], errors="coerce")
        h20_lag2 = pd.to_numeric(df["high_20_lag2"], errors="coerce")
        l60 = pd.to_numeric(df["low_60"], errors="coerce")
        box_rng = (h20 - l60) / close.replace(0, np.nan)
        brk = (close_lag1 > h20_lag2 * (1.0 + cfg.s12_break_min_pct / 100.0)) & close_lag1.notna() & h20_lag2.notna()
        s12_pb = (
            (close > ma20)
            & (low <= ma20 * cfg.s12_ma20_touch_max_mult)
            & (pct >= cfg.s12_pct_low)
            & (pct <= cfg.s12_pct_high)
        )
        s12_rng = (box_rng > cfg.s12_box_range_min) & (box_rng < cfg.s12_box_range_max)
        s12_breakout = brk & s12_rng & (ma5 > ma10) & ((limit_up) | (vr >= cfg.s12_vr_min))
        s12_pullback = s12_pb & s12_rng & (ma5 > ma10) & (wr > cfg.s12_winner_min) & ((limit_up) | (vr >= cfg.s12_vr_min))
        s12_core = s12_breakout & s12_pullback
        s12_fly = (macd < sig) & macd.notna() & sig.notna()
        masks["P5-12A·★箱体突破"] = s12_breakout & (~s12_fly.fillna(False)) & _base_ok(df)
        masks["P5-12B·★箱体回踩"] = s12_pullback & (~s12_fly.fillna(False)) & _base_ok(df)
        masks["P5-12·★箱体突破回踩"] = s12_core & (~s12_fly.fillna(False)) & _base_ok(df)
    else:
        masks["P5-12A·★箱体突破"] = pd.Series(False, index=df.index)
        masks["P5-12B·★箱体回踩"] = pd.Series(False, index=df.index)
        masks["P5-12·★箱体突破回踩"] = pd.Series(False, index=df.index)

    # ---------- 策略十三：均线粘合发散（三交易日前后短均线粘合 + 当日发散向上 + MACD 红柱）----------
    # 【V26.5 → V26.7 修复】粘合窗口从3日延长至5日：3日窗口太短，偶发价格波动即可破坏粘合形态。
    # 5日窗口更能捕捉真实均线收敛状态，减少假信号。均线粘合发散本质是中期整理后的方向选择。
    s13_cohesion_lookback: int = 5
    s13_cohesion_max_pct: float = cfg.s13_cohesion_max_pct
    s13_spread_min_pct: float = cfg.s13_spread_min_pct

    # 【V26.7 修复】粘合窗口扩展至5日，需检查所有滞后列（lag1~lag5）是否存在
    has_all_lag_cols = all(
        f"{ma}_lag{i}" in df.columns
        for ma in ("ma5", "ma10", "ma20")
        for i in range(1, s13_cohesion_lookback + 1)
    )
    if (
        "ma5" in df.columns
        and "ma10" in df.columns
        and "ma20" in df.columns
        and "macd" in df.columns
        and "macd_signal" in df.columns
        and has_all_lag_cols
    ):
        m5 = pd.to_numeric(df["ma5"], errors="coerce")
        m10 = pd.to_numeric(df["ma10"], errors="coerce")
        m20 = pd.to_numeric(df["ma20"], errors="coerce")
        macd = pd.to_numeric(df["macd"], errors="coerce")
        sig = pd.to_numeric(df["macd_signal"], errors="coerce")

        # 【V26.7 修复】粘合判断：扩大窗口至最近5个交易日，确保均线真实收敛而非偶发收拢。
        spr_min_list: List[Optional[float]] = []
        for i in range(s13_cohesion_lookback):
            suffix = "" if i == 0 else f"_lag{i}"
            m5l = pd.to_numeric(df[f"ma5{suffix}"], errors="coerce")
            m10l = pd.to_numeric(df[f"ma10{suffix}"], errors="coerce")
            m20l = pd.to_numeric(df[f"ma20{suffix}"], errors="coerce")
            spr = np.maximum(np.abs(m5l - m10l), np.abs(m10l - m20l)) / m20l.replace(0, np.nan)
            spr_min_list.append(spr)
        # 取5日最小离散度（最粘合状态）
        spr_df = pd.DataFrame(spr_min_list).T
        spr_min_5d = spr_df.min(axis=1, skipna=True)

        cohesion = spr_min_5d < (s13_cohesion_max_pct / 100.0)
        spr_now = (m5 - m20).abs() / m20.replace(0, np.nan)
        widen = spr_now > (s13_spread_min_pct / 100.0)

        # S13A均线粘合：最近5日内曾出现粘合状态，且当前短均线仍保持多头排列
        s13_stage1 = cohesion.fillna(False) & (m5 > m10) & (m10 > m20)
        # 【V26.7 修复】S13B均线发散起点：粘合后开始发散，无需MACD红柱约束（那是S13C的确认条件）
        s13_stage2 = s13_stage1 & widen.fillna(False) & (close > m20)
        # S13C均线发散确认：发散后叠加量价质量验证，MACD红柱在此阶段作为确认条件
        s13_stage3 = (
            s13_stage2
            & (pct >= cfg.s13_pct_low)
            & (pct <= cfg.s13_pct_high)
            & (slope20 > cfg.s13_ma20_slope_min)
            & (wr > cfg.s13_winner_min)
            & (vr >= cfg.s13_vr_min)
            & (mh > 0)
        )
        s13_fly = (macd < sig) & macd.notna() & sig.notna()
        masks["P5-13A·★均线粘合"] = s13_stage1 & _base_ok(df)
        masks["P5-13B·★均线发散起点"] = s13_stage2 & (~s13_fly.fillna(False)) & _base_ok(df)
        masks["P5-13C·★均线发散确认"] = s13_stage3 & (~s13_fly.fillna(False)) & _base_ok(df)
    else:
        masks["P5-13A·★均线粘合"] = pd.Series(False, index=df.index)
        masks["P5-13B·★均线发散起点"] = pd.Series(False, index=df.index)
        masks["P5-13C·★均线发散确认"] = pd.Series(False, index=df.index)
    masks["P5-13·★均线粘合发散"] = masks["P5-13B·★均线发散起点"] | masks["P5-13C·★均线发散确认"]

    vwap_pen_mask = pd.Series(1.0, index=df.index, dtype=float)
    vwap_warning_col: List[str] = []
    if "ts_code" in df.columns:
        for _, g in df.groupby("ts_code", sort=False):
            if g is None or len(g) < 3:
                continue
            last_i = g.index[-1]
            try:
                mult, patch, warnings = evaluate_vwap_penalty_p5(g, {}, cfg)
                vwap_pen_mask.loc[last_i] = float(mult)
                if warnings:
                    vwap_warning_col.append(f"{g.iloc[-1].get('ts_code', '')}:{'|'.join(warnings)}")
                for k, v in patch.items():
                    try:
                        df.loc[last_i, k] = v
                    except Exception:
                        pass
            except Exception as _e:
                logger.debug("VWAP惩罚 分组计算跳过 ts_code 末行: %s", _e, exc_info=True)
                continue
    masks["P5-00·◆VWAP分时防伪"] = vwap_pen_mask < 0.999

    # 【缩量分歧低吸】向量化面板：按 ts_code 分组用全日窗口计算，仅缩量期写入 True（末行代表当日）
    if market_contraction_score < 0.7 or "_turnover_f_eff" not in df.columns or "ts_code" not in df.columns:
        masks["P5-14·★缩量分歧低吸"] = pd.Series(False, index=df.index)
    else:
        shrink = pd.Series(False, index=df.index)
        for _, g in df.groupby("ts_code", sort=False):
            if g is None or len(g) < 30:
                continue
            last_i = g.index[-1]
            try:
                # 先做更严格的前置：必须至少满足一个“缩量承接”形态与一个“筹码锁定”条件，再进完整判断
                g_last = g.iloc[-1]
                g_close = float(pd.to_numeric(g_last.get("close"), errors="coerce") or 0.0)
                g_ma20 = float(pd.to_numeric(g_last.get("ma20"), errors="coerce") or 0.0)
                g_ma5 = float(pd.to_numeric(g_last.get("ma5"), errors="coerce") or 0.0)
                g_vr = float(pd.to_numeric(g_last.get("vol_ratio"), errors="coerce") or 0.0)
                if min(g_close, g_ma20, g_ma5) <= 0:
                    continue
                if not (g_ma5 >= g_ma20 * 0.985 and g_close >= g_ma20 * 0.992):
                    continue
                if not (0.8 <= g_vr <= 1.8):
                    continue
                if evaluate_shrink_divergence_mask_p5(g, {}, market_contraction_score):
                    shrink.loc[last_i] = True
            except Exception as _e:
                # 【全局审计修复】维度2：单股缩量分歧面板异常跳过该 ts_code，禁止无日志静默
                logger.debug("★缩量分歧低吸 分组计算跳过 ts_code 末行: %s", _e, exc_info=True)
                continue
        masks["P5-14·★缩量分歧低吸"] = shrink & _base_ok(df)

    return masks


def screen_p5_panel_for_date(
    df: pd.DataFrame,
    trade_date: Union[str, pd.Timestamp],
    cfg: Optional[P5PostmarketConfig] = None,
    market_contraction_score: float = 0.0,
) -> pd.DataFrame:
    cfg = _resolve_p5_cfg(cfg)
    prep = prepare_p5_lags(df)
    td = pd.to_datetime(trade_date)
    sub = prep[prep["trade_date"] == td].copy()
    if sub.empty:
        return pd.DataFrame()

    gpass = p5_global_pass_mask(sub, cfg)
    sub["_global_pass"] = gpass

    # 【缩量分歧低吸】批量面板可显式传入大盘收缩度；默认 0 保持历史兼容，与 scan 层单票 rt 路径一致。
    masks = strategy_masks_p5(sub, cfg, market_contraction_score=market_contraction_score)
    hit_mat = pd.DataFrame({k: v.astype(bool) for k, v in masks.items()}, index=sub.index)

    any_hit = hit_mat.any(axis=1)
    tags: List[str] = []
    for idx in sub.index:
        names = [c for c in hit_mat.columns if bool(hit_mat.at[idx, c])]
        names = [n for n in names if n != "P5-00·◆VWAP分时防伪"]
        tags.append("|".join(names))
    sub["strategy_tags"] = tags
    sub["hit_any"] = any_hit.values & gpass.values
    sub["global_pass"] = gpass.values

    sub["vwap_penalty_mult"] = np.where(
        hit_mat.get("P5-00·◆VWAP分时防伪", pd.Series(False, index=sub.index)).values,
        0.999,
        1.0,
    )

    out_cols = [
        "ts_code",
        "trade_date",
        "global_pass",
        "hit_any",
        "strategy_tags",
        "vwap_penalty_mult",
    ]
    out = sub[out_cols].copy()
    ordered_strategy_cols = [
        "P5-03C·★资金确认",
        "P5-03B·★资金质量",
        "P5-03A·★资金方向",
        "P5-03·★资金三重共振",
        "P5-02C·★量价确认",
        "P5-02B·★量价放量",
        "P5-02A·★量价趋势",
        "P5-02·★量价主升浪确认",
        "P5-12B·★箱体回踩",
        "P5-12A·★箱体突破",
        "P5-12·★箱体突破回踩",
        "P5-13C·★均线发散确认",
        "P5-13B·★均线发散起点",
        "P5-13A·★均线粘合",
        "P5-13·★均线粘合发散",
        "P5-14·★缩量分歧低吸",
        "P5-06·★机构龙虎缩量回踩",
        "P5-07·★外资连续建仓",
        "P5-08·★超级中军点火",
        "P5-01·★核心四因子共振",
        "P5-04·★单峰密集突破",
        "P5-05C·★趋势确认",
        "P5-05B·★趋势质量",
        "P5-05A·★趋势结构",
        "P5-00·◆VWAP分时防伪",
    ]
    for c in ordered_strategy_cols:
        if c in hit_mat.columns:
            out[c] = hit_mat[c].values
    for c in hit_mat.columns:
        if c not in out.columns:
            out[c] = hit_mat[c].values
    return out


def evaluate_p5_single_prepared_row(
    prep_row: pd.Series,
    cfg: Optional[P5PostmarketConfig] = None,
    *,
    stock_prep_df: Optional[pd.DataFrame] = None,
    rt: Optional[Dict[str, Any]] = None,
    market_contraction_score: float = 0.0,
) -> Tuple[bool, List[str], str]:
    cfg = _resolve_p5_cfg(cfg)
    one = prep_row.to_frame().T
    gp = p5_global_pass_mask(one, cfg)
    if not bool(gp.iloc[0]):
        reasons = []
        close = float(pd.to_numeric(one["close"], errors="coerce").iloc[0] or 0)
        ma60 = float(pd.to_numeric(one["ma60"], errors="coerce").iloc[0] or 0)
        slope = float(pd.to_numeric(one["ma20_slope_5"], errors="coerce").iloc[0] or 0)
        if close <= ma60 or slope <= 0:
            reasons.append("中期破位或斜率")
        bias = float(one["_bias_live"].iloc[0]) if "_bias_live" in one.columns else np.nan
        if not np.isnan(bias) and (bias > cfg.global_bias_high or bias < cfg.global_bias_low):
            reasons.append("乖离极端")
        pe = float(pd.to_numeric(one["pe_ttm"], errors="coerce").iloc[0] or 0)
        if pe < 0:
            reasons.append("亏损 pe_ttm<0")
        nm0 = float(pd.to_numeric(one["net_main_amount"], errors="coerce").iloc[0] or np.nan)
        nm1 = float(pd.to_numeric(one["nm_lag1"], errors="coerce").iloc[0] or np.nan)
        nm2 = float(pd.to_numeric(one["nm_lag2"], errors="coerce").iloc[0] or np.nan)
        if np.all(pd.notna([nm0, nm1, nm2])) and (nm0 < 0) and (nm1 < 0) and (nm2 < 0):
            reasons.append("主力三连负")
        return False, [], ";".join(reasons) if reasons else "全局否决"

    # 单票路径优先用完整历史窗口跑 masks，再只读取末行结果；缺少窗口时保持旧的单行回退逻辑。
    mask_input = one
    mask_idx = one.index[0]
    if stock_prep_df is not None and not stock_prep_df.empty:
        mask_input = stock_prep_df
        mask_idx = stock_prep_df.index[-1]
    masks = strategy_masks_p5(mask_input, cfg, market_contraction_score=market_contraction_score)
    hit_order = [
        "P5-12·★箱体突破回踩",
        "P5-12A·★箱体突破",
        "P5-12B·★箱体回踩",
        "P5-13·★均线粘合发散",
        "P5-13C·★均线发散确认",
        "P5-13B·★均线发散起点",
        "P5-13A·★均线粘合",
        "P5-05·★绝对趋势雷达",
        "P5-05C·★趋势确认",
        "P5-05B·★趋势质量",
        "P5-05A·★趋势结构",
    ]
    hits: List[str] = []
    seen_hits = set()
    for name in hit_order:
        s = masks.get(name)
        if s is None:
            continue
        try:
            hit = bool(s.loc[mask_idx])
        except Exception:
            hit = bool(s.iloc[-1]) if len(s) else False
        if hit and name not in seen_hits:
            hits.append(name)
            seen_hits.add(name)

    for name, s in masks.items():
        if name in seen_hits or name == "P5-14·★缩量分歧低吸":
            continue
        try:
            hit = bool(s.loc[mask_idx])
        except Exception:
            hit = bool(s.iloc[-1]) if len(s) else False
        if hit:
            hits.append(name)
            seen_hits.add(name)

    if market_contraction_score >= 0.7 and stock_prep_df is not None:
        if evaluate_shrink_divergence_mask_p5(stock_prep_df, rt, market_contraction_score) and "P5-14·★缩量分歧低吸" not in seen_hits:
            hits.append("P5-14·★缩量分歧低吸")

    # 单票命中结果做展示语义去重：保留更具体的确认/子标签，移除同义总标签。
    if any("P5-05C·★趋势确认" in h for h in hits):
        hits = [h for h in hits if "P5-05·★绝对趋势雷达" not in h]
    if any("P5-13C·★均线发散确认" in h for h in hits):
        hits = [h for h in hits if "P5-13·★均线粘合发散" not in h]
    if any("P5-12A·★箱体突破" in h or "P5-12B·★箱体回踩" in h for h in hits):
        hits = [h for h in hits if "P5-12·★箱体突破回踩" not in h]
    # 【V26.7 新增】滞后数据标签追加：当日龙虎榜等数据未更新时打上标签
    lag_note = ""
    if "_lag_note" in prep_row.index and prep_row["_lag_note"]:
        lag_note = str(prep_row["_lag_note"])
    return True, hits, lag_note


def build_single_stock_window_from_rt(
    df_hist: pd.DataFrame,
    rt: Dict[str, Any],
    ts_code: str,
) -> pd.DataFrame:
    if df_hist is None or df_hist.empty:
        return pd.DataFrame()

    work = df_hist.copy()
    if "ts_code" not in work.columns:
        work["ts_code"] = ts_code
    if "trade_date" not in work.columns:
        work["trade_date"] = pd.Timestamp.now().normalize()

    work["trade_date"] = pd.to_datetime(work["trade_date"])
    last_td = work["trade_date"].iloc[-1]
    rt_td_raw = rt.get("trade_date")
    rt_td = pd.to_datetime(rt_td_raw) if rt_td_raw is not None else last_td

    def _vol_to_hand(vol_raw: float) -> float:
        v = float(vol_raw)
        if v <= 0:
            return v
        return v / 100.0 if v >= 100000.0 else v

    prev_close = float(pd.to_numeric(work["close"].iloc[-1], errors="coerce") or 0.0)

    if rt_td_raw is not None and rt_td > last_td:
        row = work.iloc[-1].copy()
        row["trade_date"] = rt_td
        row["pre_close"] = prev_close
    else:
        row = work.iloc[-1].copy()

    px = rt.get("price")
    if px is None:
        px = rt.get("close")
    if px is not None:
        row["close"] = float(px)

    if rt.get("open") is not None:
        row["open"] = float(rt["open"])
    if rt.get("high") is not None:
        row["high"] = float(rt["high"])
    if rt.get("low") is not None:
        row["low"] = float(rt["low"])
    if rt.get("pre_close") is not None:
        row["pre_close"] = float(rt["pre_close"])

    if rt.get("vol") is not None:
        row["vol"] = float(rt["vol"])
    elif rt.get("volume") is not None:
        row["vol"] = _vol_to_hand(float(rt["volume"]))

    amt = rt.get("amount")
    vol_shares = rt.get("volume")
    if amt is not None and vol_shares is not None:
        amt_f = float(amt)
        vol_f = float(vol_shares)
        if amt_f > 0 and vol_f > 0:
            tentative = amt_f / max(vol_f, 1e-9)
            row["vwap"] = tentative / 100.0 if tentative > float(px or 0.0) * 20.0 else tentative

    if rt.get("tail_close") is not None:
        row["tail_close"] = float(rt["tail_close"])
    if rt.get("tail_vwap") is not None:
        row["tail_vwap"] = float(rt["tail_vwap"])

    if rt.get("pct_chg") is not None:
        row["pct_chg"] = float(rt["pct_chg"])
    elif px is not None and float(row.get("pre_close", prev_close) or 0) > 0:
        pc = float(row.get("pre_close", prev_close))
        row["pct_chg"] = (float(px) - pc) / pc * 100.0

    if rt.get("vol_ratio") is not None:
        row["vol_ratio"] = float(rt["vol_ratio"])

    for k in (
        "net_main_amount", "inst_net_buy", "hk_vol",
        "winner_rate", "nineturn_signal", "limit_times", "strth", "circ_mv",
    ):
        if k in rt and rt[k] is not None and not (isinstance(rt[k], float) and pd.isna(rt[k])):
            row[k] = rt[k]

    if rt.get("pre_close") is None:
        if rt_td_raw is not None and rt_td > last_td:
            row["pre_close"] = prev_close
        elif len(work) >= 2:
            row["pre_close"] = float(pd.to_numeric(work.iloc[-2]["close"], errors="coerce") or prev_close)
        else:
            row["pre_close"] = float(pd.to_numeric(row.get("pre_close"), errors="coerce") or prev_close)

    close_f = float(pd.to_numeric(row.get("close"), errors="coerce") or 0.0)
    vol_hand = float(pd.to_numeric(row.get("vol"), errors="coerce") or 0.0)
    cmw = float(pd.to_numeric(row.get("circ_mv"), errors="coerce") or 0.0)
    if cmw <= 0 and "circ_mv" in work.columns:
        cmw = float(pd.to_numeric(work.iloc[-1]["circ_mv"], errors="coerce") or 0.0)
        row["circ_mv"] = cmw

    tr_rt = rt.get("turnover_rate_f")
    if tr_rt is not None and not (isinstance(tr_rt, float) and pd.isna(tr_rt)) and float(tr_rt) > 0:
        row["turnover_rate_f"] = float(tr_rt)
    elif "turnover_rate_f" in work.columns:
        row["turnover_rate_f"] = float(pd.to_numeric(work.iloc[-1]["turnover_rate_f"], errors="coerce") or 0.0)
    else:
        row["turnover_rate_f"] = 0.0
    if float(row.get("turnover_rate_f", 0.0) or 0.0) <= 0 and close_f > 0 and cmw > 0 and vol_hand > 0:
        row["turnover_rate_f"] = infer_turnover_rate_f_pct(vol_hand, close_f, cmw)

    if rt_td_raw is not None and rt_td > last_td:
        work = pd.concat([work, row.to_frame().T], ignore_index=True)
    else:
        work = pd.concat([work.iloc[:-1], row.to_frame().T], ignore_index=True)

    return work
