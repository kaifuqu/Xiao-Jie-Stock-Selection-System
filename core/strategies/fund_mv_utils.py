# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 流通市值锚定资金门槛 + 真实换手率（turnover_rate_f）统一工具

【口径约定】
- circ_mv（TuShare daily_basic / 本项目库）：**万元**
- circ_mv_yi（亿）：circ_mv_wan / 10000
- net_main_amount / net_elg_amount / inst_net_buy：**元**
- hk_vol（北向持股）：**股**（与 strat_golden_10 中 hk_vol/10000=万股 一致）
- vol / volume（日线成交量）：**手**（1 手=100 股）
- amount（成交额）：**元**
- vol_ratio：量比，表示相对自身均量的放量倍数，不等于换手率
- turnover_rate_f：**百分数**（例如 5.0 表示 5%），自由流通口径，禁止降级使用 total turnover_rate。

【动态门槛】
threshold = max(流通市值(元) * ratio, 该市值阶梯绝对地板)
杜绝「5000 万一刀切」在巨无霸票上失敏、小票上过苛。
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _safe_observation_pool_relax_settings() -> Dict[str, float]:
    """
    读取 sop_v11.observation_pool_relax 三参数；任一步失败回退 0.95 / 500 / 0.56。
    即使 config_manager 导入失败或返回非 dict，也不向打分链路上抛异常。
    """
    _defaults: Dict[str, float] = {
        "vr_shrink_gate": 0.95,
        "large_cap_yi_min": 500.0,
        "turnover_floor_pct": 0.56,
    }
    try:
        from core.config_manager import get_observation_pool_relax_settings

        merged = get_observation_pool_relax_settings()
        if not isinstance(merged, dict):
            return dict(_defaults)
        out: Dict[str, float] = dict(_defaults)
        for _k in _defaults:
            try:
                out[_k] = float(merged.get(_k, _defaults[_k]))
            except (TypeError, ValueError):
                out[_k] = _defaults[_k]
        return out
    except Exception as _e:
        logger.debug("_safe_observation_pool_relax_settings 回退默认: %s", _e)
        return dict(_defaults)


def _sf(val: Any, default: float = 0.0) -> float:
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


def circ_mv_wan_from(
    y: Union[pd.Series, Dict[str, Any]],
    rt: Optional[Dict[str, Any]] = None,
) -> float:
    """流通市值（万元）：优先 rt，再 y；缺失时用 total_mv*0.6 兜底。"""
    rt = rt or {}
    raw = rt.get("circ_mv")
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        if isinstance(y, pd.Series):
            raw = y.get("circ_mv")
        else:
            raw = y.get("circ_mv") if isinstance(y, dict) else None
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        tm = _sf(rt.get("total_mv"), 0.0)
        if tm <= 0 and isinstance(y, pd.Series):
            tm = _sf(y.get("total_mv"), 0.0)
        raw = tm * 0.6 if tm > 0 else 0.0
    return max(_sf(raw, 0.0), 0.0)


def circ_mv_yi_from(y: Union[pd.Series, Dict[str, Any]], rt: Optional[Dict[str, Any]] = None) -> float:
    return circ_mv_wan_from(y, rt) / 10000.0


def circ_mv_yuan(circ_mv_wan: float) -> float:
    return max(circ_mv_wan, 0.0) * 10000.0


# ---------- 市值阶梯地板（元）：与 golden / 原 P4-P5 量级一致，略偏保守 ----------
def _floor_net_main_yuan(cm_yi: float) -> float:
    if cm_yi >= 2000.0:
        return 300_000_000.0  # 3 亿
    if cm_yi >= 1000.0:
        return 200_000_000.0
    if cm_yi >= 500.0:
        return 100_000_000.0
    if cm_yi >= 100.0:
        return 30_000_000.0  # 3 千万
    return 15_000_000.0


def _floor_net_elg_yuan(cm_yi: float) -> float:
    if cm_yi >= 2000.0:
        return 200_000_000.0
    if cm_yi >= 1000.0:
        return 150_000_000.0
    if cm_yi >= 500.0:
        return 80_000_000.0
    if cm_yi >= 100.0:
        return 25_000_000.0
    return 12_000_000.0


def _floor_inst_single_yuan(cm_yi: float) -> float:
    """单日龙虎榜机构净买（元）正数门槛地板。"""
    if cm_yi >= 2000.0:
        return 50_000_000.0
    if cm_yi >= 1000.0:
        return 35_000_000.0
    if cm_yi >= 500.0:
        return 20_000_000.0
    if cm_yi >= 100.0:
        return 5_000_000.0
    return 3_000_000.0


def _floor_inst_sum3_yuan(cm_yi: float) -> float:
    if cm_yi >= 2000.0:
        return 80_000_000.0
    if cm_yi >= 1000.0:
        return 50_000_000.0
    if cm_yi >= 500.0:
        return 30_000_000.0
    if cm_yi >= 100.0:
        return 8_000_000.0
    return 5_000_000.0


def dynamic_net_main_threshold_yuan(circ_mv_wan: float, ratio_of_float_mv: float) -> float:
    """主力净额(元)过线：max(流通市值*比例, 阶梯地板)。"""
    cy = circ_mv_yuan(circ_mv_wan)
    cm_yi = circ_mv_wan / 10000.0
    return max(cy * float(ratio_of_float_mv), _floor_net_main_yuan(cm_yi))


def dynamic_net_elg_threshold_yuan(circ_mv_wan: float, ratio_of_float_mv: float) -> float:
    cy = circ_mv_yuan(circ_mv_wan)
    cm_yi = circ_mv_wan / 10000.0
    return max(cy * float(ratio_of_float_mv), _floor_net_elg_yuan(cm_yi))


def dynamic_inst_single_threshold_yuan(circ_mv_wan: float, ratio_of_float_mv: float) -> float:
    cy = circ_mv_yuan(circ_mv_wan)
    cm_yi = circ_mv_wan / 10000.0
    return max(cy * float(ratio_of_float_mv), _floor_inst_single_yuan(cm_yi))


def dynamic_inst_sum3_threshold_yuan(circ_mv_wan: float, ratio_of_float_mv: float) -> float:
    cy = circ_mv_yuan(circ_mv_wan)
    cm_yi = circ_mv_wan / 10000.0
    return max(cy * float(ratio_of_float_mv), _floor_inst_sum3_yuan(cm_yi))


def dynamic_strth_threshold_wan(circ_mv_wan: float, ratio_of_circ_mv_wan: float) -> float:
    """
    limit_list.strth 与 circ_mv 同库时多为「万元」量级：要求 strth(万) > circ_mv(万) * ratio。
    """
    return max(circ_mv_wan * float(ratio_of_circ_mv_wan), 3000.0)


def golden_tier_elg_hk_punish_wan(cm_yi: float) -> Tuple[float, float, float]:
    """
    与 strat_golden_10 中 req_elg / req_hk / punish_elg（万元）一致，供引擎侧比较：
    - net_elg 与 net_elg_wan = net_elg/10000 比较用 req_elg
    - hk_vol_wan = hk_vol/10000 与 req_hk 比较
    """
    if cm_yi >= 2000.0:
        return 7000.0, 3500.0, -10000.0
    if cm_yi >= 1000.0:
        return 6200.0, 3100.0, -8000.0
    if cm_yi >= 500.0:
        return 3200.0, 1600.0, -5000.0
    if cm_yi >= 100.0:
        return 1500.0, 800.0, -2500.0
    return 800.0, 400.0, -1200.0


def golden_tier_net_elg_min_yuan(cm_yi: float) -> float:
    """特大单净额门槛（元），与 golden net_elg_wan 比较用 net_elg/10000 > eg_w 等价于 net_elg > 本返回值。"""
    eg_w, _, _ = golden_tier_elg_hk_punish_wan(cm_yi)
    return eg_w * 10000.0


def golden_tier_hk_min_shares(cm_yi: float) -> float:
    """北向持股量门槛（股）：hk_vol > 本值 等价于 golden 中 hk_vol/10000 > hk_w(万)。"""
    _, hk_w, _ = golden_tier_elg_hk_punish_wan(cm_yi)
    return hk_w * 10000.0


def golden_tier_punish_net_elg_yuan(cm_yi: float) -> float:
    """出货惩罚线（元，负数）。"""
    _, _, pu_w = golden_tier_elg_hk_punish_wan(cm_yi)
    return pu_w * 10000.0


# ---------- 真实换手率：仅 turnover_rate_f；缺失则手数反算，禁止 total turnover_rate ----------
def infer_turnover_rate_f_pct(vol_hand: float, close: float, circ_mv_wan: float) -> float:
    """
    自由流通换手(%) = 成交股数 / 流通市值(元) * 100
    = (vol_hand*100*close) / (circ_mv_wan*10000) * 100 = vol_hand*close / circ_mv_wan
    vol_hand=手, circ_mv_wan=万元, close=元/股。

    停牌、新股除权后 circ_mv 异常小、或脏数据时，裸除法可能产生 inf；
    这里对分母做下界并对结果做有限值裁剪，保证上游 P1/扫描引擎永不因换手反算而崩。
    """
    vh = _sf(vol_hand, 0.0)
    cl = _sf(close, 0.0)
    cm = _sf(circ_mv_wan, 0.0)
    if vh <= 0 or cl <= 0 or cm <= 0:
        return 0.0
    denom = max(cm, 1e-9)
    raw = vh * cl / denom
    if not np.isfinite(raw):
        return 0.0
    return float(np.clip(raw, 0.0, 100.0))


def vol_hand_from_rt_or_y(rt: Dict[str, Any], y: pd.Series) -> float:
    v = _sf(rt.get("volume"), 0.0)
    if v > 0:
        return v / 100.0
    if "vol" in y.index:
        return _sf(y.get("vol"), 0.0)
    return 0.0


def effective_turnover_rate_f(
    rt: Dict[str, Any],
    y: pd.Series,
    close_live: float,
) -> float:
    """
    真实自由流通换手率（%）。

    优先级：
    1) rt.turnover_rate_f
    2) y.turnover_rate_f
    3) 用 volume/vol + close + circ_mv 反算

    绝不读取 turnover_rate（总股本换手）。
    """
    raw = rt.get("turnover_rate_f")
    if raw is not None and not (isinstance(raw, float) and pd.isna(raw)):
        t = _sf(raw, 0.0)
        if t > 0 and np.isfinite(t):
            return float(np.clip(t, 0.0, 100.0))
    t2 = _sf(y.get("turnover_rate_f"), 0.0)
    if t2 > 0 and np.isfinite(t2):
        return float(np.clip(t2, 0.0, 100.0))
    cm = circ_mv_wan_from(y, rt)
    vh = vol_hand_from_rt_or_y(rt, y)
    clv = _sf(close_live, 0.0)
    out = infer_turnover_rate_f_pct(vh, clv, cm)
    if not np.isfinite(out):
        return 0.0
    return float(np.clip(out, 0.0, 100.0))


def vector_effective_turnover_rate_f(
    vol_hand: pd.Series,
    close: pd.Series,
    circ_mv_wan: pd.Series,
    turnover_f_existing: pd.Series,
) -> pd.Series:
    """向量化：优先已有 turnover_rate_f，否则 vol*close/circ_mv 反算（与同模块单点公式一致）。"""
    vh = pd.to_numeric(vol_hand, errors="coerce").fillna(0.0)
    cl = pd.to_numeric(close, errors="coerce").fillna(0.0)
    cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
    te = pd.to_numeric(turnover_f_existing, errors="coerce")
    cmv = cm.to_numpy(dtype=float)
    clv = cl.to_numpy(dtype=float)
    vhv = vh.to_numpy(dtype=float)
    denom = np.maximum(cmv, 1e-9)
    inferred = np.where((cmv > 0) & (clv > 0), vhv * clv / denom, 0.0)
    inferred = np.where(np.isfinite(inferred), inferred, 0.0)
    inferred = np.clip(inferred, 0.0, 100.0)
    tev = te.to_numpy(dtype=float)
    out = np.where(np.isfinite(tev) & (tev > 0), tev, inferred)
    out = np.where(np.isfinite(out), np.clip(out, 0.0, 100.0), 0.0)
    return pd.Series(out, index=vh.index, dtype=float)


def _vol_hand_series_from_daily_df(df: pd.DataFrame) -> pd.Series:
    idx = df.index
    if "vol" in df.columns:
        return pd.to_numeric(df["vol"], errors="coerce").fillna(0.0)
    if "volume" in df.columns:
        return pd.to_numeric(df["volume"], errors="coerce").fillna(0.0) / 100.0
    return pd.Series(0.0, index=idx)


def _circ_mv_wan_series_from_daily_df(df: pd.DataFrame) -> pd.Series:
    if "circ_mv" in df.columns:
        cm = pd.to_numeric(df["circ_mv"], errors="coerce").fillna(0.0)
    else:
        cm = pd.Series(0.0, index=df.index)
    if "total_mv" in df.columns:
        tm = pd.to_numeric(df["total_mv"], errors="coerce").fillna(0.0)
        cm = cm.where(cm > 0, tm * 0.6)
    return cm


def series_effective_turnover_f_daily(df: pd.DataFrame) -> pd.Series:
    """
    日线逐行有效真实换手(%)：优先 turnover_rate_f>0，否则 vol(手)×close/circ_mv(万)。
    禁止读取 turnover_rate。
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)
    vh = _vol_hand_series_from_daily_df(df)
    cl = pd.to_numeric(df["close"], errors="coerce").fillna(0.0) if "close" in df.columns else pd.Series(0.0, index=df.index)
    cm = _circ_mv_wan_series_from_daily_df(df)
    if "turnover_rate_f" in df.columns:
        tf = pd.to_numeric(df["turnover_rate_f"], errors="coerce")
    else:
        tf = pd.Series(np.nan, index=df.index)
    return vector_effective_turnover_rate_f(vh, cl, cm, tf)


def mean_effective_turnover_f_last_n(df: pd.DataFrame, n: int = 3) -> float:
    """最近 n 根日 K 的有效真实换手算术均值(%)。"""
    if df is None or len(df) < 1 or n < 1:
        return 0.0
    s = series_effective_turnover_f_daily(df.tail(int(n)))
    if s.empty:
        return 0.0
    return float(s.mean())


def vector_dynamic_net_main_threshold(
    circ_mv_wan: pd.Series,
    ratio: float,
) -> pd.Series:
    cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
    cm_yi = cm / 10000.0
    cy = cm * 10000.0
    floor = np.select(
        [cm_yi >= 2000, cm_yi >= 1000, cm_yi >= 500, cm_yi >= 100],
        [300_000_000.0, 200_000_000.0, 100_000_000.0, 30_000_000.0],
        default=15_000_000.0,
    )
    return pd.Series(np.maximum(cy * float(ratio), floor), index=cm.index)


def vector_dynamic_net_elg_threshold(circ_mv_wan: pd.Series, ratio: float) -> pd.Series:
    cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
    cm_yi = cm / 10000.0
    cy = cm * 10000.0
    floor = np.select(
        [cm_yi >= 2000, cm_yi >= 1000, cm_yi >= 500, cm_yi >= 100],
        [200_000_000.0, 150_000_000.0, 80_000_000.0, 25_000_000.0],
        default=12_000_000.0,
    )
    return pd.Series(np.maximum(cy * float(ratio), floor), index=cm.index)


def vector_dynamic_inst_sum3_threshold(circ_mv_wan: pd.Series, ratio: float) -> pd.Series:
    cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
    cm_yi = cm / 10000.0
    cy = cm * 10000.0
    floor = np.select(
        [cm_yi >= 2000, cm_yi >= 1000, cm_yi >= 500, cm_yi >= 100],
        [80_000_000.0, 50_000_000.0, 30_000_000.0, 8_000_000.0],
        default=5_000_000.0,
    )
    return pd.Series(np.maximum(cy * float(ratio), floor), index=cm.index)


def vector_dynamic_strth_threshold_wan(circ_mv_wan: pd.Series, ratio: float) -> pd.Series:
    cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
    return pd.Series(np.maximum(cm * float(ratio), 3000.0), index=cm.index)


def vector_nm_fly_negative_threshold(circ_mv_wan: pd.Series, ratio: float) -> pd.Series:
    """策略六防飞刀：主力净额不得深负；幅度与市值成比例（元，负数）。"""
    cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
    cy = cm * 10000.0
    return pd.Series(-np.maximum(cy * float(ratio), 30_000_000.0), index=cm.index)


def adaptive_turnover_f_range(circ_mv_wan: Union[float, pd.Series], base_low: float, base_high: float) -> Tuple[Union[float, pd.Series], Union[float, pd.Series]]:
    """
    【V26.5 A股换手率自适应】根据流通市值动态调整换手率合理区间。
    A股不同市值级别股票的换手率差异巨大：
    - 大盘蓝筹（>=500亿）：0.3%~2%
    - 中盘成长（100亿~500亿）：0.5%~4%
    - 小盘题材（30亿~100亿）：1%~6%
    - 微盘次新（<30亿）：2%~10%

    参数：
        circ_mv_wan: 流通市值（万元）
        base_low/high: 基准下限/上限（应用于中盘股）
    返回：(自适应下限, 自适应上限)
    """
    if isinstance(circ_mv_wan, pd.Series):
        cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
        cm_yi = cm / 10000.0
        mult_low = np.select(
            [cm_yi >= 500, cm_yi >= 100, cm_yi >= 30],
            [0.20, 0.50, 0.80],
            default=1.50,
        )
        mult_high = np.select(
            [cm_yi >= 500, cm_yi >= 100, cm_yi >= 30],
            [0.35, 0.70, 1.20],
            default=1.80,
        )
        return pd.Series(np.maximum(base_low * mult_low, 0.5), index=cm.index), pd.Series(np.minimum(base_high * mult_high, 25.0), index=cm.index)
    else:
        cm_yi = float(circ_mv_wan) / 10000.0
        if cm_yi >= 500:
            mult_low, mult_high = 0.20, 0.35
        elif cm_yi >= 100:
            mult_low, mult_high = 0.50, 0.70
        elif cm_yi >= 30:
            mult_low, mult_high = 0.80, 1.20
        else:
            mult_low, mult_high = 1.50, 1.80
        return max(base_low * mult_low, 0.5), min(base_high * mult_high, 25.0)


def vector_adaptive_turnover_f_threshold(
    circ_mv_wan: pd.Series,
    base_low: float,
    base_high: float,
) -> Tuple[pd.Series, pd.Series]:
    """
    向量化版 adaptive_turnover_f_range（P5 向量化筛选器专用）。
    返回：(自适应下限Series, 自适应上限Series)
    """
    cm = pd.to_numeric(circ_mv_wan, errors="coerce").fillna(0.0)
    cm_yi = cm / 10000.0
    mult_low = np.select(
        [cm_yi >= 500, cm_yi >= 100, cm_yi >= 30],
        [0.20, 0.50, 0.80],
        default=1.50,
    )
    mult_high = np.select(
        [cm_yi >= 500, cm_yi >= 100, cm_yi >= 30],
        [0.35, 0.70, 1.20],
        default=1.80,
    )
    return (
        pd.Series(np.maximum(base_low * mult_low, 0.5), index=cm.index),
        pd.Series(np.minimum(base_high * mult_high, 25.0), index=cm.index),
    )


# ---------- 【自适应优化】市场极度缩量期：全市场因子 + 辅助观察池门槛（不改变工具函数原有调用语义） ----------


def compute_market_contraction_context(
    base_items: List[Dict[str, Any]],
    rt_map: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    # 【自适应优化】基于当前扫描/洗盘样本与 rt_map，估计全市场情绪收缩度 market_contraction_score（0~1）。
    # 使用已有日线 df：近5日有效真实换手均值；rt 量比中位数、成交额中位数。换手+量比中位双阈值同时满足时 score≥0.7 触发放宽。
    # 量比门限 vr_shrink_gate 来自 config.yaml sop_v11.observation_pool_relax，失败时回退默认。
    """
    _opr = _safe_observation_pool_relax_settings()
    _vr_shrink_gate = float(_opr.get("vr_shrink_gate", 0.95))
    rt_map = rt_map or {}
    turnovers: List[float] = []
    vrs: List[float] = []
    amounts: List[float] = []

    for item in base_items or []:
        if not isinstance(item, dict):
            continue
        df = item.get("df")
        code = item.get("code", "")
        # 【全局审计修复】样本污染：极短 K 线不参与全市场换手均值；有效样本须近 5 日换手可算且 ≥0.3%（过滤 NaN/脏零）
        if not isinstance(df, pd.DataFrame) or df.empty or len(df) < 5:
            continue
        try:
            t_eff = float(mean_effective_turnover_f_last_n(df, 5))
        except Exception:
            continue
        if not np.isfinite(t_eff) or t_eff < 0.3:
            continue
        turnovers.append(t_eff)
        sc = str(code).split(".")[0][:6]
        rtd = rt_map.get(sc, {}) if isinstance(rt_map, dict) else {}
        if not isinstance(rtd, dict):
            rtd = {}
        vr = _sf(rtd.get("vol_ratio", 0.0))
        if vr <= 0:
            try:
                yb = df.iloc[-1]
                vh = _sf(yb.get("vol", yb.get("volume", 0.0)))
                if "volume" in yb.index and "vol" not in yb.index:
                    vh = _sf(yb.get("volume", 0.0)) / 100.0
                vma5 = _sf(yb.get("vol_ma5"), 0.0)
                if vma5 <= 0:
                    vma5 = _sf(yb.get("vol_ma10"), 0.0)
                if vma5 > 0 and vh > 0:
                    vr = float(vh / vma5)
            except Exception:
                vr = 0.0
        if vr > 0:
            # 【全局审计修复】维度2：异常 vr 若为 NaN 会污染全市场中位数，入列前须有限
            _fvr = min(float(vr), 20.0)
            if np.isfinite(_fvr):
                vrs.append(_fvr)
        am = _sf(rtd.get("amount", 0.0))
        if am > 0:
            amounts.append(float(am))

    avg_t = float(np.mean(turnovers)) if turnovers else 0.0
    med_vr = float(np.median(vrs)) if vrs else 1.0
    med_amt = float(np.median(amounts)) if amounts else 0.0
    # 【全局审计修复】维度2：均值/中位数在脏样本下可能为 NaN，先净化再参与阈值与文案
    if not np.isfinite(avg_t):
        avg_t = 0.0
    if not np.isfinite(med_vr):
        med_vr = 1.0
    if not np.isfinite(med_amt):
        med_amt = 0.0

    score = 0.0
    # 【全局审计修复】缩量判定可靠性：至少 30 只有效样本（近 5 日换手≥0.3% 且 K 线≥5）才允许 score≥0.7，降低误判
    # 【配置驱动】量比中位门限 = vr_shrink_gate（YAML 可调），文案同步拼接该数值
    if avg_t < 1.5 and med_vr < _vr_shrink_gate and len(turnovers) >= 30:
        score = min(
            1.0,
            0.7
            + (1.5 - avg_t) * 0.06
            + (_vr_shrink_gate - med_vr) * 0.12,
        )
        adaptive_reason = (
            f"缩量放宽·已触发｜换手{avg_t:.2f}% 量比中位{med_vr:.2f} 额中位{med_amt:.0f}｜n={len(turnovers)}"
        )
    else:
        adaptive_reason = (
            f"缩量放宽·未触发｜换手{avg_t:.2f}% 量比{med_vr:.2f} 额中位{med_amt:.0f}｜"
            f"n={len(turnovers)}（放宽需 n≥30、换手<1.5%、量比<{_vr_shrink_gate}）"
        )

    # 【全局审计修复】维度2：换手/量比样本若含 NaN/inf，min 组合可能产生非有限 score，统一压入 [0,1] 避免下游比较失真
    if not np.isfinite(score):
        logger.debug("compute_market_contraction_context: score 非有限(%s)，置 0", score)
        score = 0.0
    score = float(np.clip(score, 0.0, 1.0))

    return {
        "score": score,
        "avg_turnover_5d": avg_t,
        "median_vol_ratio": med_vr,
        "median_amount": med_amt,
        "adaptive_reason": adaptive_reason,
        "sample_count": len(turnovers),
    }


def adaptive_turnover_kill_threshold_relaxed(
    circ_mv_yi: float,
    net_main_amount: Optional[float] = None,
    net_elg_amount: Optional[float] = None,  # 已弃用：保留参数避免调用方签名破坏，不参与判断
) -> float:
    """
    【自适应优化】P1 股性呆滞撤销线：极度缩量期按市值档下调约 25%，基础不低于全局真实换手地板 0.8%。

    【配置驱动】流通市值≥large_cap_yi_min（亿元）且主力净额为正时，在已放宽阈值上再 ×0.8；
    绝对底线不低于 turnover_floor_pct（config.yaml sop_v11.observation_pool_relax，失败回退 500 / 0.56）。
    """
    _opr = _safe_observation_pool_relax_settings()
    _large_cap_yi_min = float(_opr.get("large_cap_yi_min", 500.0))
    _turnover_floor_pct = float(_opr.get("turnover_floor_pct", 0.56))
    if circ_mv_yi >= 2000.0:
        raw = 0.65 * 0.75
    elif circ_mv_yi >= 1000.0:
        raw = 1.05 * 0.75
    else:
        raw = 1.5 * 0.75
    base = max(0.8, float(raw))
    nm = _sf(net_main_amount, 0.0)
    if circ_mv_yi >= _large_cap_yi_min and nm > 0.0:
        base = max(_turnover_floor_pct, base * 0.8)
    return float(base)


def adaptive_relax_vol_ratio_min(base_vr_min: float) -> float:
    # 【自适应优化】量比门槛下调约 25%，绝对不低于 0.6
    return max(0.6, float(base_vr_min) * 0.75)


def adaptive_fund_strict_both_positive(net_main: float, net_elg: float) -> bool:
    """严格资金面：主力与特大单双正（核心主池语义，供对照）。"""
    return net_main > 0.0 and net_elg > 0.0


def adaptive_fund_observer_any_positive(net_main: float, net_elg: float = 0.0) -> bool:
    # 【自适应优化】辅助观察池：以主力净流入为唯一口径（net_elg 参数忽略）
    return net_main > 0.0


def adaptive_observer_min_entry_score(core_line: float = 60.0, observer_line: float = 55.0) -> Tuple[float, float]:
    # 【自适应优化】返回 (核心线, 辅助观察线)；扫描层按门禁路径择一（与 scan_engine min_pass 缺省对齐）
    return float(core_line), float(observer_line)


def _adaptive_resolve_pre_close_rt_y(rt: Dict[str, Any], curr: Any) -> float:
    if not isinstance(rt, dict):
        rt = {}
    pc = _sf(rt.get("pre_close", 0.0))
    if pc > 0:
        return pc
    if curr is not None and hasattr(curr, "get"):
        pc = _sf(curr.get("pre_close", 0.0))
        if pc > 0:
            return pc
        pc = _sf(curr.get("close", 0.0))
    return float(pc or 0.0)


def _observer_macd_weak_bull_ok(df: pd.DataFrame, curr: Any, macd_bar: float) -> bool:
    """
    【缩量辅助门禁·P3】不要求 MACD 柱绝对翻红：做空动能衰竭即可——
    柱体较昨日回升（macd_bar > macd_bar_prev）且柱值不深于 -0.08。
    """
    macd_bar_prev = 0.0
    try:
        if df is not None and isinstance(df, pd.DataFrame) and len(df) >= 2:
            prev_row = df.iloc[-2]
            macd_bar_prev = _sf(
                prev_row.get("macd_bar", prev_row.get("macd_hist", 0.0))
            )
    except Exception:
        macd_bar_prev = 0.0
    return bool((macd_bar > macd_bar_prev) and (macd_bar > -0.08))


def adaptive_relaxed_golden_gate_ok(
    pool_key: Optional[str],
    df: pd.DataFrame,
    rt: Dict[str, Any],
    market_contraction_score: float,
) -> bool:
    """
    # 【自适应优化】仅当 market_contraction_score≥0.7 时生效：镜像 strat_base 各池门禁但量比/换手阈值下调约 25%（底线 0.6），
    # P3/P4 观察池辅助路径：量比底线进一步放宽至 0.75；P3 的 MACD 允许「动能衰竭」而非强制柱>0。
    # P4 资金面由「特大单必须为正」放宽为「主力或特大单至少一项为正」。不改变核心 strict_golden_burst_ok 实现。
    """
    if market_contraction_score < 0.7:
        return False
    try:
        if df is None or rt is None or not isinstance(df, pd.DataFrame) or df.empty:
            return False
        curr = df.iloc[-1]
        pk = str(pool_key or "").strip().lower()

        price = _sf(rt.get("price", curr.get("close", 0.0)))
        pre_close = _adaptive_resolve_pre_close_rt_y(rt, curr)
        open_price = _sf(rt.get("open", curr.get("open", 0.0)))
        high_price = _sf(rt.get("high", curr.get("high", 0.0)))
        vr = _sf(rt.get("vol_ratio", 0.0))
        if vr <= 0:
            vr = max(_sf(curr.get("vol_ratio", 0.0)), 0.05)
        if vr <= 0:
            vr = 1.0
        if price <= 0 or pre_close <= 0:
            return False
        pct_chg = (price - pre_close) / pre_close * 100.0
        macd_bar = _sf(curr.get("macd_bar", curr.get("macd_hist", 0.0)))

        if pk == "p2":
            winner_rate = _sf(rt.get("winner_rate", curr.get("winner_rate", 0.0)))
            cost_50th = _sf(rt.get("cost_50th", curr.get("cost_50th", 0.0)))
            open_pct = (open_price - pre_close) / pre_close * 100.0 if pre_close > 0 else 0.0
            vr_min = adaptive_relax_vol_ratio_min(1.2)  # 【自适应优化】原 1.2 → 约 0.9，不低于 0.6
            if not (open_pct > 1.0 and vr >= vr_min):
                return False
            chip_ok = (
                winner_rate > 0
                and cost_50th > 0
                and cost_50th < price * 20.0
                and cost_50th > price * 0.05
            )
            if chip_ok and not (winner_rate > 85.0 and open_price > cost_50th):
                return False
            return True

        if pk == "p3":
            amount = _sf(rt.get("amount", 0.0))
            volume = _sf(rt.get("volume", 0.0))
            # 【缩量观察池】量比底线 0.75（原 adaptive_relax_vol_ratio_min(1.2)≈0.9，进一步容忍地量盘）
            vr_min = 0.75
            if not (pct_chg > 1.2 and vr >= vr_min):
                return False
            macd_fade = _observer_macd_weak_bull_ok(df, curr, macd_bar)
            if amount <= 0 or volume <= 0:
                return macd_fade
            vol_safe = max(volume, 1e-9)
            tentative = amount / vol_safe
            vwap = amount / max(volume * 100.0, 1e-9) if tentative > price * 20 else tentative
            if vwap <= 0:
                return macd_fade
            if not (price > vwap):
                return False
            if not macd_fade:
                return False
            return True

        if pk == "p4":
            tail_vol_ratio = _sf(rt.get("tail_vol_ratio", 0.0))
            net_main_amt = _sf(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)))
            # 【缩量观察池】与 P3 对齐量比底线 0.75（原 adaptive_relax_vol_ratio_min(1.1)≈0.825）
            vr_min = 0.75
            if not (2.0 < pct_chg < 8.0 and vr >= vr_min):
                return False
            if pre_close <= 0:
                return False
            upper_shadow_pct = (high_price - max(price, open_price)) / pre_close * 100.0
            tail_ok = (tail_vol_ratio > 1.5) if tail_vol_ratio > 0 else True
            fund_ok = net_main_amt > 0.0
            if not (tail_ok and upper_shadow_pct < 2.8 and fund_ok):
                return False
            return True

        if pk == "p5":
            try:
                from core.config_manager import get_golden_config

                gb = get_golden_config()
                p5_vr_min = float(gb.get("p5_golden_vr_min", 1.2))
                p5_lo = float(gb.get("p5_golden_pct_low", 2.0))
                p5_hi = float(gb.get("p5_golden_pct_high", 7.0))
            except Exception:
                p5_vr_min, p5_lo, p5_hi = 1.2, 2.0, 7.0
            vr_need = adaptive_relax_vol_ratio_min(p5_vr_min)
            if vr < vr_need:
                return False
            if not (p5_lo < pct_chg < p5_hi):
                return False
            net_main_amt = _sf(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)))
            if net_main_amt <= 0.0:
                return False
            ma5_line = _sf(curr.get("ma5", 0.0))
            ma20_line = _sf(curr.get("ma20", 0.0))
            if ma20_line <= 0:
                return False
            if not (price > ma20_line and price > ma5_line):
                return False
            return True

        return False
    except Exception as _e:
        # 【全局审计修复】维度2：辅助门禁异常时拒绝放行，并打 debug 便于复盘脏 rt/df，禁止静默吞掉
        logger.debug("adaptive_relaxed_golden_gate_ok 异常回退 False: %s", _e, exc_info=True)
        return False
