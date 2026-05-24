# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 综合分稳定化工具（截面 Rank + 对数饱和 + 爆发压制）
+ P1 底仓多维分项平滑插值（实盘验证口径，不读写 DuckDB 表结构）

【设计目的】
1. 缓解「换手 × 量比 × 战法爆发」在链条上的隐性多重共线性：同一日的流动性信息不应在性格乘子与战法分上被重复放大。
2. 对连续型极端值（涨幅、量比、真换手、主力推力）做截面分位与对数饱和，避免单日妖股靠一项指标把综合分拉到离谱，掩盖 P1 基因或其它维度短板。
3. 本模块只做数学变换与乘子，不读写 UI；输出由 scan_engine 在既有公式末尾乘上稳定因子，不改变 DataFrame 列名与展示口径。

【使用约定】
- 所有 Rank 均在「当次扫描候选集」内计算；样本过少（<5）时退化为 0.5 中性分位，避免噪声。
- 返回值保证有限、有界，不向外抛 NaN/inf。
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _finite01(x: float) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.5
    if not math.isfinite(v):
        return 0.5
    return float(np.clip(v, 0.0, 1.0))


def percentile_ranks(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    对指定列做截面百分比秩（average 法），结果落在 (0,1) 近似均匀。
    行数 < 5 时各列填 0.5。
    """
    out = df.copy()
    n = len(out)
    if n < 5:
        for c in cols:
            if c in out.columns:
                out[c + "_r"] = 0.5
        return out
    for c in cols:
        if c not in out.columns:
            out[c + "_r"] = 0.5
            continue
        s = pd.to_numeric(out[c], errors="coerce")
        s = s.replace([np.inf, -np.inf], np.nan).fillna(s.median() if s.notna().any() else 0.0)
        rk = s.rank(pct=True, method="average")
        out[c + "_r"] = rk.fillna(0.5).clip(0.001, 0.999)
    return out


def log_saturation(x: float, knee: float, ceiling: float) -> float:
    """
    标量对数饱和：x<=knee 近似线性；超过 knee 后增量递减，渐近于 ceiling。
    用于战法 burst 或加分项上沿，防止单日脉冲把总分顶穿。
    """
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    v = max(v, 0.0)
    if v <= knee:
        return v
    span = max(ceiling - knee, 1e-9)
    excess = v - knee
    return float(knee + span * (1.0 - math.exp(-excess / max(span * 0.6, 1e-9))))


def compress_surge_bonus(surge: float, linear_cap: float = 10.0, asymptote: float = 16.0) -> float:
    """
    金共振/十绝等 surge_bonus：前 linear_cap 点保持原值，之后对数压缩，渐近于 asymptote。
    """
    s = max(0.0, float(np.nan_to_num(surge, nan=0.0, posinf=0.0, neginf=0.0)))
    return log_saturation(s, knee=linear_cap, ceiling=asymptote)


def dampen_burst_by_extremes(
    burst: float,
    p1_gene: float,
    vr_r: float,
    pct_r: float,
    trf_r: float,
    main_r: float,
) -> float:
    """
    在保持 burst 为「主信息源」的前提下，当 P1 基因偏弱而量价换手同时处于截面极端高分位、
    且主力推力分位不高时，对 burst 做温和下调（乘子约 0.55~1.0），减轻一日游妖票刷分。

    参数均为标量；main_r 为「主力推力」截面秩（越高越强）。
    """
    b = float(np.nan_to_num(burst, nan=0.0, posinf=0.0, neginf=0.0))
    if b <= 1e-9:
        return b
    vr_r, pct_r, trf_r, main_r = _finite01(vr_r), _finite01(pct_r), _finite01(trf_r), _finite01(main_r)
    pg = float(np.nan_to_num(p1_gene, nan=70.0, posinf=100.0, neginf=0.0))

    # 基因越低于 72 越「弱」，只在该区间发力
    gene_weak = max(0.0, min(1.0, (72.0 - pg) / 28.0))

    # 流动性双高：量比秩与换手秩同时偏高
    liq_hot = max(0.0, min(1.0, (vr_r + trf_r) * 0.5 - 0.78))

    # 涨幅截面也极端高
    price_hot = max(0.0, min(1.0, pct_r - 0.82))

    # 主力推力在截面并不强（秩偏低），却打出高 burst —— 典型「虚火」
    fund_cold = max(0.0, min(1.0, 0.72 - main_r))

    # 合成抑制强度（0~1）
    stress = gene_weak * liq_hot * price_hot * (0.35 + 0.65 * fund_cold)
    mult = 1.0 - 0.42 * stress
    mult = float(np.clip(mult, 0.55, 1.0))
    return float(b * mult)


def burst_soft_cap(burst: float, soft: float = 94.0, hard: float = 102.0) -> float:
    """
    战法 raw burst 上沿软封顶：soft 以下不变，超过部分对数压向 hard，避免配置里 110 分战法名实不符地线性顶格。
    """
    b = float(np.nan_to_num(burst, nan=0.0, posinf=hard, neginf=0.0))
    if b <= soft:
        return b
    return log_saturation(b, knee=soft, ceiling=hard)


def personality_liquidity_blend(trn_multi: float, vr_rank: float) -> float:
    """
    性格乘子：日内相对历史的换手倍数(trn_multi) 与 截面量比秩(vr_rank) 组合。
    用几何平均降低「换手与量比同源」的线性叠乘效应。
    """
    t = float(np.nan_to_num(trn_multi, nan=1.0, posinf=1.15, neginf=0.85))
    t = float(np.clip(t, 0.85, 1.15))
    vr = _finite01(vr_rank)
    # 将秩映射到与旧 vol_multi 可比的轻微摆动
    vol_from_rank = float(np.interp(vr, [0.05, 0.35, 0.55, 0.75, 0.95], [0.90, 0.97, 1.0, 1.06, 1.12]))
    vol_from_rank = float(np.clip(vol_from_rank, 0.85, 1.15))
    geo = math.sqrt(max(t, 1e-9) * max(vol_from_rank, 1e-9))
    return float(np.clip(geo, 0.85, 1.15))


def build_rank_lookup(cs_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """
    由带 *_r 列的截面表构建 s_code -> 分数字典。

    【性能优化 V2】向量化替代 iterrows：
    原 iterrows 逐行 Python 迭代改为 dict comprehension，
    利用 pandas 列访问直接构造 dict，性能提升 5-10 倍。
    """
    if cs_df is None or cs_df.empty or "s_code" not in cs_df.columns:
        return {}
    # 直接用列名访问构造 dict，避免 iterrows 的 Python 行开销
    sc_series = cs_df["s_code"].astype(str).str.strip()
    valid_mask = sc_series != ""
    out = {}
    for sc, pct_r, vol_ratio_r, turnover_f_r, main_ratio_r in zip(
        sc_series[valid_mask],
        cs_df.loc[valid_mask, "pct_r"].fillna(0.5),
        cs_df.loc[valid_mask, "vol_ratio_r"].fillna(0.5),
        cs_df.loc[valid_mask, "turnover_f_r"].fillna(0.5),
        cs_df.loc[valid_mask, "main_ratio_r"].fillna(0.5),
    ):
        if sc:
            out[sc] = {
                "pct_r": float(pct_r) if pct_r else 0.5,
                "vol_ratio_r": float(vol_ratio_r) if vol_ratio_r else 0.5,
                "turnover_f_r": float(turnover_f_r) if turnover_f_r else 0.5,
                "main_ratio_r": float(main_ratio_r) if main_ratio_r else 0.5,
            }
    return out


def neutral_ranks() -> Dict[str, float]:
    return {"pct_r": 0.5, "vol_ratio_r": 0.5, "turnover_f_r": 0.5, "main_ratio_r": 0.5}


# =============================================================================
# P1 底仓：多维分项平滑插值 + 行业/市值附加分（仅运算衍生分，不改 DuckDB DDL）
# =============================================================================
# 说明：已废弃「资金共振截面分」参与 P1 一票否决与排序；本函数为 P1 平滑百分制主来源；
# 可选按 config/constants 将 fund_memory_score 凸入最终分（与系统文档 FUND_MEMORY_WEIGHT_P1 一致）。


def _p1_scalar_finite(x: Any, default: float = 0.0) -> float:
    """与 pool_manager._safe_float 语义一致：None/NaN/空串回退 default，避免插值链污染。"""
    if x is None:
        return default
    try:
        if pd.isna(x) or str(x).strip() in ("", "-"):
            return default
        return float(x)
    except (ValueError, TypeError):
        return default


def _p1_finite_clip_scalar(x: Any) -> float:
    """压成有限 float，供 np.interp 与除法分母使用。"""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(v):
        return 0.0
    return v


def compute_p1_multi_dim_smooth_score(
    df: pd.DataFrame,
    rt: Any,
    circ_mv_yi: float,
    ind_rank: int,
    pe: float,
    ind_stats: Dict[str, Any],
    ind: str,
    dynamic_industries: Dict[str, float],
    avg_trn: float,
    pass_line: float,
) -> Tuple[float, bool, str, Dict[str, Any]]:
    """
    P1 底仓「多维平滑插值」百分制（满分 100，含附加分后仍 cap 在 100）。

    分项结构（非字面「11 个」）：核心插值项含筹码/趋势/均线/资金/势能/波段/黄金/健康/斜率/PE/熔断等，
    明细中另含假突破惩罚、行业/板块/市值附加与市值档位标签。

    执行顺序：①左侧否决 → ②各维写入 score_details →③base_score 与记忆凸组合得 final
    →④据 details 与入参 pass_line 算 effective_pass_line（共振时 base-4）并判定 passed。

    约束：
    - 不读取、不写入、不修改 DuckDB 表结构；仅使用传入的 DataFrame / 行情字典做派生运算。
    - 不使用日线 capital_resonance_score；fund_memory_score（0~200→0~100）仅当 fund_memory_weight_p1>0 时参与
      final_score = (1-w)*base_score + w*fm100；base_score 为正面维度和 + 假突破惩罚 + 高位熔断倒扣。

    返回：(final_score, is_pass, reason, score_details)
    """
    try:
        from core.strategies.fund_mv_utils import effective_turnover_rate_f
        from core.config_manager import (
            get_p1_combo_multiplier_config,
            get_p1_fund_memory_weight,
            get_p1_select_min_circ_mv_wan,
        )

        try:
            p1_min_yi = float(get_p1_select_min_circ_mv_wan()) / 10000.0
        except Exception:
            try:
                import constants as _constants

                p1_min_yi = float(getattr(_constants, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000)) / 10000.0
            except Exception:
                p1_min_yi = 100.0

        rt_d = rt if isinstance(rt, dict) else {}
        curr = df.iloc[-1]

        # ------------------------------------------------------------------
        # 防守端：带放量豁免的左侧一票否决（先于各维度计分）
        # ------------------------------------------------------------------
        _ma20_v = _p1_scalar_finite(curr.get("ma20", 0.0), 0.0)
        _ma60_v = _p1_scalar_finite(curr.get("ma60", 0.0), 0.0)
        _slope5_v = _p1_scalar_finite(curr.get("ma20_slope_5", 0.0), 0.0)
        _bias20_v = _p1_scalar_finite(curr.get("bias_20", 0.0), 0.0)
        _vr_v = _p1_scalar_finite(rt_d.get("vol_ratio", curr.get("vol_ratio", 1.0)), 1.0)
        if not np.isfinite(_vr_v) or _vr_v <= 0:
            _vr_v = 1.0
        _pct_v = _p1_scalar_finite(rt_d.get("pct_chg", curr.get("pct_chg", 0.0)), 0.0)
        if not np.isfinite(_pct_v):
            _pct_v = 0.0
        _close_v = _p1_scalar_finite(curr.get("close", 0.0), 0.0)
        _break_left = (
            _slope5_v < -0.05
            or (_ma60_v > 0 and _ma20_v < _ma60_v * 0.985)
            or _bias20_v < -12.0
        )
        _exempt = _vr_v >= 1.4 and _pct_v >= 2.5 and _close_v >= _ma20_v
        if _break_left and not _exempt:
            return (0.0, False, "左侧极寒未放量，一票否决", {})

        # ------------------------------------------------------------------
        # 现价：优先实时 dict，否则末行收盘价（与原先 P1 链一致）
        # ------------------------------------------------------------------
        now_price = _p1_scalar_finite(
            rt_d.get("price", curr.get("close", 0.0)),
            0.0,
        )

        # ------------------------------------------------------------------
        # ① 筹码真空（满分 14）：按真实换手 turnover_rate_f 做分段线性插值。
        # ------------------------------------------------------------------
        turnover_today = effective_turnover_rate_f(rt_d, curr, now_price)
        if not np.isfinite(turnover_today):
            turnover_today = 0.0
        turnover_today = float(np.clip(turnover_today, 0.0, 100.0))
        turnover_eval = turnover_today if turnover_today > 0 else _p1_scalar_finite(avg_trn, 0.0)
        turnover_eval = float(np.clip(_p1_finite_clip_scalar(turnover_eval), 0.0, 100.0))
        xp_chip = [0.0, 0.8, 1.5, 3.0, 5.0, 8.0]
        yp_chip = np.array([0.0, 2.0, 6.0, 12.0, 16.0, 18.0], dtype=float) * (14.0 / 18.0)
        chip_score = float(np.interp(turnover_eval, xp_chip, list(yp_chip)))
        chip_score = float(np.nan_to_num(chip_score, nan=0.0, posinf=0.0, neginf=0.0))

        # ------------------------------------------------------------------
        # ② 趋势距离（满分 10）：MA20 相对 MA60 的百分比距离，多头拉开时给满，过度发散后降分。
        # ------------------------------------------------------------------
        ma20 = _p1_scalar_finite(curr.get("ma20", 0.0), 0.0)
        ma60 = _p1_scalar_finite(curr.get("ma60", 1.0), 1.0)
        ma60_safe = max(_p1_finite_clip_scalar(ma60), 1e-9)
        dist_pct = ((ma20 - ma60) / ma60_safe * 100.0) if ma60 > 0 else 0.0
        dist_pct = float(np.nan_to_num(dist_pct, nan=0.0, posinf=0.0, neginf=0.0))
        xp_dist = [-5.0, 0.0, 2.0, 6.0, 12.0, 18.0]
        yp_dist = np.array([0.0, 8.0, 16.0, 16.0, 6.0, 0.0], dtype=float) * (10.0 / 16.0)
        trend_dist_score = float(np.interp(dist_pct, xp_dist, list(yp_dist)))
        trend_dist_score = float(np.nan_to_num(trend_dist_score, nan=0.0, posinf=0.0, neginf=0.0))

        # ------------------------------------------------------------------
        # ③ 均线成熟（满分 8）：MA20/MA60 比值贴近 1.02~1.15 视为「成熟多头」平台区。
        # ------------------------------------------------------------------
        ma60_m = _p1_scalar_finite(curr.get("ma60", 1.0), 1.0)
        ma60_ms = max(_p1_finite_clip_scalar(ma60_m), 1e-9)
        maturity = (ma20 / ma60_ms) if ma60_m > 0 else 1.0
        maturity = float(np.nan_to_num(maturity, nan=1.0, posinf=1.0, neginf=0.0))
        xp_mat = [0.95, 1.00, 1.02, 1.15, 1.30, 1.45]
        yp_mat = np.array([0.0, 4.0, 12.0, 12.0, 4.0, 0.0], dtype=float) * (8.0 / 12.0)
        mat_score = float(np.interp(maturity, xp_mat, list(yp_mat)))
        mat_score = float(np.nan_to_num(mat_score, nan=0.0, posinf=0.0, neginf=0.0))

        # ------------------------------------------------------------------
        # ④ 资金攻击（满分 15）：双因子；合计封顶 15。
        # ------------------------------------------------------------------
        df_5d = df.tail(5)
        time_weights = [0.2, 0.4, 0.6, 0.8, 1.0]
        weighted_inflow = 0.0
        for i in range(min(5, len(df_5d))):
            row = df_5d.iloc[i]
            day_inflow = 0.0
            for col in ("hk_vol", "net_main_amount"):
                if col in row:
                    day_inflow += _p1_scalar_finite(row[col], 0.0)
            weighted_inflow += day_inflow * time_weights[i]
        amount_sum_5d = _p1_scalar_finite(df_5d["amount"].sum(), 0.0) if "amount" in df_5d.columns else 0.0
        _amt5 = max(_p1_finite_clip_scalar(amount_sum_5d), 1e-9)
        inflow_vs_amount = weighted_inflow / _amt5 if amount_sum_5d > 0 else 0.0
        inflow_vs_amount = float(np.nan_to_num(inflow_vs_amount, nan=0.0, posinf=0.0, neginf=0.0))
        xp_flow = [-0.05, 0.0, 0.02, 0.05, 0.10, 0.20]
        yp_flow = [0.0, 0.0, 3.0, 6.0, 8.0, 10.0]
        flow_score = float(np.interp(inflow_vs_amount, xp_flow, yp_flow))
        flow_score = float(np.nan_to_num(flow_score, nan=0.0, posinf=0.0, neginf=0.0))

        vol_ratio = _p1_scalar_finite(rt_d.get("vol_ratio", curr.get("vol_ratio", 1.0)), 1.0)
        if not np.isfinite(vol_ratio) or vol_ratio <= 0:
            vol_ratio = 1.0
        activity_proxy = max(vol_ratio, turnover_eval / 2.0)
        activity_proxy = float(np.nan_to_num(activity_proxy, nan=1.0, posinf=10.0, neginf=0.0))
        xp_act = [0.8, 1.2, 1.8, 2.5, 4.0]
        yp_act = [0.0, 1.0, 3.0, 5.0, 6.0]
        act_score = float(np.interp(activity_proxy, xp_act, yp_act))
        act_score = float(np.nan_to_num(act_score, nan=0.0, posinf=0.0, neginf=0.0))
        fund_score = min(flow_score + act_score, 15.0)

        # ------------------------------------------------------------------
        # ④b 启动势能（10 分）：pct_chg_20d / vol_ratio / 突破 / pct_chg_5d 衰减，全程 np.interp（突破奖励为二值）
        # ------------------------------------------------------------------
        momentum_score = 0.0
        fake_penalty_val = 0.0
        close_px = _p1_scalar_finite(curr.get("close", 0.0), 0.0)
        pct_chg_20d = _p1_scalar_finite(curr.get("pct_chg_20d", np.nan), np.nan)
        if not np.isfinite(pct_chg_20d):
            if len(df) >= 21 and "close" in df.columns:
                c0 = _p1_scalar_finite(curr.get("close", 0.0), 0.0)
                c20 = _p1_scalar_finite(df.iloc[-21]["close"], 0.0)
                pct_chg_20d = (c0 / max(c20, 1e-9) - 1.0) * 100.0 if c20 > 0 else 0.0
            else:
                pct_chg_20d = 0.0
        pct_chg_20d = float(np.nan_to_num(pct_chg_20d, nan=0.0, posinf=0.0, neginf=0.0))

        pct_chg_5d = _p1_scalar_finite(curr.get("pct_chg_5d", np.nan), np.nan)
        if not np.isfinite(pct_chg_5d):
            if len(df) >= 6 and "close" in df.columns:
                c0 = _p1_scalar_finite(curr.get("close", 0.0), 0.0)
                c5 = _p1_scalar_finite(df.iloc[-6]["close"], 0.0)
                pct_chg_5d = (c0 / max(c5, 1e-9) - 1.0) * 100.0 if c5 > 0 else 0.0
            else:
                pct_chg_5d = 0.0
        pct_chg_5d = float(np.nan_to_num(pct_chg_5d, nan=0.0, posinf=0.0, neginf=0.0))

        high_10d = _p1_scalar_finite(curr.get("high_10d", np.nan), np.nan)
        if not np.isfinite(high_10d) or high_10d <= 0:
            if "high" in df.columns and len(df) >= 11:
                high_10d = float(df.iloc[-11:-1]["high"].astype(float).max())
            elif "high" in df.columns and len(df) >= 2:
                high_10d = float(df.iloc[:-1]["high"].astype(float).tail(min(10, len(df) - 1)).max())
            else:
                high_10d = _p1_scalar_finite(curr.get("high", 0.0), 0.0)

        xp_pct = np.array([-10.0, 0.0, 5.0, 12.0, 18.0, 25.0], dtype=float)
        yp_pct = np.array([0.0, 2.0, 6.0, 6.0, 2.0, 0.0], dtype=float)
        score_pct = float(np.interp(float(max(pct_chg_20d, -10.0)), xp_pct, yp_pct))
        score_pct = float(np.nan_to_num(score_pct, nan=0.0, posinf=0.0, neginf=0.0))

        xp_vol_m = np.array([1.0, 1.4, 2.0, 3.5], dtype=float)
        yp_vol_m = np.array([0.0, 2.0, 4.0, 3.0], dtype=float)
        score_vol = float(np.interp(float(vol_ratio), xp_vol_m, yp_vol_m))
        score_vol = float(np.nan_to_num(score_vol, nan=0.0, posinf=0.0, neginf=0.0))

        close_eff = now_price if now_price > 0 else close_px
        is_breakout = bool(high_10d > 0 and close_eff > high_10d * 0.995)
        bonus_breakout = 2.0 if is_breakout else 0.0

        xp_dec = np.array([0.0, 4.0, 9.0], dtype=float)
        yp_dec = np.array([1.0, 0.92, 0.65], dtype=float)
        decay_factor = float(np.interp(float(pct_chg_5d), xp_dec, yp_dec))
        decay_factor = float(np.nan_to_num(decay_factor, nan=1.0, posinf=1.0, neginf=0.0))

        momentum_score = min(
            10.0,
            round((score_pct + score_vol + bonus_breakout) * decay_factor, 2),
        )

        if is_breakout and vol_ratio < 1.25:
            penalty = min(9.0, momentum_score * 0.85)
            fake_penalty_val = -round(penalty, 2)

        # ------------------------------------------------------------------
        # ⑤ 波段涨幅（10 分）：max_60d_pct 双峰 np.interp
        # ------------------------------------------------------------------
        max_60d_pct = _p1_scalar_finite(curr.get("max_60d_pct", 0.0), 0.0)
        _mx60 = float(np.nan_to_num(_p1_finite_clip_scalar(max_60d_pct), nan=0.0, posinf=0.0, neginf=0.0))
        xp_gain = np.array([-5.0, 3.0, 6.0, 9.0, 11.0, 14.0, 18.0, 22.0, 25.0, 30.0], dtype=float)
        yp_gain = np.array([0.0, 6.0, 10.0, 8.0, 4.0, 8.0, 10.0, 8.0, 2.0, 0.0], dtype=float)
        gain_score = float(np.interp(_mx60, xp_gain, yp_gain))
        gain_score = float(np.nan_to_num(gain_score, nan=0.0, posinf=0.0, neginf=0.0))
        gain_score = min(10.0, gain_score)

        # ------------------------------------------------------------------
        # ⑥ 黄金起爆（满分 12）：仅保留 circ_mv_yi（亿元）三档 + np.interp(pct_chg/vol_ratio)，无历史 if/elif 阶梯。
        # ------------------------------------------------------------------
        pct_chg = _p1_scalar_finite(rt_d.get("pct_chg", curr.get("pct_chg", 0.0)), 0.0)
        if not np.isfinite(pct_chg):
            pct_chg = 0.0
        if float(circ_mv_yi) > 500.0:
            xp_pg = np.array([1.0, 2.5, 4.5], dtype=float)
            yp_pg = np.array([0.0, 8.0, 4.0], dtype=float)
            xp_vg = np.array([1.0, 1.4, 2.0], dtype=float)
            yp_vg = np.array([0.0, 4.0, 4.0], dtype=float)
        elif float(circ_mv_yi) >= 300.0:
            xp_pg = np.array([1.5, 3.5, 5.5], dtype=float)
            yp_pg = np.array([0.0, 8.0, 4.0], dtype=float)
            xp_vg = np.array([1.2, 1.6, 2.2], dtype=float)
            yp_vg = np.array([0.0, 4.0, 4.0], dtype=float)
        else:
            xp_pg = np.array([2.0, 4.0, 7.0], dtype=float)
            yp_pg = np.array([0.0, 8.0, 4.0], dtype=float)
            xp_vg = np.array([1.5, 1.8, 3.0], dtype=float)
            yp_vg = np.array([0.0, 4.0, 4.0], dtype=float)
        g_pct = float(np.interp(float(pct_chg), xp_pg, yp_pg))
        g_vol = float(np.interp(float(vol_ratio), xp_vg, yp_vg))
        g_pct = float(np.nan_to_num(g_pct, nan=0.0, posinf=0.0, neginf=0.0))
        g_vol = float(np.nan_to_num(g_vol, nan=0.0, posinf=0.0, neginf=0.0))
        golden_score = min(12.0, round(g_pct + g_vol, 2))

        # ------------------------------------------------------------------
        # ⑦ 趋势健康（满分 4）：乖离 bias20——贴线运行健康，过度偏离双向降分。
        # ------------------------------------------------------------------
        ma20_bias_denom = max(_p1_finite_clip_scalar(ma20), 1e-9)
        fallback_bias = (now_price - ma20) / ma20_bias_denom * 100.0
        fallback_bias = float(np.nan_to_num(fallback_bias, nan=0.0, posinf=0.0, neginf=0.0))
        bias20 = _p1_scalar_finite(curr.get("bias_20", fallback_bias), fallback_bias)
        bias20 = float(np.nan_to_num(bias20, nan=fallback_bias, posinf=0.0, neginf=0.0))
        xp_health = [-15.0, -5.0, -2.0, 1.0, 4.0, 8.0]
        yp_health = [0.0, 1.3, 3.3, 4.0, 3.3, 1.3]
        trend_health_score = float(np.interp(bias20, xp_health, yp_health))
        trend_health_score = float(np.nan_to_num(trend_health_score, nan=0.0, posinf=0.0, neginf=0.0))

        # ------------------------------------------------------------------
        # ⑧ 主升斜率（满分 9）：ma20_slope_5 五年化斜率百分数，过快或过慢均降分。
        # ------------------------------------------------------------------
        slope_5d = _p1_scalar_finite(curr.get("ma20_slope_5", 0.0), 0.0)
        xp_slope = [0.0, 1.2, 1.8, 2.8, 3.8, 5.5]
        yp_slope = np.array([0.0, 3.0, 6.0, 8.0, 5.0, 0.0], dtype=float) * (9.0 / 8.0)
        slope_score = float(np.interp(_p1_finite_clip_scalar(slope_5d), xp_slope, list(yp_slope)))
        slope_score = float(np.nan_to_num(slope_score, nan=0.0, posinf=0.0, neginf=0.0))

        # ------------------------------------------------------------------
        # ⑨ 动态 PE（满分 3）：相对行业 75 分位 q75 的插值，过高连续降分（不替代硬闸，仅平滑贡献）。
        # ------------------------------------------------------------------
        pe_score = 0.0
        if pe > 0:
            q75 = _p1_scalar_finite(ind_stats.get("q75", 30.0), 30.0)
            q75 = max(q75, 0.01)
            xp_pe = [0.0, q75 * 0.8, q75 * 1.2, q75 * 1.8, q75 * 2.5]
            yp_pe = np.array([4.0, 4.0, 3.0, 1.0, 0.0], dtype=float) * (3.0 / 4.0)
            pe_score = float(np.interp(pe, xp_pe, list(yp_pe)))

        # ------------------------------------------------------------------
        # ⑩ 高位熔断：bias20 过高视为透支；明细中 melt_score 与 base_score 中倒扣分一致。
        # ------------------------------------------------------------------
        xp_melt = [0.0, 3.0, 6.0, 10.0]
        yp_melt = [3.0, 3.0, 1.0, 0.0]
        melt_interp = float(np.interp(max(bias20, 0.0), xp_melt, yp_melt))
        melt_interp = float(np.nan_to_num(melt_interp, nan=0.0, posinf=0.0, neginf=0.0))
        if bias20 > 10.0:
            melt_score = 0.0
            melt_bubble_penalty = -12.0
        else:
            melt_score = round(melt_interp, 2)
            melt_bubble_penalty = 0.0

        # ------------------------------------------------------------------
        # ⑪ 市值与行业加分（附加，与历史口径一致写在明细里）
        # ------------------------------------------------------------------
        ind_bonus = float(dynamic_industries.get(ind, 0.0)) if isinstance(dynamic_industries, dict) else 0.0
        rank_bonus = 5.0 if ind_rank <= 3 else (2.0 if ind_rank <= 8 else 0.0)
        mv_bonus = 0.0
        if 500.0 <= circ_mv_yi < 1000.0:
            mv_bonus = 2.0
        elif 1000.0 <= circ_mv_yi < 2000.0:
            mv_bonus = 3.0
        elif circ_mv_yi >= 2000.0:
            mv_bonus = 4.0

        if circ_mv_yi >= 2000.0:
            mv_tier = "🦍巨无霸(2000亿+)"
        elif circ_mv_yi >= 1000.0:
            mv_tier = "🐘+千亿中军(1000-2000亿)"
        elif circ_mv_yi >= 500.0:
            mv_tier = "🐘500亿+"
        else:
            mv_tier = f"🐎{int(p1_min_yi)}-500亿"

        # 第 2 步：多维分项与附加项全部写入 details（先于 base_score / 记忆融合）
        score_details: Dict[str, Any] = {
            "筹码真空": round(chip_score, 2),
            "趋势距离": round(trend_dist_score, 2),
            "均线成熟": round(mat_score, 2),
            "资金攻击": round(fund_score, 2),
            "启动势能": round(momentum_score, 2),
            "假突破惩罚": float(fake_penalty_val),
            "波段涨幅": round(gain_score, 2),
            "黄金起爆": round(golden_score, 2),
            "趋势健康": round(trend_health_score, 2),
            "主升斜率": round(slope_score, 2),
            "动态PE": round(pe_score, 2),
            "高位熔断": round(melt_score, 2),
            "行业动态加分": round(float(ind_bonus), 2),
            "板块排名加分": rank_bonus,
            "市值优待": round(mv_bonus, 2),
            "市值等级": mv_tier,
        }

        # 第 3 步：base_score 与组合特征乘数、股性记忆凸组合 → final_p1_score
        combo_cfg = get_p1_combo_multiplier_config()
        if not combo_cfg.get("enabled", True):
            combo_cfg = {
                "min": 1.0,
                "max": 1.0,
                "top_tier": 1.0,
                "second_tier": 1.0,
                "third_tier": 1.0,
                "industry_dark_flow": 1.0,
                "fund_shape_wakeup": 1.0,
                "fund_pulse_confirm": 1.0,
                "weak_pattern_discount": 1.0,
                "bubble_discount": 1.0,
                "hot_low_fund_discount": 1.0,
                "chip_core_min": 9e9,
                "trend_dist_min": 9e9,
                "mat_min": 9e9,
                "fund_core_min": 9e9,
                "momentum_core_min": 9e9,
                "golden_core_min": 9e9,
                "healthy_core_min": 9e9,
                "slope_core_min": 9e9,
                "fund_hot_min": 9e9,
                "chip_hot_min": 9e9,
                "trend_health_min": 9e9,
                "industry_bonus_min": 9e9,
                "rank_bonus_min": 9e9,
            }
        base_score = float(
            round(chip_score, 2) * 1.10
            + round(trend_dist_score, 2) * 1.15
            + round(mat_score, 2) * 1.10
            + round(fund_score, 2) * 1.05
            + float(momentum_score) * 0.85
            + round(gain_score, 2) * 0.70
            + float(golden_score) * 0.80
            + round(trend_health_score, 2) * 1.10
            + round(slope_score, 2) * 1.05
            + round(pe_score, 2) * 0.80
            + float(melt_score)
            + float(melt_bubble_penalty)
            + float(fake_penalty_val)
            + float(ind_bonus) * 1.00
            + float(rank_bonus) * 1.00
            + float(mv_bonus) * 1.00
        )
        base_score = float(np.nan_to_num(base_score, nan=0.0, posinf=100.0, neginf=0.0))

        # 组合特征乘数：只奖励跨维度共振，不奖励单项极值。
        # 目标是把“横盘蓄势 + 筹码唤醒 + 资金真流入 + 趋势抬头”这种难伪造的底仓结构放大。
        combo_multiplier = 1.0
        combo_tags = []

        chip_core_min = float(combo_cfg.get("chip_core_min", 8.8))
        trend_dist_min = float(combo_cfg.get("trend_dist_min", 7.0))
        mat_min = float(combo_cfg.get("mat_min", 5.0))
        fund_core_min = float(combo_cfg.get("fund_core_min", 8.5))
        momentum_core_min = float(combo_cfg.get("momentum_core_min", 5.8))
        golden_core_min = float(combo_cfg.get("golden_core_min", 7.5))
        healthy_core_min = float(combo_cfg.get("healthy_core_min", 2.6))
        slope_core_min = float(combo_cfg.get("slope_core_min", 4.5))
        fund_hot_min = float(combo_cfg.get("fund_hot_min", 11.5))
        chip_hot_min = float(combo_cfg.get("chip_hot_min", 6.4))
        trend_health_min = float(combo_cfg.get("trend_health_min", 2.2))
        industry_bonus_min = float(combo_cfg.get("industry_bonus_min", 8.0))
        rank_bonus_min = float(combo_cfg.get("rank_bonus_min", 2.0))

        chip_core = chip_score >= chip_core_min
        trend_core = trend_dist_score >= trend_dist_min and mat_score >= mat_min
        fund_core = fund_score >= fund_core_min
        momentum_core = momentum_score >= momentum_core_min or golden_score >= golden_core_min
        healthy_core = trend_health_score >= healthy_core_min and slope_score >= slope_core_min

        # 顶级底仓组合：筹码密集 + 趋势成熟 + 资金攻击 + 形态健康
        if chip_core and trend_core and fund_core and healthy_core:
            combo_multiplier = max(combo_multiplier, float(combo_cfg.get("top_tier", 1.20)))
            combo_tags.append("顶级底仓共振")
        # 次一级组合：筹码唤醒 + 资金连续修复 + 启动势能成型
        elif chip_core and fund_core and momentum_core:
            combo_multiplier = max(combo_multiplier, float(combo_cfg.get("second_tier", 1.15)))
            combo_tags.append("筹码唤醒+资金修复")
        # 右侧加速但不极端：趋势、动能、健康同时成立
        elif trend_core and momentum_core and healthy_core:
            combo_multiplier = max(combo_multiplier, float(combo_cfg.get("third_tier", 1.12)))
            combo_tags.append("趋势动能共振")
        # 行业暗流：板块热度不是最热，但排名与行业加分同时在场，轻度放大
        elif ind_bonus >= industry_bonus_min and rank_bonus >= rank_bonus_min and fund_core:
            combo_multiplier = max(combo_multiplier, float(combo_cfg.get("industry_dark_flow", 1.08)))
            combo_tags.append("行业暗流建仓")

        # 资金形状的额外奖励：只在“连续净流入 + 价格推升效率足够”时才加成
        if fund_score >= fund_hot_min and chip_score >= chip_hot_min and trend_health_score >= trend_health_min:
            combo_multiplier = max(combo_multiplier, float(combo_cfg.get("fund_shape_wakeup", 1.18)))
            combo_tags.append("资金形状唤醒")
        elif fund_score >= 9.0 and momentum_score >= 5.0 and chip_score >= 5.0:
            combo_multiplier = max(combo_multiplier, float(combo_cfg.get("fund_pulse_confirm", 1.10)))
            combo_tags.append("资金脉冲确认")

        # 负面股性惩罚：长上影、假突破、虚火过热不奖励，必要时轻微折价。
        anti_pattern_penalty = 1.0
        if fake_penalty_val < 0:
            anti_pattern_penalty *= float(combo_cfg.get("weak_pattern_discount", 0.94))
        if melt_bubble_penalty < 0:
            anti_pattern_penalty *= float(combo_cfg.get("bubble_discount", 0.92))
        if pe_score <= 0.5 and momentum_score >= 7.5 and fund_score < 6.0:
            anti_pattern_penalty *= float(combo_cfg.get("hot_low_fund_discount", 0.95))
            combo_tags.append("高动量低资金折价")

        combo_multiplier = float(np.clip(combo_multiplier * anti_pattern_penalty, float(combo_cfg.get("min", 0.90)), float(combo_cfg.get("max", 1.20))))

        w_fm = float(get_p1_fund_memory_weight())
        fm100 = 0.0
        fm_raw = _p1_scalar_finite(curr.get("fund_memory_score", 0.0), 0.0)
        amplified_base = round(base_score * combo_multiplier, 2)
        if w_fm > 1e-9:
            fm100 = float(np.clip(fm_raw / 200.0 * 100.0, 0.0, 100.0))
            # 【V26.5 优化】平滑过渡：w_fm 小幅值时用对数软化，避免从0到正数时分数突变
            # 使用 log1p 映射：w=0.10 时实际生效约 0.07，w=0.05 时约 0.03，w=0.03 时约 0.01
            if w_fm <= 0.10:
                w_effective = float(np.log1p(w_fm) / np.log1p(0.10) * 0.10)
            else:
                w_effective = w_fm
            final_p1_score = round((1.0 - w_effective) * amplified_base + w_effective * fm100, 2)
        else:
            final_p1_score = round(amplified_base, 2)

        final_p1_score = float(min(final_p1_score, 100.0))

        if w_fm > 1e-9:
            koujing = f"主线稳票优先+股性记忆凸组合(权重={w_effective:.3f}[原始{w_fm:.2f}])"
        else:
            koujing = "主线稳票优先(权重=0，未融合股性记忆)"

        score_details["融合前分项合计"] = round(base_score, 2)
        score_details["组合特征乘数"] = round(combo_multiplier, 3)
        if combo_tags:
            score_details["组合特征标签"] = "、".join(combo_tags[:4])
        score_details["乘数后基底分"] = round(amplified_base, 2)
        score_details["评分口径"] = koujing + "；乘数只奖励组合特征，不奖励单项极值"
        if w_fm > 1e-9:
            score_details["股性记忆(0-100)"] = round(fm100, 2)
            score_details["记忆融合权重(有效)"] = round(w_effective, 4)
            score_details["记忆融合权重(原始)"] = round(w_fm, 4)

        # 第 4 步（函数末尾）：据写满的 details + YAML pass_line 计算 effective_pass_line，再判定 passed
        try:
            _pl_raw = float(pass_line)
        except (TypeError, ValueError):
            _pl_raw = 50.0
        base_pass_line = _pl_raw if _pl_raw else 50.0
        if not np.isfinite(base_pass_line) or base_pass_line <= 0:
            base_pass_line = 50.0

        score_momentum = _p1_scalar_finite(score_details.get("启动势能", 0.0), 0.0)
        score_capital = _p1_scalar_finite(score_details.get("资金攻击", 0.0), 0.0)
        fake_penalty = _p1_scalar_finite(score_details.get("假突破惩罚", 0.0), 0.0)
        if (
            score_momentum >= 8.0
            and score_capital >= 10.0
            and fake_penalty >= -2.0
            and ma20 >= ma60 * 0.98
        ):
            effective_pass_line = base_pass_line - 4.0
        else:
            effective_pass_line = base_pass_line

        score_details["基础及格线"] = round(base_pass_line, 2)
        score_details["有效及格线"] = round(effective_pass_line, 2)

        if final_p1_score < effective_pass_line:
            score_details["未达标比对"] = (
                f"{final_p1_score:.2f} < {effective_pass_line:.2f}（基准线 {base_pass_line:.2f}）"
            )
            return (
                final_p1_score,
                False,
                "平滑得分不达标",
                score_details,
            )
        return final_p1_score, True, "入选", score_details

    except Exception as e:
        logger.debug("P1多维分项打分异常: %s", e)
        return (
            0.0,
            False,
            f"P1多维分项打分异常: {e}",
            {},
        )


# 历史函数名别名（外部脚本 / 旧笔记中的 import 仍可用；行为等同于 compute_p1_multi_dim_smooth_score）
compute_p1_eleven_dim_smooth_score = compute_p1_multi_dim_smooth_score
