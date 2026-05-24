# -*- coding: utf-8 -*-
"""
股性记忆池 / 资金活跃度记忆（daily_data.fund_memory_score，0~200）

【V26.5 新增资金记忆体系 · 第三阶段】日线半衰期状态机列；供 P1 最终打分融合（见 pool_manager、constants），
与 P4/P5 右侧量价扫描路径解耦（扫描引擎不读取本列作硬闸）。

================================================================================
一、业务语义（自然语言说明，供产品/量化/运维对齐）
================================================================================
本字段刻画「大票上、有历史放量痕迹的资金异动记忆」：当标的曾在满足市值门槛时
出现涨停级走势或天量换手，则注入能量；能量随**交易日**推进按指数规律衰减；
若衰减过程中再次出现同类异动，则在当前记忆值上「充值」+100，但总分封顶 200。

半衰期：由 constants.FUND_MEMORY_HALF_LIFE_DAYS 配置（默认 **21 个交易日**），按**指数衰减**语义实现。
离散实现：相邻两个**已落库的交易日**之间，记忆状态乘以固定因子 decay = 0.5^(1/T_half)（等价于每 T_half 步衰减一半）。

双重噪音过滤（输出层）：
1) 规模闸：仅当**当日**流通市值 ≥ 100 亿元人民币时，才允许输出非零记忆分；
   否则当日输出强制为 0（内部状态仍继续衰减与充值，见下）。
2) 历史异动闸：仅当**截至当日**的最近 60 个交易日内，至少出现过一次「放量异动」，
   才允许输出非零；否则输出 0。

充值触发（仅当当日 circ_mv 已达 100 亿时才允许 +100，避免小票噪声污染状态机）：
- 涨停代理：limit_times≥1 或 pct_chg≥9.8%。
- 天量换手代理：turnover_rate_f≥15% 或 vol_ratio≥3.0。

60 日「放量异动」判定：vol_ratio≥2.0，或 vol ≥ 1.5×vol_ma20。

内部状态 vs 输出：
- 状态变量 state 在每条交易日记录上先做衰减，再按规则充值（上限 200）。
- 当日 fund_memory_score = state（若双重过滤通过）否则 0。

================================================================================
二、工程约束
================================================================================
- 仅依赖 pandas/numpy；按 ts_code 分组后在 NumPy 层做 O(交易日) 循环。
- 与 daily_data 其它列解耦；输出 Series 与输入 df 索引对齐。
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

try:
    import constants

    _HALF = int(getattr(constants, "FUND_MEMORY_HALF_LIFE_DAYS", 21))
except Exception:
    _HALF = 21

HALF_LIFE_TRADING_DAYS = max(1, _HALF)
DECAY_PER_TRADING_DAY = math.pow(0.5, 1.0 / float(HALF_LIFE_TRADING_DAYS))

CIRC_MV_WAN_MIN = 1_000_000.0
RECHARGE_POINTS = 100.0
SCORE_CAP = 200.0
PCT_LIMIT_MAIN = 9.8
TURNOVER_F_HEAVY = 15.0
VOL_RATIO_HEAVY = 3.0
VOL_RATIO_SPIKE = 2.0
VOL_TO_MA20_SPIKE_RATIO = 1.5
ROLL_SPIKE_DAYS = 60


def _safe_f(s: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default)


def compute_fund_memory_score(
    df: pd.DataFrame,
    *,
    ts_code_col: str = "ts_code",
    trade_date_col: str = "trade_date",
) -> pd.Series:
    """
    输入：全市场或子集日线长表；至少含 ts_code, trade_date, circ_mv, vol, vol_ma20,
    vol_ratio, turnover_rate_f, pct_chg, limit_times（可缺，按 0）。

    输出：与 df 当前索引对齐的 float64 Series，取值 [0, 200]。
    【V26.5 新增资金记忆体系 · 第三阶段】
    """
    if df is None or df.empty:
        return pd.Series(dtype="float64")

    work = df.copy()
    if trade_date_col in work.columns:
        work["_td"] = pd.to_datetime(work[trade_date_col], errors="coerce")
    else:
        return pd.Series(0.0, index=df.index)

    for c in ("vol_ratio", "turnover_rate_f", "pct_chg", "limit_times", "circ_mv", "vol", "vol_ma20"):
        if c not in work.columns:
            work[c] = 0.0

    work["_circ"] = _safe_f(work["circ_mv"], 0.0)
    work["_vr"] = _safe_f(work["vol_ratio"], 0.0)
    work["_trf"] = _safe_f(work["turnover_rate_f"], 0.0)
    work["_pct"] = _safe_f(work["pct_chg"], 0.0)
    work["_lim"] = _safe_f(work["limit_times"], 0.0)
    work["_vol"] = _safe_f(work["vol"], 0.0)
    work["_vm20"] = _safe_f(work["vol_ma20"], 0.0)

    ts = work[ts_code_col].astype(str)
    spike_day = (work["_vr"] >= VOL_RATIO_SPIKE) | (
        (work["_vm20"] > 0) & (work["_vol"] >= VOL_TO_MA20_SPIKE_RATIO * work["_vm20"])
    )
    spike_day_vals = spike_day.astype(np.float64)
    work["_spike60"] = (
        spike_day_vals.groupby(ts)
        .transform(lambda s: s.rolling(ROLL_SPIKE_DAYS, min_periods=1).max() > 0.5)
        .astype(bool)
    )

    lim_hit = (work["_lim"] >= 1.0) | (work["_pct"] >= PCT_LIMIT_MAIN)
    heavy_hit = (work["_trf"] >= TURNOVER_F_HEAVY) | (work["_vr"] >= VOL_RATIO_HEAVY)
    event = (lim_hit | heavy_hit) & (work["_circ"] >= CIRC_MV_WAN_MIN)

    out = pd.Series(0.0, index=work.index, dtype="float64")
    decay = DECAY_PER_TRADING_DAY
    for _, sub in work.groupby(ts_code_col, sort=False):
        sub = sub.sort_values("_td")
        idx = sub.index.to_numpy()
        circ = sub["_circ"].to_numpy(dtype=np.float64)
        sp60 = sub["_spike60"].to_numpy(dtype=bool)
        ev = event.loc[sub.index].fillna(False).to_numpy(dtype=bool)

        n = len(sub)
        state = 0.0
        vals = np.zeros(n, dtype=np.float64)
        for i in range(n):
            state *= decay
            if ev[i]:
                state = min(SCORE_CAP, state + RECHARGE_POINTS)
            if circ[i] >= CIRC_MV_WAN_MIN and sp60[i]:
                vals[i] = state
            else:
                vals[i] = 0.0
        out.loc[idx] = vals

    out = out.reindex(df.index).fillna(0.0)
    return out.clip(lower=0.0, upper=SCORE_CAP)
