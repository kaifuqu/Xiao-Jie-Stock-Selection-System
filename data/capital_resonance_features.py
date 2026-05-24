# -*- coding: utf-8 -*-
"""
资金共振复合分（capital_resonance_score，0~100）

【V26.5 第一阶段·特征引擎】本列仍为 daily_data 中的单一 DOUBLE 列，**不增删表结构**；
仅变更向量化算法口径，与增量管道 _sync_daily_features_capital_resonance 配套。
【V26.5 新增资金记忆体系】与 fund_memory_score 并列，由 data_fetcher 夜间增量 `_sync_daily_features()` 同序维护；二者独立算法。P3–P5 动态分可加权本列；P1 底仓平滑分不读此列作硬闸。

设计总览（算术可加、可解释，禁止 Python 层按行 for 循环）：
- **固定底座 80 分** = 筹码单峰底座 50 分 + 主力资金底座 30 分。
- **可选加分项 20 分** = 两融融资近 5 日买入力度环比增速，在当日截面上 Rank 映射；无两融或窗口全零则该分项为 0，
  **不扣底座分**（不惩罚）。

分项说明（自然语言，便于产品/运维对齐）：
1) **筹码单峰底座（50 分）**  
   使用 cyq_concentration（筹码集中度，百分比口径）。以 65% 为起评分界线：低于 65% 得 0；
   65%~100% 之间做平滑线性映射到 0~50 分。实现上等价于 clip((conc-65)/35,0,1)*50，全程向量运算。

2) **主力资金底座（30 分）**  
   先对每个 ts_code 计算「近 5 个交易日 net_main_amount 之和」。该字段在生肉管道中已定义为
   （超大单+大单）合并净流入（元）。再除以当日流通市值（万元 ×10000 → 元），得到无量纲强度 ratio。
   游资极值会在**每个 trade_date 截面**上做轻度 **MAD 截断**：对 ratio 取截面中位数 med 与中位绝对偏差 MAD，
   将 ratio 限制在 [med - 3*MAD, med + 3*MAD]（MAD 为 0 时用极小正数兜底，避免除零/空窗）。
   截断后再在当日截面上做分位 Rank（pct=True），映射为 0~30 分。全程 groupby.transform / rank，无逐行循环。

3) **两融加分项（20 分）**  
   使用 rzmre（融资买入额，元）。对每个 ts_code：当前窗为近 5 日融资买入额之和 cur5，对比向前错位 5 日的上一窗和 prev5，
   计算环比增速 growth = (cur5-prev5)/(abs(prev5)+eps)。仅在「cur5 或 prev5 至少一侧为正」时参与排名并给分；
   否则（长期无两融、全 NaN/全零）该项记 **0 分**。有参与时，在 trade_date 截面上对 growth 做 Rank，映射 0~20 分。

最终：score = chip50 + main30 + margin20，再 clip 到 [0,100]。

本模块仅消费日线宽表字段，不向分时专用特征扩展。
"""
from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

_EPS = 1e-9


def _safe_numeric(
    s: pd.Series | np.ndarray | list | tuple | float | int | None,
    *,
    index: pd.Index | None = None,
    fill: float = 0.0,
) -> pd.Series:
    """
    Return a numeric Series aligned to `index`.
    Guard against scalar/None input (pd.to_numeric(None) -> numpy.float64),
    which would otherwise break chained `.fillna()` calls downstream.
    """
    if isinstance(s, pd.Series):
        out = pd.to_numeric(s, errors="coerce")
        if index is not None:
            out = out.reindex(index)
        return out.fillna(fill)

    if s is None:
        if index is not None:
            return pd.Series(fill, index=index, dtype="float64")
        return pd.Series(dtype="float64")

    if np.isscalar(s):
        if index is not None:
            return pd.Series(float(s), index=index, dtype="float64").fillna(fill)
        return pd.Series([float(s)], dtype="float64").fillna(fill)

    out = pd.to_numeric(pd.Series(s), errors="coerce")
    if index is not None:
        out = out.reindex(range(len(index)))
        out.index = index
    return out.fillna(fill)


def compute_capital_resonance_score(
    df: pd.DataFrame,
    *,
    ts_code_col: str = "ts_code",
    trade_date_col: str = "trade_date",
) -> pd.Series:
    """
    输入：已按 (ts_code, trade_date) 升序排列的全市场日线长表。
    必需/常用列：cyq_concentration, net_main_amount, circ_mv, rzmre（可缺则两融加分全 0）。

    输出：与 df.index 对齐的 float64 Series，取值 [0, 100]。
    【V26.5 新增资金记忆体系】与股性记忆列独立；本函数不读取 fund_memory_score。
    """
    if df is None or df.empty:
        return pd.Series(dtype="float64")

    idx = df.index
    ts = df[ts_code_col].astype(str)
    td = df[trade_date_col]

    # ---------- 底座 A：筹码单峰 50 分（65% 起评，线性铺到 50）----------
    conc = _safe_numeric(df.get("cyq_concentration"), index=idx, fill=0.0)
    chip50 = ((conc - 65.0) / 35.0).clip(lower=0.0, upper=1.0) * 50.0

    # ---------- 底座 B：主力资金 30 分（5 日主力净额 / 流通市值 + 截面 MAD 截断 + Rank）----------
    flow = _safe_numeric(df.get("net_main_amount"), index=idx, fill=0.0)
    sum5_flow = flow.groupby(ts, sort=False).transform(
        lambda s: s.rolling(5, min_periods=1).sum()
    )
    circ_mv_wan = _safe_numeric(df.get("circ_mv"), index=idx, fill=0.0)
    circ_yuan = circ_mv_wan * 10000.0 + _EPS
    ratio_raw = sum5_flow / circ_yuan

    med = ratio_raw.groupby(td, sort=False).transform("median")
    abs_dev = (ratio_raw - med).abs()
    mad = abs_dev.groupby(td, sort=False).transform("median")
    mad_eff = mad.clip(lower=_EPS)
    lo = med - 3.0 * mad_eff
    hi = med + 3.0 * mad_eff
    ratio_c = ratio_raw.clip(lower=lo, upper=hi)

    main_rank = ratio_c.groupby(td, sort=False).rank(pct=True, method="average")
    main_rank = main_rank.fillna(0.5)
    main30 = main_rank * 30.0

    # ---------- 加分 C：两融 20 分（5 日融资买入和环比增速 Rank；无数据则 0）----------
    if "rzmre" in df.columns:
        rz = _safe_numeric(df["rzmre"], index=idx, fill=0.0)
    else:
        rz = pd.Series(0.0, index=idx)

    cur5 = rz.groupby(ts, sort=False).transform(
        lambda s: s.rolling(5, min_periods=1).sum()
    )
    prev5 = cur5.groupby(ts, sort=False).shift(5)
    denom = prev5.abs() + _EPS
    growth = (cur5 - prev5) / denom
    growth = growth.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    eligible_m = (cur5.fillna(0.0) > _EPS) | (prev5.fillna(0.0) > _EPS)
    margin_rank = growth.groupby(td, sort=False).rank(pct=True, method="average")
    margin_rank = margin_rank.fillna(0.5)
    margin20 = (margin_rank * 20.0).where(eligible_m, 0.0)

    score = chip50 + main30 + margin20
    return score.clip(0.0, 100.0).astype("float64")


def describe_capital_resonance_schema() -> Tuple[str, ...]:
    """DuckDB / daily_data 列名（供文档与巡检）；与历史表结构兼容，不新增物理列名。"""
    return ("capital_resonance_score",)
