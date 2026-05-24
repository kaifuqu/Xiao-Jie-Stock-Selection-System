# -*- coding: utf-8 -*-
"""
痛点行情专项回测：内置时间段、流通市值带（万元）、周期键解析。

量纲约定（与全项目一致）：
- circ_mv：万元；100 亿元 = 1_000_000 万元；500 亿元 = 5_000_000 万元。

「健康主升浪」马/象监测带：100～500 亿元 → circ_mv_wan ∈ [CIRC_MV_WAN_MIN, CIRC_MV_WAN_MAX]。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Tuple, Union

import pandas as pd

# 流通市值带：100 亿～500 亿（万元）
CIRC_MV_WAN_MIN: float = 1_000_000.0
CIRC_MV_WAN_MAX: float = 5_000_000.0

# 内置痛点片段（键 → (start, end) 含首尾交易日语义，由回测按日历切片）
PAIN_PERIODS: Dict[str, Tuple[str, str]] = {
    # 科技主升浪
    "tech_rally": ("2024-10-08", "2024-11-20"),
    "20241008_20241120": ("2024-10-08", "2024-11-20"),
    # 大盘白马加速期
    "bluechip_accel": ("2025-03-05", "2025-04-15"),
    "20250305_20250415": ("2025-03-05", "2025-04-15"),
}


@dataclass(frozen=True)
class PainpointWindow:
    """解析后的痛点窗口（pandas 可比较日期）。"""

    key: str
    start: pd.Timestamp
    end: pd.Timestamp

    def contains(self, ts: Union[pd.Timestamp, datetime, str]) -> bool:
        t = pd.to_datetime(ts)
        return self.start.normalize() <= t.normalize() <= self.end.normalize()


def parse_pain_period(period: str) -> PainpointWindow:
    """
    将 --period 参数解析为 PainpointWindow。

    支持：
    - 预设键：tech_rally, bluechip_accel, 20241008_20241120, 20250305_20250415
    - 原始格式：YYYYMMDD_YYYYMMDD（下划线分隔）
    """
    s = (period or "").strip()
    if not s:
        raise ValueError("period 不能为空")

    if s in PAIN_PERIODS:
        a, b = PAIN_PERIODS[s]
        start = pd.to_datetime(a)
        end = pd.to_datetime(b)
        return PainpointWindow(key=s, start=start, end=end)

    if "_" in s and len(s) >= 17:
        parts = s.split("_", 1)
        if len(parts) == 2 and len(parts[0]) == 8 and len(parts[1]) == 8:
            start = pd.to_datetime(parts[0], format="%Y%m%d")
            end = pd.to_datetime(parts[1], format="%Y%m%d")
            return PainpointWindow(key=s, start=start, end=end)

    raise ValueError(
        f"无法解析 period={period!r}；请使用内置键（如 tech_rally）或 YYYYMMDD_YYYYMMDD"
    )


def circ_mv_in_horse_elephant_band(circ_mv_wan: float) -> bool:
    """是否落在 100～500 亿元（万元）监测带。"""
    try:
        x = float(circ_mv_wan)
    except (TypeError, ValueError):
        return False
    if x != x:  # nan
        return False
    return CIRC_MV_WAN_MIN <= x <= CIRC_MV_WAN_MAX
