# -*- coding: utf-8 -*-
"""
界面展示：档位中文名、英文列名→中文、表格居中样式（不改库表/API 字段，仅展示层）。
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd

# 居中表格中按两位小数展示的「得分/分位」列（含阵亡诊断、扫描池、实验室等）
_SCORE_DISPLAY_COLUMNS: frozenset = frozenset({
    "当前得分",
    "综合分",
    "p1_score",
    "最高分",
    "建议最低分",
    "score",
    "max_score",
    "quality_score",
    "burst_score",
    "surge_bonus",
    "penalty",
    "min_pass",
    "suggested_min_entry_score",
    "爆量分",
    "抢筹分",
    "筹码分",
    "性格动量",
    "绞杀分",
    "时权抢筹",
    "异动分",
    "量比映射分",
    "param_val",  # 实验室参数扫描数值
    "max_p1_score",
})

# ---------- 档位（全站展示统一用「N档·语义」）----------
POOL_KEY_CN: Dict[str, str] = {
    "p1": "底仓池",
    "p2": "竞价池",
    "p3": "盘中池",
    "p4": "盘尾池",
    "p5": "盘后池",
    "golden_burst": "黄金起爆专项",
}

P1_PROFILE_CN: Dict[str, str] = {
    "strict": "严格档",
    "neutral": "中性档",
    "relaxed": "宽松档",
}


def pool_cn(pool_key: Optional[str]) -> str:
    k = str(pool_key or "").strip().lower()
    return POOL_KEY_CN.get(k, str(pool_key or ""))


def p1_profile_cn(name: Optional[str]) -> str:
    return P1_PROFILE_CN.get(str(name or "").strip().lower(), str(name or ""))


SIGNAL_LOG_COLUMNS: Dict[str, str] = {
    "trade_date": "交易日期",
    "ts_code": "证券代码",
    "name": "名称",
    "pool": "触发档位",
    "strategy": "战法",
    "score": "得分",
    "regime": "市场环境",
    "suggest_pos": "建议仓位",
    "created_at": "记录时间",
}

LAB_SWEEP_RESULT_COLUMNS: Dict[str, str] = {
    "param_val": "参数值",
    "pass_count": "入池数",
    "max_score": "最高分",
}


def rename_columns_for_display(df: pd.DataFrame, mapping: Mapping[str, str]) -> pd.DataFrame:
    ren = {c: mapping[c] for c in df.columns if c in mapping}
    return df.rename(columns=ren) if ren else df


def _coerce_score_cell_two_decimals(val: Any) -> Any:
    """得分列：有限数值保留两位小数；占位符与非数保持原样。"""
    if val is None:
        return val
    if isinstance(val, str) and val.strip() in ("--", "—", ""):
        return val
    if isinstance(val, (float, np.floating)) and (np.isnan(val) or np.isinf(val)):
        return val
    try:
        v = float(val)
        if not np.isfinite(v):
            return val
        return round(v, 2)
    except (TypeError, ValueError):
        return val


def style_dataframe_center(df: pd.DataFrame) -> Any:
    """表头与单元格水平居中；得分相关列统一两位小数（配合 st.dataframe）。"""
    styles = [
        {"selector": "th", "props": [("text-align", "center")]},
        {"selector": "td", "props": [("text-align", "center")]},
    ]
    try:
        df2 = df.copy()
        fmt: Dict[str, str] = {}
        for col in df2.columns:
            if col not in _SCORE_DISPLAY_COLUMNS:
                continue
            df2[col] = df2[col].map(_coerce_score_cell_two_decimals)
            if pd.api.types.is_numeric_dtype(df2[col]):
                fmt[col] = "{:.2f}"
        sty = df2.style.set_table_styles(styles)
        if fmt:
            sty = sty.format(**fmt, na_rep="—")
        return sty
    except Exception:
        return df
