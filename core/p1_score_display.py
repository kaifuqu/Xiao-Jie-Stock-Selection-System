# -*- coding: utf-8 -*-
"""P1 打分明细 → 满分项/最低项展示字符串（阵亡仓、策略实验室共用）。"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np

# 与 score_calibration.compute_p1_multi_dim_smooth_score 的 score_details 键顺序对齐（附加项接后）
P1_SCORE_DETAIL_DISPLAY_ORDER: Tuple[str, ...] = (
    "筹码真空",
    "趋势距离",
    "均线成熟",
    "资金攻击",
    "启动势能",
    "假突破惩罚",
    "波段涨幅",
    "黄金起爆",
    "趋势健康",
    "主升斜率",
    "动态PE",
    "高位熔断",
    "行业动态加分",
    "板块排名加分",
    "市值优待",
    "市值等级",
    "组合特征乘数",
    "组合特征标签",
    "乘数后基底分",
    "融合前分项合计",
    "股性记忆(0-100)",
    "记忆融合权重",
    "评分口径",
    "基础及格线",
    "有效及格线",
    "未达标比对",
)

# 与 score_calibration.compute_p1_multi_dim_smooth_score 各子项封顶一致（附加项见注释）
P1_DIM_FULL_MARK: Dict[str, str] = {
    "筹码真空": "14",
    "趋势距离": "10",
    "均线成熟": "8",
    "资金攻击": "15",
    "启动势能": "10",
    "假突破惩罚": "0",
    "波段涨幅": "10",
    "黄金起爆": "12",
    "趋势健康": "4",
    "主升斜率": "9",
    "动态PE": "3",
    # 明细行仅 melt 插值分；bias20>10 时融合前合计另含 -12（本行显示 0）
    "高位熔断": "3",
    "行业动态加分": "12",
    "板块排名加分": "5",
    "市值优待": "4",
    "市值等级": "-",
    "组合特征乘数": "0.90~1.20",
    "组合特征标签": "-",
    "乘数后基底分": "100",
    "融合前分项合计": "100",
    "股性记忆(0-100)": "100",
    "记忆融合权重": "<=0.30",
    "评分口径": "-",
    "基础及格线": "-",
    "有效及格线": "-",
    "未达标比对": "-",
}


P1_DIM_HINTS: Dict[str, str] = {
    "筹码真空": "真实换手及近5日均换手兜底后的平滑映射",
    "趋势距离": "MA20 相对 MA60 的距离（%）",
    "均线成熟": "MA20/MA60 比值成熟平台区",
    "资金攻击": "近5日加权主力+北向 vs 成交额，叠加量比/换手活跃度",
    "启动势能": "20日涨幅/量比/突破/5日拉升衰减",
    "假突破惩罚": "缩量假突破扣分（负值或0）",
    "波段涨幅": "60日涨幅双峰 max_60d_pct",
    "黄金起爆": "市值分层涨幅+量比 np.interp，封顶12",
    "趋势健康": "乖离 bias20 形态",
    "主升斜率": "ma20_slope_5",
    "动态PE": "相对行业 q75 分位",
    "高位熔断": "bias20 过高惩罚（可倒扣）",
    "行业动态加分": "高景气行业 dynamic_industries",
    "板块排名加分": "板块排名前8/前3",
    "市值优待": "500亿以上档位加分",
    "市值等级": "市值档位标签",
    "融合前分项合计": "正面维度合计+假突破惩罚+高位熔断倒扣（融合前）",
    "股性记忆(0-100)": "fund_memory_score 映射到 0~100",
    "记忆融合权重": "与多维分项凸组合权重 w（config/constants）",
    "评分口径": "算法版本说明",
    "基础及格线": "配置中 pass_line 对应的基准入围线",
    "有效及格线": "启动势能+资金攻击等共振达标时，可在基准线上降低 4 分",
    "未达标比对": "低于有效及格线时的数值对比",
}


def normalize_p1_score_details_for_display(score_details: Any) -> Dict[str, Any]:
    """
    将历史缓存中的英文键（base_score / base_pass_line / effective_pass_line）转为界面中文键；
    新引擎已直接写入中文键，此函数保证旧数据仍可正确展示。
    """
    if not isinstance(score_details, dict):
        return {}
    skip = frozenset({"base_score", "base_pass_line", "effective_pass_line", "融合前十一维小计"})
    out: Dict[str, Any] = {}
    for k, v in score_details.items():
        ks = str(k)
        if ks in skip:
            continue
        out[ks] = v
    if "融合前分项合计" not in out and "base_score" in score_details:
        out["融合前分项合计"] = score_details["base_score"]
    if "融合前分项合计" not in out and "融合前十一维小计" in score_details:
        out["融合前分项合计"] = score_details["融合前十一维小计"]
    if "基础及格线" not in out and "base_pass_line" in score_details:
        out["基础及格线"] = score_details["base_pass_line"]
    if "有效及格线" not in out and "effective_pass_line" in score_details:
        out["有效及格线"] = score_details["effective_pass_line"]
    return out


def score_details_json_safe(score_details: Any) -> Dict[str, Any]:
    """淘汰缓存 JSON 落盘用：仅保留可 JSON 序列化的标量；键名统一为中文展示。"""
    score_details = normalize_p1_score_details_for_display(score_details)
    if not isinstance(score_details, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in score_details.items():
        ks = str(k)
        if isinstance(v, (bool, str)):
            out[ks] = v
        elif isinstance(v, (int, float, np.integer, np.floating)):
            try:
                fv = float(v)
                if np.isfinite(fv):
                    out[ks] = round(fv, 4)
            except (TypeError, ValueError):
                out[ks] = str(v)
        else:
            out[ks] = str(v)
    return out


def _p1_detail_cell_str(val: Any) -> str:
    """Streamlit DataFrame→Arrow 要求列类型一致；得分列若混 float 与 str（如市值等级）会报错，故一律输出字符串。"""
    if isinstance(val, (int, float, np.integer, np.floating)):
        try:
            return str(round(float(val), 2))
        except (TypeError, ValueError):
            return str(val)
    return str(val)


def p1_score_details_to_rows(score_details: Any) -> List[Dict[str, Any]]:
    """转为主表行：维度、项满分、得分/内容、说明（供 Streamlit dataframe 展示）。"""
    score_details = normalize_p1_score_details_for_display(score_details)
    if not isinstance(score_details, dict):
        return []
    rows: List[Dict[str, Any]] = []
    seen = set()
    for key in P1_SCORE_DETAIL_DISPLAY_ORDER:
        if key not in score_details:
            continue
        seen.add(key)
        val = score_details[key]
        disp = _p1_detail_cell_str(val)
        full_mark = P1_DIM_FULL_MARK.get(key, "-")
        rows.append(
            {
                "维度": key,
                "项满分": full_mark,
                "得分或内容": disp,
                "说明": P1_DIM_HINTS.get(key, ""),
            }
        )
    for key, val in score_details.items():
        if key in seen:
            continue
        disp = _p1_detail_cell_str(val)
        full_mark = P1_DIM_FULL_MARK.get(str(key), "-")
        rows.append(
            {
                "维度": str(key),
                "项满分": full_mark,
                "得分或内容": disp,
                "说明": "",
            }
        )
    return rows


def p1_score_details_to_extreme_labels(score_details: Any, score: float) -> Tuple[str, str, str, str]:
    """
    从 P1 打分明细提取两项最高、两项最低（仅数值维），格式与阵亡仓诊断表「满分项/最低项」一致。
    """
    score_details = normalize_p1_score_details_for_display(score_details)
    top_1 = top_2 = worst_1 = worst_2 = "--"
    try:
        sc = float(score or 0.0)
    except (TypeError, ValueError):
        sc = 0.0
    if not isinstance(score_details, dict) or sc <= 0:
        return top_1, top_2, worst_1, worst_2
    numeric_items = []
    for k, v in score_details.items():
        try:
            if isinstance(v, (int, float, np.number)):
                numeric_items.append((k, float(v)))
        except Exception:
            continue
    if not numeric_items:
        return top_1, top_2, worst_1, worst_2
    sorted_scores_asc = sorted(numeric_items, key=lambda x: x[1])
    if len(sorted_scores_asc) >= 1:
        worst_1 = f"{sorted_scores_asc[0][0]}({sorted_scores_asc[0][1]:.2f})"
    if len(sorted_scores_asc) >= 2:
        worst_2 = f"{sorted_scores_asc[1][0]}({sorted_scores_asc[1][1]:.2f})"
    sorted_scores_desc = sorted(numeric_items, key=lambda x: x[1], reverse=True)
    if len(sorted_scores_desc) >= 1:
        top_1 = f"{sorted_scores_desc[0][0]}({sorted_scores_desc[0][1]:.2f})"
    if len(sorted_scores_desc) >= 2:
        top_2 = f"{sorted_scores_desc[1][0]}({sorted_scores_desc[1][1]:.2f})"
    return top_1, top_2, worst_1, worst_2
