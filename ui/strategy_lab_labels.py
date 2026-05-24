# -*- coding: utf-8 -*-
"""
策略实验室：参数中文名、一句话释义、与默认值一致的允许区间、自适应步长。
"""
from __future__ import annotations

import math
from typing import Any, Dict, Tuple


def int_to_zh_num(n: int) -> str:
    """非负整数转中文数字（用于标签中的默认値与区间端点）。"""
    if n < 0:
        return "负" + int_to_zh_num(-n)
    if n == 0:
        return "零"
    _d = "零一二三四五六七八九"

    def _below_wan(num: int) -> str:
        if num == 0:
            return ""
        if num < 10:
            return _d[num]
        if num < 20:
            return "十" + (_d[num % 10] if num > 10 else "")
        if num < 100:
            t, o = divmod(num, 10)
            return _d[t] + "十" + (_d[o] if o else "")
        if num < 1000:
            h, r = divmod(num, 100)
            seg = _d[h] + "百"
            if r == 0:
                return seg
            if r < 10:
                return seg + _d[r]
            return seg + _below_wan(r)
        th, r = divmod(num, 1000)
        seg = _d[th] + "千"
        if r == 0:
            return seg
        if r < 100:
            return seg + "零" + _below_wan(r)
        return seg + _below_wan(r)

    parts = []
    yi, n = divmod(n, 100_000_000)
    if yi:
        parts.append(_below_wan(yi) + "亿")
    wan, rest = divmod(n, 10000)
    if wan:
        parts.append(_below_wan(wan) + "万")
    if rest:
        if wan and rest < 1000:
            parts.append("零" + _below_wan(rest))
        elif not wan:
            parts.append(_below_wan(rest))
        else:
            parts.append(_below_wan(rest))
    if not parts:
        return "零"
    return "".join(parts)


def _key_is_circ_mv_wan(key: str) -> bool:
    k = (key or "").lower()
    return "circ_mv" in k and "wan" in k


def fmt_value_for_label(x: Any, key: str = "") -> str:
    """策略实验室标签用：整数优先中文数字；市值类大额不用科学计数法。"""
    if x is None:
        return "—"
    if isinstance(x, bool):
        return "是" if x else "否"
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return str(x)
    if not math.isfinite(xf):
        return str(xf)

    if _key_is_circ_mv_wan(key):
        if abs(xf) >= 10000.0 and abs(xf - round(xf)) < 1e-6:
            wn = int(round(xf / 10000.0))
            if wn == 0:
                return "零"
            return int_to_zh_num(wn) + "万"
        if abs(xf - round(xf)) < 1e-9:
            return int_to_zh_num(int(round(xf)))

    if abs(xf) >= 1_000_000.0 and abs(xf - round(xf)) < 1e-6:
        xi = int(round(xf))
        if xi != 0 and xi % 10000 == 0:
            return int_to_zh_num(xi // 10000) + "万"
        return str(xi)

    if abs(xf - round(xf)) < 1e-9:
        ir = int(round(xf))
        if abs(ir) > 10_000_000_000:
            return str(ir)
        return int_to_zh_num(ir)

    s = f"{xf:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"

# ---------- 中文显示名 ----------
PARAM_LABEL_ZH: Dict[str, str] = {
    "trend_ma120_min_ratio": "六十日与一百二十日均线粘合比下限（MA60/MA120）",
    "trend_slope_fastpass": "二十日均线五日斜率弱多头放行阈值（MA20）",
    "near_ma20_min_ratio": "收盘价贴近二十日均线比例下限（MA20）",
    "macd_bar_kill": "指数平滑异同柱淘汰线（MACD，低于此值拦截）",
    "vol_divergence_ratio": "量价背离：阳量/阴量倍数要求",
    "pass_line": "底仓综合分及格线（1档）",
    "golden_burst_pct_low": "黄金起爆涨幅带下沿（预留）",
    "golden_burst_pct_high": "黄金起爆涨幅带上沿（预留）",
    "golden_burst_vr_low": "黄金起爆量比下沿（预留）",
    "golden_burst_vr_high": "黄金起爆量比上沿（预留）",
    "p5_golden_vr_min": "盘后池全日量比下限（5档）",
    "p5_golden_pct_low": "盘后池全日涨幅窗口下沿（开区间，5档）",
    "p5_golden_pct_high": "盘后池全日涨幅窗口上沿（开区间，5档）",
    "circ_mv_min_wan": "参与门槛：最低流通市值（万元）",
    "circ_mv_prefer_wan": "超大盘加权阈值（万元）",
    "open_bias20_max_pct": "开盘相对二十日均线最大乖离（%，MA20）",
    "auction_pct_chg_max": "竞价高开幅度上限（%）",
    "prev_turnover_min_pct": "昨日真实换手下限（%）",
    "s1_winner_min": "策略一：筹码胜率下限",
    "s1_pct_low": "策略一：涨幅下限（%）",
    "s1_pct_high": "策略一：涨幅上限（%）",
    "s1_vol_ratio_min": "策略一：量比下限",
    "s1_atr_pct_max": "策略一：平均真实波幅上限（ATR%）",
    "s1_auction_amount_vs_prev_ratio_min": "策略一：竞价额/昨全日额 下限",
    "s2_prev_pct_low": "策略二：昨涨幅下限（%）",
    "s2_prev_pct_high": "策略二：昨涨幅上限（%）",
    "s2_prev_vol_ratio_min": "策略二：昨量比下限",
    "s2_pct_high": "策略二：涨幅上限（%）",
    "s2_auction_pct_low": "策略二：今竞价涨幅下限（%）",
    "s2_auction_pct_high": "策略二：今竞价涨幅上限（%）",
    "s3_pct_low": "策略三：竞价涨幅下限（%）",
    "s3_pct_high": "策略三：竞价涨幅上限（%）",
    "s3_auction_turnover_min_pct": "策略三：竞价换手下限（%）",
    "s3_upper_shadow_max_pct": "策略三：上影上限（%）",
    "s3_inst_net_buy_ratio_of_float_mv": "策略三：机构净买/流通市值 比例",
    "s4_circ_mv_min_wan": "策略四：最低流通市值（万元）",
    "s4_net_main_ratio_of_float_mv": "策略四：主力净额/流通市值 比例",
    "s4_pct_low": "策略四：竞价涨幅下限（%）",
    "s4_pct_high": "策略四：竞价涨幅上限（%）",
    "s4_strth_ratio_of_circ_mv_wan": "策略四：涨停强度/流通市值(万) 比例",
    "bias_20_max_pct": "全局：二十日乖离率上限（%，Bias20）",
    "s1_vol_ma5_mult": "策略一：成交量相对五日均线倍数（MA5）",
    "s2_ma20_touch_ratio": "策略二：触及二十日均线比例（MA20）",
    "s3_vol_ratio_min": "策略三：量比下限",
    "s3_rsi_fly": "策略三：相对强弱指标飞刀线（RSI）",
    "s4_inst_net_buy_ratio_of_float_mv": "策略四：机构净买/流通市值 比例",
    "s5_vol_ratio_min": "策略五：量比下限",
    "s5_cost50_mult": "策略五：现价相对五十分位成本倍数（cost_50th）",
    "s5_atr_fly_max": "策略五：平均真实波幅飞刀上限（ATR%）",
    "s6_pct_low": "策略六：涨幅下限（%）",
    "s6_pct_high": "策略六：涨幅上限（%）",
    "s6_winner_min": "策略六：筹码胜率下限",
    "s6_bias_fly": "策略六：乖离飞刀线（%）",
    "s7_pct_low": "策略七：涨幅下限（%）",
    "s7_pct_high": "策略七：涨幅上限（%）",
    "s7_net_main_ratio_of_float_mv": "策略七：主力净额/流通市值 比例",
    "s7_pre_close_low_mult": "策略七：最低价相对昨收倍数上限",
    "s7_vol_ratio_low": "策略七：量比区间下限",
    "s7_vol_ratio_high": "策略七：量比区间上限",
    "s7_turnover_fly": "策略七：真实换手飞刀（%）",
    "breakout_yang_vol_ratio_min": "突破共性：阳量/阴量 倍数下限",
    "breakout_vwap_eps": "突破共性：成交量加权均价下方容忍比例（VWAP）",
    "giant_min_vol_ratio": "巨头策略：最低量比",
    "global_fund_reflow_pct_max": "全局：主力为负时允许的最大涨幅（%）",
    "global_upper_shadow_ratio_max": "全局：上影/收盘 比上限",
    "global_upper_turnover_f_min": "全局：长上影时换手下限（%）",
    "ma60_downtrend_rel": "六十日均线弱势相对比（MA60）",
    "s1_close_to_high_min": "策略一：收盘接近最高价比例下限",
    "s1_vol_ratio_min": "策略一：量比下限",
    "s1_ma20_slope_min": "策略一：二十日均线斜率下限（MA20）",
    "s2_turnover_f_low": "策略二：真实换手区间下限（%）",
    "s2_turnover_f_high": "策略二：真实换手区间上限（%）",
    "s3_net_main_ratio_of_float_mv": "策略三：主力/流通市值 比例",
    "s3_inst_net_buy_ratio_of_float_mv": "策略三：机构/流通市值 比例",
    "s4_vol_shrink_ratio": "策略四：缩量比（相对均量）",
    "s4_touch_ma_eps": "策略四：触及均线倍数容差",
    "s4_winner_fly_lt": "策略四：胜率飞刀线（低于则风险）",
    "s5_pe_max": "策略五：市盈率上限（PE）",
    "s5_turnover_f_max_lt": "策略五：换手上限（%）",
    "s6_mhist_expand_min_ratio": "策略六：指数平滑异同柱扩张比下限（MACD）",
    "s6_turnover_f_min": "策略六：换手下限（%）",
    "s7_net_main_ratio_of_float_mv": "策略七：主力/流通市值 比例",
    "s7_ma5_touch_eps": "策略七：触及五日均线倍数（MA5）",
    "s7_vol_ratio_min": "策略七：量比下限",
    "s7_turnover_f_fly_gt": "策略七：换手飞刀下限（%）",
    "s8_atr_pct_fly_gt": "策略八：平均真实波幅飞刀线（ATR%）",
    "s8_bias_fly_gt": "策略八：乖离飞刀线（%）",
    "s8_net_main_ratio_of_float_mv": "策略八：主力/流通市值 比例",
    "s8_min_vol_vs_vma5": "策略八：全日量相对五日均量比下限（MA5）",
    "global_bias_high": "全局：乖离率上限（%，Bias）",
    "global_bias_low": "全局：乖离率下限（%，Bias）",
    "s1_vr_min": "策略一：量比下限",
    "s1_upper_shadow_ratio_fly_gt": "策略一：上影比飞刀线",
    "s2_vol_over_vma5_mult": "策略二：成交量相对五日均线倍数上限（MA5）",
    "s2_cost95_to_close_ratio_fly_lt": "策略二：九十五分位成本与收盘价比飞刀线",
    "s3_vol_over_vma5_fly_mult": "策略三：成交量相对五日均线飞刀倍数（MA5）",
    "s4_ma20_slope_min": "策略四：二十日均线斜率下限（MA20）",
    "s4_vol_over_vma5_max_mult": "策略四：成交量相对五日均线倍数上限（MA5）",
    "s5_circ_mv_fly_lt_wan": "策略五：流通市值飞刀线（万元）",
    "s6_inst_sum3_ratio_of_float_mv": "策略六：三日机构净和相对流通市值比例",
    "s6_vol_under_vma5_ratio": "策略六：成交量低于五日均线比例上限（MA5）",
    "s6_net_main_fly_ratio_of_float_mv": "策略六：主力飞刀/流通市值 比例",
    "s7_pe_max": "策略七：市盈率上限（PE）",
    "s7_circ_mv_min_wan": "策略七：最低流通市值（万元）",
    "s8_circ_mv_min_wan": "策略八：流通市值下限（万元）",
    "s8_circ_mv_max_wan": "策略八：流通市值上限（万元）",
    "s8_turnover_f_low": "策略八：换手区间下限（%）",
    "s8_turnover_f_high": "策略八：换手区间上限（%）",
    "s9_pct_chg_min": "策略九：涨幅下限（%）",
    "s9_vr_min": "策略九：量比下限",
    "s9_strth_ratio_of_circ_mv_wan": "策略九：涨停强度/流通市值(万) 比例",
    "s9_winner_fly_lt": "策略九：胜率飞刀线",
    "s10_pe_max": "策略十：市盈率上限（PE）",
    "s10_ps_max": "策略十：市销率上限（PS）",
    "s10_atr_pct_max": "策略十：平均真实波幅上限（ATR%）",
    "s10_winner_fly_lt": "策略十：胜率飞刀线",
    "s11_pe_max": "策略十一：市盈率（PE）上限",
    "s11_pct_low": "策略十一：涨幅下限（%，pct_low）",
    "s11_pct_high": "策略十一：涨幅上限（%，pct_high）",
    "s11_winner_min": "策略十一：筹码胜率下限（%，winner_min）",
    "enable_ma_compensation": "P5爆发分：均线动能补偿总开关（异构共振 + MA20斜率奖励）",
    "enable_vwap_penalty": "P5：VWAP分时防伪总开关（收盘偏离与尾盘脉冲惩罚）",
    "ma_bias_overheat_pct": "P5：MA5相对MA20乖离虚火线（%，超过且多头排列则分级降权）",
    "circ_mv_wan_large_min": "P5：大市值下限（万元），≥此且虚火时重罚乘子",
    "circ_mv_wan_mid_min": "P5：中市值下限（万元），[此,大市值)且虚火时轻罚乘子",
    "ma_overheat_mult_large": "P5：大市值虚火惩罚乘子",
    "ma_overheat_mult_mid": "P5：中市值虚火惩罚乘子",
    "ma_slope_reward_threshold_pct": "P5：MA20五日斜率奖励门槛（%）",
    "ma_slope_reward_mult_min": "P5：斜率奖励乘子下限",
    "ma_slope_reward_mult_max": "P5：斜率奖励乘子上限",
    "ma_slope_reward_interp_high_pct": "P5：斜率奖励插值上沿（%）",
    "vwap_dev_soft_pct": "P5：VWAP收盘偏离警惕线（%）",
    "vwap_dev_hard_pct": "P5：VWAP收盘偏离重锤线（%）",
    "vwap_tail_spike_pct": "P5：尾盘脉冲判定线（%）",
    "vwap_tail_spike_mult": "P5：尾盘脉冲惩罚乘子",
    "vwap_hard_mult": "P5：VWAP重锤惩罚乘子",
    "vwap_tail_minutes": "P5：尾盘统计窗口（分钟）",
}

# ---------- 一句话释义（紧跟在「默认」后，宜短）----------
PARAM_HINT_ZH: Dict[str, str] = {
    "trend_ma120_min_ratio": "六十日线不得低于一百二十日线×该值",
    "trend_slope_fastpass": "斜率达此可弱多头放行",
    "near_ma20_min_ratio": "收盘须不低于二十日均线×该值",
    "macd_bar_kill": "指数平滑异同柱低于此视为动能过弱（MACD）",
    "vol_divergence_ratio": "阳量须 ≥ 阴量×该值",
    "pass_line": "综合分低于此不入底仓池（1档）",
    "golden_burst_pct_low": "预留：涨幅带下沿",
    "golden_burst_pct_high": "预留：涨幅带上沿",
    "golden_burst_vr_low": "预留：量比下沿",
    "golden_burst_vr_high": "预留：量比上沿",
    "p5_golden_vr_min": "盘后池门禁：量比须高于此（5档）",
    "p5_golden_pct_low": "盘后池门禁：涨幅须高于此（%，5档）",
    "p5_golden_pct_high": "盘后池门禁：涨幅须低于此（%，5档）",
    "circ_mv_min_wan": "低于此流通市值（万元）一票否决",
    "circ_mv_prefer_wan": "超大盘排序加权阈值",
    "open_bias20_max_pct": "开盘乖离二十日均线上限（%，MA20）",
    "auction_pct_chg_max": "竞价涨幅上限（%）防追高",
    "prev_turnover_min_pct": "昨真实换手下限（%）",
    "s1_winner_min": "筹码胜率下限（%）",
    "s1_pct_low": "策略一涨幅下限（%）",
    "s1_pct_high": "策略一涨幅上限（%）",
    "s1_vol_ratio_min": "量比下限",
    "s1_atr_pct_max": "平均真实波幅过大则剔除（ATR%）",
    "s1_auction_amount_vs_prev_ratio_min": "竞价额占昨全日额比例下限",
    "s2_prev_pct_low": "昨收涨幅下限（%）",
    "s2_prev_pct_high": "昨收涨幅上限（%）",
    "s2_prev_vol_ratio_min": "昨量比下限",
    "s2_pct_high": "策略二涨幅上限（%）",
    "s2_auction_pct_low": "今竞价涨幅下限（%）",
    "s2_auction_pct_high": "今竞价涨幅上限（%）",
    "s3_pct_low": "策略三竞价涨幅下限（%）",
    "s3_pct_high": "策略三竞价涨幅上限（%）",
    "s3_auction_turnover_min_pct": "竞价阶段真实换手下限（%）",
    "s3_upper_shadow_max_pct": "上影线占昨收%上限，防冲高回落",
    "s3_inst_net_buy_ratio_of_float_mv": "机构净买/流通市值，与阶梯地板取大",
    "s4_circ_mv_min_wan": "策略四市值门槛（万）",
    "s4_net_main_ratio_of_float_mv": "主力净额占流通市值比例",
    "s4_pct_low": "策略四涨幅下限（%）",
    "s4_pct_high": "策略四涨幅上限（%）",
    "s4_strth_ratio_of_circ_mv_wan": "涨停强度相对市值",
    "bias_20_max_pct": "二十日乖离率绝对值过大不追（Bias20）",
    "s1_vol_ma5_mult": "成交量相对五日均线倍数（MA5）",
    "s2_ma20_touch_ratio": "回踩二十日均线的贴近倍数（MA20）",
    "s3_vol_ratio_min": "量比下限",
    "s3_rsi_fly": "相对强弱指标过高视为过热（RSI）",
    "s4_inst_net_buy_ratio_of_float_mv": "机构净买占流通市值比例",
    "s5_vol_ratio_min": "量比下限",
    "s5_cost50_mult": "现价相对五十分位成本倍数",
    "s5_atr_fly_max": "平均真实波幅过高防波动风险（ATR%）",
    "s6_pct_low": "策略六涨幅下限（%）",
    "s6_pct_high": "策略六涨幅上限（%）",
    "s6_winner_min": "筹码胜率下限",
    "s6_bias_fly": "乖离过大防追高",
    "s7_pct_low": "策略七涨幅下限（%）",
    "s7_pct_high": "策略七涨幅上限（%）",
    "s7_net_main_ratio_of_float_mv": "主力净额占流通市值比例",
    "s7_pre_close_low_mult": "最低价相对昨收倍数上限",
    "s7_vol_ratio_low": "量比区间下沿",
    "s7_vol_ratio_high": "量比区间上沿",
    "s7_turnover_fly": "换手过高防诱多",
    "breakout_yang_vol_ratio_min": "突破须阳量占优",
    "breakout_vwap_eps": "低于成交量加权均价的容忍比例（VWAP）",
    "giant_min_vol_ratio": "巨头战法最低量比",
    "global_fund_reflow_pct_max": "主力为负时涨幅须超此（%）才放行",
    "global_upper_shadow_ratio_max": "上影长度/收盘，小数非百分点",
    "global_upper_turnover_f_min": "长上影时换手须达此（%）",
    "ma60_downtrend_rel": "相对六十日均线弱势判定（MA60）",
    "s1_close_to_high_min": "收盘接近日高的比例",
    "s1_vol_ratio_min": "量比下限",
    "s1_ma20_slope_min": "二十日均线斜率下限（MA20）",
    "s2_turnover_f_low": "真实换手区间下限（%）",
    "s2_turnover_f_high": "真实换手区间上限（%）",
    "s3_net_main_ratio_of_float_mv": "主力占流通市值比例",
    "s3_inst_net_buy_ratio_of_float_mv": "机构占流通市值比例",
    "s4_pct_low": "策略四涨幅下限（%）",
    "s4_pct_high": "策略四涨幅上限（%）",
    "s4_vol_shrink_ratio": "相对均量缩量比",
    "s4_touch_ma_eps": "触及均线的倍数容差",
    "s4_winner_fly_lt": "胜率低于此视为弱势",
    "s5_pct_low": "策略五涨幅下限（%）",
    "s5_pct_high": "策略五涨幅上限（%）",
    "s5_pe_max": "市盈率上限（PE）",
    "s5_turnover_f_max_lt": "低换手上限（%）",
    "s6_mhist_expand_min_ratio": "指数平滑异同柱相对扩张比（MACD）",
    "s6_turnover_f_min": "换手下限（%）",
    "s7_net_main_ratio_of_float_mv": "主力占流通市值比例",
    "s7_ma5_touch_eps": "与五日均线触碰倍数（MA5）",
    "s7_vol_ratio_min": "量比下限",
    "s7_turnover_f_fly_gt": "换手过高警戒（%）",
    "s8_atr_pct_fly_gt": "平均真实波幅过高警戒（ATR%）",
    "s8_bias_fly_gt": "乖离过高（%）",
    "s8_net_main_ratio_of_float_mv": "主力占流通市值比例",
    "s8_min_vol_vs_vma5": "全日量相对 5 日均量",
    "global_bias_high": "乖离率上限（%，Bias）",
    "global_bias_low": "乖离率下限（%，Bias）",
    "s1_vr_min": "量比下限",
    "s1_upper_shadow_ratio_fly_gt": "上影占收盘比例飞刀线（小数）",
    "s2_vol_over_vma5_mult": "量相对五日均线倍数上限（MA5）",
    "s2_cost95_to_close_ratio_fly_lt": "95%成本与现价比",
    "s3_net_main_ratio_of_float_mv": "主力占流通市值比例",
    "s3_vol_over_vma5_fly_mult": "放量过猛倍数",
    "s4_ma20_slope_min": "二十日均线斜率下限（MA20）",
    "s4_vol_over_vma5_max_mult": "量相对五日均线上限（MA5）",
    "s5_circ_mv_fly_lt_wan": "过小市值飞刀（万）",
    "s6_inst_sum3_ratio_of_float_mv": "3 日机构净和占流通市值",
    "s6_vol_under_vma5_ratio": "低于五日均线量的比例上限（MA5）",
    "s6_net_main_fly_ratio_of_float_mv": "主力弱势比例",
    "s7_pe_max": "市盈率上限（PE）",
    "s7_circ_mv_min_wan": "最低流通市值（万）",
    "s8_circ_mv_min_wan": "流通市值下限（万）",
    "s8_circ_mv_max_wan": "流通市值上限（万）",
    "s8_turnover_f_low": "换手区间下限（%）",
    "s8_turnover_f_high": "换手区间上限（%）",
    "s9_pct_chg_min": "涨停附近涨幅下限（%）",
    "s9_vr_min": "量比下限",
    "s9_strth_ratio_of_circ_mv_wan": "涨停强度/市值(万)",
    "s9_winner_fly_lt": "胜率飞刀线",
    "s10_pe_max": "市盈率上限（PE）",
    "s10_ps_max": "市销率上限（PS）",
    "s10_atr_pct_max": "平均真实波幅上限（ATR%）",
    "s10_winner_fly_lt": "胜率飞刀线",
    "s11_pe_max": "市盈率过高则剔除（PE）",
    "s11_pct_low": "全日涨幅区间下沿（%，pct_low）",
    "s11_pct_high": "全日涨幅区间上沿（%，pct_high）",
    "s11_winner_min": "筹码获利占比下限（%，winner_min）",
    "enable_ma_compensation": "关闭则 P5 爆发分回到纯量比+命中数公式",
    "ma_bias_overheat_pct": "乖离超此且多头视为过热区起点",
    "circ_mv_wan_large_min": "流通市值≥此（万）用重罚乘子",
    "circ_mv_wan_mid_min": "流通市值≥此（万）且小于大市值门槛用轻罚",
    "ma_overheat_mult_large": "大盘股虚火乘子",
    "ma_overheat_mult_mid": "中盘虚火乘子",
    "ma_slope_reward_threshold_pct": "斜率高于此且加速才给奖励",
    "ma_slope_reward_mult_min": "斜率在门槛处对应乘子",
    "ma_slope_reward_mult_max": "斜率≥插值上沿时封顶乘子",
    "ma_slope_reward_interp_high_pct": "斜率线性映射到满额奖励的上界",
}

PARAM_BOUNDS: Dict[str, Tuple[float, float]] = {
    # P1 / 黄金：实战龙头参数区间极宽，避免 Slider / Number Input 卡死极端输入
    "trend_ma120_min_ratio": (0.5, 1.2),
    "trend_slope_fastpass": (0.0, 8.0),
    "near_ma20_min_ratio": (0.75, 1.15),
    "macd_bar_kill": (-2.0, 0.5),
    "vol_divergence_ratio": (0.01, 100.0),
    "pass_line": (0.0, 200.0),
    "golden_burst_pct_low": (-30.0, 200.0),
    "golden_burst_pct_high": (-30.0, 200.0),
    "golden_burst_vr_low": (0.01, 100.0),
    "golden_burst_vr_high": (0.01, 100.0),
    "p5_golden_vr_min": (0.01, 100.0),
    "p5_golden_pct_low": (-50.0, 200.0),
    "p5_golden_pct_high": (-50.0, 200.0),
    # 易混淆：上影「百分比」字段（非 global_upper_shadow_ratio 小数比）
    "s3_upper_shadow_max_pct": (0.0, 150.0),
    "global_upper_shadow_ratio_max": (0.0, 0.5),
    "s1_upper_shadow_ratio_fly_gt": (0.0, 0.5),
}


def label_zh(key: str) -> str:
    return PARAM_LABEL_ZH.get(key, key)


def hint_zh(key: str) -> str:
    return PARAM_HINT_ZH.get(key, "")


def fmt_default_for_label(v: Any) -> str:
    """兼容旧名：与 fmt_value_for_label 一致（无字段 key 时按通用规则）。"""
    return fmt_value_for_label(v, "")


def _infer_bounds(key: str, default: float, is_int: bool) -> Tuple[float, float]:
    """
    未写入 PARAM_BOUNDS 的键。注意顺序：先「占流通市值比例」再「涨跌幅%」再「上影小数比」，
    避免 upper_shadow / pct 误伤。
    """
    k = key.lower()
    if "circ_mv" in k and "wan" in k:
        return 0.0, 5.0e7
    if "ratio_of_float_mv" in k or k.endswith("_ratio_of_float_mv"):
        return 0.0, 0.02
    if "strth_ratio" in k:
        return 0.0, 0.02
    if "inst_sum3_ratio" in k:
        return 0.0, 0.02
    if "winner" in k and "min" in k:
        return 50.0, 100.0
    if "winner_fly" in k or ("winner" in k and "fly" in k and "min" not in k):
        return 40.0, 95.0
    if "nineturn" in k:
        return 0.0, 15.0
    if "cci" in k:
        return -200.0, 300.0
    if "pe_max" in k or k.endswith("_pe_max"):
        return 0.0, 200.0
    if "pb_max" in k or ("pb" in k and "fly" in k):
        return 0.0, 25.0
    if "ps_max" in k:
        return 0.0, 50.0
    if "dv_min" in k:
        return 0.0, 15.0
    if k.endswith("_vr_min"):
        return 0.01, 100.0
    if "breakout_yang" in k or "giant_min_vol" in k:
        return 0.01, 100.0
    if "vol_ratio_low" in k or "vol_ratio_high" in k:
        return 0.01, 100.0
    if "vol_ratio" in k and "ratio_of_float" not in k:
        return 0.01, 100.0
    # 竞价/昨收换手「百分数」
    if ("turnover" in k or "auction" in k) and ("pct" in k or k.endswith("_min_pct") or k.endswith("_max_pct")):
        if "upper_shadow" in k:
            pass
        elif "bias" in k or "atr" in k:
            pass
        else:
            return 0.0, 100.0
    # 涨跌门槛（%）
    if k.endswith("_pct_low") or k.endswith("_pct_high"):
        return -100.0, 200.0
    if "pct_chg_min" in k:
        return -20.0, 150.0
    # 其它 *_max_pct / *_min_pct：百分数字面（上影%、ATR%、乖离% 等）
    if k.endswith("_max_pct") or k.endswith("_min_pct"):
        return 0.0, 200.0
    # 真实换手（字段 turnover_f 或 turnover 且非 pct 口径）
    if "turnover_f" in k or ("turnover" in k and "pct" not in k):
        return 0.0, 100.0
    if "global_upper_shadow_ratio" in k:
        return 0.0, 0.2
    if "upper_shadow" in k and "ratio" in k and "pct" not in k:
        return 0.0, 0.25
    if "global_fund_reflow_pct_max" in k:
        return -20.0, 50.0
    if "bias" in k:
        return -200.0, 200.0
    if "macd" in k:
        return -0.6, 0.1
    if any(x in k for x in ("ma5", "ma10", "ma20", "ma60", "ma120", "ma250")) and (
        "ratio" in k or "eps" in k or "touch" in k or "rel" in k or "near" in k
    ):
        return 0.85, 1.15
    if "slope" in k:
        return -2.0, 8.0
    if "vol_shrink" in k or "under_vma5" in k:
        return 0.1, 2.0
    if "amount_vs_prev" in k:
        return 0.0, 1.0
    if "atr" in k:
        return 0.0, 200.0
    if "rsi" in k or "kdj" in k:
        return 0.0, 100.0
    if "mhist_expand" in k:
        return 0.8, 2.0
    if "close_to_high" in k:
        return 0.5, 1.0
    if "min_vol_vs_vma5" in k:
        return 0.05, 2.0
    if "open_bias" in k or "bias_20_max" in k:
        return 0.0, 200.0
    if "auction_pct_chg_max" in k:
        return 0.0, 150.0
    if "mult" in k and "vol" in k:
        return 0.01, 100.0
    if "cost95" in k:
        return 0.9, 1.2
    if "breakout_vwap_eps" in k:
        return 0.0, 0.05
    if default == 0:
        return ((-1e6, 1e6) if not is_int else (-10_000.0, 10_000.0))
    # 不再用 default×20 收窄：未知键给对称宽窗，避免龙头极端参数被误杀
    ad = abs(float(default))
    span = max(ad * 500.0, 1.0)
    lo = float(default) - span
    hi = float(default) + span
    if lo > hi:
        lo, hi = hi, lo
    lo = max(lo, -1e9)
    hi = min(hi, 1e9)
    return float(lo), float(hi)


def bounds_for(key: str, default: float, is_int: bool) -> Tuple[float, float]:
    if key in PARAM_BOUNDS:
        return PARAM_BOUNDS[key]
    return _infer_bounds(key, float(default), is_int)


def clamp_value(key: str, value: float, default: float, is_int: bool) -> float:
    lo, hi = bounds_for(key, default, is_int)
    v = max(lo, min(hi, float(value)))
    if is_int:
        return float(int(round(v)))
    return v


def build_field_label(key: str, default_val: Any) -> str:
    zh = label_zh(key)
    d = fmt_value_for_label(default_val, key)
    hint = hint_zh(key)
    is_int = isinstance(default_val, int) and not isinstance(default_val, bool)
    lo, hi = bounds_for(key, float(default_val if default_val is not None else 0.0), is_int)
    lo_s = fmt_value_for_label(lo, key)
    hi_s = fmt_value_for_label(hi, key)
    core = f"{zh}（默认 {d}"
    if hint:
        core += f"，{hint}"
    core += f"）｜允许 {lo_s}～{hi_s}"
    return core


def lab_input_step(default: float, lo: float, hi: float, is_int: bool) -> float:
    """
    按数量级给步长：默认约 1 → 0.1；约 0.2 → 0.01。并限制步长不超过区间跨度的约 1/20。
    """
    span = float(hi) - float(lo)
    if is_int:
        if span <= 1:
            return 1.0
        return max(1.0, min(5.0, round(span / 15.0)))
    ref = abs(float(default))
    if ref < 1e-12:
        ref = max(abs(lo), abs(hi), 1e-9)
        if ref < 1e-12:
            ref = 0.01
    if ref >= 1_000_000:
        step = 10.0 ** max(0, int(math.log10(ref)) - 1)
    elif ref >= 10_000:
        step = 1000.0
    elif ref >= 1000:
        step = 100.0
    elif ref >= 100:
        step = 10.0
    elif ref >= 10:
        step = 1.0
    elif ref >= 1:
        step = 0.1
    elif ref >= 0.1:
        step = 0.01
    elif ref >= 0.01:
        step = 0.001
    else:
        step = 0.0001
    if span > 0:
        step = min(step, max(span / 20.0, 1e-7))
    return float(step)
