# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 三层分级全局风控底座（全池最高优先级安检门）
================================================================================
【设计目标】
1. 第一层「死亡红线」：全策略（P2/P3/P4/P5）统一执行，触碰即否决，记录 veto_reason。
2. 第二层「右侧攻击红线」：仅当 is_right_side_strategy=True（突破/主升浪类）时生效；低吸策略传 False 则本层恒为通过。
3. 第三层「雷达降权」：不否决标的，累计 penalty 与 risk_tags，供 burst_score 扣减与展示；
   并给出 suggested_min_entry_score，提示「建议至少达到该综合分再考虑买入」（偏稳、仍可出票）。

【与 P1 分工】底仓池（pool_manager）已剔除亏损股与 PE>300 绝对泡沫；本模块不写 pe<0 特赦分支，仅对有效 pe>0 做 layer1 估值硬杀（默认 pe>280）。轻资产科技股不因 pb 一票否决。

【量纲约定】（与 fund_mv_utils / 日线库一致）
- circ_mv：万元；流通市值(元) = circ_mv * 10000
- net_main_amount：元
- vol（日线/昨收）：手；rt['volume']：股 → 换算手数 = volume / 100
- turnover_rate_f：真实换手，百分数（如 5.0 表示 5%）

【调用方式】
- 快照单票：evaluate_3layer_risk(df, rt, cfg, is_right_side_strategy=False, pool_key=None)
- P5 向量化：evaluate_3layer_risk_vector(prep_df, cfg, is_right_side_series=None)

详见各函数 docstring 与引擎内集成注释。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

_rce_log = logging.getLogger(__name__)

try:
    from core.strategies.fund_mv_utils import effective_turnover_rate_f
except ImportError:
    from strategies.fund_mv_utils import effective_turnover_rate_f  # type: ignore

try:
    from core.backtest_context import is_backtest_legacy_mode
except ImportError:
    def is_backtest_legacy_mode() -> bool:
        return False


BJ_TZ = timezone(timedelta(hours=8))


@dataclass
class RiskControlConfig:
    """
    风控阈值集中配置：所有「魔法数字」仅允许出现在此数据类中，便于按环境/风格动态调优。
    字段默认值与产品文档《三层分级风控》一致；可在实例化时整体或局部覆盖。
    """

    # ---------- 第一层：死亡红线 ----------
    # 1) 趋势破位：现价低于 ma20 且短期均线斜率向下
    layer1_price_below_ma20_enabled: bool = True
    layer1_ma20_slope_negative_max: float = 0.0  # ma20_slope_5 < 该值视为下行（一般 <0）

    # 2) 恶性派发：① 巨量+收跌；② 更大量+微涨+长上影（防尾盘拉红骗线）
    layer1_turnover_f_dump_min_pct: float = 25.0
    layer1_turnover_f_stagnant_min_pct: float = 30.0
    layer1_dump_stagnant_pct_chg_max: float = 2.0
    layer1_dump_stagnant_shadow_min: float = 0.3

    # 3) 危险高开：高开过猛但非涨停（易接飞刀）；一字/秒板 pct_chg>=limit 附近则放行
    layer1_open_pct_chg_min_danger: float = 6.0
    layer1_pct_chg_limit_proxy_pct: float = 9.5  # A 股主板涨停附近代理阈值

    # 4) 长上影诱多：上影占全日振幅比例 + 超倍量（不要求收阴，防假阳线）
    layer1_upper_shadow_ratio_min: float = 0.55
    layer1_vol_vs_ma5_mult: float = 2.0

    # 5) 主力资金出逃：主力净额为负且相对流通市值比例超过阈值（用户给定 0.5% → -0.005）
    layer1_net_main_outflow_ratio_of_float_mv: float = -0.005

    # 6) 极端估值：P1 已拦 PE>300；此处仅防泡沫破裂硬杀，给主升浪龙头溢价空间（不写亏损股特赦，无效 pe 不触发）
    layer1_pe_ttm_max: float = 280.0

    # ---------- 第二层：右侧攻击专属（科技成长弹性）----------
    # 获利盘：量比极大时放宽 winner 门槛；否则维持严格筹码干净度
    layer2_winner_rate_min_strict: float = 85.0
    layer2_winner_rate_min_relaxed: float = 75.0
    layer2_winner_relax_vol_ratio_min: float = 2.0
    layer2_macd_hist_min: float = 0.0  # 要求 macd_hist >= 该值（动能已启动）
    layer2_bias_20_max: float = 12.0
    layer2_vol_ratio_min: float = 1.2

    # ---------- 第三层：雷达降权 ----------
    layer3_fake_rise_vol_ratio_max: float = 1.35
    layer3_fake_rise_pct_chg_min: float = 3.0
    layer3_penalty_fake_rise: float = 20.0

    layer3_tail_trap_start_minute: int = 870  # 14:30 = 14*60+30
    layer3_tail_trap_pct_chg_min: float = 2.0
    layer3_tail_trap_vol_proj_vs_ma5_max: float = 0.8
    layer3_penalty_tail_trap: float = 25.0
    layer3_full_session_minutes: float = 240.0  # 用于预估全日成交量（分钟刻度近似）

    layer3_overheat_pct_chg_min: float = 7.0
    layer3_overheat_bias_min: float = 6.0
    # 仅当量比低于此值视为缩量透支；放量强攻的高乖离不扣分
    layer3_overheat_vol_ratio_max: float = 1.0
    layer3_penalty_overheat: float = 30.0

    layer3_fatigue_up_days: int = 4
    layer3_penalty_fatigue: float = 20.0

    layer3_winner_drop_min: float = 5.0  # 当日 winner 较昨日下降 >= 该值（百分点）
    layer3_chip_loosen_turnover_min_pct: float = 15.0  # 筹码松动需配合高换手（真实换手%）
    layer3_penalty_chip_loosen: float = 25.0

    # ---------- 回测 legacy 模式：近似旧版 KDJ 超买扣分（仅 is_backtest_legacy_mode() 为真时生效）----------
    layer3_legacy_kdj_k_threshold: float = 85.0
    layer3_legacy_kdj_penalty: float = 18.0

    # ---------- 建议最低买入综合分（展示用，非硬否决）----------
    base_suggested_min_entry_score: float = 52.0
    penalty_to_min_score_weight: float = 0.12  # suggested += penalty * weight

    # ---------- UI 纯预警模式（默认开启）----------
    # True：一层/二层原「死亡红线」不再拦截战法匹配，仅写入 risk_tags / ui_warnings 供界面提示；
    # False：恢复旧版硬否决（触碰一层即 return，右侧二层未过即清空命中）。
    ui_alert_only: bool = True


DEFAULT_RISK_CONFIG = RiskControlConfig()


def _layer2_winner_threshold(vol_ratio: float, cfg: RiskControlConfig) -> float:
    """量比达阈值时放宽获利盘要求（科技成长放量上攻）。"""
    if vol_ratio >= cfg.layer2_winner_relax_vol_ratio_min:
        return float(cfg.layer2_winner_rate_min_relaxed)
    return float(cfg.layer2_winner_rate_min_strict)


def apply_layer2_right_side_rules(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    now_price: float,
    cfg: RiskControlConfig,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    第二层右侧红线完整判别（标量）。
    返回 (pass_layer2, veto_reason, raw_detail)。
    """
    detail: Dict[str, Any] = {}
    vol_ratio = _sf(rt.get("vol_ratio"), _sf(y.get("vol_ratio"), 0.0))
    wr = _sf(rt.get("winner_rate"), _sf(y.get("winner_rate"), 0.0))
    mh = float(_macd_hist_from_series(y))
    bias20 = float(_bias_20_from_series(y, now_price))
    wr_need = _layer2_winner_threshold(vol_ratio, cfg)

    if wr < wr_need:
        detail["l2_winner"] = wr
        detail["l2_wr_required"] = wr_need
        return (
            False,
            f"二层[右侧红线]获利盘不足：winner_rate<{wr_need:.0f}%（当前量比{vol_ratio:.2f}，放量门槛≥{cfg.layer2_winner_relax_vol_ratio_min}时放宽至{cfg.layer2_winner_rate_min_relaxed:.0f}%）",
            detail,
        )
    if mh < cfg.layer2_macd_hist_min:
        detail["l2_macd"] = mh
        return False, "二层[右侧红线]动能未启动：macd_hist<0", detail

    if bias20 > cfg.layer2_bias_20_max:
        detail["l2_bias"] = bias20
        return False, "二层[右侧红线]过热：bias_20过高", detail
    if vol_ratio < cfg.layer2_vol_ratio_min:
        detail["l2_vr"] = vol_ratio
        return False, "二层[右侧红线]量能不足：vol_ratio过低", detail
    return True, "", detail


def hits_indicate_right_side_attack(hits: List[str]) -> bool:
    """
    根据策略命中标签判断是否按「右侧攻击/突破主升浪」口径执行第二层红线。
    与 P3「P3-01·★右侧起爆」、P4 突破类文案对齐；低吸/均线类不含下列关键字则豁免二层。
    """
    if not hits:
        return False
    # P4 新增关键词覆盖：
    # P4-06 "动能突破共振" → "突破"/"动能"
    # P4-11 "底仓主线共振" → "主线"
    # P4-10 "沿5日线主升缩量" → "主升"
    keys = (
        "右侧", "起爆", "突破", "跃迁", "单峰", "平台二次",
        "水上金叉", "苍穹", "连阳", "点火", "倍量",
        "动能", "主线", "主升",
    )
    for h in hits:
        s = str(h)
        if any(k in s for k in keys):
            return True
    return False


def evaluate_layer2_right_side_only(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: RiskControlConfig = DEFAULT_RISK_CONFIG,
) -> Dict[str, Any]:
    """
    在已通过第一层、且策略命中「右侧攻击」时单独复检第二层，避免重复计算第三层扣分。
    返回: pass_layer2 (bool), veto_reason (str), raw_detail (dict), ui_warnings (list, 可选)
    """
    raw_detail: Dict[str, Any] = {}
    out: Dict[str, Any] = {
        "pass_layer2": True,
        "veto_reason": "",
        "raw_detail": raw_detail,
        "ui_warnings": [],
    }
    if df is None or df.empty:
        out["pass_layer2"] = False
        out["veto_reason"] = "二层：历史数据为空"
        return out
    y = df.iloc[-1]
    pre_close = _sf(rt.get("pre_close"), _sf(y.get("pre_close"), _sf(y.get("close"), 0.0)))
    now_price = _sf(rt.get("price"), 0.0)
    if now_price <= 0:
        now_price = _sf(y.get("close"), 0.0)
    if pre_close <= 0 or now_price <= 0:
        out["pass_layer2"] = False
        out["veto_reason"] = "二层：价格数据无效"
        return out

    ok, reason, detail = apply_layer2_right_side_rules(df, rt, y, now_price, cfg)
    if not ok and cfg.ui_alert_only:
        out["pass_layer2"] = True
        out["veto_reason"] = ""
        out["ui_warnings"] = [str(reason)]
        out["raw_detail"] = detail
        return out
    out["pass_layer2"] = ok
    out["veto_reason"] = reason
    out["raw_detail"] = detail
    return out


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


def _macd_hist_from_series(y: pd.Series) -> float:
    """MACD 柱：与项目其它模块一致，优先 macd_hist，其次 macd_bar。"""
    v = _sf(y.get("macd_hist"), float("nan"))
    if not np.isfinite(v) or v == 0.0:
        v = _sf(y.get("macd_bar"), 0.0)
    return v


def _bias_20_from_series(y: pd.Series, now_price: float) -> float:
    b = _sf(y.get("bias_20"), float("nan"))
    if np.isfinite(b) and b != 0.0:
        return b
    ma20 = _sf(y.get("ma20"), 0.0)
    if ma20 <= 0 or now_price <= 0:
        return 0.0
    return (now_price - ma20) / ma20 * 100.0


def _vol_hand_now(rt: Dict[str, Any], y: pd.Series) -> float:
    v = _sf(rt.get("volume"), 0.0)
    if v > 0:
        return v / 100.0
    return _sf(y.get("vol"), 0.0)


def _intraday_elapsed_minutes(rt: Dict[str, Any]) -> Optional[float]:
    """
    从 rt 推断已交易分钟数，供预估全日成交量。
    优先 rt['elapsed_mins'] / rt['elapsed_minutes']；否则用 curr_min - 570（9:30）做粗略近似。
    """
    for key in ("elapsed_mins", "elapsed_minutes", "intraday_elapsed_mins"):
        if key in rt and rt[key] is not None:
            em = _sf(rt[key], 0.0)
            if em > 0:
                return em
    cm = rt.get("curr_min") or rt.get("current_minute")
    if cm is not None:
        m = float(cm)
        if m >= 570:
            return max(1.0, m - 570)
    try:
        now = datetime.now(BJ_TZ)
        cur = now.hour * 60 + now.minute
        if 570 <= cur <= 690:  # 9:30-11:30
            return max(1.0, float(cur - 570))
        if 780 <= cur <= 900:  # 13:00-15:00
            return max(1.0, 120.0 + float(cur - 780))
    except Exception as _e:
        # 【全局审计修复】维度2：时钟解析失败时回退 None，须留痕避免「预估量逻辑静默失效」无据可查
        _rce_log.debug("_intraday_elapsed_minutes 系统时钟分支异常: %s", _e, exc_info=True)
    return None


def _pool_is_tail_or_post(pool_key: Optional[str], rt: Dict[str, Any]) -> bool:
    pk = (pool_key or rt.get("_pool_key") or rt.get("pool_key") or "").strip().lower()
    if pk in ("p4", "p5", "postmarket", "tail"):
        return True
    return False


def _upper_shadow_ratio(high: float, low: float, open_px: float, close_px: float) -> float:
    """
    上影线占全日振幅比例：(high - max(open,close)) / (high-low)
    分母为 0 时返回 0.0（不触发「长上影」条件，避免除零误杀）。
    """
    rng = high - low
    if rng <= 1e-12:
        return 0.0
    body_top = max(open_px, close_px)
    up = high - body_top
    return max(0.0, up / rng)


def _consecutive_positive_pct_days(df: pd.DataFrame, n: int) -> bool:
    """最近 n 根日线 pct_chg 是否全部 > 0（含当日则用最后一根为「今日」）。"""
    if df is None or len(df) < n or "pct_chg" not in df.columns:
        return False
    tail = df["pct_chg"].tail(n)
    if len(tail) < n:
        return False
    return bool((tail > 0).all())


def evaluate_3layer_risk(
    df: Optional[pd.DataFrame],
    rt: Optional[Dict[str, Any]],
    cfg: RiskControlConfig = DEFAULT_RISK_CONFIG,
    is_right_side_strategy: bool = False,
    pool_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    三层风控单票快照评估（P2/P3/P4/P5 共用）。

    参数:
        df: 历史日线，至少含最后一根可代表「昨收指标」；长度不足时仅放宽部分条件。
        rt: 实时/竞价/尾盘快照字典（price, open, high, low, pre_close, vol_ratio, volume, ...）。
        cfg: RiskControlConfig 实例。
        is_right_side_strategy: True 表示当前标的按「右侧突破/主升浪」口径考核第二层；False 时第二层恒通过。
        pool_key: 可选池标识（如 'p3','p4'），用于尾盘诱多等场景与 rt['_pool_key'] 合并判断。

    返回字典字段:
        pass_layer1, pass_layer2, veto_reason, penalty, risk_tags,
        suggested_min_entry_score, raw_detail（调试细项）,
        ui_warnings（仅 ui_alert_only 时非空：一层/二层原否决文案，供前端高亮）
    """
    rt = rt if isinstance(rt, dict) else {}
    tags: List[str] = []
    ui_warnings: List[str] = []
    penalty = 0.0
    raw_detail: Dict[str, Any] = {}

    out = {
        "pass_layer1": True,
        "pass_layer2": True,
        "veto_reason": "",
        "penalty": 0.0,
        "risk_tags": tags,
        "suggested_min_entry_score": float(cfg.base_suggested_min_entry_score),
        "raw_detail": raw_detail,
        "ui_warnings": ui_warnings,
    }

    if df is None or df.empty:
        out["pass_layer1"] = False
        out["veto_reason"] = "风控：历史K线为空，无法评估"
        return out

    y = df.iloc[-1]
    pre_close = _sf(rt.get("pre_close"), _sf(y.get("pre_close"), _sf(y.get("close"), 0.0)))
    now_price = _sf(rt.get("price"), 0.0)
    if now_price <= 0:
        now_price = _sf(y.get("close"), 0.0)
    open_px = _sf(rt.get("open"), now_price)
    high_px = _sf(rt.get("high"), max(now_price, open_px))
    low_px = _sf(rt.get("low"), min(now_price, open_px))

    if pre_close <= 0 or now_price <= 0:
        out["pass_layer1"] = False
        out["veto_reason"] = "风控：昨收或现价无效"
        return out

    ui_only = bool(cfg.ui_alert_only)

    pct_chg_live = (now_price - pre_close) / pre_close * 100.0
    open_pct_chg = (open_px - pre_close) / pre_close * 100.0 if pre_close > 0 else 0.0
    ma20 = _sf(y.get("ma20"), 0.0)
    ma20_slope_5 = _sf(y.get("ma20_slope_5"), 0.0)
    turnover_f = float(effective_turnover_rate_f(rt, y, now_price))
    vol_ratio = _sf(rt.get("vol_ratio"), _sf(y.get("vol_ratio"), 0.0))
    vol_hand = _vol_hand_now(rt, y)
    vol_ma5 = _sf(y.get("vol_ma5"), _sf(y.get("vma5"), 0.0))

    circ_mv_wan = _sf(rt.get("circ_mv"), _sf(y.get("circ_mv"), 0.0))
    if circ_mv_wan <= 0:
        tm = _sf(rt.get("total_mv"), _sf(y.get("total_mv"), 0.0))
        circ_mv_wan = tm * 0.6 if tm > 0 else 0.0

    net_main = _sf(rt.get("net_main_amount"), _sf(y.get("net_main_amount"), 0.0))
    pe_ttm = _sf(y.get("pe_ttm"), _sf(rt.get("pe_ttm"), float("nan")))

    float_mv_yuan = circ_mv_wan * 10000.0
    net_main_ratio = (net_main / float_mv_yuan) if float_mv_yuan > 1e-6 else 0.0

    # ======================== 第一层：死亡红线 ========================
    if cfg.layer1_price_below_ma20_enabled and ma20 > 0:
        if now_price < ma20 and ma20_slope_5 < cfg.layer1_ma20_slope_negative_max:
            msg = "一层[红线]趋势破位：现价<ma20且ma20_slope_5下行"
            raw_detail["l1_trend_break"] = True
            if ui_only:
                tags.append("⚠️[预警]" + msg)
                ui_warnings.append(msg)
            else:
                out["pass_layer1"] = False
                out["veto_reason"] = msg
                return out

    # 上影比例：恶性派发第二分支与长上影红线共用，须先于二者计算
    usr = _upper_shadow_ratio(high_px, low_px, open_px, now_price)
    raw_detail["upper_shadow_ratio"] = usr

    l1_dump_down = turnover_f > cfg.layer1_turnover_f_dump_min_pct and pct_chg_live < 0.0
    l1_dump_stagnant = (
        turnover_f > cfg.layer1_turnover_f_stagnant_min_pct
        and pct_chg_live < cfg.layer1_dump_stagnant_pct_chg_max
        and usr > cfg.layer1_dump_stagnant_shadow_min
    )
    if l1_dump_down or l1_dump_stagnant:
        msg = "一层[红线]恶性派发：巨量收跌或巨量滞涨长上影"
        raw_detail["l1_turnover_dump"] = True
        if ui_only:
            tags.append("⚠️[预警]" + msg)
            ui_warnings.append(msg)
        else:
            out["pass_layer1"] = False
            out["veto_reason"] = msg
            return out

    if open_pct_chg > cfg.layer1_open_pct_chg_min_danger and pct_chg_live < cfg.layer1_pct_chg_limit_proxy_pct:
        msg = "一层[红线]危险高开：高开>6%且未触及涨停附近"
        raw_detail["l1_gap_risk"] = True
        if ui_only:
            tags.append("⚠️[预警]" + msg)
            ui_warnings.append(msg)
        else:
            out["pass_layer1"] = False
            out["veto_reason"] = msg
            return out

    if (
        usr > cfg.layer1_upper_shadow_ratio_min
        and vol_ma5 > 0
        and vol_hand > cfg.layer1_vol_vs_ma5_mult * vol_ma5
    ):
        msg = "一层[红线]长上影诱多：高上影+超倍量"
        raw_detail["l1_upper_shadow_trap"] = True
        if ui_only:
            tags.append("⚠️[预警]" + msg)
            ui_warnings.append(msg)
        else:
            out["pass_layer1"] = False
            out["veto_reason"] = msg
            return out

    if net_main < 0 and net_main_ratio < cfg.layer1_net_main_outflow_ratio_of_float_mv:
        msg = "一层[红线]主力资金出逃：主力净额负且流出超流通市值0.5%"
        raw_detail["l1_main_flee"] = True
        if ui_only:
            tags.append("⚠️[预警]" + msg)
            ui_warnings.append(msg)
        else:
            out["pass_layer1"] = False
            out["veto_reason"] = msg
            return out

    if np.isfinite(pe_ttm) and pe_ttm > 0 and pe_ttm > cfg.layer1_pe_ttm_max:
        msg = "一层[红线]极端估值：pe_ttm过高"
        raw_detail["l1_pe"] = True
        if ui_only:
            tags.append("⚠️[预警]" + msg)
            ui_warnings.append(msg)
        else:
            out["pass_layer1"] = False
            out["veto_reason"] = msg
            return out

    # ======================== 第二层：右侧攻击专属 ========================
    pass_layer2 = True
    if is_right_side_strategy:
        pass_layer2, l2_reason, l2_detail = apply_layer2_right_side_rules(df, rt, y, now_price, cfg)
        if not pass_layer2:
            raw_detail.update(l2_detail)
            if ui_only:
                tags.append("⚠️[预警]" + str(l2_reason))
                ui_warnings.append(str(l2_reason))
                pass_layer2 = True
            else:
                out["veto_reason"] = l2_reason

    out["pass_layer2"] = pass_layer2
    if not pass_layer2:
        return out

    # ======================== 第三层：雷达降权（不否决） ========================
    # 1) 无量上涨
    if vol_ratio < cfg.layer3_fake_rise_vol_ratio_max and pct_chg_live > cfg.layer3_fake_rise_pct_chg_min:
        penalty += cfg.layer3_penalty_fake_rise
        tags.append("🩹[风险]无量虚涨")

    # 2) 尾盘诱多
    bj_now = datetime.now(BJ_TZ)
    cur_min = bj_now.hour * 60 + bj_now.minute
    cm_rt = rt.get("curr_min")
    if cm_rt is not None:
        try:
            cur_min = int(cm_rt)
        except (TypeError, ValueError):
            pass
    tail_pool = _pool_is_tail_or_post(pool_key, rt)
    is_after_1430 = cur_min >= cfg.layer3_tail_trap_start_minute
    em = _intraday_elapsed_minutes(rt)
    projected_vol_hand = None
    if em is not None and em > 0:
        projected_vol_hand = vol_hand * (cfg.layer3_full_session_minutes / em)
    if (is_after_1430 or tail_pool) and pct_chg_live > cfg.layer3_tail_trap_pct_chg_min:
        if vol_ma5 > 0 and projected_vol_hand is not None:
            if projected_vol_hand < cfg.layer3_tail_trap_vol_proj_vs_ma5_max * vol_ma5:
                penalty += cfg.layer3_penalty_tail_trap
                tags.append("🩹[风险]尾盘诱多")

    # 3) 高位加速透支：仅「大涨 + 高乖离 + 缩量」同时成立才扣分；放量上攻视为强势
    if pct_chg_live > cfg.layer3_overheat_pct_chg_min:
        b20 = _bias_20_from_series(y, now_price)
        if (
            b20 > cfg.layer3_overheat_bias_min
            and vol_ratio < cfg.layer3_overheat_vol_ratio_max
        ):
            penalty += cfg.layer3_penalty_overheat
            tags.append("🩹[风险]高位加速透支")

    # 4) 趋势疲劳：连涨4日 + 当日量 < 昨日量
    if _consecutive_positive_pct_days(df, cfg.layer3_fatigue_up_days):
        if len(df) >= 2:
            v0 = _sf(df.iloc[-1].get("vol"), 0.0)
            v1 = _sf(df.iloc[-2].get("vol"), 0.0)
            if v1 > 0 and v0 < v1:
                penalty += cfg.layer3_penalty_fatigue
                tags.append("🩹[风险]趋势疲劳缩量")

    # 5) 筹码剧烈松动：获利盘大减 + 当日收阴 + 高换手；红盘放量换手视为分歧洗盘不扣分
    if len(df) >= 2:
        wr0 = _sf(rt.get("winner_rate"), _sf(df.iloc[-1].get("winner_rate"), float("nan")))
        wr1 = _sf(df.iloc[-2].get("winner_rate"), float("nan"))
        if np.isfinite(wr0) and np.isfinite(wr1):
            if (
                (wr0 - wr1) <= -cfg.layer3_winner_drop_min
                and pct_chg_live < 0.0
                and turnover_f > cfg.layer3_chip_loosen_turnover_min_pct
            ):
                penalty += cfg.layer3_penalty_chip_loosen
                tags.append("🩹[风险]筹码剧烈松动")

    if is_backtest_legacy_mode():
        kdj_k = _sf(y.get("kdj_k"), float("nan"))
        if np.isfinite(kdj_k) and kdj_k > float(cfg.layer3_legacy_kdj_k_threshold):
            penalty += float(cfg.layer3_legacy_kdj_penalty)
            tags.append("legacy:KDJ超买扣分")

    suggested = float(cfg.base_suggested_min_entry_score + penalty * cfg.penalty_to_min_score_weight)
    suggested = min(95.0, max(40.0, suggested))

    out["penalty"] = round(penalty, 2)
    out["risk_tags"] = tags
    out["suggested_min_entry_score"] = round(suggested, 2)
    out["raw_detail"] = raw_detail
    out["ui_warnings"] = ui_warnings
    return out


def evaluate_3layer_risk_vector(
    df: pd.DataFrame,
    cfg: RiskControlConfig = DEFAULT_RISK_CONFIG,
    is_right_side: Optional[Union[pd.Series, np.ndarray]] = None,
    pool_key_series: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    P5 / 批量截面：对每一行（一只标的在决策日上的特征行）做向量化风控。

    必选/推荐列与标量版一致；向量化第三层「尾盘诱多」依赖下列之一：
        - df['curr_min']：当前交易分钟（如 14*60+35）
        - df['risk_is_tail_or_post']：bool，为 True 视为尾盘/盘后池场景
        - pool_key_series：与 df 索引对齐的池标识（p4/p5 视为尾盘逻辑）

    可选列:
        is_right_side / 参数 is_right_side：第二层是否考核右侧红线。
        macd_hist_prev（或 macd_bar_prev）：昨日 MACD 柱（可用于扩展逻辑，当前二层不组合九转）。
        winner_rate_prev：筹码松动。
        rc_consecutive_up_4：bool，四连涨（含当日）。
        vol_prev：昨日成交量(手)，趋势疲劳。
        intraday_elapsed_mins：已交易分钟数，用于预估全日量（尾盘诱多）。

    返回：与 df 索引对齐，含 pass_layer1, pass_layer2, veto_reason, penalty, risk_tags_str, suggested_min_entry_score。
    """
    n = len(df)
    idx = df.index
    out = pd.DataFrame(index=idx)

    pre = pd.to_numeric(df.get("pre_close", np.nan), errors="coerce")
    close_live = pd.to_numeric(df.get("price_live", df.get("close", np.nan)), errors="coerce")
    op = pd.to_numeric(df.get("open", np.nan), errors="coerce")
    hi = pd.to_numeric(df.get("high", np.nan), errors="coerce")
    lo = pd.to_numeric(df.get("low", np.nan), errors="coerce")

    ma20 = pd.to_numeric(df.get("ma20", np.nan), errors="coerce")
    slope = pd.to_numeric(df.get("ma20_slope_5", np.nan), errors="coerce")
    trf = pd.to_numeric(df.get("turnover_rate_f", np.nan), errors="coerce").fillna(0.0)
    pct = pd.to_numeric(df.get("pct_chg", np.nan), errors="coerce")
    auto_pct = (close_live - pre) / pre.replace(0, np.nan) * 100.0
    pct = pct.where(pct.notna(), auto_pct).fillna(0.0)

    vr = pd.to_numeric(df.get("vol_ratio", np.nan), errors="coerce").fillna(0.0)
    vol = pd.to_numeric(df.get("vol", np.nan), errors="coerce").fillna(0.0)
    vma5 = pd.to_numeric(df.get("vol_ma5", df.get("vma5", np.nan)), errors="coerce").fillna(0.0)

    circ = pd.to_numeric(df.get("circ_mv", np.nan), errors="coerce").fillna(0.0)
    tm = pd.to_numeric(df.get("total_mv", np.nan), errors="coerce").fillna(0.0)
    circ = circ.where(circ > 0, tm * 0.6)

    nm = pd.to_numeric(df.get("net_main_amount", np.nan), errors="coerce").fillna(0.0)
    pe = pd.to_numeric(df.get("pe_ttm", np.nan), errors="coerce")

    float_yuan = circ * 10000.0
    nm_ratio = np.where(float_yuan.values > 1e-6, nm.values / float_yuan.values, 0.0)

    op = op.fillna(close_live)
    hi = hi.fillna(close_live)
    lo = lo.fillna(close_live)

    open_pct = np.where(pre.values > 0, (op.values - pre.values) / pre.values * 100.0, 0.0)

    rng = hi.values - lo.values
    body_top = np.maximum(op.values, close_live.values)
    up_ratio = np.where(rng > 1e-12, (hi.values - body_top) / rng, 0.0)

    l1_trend = (
        (close_live.values < ma20.values)
        & (slope.values < cfg.layer1_ma20_slope_negative_max)
        & (ma20.values > 0)
    )
    l1_turn = (
        (trf.values > cfg.layer1_turnover_f_dump_min_pct) & (pct.values < 0.0)
    ) | (
        (trf.values > cfg.layer1_turnover_f_stagnant_min_pct)
        & (pct.values < cfg.layer1_dump_stagnant_pct_chg_max)
        & (up_ratio > cfg.layer1_dump_stagnant_shadow_min)
    )
    l1_gap = (open_pct > cfg.layer1_open_pct_chg_min_danger) & (pct.values < cfg.layer1_pct_chg_limit_proxy_pct)
    l1_shadow = (
        (up_ratio > cfg.layer1_upper_shadow_ratio_min)
        & (vol.values > cfg.layer1_vol_vs_ma5_mult * vma5.values)
        & (vma5.values > 0)
    )
    l1_main = (nm.values < 0) & (nm_ratio < cfg.layer1_net_main_outflow_ratio_of_float_mv)
    l1_pe = np.isfinite(pe.values) & (pe.values > 0) & (pe.values > cfg.layer1_pe_ttm_max)

    fail = l1_turn | l1_gap | l1_shadow | l1_main | l1_pe
    if cfg.layer1_price_below_ma20_enabled:
        fail = fail | l1_trend

    veto_list: List[str] = []
    for i in range(n):
        if not fail[i]:
            veto_list.append("")
            continue
        msg = ""
        if cfg.layer1_price_below_ma20_enabled and l1_trend[i]:
            msg = "一层[红线]趋势破位：现价<ma20且ma20_slope_5下行"
        elif l1_turn[i]:
            msg = "一层[红线]恶性派发：巨量收跌或巨量滞涨长上影"
        elif l1_gap[i]:
            msg = "一层[红线]危险高开"
        elif l1_shadow[i]:
            msg = "一层[红线]长上影诱多：高上影+超倍量"
        elif l1_main[i]:
            msg = "一层[红线]主力资金出逃"
        elif l1_pe[i]:
            msg = "一层[红线]极端估值：pe_ttm过高"
        veto_list.append(msg)

    wr = pd.to_numeric(df.get("winner_rate", np.nan), errors="coerce")
    if "macd_hist" in df.columns:
        mh = pd.to_numeric(df["macd_hist"], errors="coerce").fillna(0.0)
    elif "macd_bar" in df.columns:
        mh = pd.to_numeric(df["macd_bar"], errors="coerce").fillna(0.0)
    else:
        mh = pd.Series(0.0, index=idx)
    bias_col = pd.to_numeric(df.get("bias_20", np.nan), errors="coerce")
    bias_auto = np.where(ma20.values > 0, (close_live.values - ma20.values) / ma20.values * 100.0, 0.0)
    bias_vals = np.where(bias_col.notna().values, bias_col.values, bias_auto)

    if is_right_side is None and "is_right_side" in df.columns:
        rs = df["is_right_side"].astype(bool).values
    elif is_right_side is not None:
        rs = np.asarray(is_right_side, dtype=bool).reshape(-1)
        if len(rs) == 1 and n > 1:
            rs = np.repeat(rs, n)
        elif len(rs) != n:
            rs = np.zeros(n, dtype=bool)
    else:
        rs = np.zeros(n, dtype=bool)

    wr_min_vec = np.where(
        vr.values >= cfg.layer2_winner_relax_vol_ratio_min,
        cfg.layer2_winner_rate_min_relaxed,
        cfg.layer2_winner_rate_min_strict,
    )
    l2_wr_fail = wr.values < wr_min_vec

    l2_fail = rs & (
        l2_wr_fail
        | (mh.values < cfg.layer2_macd_hist_min)
        | (bias_vals > cfg.layer2_bias_20_max)
        | (vr.values < cfg.layer2_vol_ratio_min)
    )
    pass_l1 = ~fail
    pass_l2 = (~l2_fail) | (~rs)

    for i in range(n):
        if pass_l1[i] and not pass_l2[i]:
            veto_list[i] = "二层[右侧红线]条件未通过"

    out["pass_layer1"] = pass_l1
    out["pass_layer2"] = pass_l2

    penalty = np.zeros(n, dtype=float)
    t_fake = (vr.values < cfg.layer3_fake_rise_vol_ratio_max) & (pct.values > cfg.layer3_fake_rise_pct_chg_min)
    t_oh = (
        (pct.values > cfg.layer3_overheat_pct_chg_min)
        & (bias_vals > cfg.layer3_overheat_bias_min)
        & (vr.values < cfg.layer3_overheat_vol_ratio_max)
    )

    if "curr_min" in df.columns:
        cur_m = pd.to_numeric(df["curr_min"], errors="coerce").fillna(0).values.astype(int)
    else:
        cur_m = np.zeros(n, dtype=int)

    if pool_key_series is not None:
        pk = pool_key_series.reindex(idx).astype(str).str.lower().values
        tail_pool = np.isin(pk, ["p4", "p5", "tail", "postmarket"])
    elif "risk_is_tail_or_post" in df.columns:
        tail_pool = df["risk_is_tail_or_post"].astype(bool).values
    else:
        tail_pool = np.zeros(n, dtype=bool)

    is_after_1430 = cur_m >= cfg.layer3_tail_trap_start_minute
    em = None
    if "intraday_elapsed_mins" in df.columns:
        em = pd.to_numeric(df["intraday_elapsed_mins"], errors="coerce").fillna(0.0).values
    t_tail = np.zeros(n, dtype=bool)
    if em is not None:
        proj = np.where(em > 0, vol.values * (cfg.layer3_full_session_minutes / np.maximum(em, 1.0)), np.nan)
        t_tail = (is_after_1430 | tail_pool) & (pct.values > cfg.layer3_tail_trap_pct_chg_min)
        t_tail = t_tail & (vma5.values > 0) & np.isfinite(proj) & (proj < cfg.layer3_tail_trap_vol_proj_vs_ma5_max * vma5.values)

    ok_soft = pass_l1 & pass_l2
    penalty += np.where(t_fake & ok_soft, cfg.layer3_penalty_fake_rise, 0.0)
    penalty += np.where(t_oh & ok_soft, cfg.layer3_penalty_overheat, 0.0)
    penalty += np.where(t_tail & ok_soft, cfg.layer3_penalty_tail_trap, 0.0)

    if "rc_consecutive_up_4" in df.columns and "vol_prev" in df.columns:
        vprev = pd.to_numeric(df["vol_prev"], errors="coerce").fillna(0.0).values
        fat = df["rc_consecutive_up_4"].astype(bool).values & (vol.values < vprev) & (vprev > 0)
        penalty += np.where(fat & ok_soft, cfg.layer3_penalty_fatigue, 0.0)
    if "winner_rate_prev" in df.columns:
        wrp = pd.to_numeric(df["winner_rate_prev"], errors="coerce")
        t_chip = (
            ((wr.values - wrp.values) <= -cfg.layer3_winner_drop_min)
            & (pct.values < 0.0)
            & (trf.values > cfg.layer3_chip_loosen_turnover_min_pct)
        )
        penalty += np.where(
            t_chip & np.isfinite(wr.values) & np.isfinite(wrp.values) & ok_soft,
            cfg.layer3_penalty_chip_loosen,
            0.0,
        )

    legacy_kdj = np.zeros(n, dtype=bool)
    if is_backtest_legacy_mode() and "kdj_k" in df.columns:
        kdj_k_vec = pd.to_numeric(df["kdj_k"], errors="coerce").values
        legacy_kdj = np.isfinite(kdj_k_vec) & (kdj_k_vec > float(cfg.layer3_legacy_kdj_k_threshold))
        penalty = penalty + np.where(legacy_kdj & ok_soft, float(cfg.layer3_legacy_kdj_penalty), 0.0)

    tag_matrix: List[str] = []
    for i in range(n):
        tags_i: List[str] = []
        if ok_soft[i]:
            if t_fake[i]:
                tags_i.append("🩹[风险]无量虚涨")
            if t_tail[i]:
                tags_i.append("🩹[风险]尾盘诱多")
            if t_oh[i]:
                tags_i.append("🩹[风险]高位加速透支")
            if "rc_consecutive_up_4" in df.columns and "vol_prev" in df.columns:
                vprev = float(pd.to_numeric(df["vol_prev"], errors="coerce").iloc[i] or 0.0)
                if bool(df["rc_consecutive_up_4"].iloc[i]) and vprev > 0 and vol.values[i] < vprev:
                    tags_i.append("🩹[风险]趋势疲劳缩量")
            if "winner_rate_prev" in df.columns:
                w0 = float(wr.iloc[i]) if not pd.isna(wr.iloc[i]) else float("nan")
                w1 = float(wrp.iloc[i]) if not pd.isna(wrp.iloc[i]) else float("nan")
                if (
                    np.isfinite(w0)
                    and np.isfinite(w1)
                    and (w0 - w1) <= -cfg.layer3_winner_drop_min
                    and pct.values[i] < 0.0
                    and trf.values[i] > cfg.layer3_chip_loosen_turnover_min_pct
                ):
                    tags_i.append("🩹[风险]筹码剧烈松动")
            if legacy_kdj[i]:
                tags_i.append("legacy:KDJ超买扣分")
        tag_matrix.append("|".join(tags_i))

    out["penalty"] = penalty
    out["veto_reason"] = veto_list
    out["risk_tags_str"] = tag_matrix
    out["suggested_min_entry_score"] = np.clip(
        cfg.base_suggested_min_entry_score + penalty * cfg.penalty_to_min_score_weight,
        40.0,
        95.0,
    )
    return out


def merge_risk_into_engine_result(
    base: Dict[str, Any],
    risk: Dict[str, Any],
    penalty_key: str = "penalty",
) -> Dict[str, Any]:
    """
    将第三层风控结果并入引擎输出：
    - 将风控 penalty 与引擎原有 penalty 相加写入 penalty_key；
    - 将 burst_score 扣除风控 penalty（软降权，最低 0）；
    - 写入 risk_control 摘要、risk_tags、suggested_min_entry_score 供 UI 展示。
    """
    base = dict(base)
    p_engine = float(base.get(penalty_key, 0.0) or 0.0)
    p_risk = float(risk.get("penalty", 0.0) or 0.0)
    base[penalty_key] = round(p_engine + p_risk, 2)

    base["risk_control"] = {
        "pass_layer1": bool(risk.get("pass_layer1")),
        "pass_layer2": bool(risk.get("pass_layer2")),
        "veto_reason": str(risk.get("veto_reason", "") or ""),
        "penalty": p_risk,
        "risk_tags": list(risk.get("risk_tags", []) or []),
        "suggested_min_entry_score": float(risk.get("suggested_min_entry_score", 0.0) or 0.0),
        "ui_warnings": list(risk.get("ui_warnings", []) or []),
    }

    tags_existing = base.get("risk_tags")
    if not isinstance(tags_existing, list):
        tags_existing = []
    base["risk_tags"] = tags_existing + list(risk.get("risk_tags", []) or [])
    base["suggested_min_entry_score"] = float(risk.get("suggested_min_entry_score", 0.0) or 0.0)

    bs = float(base.get("burst_score", 0.0) or 0.0)
    base["burst_score"] = round(max(0.0, min(100.0, bs - p_risk)), 2)
    return base
