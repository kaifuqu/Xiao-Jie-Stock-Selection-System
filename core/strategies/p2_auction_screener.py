# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 — P2 竞价选股池「物理胸甲」核心筛选模块
================================================================================
本模块严格对齐 data_fetcher.ALL_55_COLS 字段契约，实现：

  【一】全局风控一票否决（先于四大策略执行）
  【二】四大独立竞价策略（满足任一即入池，并打策略标签）

----------------------------------------------------------------------
增量更新约定（务必遵守）
----------------------------------------------------------------------
1. 调用方只应传入「截至上一交易日 T-1 已收盘」的历史日线 df（按 ts_code 升序、trade_date 升序），
   再加上「当日 T」竞价/快照 rt（open、pre_close、vol_ratio、amount、turnover_rate_f 等）。
2. 本模块内部 **不会** 发起网络请求、**不会** 合并全市场历史、**不会** 触发 DuckDB 全表扫描；
   单日全市场筛选时，由上层按代码列表循环，每只股票只加载各自需要的 df 尾部窗口即可。
3. 批量接口 screen_p2_universe() 仅对内存中的 (df, rt) 列表做向量化/循环筛选，适合「只算今天」的日批任务。

----------------------------------------------------------------------
量纲说明（与 TuShare / 本项目落库一致）
----------------------------------------------------------------------
- circ_mv、total_mv：万元；100 亿 = 1,000,000 万元。
- net_elg_amount、net_main_amount、inst_net_buy、hk_vol 等资金字段：元（本项目 moneyflow 已 *10000）。
- strth：涨停/连板强度类字段，本模块按「万元」口径与策略四文案对齐（若数据源为其它量纲，请通过 P2ScreenerConfig 调整）。
- daily.amount：TuShare 日线成交额多为「千元」；竞价 rt['amount'] 依行情源可能为「元」或其它。
  「防飞刀·额萎缩」在无法确认量纲或缺失时 **不强行否决**，避免误杀（见 _auction_amount_shrinked）。

================================================================================
交钥匙部署指南（摘要 — 详见项目 README 或团队 Wiki 时可复制本节）
================================================================================
1) 文件位置：core/strategies/p2_auction_screener.py（本文件）
2) 引擎接入：core/strategies/strat_p2_auction.py 中 P2Auction.run_all 已调用
   evaluate_p2_screener()；无需再改数据管道。
3) 扫描集成：core/scan_engine.py 对 P2 增加 hit_res['p2_core_screener_pass']，
   在通过四大策略之一时放宽 strict_golden_burst_ok 的硬性门槛（否则策略三 0.5% 高开会被误杀）。
4) 调用示例（单股、增量一日）：
       from core.strategies.p2_auction_screener import evaluate_p2_screener
       r = evaluate_p2_screener(df_hist_tail, rt_snapshot)
       if r["strategies"]:
           print(r["strategies"], r["detail"])
5) 批量（多股、仍是一日增量）：
       from core.strategies.p2_auction_screener import screen_p2_universe
       df_report = screen_p2_universe(list_of_tuples)  # 每项 (ts_code, df, rt)
6) 运行注意：
   - df 至少 21 行（high_20、均线交叉等更稳）；不足时部分策略自动不满足，不抛异常。
   - 竞价阶段 vol_ratio、amount 若尚未落地，防飞刀与量能相关条件可能放宽跳过。
   - 调参只改 P2ScreenerConfig 实例，勿改 55 维字段名。
================================================================================
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

try:
    from core.strategies.fund_mv_utils import (
        circ_mv_wan_from,
        dynamic_inst_single_threshold_yuan,
        dynamic_net_main_threshold_yuan,
        dynamic_strth_threshold_wan,
        effective_turnover_rate_f,
    )
except ImportError:
    from strategies.fund_mv_utils import (  # type: ignore
        circ_mv_wan_from,
        dynamic_inst_single_threshold_yuan,
        dynamic_net_main_threshold_yuan,
        dynamic_strth_threshold_wan,
        effective_turnover_rate_f,
    )

logger = logging.getLogger(__name__)

try:
    import constants as _C
    _P1_MIN_CIRC_MV_WAN = float(getattr(_C, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000))
except Exception:
    _P1_MIN_CIRC_MV_WAN = 1_000_000.0


# ---------------------------------------------------------------------------
# 可调参数集中配置（实盘可按回测结果微调，避免魔法数散落在分支里）
# ---------------------------------------------------------------------------
@dataclass
class P2ScreenerConfig:
    """P2 竞价筛选阈值配置（全部带默认值，可直接实例化后改字段）。"""

    # --- 全局风控 ---
    # 【V26.7 修复】流通市值下限强制提升至 100 亿（1,000,000 万元）。
    # 配置文件 constants.P1_SELECT_MIN_CIRC_MV_WAN 穿透时优先使用；
    # 即使配置穿透失败，底层默认值也从 60 亿提升至 100 亿，彻底封死袖珍盘漏洞。
    # A股袖珍盘（小票）容易被资金操控、流动性差、波动剧烈，实盘风险极高。
    circ_mv_min_wan: float = _P1_MIN_CIRC_MV_WAN
    # 超大盘加权阈值（万元）：高于此值在结果 detail 中记 sort_weight_bonus，供排序使用
    circ_mv_prefer_wan: float = 5_000_000.0
    # 开盘乖离：用开盘价相对昨收 ma20 计算，超过则否决
    open_bias20_max_pct: float = 7.5
    # 竞价高开幅度上限（相对昨收 %）
    auction_pct_chg_max: float = 5.5
    # 昨日换手率下限（%）
    # 真实换手 turnover_rate_f 通常高于总股本换手，阈值略高于旧版 1%
    prev_turnover_min_pct: float = 2.5

    # --- 策略一：主升浪确认 ---
    s1_prev_pct_low: float = 4.0
    s1_prev_pct_high: float = 6.0
    s1_prev_vol_ratio_min: float = 1.8
    # 【V26.6 优化】昨日 MACD 柱须为正（昨日要有上涨动能）
    s1_prev_macd_hist_positive: bool = True
    # 昨日获利盘下限（筹码集中度门槛，>85 表示大部分筹码盈利）
    s1_winner_min: float = 85.0
    # 今日竞价量比下限（昨量比 vs 今量比，涨跌停时跳过此检查）
    s1_vol_ratio_min: float = 1.8
    s1_auction_pct_low: float = 1.0
    s1_auction_pct_high: float = 3.0
    # 【V26.6 新增】昨 ATR 超限否决阈值
    s1_atr_pct_max: float = 8.0

    # --- 策略二：机构连贯发力 ---
    s2_prev_pct_low: float = 4.0
    s2_prev_pct_high: float = 6.0
    s2_pct_low: float = 0.5
    s2_pct_high: float = 2.5
    s2_auction_pct_low: float = 0.5
    s2_auction_pct_high: float = 2.5
    s2_auction_turnover_min_pct: float = 0.5
    s2_upper_shadow_max_pct: float = 2.5
    s2_inst_net_buy_ratio_of_float_mv: float = 0.00006
    s2_prev_vol_ratio_min: float = 1.8

    # --- 策略三：无尽苍穹 ---
    s3_winner_min: float = 90.0
    s3_pct_low: float = 1.0
    s3_pct_high: float = 4.0
    s3_vol_ratio_min: float = 1.5
    s3_atr_pct_max: float = 8.0
    # 竞价额相对昨日全天成交额比例低于此值视为「异常萎缩」（量纲一致时生效）
    s3_auction_amount_vs_prev_ratio_min: float = 0.05
    # 【V26.6 优化】P2-03 涨跌停宽容：涨停时换手率下限适当放宽（涨停日封板后换手自然降低）
    s3_auction_turnover_min_pct: float = 0.4
    # 【V26.6 优化】P2-03 长上影容忍（涨停日上影正常存在）
    s3_upper_shadow_max_pct: float = 2.5
    # 【V26.6 修复】策略三要求连续2日龙虎榜机构净买达动态门槛
    s3_inst_net_buy_ratio_of_float_mv: float = 0.00006

    # --- 策略四：底仓重金点火（流通市值比例 + 阶梯地板，见 fund_mv_utils）---
    s4_circ_mv_min_wan: float = 5_000_000.0
    s4_net_main_ratio_of_float_mv: float = 0.00018
    s4_pct_low: float = 2.0
    s4_pct_high: float = 4.0
    s4_strth_ratio_of_circ_mv_wan: float = 0.0004

    # --- P2 新主线增强：板块 / 分时生死线 / 次日预期 ---
    regime_strict_boost: float = 1.12
    regime_relaxed_boost: float = 0.92
    board_beta_core_min: float = 1.15
    board_beta_hot_min: float = 1.05
    board_beta_cold_max: float = 0.90
    open_confirm_minute_low: int = 573
    open_confirm_minute_high: int = 575
    open_confirm_vwap_eps: float = 0.002
    open_confirm_penalty_miss: float = 18.0
    open_confirm_bonus_hit: float = 8.0
    # 【V26.7 新增】竞价确认时间窗口模式（详见 _open_confirm_window 注释）
    # "09:35盘初确认"（默认）：09:33~09:35 区间判定，反映盘初蓄力阶段（推荐）
    # "09:25纯竞价"：严格 09:25 集合竞价结束时刻判定（curr_min == 545），适合竞价超预期策略
    open_confirm_mode: str = "09:35盘初确认"
    vwap_death_gap_min_pct: float = 0.6
    vwap_death_penalty: float = 25.0
    vwap_death_hard_pct: float = 1.8
    t1_memory_weight: float = 0.20
    t1_memory_min_samples: int = 6
    t1_memory_boost_max: float = 10.0
    t1_memory_penalty_max: float = 8.0


def _resolve_p2_cfg(cfg: Optional[P2ScreenerConfig]) -> P2ScreenerConfig:
    if cfg is not None:
        return cfg
    from core.config_manager import get_p2_screener_config

    return get_p2_screener_config()


def _regime_bucket(rt: Dict[str, Any]) -> str:
    s = str(rt.get("_regime_state", rt.get("regime", "")) or "")
    if any(k in s for k in ["主升", "趋势"]):
        return "strict"
    if any(k in s for k in ["退潮", "空头", "主跌"]):
        return "relaxed"
    return "neutral"


def _board_beta(rt: Dict[str, Any]) -> float:
    for k in ("sector_beta", "industry_beta", "sector_mult"):
        v = _safe_float(rt.get(k), 0.0)
        if v > 0:
            return max(0.7, min(1.5, v))
    return 1.0


def _mainline_sector_score(rt: Dict[str, Any], board_beta: float) -> Tuple[float, str]:
    """主线板块识别：偏向强板块中最强、最稳的那一类。"""
    sector_strength = _safe_float(rt.get("sector_strength", rt.get("industry_strength", board_beta)), board_beta)
    sector_rank = _safe_float(rt.get("sector_rank"), 999.0)
    sector_total = _safe_float(rt.get("sector_total"), 0.0)
    sector_advance = _safe_float(rt.get("sector_advance_ratio"), 0.0)
    sector_vwap_gap = _safe_float(rt.get("sector_vwap_gap_pct"), 0.0)

    score = 0.0
    reasons = []
    if sector_strength >= 1.15:
        score += 3.0
        reasons.append("强度>1.15")
    elif sector_strength >= 1.05:
        score += 1.8
        reasons.append("强度>1.05")
    elif sector_strength <= 0.95:
        score -= 2.5
        reasons.append("强度偏弱")

    if sector_advance >= 0.55:
        score += 1.5
        reasons.append("上涨家数占优")
    elif sector_advance > 0 and sector_advance < 0.35:
        score -= 1.2
        reasons.append("上涨家数不足")

    if sector_vwap_gap > 0.8:
        score += 1.0
        reasons.append("板块VWAP上方")
    elif sector_vwap_gap < -0.6:
        score -= 1.0
        reasons.append("板块VWAP下方")

    if sector_total >= 3 and sector_rank > 0:
        if sector_rank <= 2:
            score += 2.0
            reasons.append("板块前排")
        elif sector_rank <= 5:
            score += 1.0
            reasons.append("板块前列")
        elif sector_rank > sector_total - 3:
            score -= 2.0
            reasons.append("板块后排")

    return float(score), "/".join(reasons) if reasons else "中性"


def _open_confirm_window(rt: Dict[str, Any], cfg: P2ScreenerConfig) -> bool:
    """
    【V26.7 业务注释】竞价时间窗口判定。

    支持两种模式（由 cfg.open_confirm_mode 配置）：

    1. "09:35盘初确认"（默认）：
       在 09:33~09:35 区间（curr_min: 573~575）判定竞价是否持续强于 VWAP。
       A股集合竞价 09:15~09:25 结束后，09:30 开始连续交易。
       09:33~09:35 是盘初前3分钟的分时表现：
       - 能过滤集合竞价最后一秒的大单拉抬（虚价信号）
       - 能捕捉开盘后资金快速拉升前的蓄力阶段（真实意愿）
       - 这是"盘初分时确认"而非"纯竞价池"，适合与 P2 四大策略配合筛选真强势股
       当前实现即为此模式。

    2. "09:25纯竞价"：
       严格在 09:25（curr_min == 545）集合竞价结束时刻判定。
       仅以集合竞价结果为依据，适合专门筛选"竞价超预期"形态的独立策略。
       注意：09:25 数据在部分实时行情源中可能不单独推送，
       此时若 curr_min 处于集合竞价窗口（495~545），会尝试宽松判定。

    参数:
        rt: 实时快照，需含 curr_min（当前分钟数，如 573 表示 09:33）
        cfg: P2ScreenerConfig，含 open_confirm_mode / open_confirm_minute_low / high

    返回:
        True 表示当前处于竞价确认时间窗口内
    """
    m = rt.get("curr_min")
    if m is None:
        return False
    try:
        mi = int(float(m))
    except Exception:
        return False

    mode = str(getattr(cfg, "open_confirm_mode", "09:35盘初确认") or "09:35盘初确认")

    if mode == "09:25纯竞价":
        # 严格在 09:25 集合竞价结束时刻（curr_min == 545）
        if mi == 545:
            return True
        # 若行情源将 09:25 快照以 09:30 推送（curr_min == 550），做1分钟容错
        if 545 <= mi <= 550:
            return True
        return False

    # 默认模式：09:35盘初确认（09:33~09:35 区间）
    return int(cfg.open_confirm_minute_low) <= mi <= int(cfg.open_confirm_minute_high)


def _estimate_vwap_from_rt(rt: Dict[str, Any], ref_price: float, fallback_price: float = 0.0) -> float:
    """
    【V26.7 安全升级】盘中VWAP估算。
    量纲：东财amount=元，volume=股 → tentative=元/股 = 正确VWAP。
    若偏离ref_price超过20% → 尝试把手→股修正 → 仍不合理则降级fallback。
    """
    amt = _safe_float(rt.get("amount"), 0.0)
    vol = _safe_float(rt.get("volume"), 0.0)
    if amt <= 0 or vol <= 0:
        return fallback_price
    tentative = amt / max(vol, 1e-9)
    # 20%相对偏差校验（替代旧版 > price*20 的绝对判断，更稳健）
    if ref_price > 0 and abs(tentative - ref_price) / ref_price > 0.20:
        vol_as_hand = vol * 100.0
        corrected = amt / max(vol_as_hand, 1e-9)
        if abs(corrected - ref_price) / ref_price <= 0.20:
            return corrected
        return fallback_price
    return tentative


def _open_vwap_support_ok(rt: Dict[str, Any], now_px: float, cfg: P2ScreenerConfig) -> bool:
    vw = _estimate_vwap_from_rt(rt, now_px, fallback_price=now_px)
    if vw <= 0 or now_px <= 0:
        return True
    return now_px >= vw * (1.0 - cfg.open_confirm_vwap_eps)


def _p2_collect_t1_memory(rt: Dict[str, Any]) -> Tuple[float, float, float]:
    mem = rt.get("_t1_memory") if isinstance(rt.get("_t1_memory"), dict) else {}
    avg_ret = _safe_float(mem.get("avg_ret_t1_pct", 0.0), 0.0)
    win_rate = _safe_float(mem.get("win_rate_t1_pct", 0.0), 0.0)
    sample_n = _safe_float(mem.get("sample_n", 0.0), 0.0)
    return avg_ret, win_rate, sample_n


def _p2_t1_memory_score(avg_ret: float, win_rate: float, sample_n: float, cfg: P2ScreenerConfig) -> float:
    if sample_n < cfg.t1_memory_min_samples:
        return 0.0
    score = 0.0
    if avg_ret > 0:
        score += min(cfg.t1_memory_boost_max, avg_ret * cfg.t1_memory_weight)
    else:
        score -= min(cfg.t1_memory_penalty_max, abs(avg_ret) * cfg.t1_memory_weight)
    if win_rate >= 55.0:
        score += min(cfg.t1_memory_boost_max * 0.6, (win_rate - 50.0) * 0.10)
    elif win_rate < 45.0:
        score -= min(cfg.t1_memory_penalty_max * 0.6, (45.0 - win_rate) * 0.12)
    return float(score)


# 兼容旧 import；运行时应使用 _resolve_p2_cfg(None) 或 ConfigManager
DEFAULT_P2_CONFIG = P2ScreenerConfig()


def _safe_float(val: Any, default: float = 0.0) -> float:
    """安全浮点转换：与 scan_engine / strat_base 行为保持一致。"""
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


def _detect_limit_up(
    ts_code: str,
    open_px: float,
    pre_close: float,
    rt: Dict[str, Any],
) -> str:
    """
    【V26.7 新增】动态涨跌停判定：按 A 股板块规则精确计算涨停价。
    - 60/00 开头：主板，涨停幅度 10%（部分 ST 为 5%）
    - 688 开头：科创板，涨停幅度 20%
    - 300 开头：创业板，涨停幅度 20%
    - 北交所（83/87/43开头）：涨停幅度 30%
    - 4开头（北交所）：涨停幅度 30%
    - 其他（如8开头新三板精选层等）：默认 10%
    - 若 rt['_is_limit'](上层已计算)存在则直接返回，避免重复计算
    - 只有当 open_px >= 动态涨停价时才判定为 UP，否则返回空串
    - 除权除息日 pre_close 可能与日线 close 差异极大，此时不轻信 pre_close，
      改用日线 close 估算（若两者差距 > 20%，说明可能已发生除权，跳过涨跌停判定）
    """
    if str(rt.get("_is_limit", "")) in ("UP", "DOWN"):
        return str(rt["_is_limit"])

    if not ts_code or open_px <= 0 or pre_close <= 0:
        return ""

    code_base = ts_code.split(".")[0] if "." in ts_code else str(ts_code).strip()

    # 判断涨跌停幅度
    if code_base.startswith("688"):
        limit_pct = 0.20
    elif code_base.startswith("300"):
        limit_pct = 0.20
    elif code_base.startswith("4") and len(code_base) == 4:
        limit_pct = 0.30
    elif code_base.startswith(("83", "87", "43")):
        limit_pct = 0.30
    elif code_base.startswith(("60", "00")):
        limit_pct = 0.10
    else:
        limit_pct = 0.10

    # 【V26.7 修复】除权除息日容错：当 pre_close 与日线 close 差距过大时（>20%），
    # 说明 pre_close 可能未复权，此时不依据 pre_close 计算涨跌停，改用日线 close 估算。
    y_close = _safe_float(rt.get("_y_close"), 0.0)
    if y_close > 0 and pre_close > 0:
        ratio_diff = abs(pre_close - y_close) / y_close
        if ratio_diff > 0.20:
            return ""
    elif y_close <= 0:
        return ""

    limit_price = pre_close * (1.0 + limit_pct)
    if open_px >= limit_price:
        return "UP"
    return ""


def _macd_hist_row(row: pd.Series) -> float:
    """统一 MACD 柱：优先 macd_hist，其次 macd_bar（兼容不同版本列名）。"""
    if row is None or row.empty:
        return 0.0
    v = row.get("macd_hist", np.nan)
    if pd.isna(v) or _safe_float(v, 0.0) == 0.0:
        v = row.get("macd_bar", 0.0)
    return _safe_float(v, 0.0)


def _open_bias20_pct(open_px: float, ma20: float) -> float:
    """以开盘价估算的 20 日乖离率（分母为昨日均线 ma20）。"""
    if ma20 <= 0 or open_px <= 0:
        return 0.0
    return (open_px - ma20) / ma20 * 100.0


def _auction_pct_vs_preclose(open_px: float, pre_close: float) -> float:
    """竞价涨幅 %：相对昨收（或 pre_close）。"""
    if pre_close <= 0 or open_px <= 0:
        return 0.0
    return (open_px - pre_close) / pre_close * 100.0


def _vol_ma_cross_upward(df: pd.DataFrame) -> bool:
    """
    量能均线「上穿」判定（在 T-1 收盘时刻下可计算的部分）。

    业务含义：用最近两根已收盘 K 上的 vol_ma5 / vol_ma20，判断是否发生由下转上的金叉。
    竞价时刻无法预知当日全日量，因此不臆造「预估成交量」，只使用已落地日线量能均线。
    """
    if df is None or len(df) < 2:
        return False
    if "vol_ma5" not in df.columns or "vol_ma20" not in df.columns:
        return False
    a = df.iloc[-2]
    b = df.iloc[-1]
    v5a, v20a = _safe_float(a.get("vol_ma5")), _safe_float(a.get("vol_ma20"))
    v5b, v20b = _safe_float(b.get("vol_ma5")), _safe_float(b.get("vol_ma20"))
    if v5a <= 0 or v20a <= 0 or v5b <= 0 or v20b <= 0:
        return False
    return (v5a <= v20a) and (v5b > v20b)


def _auction_amount_shrinked(rt: Dict[str, Any], prev_row: pd.Series, cfg: P2ScreenerConfig) -> bool:
    """
    【V26.7 修复】策略一防飞刀：竞价成交额相对昨日全天额是否「异常萎缩」。

    量纲不一致问题（A 股数据源常见）：
    - TuShare 日线 amount：通常为「千元」单位
    - 实时行情快照 rt['amount']：不同行情源量纲不同，可能为「元」或「千元」
    - 若直接比较两者而不做量纲对齐，会产生 1000 倍偏差，导致竞价额被误判为「极度萎缩」

    健壮处理策略：
    1. 任一字段缺失/为 0 → 跳过（不否决）
    2. 竞价额 > 日线额 × 10 → 量纲明显不一致（可能是元 vs 千元），跳过
    3. 竞价额 < 日线额 × 0.001 → 同样量纲不一致，跳过
    4. 量纲对齐后：ratio < 阈值（如 5%）且绝对值极小 → 才触发飞刀

    返回 True 表示「异常萎缩，应剔除」；返回 False 表示「通过或无法判断」。
    """
    rt_amt = _safe_float(rt.get("amount"), 0.0)
    prev_amt = _safe_float(prev_row.get("amount"), 0.0)

    # 步骤一：基础空值检查，任一缺失直接跳过
    if rt_amt <= 0 or prev_amt <= 0:
        return False

    # 步骤二：量纲一致性检查（核心修复）
    # TuShare 日线 amount 通常为千元，实时行情可能为元或千元，差异可能高达 1000 倍。
    # 若竞价额显著大于日线额（>10倍），说明量纲不一致（元 vs 千元），跳过不否决。
    # 同理，若竞价额异常小于日线额（<千分之一），也可能是量纲错位，跳过。
    if rt_amt > prev_amt * 10.0:
        return False
    if rt_amt < prev_amt * 0.001:
        return False

    # 步骤三：量纲对齐后计算比例
    # 此时两者量纲一致（均为元或均为千元），计算相对萎缩比例
    ratio = rt_amt / prev_amt
    thr = float(cfg.s1_auction_amount_vs_prev_ratio_min)

    # 步骤四：若竞价额已经大于昨日全天（量纲已确认一致），视为正常，不杀
    if ratio >= 1.0:
        return False

    # 步骤五：比例低于阈值才触发飞刀剔除
    # 正常情况下竞价额约为日线额 5%~20%，若远低于 5% 说明资金参与度极低
    return ratio < thr


def p2_global_risk_veto(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: Optional[P2ScreenerConfig] = None,
) -> Tuple[bool, str]:
    """
    P2 全局风控（一票否决）。

    返回:
        (True, "")  — 通过，可进入四大策略
        (False, 原因文案) — 不通过

    字段来源:
        - 体量、估值、换手、均线斜率：优先取 df 最后一根（T-1 收盘）
        - 开盘价、竞价涨幅、乖离：用 rt['open'] 与昨收 ma20
    """
    cfg = _resolve_p2_cfg(cfg)
    if df is None or df.empty:
        return False, "历史K线为空"

    y = df.iloc[-1]

    circ_mv = _safe_float(rt.get("circ_mv"), _safe_float(y.get("circ_mv"), 0.0))
    if circ_mv < cfg.circ_mv_min_wan:
        return False, f"流通市值过小 circ_mv={circ_mv:.0f}万 < {cfg.circ_mv_min_wan:.0f}万"

    open_px = _safe_float(rt.get("open"), 0.0)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    auc_pct = _auction_pct_vs_preclose(open_px, pre_close)

    ma60 = _safe_float(y.get("ma60"), 0.0)
    y_close = _safe_float(y.get("close"), 0.0)
    price_ref = open_px if open_px > 0 else _safe_float(rt.get("price"), y_close)
    if ma60 > 0 and y_close > 0 and y_close <= ma60:
        return False, "左侧/下行: 昨收 close <= ma60"

    ma20_slope_5 = _safe_float(y.get("ma20_slope_5"), 0.0)
    if ma20_slope_5 <= 0:
        return False, f"20日均线斜率 ma20_slope_5={ma20_slope_5:.4f} <= 0"

    ma20 = _safe_float(y.get("ma20"), 0.0)
    bias_open = _open_bias20_pct(open_px, ma20)
    # 【V26.7 修复】涨跌停宽容：_is_limit 由 evaluate_p2_screener 入口处的 _detect_limit_up 统一计算，
    # 此处直接读取。_detect_limit_up 按股票代码前缀动态判断涨停幅度（主板10%、科创/创业20%、北交所30%）。
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"
    bias_limit = 15.0 if is_limit_up else cfg.open_bias20_max_pct
    if bias_open > bias_limit:
        return False, f"极端乖离开局 bias20(open)={bias_open:.2f}% > {bias_limit:.2f}%"

    auc_limit = 10.0 if is_limit_up else cfg.auction_pct_chg_max
    if auc_pct > auc_limit:
        return False, f"假突破陷阱 竞价高开 pct={auc_pct:.2f}% > {auc_limit:.2f}%"

    # 【V26.7 修复】涨跌停宽容：_is_limit 由 evaluate_p2_screener 统一写入，此处直接读取
    if not is_limit_up:
        wr_y = _safe_float(y.get("winner_rate"), 0.0)
        if auc_pct >= 3.0 and wr_y < 82.0:
            return False, "高开陷阱: 竞价涨幅≥3%但昨获利盘不足82%(易遭抛压)"

    # 【V26.7 修复】涨跌停宽容：_is_limit 由 evaluate_p2_screener 统一写入，此处直接读取
    if not is_limit_up:
        if bias_open > 6.0 and auc_pct >= 2.5:
            px_a = open_px if open_px > 0 else _safe_float(rt.get("price"), y_close)
            est_to = effective_turnover_rate_f(rt, y, px_a if px_a > 0 else y_close)
            if est_to > 22.0:
                return False, "高开陷阱: 乖离过大且竞价换手畸高(疑似出货)"

    pe_ttm = _safe_float(y.get("pe_ttm"), 0.0)
    if pe_ttm < 0:
        return False, "市盈率 pe_ttm < 0"

    prev_to = effective_turnover_rate_f({}, y, y_close)
    if prev_to < cfg.prev_turnover_min_pct:
        return False, f"昨日真实换手过低 turnover_rate_f={prev_to:.2f}% < {cfg.prev_turnover_min_pct}%"

    return True, ""


def _strategy_main_wave_confirm(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P2ScreenerConfig,
) -> Tuple[bool, str]:
    """
    策略一：主升浪确认（量价引擎共振接力）

    昨日:
        pct_chg 4%~6%, macd_hist > 0, vol_ratio >= 1.8, winner_rate > {s1_winner_min}%, 昨收站稳 MA5
    今日竞价:
        open > ma5, pct 1%~3%, 量比 >= 1.8
    均线:
        vol_ma5 上穿 vol_ma20（在昨收可判定意义下）
    防飞刀:
        昨日 net_main_amount < 0 则剔除, atr_pct > 8% 则剔除
    """
    # 【V26.6 A股涨跌停宽容】涨停时相关条件做相应放宽
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    # 【V26.6 优化】新增：昨日 MACD 柱须为正（昨日要有上涨动能）
    mh = _macd_hist_row(y)
    if mh <= 0:
        return False, "昨日 macd_hist 未为正"

    open_px = _safe_float(rt.get("open"), 0.0)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    pct = _auction_pct_vs_preclose(open_px, pre_close)
    vr = _safe_float(rt.get("vol_ratio"), 0.0)

    wr = _safe_float(y.get("winner_rate"), _safe_float(rt.get("winner_rate"), 0.0))
    c95 = _safe_float(y.get("cost_95th"), _safe_float(rt.get("cost_95th"), 0.0))
    h20 = _safe_float(y.get("high_20"), 0.0)

    # 昨收须站稳 MA5（竞价前日须处于上升趋势中）
    y_close_local = _safe_float(y.get("close"), 0.0)
    ma5_y = _safe_float(y.get("ma5"), 0.0)
    if ma5_y > 0 and y_close_local <= ma5_y:
        return False, "昨收未站稳 ma5"

    if not (wr > cfg.s1_winner_min):
        return False, f"winner_rate({wr:.1f}%) 未超 {cfg.s1_winner_min}%"
    if c95 <= 0 or open_px <= c95:
        return False, "开盘未过 cost_95th"
    if h20 <= 0 or open_px <= h20:
        return False, "开盘未过 high_20"
    # 【V26.6 A股涨跌停宽容】涨停时涨幅上限和量比要求放宽
    pct_high_s1 = 9.5 if is_limit_up else cfg.s1_auction_pct_high
    if not (cfg.s1_pct_low <= pct <= pct_high_s1):
        return False, f"竞价涨幅不在 {cfg.s1_pct_low}%~{pct_high_s1}%"
    if not is_limit_up and vr < cfg.s1_vol_ratio_min:
        return False, "量比不足"

    atrp = _safe_float(y.get("atr_pct"), 0.0)
    if atrp > cfg.s1_atr_pct_max:
        return False, f"防飞刀 atr_pct={atrp:.2f}% > {cfg.s1_atr_pct_max}%"

    # 【V26.6 A股涨跌停宽容】涨停时竞价额萎缩是正常现象
    if not is_limit_up and _auction_amount_shrinked(rt, y, cfg):
        return False, "防飞刀 竞价额相对昨日萎缩"

    return True, "OK"


def _strategy_institution_streak(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P2ScreenerConfig,
) -> Tuple[bool, str]:
    """
    策略二：机构连贯发力（长线资金护航高开）

    昨日:
        pct_chg 4%~6%（涨停时放宽到9.5%）, macd_hist > 0, vol_ratio >= 1.8
    趋势:
        ma5 > ma20 > ma60（昨日收盘均线多头）
    今日竞价:
        open > ma5 且站稳 ma10, pct {s2_pct_low}%~{s2_pct_high}%, 预估开盘 turnover_rate_f > {s2_auction_turnover_min_pct}%
        MACD 须呈水上金叉（diff > dea > 0）
        【涨跌停时 pct 上限放宽到 9.5%】
    防飞刀:
        昨日长上影 (high-close)/close > {s2_upper_shadow_max_pct}%
    """
    # 【V26.6 A股涨跌停宽容】涨停时相关条件做相应放宽
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    y_pct = _safe_float(y.get("pct_chg"), 0.0)
    y_vr = _safe_float(y.get("vol_ratio"), 0.0)
    mh = _macd_hist_row(y)
    # 【V26.6 A股涨跌停宽容】昨日涨停时，涨幅上限放宽到9.5%
    pct_high_prev = 9.5 if is_limit_up else cfg.s2_prev_pct_high
    if not (cfg.s2_prev_pct_low <= y_pct <= pct_high_prev):
        return False, f"昨日涨幅不在 {cfg.s2_prev_pct_low}%~{pct_high_prev}%"
    if mh <= 0:
        return False, "昨日 macd_hist 未为正"
    if y_vr < cfg.s2_prev_vol_ratio_min:
        return False, "昨日量比不足 1.8"

    # 【V26.6 优化】新增：昨日长上影防飞刀（与P3/P4/P5保持一致）
    y_high = _safe_float(y.get("high"), 0.0)
    y_close = _safe_float(y.get("close"), 0.0)
    if y_close > 0:
        upper = (y_high - y_close) / y_close * 100.0
        # 【V26.6 A股涨跌停宽容】涨停时上影属于正常现象，不触发否决
        if not is_limit_up and upper > cfg.s2_upper_shadow_max_pct:
            return False, f"防飞刀 昨日长上影 {upper:.2f}% > {cfg.s2_upper_shadow_max_pct}%"

    open_px = _safe_float(rt.get("open"), 0.0)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma10 = _safe_float(y.get("ma10"), 0.0)
    pct = _auction_pct_vs_preclose(open_px, pre_close)
    if ma5 <= 0 or open_px <= ma5:
        return False, "开盘未站上 ma5"
    # 【V26.6 优化】今日开盘须站稳 MA10（确保短期均线支撑）
    if ma10 > 0 and open_px <= ma10:
        return False, "开盘未站稳 ma10"

    # 【V26.6 优化】MACD 水上金叉：diff > dea 且两者均为正值（多头排列）
    macd_diff = _safe_float(y.get("macd_diff"), 0.0)
    macd_dea = _safe_float(y.get("macd_dea"), 0.0)
    if not (macd_diff > macd_dea and macd_dea > 0):
        return False, "MACD未呈水上金叉(diff>dea>0)"
    # 【V26.6 A股涨跌停宽容】涨停时涨幅上限放宽
    pct_high_s2 = 9.5 if is_limit_up else cfg.s2_pct_high
    if not (cfg.s2_pct_low <= pct <= pct_high_s2):
        return False, f"竞价涨幅不在 {cfg.s2_pct_low}%~{pct_high_s2}%"

    if not _vol_ma_cross_upward(df):
        return False, "vol_ma5 未上穿 vol_ma20"

    nm = _safe_float(y.get("net_main_amount"), 0.0)
    if nm < 0:
        return False, "防飞刀 昨日主力净额 < 0"

    return True, "OK"


def _strategy_endless_sky(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P2ScreenerConfig,
) -> Tuple[bool, str]:
    """
    策略三：无尽苍穹（筹码彻底解放型高开）

    条件同时满足:
        - 昨日主力净额连续2日均为正（资金持续流入）
        - 昨收站稳 MA5（竞价前日须处于上升趋势）
        - 昨日 winner_rate > 90%
        - 昨日 MACD 柱 > 0
        - 今日 open > 昨日 cost_95th
        - 今日 open > 近20日 high_20（取昨日行上的 rolling 值）
        - 今日 pct_chg 1%~4%（涨停时放宽到 9.5%）
        - 今日 vol_ratio >= 1.5（涨停时放宽跳过）
        - 今日预估开盘换手 > 阈值（涨停时放宽）
    防飞刀:
        - 竞价额异常萎缩 或 昨日 atr_pct > 8%
        - 长上影（涨停时容忍）
    """
    if len(df) < 2:
        return False, "历史不足 2 日"

    # 【V26.6 A股涨跌停宽容】涨停时相关条件做相应放宽
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"

    y_prev = df.iloc[-2]
    cmw = circ_mv_wan_from(y, rt)
    inst_thr = dynamic_inst_single_threshold_yuan(cmw, cfg.s3_inst_net_buy_ratio_of_float_mv)
    inst1 = _safe_float(y.get("inst_net_buy"), 0.0)
    inst0 = _safe_float(y_prev.get("inst_net_buy"), 0.0)
    if not (inst1 > inst_thr and inst0 > inst_thr):
        return False, "inst_net_buy 未连续两日达动态机构门槛"

    # 【V26.6 优化】昨日主力净额须连续2日为正（确保资金持续流入）
    nm1 = _safe_float(y.get("net_main_amount"), 0.0)
    nm0 = _safe_float(y_prev.get("net_main_amount"), 0.0)
    if not (nm1 > 0 and nm0 > 0):
        return False, "昨日主力净额未连续两日为正"

    # 【V26.6 优化】昨收须站稳 MA5（竞价前日须处于上升趋势中）
    y_close = _safe_float(y.get("close"), 0.0)
    ma5_y = _safe_float(y.get("ma5"), 0.0)
    if ma5_y > 0 and y_close <= ma5_y:
        return False, "昨收未站稳 ma5"

    # 【V26.6 优化】hk_vol 为日线结算数据，竞价时无法获取当日值，
    # 不再作为硬性否决条件，改为记录状态供后续标注参考。
    hk_vol_val = _safe_float(y.get("hk_vol"), 0.0)
    rt["_hk_vol_positive_days"] = 1 if hk_vol_val > 0 else 0
    # 近3日北向数据辅助判断（用于竞价阶段标注）
    hk_3day_positive = 0
    if len(df) >= 3 and "hk_vol" in df.columns:
        for idx in range(-3, 0):
            if len(df) >= abs(idx) and _safe_float(df.iloc[idx].get("hk_vol"), 0.0) > 0:
                hk_3day_positive += 1
    rt["_hk_vol_3day_count"] = hk_3day_positive

    # 【V26.6 优化】新增：昨日 MACD 柱须为正（与策略一/二保持一致）
    mh = _macd_hist_row(y)
    if mh <= 0:
        return False, "昨日 macd_hist 未为正"

    ma5 = _safe_float(y.get("ma5"), 0.0)
    ma20 = _safe_float(y.get("ma20"), 0.0)
    ma60 = _safe_float(y.get("ma60"), 0.0)
    if not (ma5 > ma20 > ma60):
        return False, "均线未呈 ma5>ma20>ma60"

    open_px = _safe_float(rt.get("open"), 0.0)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    pct = _auction_pct_vs_preclose(open_px, pre_close)
    # 【V26.6 A股涨跌停宽容】涨停时涨幅上限放宽
    pct_high_s3 = 9.5 if is_limit_up else cfg.s3_pct_high
    if not (cfg.s3_pct_low <= pct <= pct_high_s3):
        return False, f"竞价涨幅不在 {cfg.s3_pct_low}%~{pct_high_s3}%"

    px_auction = open_px if open_px > 0 else _safe_float(rt.get("price"), pre_close)
    est_to = effective_turnover_rate_f(rt, y, px_auction if px_auction > 0 else pre_close)
    # 【V26.6 A股涨跌停宽容】涨停时换手率下限适当放宽
    to_min = 0.15 if is_limit_up else cfg.s3_auction_turnover_min_pct
    if est_to <= to_min:
        return False, f"预估开盘换手 {est_to:.3f}% <= {to_min:.3f}%"

    # 【V26.6 A股涨跌停宽容】涨停时量比萎缩是正常现象，不应因此排除
    vr = _safe_float(rt.get("vol_ratio"), 0.0)
    if not is_limit_up and vr < cfg.s3_vol_ratio_min:
        return False, "量比不足"

    y_high = _safe_float(y.get("high"), 0.0)
    # y_close 在前文已声明（昨收站稳MA5检查处），复用避免重复定义
    if y_close > 0:
        upper = (y_high - y_close) / y_close * 100.0
        # 【V26.6 A股涨跌停宽容】涨停时上影属于正常现象，不触发否决
        if not is_limit_up and upper > cfg.s3_upper_shadow_max_pct:
            return False, f"防飞刀 长上影 {upper:.2f}% > {cfg.s3_upper_shadow_max_pct}%"

    # 【V26.6 A股涨跌停宽容】涨停时 ATR 超限也是正常波动，不否决
    if not is_limit_up:
        atrp = _safe_float(y.get("atr_pct"), 0.0)
        if atrp > cfg.s3_atr_pct_max:
            return False, f"防飞刀 atr_pct={atrp:.2f}% > {cfg.s3_atr_pct_max}%"

    # 【V26.6 A股涨跌停宽容】涨停时竞价额萎缩是正常现象
    if not is_limit_up and _auction_amount_shrinked(rt, y, cfg):
        return False, "防飞刀 竞价额相对昨日萎缩"

    return True, "OK"


def _strategy_heavy_base_ignite(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    y: pd.Series,
    cfg: P2ScreenerConfig,
) -> Tuple[bool, str]:
    """
    策略四：底仓重金点火（巨头温和启动）

    体量:
        circ_mv > 5000000 万（500 亿）
    异动:
        近 20 日 limit_times == 1 或 2（取昨日行上的窗口统计值）
    资金:
        昨日 net_main_amount 达 max(流通市值×比例, 市值阶梯地板)（元）
    今日竞价:
        pct 2%~4%, strth(万) > max(circ_mv(万)×比例, 3000 万)
    防飞刀:
        今日 open <= 昨日 cost_50th 则否决
    """
    cmw = circ_mv_wan_from(y, rt)
    if cmw <= cfg.s4_circ_mv_min_wan:
        return False, "流通市值未达 500 亿档"

    lt = int(_safe_float(y.get("limit_times"), 0.0))
    if lt not in (1, 2):
        return False, "limit_times 不为 1 或 2"

    nm = _safe_float(y.get("net_main_amount"), 0.0)
    nm_thr_yuan = dynamic_net_main_threshold_yuan(cmw, cfg.s4_net_main_ratio_of_float_mv)
    if nm <= nm_thr_yuan:
        return False, "昨日主力净额不足(动态门槛)"

    open_px = _safe_float(rt.get("open"), 0.0)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    pct = _auction_pct_vs_preclose(open_px, pre_close)
    # 【V26.6 A股涨跌停宽容】涨停时涨幅上限放宽
    is_limit_up = str(rt.get("_is_limit", "")) == "UP"
    pct_high_s4 = 9.5 if is_limit_up else cfg.s4_pct_high
    if not (cfg.s4_pct_low <= pct <= pct_high_s4):
        return False, f"竞价涨幅不在 {cfg.s4_pct_low}%~{pct_high_s4}%"

    strth = _safe_float(rt.get("strth"), _safe_float(y.get("strth"), 0.0))
    strth_need = dynamic_strth_threshold_wan(cmw, cfg.s4_strth_ratio_of_circ_mv_wan)
    if strth <= strth_need:
        return False, "strth 不足(动态门槛)"

    # 【V26.6 优化】hk_vol 为日线结算数据，竞价时无法获取当日值，
    # 不再作为硬性否决条件，改为记录状态供后续标注参考。
    hk_vol_val = _safe_float(y.get("hk_vol"), 0.0)
    rt["_hk_vol_positive_days"] = 1 if hk_vol_val > 0 else 0
    # 近3日北向数据辅助判断（用于竞价阶段标注）
    hk_3day_positive = 0
    if len(df) >= 3 and "hk_vol" in df.columns:
        for idx in range(-3, 0):
            if len(df) >= abs(idx) and _safe_float(df.iloc[idx].get("hk_vol"), 0.0) > 0:
                hk_3day_positive += 1
    rt["_hk_vol_3day_count"] = hk_3day_positive

    c50 = _safe_float(y.get("cost_50th"), _safe_float(rt.get("cost_50th"), 0.0))
    if c50 > 0 and open_px <= c50:
        return False, "防飞刀 开盘未过 cost_50th"

    return True, "OK"


def evaluate_p2_screener(
    df: pd.DataFrame,
    rt: Dict[str, Any],
    cfg: Optional[P2ScreenerConfig] = None,
) -> Dict[str, Any]:
    """
    单股 P2 完整筛选（全局风控 + 四大策略）。

    参数:
        df: 截至 T-1 的日线，需含 ALL_55_COLS 中策略用到的列（缺失列策略自动不满足）
        rt: 当日竞价快照 dict
        cfg: 阈值配置

    返回 dict 键:
        veto_pass: bool
        veto_reason: str
        strategies: List[str]  命中的策略标签（可多条，业务上满足任一即可入池）
        strategy_checks: Dict[str, str] 各策略未通过原因或 OK
        sort_weight_bonus: float  市值超配加权系数（>1 表示更优先排序）
        p2_core_screener_pass: bool  是否命中至少一条策略（供 scan_engine 放宽黄金门禁）
        detail: dict 调试信息
    """
    cfg = _resolve_p2_cfg(cfg)
    out: Dict[str, Any] = {
        "veto_pass": False,
        "veto_reason": "",
        "strategies": [],
        "strategy_checks": {},
        "sort_weight_bonus": 1.0,
        "p2_core_screener_pass": False,
        "detail": {},
    }

    # 【V26.7 新增】动态涨跌停判定：在任何策略执行前先确定是否为涨停
    # 必须在 rt 中写入 _is_limit，后续所有策略的涨跌停宽容逻辑均依赖此标记
    y = df.iloc[-1]
    ts_code = str(rt.get("ts_code", y.get("ts_code", "")) or "")
    open_px = _safe_float(rt.get("open"), 0.0)
    pre_close = _safe_float(rt.get("pre_close"), _safe_float(y.get("close"), 0.0))
    rt["_y_close"] = _safe_float(y.get("close"), 0.0)
    rt["_is_limit"] = _detect_limit_up(ts_code, open_px, pre_close, rt)
    # 若 pre_close 与日线 close 差距过大（疑似除权除息），添加标记供后续参考
    y_close = rt["_y_close"]
    if y_close > 0 and pre_close > 0:
        ratio_diff = abs(pre_close - y_close) / y_close
        if ratio_diff > 0.20:
            rt["_ex_right_adjust"] = True
            out["detail"]["除权除息标记"] = True
            out["detail"]["pre_close_vs_close_diff_pct"] = round(ratio_diff * 100, 2)

    ok, reason = p2_global_risk_veto(df, rt, cfg)
    out["veto_pass"] = ok
    out["veto_reason"] = reason
    if not ok:
        return out
    circ_mv = _safe_float(rt.get("circ_mv"), _safe_float(y.get("circ_mv"), 0.0))
    if circ_mv >= cfg.circ_mv_prefer_wan:
        out["sort_weight_bonus"] = 1.08

    now_px = _safe_float(rt.get("open"), _safe_float(rt.get("price"), 0.0))
    regime_bucket = _regime_bucket(rt)
    board_beta = _board_beta(rt)
    sector_strength = float(rt.get("sector_strength", rt.get("industry_strength", board_beta)) or board_beta)
    sector_rank = _safe_float(rt.get("sector_rank"), 999.0)
    sector_total = _safe_float(rt.get("sector_total"), 0.0)
    mainline_score, mainline_reason = _mainline_sector_score(rt, board_beta)
    t1_avg, t1_win, t1_n = _p2_collect_t1_memory(rt)
    t1_memory_score = _p2_t1_memory_score(t1_avg, t1_win, t1_n, cfg)
    open_confirm_hit = _open_confirm_window(rt, cfg) and _open_vwap_support_ok(rt, now_px, cfg)
    vwap_px = _estimate_vwap_from_rt(rt, now_px, fallback_price=now_px)
    vwap_gap_pct = ((now_px - vwap_px) / vwap_px * 100.0) if vwap_px > 0 and now_px > 0 else 0.0

    # 序号 P2-01～P2-04：与产品文档/配置常量命名对齐，便于检索与复盘
    strat_defs = [
        ("P2-01·★主升浪确认", lambda: _strategy_main_wave_confirm(df, rt, y, cfg)),
        ("P2-02·★机构连贯发力", lambda: _strategy_institution_streak(df, rt, y, cfg)),
        ("P2-03·★无尽苍穹", lambda: _strategy_endless_sky(df, rt, y, cfg)),
        ("P2-04·★底仓重金点火", lambda: _strategy_heavy_base_ignite(df, rt, y, cfg)),
    ]

    if regime_bucket == "strict":
        out["sort_weight_bonus"] *= cfg.regime_strict_boost
    elif regime_bucket == "relaxed":
        out["sort_weight_bonus"] *= cfg.regime_relaxed_boost
    if mainline_score >= 3.0:
        out["sort_weight_bonus"] *= 1.12
    elif mainline_score >= 1.0:
        out["sort_weight_bonus"] *= 1.06
    elif mainline_score <= -2.0:
        out["sort_weight_bonus"] *= 0.84
    if board_beta >= cfg.board_beta_core_min:
        out["sort_weight_bonus"] *= 1.08
    elif board_beta >= cfg.board_beta_hot_min:
        out["sort_weight_bonus"] *= 1.03
    elif board_beta <= cfg.board_beta_cold_max:
        out["sort_weight_bonus"] *= 0.92
    if sector_strength >= 1.10:
        out["sort_weight_bonus"] *= 1.12
    elif sector_strength >= 1.03:
        out["sort_weight_bonus"] *= 1.06
    elif sector_strength <= 0.95:
        out["sort_weight_bonus"] *= 0.88
    if sector_total >= 3 and sector_rank > 0:
        if sector_rank <= 2:
            out["sort_weight_bonus"] *= 1.10
        elif sector_rank <= 5:
            out["sort_weight_bonus"] *= 1.05
        elif sector_rank > sector_total - 3:
            out["sort_weight_bonus"] *= 0.90
    if open_confirm_hit:
        out["sort_weight_bonus"] *= 1.05
    if t1_n >= cfg.t1_memory_min_samples:
        if t1_memory_score > 0:
            out["sort_weight_bonus"] *= min(1.15, 1.0 + t1_memory_score / 100.0)
        elif t1_memory_score < 0:
            out["sort_weight_bonus"] *= max(0.88, 1.0 + t1_memory_score / 120.0)
    if vwap_gap_pct > 1.8:
        out["sort_weight_bonus"] *= 0.90
    elif vwap_gap_pct < -0.8:
        out["sort_weight_bonus"] *= 0.86

    hits: List[str] = []
    checks: Dict[str, str] = {}
    for name, fn in strat_defs:
        try:
            passed, msg = fn()
        except Exception as ex:
            logger.debug("P2 策略 %s 判定异常: %s", name, ex)
            passed, msg = False, f"异常:{ex}"
        checks[name] = msg if passed else msg
        if passed:
            hits.append(name)

    if open_confirm_hit:
        out["sort_weight_bonus"] *= 1.06
    if t1_memory_score != 0.0:
        out["sort_weight_bonus"] *= max(0.9, min(1.12, 1.0 + t1_memory_score / 100.0))

    if sector_total >= 3 and sector_rank > 0 and hits:
        if sector_rank <= 2 and any(tag in hits[0] for tag in ("P2-01", "P2-04", "P2-02")):
            out["sort_weight_bonus"] *= 1.05
        elif sector_rank > sector_total - 3:
            out["sort_weight_bonus"] *= 0.95

    if mainline_score >= 3.0 and hits:
        if any(tag in hits[0] for tag in ("P2-01", "P2-02", "P2-04")):
            out["sort_weight_bonus"] *= 1.04

    if sector_strength >= 1.10 and hits:
        hit_primary = any(tag in hits[0] for tag in ("P2-01", "P2-04", "P2-02"))
        if hit_primary:
            out["sort_weight_bonus"] *= 1.05

    # 【V26.6 优化】hk_vol / net_main_amount / inst_net_buy 为日线结算数据，
    # 竞价显示"昨"标注，不作为当日判断依据，仅供参考。
    hk_vol_warn = ""
    hk_3day = rt.get("_hk_vol_3day_count", 0)
    if hk_3day >= 3:
        hk_vol_warn = "近3日北向持续正(昨)"
    elif hk_3day >= 2:
        hk_vol_warn = "近3日北向2日正(昨)"
    elif rt.get("_hk_vol_positive_days", 0) >= 1:
        hk_vol_warn = "昨日北向正(昨)"
    else:
        hk_vol_warn = "北向数据(昨-滞后)"

    out["strategies"] = hits
    out["strategy_checks"] = checks
    out["p2_core_screener_pass"] = len(hits) > 0
    out["detail"] = {
        "circ_mv_wan": circ_mv,
        "sort_weight_bonus": out["sort_weight_bonus"],
        "regime_bucket": regime_bucket,
        "board_beta": round(board_beta, 3),
        "sector_strength": round(sector_strength, 3),
        "sector_rank": int(sector_rank if sector_rank != 999 else 999),
        "sector_total": int(sector_total),
        "mainline_score": round(mainline_score, 2),
        "mainline_reason": mainline_reason,
        "t1_avg_ret_pct": round(t1_avg, 3),
        "t1_win_rate_pct": round(t1_win, 1),
        "t1_sample_n": int(t1_n),
        "t1_memory_score": round(t1_memory_score, 2),
        "open_confirm_hit": bool(open_confirm_hit),
        "y_trade_date": str(y.get("trade_date", "")),
        "hk_vol_data_note": hk_vol_warn,  # V26.6 竞价北向数据滞后提示
    }
    return out


def screen_p2_universe(
    rows: List[Tuple[str, pd.DataFrame, Dict[str, Any]]],
    cfg: Optional[P2ScreenerConfig] = None,
) -> pd.DataFrame:
    """
    批量增量筛选（一日、全市场）。

    参数:
        rows: [(ts_code, df_hist, rt), ...]
            每只股票仅携带自身历史与当日 rt，不依赖全表。

    返回:
        pandas.DataFrame，便于导出 CSV / 写库；列含 ts_code、是否否决、命中策略等。
    """
    cfg = _resolve_p2_cfg(cfg)
    rec: List[Dict[str, Any]] = []
    for ts_code, df, rt in rows:
        r = evaluate_p2_screener(df, rt, cfg)
        rec.append(
            {
                "ts_code": ts_code,
                "veto_pass": r["veto_pass"],
                "veto_reason": r["veto_reason"],
                "strategies": "|".join(r["strategies"]),
                "p2_core_screener_pass": r["p2_core_screener_pass"],
                "sort_weight_bonus": r["sort_weight_bonus"],
            }
        )
    return pd.DataFrame(rec)
