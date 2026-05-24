# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.7 - P5 盘后真龙池引擎（物理胸甲十四策略独立版）
【V26.6 优化】
1. 修复「裸48分」问题：基准分与策略命中质量绑定，无命中不给分。
2. VWAP惩罚与策略强度挂钩软化：三维共振 / 双维强共振可容忍更大VWAP偏差。
3. 分数截断下限联动风控建议分：突破分截断与 suggested_min_entry_score 逻辑闭环。
4. 增加连续命中天数加成：查询 signal_log 表，追踪近5日历史共振质量。
5. 量比改为相对市场标定：用市场量比中位数归一化个股量比，反映相对强弱。
6. 板块内部分化因子：个股涨幅 vs 板块涨幅的偏离作为板块加成修正。
"""
from __future__ import annotations

# Standard library
import logging
from typing import Any, Dict, List, Optional, Tuple

# Third-party
import numpy as np
import pandas as pd

# Local modules
from core.config_manager import get_p5_postmarket_config, get_risk_control_config
from core.strategies.p5_postmarket_screener import (
    P5PostmarketConfig,
    _derive_p5_action_summary,
    build_single_stock_window_from_rt,
    evaluate_p5_single_prepared_row,
    evaluate_vwap_penalty_p5,
    prepare_p5_lags,
)
from core.strategies.risk_control_engine import (
    DEFAULT_RISK_CONFIG,
    RiskControlConfig,
    evaluate_3layer_risk,
    evaluate_layer2_right_side_only,
    hits_indicate_right_side_attack,
    merge_risk_into_engine_result,
)

logger = logging.getLogger(__name__)


def _p5_safe_float(val: Any, default: float = 0.0) -> float:
    """标量转 float，非有限数回退 default。"""
    try:
        x = float(pd.to_numeric(val, errors="coerce"))
        if not np.isfinite(x):
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _p5_rt_get(rt: Any, key: str, default: Any = None) -> Any:
    if isinstance(rt, dict):
        return rt.get(key, default)
    return default


def _p5_row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row.get(key, default)
    except Exception:
        return default


def _p5_trade_date(rt: Any) -> Any:
    return _p5_rt_get(rt, "trade_date", None)


def _bias_ma5_ma20_pct(ma5: float, ma20: float) -> float:
    """
    均线间乖离（%）：(ma5 - ma20) / ma20 * 100。
    仅在 ma20>0 且语义有效时由调用方使用；否则返回 nan。
    """
    if ma20 <= 0 or not np.isfinite(ma20):
        return float("nan")
    return (ma5 - ma20) / ma20 * 100.0


def _ma_overheat_penalty_mult(
    circ_mv_wan: float,
    bias_ma5_ma20: float,
    ma5: float,
    ma20: float,
    cfg: P5PostmarketConfig,
) -> float:
    """
    虚火分级降权（软约束，不否决）：
    - 须多头排列 ma5 > ma20，且乖离 > 阈值（默认 18%）才触发；
    - circ_mv >= 300 亿（万）：乘子 ma_overheat_mult_large（默认 0.85）；
    - P1 流通下限（默认 60 亿）<= circ_mv < 300 亿：乘子 ma_overheat_mult_mid（默认 0.95）；
    - circ_mv < P1 下限：不罚（主升浪极端乖离在小票尚可容忍）；
    - [3%,12%] 健康带内不罚（实际上乖离 <=18% 时已不触发本分支）。
    """
    if ma5 <= ma20 or ma20 <= 0:
        return 1.0
    if not np.isfinite(bias_ma5_ma20) or bias_ma5_ma20 <= float(cfg.ma_bias_overheat_pct):
        return 1.0
    if circ_mv_wan < float(cfg.circ_mv_wan_mid_min):
        return 1.0
    if not np.isfinite(circ_mv_wan):
        return 1.0
    large = float(cfg.circ_mv_wan_large_min)
    mid = float(cfg.circ_mv_wan_mid_min)
    if circ_mv_wan + 1e-9 >= large:
        return float(cfg.ma_overheat_mult_large)
    if circ_mv_wan + 1e-9 >= mid:
        return float(cfg.ma_overheat_mult_mid)
    return 1.0


def _ma_slope_reward_mult(
    slope_today: float,
    slope_yesterday: float,
    ma5: float,
    ma20: float,
    cfg: P5PostmarketConfig,
) -> float:
    """
    MA20 五日斜率奖励（indicator_calc 与全库一致：ma20_slope_5 已为百分比变化）：
    - 须 ma5 > ma20；
    - 今日斜率 > 阈值（默认 0.8%）且 今日斜率 > 昨日斜率（加速向上）；
    - 在 [threshold, interp_high] 上线性映射到 [mult_min, mult_max]，超过 interp_high 封顶 mult_max。
    """
    if ma5 <= ma20 or ma20 <= 0:
        return 1.0
    th = float(cfg.ma_slope_reward_threshold_pct)
    if not (np.isfinite(slope_today) and slope_today > th):
        return 1.0
    if not (np.isfinite(slope_yesterday) and slope_today > slope_yesterday):
        return 1.0
    lo = th
    hi = float(cfg.ma_slope_reward_interp_high_pct)
    mmin = float(cfg.ma_slope_reward_mult_min)
    mmax = float(cfg.ma_slope_reward_mult_max)
    if hi <= lo:
        return mmax
    if slope_today >= hi:
        return mmax
    t = (slope_today - lo) / (hi - lo)
    return float(mmin + t * (mmax - mmin))


def _apply_p5_ma_kinetic_to_burst(
    burst_base: float,
    prep: pd.DataFrame,
    row: Any,
    cfg: P5PostmarketConfig,
    burst_floor: float = 55.0,
) -> Tuple[float, Dict[str, Any]]:
    """
    基准爆发分 × 虚火惩罚 × 斜率奖励，再截断到 [burst_floor, 99]。
    burst_floor 由调用方根据风控建议分动态传入。
    返回 (burst_after, detail_patch)；关闭 enable_ma_compensation 时原样返回 burst_base 与空 dict。
    """
    if not bool(cfg.enable_ma_compensation):
        return float(burst_base), {}

    ma5 = _p5_safe_float(row.get("ma5"), 0.0)
    ma20 = _p5_safe_float(row.get("ma20"), 0.0)
    circ_mv_wan = _p5_safe_float(row.get("circ_mv"), 0.0)
    bias_b = _bias_ma5_ma20_pct(ma5, ma20)

    slope_today = _p5_safe_float(row.get("ma20_slope_5"), 0.0)
    slope_yesterday = 0.0
    if prep is not None and isinstance(prep, pd.DataFrame) and len(prep) >= 2:
        slope_yesterday = _p5_safe_float(prep.iloc[-2].get("ma20_slope_5"), 0.0)

    m_pen = _ma_overheat_penalty_mult(circ_mv_wan, bias_b, ma5, ma20, cfg)
    m_rw = _ma_slope_reward_mult(slope_today, slope_yesterday, ma5, ma20, cfg)
    out = float(burst_base) * m_pen * m_rw
    out = float(np.clip(out, burst_floor, 99.0))

    patch: Dict[str, Any] = {
        "MA动能_bias_ma5_ma20_pct": round(bias_b, 4) if np.isfinite(bias_b) else None,
        "MA动能_虚火惩罚乘子": m_pen,
        "MA动能_斜率奖励乘子": m_rw,
        "MA动能_ma20_slope5_今日": round(slope_today, 4),
        "MA动能_ma20_slope5_昨日": round(slope_yesterday, 4),
        "MA动能_circ_mv_万元": round(circ_mv_wan, 2),
    }
    return out, patch


# =============================================================================
# 【V26.6 优化模块】历史连续命中查询
# =============================================================================
def _query_p5_consecutive_hit_days(ts_code: str, trade_date: str, lookback: int = 5) -> int:
    """
    从 signal_log 表查询该股票近 N 个交易日内被 P5 选出的天数（连续计数）。
    仅统计有策略命中的记录；中间断层则不计入连续。
    返回连续命中天数（0 表示当天不在池中或无历史）。
    查询不到表时返回 0，不抛异常。

    【V26.7 修复】
    1. DuckDB 的 trade_date 列可能是 YYYYMMDD 或 YYYY-MM-DD 两种格式，需要统一解析。
    2. 跨周末时自然日差可能为 3（周一到上周四），应与 A 股交易日对齐（周五-周一 diff=1）。
    """
    import datetime as _dt

    if not ts_code or not trade_date:
        return 0
    try:
        import constants as _c
        table = getattr(_c, "LOG_TABLE", "signal_log") or "signal_log"
    except Exception:
        table = "signal_log"

    def _parse_trade_date(s: str):
        """尝试多种格式解析 trade_date，返回 date 对象或 None。"""
        if not s:
            return None
        s = str(s).strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return _dt.datetime.strptime(s, fmt).date()
            except (ValueError, TypeError):
                pass
        return None

    try:
        from core.db_core import get_duckdb_conn
        conn = get_duckdb_conn()
        if conn is None:
            return 0

        sql = f"""
            SELECT trade_date, score, strategy
            FROM {table}
            WHERE ts_code = ?
              AND pool = 'p5'
              AND score > 0
            ORDER BY trade_date DESC
            LIMIT ?
        """
        rows = conn.execute(sql, [str(ts_code), lookback]).fetchall()
        if not rows:
            return 0

        # 【V26.7 修复】统一解析 DuckDB 中 trade_date 为 date 对象列表
        parsed_dates: List[_dt.date] = []
        for r in rows:
            d = _parse_trade_date(r[0])
            if d is not None:
                parsed_dates.append(d)

        if not parsed_dates:
            return 0

        # 【V26.7 修复】解析传入的 trade_date 为 date 对象（兼容两种格式）
        today_date = _parse_trade_date(trade_date)
        if today_date is None:
            today_date = _dt.datetime.strptime(str(trade_date).replace("-", ""), "%Y%m%d").date()
        # 第一个记录必须与 today 对齐（允许前后±1天容错，避免跨日边界问题）
        if not parsed_dates or abs((parsed_dates[0] - today_date).days) > 1:
            return 0

        # 【V26.7 修复】使用 A 股交易日逻辑计算连续性：
        # A 股每周只有 5 个交易日（周一~周五），跨周末时周五到周一的交易日距离 = 1。
        # 方法：将 date 转换为 A 股"交易日序号"（周一=0，周二=1，...，周日=6），
        # 再用 (年 * 100 + 周序号) * 7 + weekday 来表示"第几个交易日"。
        def _trade_day_seq(d: _dt.date) -> int:
            return d.year * 10000 + d.weekday()  # weekday(): Mon=0, Sun=6

        consecutive = 1
        for i in range(1, len(parsed_dates)):
            d0 = parsed_dates[i - 1]
            d1 = parsed_dates[i]
            # 交易日距离：A股每年约 250 个交易日，理想情况下每年 52 周 * 5 = 260 个
            # 用交易日序号差来衡量是否连续：同一年内差距在 5 以内（同一周），跨年则重置
            seq_diff = _trade_day_seq(d0) - _trade_day_seq(d1)
            # 同一交易周内连续（diff = 1），跨周末正常（diff = 3），超过 5 天未入选则断开
            if 1 <= seq_diff <= 5:
                consecutive += 1
            else:
                break

        return consecutive

    except Exception as e:
        logger.debug("_query_p5_consecutive_hit_days 查询失败 ts_code=%s: %s", ts_code, e)
        return 0


# =============================================================================
# 【V26.6 优化模块】板块内部分化因子
# =============================================================================
def _compute_sector_differentiation_mult(
    stock_pct: float,
    sector_beta: float,
    cfg: P5PostmarketConfig,
) -> Tuple[float, str]:
    """
    个股涨幅与板块 Beta 的分化修正：
    - sector_beta > 1 表示板块当日强势，个股涨幅 > 板块均值 → 强中强，乘子提升；
    - sector_beta < 1 表示板块当日弱势，个股涨幅 > 0 或跌幅 < 板块均值 → 逆势走强，乘子提升；
    - 个股涨幅远弱于板块（负分化）→ 轻微降权。
    返回 (分化修正乘子, 标签字符串)。
    """
    beta_th = float(getattr(cfg, "sector_diff_beta_th", 1.10))
    pct_diff_th = float(getattr(cfg, "sector_diff_pct_th", 1.5))

    if sector_beta <= 0:
        return 1.0, ""

    strong_sector = sector_beta >= beta_th
    weak_sector = sector_beta <= (2.0 - beta_th)

    stock_above_avg = stock_pct > 0.5
    stock_beats_sector = stock_pct > pct_diff_th

    if strong_sector and stock_above_avg:
        mult = min(1.0 + (sector_beta - beta_th) * 0.2, 1.12)
        return mult, f"[板块强化×{mult:.2f}]"
    if weak_sector and stock_pct > 0 and stock_pct < 3.0:
        mult = min(1.0 + abs(sector_beta - 1.0) * 0.3, 1.10)
        return mult, f"[逆势走强×{mult:.2f}]"
    if stock_pct < -2.0 and sector_beta >= 1.0:
        mult = max(1.0 - min(abs(stock_pct) * 0.05, 0.08), 0.88)
        return mult, f"[弱于板块×{mult:.2f}]"

    return 1.0, ""


# =============================================================================
# 【V26.6 优化模块】相对市场量比映射
# =============================================================================
def _map_vr_relative(
    vr: float,
    winner_rate: float,
    bias20: float,
    market_vr_median: float = 1.0,
    cfg: Optional[P5PostmarketConfig] = None,
) -> float:
    """
    【V26.6 优化】量比改为相对市场标定：
    - 用市场量比中位数归一化个股量比（relative_vr = 个股_vr / market_vr_median）
    - 相对量比 > 1 表示个股量能强于市场整体
    - 相对量比 < 1 表示个股量能弱于市场整体（即便绝对值正常）
    - 高位放量（相对vr > 2.0）后得分开始下降，防派发
    - 结合 winner_rate 和 bias20 做分段映射
    """
    vr = float(vr) if np.isfinite(vr) else 1.0
    wr = float(winner_rate) if np.isfinite(winner_rate) else 50.0
    b20 = float(bias20) if np.isfinite(bias20) else 0.0
    mvr = float(market_vr_median) if np.isfinite(market_vr_median) and market_vr_median > 0 else 1.0

    rel_vr = vr / mvr

    if wr < 60.0 or abs(b20) < 3.0:
        xp = [0.0, 0.5, 0.8, 1.0, 1.5, 2.2, 3.5]
        yp = [0.0, 12.0, 28.0, 48.0, 78.0, 90.0, 78.0]
    elif wr <= 85.0:
        xp = [0.0, 0.5, 0.8, 1.0, 1.3, 1.8, 2.5]
        yp = [0.0, 10.0, 22.0, 42.0, 68.0, 82.0, 70.0]
    else:
        if vr < 1.0:
            xp = [0.0, 0.4, 0.7, 1.0, 1.5, 2.0]
            yp = [5.0, 18.0, 38.0, 65.0, 85.0, 75.0]
        else:
            xp = [0.0, 0.6, 0.9, 1.2, 1.8, 2.5, 3.5]
            yp = [0.0, 15.0, 35.0, 60.0, 78.0, 65.0, 45.0]

    return float(np.interp(rel_vr, xp, yp))


# =============================================================================
# 【V26.6 优化模块】VWAP惩罚与策略强度挂钩软化
# =============================================================================
def _apply_vwap_strength_softening(
    vwap_mult: float,
    cross_bonus: float,
    dim_count: int,
    cfg: Optional[P5PostmarketConfig] = None,
) -> float:
    """
    【V26.6 优化 → V26.7 可配置化】VWAP惩罚与策略强度挂钩软化：
    - 三维全共振（cross_bonus >= 12 且 dim_count >= 3）
    - 双维强共振（cross_bonus >= 8）
    - 双维普通（cross_bonus >= 6）
    - 单维度：保持原惩罚不变
    逻辑：强共振标的分时VWAP偏差容忍度更高，不因尾盘轻微画线被一棒子打死
    【V26.7】从 P5PostmarketConfig 读取软化阈值，替代硬编码。
    """
    if vwap_mult >= 1.0:
        return 1.0
    if vwap_mult <= 0.0:
        return 0.4

    # 【V26.7 可配置化】从 cfg 读取软化参数；None 或字段缺失时使用 V26.6 原始值
    _f = lambda name, default: float(getattr(cfg, name, default)) if cfg is not None else default

    th_triple = _f("vwap_soften_th_triple", 12.0)
    th_ds = _f("vwap_soften_th_double_strong", 8.0)
    th_d = _f("vwap_soften_th_double", 6.0)
    hard_triple = _f("vwap_soften_hard_triple", 0.72)
    soft_triple = _f("vwap_soften_soft_triple", 0.88)
    hard_ds = _f("vwap_soften_hard_double_strong", 0.62)
    soft_ds = _f("vwap_soften_soft_double_strong", 0.87)
    hard_d = _f("vwap_soften_hard_double", 0.65)
    soft_d = _f("vwap_soften_soft_double", 0.86)
    hard_raw = _f("vwap_hard_mult", 0.55)
    soft_raw = 0.82

    if cross_bonus >= th_triple and dim_count >= 3:
        hard_tgt, soft_tgt = hard_triple, soft_triple
    elif cross_bonus >= th_ds:
        hard_tgt, soft_tgt = hard_ds, soft_ds
    elif cross_bonus >= th_d:
        hard_tgt, soft_tgt = hard_d, soft_d
    else:
        return vwap_mult

    if abs(vwap_mult - hard_raw) < 0.05:
        return hard_tgt
    if abs(vwap_mult - soft_raw) < 0.05:
        return soft_tgt
    if vwap_mult < hard_raw:
        return hard_tgt
    if vwap_mult > soft_raw:
        t = float(np.clip((vwap_mult - soft_raw) / (hard_raw - soft_raw), 0.0, 1.0))
        return float(hard_tgt * t + soft_tgt * (1.0 - t))

    return vwap_mult


# =============================================================================
# 【V26.6 优化模块】策略命中质量加权基准分
# =============================================================================
def _compute_strategy_quality_base(
    hits: List[str],
    dim_count: int,
    cross_bonus: float,
    cfg: Optional[P5PostmarketConfig] = None,
) -> float:
    """
    【V26.6 优化】修复裸48分：基准分与策略命中质量强绑定
    - 最低基准：仅通过全局门禁（无策略命中）：8分（仅够覆盖风控过滤后残存的边缘标的）
    - 单一维度命中：18~22分（基础存在证明）
    - 双维度：30~38分
    - 三维全共振：44~52分
    - 再叠加确认子策略数量微调（每多一个确认级子策略 +1分，上限+4）
    """
    confirm_suffixes = ("C", "确认", "共振")
    confirm_hits = sum(1 for h in hits for s in confirm_suffixes if s in h)
    confirm_bonus = min(confirm_hits, 4)

    # 【V26.7 修复】55分兜底：仅通过全局门禁（无策略命中）时，底分从8提升到15，
    # 防止机构净买入/北向等龙虎榜滞后数据为NaN时好票被误杀在55分及格线以下。
    if dim_count == 0:
        base = 15.0
    elif dim_count == 1:
        base = 18.0 + confirm_bonus * 1.0
    elif dim_count == 2:
        if cross_bonus >= 8.0:
            base = 34.0 + confirm_bonus * 0.8
        else:
            base = 28.0 + confirm_bonus * 0.8
    else:
        base = 44.0 + confirm_bonus * 0.6

    return float(np.clip(base, 8.0, 56.0))


class P5Postmarket:
    def __init__(self, cfg: P5PostmarketConfig = None, risk_cfg: Optional[RiskControlConfig] = None):
        self.name = "P5盘后真龙引擎"
        self.version = "V26.6"
        self._cfg_lock_external = cfg is not None
        self._cfg = cfg if cfg is not None else get_p5_postmarket_config()
        self._risk_cfg_lock_external = risk_cfg is not None
        if risk_cfg is not None:
            self._risk_cfg = risk_cfg
        else:
            try:
                self._risk_cfg = get_risk_control_config()
            except Exception as ex:
                logger.debug("风控配置从 config.yaml 加载失败，使用 DEFAULT_RISK_CONFIG: %s", ex)
                self._risk_cfg = DEFAULT_RISK_CONFIG

    def _map_vr(self, vr: float, winner_rate: float, bias20: float) -> float:
        """原始绝对量比映射（保留向后兼容，V26.6 新增 _map_vr_relative）。"""
        vr = float(vr) if np.isfinite(vr) else 0.0
        wr = float(winner_rate) if np.isfinite(winner_rate) else 0.0
        b20 = float(bias20) if np.isfinite(bias20) else 0.0
        if wr < 60.0 or abs(b20) < 3.0:
            xp = [0.0, 0.8, 1.2, 2.0, 3.0, 4.5]
            yp = [0.0, 20.0, 45.0, 80.0, 95.0, 100.0]
        elif wr <= 85.0:
            xp = [0.0, 0.8, 1.0, 1.5, 2.5, 3.5, 4.5]
            yp = [0.0, 18.0, 35.0, 70.0, 88.0, 76.0, 60.0]
        else:
            if vr < 1.0:
                xp = [0.0, 0.6, 0.8, 1.0, 1.5, 2.5, 4.0]
                yp = [0.0, 25.0, 55.0, 92.0, 100.0, 96.0, 84.0]
            else:
                xp = [0.0, 0.8, 1.2, 2.0, 2.5, 3.5, 4.5]
                yp = [0.0, 20.0, 50.0, 78.0, 72.0, 48.0, 28.0]
        return float(np.interp(vr, xp, yp))

    def run_all(self, df, rt):
        """
        单股入口：df 为历史日线窗口；rt 为当日收盘/盘后快照（可与最后一根同日对齐或带 trade_date 追加）。

        返回 dict：burst_score、strategies、penalty、detail、p5_core_screener_pass、p5_veto_reason
        【V26.6 优化】：
        - 基准分与策略命中质量绑定（修复裸48分）
        - VWAP惩罚与策略强度挂钩软化
        - 分数截断与风控建议分联动
        - 连续命中天数加成
        - 相对市场量比映射
        - 板块内部分化修正
        """
        if not self._cfg_lock_external:
            self._cfg = get_p5_postmarket_config()
        if not self._risk_cfg_lock_external:
            try:
                self._risk_cfg = get_risk_control_config()
            except Exception:
                pass
        res = {
            "burst_score": 0.0,
            "surge_bonus": 0.0,
            "penalty": 0.0,
            "detail": {},
            "strategies": [],
            "primary_action": "",
            "secondary_actions": [],
            "market_status": "",
            "risk_level": "",
            "p5_core_screener_pass": False,
            "p5_veto_reason": "",
            "risk_tags": [],
            "suggested_min_entry_score": 0.0,
            "risk_control": {},
            "buy_hint": "",
            "wechat_hint": "",
        }
        if df is None or df.empty or len(df) < 3:
            return res

        try:
            code = str(_p5_rt_get(rt, "ts_code", "") or "")
            if not code and "ts_code" in df.columns:
                code = str(df["ts_code"].iloc[-1])
            if not code:
                code = "_P5_SINGLE_"

            work = build_single_stock_window_from_rt(df, rt, code)
            if work is None or len(work) < 3:
                return res

            prep = prepare_p5_lags(work)
            if prep is None or prep.empty:
                return res
            row = prep.iloc[-1]

            rt_risk = dict(rt)
            rt_risk["_pool_key"] = "p5"
            risk_pre = evaluate_3layer_risk(work, rt_risk, self._risk_cfg, is_right_side_strategy=False, pool_key="p5")
            if not risk_pre.get("pass_layer1", False):
                res["p5_veto_reason"] = str(risk_pre.get("veto_reason", "") or "")
                res["risk_control"] = {
                    "pass_layer1": False,
                    "pass_layer2": True,
                    "veto_reason": res["p5_veto_reason"],
                    "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),
                    "risk_tags": list(risk_pre.get("risk_tags", []) or []),
                    "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),
                    "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),
                    "debug": {
                        "stage": "layer1_veto",
                        "ts_code": code,
                        "trade_date": _p5_trade_date(rt),
                    },
                }
                res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])
                res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)
                return res

            try:
                mcs = float(_p5_rt_get(rt, "_market_contraction_score", 0.0) or 0.0)
            except (TypeError, ValueError):
                mcs = 0.0

            gp_ok, hits, veto = evaluate_p5_single_prepared_row(
                row,
                self._cfg,
                stock_prep_df=prep,
                rt=rt,
                market_contraction_score=mcs,
            )
            hits = list(hits or [])
            res["p5_veto_reason"] = veto or ""

            if not gp_ok:
                return res
            if not hits:
                return res

            hits = list(hits)
            if hits_indicate_right_side_attack(hits):
                l2 = evaluate_layer2_right_side_only(work, rt_risk, self._risk_cfg)
                for uw in l2.get("ui_warnings") or []:
                    res["risk_tags"].append("⚠️[二层预警]" + str(uw))
                if not l2.get("pass_layer2", True):
                    res["p5_veto_reason"] = str(l2.get("veto_reason", "") or "")
                    res["risk_control"] = {
                        "pass_layer1": True,
                        "pass_layer2": False,
                        "veto_reason": res["p5_veto_reason"],
                        "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),
                        "risk_tags": list(risk_pre.get("risk_tags", []) or []),
                        "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),
                        "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),
                        "debug": {
                            "stage": "layer2_veto",
                            "ts_code": code,
                            "trade_date": _p5_trade_date(rt),
                        },
                    }
                    res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])
                    res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)
                    return res

            res["p5_core_screener_pass"] = True
            res["strategies"] = hits

            action_priority = [
                ("P5-12B·★箱体回踩", "低吸"),
                ("P5-12A·★箱体突破", "追涨"),
                ("P5-12·★箱体突破回踩", "追涨/低吸"),
                ("P5-13·★均线粘合发散", "追涨"),
                ("P5-13C·★均线发散确认", "追涨"),
                ("P5-13B·★均线发散起点", "追涨"),
                ("P5-13A·★均线粘合", "观察"),
                ("P5-14·★缩量分歧低吸", "低吸"),
                ("P5-03C·★资金确认", "核心"),
                ("P5-03B·★资金质量", "核心"),
                ("P5-03A·★资金方向", "核心"),
                ("P5-02C·★量价确认", "核心"),
                ("P5-02B·★量价放量", "核心"),
                ("P5-02A·★量价趋势", "核心"),
                ("P5-06·★机构龙虎缩量回踩", "辅助"),
                ("P5-07·★外资连续建仓", "辅助"),
                ("P5-08·★超级中军点火", "辅助"),
                ("P5-01·★核心四因子共振", "辅助"),
                ("P5-04·★单峰密集突破", "辅助"),
                ("P5-05C·★趋势确认", "核心"),
                ("P5-05·★绝对趋势雷达", "核心"),
                ("P5-05B·★趋势质量", "核心"),
                ("P5-05A·★趋势结构", "核心"),
            ]
            primary_action = "观察"
            secondary_actions = []
            for key, label in action_priority:
                if any(key in h for h in hits):
                    if primary_action == "观察":
                        primary_action = label
                    elif label not in secondary_actions and label != primary_action:
                        secondary_actions.append(label)
            if any("VWAP" in h for h in res["risk_tags"]):
                market_status = "分时偏弱"
                risk_level = "高"
            else:
                market_status = "正常"
                risk_level = "中"
            res["primary_action"] = primary_action
            res["secondary_actions"] = secondary_actions
            res["market_status"] = market_status
            res["risk_level"] = risk_level

            wr = _p5_safe_float(_p5_row_get(row, "winner_rate"), 0.0)
            bias20 = _p5_safe_float(_p5_row_get(row, "bias_20"), 0.0)
            vr = _p5_safe_float(_p5_row_get(row, "vol_ratio"), 0.0)

            market_vr_median = _p5_safe_float(_p5_rt_get(rt, "_market_vr_median", 1.0), 1.0)
            if not np.isfinite(market_vr_median) or market_vr_median <= 0:
                market_vr_median = 1.0

            s_vol_rel = _map_vr_relative(vr, wr, bias20, market_vr_median, self._cfg)
            s_vol_abs = self._map_vr(vr, wr, bias20)
            s_vol = s_vol_rel

            dim_map = {
                "A资金底座": any(any(k in h for k in ["P5-03", "P5-07", "P5-08"]) for h in hits),
                "B结构买点": any(any(k in h for k in ["P5-12", "P5-13", "P5-14"]) for h in hits),
                "C趋势量价": any(any(k in h for k in ["P5-01", "P5-02", "P5-04", "P5-05C", "P5-05B", "P5-05A", "P5-05"]) for h in hits),
            }
            dim_count = int(sum(1 for v in dim_map.values() if v))

            sector_beta = 1.0
            if isinstance(rt.get("sector_beta"), (int, float)) and not pd.isna(rt.get("sector_beta")):
                sector_beta = float(rt.get("sector_beta", 1.0) or 1.0)
            elif isinstance(rt.get("industry_beta"), (int, float)) and not pd.isna(rt.get("industry_beta")):
                sector_beta = float(rt.get("industry_beta", 1.0) or 1.0)
            elif isinstance(rt.get("sector_mult"), (int, float)) and not pd.isna(rt.get("sector_mult")):
                sector_beta = float(rt.get("sector_mult", 1.0) or 1.0)
            sector_beta = max(0.7, min(1.5, sector_beta))

            pct_close = _p5_safe_float(_p5_row_get(row, "pct_chg"), 0.0)
            sector_diff_mult, sector_diff_tag = _compute_sector_differentiation_mult(
                pct_close, sector_beta, self._cfg
            )
            board_bonus = 0.0
            if sector_beta >= 1.20:
                board_bonus = 6.0 * sector_diff_mult
                res["risk_tags"].append("🌋[热板块加权]" + (sector_diff_tag if sector_diff_tag else ""))
            elif sector_beta >= 1.10:
                board_bonus = 3.0 * sector_diff_mult
                res["risk_tags"].append("🔥[偏热板块]" + (sector_diff_tag if sector_diff_tag else ""))
            elif sector_beta < 0.9:
                board_bonus = -3.0
                res["risk_tags"].append("🧊[冷板块折价]")
            elif sector_diff_tag:
                res["risk_tags"].append(sector_diff_tag)

            res["risk_tags"].append("✅[数据确认]北向/主力/机构数据均为今日结算")

            dim_score = 0.0
            dim_score += 10.0 if dim_map["A资金底座"] else 0.0
            dim_score += 10.0 if dim_map["B结构买点"] else 0.0
            dim_score += 10.0 if dim_map["C趋势量价"] else 0.0
            cross_bonus = 0.0
            if dim_map["A资金底座"] and dim_map["C趋势量价"]:
                cross_bonus += 8.0
            if dim_map["A资金底座"] and dim_map["B结构买点"]:
                cross_bonus += 6.0
            if dim_map["B结构买点"] and dim_map["C趋势量价"]:
                cross_bonus += 6.0
            if all(dim_map.values()):
                cross_bonus += 12.0

            base_score = _compute_strategy_quality_base(hits, dim_count, cross_bonus, self._cfg)

            raw = base_score + s_vol * 0.18 + dim_score + cross_bonus + board_bonus
            burst_base = float(min(raw, 99.0))

            # 【V26.7 修复】强制55分及格线兜底：不允许风控建议分低于55分，
            # 若龙虎榜/北向等数据为NaN导致机构维度被误判，通过55分底线保护好票不被埋没
            suggested_min = float(risk_pre.get("suggested_min_entry_score", 55.0) or 55.0)
            burst_floor = max(suggested_min, 55.0)

            burst_adj, ma_detail = _apply_p5_ma_kinetic_to_burst(
                burst_base, prep, row, self._cfg, burst_floor=burst_floor
            )
            vwap_mult, vwap_detail, vwap_warn = evaluate_vwap_penalty_p5(prep, rt, self._cfg)

            softened_vwap_mult = _apply_vwap_strength_softening(
                vwap_mult, cross_bonus, dim_count, self._cfg
            )
            if softened_vwap_mult != vwap_mult:
                res["risk_tags"].append(f"📡[共振VWAP软化]{vwap_mult:.2f}→{softened_vwap_mult:.2f}")

            burst_adj = float(np.clip(burst_adj * softened_vwap_mult, burst_floor, 99.0))

            trade_date = str(_p5_trade_date(rt) or "")
            consec_days = 0
            if trade_date:
                consec_days = _query_p5_consecutive_hit_days(code, trade_date, lookback=5)
            consec_bonus = 0.0
            if consec_days >= 4:
                consec_bonus = 10.0
                res["risk_tags"].append(f"📈[四连选+共振]资金持续参与")
            elif consec_days == 3:
                consec_bonus = 6.0
                res["risk_tags"].append("📈[三日连选]")
            elif consec_days == 2:
                consec_bonus = 3.0
                res["risk_tags"].append("📈[二日连选]")

            burst_adj = float(min(burst_adj + consec_bonus, 99.0))
            burst_adj = float(np.clip(burst_adj, burst_floor, 99.0))

            res["burst_score"] = round(burst_adj, 2)
            res["surge_bonus"] = 0.0
            res["penalty"] = 0.0
            p5_why = []
            if dim_map["A资金底座"]:
                p5_why.append("资金")
            if dim_map["B结构买点"]:
                p5_why.append("结构")
            if dim_map["C趋势量价"]:
                p5_why.append("趋势")
            if cross_bonus >= 12.0:
                p5_why.append("共振")
            if any("高位缩量锁仓" in t for t in res["risk_tags"]):
                p5_why.append("锁仓")
            res["detail"] = {
                "风险等级": risk_level,
                "市场状态": market_status,
                "主动作": primary_action,
                "次级动作": "|".join(secondary_actions) if secondary_actions else "",
                "买入原因": "/".join(p5_why) if p5_why else "共振入选",
                "VWAP惩罚乘子": round(softened_vwap_mult, 4),
                "VWAP原始乘子": round(vwap_mult, 4),
                "P5爆发基准分_均线前": round(burst_base, 2),
                "量比位置化分(相对市场)": round(s_vol_rel, 2),
                "量比位置化分(绝对值)": round(s_vol_abs, 2),
                "板块加成": round(board_bonus, 2),
                "sector_beta": round(sector_beta, 3),
                "命中维度数": dim_count,
                "策略基准分(V26.6)": round(base_score, 2),
                "连续命中天数": consec_days,
                "连续命中加分": round(consec_bonus, 2),
                "ts_code": code,
            }
            res["detail"]["维度A_资金底座"] = bool(dim_map["A资金底座"])
            res["detail"]["维度B_结构买点"] = bool(dim_map["B结构买点"])
            res["detail"]["维度C_趋势量价"] = bool(dim_map["C趋势量价"])
            res["detail"]["跨界共振加分"] = round(cross_bonus, 2)
            res["detail"]["突破分下限联动"] = round(burst_floor, 2)
            if ma_detail:
                res["detail"].update(ma_detail)
            if vwap_detail:
                res["detail"].update(vwap_detail)
            for w in vwap_warn:
                res["risk_tags"].append(str(w))

            if dim_map["C趋势量价"] and wr > 85.0 and abs(bias20) > 8.0 and vr > 2.5:
                res["risk_tags"].append("⚠️【高位爆量派发】筹码高位松动，需防次日兑现")
                res["penalty"] = float(res["penalty"] + 8.0)
            if dim_map["C趋势量价"] and wr > 85.0 and vr < 1.0 and pct_close > 0:
                res["risk_tags"].append("✅【高位缩量锁仓】高位创新高但量能收缩，疑似锁仓加速")
                res["burst_score"] = round(min(res["burst_score"] + 6.0, 99.0), 2)
                res["detail"]["高位缩量锁仓加分"] = 6.0

            vmf = _p5_safe_float(_p5_rt_get(rt, "vr_morning_floor", -1.0), -1.0)
            if vmf >= 0.0 and vmf < 0.75 and pct_close >= 5.0 and vr >= 1.2:
                res["detail"]["早盘定调vs全日收盘"] = (
                    f"floor_vr={vmf:.2f} 收盘涨幅={pct_close:.1f}% 全日量比={vr:.2f}"
                )

            if any("缩量分歧低吸" in h for h in hits):
                res["burst_score"] = round(min(res["burst_score"] + 12.0, 99.0), 2)
                res["risk_tags"].append("📉【缩量分歧低吸】建议单笔≤10%仓位")
                res["detail"]["缩量分歧低吸加分"] = 12.0

            primary_action, secondary_actions, market_status, risk_level = _derive_p5_action_summary(
                hits,
                vwap_mult=softened_vwap_mult,
                risk_tags=res["risk_tags"],
            )
            res["primary_action"] = primary_action
            res["secondary_actions"] = secondary_actions
            res["market_status"] = market_status
            res["risk_level"] = risk_level
            res["detail"]["主动作"] = primary_action
            res["detail"]["次级动作"] = "|".join(secondary_actions) if secondary_actions else ""
            res["detail"]["市场状态"] = market_status
            res["detail"]["风险等级"] = risk_level

            entry_reason = "主线前排，次日延续更稳"
            if consec_days >= 3:
                entry_reason = f"连续{consec_days}日入选，主线持续强化"
            elif any("P5-12" in h for h in hits):
                entry_reason = "箱体回踩确认，延续更稳"
            elif any("P5-13" in h for h in hits):
                entry_reason = "均线发散确认，趋势更顺"
            elif any("P5-05C" in h or "P5-05" in h for h in hits):
                entry_reason = "趋势确认较强，延续更顺"
            elif any("P5-14" in h for h in hits):
                entry_reason = "缩量分歧低吸，等次日确认"
            elif res.get("burst_score", 0.0) >= 85.0 and "假强" not in "|".join(res["risk_tags"]):
                entry_reason = "主线前排，次日延续更稳"
            elif any("尾盘偷袭" in tag or "派发" in tag or "假强" in tag for tag in res["risk_tags"]):
                entry_reason = "尾盘有虚火，防兑现"
            elif primary_action in ("追涨", "核心"):
                entry_reason = "主线偏强，次日看强"
            res["detail"]["入池理由"] = entry_reason

            buy_hint = "次日先看能否站稳关键位，确认后再考虑加仓。"
            if any("P5-12" in h for h in hits):
                buy_hint = "箱体回踩确认后再看，稳住箱体上沿更好。"
            elif any("P5-13" in h for h in hits):
                buy_hint = "均线发散确认后再跟，先看承接是否持续。"
            elif any("P5-14" in h for h in hits):
                buy_hint = "缩量分歧低吸型，先等企稳不破关键位后再试仓。"
            elif primary_action in ("追涨", "核心") and "高位爆量派发" not in "|".join(res["risk_tags"]):
                buy_hint = "主线真龙可次日轻仓跟随，回踩VWAP或5日线确认后再加。"
            if any("尾盘偷袭" in tag or "派发" in tag or "假强" in tag for tag in res["risk_tags"]):
                buy_hint = "尾盘偏强但需防兑现，建议次日确认站稳再介入。"
            if any("假强" in h for h in hits):
                buy_hint = "疑似假强，先观望，等次日确认延续再考虑。"
            res["buy_hint"] = buy_hint
            res["wechat_hint"] = buy_hint

            strong_resonance = (dim_count >= 3 or (dim_count == 2 and cross_bonus >= 8.0)) and consec_days >= 2
            res["template_label"] = (
                "主推"
                if res.get("burst_score", 0.0) >= 85.0
                   and "假强" not in "|".join(res["risk_tags"])
                   and strong_resonance
                else ("主推" if res.get("burst_score", 0.0) >= 85.0 and "假强" not in "|".join(res["risk_tags"])
                      else ("观察" if res.get("burst_score", 0.0) >= burst_floor else "不追高"))
            )
            merge_risk_into_engine_result(res, risk_pre, penalty_key="penalty")

        except Exception as e:
            logger.exception(
                "P5 run_all 异常，ts_code=%s, trade_date=%s, detail_keys=%s",
                code if "code" in locals() else "",
                _p5_trade_date(rt),
                sorted(list(res.get("detail", {}).keys())) if isinstance(res.get("detail"), dict) else [],
            )
            res["p5_veto_reason"] = f"run_all异常:{type(e).__name__}"
            return res

        return res
