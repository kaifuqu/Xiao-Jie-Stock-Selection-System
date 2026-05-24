# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - P4 尾盘战法引擎（核心六战法版）
【架构】
1. 硬阈值筛选在 p4_tail_screener.py；本类负责打分与外资辅助标签。
2. P5 盘后已独立为 strat_p5_postmarket.P5Postmarket；scan_engine 仅对 p4 调用本引擎。
3. 黄金门禁由 scan_engine 在 p4_core_screener_pass 为真时放宽。
"""
# Standard library
from datetime import datetime, timezone, timedelta
import logging
from typing import Optional, Tuple

# Third-party
import numpy as np
import pandas as pd

# Local modules
from core.config_manager import get_p4_tail_screener_config, get_risk_control_config
from core.strategies.p4_tail_screener import evaluate_p4_tail_screener
from core.strategies.risk_control_engine import (
    DEFAULT_RISK_CONFIG,
    RiskControlConfig,
    evaluate_3layer_risk,
    evaluate_layer2_right_side_only,
    hits_indicate_right_side_attack,
    merge_risk_into_engine_result,
)
from core.strategies.strat_base import _p1_min_circ_mv_yi_strat
from core.strategies.fund_mv_utils import (
    effective_turnover_rate_f,
    golden_tier_hk_min_shares,
    golden_tier_net_elg_min_yuan,
    golden_tier_punish_net_elg_yuan,
    infer_turnover_rate_f_pct,
)


# =============================================================================
# A 股时间体系工具（午休陷阱剔除 + 安全的 VWAP 量纲对齐）
# =============================================================================
# 与 p3_intraday_screener.py / strat_p3_intraday.py 保持完全一致的实现逻辑，
# 确保 P4 尾盘引擎层在计算 curr_min 和 VWAP 时与 screener 层对齐。
# -----------------------------------------------------------------------------
# A 股交易时间表:
#   上午盘: 09:30 - 11:30  -> 120 分钟
#   午休  : 11:30 - 13:00  -> 90 分钟（休市，不报价）
#   下午盘: 13:00 - 15:00  -> 120 分钟
#   全天合计: 240 分钟
#
# 核心问题: curr_min 从 09:25 累加（如 14:30=870），直接减 570 = 300 分钟（比实际多 30 分钟），
# 这会影响风控引擎对尾盘时段（14:30+）的分钟数判断。P4 主要在 14:30 后运行，
# 午休清洗保证了风控和 VWAP 计算的一致性。
# =============================================================================


def _curr_min_lunch_cleaned(rt):
    """
    【V26.7 新增】A股已交易分钟数（剔除午休时间）。

    curr_min 基准点:
        09:25 = 565, 09:30 = 570, 11:30 = 630, 13:00 = 720, 15:00 = 810
    """
    curr_min = float(rt.get("curr_min", 0.0) or 0.0)
    MORNING_END = 630      # 11:30
    AFTERNOON_START = 720  # 13:00
    DAY_START = 570        # 09:30

    if curr_min <= DAY_START:
        return 0.0
    if curr_min <= MORNING_END:
        return max(0.0, curr_min - DAY_START)

    # 下午盘: 上午 120 分钟 + 午后已过分钟数
    afternoon_mins = max(0.0, curr_min - AFTERNOON_START)
    return 120.0 + afternoon_mins


def _safe_vwap_from_rt(rt, ref_price, fallback_price):
    """
    【V26.7 新增】安全的盘中分时均价线（VWAP）计算。

    量纲错位问题:
    - TuShare 日线 amount 单位为"千元"，实时行情为"元"
    - volume 有时为"手"（100股），有时为"股"

    判断量纲异常：计算出的 VWAP 偏离 ref_price 超过 20%，说明量纲错位，
    降级使用 fallback_price（通常为昨收价）。
    """
    amt = float(rt.get("amount", 0.0) or 0.0)
    vol = float(rt.get("volume", 0.0) or 0.0)
    if amt <= 0 or vol <= 0:
        return fallback_price

    tentative = amt / max(vol, 1e-9)

    if ref_price > 0 and abs(tentative - ref_price) / ref_price > 0.20:
        # 尝试 vol*100（把手转股）
        corrected = amt / max(vol * 100.0, 1e-9)
        if abs(corrected - ref_price) / ref_price <= 0.20:
            return corrected
        return fallback_price

    return tentative


class P4Tail:
    def __init__(self, screener_cfg=None, risk_cfg: Optional[RiskControlConfig] = None):
        self.name = "P4尾盘智能引擎"
        self.version = "V26.6_Core6Strategies_Reindexed_VwapBetaMemory"
        self.db_cache = {}
        self._cfg_lock_external = screener_cfg is not None
        self._cfg = screener_cfg if screener_cfg is not None else get_p4_tail_screener_config()
        self._risk_cfg_lock_external = risk_cfg is not None
        if risk_cfg is not None:
            self._risk_cfg = risk_cfg
        else:
            try:
                self._risk_cfg = get_risk_control_config()
            except Exception as ex:
                logging.debug("风控配置从 config.yaml 加载失败，使用 DEFAULT_RISK_CONFIG: %s", ex)
                self._risk_cfg = DEFAULT_RISK_CONFIG

    def _map_vr(self, vr):
        xp = [0.0, 0.8, 1.2, 2.0, 4.0]
        yp = [0.0, 30.0, 60.0, 90.0, 100.0]
        return float(np.interp(vr, xp, yp))

    def _map_funds(self, amount, circ_mv_yi):
        """成交额强度映射（amount 为元）；不是换手率，只用于资金评分。"""
        if circ_mv_yi <= 0:
            return 0.0
        inflow_ratio = (amount / (circ_mv_yi * 100000000.0)) * 1000.0
        xp = [0.0, 0.5, 2.0, 5.0, 10.0]
        yp = [0.0, 30.0, 65.0, 85.0, 100.0]
        return float(np.interp(inflow_ratio, xp, yp))

    def _map_cyq(self, cyq):
        xp = [5.0, 10.0, 15.0, 20.0, 25.0, 30.0]
        yp = [100.0, 100.0, 70.0, 40.0, 20.0, 0.0]
        return float(np.interp(cyq, xp, yp))

    def _map_tail_abnormal(self, tail_vol_ratio):
        xp = [0.5, 1.0, 1.5, 3.0, 5.0]
        yp = [10.0, 50.0, 70.0, 95.0, 100.0]
        return float(np.interp(tail_vol_ratio, xp, yp))

    def _check_second_surge(self, df, rt):
        try:
            if df is None or len(df) < 60:
                return 0.0
            curr = df.iloc[-1]
            if float(curr.get("max_60d_pct", 0.0)) < 15.0:
                return 0.0
            recent_20_high = df["high"].tail(21).max()
            now_px = float(rt.get("price", 0.0))
            if now_px < recent_20_high * 0.96:
                return 0.0
            ma20 = float(curr.get("ma20", 0.0) or 0.0)
            if ma20 > 0 and now_px < ma20:
                return 0.0
            # 【性能优化 V2→V3】向量化替代 iterrows：批量计算近3日换手率
            # 保留原 try 结构，仅将 except 分支改为向量化计算
            df_tail3 = df.tail(3)
            try:
                vol3 = pd.to_numeric(df_tail3["vol"], errors="coerce").fillna(0)
                close3 = pd.to_numeric(df_tail3["close"], errors="coerce").fillna(0)
                cm3 = pd.to_numeric(df_tail3["circ_mv"], errors="coerce").fillna(0)
                tr_f3 = pd.to_numeric(df_tail3["turnover_rate_f"], errors="coerce").fillna(0)
                inferred_tr3 = np.where(
                    (tr_f3 > 0) | (cm3 <= 0) | (close3 <= 0),
                    tr_f3,
                    vol3 * 100 / np.maximum(cm3, 1e-9),
                )
                _tr3_arr = np.array(inferred_tr3, dtype=float)
                _tr3_arr = _tr3_arr[np.isfinite(_tr3_arr)]
                avg_tr3 = float(np.mean(_tr3_arr)) if len(_tr3_arr) > 0 else 0.0
                turnover_shrink = avg_tr3 < 3.0
            except Exception:
                # 兜底：改用纯 NumPy 数组计算
                vol3_arr = pd.to_numeric(df_tail3["vol"], errors="coerce").fillna(0).to_numpy()
                close3_arr = pd.to_numeric(df_tail3["close"], errors="coerce").fillna(0).to_numpy()
                cm3_arr = pd.to_numeric(df_tail3["circ_mv"], errors="coerce").fillna(0).to_numpy()
                tr_f3_arr = pd.to_numeric(df_tail3["turnover_rate_f"], errors="coerce").fillna(0).to_numpy()
                with np.errstate(divide='ignore', invalid='ignore'):
                    inferred_tr3_vec = np.where(
                        (tr_f3_arr > 0) | (cm3_arr <= 0) | (close3_arr <= 0),
                        tr_f3_arr,
                        vol3_arr * 100 / np.maximum(cm3_arr, 1e-9),
                    )
                inferred_tr3_vec = np.where(np.isfinite(inferred_tr3_vec), inferred_tr3_vec, np.nan)
                valid_tr3 = inferred_tr3_vec[~np.isnan(inferred_tr3_vec)]
                avg_tr3 = float(np.mean(valid_tr3)) if len(valid_tr3) > 0 else 0.0
                turnover_shrink = avg_tr3 < 3.0
            macd_strong = float(curr.get("macd_diff", 0.0)) > float(curr.get("macd_dea", 0.0))
            if not macd_strong and "macd" in curr.index and "macd_signal" in curr.index:
                macd_strong = float(curr.get("macd", 0.0)) > float(curr.get("macd_signal", 0.0))
            cyq_stable = float(rt.get("cyq_concentration", 99.0)) < 20.0
            matches = sum([turnover_shrink, macd_strong, cyq_stable])
            if matches == 3:
                return 8.0
            if matches == 2:
                return 5.0
            if matches == 1:
                return 2.0
            return 0.0
        except Exception as e:
            logging.debug(f"P4 _check_second_surge 异常: {e}")
            return 0.0

    def _close_vwap_deviation_pct(self, rt, df):
        try:
            close_live = float(rt.get("price", 0.0) or rt.get("close", 0.0) or 0.0)
            if close_live <= 0:
                return None
            vwap = rt.get("vwap")
            if vwap is None or (isinstance(vwap, float) and pd.isna(vwap)):
                vwap = rt.get("vwap_price")
            if vwap is None or (isinstance(vwap, float) and pd.isna(vwap)):
                # 兼容：用日线近似 VWAP，不强行造假；缺失时返回 None。
                if df is not None and len(df) > 0 and "amount" in df.columns and "vol" in df.columns:
                    last = df.iloc[-1]
                    amt = float(last.get("amount", 0.0) or 0.0)
                    vol = float(last.get("vol", 0.0) or 0.0)
                    if amt > 0 and vol > 0:
                        vwap = amt / max(vol, 1e-9)
            vwap = float(vwap) if vwap is not None and not pd.isna(vwap) else 0.0
            if vwap <= 0:
                return None
            return (close_live - vwap) / vwap * 100.0
        except Exception:
            return None

    def _validate_right_side_ma_guard(self, rt, df, close_live: float, now_price: float) -> Tuple[bool, str]:
        """
        【V26.7 新增】强制右侧均线多头防守：过滤全天均价线附近震荡但在尾盘破位跳水的标的。

        核心逻辑:
        - 必须满足 ma5 > ma20（多头排列），且 ma20 趋势向上（ma20_slope_5 > 0）。
        - 若尾盘价格跌破 ma20，说明均线支撑已失，属于左侧破位，坚决不接飞刀。
        - 此函数在入池前作为最终防守底线，与 screener 的 VWAP 4% 诱多否决共同组成双重保险。

        左侧飞刀场景示例:
        - 全天在 VWAP 附近横盘震荡，尾盘突然跳水破位 MA20；
        - 均线多头排列但尾盘价格已大幅跌破 MA5/MA20，次日继续低开概率极高。

        返回: (is_safe, veto_reason)
            is_safe=True  -> 通过防守校验，可考虑入池
            is_safe=False -> 被均线防守否决，打上明确 veto_reason 标签
        """
        if df is None or df.empty:
            # 历史数据不足时保守处理：不做均线否决（避免全灭），但记录警告
            return True, ""
        try:
            y = df.iloc[-1]
            ma5 = float(y.get("ma5", 0.0) or 0.0)
            ma20 = float(y.get("ma20", 0.0) or 0.0)
            ma60 = float(y.get("ma60", 0.0) or 0.0)
            ma20_slope = float(y.get("ma20_slope_5", 0.0) or 0.0)
            # 若昨日日线缺失均线数据，尝试从今日实时快照读取（盘中数据可能带实时均线）
            if ma5 <= 0:
                ma5 = float(rt.get("ma5", 0.0) or 0.0)
            if ma20 <= 0:
                ma20 = float(rt.get("ma20", 0.0) or 0.0)
            if ma20_slope == 0:
                ma20_slope = float(rt.get("ma20_slope_5", 0.0) or 0.0)

            # 防线1：均线多头排列（ma5 > ma20 > ma60）
            # 若 ma5 <= ma20，不满足多头排列，为左侧逆势，坚决否决
            if not (ma5 > ma20 > 0):
                return False, "均线防守否决: ma5 <= ma20（空头排列，拒绝逆势接刀）"

            # 防线2：ma20 必须向上（斜率为正）
            # ma20_slope_5 为百分比（%），> 0 表示均线向上延伸
            if ma20_slope <= 0:
                return False, f"均线防守否决: ma20_slope={ma20_slope:.4f}<=0（均线走平/向下，拒绝接盘）"

            # 防线3：收盘价不能跌破 ma20（尾盘破位飞刀）
            # 允许轻微回踩（> -0.5%），但超过此阈值视为有效破位
            if close_live > 0 and ma20 > 0:
                ma20_break_pct = (close_live - ma20) / ma20 * 100.0
                if ma20_break_pct < -0.5:
                    return False, f"均线防守否决: 尾盘破位 ma20 {ma20_break_pct:.2f}% < -0.5%（拒绝接左侧飞刀）"

            # 防线4：收盘价不能跌破 ma5（更深层破位）
            if close_live > 0 and ma5 > 0:
                ma5_break_pct = (close_live - ma5) / ma5 * 100.0
                if ma5_break_pct < -1.0:
                    return False, f"均线防守否决: 尾盘深破 ma5 {ma5_break_pct:.2f}% < -1.0%（深层破位，拒绝接刀）"

            return True, ""
        except Exception as e:
            # 数据异常时保守处理（不做否决），防止数据问题导致全灭
            logging.debug(f"P4 均线右侧防守校验异常（跳过）: {e}")
            return True, ""

    def _stock_character_memory(self, ts_code, stock_name, pool_key, df, rt):
        """龙头股性评分：历史高分 + 次日高开 + 次日加速 + 两日延续。"""
        try:
            if df is None or df.empty:
                return 0.0, []
            score = 0.0
            tags = []
            y = df.iloc[-1]
            ma5 = float(y.get("ma5", 0.0) or 0.0)
            ma20 = float(y.get("ma20", 0.0) or 0.0)
            ma60 = float(y.get("ma60", 0.0) or 0.0)
            wr = float(rt.get("winner_rate", y.get("winner_rate", 0.0)) or 0.0)
            cyq = float(rt.get("cyq_concentration", 99.0) or 99.0)
            if ma5 > ma20 > ma60:
                score += 6.0
                tags.append("📈[趋势龙头]")
            if wr >= 85.0:
                score += 4.0
            if cyq <= 18.0:
                score += 3.0

            # 1) 近期 P4 表现：高分样本越多，越像龙头股性
            recent_p4_scores = []
            try:
                from data.db_core import get_read_conn, table_exists

                if table_exists("signal_log"):
                    q = ts_code.replace("'", "''")
                    sql = f"""
                        SELECT trade_date, score, strategy
                        FROM signal_log
                        WHERE ts_code = '{q}' AND pool = 'p4'
                        ORDER BY trade_date DESC
                        LIMIT 12
                    """
                    with get_read_conn(read_only=True) as con:
                        rows = con.execute(sql).fetchall()
                    for r in rows or []:
                        try:
                            recent_p4_scores.append(float(r[1] or 0.0))
                        except Exception:
                            continue
                    if recent_p4_scores:
                        hi = sum(1 for x in recent_p4_scores if x >= 85.0)
                        mid = sum(1 for x in recent_p4_scores if 70.0 <= x < 85.0)
                        score += min(10.0, hi * 2.8 + mid * 1.0)
                        if hi >= 3:
                            tags.append("👑[历史强龙]")
                        elif hi >= 1:
                            tags.append("🚀[高开倾向]")
                        if len(recent_p4_scores) >= 6 and hi == 0:
                            score -= 4.0
                            tags.append("🧊[历史弹性弱]")
            except Exception as ex:
                logging.debug("P4 历史分样本读取跳过: %s", ex)

            # 2) 直接从日线库回看后验表现：次日高开、次日加速、两日延续
            try:
                from data.db_core import get_read_conn, table_exists

                if "ts_code" in df.columns:
                    trade_date_raw = str(y.get("trade_date", "") or "").strip()
                    ts = str(ts_code).replace("'", "''")
                    td = trade_date_raw.replace("-", "").replace("/", "")[:8]
                    if len(td) == 8 and td.isdigit():
                        src_table = "vw_daily_data_compat" if table_exists("vw_daily_data_compat") else "daily_data"
                        if table_exists(src_table):
                            sql = f"""
                                SELECT trade_date, open, high, close, pre_close
                                FROM {src_table}
                                WHERE ts_code = '{ts}' AND trade_date >= '{td}'
                                ORDER BY trade_date ASC
                                LIMIT 3
                            """
                            with get_read_conn(read_only=True) as con:
                                rows = con.execute(sql).fetchall()
                            if len(rows) >= 2:
                                # rows[0] 可能是当天，真正的“次日/两日”从 rows[1]/rows[2] 取
                                base = rows[0]
                                d1 = rows[1]
                                d2 = rows[2] if len(rows) >= 3 else None
                                base_close = float(base[3] or 0.0)
                                if base_close > 0:
                                    o1, h1, c1 = float(d1[1] or 0.0), float(d1[2] or 0.0), float(d1[3] or 0.0)
                                    gap1 = (o1 - base_close) / base_close * 100.0 if o1 > 0 else 0.0
                                    ret1 = (c1 - base_close) / base_close * 100.0 if c1 > 0 else 0.0
                                    accel1 = (h1 - o1) / o1 * 100.0 if o1 > 0 else 0.0
                                    if gap1 >= 1.5:
                                        score += 5.0
                                        tags.append("🌅[次日高开]")
                                    elif gap1 >= 0.6:
                                        score += 2.0
                                    if ret1 >= 4.0 and accel1 >= 2.0:
                                        score += 6.0
                                        tags.append("🚀[次日加速]")
                                    elif ret1 >= 2.0:
                                        score += 2.0
                                    if d2 is not None:
                                        c2 = float(d2[3] or 0.0)
                                        ret2 = (c2 - base_close) / base_close * 100.0 if c2 > 0 else 0.0
                                    else:
                                        ret2 = 0.0
                                    if ret2 >= 6.0:
                                        score += 5.0
                                        tags.append("🧱[两日延续]")
                                    elif ret2 >= 3.0:
                                        score += 2.0
                            
                            # 若次日和两日都弱，给龙头记忆轻罚，避免“假强反复”
                            if len(rows) >= 2:
                                d1_close = float(rows[1][3] or 0.0)
                                if base_close > 0 and d1_close > 0:
                                    if (d1_close - base_close) / base_close * 100.0 < -2.0:
                                        score -= 3.0
                                        tags.append("🩸[次日弱反]")
            except Exception as ex:
                logging.debug("P4 次日/两日记忆跳过: %s", ex)

            if cyq >= 25.0 and wr < 70.0:
                score -= 6.0
                tags.append("🩸[股性松散]")
            return score, tags
        except Exception as e:
            logging.debug("P4 股性记忆异常: %s", e)
            return 0.0, []

    def run_all(self, df, rt):
        if not self._cfg_lock_external:
            self._cfg = get_p4_tail_screener_config()
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
            "p4_core_screener_pass": False,
            "p4_veto_reason": "",
            "p4_strategy_checks": {},
            "risk_tags": [],
            "suggested_min_entry_score": 0.0,
            "risk_control": {},
            "buy_hint": "",
            "wechat_hint": "",
        }
        if df is None or df.empty or len(df) < 21:
            return res

        try:
            y_last = df.iloc[-1]
            now_price = float(rt.get("price", rt.get("close", y_last.get("close", 0.0))) or 0.0)
            pre_close = float(rt.get("pre_close", 0.0) or 0.0)
            if pre_close <= 0:
                pre_close = float(y_last.get("pre_close", 0.0) or 0.0)
            if pre_close <= 0:
                pre_close = float(y_last.get("close", 0.0) or 0.0)
            if pre_close <= 0 or now_price <= 0:
                return res

            open_price = float(rt.get("open", y_last.get("open", now_price)) or now_price)
            low_price = float(rt.get("low", y_last.get("low", now_price)) or now_price)
            today_high = float(rt.get("high", y_last.get("high", now_price)) or now_price)
            pct_chg = (now_price - pre_close) / pre_close * 100.0
            # vr=量比；turnover_f=真实自由流通换手率（%），两者口径不同，不能混用。
            vr = float(rt.get("vol_ratio", y_last.get("vol_ratio", 0.0)) or 0.0)
            turnover_f = effective_turnover_rate_f(rt, y_last, now_price)

            circ_mv_raw = rt.get("circ_mv")
            if circ_mv_raw is None or pd.isna(circ_mv_raw):
                circ_mv_raw = float(df["circ_mv"].iloc[-1]) if "circ_mv" in df.columns else float(rt.get("total_mv", 10000000)) * 0.6
            circ_mv_yi = float(circ_mv_raw) / 10000.0

            if circ_mv_yi < _p1_min_circ_mv_yi_strat():
                return res

            rt_risk = dict(rt)
            rt_risk["_pool_key"] = str(rt.get("_pool_key", "p4"))
            _bj = timezone(timedelta(hours=8))
            _now = datetime.now(_bj)
            rt_risk.setdefault("curr_min", _now.hour * 60 + _now.minute)
            risk_pre = evaluate_3layer_risk(df, rt_risk, self._risk_cfg, is_right_side_strategy=False, pool_key="p4")
            if not risk_pre.get("pass_layer1", False):
                res["p4_veto_reason"] = str(risk_pre.get("veto_reason", "") or "")
                res["risk_control"] = {
                    "pass_layer1": False,
                    "pass_layer2": True,
                    "veto_reason": res["p4_veto_reason"],
                    "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),
                    "risk_tags": list(risk_pre.get("risk_tags", []) or []),
                    "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),
                    "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),
                }
                res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])
                res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)
                return res

            if circ_mv_yi >= 2000.0:
                size_emoji = "🦍"
            elif circ_mv_yi >= 1000.0:
                size_emoji = "🐘+"
            elif circ_mv_yi >= 500.0:
                size_emoji = "🐘"
            elif circ_mv_yi >= 100.0:
                size_emoji = "🐎"
            else:
                size_emoji = "🐥"

            req_main = golden_tier_net_elg_min_yuan(circ_mv_yi)
            req_hk = golden_tier_hk_min_shares(circ_mv_yi)
            punish_main = golden_tier_punish_net_elg_yuan(circ_mv_yi)

            cyq_conc = float(rt.get("cyq_concentration", y_last.get("cyq_concentration", 999.0)) or 999.0)
            hk_vol = float(rt.get("hk_vol", y_last.get("hk_vol", 0.0)) or 0.0)
            net_main_amount = float(rt.get("net_main_amount", y_last.get("net_main_amount", 0.0)) or 0.0)
            hk_assist = hk_vol > req_hk and net_main_amount > req_main

            ev = evaluate_p4_tail_screener(df, rt, self._cfg)
            res["p4_core_screener_pass"] = bool(ev.get("p4_core_screener_pass"))
            res["p4_veto_reason"] = str(ev.get("veto_reason", "") or "")
            res["p4_strategy_checks"] = dict(ev.get("strategy_checks") or {})

            if not ev.get("veto_pass"):
                return res

            hits = list(ev.get("strategies") or [])
            if not hits:
                return res

            if hits_indicate_right_side_attack(hits):
                l2 = evaluate_layer2_right_side_only(df, rt_risk, self._risk_cfg)
                for uw in l2.get("ui_warnings") or []:
                    res["risk_tags"].append("⚠️[二层预警]" + str(uw))
                if not l2.get("pass_layer2", True):
                    res["p4_veto_reason"] = str(l2.get("veto_reason", "") or "")
                    res["risk_control"] = {
                        "pass_layer1": True,
                        "pass_layer2": False,
                        "veto_reason": res["p4_veto_reason"],
                        "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),
                        "risk_tags": list(risk_pre.get("risk_tags", []) or []),
                        "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),
                        "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),
                    }
                    res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])
                    res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)
                    return res

            close_vwap_dev = self._close_vwap_deviation_pct(rt, df)
            # 【V26.7 新增】强制右侧均线多头防守：在所有评分之前先过均线验证
            # 过滤全天均价线附近震荡但在尾盘破位跳水的标的，绝对不接左侧飞刀
            ma_guard_pass, ma_guard_reason = self._validate_right_side_ma_guard(
                rt, df, now_price, now_price
            )
            if not ma_guard_pass:
                res["p4_veto_reason"] = ma_guard_reason
                res["p4_core_screener_pass"] = False
                res["risk_tags"].append("⚠️[均线防守]" + ma_guard_reason)
                res["risk_control"] = {
                    "pass_layer1": True,
                    "pass_layer2": False,
                    "veto_reason": ma_guard_reason,
                    "penalty": 0.0,
                    "risk_tags": res["risk_tags"],
                    "suggested_min_entry_score": 0.0,
                    "ui_warnings": [],
                }
                return res

            vwap_penalty = 0.0
            if close_vwap_dev is not None:
                res["detail"]["close_vwap_dev_pct"] = round(float(close_vwap_dev), 3)
                if close_vwap_dev > 2.5:
                    res["risk_tags"].append("🧨[尾盘偷袭]收盘显著高于VWAP")
                    vwap_penalty += 18.0
                elif close_vwap_dev < -1.5:
                    res["risk_tags"].append("🧊[弱收盘]收盘低于VWAP")
                    vwap_penalty += 8.0

            vwap = 0.0
            vwap_raw = rt.get("vwap")
            if vwap_raw is None or (isinstance(vwap_raw, float) and pd.isna(vwap_raw)):
                vwap_raw = rt.get("vwap_price")
            try:
                vwap = float(vwap_raw) if vwap_raw is not None and not pd.isna(vwap_raw) else 0.0
            except (TypeError, ValueError):
                vwap = 0.0
            if vwap <= 0 and close_vwap_dev is not None:
                vwap = now_price / (1.0 + float(close_vwap_dev) / 100.0)

            intraday_high_gap = ((today_high - now_price) / now_price * 100.0) if now_price > 0 else 0.0
            vwap_gap_pct = ((now_price - vwap) / vwap * 100.0) if vwap > 0 else 0.0
            if pct_chg >= 2.5 and vwap_gap_pct > 1.2 and vr >= 1.6:
                res["risk_tags"].append("🧨[假突破]尾盘远离VWAP追高")
                vwap_penalty += 20.0
            if intraday_high_gap > 1.8 and now_price < today_high * 0.985 and vr >= 1.5:
                res["risk_tags"].append("🧨[冲高回落]收盘承接不足")
                vwap_penalty += 18.0

            # 聪明钱验证：尾盘量比放大但主力净流出 → 仅 UI 级重罚与标签，不再「枪毙」清空战法命中
            tail_vr_chk = float(rt.get("tail_vol_ratio", y_last.get("tail_vol_ratio", 0.0)) or 0.0)
            _nm_rt = rt.get("net_main_amount")
            if _nm_rt is None or (isinstance(_nm_rt, float) and pd.isna(_nm_rt)):
                net_main_chk = float(y_last.get("net_main_amount", 0.0) or 0.0)
            else:
                try:
                    net_main_chk = float(_nm_rt)
                except (TypeError, ValueError):
                    net_main_chk = float(y_last.get("net_main_amount", 0.0) or 0.0)
            tail_smart_penalty = 0.0
            if tail_vr_chk > 2.0 and net_main_chk < 0.0:
                tail_smart_penalty = 100.0
                res["risk_tags"].append("🩸[派发预警]尾盘放量但主力净流出（请人工复核）")

            penalty = float(tail_smart_penalty) + float(vwap_penalty)
            if vr < 0.8:
                penalty += float(np.interp(vr, [0.0, 0.5, 0.8], [50.0, 20.0, 0.0]))
            if turnover_f < 0.8:
                penalty += float(np.interp(turnover_f, [0.0, 0.4, 0.8], [60.0, 25.0, 0.0]))

            entity_bottom = min(now_price, open_price)
            lower_shadow_pct = (entity_bottom - low_price) / pre_close * 100.0
            is_deep_v = lower_shadow_pct > 2.5 and pct_chg > 0
            if is_deep_v:
                penalty -= 15.0

            vmf = float(rt.get("vr_morning_floor", y_last.get("vr_morning_floor", -1.0)) or -1.0)
            if vmf < 0.0:
                vmf = float(rt.get("vr_1030", y_last.get("vr_1030", -1.0)) or -1.0)
            if vmf >= 0.0 and vmf < 0.8:
                penalty += 40.0

            if pct_chg >= 7.0:
                penalty += 25.0

            s_vol = self._map_vr(vr)
            s_fund = self._map_funds(hk_vol + net_main_amount, circ_mv_yi)
            s_chip = self._map_cyq(cyq_conc)
            tail_ratio_val = float(rt.get("tail_vol_ratio", y_last.get("tail_vol_ratio", 1.0)) or 1.0)
            s_tail_abnormal = self._map_tail_abnormal(tail_ratio_val)
            raw_burst = (s_vol * 0.25) + (s_fund * 0.3) + (s_chip * 0.2) + (s_tail_abnormal * 0.25)

            if vr < 0.6 or cyq_conc > 25.0:
                burst_score = min(raw_burst, 50.0)
            else:
                burst_score = min(raw_burst, 100.0)

            stock_memory_score, stock_memory_tags = self._stock_character_memory(
                rt.get("ts_code", rt.get("code", "")),
                rt.get("name", ""),
                str(rt.get("_pool_key", "p4")),
                df,
                rt,
            )
            next_day_confidence = 0.0
            if stock_memory_score:
                res["detail"]["stock_memory_score"] = round(float(stock_memory_score), 2)
                if stock_memory_tags:
                    res["risk_tags"].extend(stock_memory_tags)
                if any(tag in stock_memory_tags for tag in ("🌅[次日高开]", "🚀[次日加速]", "🧱[两日延续]")):
                    next_day_confidence += 2.0
                if any(tag in stock_memory_tags for tag in ("🧊[历史弹性弱]",)):
                    next_day_confidence -= 1.5

            surge_bonus_val = self._check_second_surge(df, rt) if len(df) >= 60 else 0.0
            if stock_memory_score:
                surge_bonus_val += min(8.0, max(0.0, stock_memory_score) * 0.22)
            if next_day_confidence:
                surge_bonus_val += min(6.0, max(-3.0, next_day_confidence))

            pool_hint = str(rt.get("_pool_key", "")).lower()
            # 【V26.6 修复】移除 now_min >= 900 自动 P5 模式切换。
            # 原逻辑：若用户点 P4 按钮（pool_hint="p4"），但系统时间是 15:00 后，
            # 自动触发 P5 增强逻辑（+3分奖励 或 +42分惩罚），导致 14:59 和 15:01 扫 P4 结果完全不同。
            # 这违反了用户意图：用户点哪个池就应跑哪个池的逻辑，不应被时钟自动篡改。
            # 修复：仅以 _pool_key 为准；P4 按钮 → P4 逻辑，P5 按钮 → P5 逻辑。
            is_p5_mode = (pool_hint == "p5")

            if is_p5_mode:
                positive_hits_before_p5 = len([h for h in hits if ("⚠️" not in h and "💀" not in h and "🩸" not in h)])
                if net_main_amount > req_main and positive_hits_before_p5 >= 1:
                    hits.append("👑[实锤] 主力真金盖章")
                    surge_bonus_val += 3.0
                elif net_main_amount < punish_main:
                    hits.append("🩸[绞肉机] 尾盘诱多派发")
                    penalty += 42.0
                if hk_vol > req_hk:
                    hits.append("✈️[外资辅助] 北向实盘认同")

            if hits and hk_assist and not any("✈️" in h for h in hits):
                hits.append("✈️[外资辅助]")

            sector_beta = 1.0
            if isinstance(rt.get("sector_beta"), (int, float)) and not pd.isna(rt.get("sector_beta")):
                sector_beta = float(rt.get("sector_beta", 1.0) or 1.0)
            elif isinstance(rt.get("industry_beta"), (int, float)) and not pd.isna(rt.get("industry_beta")):
                sector_beta = float(rt.get("industry_beta", 1.0) or 1.0)
            elif isinstance(rt.get("sector_mult"), (int, float)) and not pd.isna(rt.get("sector_mult")):
                sector_beta = float(rt.get("sector_mult", 1.0) or 1.0)
            sector_beta = max(0.7, min(1.5, sector_beta))
            board_bonus = 0.0
            if sector_beta >= 1.20:
                board_bonus = 6.0
                res["risk_tags"].append("🌋[热板块加权]")
            elif sector_beta >= 1.10:
                board_bonus = 3.0
                res["risk_tags"].append("🔥[偏热板块]")
            elif sector_beta < 0.9:
                board_bonus = -3.0
                res["risk_tags"].append("🧊[冷板块折价]")

            # 【V26.6 优化】盘尾滞后数据警告：hk_vol / net_main_amount / inst_net_buy 均为昨日结算数据
            hk_note = ev.get("detail", {}).get("hk_vol_data_note", "")
            if hk_note and "昨" in hk_note:
                res["risk_tags"].append(f"📡[数据延迟]{hk_note}")

            res["burst_score"] = round(min(burst_score + board_bonus, 100.0), 2)
            res["surge_bonus"] = round(surge_bonus_val, 2)
            res["penalty"] = round(penalty, 2)
            sector_strength = float(rt.get("sector_strength", ev.get("detail", {}).get("sector_strength", sector_beta)) or sector_beta)
            mainline_score = float(rt.get("mainline_score", ev.get("detail", {}).get("mainline_score", 0.0)) or 0.0)
            mainline_reason = str(rt.get("mainline_reason", ev.get("detail", {}).get("mainline_reason", "")) or "")
            has_p4_11 = any("P4-11" in h for h in hits)
            has_p4_07 = any("P4-07" in h for h in hits)
            has_p4_08 = any("P4-08" in h for h in hits)
            has_p4_09 = any("P4-09" in h for h in hits)
            has_p4_10 = any("P4-10" in h for h in hits)

            entry_reason = "尾盘信号偏强，可轻仓试仓"
            if has_p4_07:
                entry_reason = "底仓强票，尾盘仍稳，均线持续多头"
            elif has_p4_11:
                entry_reason = "主线底仓共振，尾盘确认强势，等次日延续"
            elif has_p4_08:
                entry_reason = "均线修复信号，量价配合，等均线收复确认"
            elif has_p4_09:
                entry_reason = "多头回踩企稳，量能回暖，等尾盘确认再攻"
            elif has_p4_10:
                entry_reason = "主升缩量再攻，等量能配合再确认"
            elif mainline_score >= 3.0:
                if next_day_confidence >= 1.5:
                    entry_reason = "主线前排，延续更稳"
                elif sector_strength >= 1.10:
                    entry_reason = "主线偏强，等次日看"
                else:
                    entry_reason = "主线里偏强，等确认"
            elif mainline_score <= -2.0:
                entry_reason = "不像主线，防假强"
            elif vwap_penalty > 0 or tail_smart_penalty > 0:
                entry_reason = "尾盘有虚火，防兑现"
            res["detail"] = {
                "异动分": round(s_tail_abnormal, 2),
                "抢筹分": round(s_fund, 2),
                "筹码分": round(s_chip, 2),
                "入池理由": entry_reason,
                "市值档位": size_emoji,
                "sector_beta": round(sector_beta, 3),
                "sector_strength": round(sector_strength, 3),
                "mainline_score": round(mainline_score, 2),
                "mainline_reason": mainline_reason,
                "board_bonus": round(board_bonus, 2),
                "next_day_confidence": round(next_day_confidence, 2),
                "p4_screener": ev.get("detail", {}),
            }
            res["strategies"] = hits
            buy_hint = "尾盘信号偏强，可轻仓试仓，次日不破VWAP再加。"
            if has_p4_07:
                buy_hint = "底仓强票，尾盘仍稳，均线上方运行良好，回踩不破可继续看。"
            elif has_p4_11:
                buy_hint = "主线底仓共振，次日可适当加仓，注意不破均线即可持有。"
            elif has_p4_08:
                buy_hint = "均线修复型，等次日站稳均线后再加仓。"
            elif has_p4_09:
                buy_hint = "多头回踩企稳，次日量能配合则可跟进。"
            elif has_p4_10:
                buy_hint = "主升缩量再攻，次日放量配合则可追加。"
            elif mainline_score >= 3.0:
                if next_day_confidence >= 1.5:
                    buy_hint = "主线板块尾盘龙头优先，可先轻仓埋伏，次日站稳VWAP或5日线后再加。"
                elif sector_strength >= 1.10:
                    buy_hint = "主线板块尾盘偏强，可轻仓试仓，次日回踩VWAP或5日线确认后再加。"
                else:
                    buy_hint = "主线板块内偏强票，可先小仓试，等次日确认承接后再加。"
            elif mainline_score <= -2.0:
                buy_hint = "疑似假强尾盘，先观察，不追高，等主线重新确认。"
            elif vwap_penalty > 0 or tail_smart_penalty > 0:
                buy_hint = "尾盘承接一般，建议等次日站稳VWAP后再考虑。"
            res["buy_hint"] = buy_hint
            res["wechat_hint"] = buy_hint

            merge_risk_into_engine_result(res, risk_pre, penalty_key="penalty")

        except Exception as e:
            logging.debug(f"P4 run_all 异常: {e}")
            return res

        return res
