# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - P3 盘中战法池（物理胸甲八大策略 + 热成像增强版）
【架构】
1. 硬阈值筛选集中在 p3_intraday_screener.py（全局否决 + 八大客观策略），单次 O(1) 尾部访问。
2. 本类负责：盘口组装、日内风控惩罚（VWAP/上影/早盘量比）、DuckDB Z-Score 附录、爆发分合成。
3. 黄金起爆点门禁交由 scan_engine：命中 p3_core_screener_pass 时放宽，避免与低吸/托底区间冲突。
"""
# Standard library
from datetime import datetime, timezone, timedelta
import logging
import os
from typing import Optional

# Third-party
import numpy as np
import pandas as pd

# Local modules
try:
    from core.strategies.fund_mv_utils import effective_turnover_rate_f, mean_effective_turnover_f_last_n
except ImportError:
    from strategies.fund_mv_utils import (  # type: ignore
        effective_turnover_rate_f,
        mean_effective_turnover_f_last_n,
    )

from core.config_manager import get_p3_intraday_screener_config, get_risk_control_config
from core.strategies.p3_intraday_screener import evaluate_p3_intraday_screener
from core.strategies.strat_base import _p1_min_circ_mv_yi_strat
from core.strategies.risk_control_engine import (
    DEFAULT_RISK_CONFIG,
    RiskControlConfig,
    evaluate_3layer_risk,
    evaluate_layer2_right_side_only,
    hits_indicate_right_side_attack,
    merge_risk_into_engine_result,
)

logger = logging.getLogger(__name__)


# =============================================================================
# A 股时间体系工具（午休陷阱剔除 + 安全的 VWAP 量纲对齐）
# =============================================================================
# 以下两个函数从 p3_intraday_screener.py 同步复刻，确保 strat_p3_intraday.py
# 引擎层与 screener 层使用完全一致的时钟逻辑和量纲安全 VWAP。
# -----------------------------------------------------------------------------
# A 股交易时间表:
#   上午盘: 09:30 - 11:30  -> 120 分钟
#   午休  : 11:30 - 13:00  -> 90 分钟（休市，不报价）
#   下午盘: 13:00 - 15:00  -> 120 分钟
#   全天合计: 240 分钟
#
# 核心问题: 若 curr_min 从 09:25 累加（如 13:01=721），直接减 570 会导致
# 下午 elapsed_mins 虚增 30 分钟，使预估全天成交量出现断崖式下跌。
# =============================================================================


def _curr_min_lunch_cleaned(rt):
    """
    【V26.7 新增】A股已交易分钟数（剔除午休时间）。

    curr_min 基准点说明:
        09:25 集合竞价 = 565（有时注入）
        09:30 上午开盘 = 570
        11:30 上午收盘 = 630
        13:00 下午开盘 = 720
        15:00 下午收盘 = 810
    """
    curr_min = _safe_float(rt.get("curr_min"), 0.0)
    MORNING_END = 630      # 11:30
    AFTERNOON_START = 720 # 13:00
    DAY_START = 570       # 09:30

    if curr_min <= DAY_START:
        # 集合竞价阶段(09:25-09:30): 视为已交易 0 分钟
        return 0.0
    if curr_min <= MORNING_END:
        # 上午盘: 直接减去基准点，即得到已交易分钟数
        return max(0.0, curr_min - DAY_START)

    # 下午盘: 先计入上午完整 120 分钟，再剔除午休 90 分钟（curr_min-720 即午后已过分钟数）
    afternoon_mins = max(0.0, curr_min - AFTERNOON_START)
    return 120.0 + afternoon_mins


def _safe_vwap_from_rt(rt, ref_price, fallback_price):
    """
    【V26.7 新增】安全的盘中分时均价线（VWAP）计算。

    量纲错位问题（A股数据常见顽疾）:
    - TuShare 日线: amount 字段单位为"千元"
    - 实时行情快照: amount 字段单位为"元"
    - volume 有时为"手"（100股），有时为"股"

    判断量纲异常的核心依据:
    - 计算出的 VWAP 偏离参考价（昨收）超过 20%，说明量纲错位。
    - 此时降级使用 fallback_price（通常为昨收价）。

    返回: 安全的 VWAP 值（float），量纲异常时返回 fallback_price。
    """
    amt = _safe_float(rt.get("amount"), 0.0)
    vol = _safe_float(rt.get("volume"), 0.0)
    if amt <= 0 or vol <= 0:
        return fallback_price

    # 假设 volume 单位为"股"（最常见），计算 tentative VWAP
    tentative = amt / max(vol, 1e-9)

    # 量纲合理性校验：偏离 ref_price 超过 20% → 尝试 vol*100（把手转股）
    if ref_price > 0 and abs(tentative - ref_price) / ref_price > 0.20:
        vol_as_hand = vol * 100.0
        corrected = amt / max(vol_as_hand, 1e-9)
        if ref_price > 0 and abs(corrected - ref_price) / ref_price <= 0.20:
            return corrected
        # 修正后仍不合理，降级到 fallback
        return fallback_price

    return tentative


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        if pd.isna(val) or str(val).strip() in ["", "-"]:
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _p3_strategy_tier(hit: str) -> str:
    s = str(hit or "")
    if any(k in s for k in ["右侧起爆", "均线低吸", "巨头连贯发力", "倍量启动延续"]):
        return "core"
    if any(k in s for k in ["单峰跃迁", "平台/二次确认", "水上金叉", "资金逆势托底"]):
        return "aux"
    if any(k in s for k in ["质量趋势底仓", "缩量分歧低吸"]):
        return "obs"
    return "aux"


class P3Intraday:
    def __init__(self, screener_cfg=None, risk_cfg: Optional[RiskControlConfig] = None):
        """
        screener_cfg: 可选 P3IntradayScreenerConfig，用于回测/实盘分区调参。
        risk_cfg: 可选三层风控；不传则从 config.yaml ``risk_control`` 加载（见 get_risk_control_config）。
        """
        self.name = "P3盘中引擎"
        self.version = "V26.57Strategies"
        self.db_cache = {}
        self._cfg_lock_external = screener_cfg is not None
        self._cfg = screener_cfg if screener_cfg is not None else get_p3_intraday_screener_config()
        self._risk_cfg_lock_external = risk_cfg is not None
        if risk_cfg is not None:
            self._risk_cfg = risk_cfg
        else:
            try:
                self._risk_cfg = get_risk_control_config()
            except Exception as ex:
                logger.debug("风控配置从 config.yaml 加载失败，使用 DEFAULT_RISK_CONFIG: %s", ex)
                self._risk_cfg = DEFAULT_RISK_CONFIG
        self._load_zscore_db()

    def _load_zscore_db(self):
        """【可选增强】DuckDB 60 日成交量切片，仅按单代码字典查找，全表只在启动读一次。"""
        try:
            from data.db_core import get_duckdb_path, get_read_conn_singleton

            db_path = get_duckdb_path()
            if not os.path.exists(db_path):
                return
            # 必须与 db_core 全局连接复用：禁止再 duckdb.connect(read_only=True)，
            # 否则在本进程已有写连接时会触发 DuckDB「same database file with a different configuration」。
            con = get_read_conn_singleton()
            if con is None:
                logging.warning("⚠️ DuckDB 只读不可用，P3 核爆 Z-Score 静默。")
                return
            tables = con.execute("SHOW TABLES").fetchdf()
            if "vol_slice_60d" in tables["name"].values:
                df = con.execute("SELECT * FROM vol_slice_60d").fetchdf()
                # 【性能优化 V2】向量化替代 iterrows：批量字典构造
                if df is not None and not df.empty and "ts_code" in df.columns:
                    ts_codes = df["ts_code"].astype(str).str.strip()
                    for idx in df.index:
                        ts = str(ts_codes.iloc[idx])
                        if not ts:
                            continue
                        s_code = ts.split(".")[0][:6]
                        self.db_cache[s_code] = {
                            "1030_mean": _safe_float(df.at[df.index[idx], "slice_1030_mean"], 0.0),
                            "1030_std": _safe_float(df.at[df.index[idx], "slice_1030_std"], 0.0),
                            "1300_mean": _safe_float(df.at[df.index[idx], "slice_1300_mean"], 0.0),
                            "1300_std": _safe_float(df.at[df.index[idx], "slice_1300_std"], 0.0),
                        }
            logging.info(f"✅ P3 热成像底座加载成功 | 覆盖标的: {len(self.db_cache)} 只")
        except ImportError:
            logging.warning("⚠️ 未安装 duckdb，P3 核爆 Z-Score 静默。")
        except Exception as e:
            logging.error(f"❌ 加载 DuckDB 异常: {e}")

    def _map_momentum(self, pct_chg, atr_pct, vr):
        if atr_pct <= 0:
            atr_pct = 2.0
        relative_move = abs(pct_chg) / atr_pct
        momentum_factor = relative_move * vr
        xp = [0.0, 0.5, 1.0, 2.5, 5.0]
        yp = [0.0, 30.0, 60.0, 90.0, 100.0]
        return float(np.interp(momentum_factor, xp, yp))

    def _map_chip_kill(self, now_price, cost_95th):
        if cost_95th <= 0:
            return 50.0
        ratio = now_price / cost_95th
        xp = [0.90, 0.95, 1.00, 1.02, 1.05]
        yp = [0.0, 30.0, 60.0, 90.0, 100.0]
        return float(np.interp(ratio, xp, yp))

    def _map_funds(self, amount, circ_mv_yi):
        if circ_mv_yi <= 0:
            return 0.0
        inflow_ratio = (amount / (circ_mv_yi * 100000000.0)) * 1000.0
        xp = [0.0, 0.5, 2.0, 5.0, 10.0]
        yp = [0.0, 30.0, 65.0, 85.0, 100.0]
        return float(np.interp(inflow_ratio, xp, yp))

    def _check_second_surge(self, df, rt):
        try:
            if df is None or len(df) < 60:
                return 0.0
            curr = df.iloc[-1]
            if _safe_float(curr.get("max_60d_pct", 0.0)) < 15.0:
                return 0.0
            recent_20_high = df["high"].tail(21).max()
            now_px = _safe_float(rt.get("price", 0.0))
            if now_px < recent_20_high * 0.96:
                return 0.0
            ma20 = _safe_float(curr.get("ma20", 0.0))
            if ma20 > 0 and now_px < ma20:
                return 0.0
            turnover_shrink = mean_effective_turnover_f_last_n(df, 3) < 3.0 if len(df) >= 3 else False
            macd_strong = _safe_float(curr.get("macd_diff", 0.0)) > _safe_float(curr.get("macd_dea", 0.0))
            if not macd_strong and "macd" in curr.index and "macd_signal" in curr.index:
                macd_strong = _safe_float(curr.get("macd", 0.0)) > _safe_float(curr.get("macd_signal", 0.0))
            cyq_stable = _safe_float(rt.get("cyq_concentration", 99.0)) < 20.0
            matches = sum([turnover_shrink, macd_strong, cyq_stable])
            if matches == 3:
                return 8.0
            if matches == 2:
                return 5.0
            if matches == 1:
                return 2.0
            return 0.0
        except Exception as e:
            logging.debug(f"P3 _check_second_surge 异常: {e}")
            return 0.0

    def run_all(self, df, rt):
        """
        增量入口：df 为截至昨收的日线；rt 为当前快照（price/volume/amount/...）。
        """
        if not self._cfg_lock_external:
            self._cfg = get_p3_intraday_screener_config()
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
            "p3_core_screener_pass": False,
            "p3_veto_reason": "",
            "p3_strategy_checks": {},
            "risk_tags": [],
            "suggested_min_entry_score": 0.0,
            "risk_control": {},
        }
        if df is None or df.empty or len(df) < 21:
            return res

        try:
            now_price = _safe_float(rt.get("price", 0.0))
            curr = df.iloc[-1]
            pre_close = _safe_float(rt.get("pre_close"), 0.0)
            if pre_close <= 0:
                pre_close = _safe_float(curr.get("pre_close"), 0.0)
            if pre_close <= 0:
                pre_close = _safe_float(curr.get("close"), 0.0)
            if pre_close <= 0 or now_price <= 0:
                return res

            open_price = _safe_float(rt.get("open", now_price))
            high_price = _safe_float(rt.get("high", now_price))
            low_price = _safe_float(rt.get("low", now_price))

            pct_chg = (now_price - pre_close) / pre_close * 100.0
            vr = _safe_float(rt.get("vol_ratio", 0.0))
            turnover_f = float(effective_turnover_rate_f(rt, curr, now_price))
            regime_state = str(rt.get("_regime_state", rt.get("regime", "")) or "")
            regime_score = _safe_float(rt.get("_regime_score", rt.get("market_contraction_score", 0.0)), 0.0)
            t1_boost = _safe_float(rt.get("_t1_memory_boost", 0.0), 0.0)
            t1_win = _safe_float(rt.get("_t1_win_rate_pct", 0.0), 0.0)
            t1_avg = _safe_float(rt.get("_t1_avg_ret_pct", 0.0), 0.0)

            amount = _safe_float(rt.get("amount", 0.0))
            vol = _safe_float(rt.get("volume"), 0.0)

            # 【V26.7 重构】安全的 VWAP 计算：使用 _safe_vwap_from_rt 替代旧版启发式判断。
            # 旧版问题：手动试探 tentative > now_price*20 来猜测量纲，逻辑粗糙且不稳定。
            # 新版：统一用 20% 偏离阈值 + 把手/股自动修正 + 降级 fallback，防止量纲异常导致 VWAP 失效。
            if amount > 0 and vol > 0:
                vwap = _safe_vwap_from_rt(rt, pre_close, pre_close)
            else:
                vwap = _safe_float(curr.get("vwap"), pre_close)

            cost_95th = _safe_float(rt.get("cost_95th", 9999.0))
            cyq_conc = _safe_float(rt.get("cyq_concentration", 999.0))
            hk_vol = _safe_float(rt.get("hk_vol", 0.0))

            df_atr = df.tail(15).copy()
            df_atr["tr1"] = df_atr["high"] - df_atr["low"]
            df_atr["tr2"] = (df_atr["high"] - df_atr["pre_close"]).abs()
            df_atr["tr3"] = (df_atr["low"] - df_atr["pre_close"]).abs()
            atr = df_atr[["tr1", "tr2", "tr3"]].max(axis=1).mean()
            atr_pct = (atr / pre_close) * 100.0 if pre_close > 0 else 2.0

            circ_mv_raw = rt.get("circ_mv")
            if pd.isna(circ_mv_raw) or circ_mv_raw is None:
                circ_mv_raw = _safe_float(df["circ_mv"].iloc[-1]) if "circ_mv" in df.columns else _safe_float(rt.get("total_mv", 10000000)) * 0.6
            circ_mv_yi = _safe_float(circ_mv_raw) / 10000.0

            if circ_mv_yi >= 2000.0:
                size_emoji = "巨无霸(2000亿+)"
            elif circ_mv_yi >= 1000.0:
                size_emoji = "千亿中军(1000-2000亿)"
            elif circ_mv_yi >= 500.0:
                size_emoji = "超级中军(500-1000亿)"
            else:
                size_emoji = "核心中盘(100-500亿)"

            if circ_mv_yi < _p1_min_circ_mv_yi_strat():
                return res

            # ================= 三层风控（最高优先级）：第一层死亡红线 =================
            rt_risk = dict(rt)
            rt_risk["_pool_key"] = str(rt.get("_pool_key", "p3"))
            # 【V26.7 午休时间清洗】curr_min 必须经 _curr_min_lunch_cleaned 处理后才能传给风控引擎。
            # 原因：风控引擎内部的分钟数判断（如 11:30 后做某类风控）依赖真实的已交易分钟数。
            # 若传入未清洗的 curr_min（如 13:01=721），下午的风控模块会把 11:30-13:00 的 90 分钟
            # 误算为已交易时间，导致所有下午才生效的风控（如 13:00+ 某类阈值）提前错误触发。
            _bj = timezone(timedelta(hours=8))
            _now = datetime.now(_bj)
            rt_risk["curr_min"] = _curr_min_lunch_cleaned({"curr_min": float(_now.hour * 60 + _now.minute)})
            risk_pre = evaluate_3layer_risk(df, rt_risk, self._risk_cfg, is_right_side_strategy=False, pool_key="p3")
            if not risk_pre.get("pass_layer1", False):
                res["p3_veto_reason"] = str(risk_pre.get("veto_reason", "") or "")
                res["risk_control"] = {
                    "pass_layer1": False,
                    "pass_layer2": True,
                    "veto_reason": res["p3_veto_reason"],
                    "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),
                    "risk_tags": list(risk_pre.get("risk_tags", []) or []),
                    "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),
                    "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),
                }
                res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])
                res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)
                return res

            # ================= 物理胸甲：硬阈值筛选（流式友好，仅尾部 + rt）=================
            ev = evaluate_p3_intraday_screener(df, rt, self._cfg)
            res["p3_core_screener_pass"] = bool(ev.get("p3_core_screener_pass"))
            res["p3_veto_reason"] = str(ev.get("veto_reason", "") or "")
            res["p3_strategy_checks"] = dict(ev.get("strategy_checks") or {})

            if not ev.get("veto_pass"):
                return res

            hits = list(ev.get("strategies") or [])
            if not hits:
                return res

            tiered = {"core": [], "aux": [], "obs": []}
            for h in hits:
                tiered[_p3_strategy_tier(h)].append(h)
            core_hits = tiered["core"]
            aux_hits = tiered["aux"]
            obs_hits = tiered["obs"]

            # ================= 三层风控：第二层（右侧攻击专属） =================
            if hits_indicate_right_side_attack(core_hits or hits):
                l2 = evaluate_layer2_right_side_only(df, rt_risk, self._risk_cfg)
                for uw in l2.get("ui_warnings") or []:
                    res["risk_tags"].append("⚠️[二层预警]" + str(uw))
                if not l2.get("pass_layer2", True):
                    res["p3_veto_reason"] = str(l2.get("veto_reason", "") or "")
                    res["risk_control"] = {
                        "pass_layer1": True,
                        "pass_layer2": False,
                        "veto_reason": res["p3_veto_reason"],
                        "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),
                        "risk_tags": list(risk_pre.get("risk_tags", []) or []),
                        "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),
                        "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),
                    }
                    res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])
                    res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)
                    return res

            penalty = 0.0
            if vr < 1.1:
                penalty += float(np.interp(vr, [0.0, 0.5, 1.1], [40.0, 20.0, 0.0]))
            if turnover_f < 0.8:
                penalty += float(np.interp(turnover_f, [0.0, 0.4, 0.8], [50.0, 20.0, 0.0]))
            # 不再因低量比直接 return：★均线低吸 本身要求缩量，与旧版「vr<0.5 一刀切」冲突；极弱流动性的惩罚已计入 penalty

            upper_shadow = high_price - max(now_price, open_price)
            if upper_shadow > (now_price * 0.03):
                penalty += 45.0

            if now_price < vwap * 0.985:
                penalty += 25.0

            if regime_state:
                if any(k in regime_state for k in ["退潮", "单边下行", "主跌", "空头"]):
                    if any("P3-01" in h or "P3-03" in h or "P3-04" in h or "P3-05" in h or "P3-06" in h or "P3-08" in h for h in hits):
                        penalty += 35.0
                        res["risk_tags"].append("🧯[退潮降权]进攻策略降权")
                elif any(k in regime_state for k in ["主升", "趋势", "修复"]):
                    penalty -= 8.0

            if regime_score < 0.35 and any("P3-01" in h or "P3-03" in h or "P3-04" in h or "P3-05" in h or "P3-06" in h or "P3-08" in h for h in hits):
                penalty += 18.0
                res["risk_tags"].append("🧊[环境偏弱]进攻信号折价")

            # 冲高回落强化：A股盘中最怕追在分时高点，必须把“远离VWAP + 回落 + 放量”单独拎出来
            vwap_gap_pct = ((now_price - vwap) / vwap * 100.0) if vwap > 0 else 0.0
            today_high = max(high_price, now_price)
            high_gap_pct = ((today_high - now_price) / now_price * 100.0) if now_price > 0 else 0.0
            if vwap > 0 and pct_chg > 3.0 and now_price < vwap * 0.99:
                penalty += 100.0
                res["risk_tags"].append("💀[致命]冲高回落破均价")
            if pct_chg >= 2.5 and vwap_gap_pct > 1.4 and vr >= 1.6:
                penalty += 35.0
                res["risk_tags"].append("🧨[假突破]远离VWAP追高")
            if today_high > 0 and high_gap_pct > 1.8 and now_price < today_high * 0.985 and vr >= 1.5:
                penalty += 30.0
                res["risk_tags"].append("🧨[冲高回落]高位承接不足")

            # 贴近 95% 成本峰、获利盘极低、无量 → 上方套牢抛压未解放
            winner_rt = _safe_float(
                rt.get("winner_rate"),
                _safe_float(curr.get("winner_rate"), float("nan")),
            )
            if (
                cost_95th > 0
                and 0.95 * cost_95th <= now_price <= 0.99 * cost_95th
                and np.isfinite(winner_rt)
                and winner_rt < 60.0
                and vr < 2.0
            ):
                penalty += 80.0
                res["risk_tags"].append("🧱[重压]头顶套牢盘且未爆量")

            # 早盘「死水→午后偷袭」：用 9:35/10:30/11:25 三档量比地板（有则 min，无则退回 vr_1030）
            vmf = _safe_float(rt.get("vr_morning_floor", -1.0))
            if vmf < 0.0:
                vmf = _safe_float(rt.get("vr_1030", -1.0))
            if vmf >= 0.0 and vmf < 0.8 and vr > 1.5:
                penalty += 25.0

            is_vwap_support = False
            if 1.0 <= pct_chg <= 6.0 and vr >= 1.5:
                if vwap * 1.001 <= now_price <= vwap * 1.025:
                    is_vwap_support = True
            if regime_state and any(k in regime_state for k in ["退潮", "主跌", "空头"]):
                is_vwap_support = False
                penalty += 12.0
            if regime_score >= 0.65:
                penalty *= 0.92
            elif regime_score <= 0.35:
                penalty *= 1.08

            s_momentum = self._map_momentum(pct_chg, atr_pct, vr)
            s_kill = self._map_chip_kill(now_price, cost_95th)
            net_main_amt = float(rt.get("net_main_amount", curr.get("net_main_amount", 0.0) or 0.0))
            s_support = self._map_funds(hk_vol + net_main_amt, circ_mv_yi)

            regime_boost = 1.0
            if regime_state:
                if any(k in regime_state for k in ["主升", "趋势", "修复"]):
                    regime_boost = 1.08
                elif any(k in regime_state for k in ["退潮", "主跌", "空头"]):
                    regime_boost = 0.82
                else:
                    regime_boost = 0.96
            if regime_score >= 0.7:
                regime_boost = max(regime_boost, 1.05)
            elif regime_score < 0.35:
                regime_boost = min(regime_boost, 0.88)

            # 热门板块加成：尽量不改主框架，仅对主线/前排/强板块做轻量加权
            hot_sector_bonus = 0.0
            if float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) >= 4.0:
                hot_sector_bonus = 10.0
            elif float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) >= 3.0:
                hot_sector_bonus = 7.0
            elif float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) >= 1.5:
                hot_sector_bonus = 4.0
            elif float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) <= -2.0:
                hot_sector_bonus = -8.0
            elif regime_score < 0.35:
                hot_sector_bonus = -3.0
            hot_sector_bonus = float(max(-10.0, min(12.0, hot_sector_bonus)))

            raw_burst = ((s_momentum * 0.4) + (s_kill * 0.3) + (s_support * 0.3)) * regime_boost
            raw_burst += hot_sector_bonus
            if t1_boost > 0:
                raw_burst += min(10.0, t1_boost)
            if is_vwap_support:
                raw_burst += 12.0
            if any("P3-01·★右侧起爆" in h or "★右侧起爆" in h for h in core_hits):
                raw_burst += 10.0
            if any("P3-02·★均线低吸" in h or "★均线低吸" in h for h in core_hits):
                raw_burst += 6.0
            if any("P3-04·★巨头连贯发力" in h or "★巨头连贯发力" in h for h in core_hits):
                raw_burst += 6.0
            if any("P3-08·★倍量启动延续" in h or "★倍量启动延续" in h for h in core_hits):
                raw_burst += 7.0

            # 【V26.7 午休时间清洗】10:30 第二波识别加成也使用午休清洗后的分钟数。
            # curr_min_raw 用于获取时间，curr_min_p3 用于判断窗口。
            # 注意：10:20-10:40 窗口完全在上午盘内（570-640），午休清洗对此时段结果无影响，
            # 但为保持全局一致性，任何从系统时钟计算 curr_min 的地方都统一走清洗流程。
            bj_tz_p3 = timezone(timedelta(hours=8))
            now_time_p3 = datetime.now(bj_tz_p3)
            rt_for_second_wave = {"curr_min": now_time_p3.hour * 60 + now_time_p3.minute}
            curr_min_p3 = _curr_min_lunch_cleaned(rt_for_second_wave)
            second_wave_bonus = 0.0
            if 620 <= curr_min_p3 <= 640:  # 10:20-10:40 窗口（午休清洗后仍为 620-640）
                if pct_chg > 1.5 and vr >= 1.2 and now_price > _safe_float(curr.get("ma5", 0.0)):
                    second_wave_bonus = 5.0
                    res["risk_tags"].append("🌊[第二波]10:30窗口确认")
            raw_burst += second_wave_bonus

            if is_vwap_support:
                raw_burst = max(raw_burst, 85.0)

            res["burst_score"] = round(min(raw_burst, 100.0), 2)
            res["surge_bonus"] = self._check_second_surge(df, rt) if len(df) >= 60 else 0.0
            res["surge_bonus"] = round(_safe_float(res["surge_bonus"], 0.0) + t1_boost, 2)
            res["penalty"] = round(penalty, 2)
            t1_source = rt.get("_t1_memory", {}) if isinstance(rt.get("_t1_memory", {}), dict) else {}
            if not t1_boost:
                t1_boost = _safe_float(t1_source.get("boost", 0.0), 0.0)
            if not t1_win:
                t1_win = _safe_float(t1_source.get("win_rate_t1_pct", 0.0), 0.0)
            if not t1_avg:
                t1_avg = _safe_float(t1_source.get("avg_ret_t1_pct", 0.0), 0.0)
            entry_reason = "承接好，能继续走"
            if float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) >= 3.0:
                entry_reason = "主线前排，容易走强"
            elif any("P3-02" in h or "P3-07" in h or "P3-10" in h for h in core_hits):
                entry_reason = "回踩确认，不怕假拉"
            elif float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) <= -2.0:
                entry_reason = "不像主线，防假强"
            res["detail"] = {
                "性格动量": round(s_momentum, 2),
                "绞杀分": round(s_kill, 2),
                "时权抢筹": round(s_support, 2),
                "入池理由": entry_reason,
                "市值档位": size_emoji,
                "regime_state": regime_state or "--",
                "regime_score": round(regime_score, 3),
                "sector_strength": round(float(ev.get("detail", {}).get("sector_strength", 1.0) or 1.0), 3),
                "mainline_score": round(float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0), 2),
                "mainline_reason": str(ev.get("detail", {}).get("mainline_reason", "") or ""),
                "hot_sector_bonus": round(float(hot_sector_bonus), 2),
                "t1_memory_boost": round(t1_boost, 2),
                "t1_win_rate_pct": round(t1_win, 1),
                "t1_avg_ret_pct": round(t1_avg, 3),
                "is_vwap_support": bool(is_vwap_support),
                "p3_screener": ev.get("detail", {}),
            }
            # 分层输出：主战法 / 辅助标签 / 观察项
            res["detail"]["core_hits"] = core_hits
            res["detail"]["aux_hits"] = aux_hits
            res["detail"]["obs_hits"] = obs_hits
            res["strategies"] = core_hits + aux_hits + obs_hits
            buy_hint = "盘中信号偏强，可轻仓分批，站稳VWAP再加。"
            if core_hits:
                if float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) >= 3.0:
                    buy_hint = "主线板块龙头可先轻仓跟随，回踩VWAP或分时均线确认后再加。"
                elif any("P3-02" in h or "P3-07" in h or "P3-10" in h for h in core_hits):
                    buy_hint = "偏低吸类信号，先等分时企稳不破VWAP再试仓。"
                else:
                    buy_hint = "右侧强势信号，建议分批跟随，避免一次性满仓。"
            elif aux_hits:
                buy_hint = "辅助信号为主，先观察承接，等主线确认再介入。"
            else:
                buy_hint = "信号偏弱，先观察，不追高。"
            if float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0) <= -2.0:
                buy_hint = "疑似假强板块，先观望，等板块回到主线再考虑。"
            res["buy_hint"] = buy_hint
            res["wechat_hint"] = buy_hint
            res["fake_breakout_guard"] = bool(not ev.get("veto_pass", True) and "假突破" in str(ev.get("veto_reason", "")))

            core_cnt = len(core_hits)
            aux_cnt = len(aux_hits)
            obs_cnt = len(obs_hits)
            if core_cnt == 0:
                raw_burst *= 0.85
            elif core_cnt == 1:
                raw_burst *= 1.03
            elif core_cnt >= 2:
                raw_burst *= 1.08
            if aux_cnt:
                raw_burst += min(6.0, aux_cnt * 1.5)
            if obs_cnt:
                raw_burst += min(2.0, obs_cnt * 0.3)

            # 观察项只给信息，不抢主分
            if any("缩量分歧低吸" in h for h in obs_hits):
                res["risk_tags"].append("👀【观察】缩量分歧低吸")
            if any("质量趋势底仓" in h for h in obs_hits):
                res["risk_tags"].append("👀【观察】质量趋势底仓")
            # 【V26.6 优化】盘中滞后数据警告：hk_vol / net_main_amount / inst_net_buy 均为昨日结算数据
            hk_note = ev.get("detail", {}).get("hk_vol_data_note", "")
            if hk_note and "昨" in hk_note:
                res["risk_tags"].append(f"📡[数据延迟]{hk_note}")

            # 并入第三层扣分、建议最低买入分、标签展示
            merge_risk_into_engine_result(res, risk_pre, penalty_key="penalty")
            res["detail"]["strategy_tier_counts"] = {"core": core_cnt, "aux": aux_cnt, "obs": obs_cnt}

            if len(df) >= 60 and self.db_cache:
                s_code = ""
                if "ts_code" in df.columns:
                    s_code = str(df["ts_code"].iloc[-1]).split(".")[0][:6]
                elif "code" in df.columns:
                    s_code = str(df["code"].iloc[-1]).split(".")[0][:6]
                else:
                    ts_code_rt = rt.get("ts_code") or rt.get("code", "")
                    if ts_code_rt:
                        s_code = str(ts_code_rt).split(".")[0][:6]

                # 【V26.7 新增】午休时间清洗：curr_min 直接从系统时钟获取后，
                # 必须经 _curr_min_lunch_cleaned 处理剔除 11:30-13:00 的 90 分钟休市时间。
                # 若不剔除，下午 13:00+ 的 curr_min 会虚增约 30 分钟，
                # 导致 DuckDB Z-Score 的 10:30/13:00 时段切片判断失效（早该切到 1300_mean 但实际仍落在 1030_mean）。
                # 详见 _curr_min_lunch_cleaned 函数的详细注释。
                bj_tz = timezone(timedelta(hours=8))
                now_time = datetime.now(bj_tz)
                rt_risk_lunch = dict(rt)
                rt_risk_lunch["curr_min"] = now_time.hour * 60 + now_time.minute
                curr_min = _curr_min_lunch_cleaned(rt_risk_lunch)

                if s_code and curr_min >= 630:
                    cache = self.db_cache.get(s_code)
                    vol_rt = _safe_float(rt.get("volume", 0.0))
                    if cache and vol_rt > 0:
                        if curr_min < 780:
                            mean = cache.get("1030_mean", 0.0)
                            std = cache.get("1030_std", 0.0)
                        else:
                            mean = cache.get("1300_mean", 0.0)
                            std = cache.get("1300_std", 0.0)
                        if std and std > 0:
                            z_slice = (vol_rt - mean) / std
                            is_big = circ_mv_yi >= 500.0
                            if (is_big and z_slice >= 2.7) or (not is_big and z_slice >= 3.2):
                                tag = f"🌋🌋核爆级异动(Z:{z_slice:.1f})" if z_slice >= 4.0 else f"🌋核能异动(Z:{z_slice:.1f})"
                                res["strategies"] = list(res["strategies"]) + [tag]

        except Exception as e:
            logging.debug(f"P3 run_all 异常: {e}")
            return res

        return res
