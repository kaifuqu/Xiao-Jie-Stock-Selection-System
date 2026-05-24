# -*- coding: utf-8 -*-

"""

小杰AI选股系统 Pro V26.6 - P2 竞价战法池（物理胸甲四策略版）

【架构说明】

1. 核心筛选逻辑已拆至 p2_auction_screener.py：全局一票否决 + 四大独立竞价策略（标签化）。

2. 本类负责：调用筛选器、将命中结果映射为 burst_score / surge_bonus / penalty，供 scan_engine 排序与展示。

3. 不再在引擎内重复实现 strict_golden_burst_ok；黄金门禁由 scan_engine 统一处理，

   对「四大主策略命中」的股票可通过 hit_res['p2_core_screener_pass'] 放宽（避免主线命中被误杀）。

"""



# Standard library

import logging

from datetime import datetime, timedelta, timezone

from typing import Optional



# Third-party

import pandas as pd



# Local modules

try:

    from core.strategies.fund_mv_utils import mean_effective_turnover_f_last_n

except ImportError:

    from strategies.fund_mv_utils import mean_effective_turnover_f_last_n  # type: ignore



from core.config_manager import get_p2_screener_config, get_risk_control_config

from core.strategies.p2_auction_screener import (
    _board_beta,
    _detect_limit_up,
    _p2_collect_t1_memory,
    _p2_t1_memory_score,
    evaluate_p2_screener,
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





class P2Auction:

    def __init__(self, screener_cfg=None, risk_cfg: Optional[RiskControlConfig] = None):

        """

        screener_cfg:

            可选 P2ScreenerConfig 实例；不传则从 config.yaml strategies.p2（及策略实验室覆写）加载。

            便于在回测脚本中注入不同阈值而不改全局常量。

        risk_cfg: 三层全局风控；不传则从 config.yaml 的 ``risk_control`` 节点加载（见 get_risk_control_config）。

        """

        self.name = "P2竞价引擎"

        self.version = "V26.6_PhysicalArmor_4Strategies"

        self._cfg_lock_external = screener_cfg is not None

        self._cfg = screener_cfg if screener_cfg is not None else get_p2_screener_config()

        # 外部显式传入 risk_cfg 时不再被 YAML 覆盖（便于回测注入）

        self._risk_cfg_lock_external = risk_cfg is not None

        if risk_cfg is not None:

            self._risk_cfg = risk_cfg

        else:

            try:

                self._risk_cfg = get_risk_control_config()

            except Exception as ex:

                logger.debug("风控配置从 config.yaml 加载失败，使用 DEFAULT_RISK_CONFIG: %s", ex)

                self._risk_cfg = DEFAULT_RISK_CONFIG



    def _map_vr_np(self, vr):

        import numpy as np

        xp = [0.0, 0.8, 1.2, 2.0, 4.0, 8.0]

        yp = [0.0, 20.0, 50.0, 80.0, 95.0, 100.0]

        return float(np.interp(float(vr), xp, yp))



    def _map_funds(self, amount, circ_mv_yi):

        """绝对金额 → 相对流通市值（万分之 X）再映射到 0~100 分。"""

        import numpy as np

        if circ_mv_yi <= 0:

            return 0.0

        inflow_ratio = (amount / (circ_mv_yi * 100000000.0)) * 10000.0

        xp = [0.0, 1.0, 2.5, 5.0, 8.0, 15.0]

        yp = [0.0, 30.0, 60.0, 85.0, 100.0, 100.0]

        return float(np.interp(inflow_ratio, xp, yp))



    def _map_cyq(self, cyq):

        """筹码集中度：越低越好。"""

        import numpy as np

        xp = [5.0, 10.0, 15.0, 20.0, 25.0, 30.0]

        yp = [100.0, 100.0, 70.0, 40.0, 20.0, 0.0]

        return float(np.interp(cyq, xp, yp))



    def _check_second_surge(self, df, rt):

        """二波/再启动加分项（与四策略正交，仅作小幅 surge_bonus）。"""

        try:

            import numpy as np

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

            turnover_shrink = mean_effective_turnover_f_last_n(df, 3) < 3.0 if len(df) >= 3 else False

            macd_strong = float(curr.get("macd_diff", 0.0)) > float(curr.get("macd_dea", 0.0))

            cyq_stable = float(rt.get("cyq_concentration", 99.0)) < 18.0

            matches = sum([turnover_shrink, macd_strong, cyq_stable])

            if matches == 3:

                return 8.0

            if matches == 2:

                return 5.0

            if matches == 1:

                return 2.0

            return 0.0

        except Exception as e:

            logging.debug(f"P2 _check_second_surge 异常: {e}")

            return 0.0



    def run_all(self, df, rt):

        """

        入口：与 scan_engine 约定一致，返回 dict。

        增量约定：df 为截至 T-1 的历史；rt 为当日竞价快照（open、pre_close、vol_ratio 等）。

        """

        if not self._cfg_lock_external:

            self._cfg = get_p2_screener_config()

        # 每轮扫描刷新风控开关（mtime 变化时 get_risk_control_config 内 _load_yaml_raw 会重载）

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

            "p2_core_screener_pass": False,

            "p2_veto_reason": "",

            "p2_strategy_checks": {},

            "risk_tags": [],

            "suggested_min_entry_score": 0.0,

            "buy_hint": "",

            "wechat_hint": "",

            "risk_control": {},

        }

        risk_pre = None

        if df is None or df.empty or len(df) < 20:

            return res



        try:

            open_price = float(rt.get("open", 0.0))
            y_last = df.iloc[-1]
            y_close = float(y_last.get("close", 0.0) or 0.0)
            pre_close_rt = float(rt.get("pre_close", 0.0) or 0.0)
            pre_close_y = float(y_last.get("pre_close", 0.0) or 0.0)
            if pre_close_rt > 0:
                pre_close = pre_close_rt
            elif pre_close_y > 0:
                pre_close = pre_close_y
            else:
                pre_close = y_close
            rt["_y_close"] = y_close
            # 【V26.7 修复】除权除息日容错：若 pre_close 与日线 close 差距 > 20%，标记后跳过涨跌停判定
            if y_close > 0 and pre_close > 0:
                ratio_diff = abs(pre_close - y_close) / y_close
                if ratio_diff > 0.20:
                    rt["_ex_right_adjust"] = True
            if pre_close <= 0 or open_price <= 0:
                return res



            open_pct = (open_price - pre_close) / pre_close * 100.0

            vr = float(rt.get("vol_ratio", 0.0))



            circ_mv_raw = rt.get("circ_mv")

            if pd.isna(circ_mv_raw) or circ_mv_raw is None:

                circ_mv_raw = float(df["circ_mv"].iloc[-1]) if "circ_mv" in df.columns else float(rt.get("total_mv", 10000000)) * 0.6

            circ_mv_yi = float(circ_mv_raw) / 10000.0



            rt_risk = dict(rt)

            rt_risk["price"] = float(open_price)

            rt_risk.setdefault("high", float(open_price))

            rt_risk.setdefault("low", float(open_price))

            rt_risk["_pool_key"] = "p2"

            _bj = timezone(timedelta(hours=8))

            _now = datetime.now(_bj)

            rt_risk.setdefault("curr_min", _now.hour * 60 + _now.minute)

            risk_pre = evaluate_3layer_risk(df, rt_risk, self._risk_cfg, is_right_side_strategy=False, pool_key="p2")

            # 一层硬否决仅保留「K 线为空 / 价格无效」；其余在 ui_alert_only 下改为标签预警（见 risk_control_engine）

            if not risk_pre.get("pass_layer1", False):

                res["p2_veto_reason"] = str(risk_pre.get("veto_reason", "") or "")

                res["risk_control"] = {

                    "pass_layer1": False,

                    "pass_layer2": True,

                    "veto_reason": res["p2_veto_reason"],

                    "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),

                    "risk_tags": list(risk_pre.get("risk_tags", []) or []),

                    "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),

                    "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),

                }

                res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])

                res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)

                return res



            if circ_mv_yi >= 1000.0:

                req_main, req_hk = 80000000.0, 40000000.0

            elif circ_mv_yi >= 500.0:

                req_main, req_hk = 40000000.0, 20000000.0

            elif circ_mv_yi >= 100.0:

                req_main, req_hk = 15000000.0, 8000000.0

            else:

                req_main, req_hk = 8000000.0, 4000000.0



            penalty = 0.0

            regime_bucket = "neutral"

            board_beta = 1.0

            t1_avg = 0.0

            t1_win = 0.0

            t1_n = 0.0

            if vr < 1.0:

                import numpy as np

                xp_pen = [0.0, 0.5, 0.8, 1.0]

                yp_pen = [40.0, 20.0, 8.0, 0.0]

                penalty += float(np.interp(vr, xp_pen, yp_pen))



            if open_pct < -1.0:

                import numpy as np

                xp_pct = [-10.0, -5.0, -2.0, -1.0]

                yp_pct = [50.0, 30.0, 10.0, 0.0]

                penalty += float(np.interp(open_pct, xp_pct, yp_pct))



            curr = df.iloc[-1]

            ma20 = float(curr.get("ma20", open_price))

            bias20_open = (open_price - ma20) / ma20 * 100.0 if ma20 > 0 else 0.0

            if bias20_open > 10.0:

                penalty += 15.0



            regime_bucket = str(rt.get("_regime_state", rt.get("regime", "")) or "")

            board_beta = _board_beta(rt)

            t1_avg, t1_win, t1_n = _p2_collect_t1_memory(rt)

            t1_memory_score = _p2_t1_memory_score(t1_avg, t1_win, t1_n, self._cfg)

            if any(k in regime_bucket for k in ["主升", "趋势"]):

                penalty *= self._cfg.regime_strict_boost

            elif any(k in regime_bucket for k in ["退潮", "空头", "主跌"]):

                penalty *= self._cfg.regime_relaxed_boost

            if board_beta >= self._cfg.board_beta_core_min:

                penalty *= 0.90

            elif board_beta >= self._cfg.board_beta_hot_min:

                penalty *= 0.95

            elif board_beta <= self._cfg.board_beta_cold_max:

                penalty += 12.0



            if vr < 0.4 or open_pct < -6.0:

                return res



            vwap_px = 0.0

            if float(rt.get("amount", 0.0) or 0.0) > 0 and float(rt.get("volume", 0.0) or 0.0) > 0:

                tentative = float(rt.get("amount", 0.0) or 0.0) / max(float(rt.get("volume", 0.0) or 0.0), 1e-9)

                vwap_px = tentative / 100.0 if tentative > open_price * 20 else tentative

            else:

                vwap_px = float(curr.get("vwap", open_price))

            vwap_gap_pct = ((open_price - vwap_px) / vwap_px * 100.0) if vwap_px > 0 and open_price > 0 else 0.0

            # 【V26.7 业务注释】竞价时间窗口：默认使用 09:33~09:35 盘初确认模式（而非纯 09:25 集合竞价）。
            # A股集合竞价 09:15~09:25 结束后，09:30 起进入连续交易。
            # 09:33~09:35 窗口反映盘初前3分钟分时表现：可过滤集合竞价最后一秒的虚价拉抬，
            # 捕捉资金快速拉升前的蓄力阶段（真实意愿）。这是"盘初分时确认"而非"纯竞价池"。
            # 若需严格纯竞价模式，可在 P2ScreenerConfig 中设置 open_confirm_mode="09:25纯竞价"。
            open_confirm_hit = (
                573 <= int(_now.hour * 60 + _now.minute) <= 575
                and (vwap_px <= 0 or open_price >= vwap_px * (1.0 - self._cfg.open_confirm_vwap_eps))
            )

            if vwap_gap_pct < -self._cfg.vwap_death_hard_pct:

                penalty += self._cfg.vwap_death_penalty

            elif vwap_gap_pct < self._cfg.vwap_death_gap_min_pct:

                penalty += 10.0

            if open_confirm_hit:

                penalty = max(0.0, penalty - self._cfg.open_confirm_bonus_hit)

            else:

                penalty += self._cfg.open_confirm_penalty_miss

            if board_beta <= self._cfg.board_beta_cold_max:

                penalty += 6.0

            if t1_n >= self._cfg.t1_memory_min_samples and t1_memory_score < 0:

                penalty += min(self._cfg.t1_memory_penalty_max, abs(t1_memory_score))



            cyq_conc = float(rt.get("cyq_concentration", 999.0))

            hk_vol = float(rt.get("hk_vol", 0.0))

            net_main_amount = float(rt.get("net_main_amount", y_last.get("net_main_amount", 0.0) or 0.0))

            hk_assist = hk_vol > req_hk and net_main_amount > req_main



        except Exception as e:

            logging.debug(f"P2 特征提取异常: {e}")

            return res



        # ================= 物理胸甲：全局否决 + 四大策略 =================
        # 【V26.7 修复】动态涨跌停判定：直接调用 screener 的 _detect_limit_up，
        # 按股票代码前缀动态判断涨停幅度（主板10%、科创/创业20%、北交所30%），
        # 废除 open_price >= pre_close * 1.098 硬编码。
        rt["_is_limit"] = _detect_limit_up(
            str(rt.get("ts_code", "")),
            open_price,
            pre_close,
            rt,
        )

        ev = evaluate_p2_screener(df, rt, self._cfg)

        res["p2_core_screener_pass"] = bool(ev.get("p2_core_screener_pass"))

        res["p2_veto_reason"] = str(ev.get("veto_reason", "") or "")

        res["p2_strategy_checks"] = dict(ev.get("strategy_checks") or {})



        if not ev.get("veto_pass"):

            return res



        hits = list(ev.get("strategies") or [])

        if not hits:

            return res



        if risk_pre is not None and hits_indicate_right_side_attack(hits):

            l2 = evaluate_layer2_right_side_only(df, rt_risk, self._risk_cfg)

            for uw in l2.get("ui_warnings") or []:

                res["risk_tags"].append("⚠️[二层预警]" + str(uw))

            if not l2.get("pass_layer2", True):

                res["p2_veto_reason"] = str(l2.get("veto_reason", "") or "")

                res["risk_control"] = {

                    "pass_layer1": True,

                    "pass_layer2": False,

                    "veto_reason": res["p2_veto_reason"],

                    "penalty": float(risk_pre.get("penalty", 0.0) or 0.0),

                    "risk_tags": list(risk_pre.get("risk_tags", []) or []),

                    "suggested_min_entry_score": float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0),

                    "ui_warnings": list(risk_pre.get("ui_warnings", []) or []),

                }

                res["risk_tags"] = list(risk_pre.get("risk_tags", []) or [])

                res["suggested_min_entry_score"] = float(risk_pre.get("suggested_min_entry_score", 0.0) or 0.0)

                return res



        if hk_assist:

            hits.append("✈️[外资辅助]")


        # 【V26.6 优化】hk_vol / net_main_amount / inst_net_buy 为日线结算数据，竞价显示"昨"标注
        hk_note = ev.get("detail", {}).get("hk_vol_data_note", "")
        if hk_note and "昨" in hk_note:
            res["risk_tags"].append(f"📡[数据延迟]{hk_note}")

        try:

            s_vol = self._map_vr_np(vr)

            s_fund = self._map_funds(hk_vol + net_main_amount, circ_mv_yi)

            s_chip = self._map_cyq(cyq_conc)

            raw_burst = (s_vol * 0.3) + (s_fund * 0.3) + (s_chip * 0.2) + 10.0

            bonus = float(ev.get("sort_weight_bonus", 1.0) or 1.0)

            if bonus > 1.0:

                raw_burst = min(raw_burst + (bonus - 1.0) * 25.0, 100.0)

            if t1_memory_score != 0.0:

                raw_burst = max(0.0, min(100.0, raw_burst + t1_memory_score))

            res["burst_score"] = round(min(raw_burst, 100.0), 2)

            res["surge_bonus"] = self._check_second_surge(df, rt)

            res["penalty"] = round(max(0.0, penalty), 2)

            sector_strength = float(ev.get("detail", {}).get("sector_strength", board_beta) or board_beta)
            sector_rank = int(ev.get("detail", {}).get("sector_rank", 999) or 999)
            ev_mainline_score = float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0)

            entry_reason = "主线前排，承接稳"

            if sector_rank <= 2 and sector_strength >= 1.10:

                entry_reason = "主线前排，承接稳"

            elif sector_strength <= 0.95:

                entry_reason = "板弱观察，先别追"

            elif ev_mainline_score <= -2.0:

                entry_reason = "不像主线，防假强"

            res["detail"] = {

                "爆量分": round(s_vol, 2),

                "抢筹分": round(s_fund, 2),

                "筹码分": round(s_chip, 2),

                "入池理由": entry_reason,

                "regime_bucket": regime_bucket,

                "board_beta": round(board_beta, 3),

                "sector_strength": round(sector_strength, 3),

                "sector_rank": sector_rank,

                "sector_total": int(ev.get("detail", {}).get("sector_total", 0) or 0),

                "mainline_score": round(float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0), 2),

                "mainline_reason": str(ev.get("detail", {}).get("mainline_reason", "") or ""),

                "t1_avg_ret_pct": round(t1_avg, 3),

                "t1_win_rate_pct": round(t1_win, 1),

                "t1_sample_n": int(t1_n),

                "t1_memory_score": round(t1_memory_score, 2),

                "open_confirm_hit": bool(open_confirm_hit),

                "vwap_gap_pct": round(vwap_gap_pct, 3),

                "sort_weight_bonus": bonus,

                "p2_screener": ev.get("detail", {}),

            }

            # 小范围增强：给 UI / 微信端附加「怎么买」提示，但不改变评分框架

            buy_hint = ""

            sector_rank = int(ev.get("detail", {}).get("sector_rank", 999) or 999)

            mainline_score = float(ev.get("detail", {}).get("mainline_score", 0.0) or 0.0)

            mainline_reason = str(ev.get("detail", {}).get("mainline_reason", "") or "")

            if hits:

                if open_confirm_hit and vr >= 1.2 and open_pct >= 0.5:

                    buy_hint = "竞价转强，分批小仓跟随，回踩开盘价附近再加"

                elif open_pct >= 1.0 and vwap_gap_pct >= -0.3 and len(hits) >= 2:

                    buy_hint = "开盘站稳VWAP可试仓，回踩不破再加"

                elif len(hits) >= 2 and board_beta >= self._cfg.board_beta_hot_min:

                    buy_hint = "强板块强信号，开盘后等回踩分时均线确认再上"

                else:

                    buy_hint = "先观察分时承接，确认不破VWAP后再轻仓"

            else:

                buy_hint = "信号偏弱，建议等盘中放量站稳再考虑"

            if mainline_score >= 3.0:

                if sector_rank <= 2:

                    buy_hint = "主线板块龙头优先，竞价后可先轻仓跟随，回踩VWAP或开盘价附近确认承接后再加。"

                else:

                    buy_hint = "主线板块偏强，先轻仓跟随，等分时确认承接后再加。"

            elif mainline_score <= -2.0:

                buy_hint = "疑似假强板块，先观望不追高，等板块回到主线再考虑。"

            elif sector_strength <= 0.95:

                buy_hint = "板块偏弱，先观察不追高，等分时站稳VWAP和板块转强再考虑。"

            res["buy_hint"] = buy_hint

            res["wechat_hint"] = buy_hint

            res["strategies"] = hits

            if risk_pre is not None:

                merge_risk_into_engine_result(res, risk_pre, penalty_key="penalty")

        except Exception as e:

            logging.debug(f"P2 打分结算异常: {e}")

            res["strategies"] = hits

            res["burst_score"] = 72.0

            if risk_pre is not None:

                merge_risk_into_engine_result(res, risk_pre, penalty_key="penalty")



        return res

