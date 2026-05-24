# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 - 金·共振真龙验尸官（分池优化版）
【说明】：
1. 🎯 外资降权重估：外资仅作辅助参考，不再压过主力净流入与结构确认。
2. ⚖️ 阶梯市值量纲：根据 🦍/🐘/🐎/🐥 体型应用动态资金门槛。
3. 分离 p2/p3/p4/p5 的职责边界，减少统一硬门槛误杀。
"""
# Standard library
import logging
from datetime import datetime, timezone, timedelta

# Third-party
import numpy as np
import pandas as pd

# Local modules
from core.strategies.strat_base import (
    _p1_min_circ_mv_yi_strat,
    _resolve_pre_close_rt_y,
    strict_golden_burst_ok,
    strict_pullback_shrink_ok,
)
try:
    from core.strategies.fund_mv_utils import effective_turnover_rate_f, golden_tier_elg_hk_punish_wan
except ImportError:
    from strategies.fund_mv_utils import effective_turnover_rate_f, golden_tier_elg_hk_punish_wan  # type: ignore

class GoldenTenStrategies:
    def __init__(self):
        self.base_bonus = 0.0

    def _init_context(self, df, rt, pool_key, ind_rank, hk_vol, cyq):
        curr = df.iloc[-1]
        now_price = float(rt.get('price', curr.get('close', 0.0)))
        pre_close = float(_resolve_pre_close_rt_y(rt, curr))
        today_open = float(rt.get('open', curr.get('open', now_price)))
        today_high = float(rt.get('high', curr.get('high', now_price)))
        pct = (now_price - pre_close) / pre_close * 100.0 if pre_close > 0 else 0.0
        vr = float(rt.get('vol_ratio', 1.0))
        trn = float(effective_turnover_rate_f(rt, curr, now_price))
        vol = float(rt.get('volume', df['vol'].iloc[-1] if 'vol' in df.columns else 0.0))
        vol_series = df['vol'] if 'vol' in df.columns else pd.Series([vol] * len(df), index=df.index)
        pct_chg_series = df['pct_chg'] if 'pct_chg' in df.columns else pd.Series([0.0] * len(df), index=df.index)
        if "vol_ma5" in df.columns:
            vma5_series = df["vol_ma5"]
        elif "vma5" in df.columns:
            vma5_series = df["vma5"]
        else:
            vma5_series = pd.Series([vol] * len(df), index=df.index)
        ma5 = float(curr.get('ma5', now_price))
        ma20 = float(curr.get('ma20', now_price))
        ma60 = float(curr.get('ma60', now_price))
        vma5 = float(curr.get("vol_ma5", curr.get("vma5", vol)))
        cost_95th = float(rt.get('cost_95th', curr.get('cost_95th', 0.0)))
        circ_mv_wan = float(df['circ_mv'].iloc[-1]) if 'circ_mv' in df.columns else float(rt.get('circ_mv', rt.get('total_mv', 10000000) * 0.6))
        circ_mv_yi = circ_mv_wan / 10000.0
        req_elg, req_hk, punish_elg = golden_tier_elg_hk_punish_wan(circ_mv_yi)
        hk_vol_wan = hk_vol / 10000.0
        net_main_wan = float(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)) or 0.0) / 10000.0
        req_hk_assist = req_hk * 0.90 if circ_mv_yi >= 500.0 else req_hk
        req_hk_assist = max(req_hk_assist, req_hk * 0.75)
        bj_tz = timezone(timedelta(hours=8))
        now_dt = datetime.now(bj_tz)
        now_min = now_dt.hour * 60 + now_dt.minute
        is_p2_pool = (pool_key == 'p2')
        is_p5_mode = (pool_key == 'p5') or (now_min >= 901)
        is_intraday = now_min < 900
        burst_ok = strict_golden_burst_ok(df, rt, pool_key)
        size_emoji = "🐥"
        if circ_mv_yi >= 2000.0:
            size_emoji = "🦍"
        elif circ_mv_yi >= 1000.0:
            size_emoji = "🐘+"
        elif circ_mv_yi >= 500.0:
            size_emoji = "🐘"
        elif circ_mv_yi >= 100.0:
            size_emoji = "🐎"

        return {
            "df": df,
            "curr": curr,
            "now_price": now_price,
            "pre_close": pre_close,
            "today_open": today_open,
            "today_high": today_high,
            "pct": pct,
            "vr": vr,
            "trn": trn,
            "vol": vol,
            "vol_series": vol_series,
            "pct_chg_series": pct_chg_series,
            "vma5_series": vma5_series,
            "ma5": ma5,
            "ma20": ma20,
            "ma60": ma60,
            "vma5": vma5,
            "cost_95th": cost_95th,
            "circ_mv_yi": circ_mv_yi,
            "req_elg": req_elg,
            "req_hk": req_hk,
            "punish_elg": punish_elg,
            "hk_vol_wan": hk_vol_wan,
            "net_main_wan": net_main_wan,
            "req_hk_assist": req_hk_assist,
            "is_p2_pool": is_p2_pool,
            "is_p5_mode": is_p5_mode,
            "is_intraday": is_intraday,
            "burst_ok": burst_ok,
            "size_emoji": size_emoji,
        }

    def _apply_common_gates(self, ctx, hits, max_burst, total_bonus, pool_key, ind_rank):
        if ctx["pre_close"] <= 0 or ctx["now_price"] <= 0:
            return True, hits, max_burst, total_bonus
        if pool_key in ['p3', 'p4', 'p5'] and not ctx["burst_ok"]:
            return True, hits, max_burst, total_bonus
        if ctx["is_p2_pool"] and not ctx["burst_ok"]:
            return True, hits, max_burst, total_bonus
        if ctx["circ_mv_yi"] < _p1_min_circ_mv_yi_strat():
            return True, [], 0.0, 0.0
        if ind_rank > 12 and ctx["vr"] < 2.0:
            return True, hits, max_burst, total_bonus
        if ind_rank <= 8:
            total_bonus += 2.0
        return False, hits, max_burst, total_bonus

    def _eval_p2(self, ctx, hits, max_burst, total_bonus):
        return hits, max_burst, total_bonus

    def _eval_p3(self, ctx, hits, max_burst, total_bonus):
        if ctx["score_t0"] >= 1 or ctx["score_t1"] >= 1:
            return self._eval_short_patterns(ctx, hits, max_burst, total_bonus, intraday_only=False)
        return hits, max_burst, total_bonus

    def _eval_p4(self, ctx, hits, max_burst, total_bonus):
        if ctx["score_t0"] >= 1:
            return self._eval_short_patterns(ctx, hits, max_burst, total_bonus, intraday_only=True)
        return hits, max_burst, total_bonus

    def _eval_p5(self, ctx, hits, max_burst, total_bonus):
        return self._eval_short_patterns(ctx, hits, max_burst, total_bonus, intraday_only=ctx["is_intraday"])

    def _eval_short_patterns(self, ctx, hits, max_burst, total_bonus, intraday_only=True):
        pct = ctx["pct"]
        vr = ctx["vr"]
        trn = ctx["trn"]
        ma5 = ctx["ma5"]
        ma20 = ctx["ma20"]
        ma60 = ctx["ma60"]
        now_price = ctx["now_price"]
        if intraday_only:
            short_ok = 1.5 <= pct <= 8.0 and vr >= 1.2 and trn >= 2.0 and now_price > ma5
        else:
            short_ok = -1.5 <= pct <= 6.0 and vr >= 1.0 and trn >= 1.8 and now_price >= ma20 * 0.98
        if short_ok and (ma5 >= ma20 * 0.98 or now_price > ma5):
            hits.append("👑⚡[金·短共振] 机构共振潜伏")
            max_burst = max(max_burst, 95.0)
        return self._eval_after_short(ctx, hits, max_burst, total_bonus)

    def _eval_after_short(self, ctx, hits, max_burst, total_bonus):
        if ctx["is_p5_mode"] and len(hits) > 0:
            if ctx["net_main_wan"] > ctx["req_elg"]:
                hits.append("👑👁️[上帝视角] 主力真金盖章")
                max_burst = max(max_burst, 97.0)
                total_bonus += 3.0
            elif ctx["net_main_wan"] < ctx["punish_elg"]:
                max_burst = 0.0
                hits = ["🩸[虚假共振] 主力出货诱多"]
            else:
                total_bonus += 0.5
                if max_burst < 90.0:
                    max_burst = 90.0
            if ctx["hk_vol_wan"] > ctx["req_hk_assist"] and ctx["net_main_wan"] > ctx["req_elg"] and max_burst > 0:
                hits.append("✈️[外资辅助] 北向认同跟随")
                total_bonus += 0.5
            if ctx["circ_mv_yi"] >= 100.0:
                hits.append(f"📏[市值档位]{ctx['size_emoji']}")

        vol_5d_avg = ctx["vol_series"].tail(5).mean()
        if ctx["vol_series"].iloc[-2] < vol_5d_avg * 0.65 and ctx["vol_series"].iloc[-3] < vol_5d_avg * 0.65:
            if ctx["pct_chg_series"].tail(5).max() >= 4.5:
                if ctx["now_price"] >= ctx["ma20"] * 0.985 and -0.5 <= ctx["pct"] <= 3.5 and strict_pullback_shrink_ok(ctx["vr"]):
                    hits.append("👑⚡[金·短共振] 突破极限回踩")
                    max_burst = max(max_burst, 95.0)

        return self._eval_pullback_and_bwave(ctx, hits, max_burst, total_bonus)

    def _eval_pullback_and_bwave(self, ctx, hits, max_burst, total_bonus):
        df = ctx["df"]
        pct = ctx["pct"]
        vr = ctx["vr"]
        trn = ctx["trn"]
        ma5 = ctx["ma5"]
        ma20 = ctx["ma20"]
        ma60 = ctx["ma60"]
        now_price = ctx["now_price"]
        vol_series = ctx["vol_series"]
        pct_chg_series = ctx["pct_chg_series"]
        vma5_series = ctx["vma5_series"]
        
        # p4/p5 的波段挂载逻辑：优先承接回踩、平台、结构再启动，而不是只看单日拉升
        pullback_to_ma5 = now_price >= ma5 * 0.995 and abs(now_price - ma5) / max(ma5, 1.0) <= 0.02
        pullback_to_ma20 = now_price >= ma20 * 0.985 and abs(now_price - ma20) / max(ma20, 1.0) <= 0.03
        trend_stack_ok = ma5 >= ma20 * 0.98 and ma20 >= ma60 * 0.98
        
        # 1) 趋势核心回踩：适合波段承接
        if trend_stack_ok and (pullback_to_ma5 or pullback_to_ma20):
            if -3.0 <= pct <= 4.0 and vr >= 1.0 and trn >= 1.6 and strict_pullback_shrink_ok(vr):
                hits.append("👑🛡️[金·月共振] 趋势核心回踩")
                max_burst = max(max_burst, 95.0)
                total_bonus += 0.8

        # 2) 平台放量突破：要求近20日有平台蓄势
        high_20 = df["high"].tail(20).max() if "high" in df.columns else now_price
        low_20 = df["low"].tail(20).min() if "low" in df.columns else now_price
        if low_20 > 0 and (high_20 - low_20) / low_20 <= 0.18:
            if now_price >= high_20 * 1.001 and vr >= 1.5 and trn >= 2.0 and pct >= 2.5 and now_price >= ma60 * 0.995:
                hits.append("👑🛡️[金·月共振] 平台放量突破")
                max_burst = max(max_burst, 95.0)
                total_bonus += 0.6

        # 3) 机构温和启动：更适合盘后筛选和次日接力
        ma20_5d_ago = float(df['ma20'].iloc[-5]) if 'ma20' in df.columns and len(df) >= 5 else 0.0
        ma20_10d_ago = float(df['ma20'].iloc[-10]) if 'ma20' in df.columns and len(df) >= 10 else 0.0
        if ma20 > ma20_5d_ago > ma20_10d_ago:
            if 0.5 <= pct <= 4.5 and 1.0 <= vr <= 2.2 and now_price >= ma20:
                hits.append("👑🛡️[金·月共振] 机构温和启动")
                max_burst = max(max_burst, 95.0)
                total_bonus += 0.5

        # 4) 强势龙头首阴：用于龙头回撤后的承接
        if pct_chg_series.tail(5).max() > 7.0 and pct_chg_series.iloc[-1] <= -2.5:
            open_price = float(ctx["today_open"])
            if pct > -1.0 and now_price >= open_price and vr >= 1.4:
                hits.append("👑🛡️[金·月共振] 强势龙头首阴")
                max_burst = max(max_burst, 95.0)
                total_bonus += 0.4

        # 5) 60 日龙抬头：作为 p5 波段复核的趋势延伸锚点
        ma60_10d_ago = float(df['ma60'].iloc[-10]) if 'ma60' in df.columns and len(df) >= 10 else 0.0
        ma60_20d_ago = float(df['ma60'].iloc[-20]) if 'ma60' in df.columns and len(df) >= 20 else 0.0
        if ma60_20d_ago >= ma60_10d_ago and ma60 > ma60_10d_ago * 1.005:
            if now_price > ma60 and pct > 1.5 and vr >= 1.4 and trn >= 1.8:
                if sum(df['close'].tail(3) > df['ma60'].tail(3)) >= 2:
                    hits.append("👑🛡️[金·月共振] 60日龙抬头")
                    max_burst = max(max_burst, 95.0)
                    total_bonus += 0.5

        return hits, max_burst, total_bonus

    def evaluate(self, df, rt, pool_key, ind_rank=999, hk_vol=0.0, net_elg=0.0, cyq=999.0):
        # net_elg 参数已弃用：资金口径统一为日线/快照 net_main_amount（元）
        hits = []
        max_burst = 0.0
        total_bonus = 0.0

        if df is None or len(df) < 30 or not rt:
            return hits, max_burst, total_bonus

        try:
            ctx = self._init_context(df, rt, pool_key, ind_rank, hk_vol, cyq)
            stop, hits, max_burst, total_bonus = self._apply_common_gates(ctx, hits, max_burst, total_bonus, pool_key, ind_rank)
            if stop:
                return hits, max_burst, total_bonus

            body = max(abs(ctx["now_price"] - ctx["today_open"]), ctx["now_price"] * 0.008)
            upper_shadow = ctx["today_high"] - max(ctx["now_price"], ctx["today_open"])
            shadow_limit = 0.70 if pool_key in ['p3', 'p4'] else 0.55
            if upper_shadow > body * shadow_limit:
                return hits, max_burst, total_bonus

            if ctx["cost_95th"] > ctx["now_price"] and (ctx["cost_95th"] - ctx["now_price"]) / ctx["now_price"] < 0.05:
                return hits, max_burst, total_bonus

            ctx["score_t1"] = 0.0
            if ctx["net_main_wan"] > ctx["req_elg"]:
                ctx["score_t1"] += 1.5
            if 0 < cyq < 18.0:
                ctx["score_t1"] += 1.0
            if ctx["hk_vol_wan"] > ctx["req_hk_assist"] and ctx["net_main_wan"] > ctx["req_elg"]:
                ctx["score_t1"] += 0.5

            ctx["score_t0"] = 0
            if ctx["vr"] > 1.5 and ctx["trn"] >= 2.5:
                ctx["score_t0"] += 1
            if ctx["pct"] >= 2.0 and ctx["now_price"] > ctx["today_open"]:
                ctx["score_t0"] += 1
            if ctx["now_price"] >= ctx["ma5"] and ctx["ma5"] >= ctx["ma20"] * 0.98:
                ctx["score_t0"] += 1

            if pool_key in ['p3', 'p4'] and ctx["score_t0"] < 1:
                if not hits:
                    return hits, max_burst, total_bonus
            if pool_key == 'p5' and (ctx["score_t1"] + ctx["score_t0"]) < 2:
                if not hits:
                    return hits, max_burst, total_bonus
            if ctx["is_p2_pool"] and ctx["score_t0"] < 1 and ctx["score_t1"] < 1:
                return hits, max_burst, total_bonus

            if pool_key == 'p2':
                return self._eval_p2(ctx, hits, max_burst, total_bonus)
            if pool_key == 'p3':
                return self._eval_p3(ctx, hits, max_burst, total_bonus)
            if pool_key == 'p4':
                return self._eval_p4(ctx, hits, max_burst, total_bonus)
            if pool_key == 'p5':
                return self._eval_p5(ctx, hits, max_burst, total_bonus)
            return self._eval_p3(ctx, hits, max_burst, total_bonus)

        except Exception as e:
            logging.debug(f"金·共振评估异常: {e}")

        return hits, max_burst, total_bonus
