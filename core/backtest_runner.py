# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 全市场秒级回测验证引擎（接口智能感知版）
【终极修复】
1. 智能接口适配：修复了部分引擎没有 run_all 方法导致的静默崩溃，加入 hasattr 智能感知。
2. 引擎补全：强力注入 GoldenTenStrategies (金·共振引擎)，彻底修复共振战法无法回测的 Bug。
3. 动态路由：将带有“共振”关键字的战法精准路由给 P4/P5 引擎。
4. 换手率防爆：将 circ_mv 和 total_mv 注入 mock_rt 盘口，防止回测过程中触发 3000% 畸形换手率拦截。
"""
import argparse
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import platform
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from typing import Any, Dict, List, Optional, Set

# 🚀 引入基建
import constants
from data.db_core import get_all_stock_codes, get_stock_data_qfq

from core.backtest_context import reset_backtest_context, set_backtest_legacy_mode
from core.backtest_painpoint_config import (
    CIRC_MV_WAN_MAX,
    CIRC_MV_WAN_MIN,
    PainpointWindow,
    circ_mv_in_horse_elephant_band,
    parse_pain_period,
)
from core.danger_signal_utils import size_emoji_from_circ_mv_wan, would_trigger_danger_sell

# ==================== 动态导入所有战法引擎 ====================
try:
    from core.strategies.fund_mv_utils import infer_turnover_rate_f_pct
except ImportError:
    def infer_turnover_rate_f_pct(vol_hand, close, circ_mv_wan):
        if vol_hand <= 0 or close <= 0 or circ_mv_wan <= 0:
            return 0.0
        return vol_hand * close / circ_mv_wan

try:
    from core.strategies.strat_p2_auction import P2Auction
    from core.strategies.strat_p3_intraday import P3Intraday
    from core.strategies.strat_p4_tail import P4Tail
    from core.strategies.strat_p5_postmarket import P5Postmarket
    from core.strategies.strat_golden_10 import GoldenTenStrategies
except ImportError as e:
    logging.error(f"⚠️ 战法引擎缺失: {e}。请确保 strategies 目录下文件完整。")
    class DummyEngine:
        def run_all(self, df, rt): return []
        def evaluate(self, df, rt, *args, **kwargs): return [], 0.0, 0.0
    P2Auction = P3Intraday = P4Tail = P5Postmarket = GoldenTenStrategies = DummyEngine

# 跨平台字体适配
system_name = platform.system()
if system_name == "Windows": plt.rcParams['font.sans-serif'] = ['SimHei']
elif system_name == "Darwin": plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
else: plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 实例化所有引擎
p2_engine = P2Auction()
p3_engine = P3Intraday()
p4_engine = P4Tail()
p5_engine = P5Postmarket()
golden_engine = GoldenTenStrategies()

def calculate_mdd(returns_array):
    """【纯 Numpy 极速计算】最大回撤"""
    if len(returns_array) == 0:
        return 0.0
    cum_ret = np.cumprod(1 + returns_array / 100.0)
    peak = np.maximum.accumulate(cum_ret)
    # 极端连亏导致 cum_ret→0 时 peak 可能为 0，须避免除零与 nan 污染整条回测链路
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdown = np.where(peak > 1e-15, (cum_ret - peak) / peak, 0.0)
    return float(abs(np.min(drawdown)) * 100)

def run_strategy_backtest(ts_code, strategy_key, generate_plot=True):
    """单只大盘股的战法回测逻辑 (极致性能优化版)"""
    # 动态选择引擎（加入“共振”关键词路由）
    if any(x in strategy_key for x in ["竞价", "拍卖", "P2", "01_一进二", "黄金缺口"]):
        engine = p2_engine
        engine_name = "P2竞价"
    elif any(x in strategy_key for x in ["P5", "真龙", "盘后"]):
        engine = p5_engine
        engine_name = "P5盘后"
    elif any(x in strategy_key for x in ["尾盘", "收盘", "P4", "82_", "83_", "84_", "共振"]):
        engine = p4_engine
        engine_name = "P4尾盘"
    else:
        engine = p3_engine
        engine_name = "P3盘中"

    df = get_stock_data_qfq(ts_code, limit=400) 
    warmup_period = 130 
    
    if df is None or df.empty or len(df) < warmup_period + 10: 
        return None, f"历史数据不足 (至少需 {warmup_period+10} 天)"
        
    signals, dates = [], []
    
    # 与 scan_engine / 落库口径一致：主列为 vol_ma5，勿仅用 'vma' in name（会漏匹配 vol_ma5）
    if "vol_ma5" in df.columns:
        vma5_col = "vol_ma5"
    elif "vma5" in df.columns:
        vma5_col = "vma5"
    elif "vol" in df.columns:
        vma5_col = "vol"
    elif "volume" in df.columns:
        vma5_col = "volume"
    else:
        return None, "回测数据缺少成交量列 vol/volume/vol_ma5，无法计算量比"

    vol_col = 'vol' if 'vol' in df.columns else 'volume'
    
    df['vol_ma5_shifted'] = df[vma5_col].shift(1).fillna(0)
    df['vr_sim'] = np.where(df['vol_ma5_shifted'] > 0, df[vol_col] / df['vol_ma5_shifted'], 0.0)
    vr_sim_values = df['vr_sim'].values
    
    # 【性能优化 V2】预计算辅助列，避免 to_dict('records') 内存拷贝
    # 原逻辑：df.to_dict('records') 将整表转成 ~400 个 dict 对象，内存拷贝成本高且每次访问需 dict 查找。
    # 新逻辑：直接在 DataFrame 列上做向量化读取 + iloc 取单行（零拷贝），.get() 方法兼容 Series 缺失键返回 NaN。
    # 预计算常用列的 values 数组，避免循环内重复列访问。
    total_mv_arr = pd.to_numeric(df['total_mv'], errors='coerce').fillna(0).values
    close_arr_df = pd.to_numeric(df['close'], errors='coerce').fillna(0).values
    open_arr_df = pd.to_numeric(df['open'], errors='coerce').fillna(0).values
    high_arr_df = pd.to_numeric(df['high'], errors='coerce').fillna(0).values
    low_arr_df = pd.to_numeric(df['low'], errors='coerce').fillna(0).values
    pre_close_arr = pd.to_numeric(df['pre_close'], errors='coerce').fillna(0).values
    amount_arr = pd.to_numeric(df['amount'], errors='coerce').fillna(0).values
    vol_arr = pd.to_numeric(df[vol_col], errors='coerce').fillna(0).values
    circ_mv_arr = pd.to_numeric(df.get('circ_mv', df['total_mv']), errors='coerce').fillna(10000000).values
    trf_arr = pd.to_numeric(df.get('turnover_rate_f', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    up_limit_arr = pd.to_numeric(df.get('up_limit', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    net_main_arr = pd.to_numeric(df.get('net_main_amount', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    net_elg_arr = pd.to_numeric(df.get('net_elg_amount', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    cyq_arr = pd.to_numeric(df.get('cyq_concentration', pd.Series(999, index=df.index)), errors='coerce').fillna(999).values
    cost50_arr = pd.to_numeric(df.get('cost_50th', close_arr_df), errors='coerce').fillna(0).values
    hk_vol_arr = pd.to_numeric(df.get('hk_vol', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    rz_arr = pd.to_numeric(df.get('rz_net_buy', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    wr_arr = pd.to_numeric(df.get('winner_rate', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    cost5_arr = pd.to_numeric(df.get('cost_5th', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    cost95_arr = pd.to_numeric(df.get('cost_95th', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    avg_cost_arr = pd.to_numeric(df.get('avg_cost', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    limit_times_arr = pd.to_numeric(df.get('limit_times', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    forecast_arr = pd.to_numeric(df.get('forecast_type', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
    td_arr = df['trade_date'].values  # 直接引用，避免循环内每次 .iloc[] 取日期
    # 辅助函数：安全取行（处理缺失列）
    def _safe_val(series_arr, idx, default=0.0):
        v = series_arr[idx] if idx < len(series_arr) else default
        return float(v) if (v == v) else default  # NaN check

    for i in range(warmup_period, len(df)):
        row = df.iloc[i]  # 直接引用 DataFrame 行（零拷贝）

        if total_mv_arr[i] < constants.MIN_STRAT_MV:
            continue

        vol_raw = vol_arr[i]
        circ_wan = circ_mv_arr[i]
        close_px = close_arr_df[i]
        trf = trf_arr[i]
        if trf <= 0 and vol_raw > 0 and close_px > 0 and circ_wan > 0:
            trf = infer_turnover_rate_f_pct(vol_raw / 100.0, close_px, circ_wan)

        mock_rt = {
            'price': close_arr_df[i], 'open': open_arr_df[i],
            'high': high_arr_df[i], 'low': low_arr_df[i],
            'pre_close': pre_close_arr[i], 'volume': vol_raw,
            'amount': amount_arr[i], 'vol_ratio': vr_sim_values[i],
            'turnover_rate_f': trf,
            'up_limit': up_limit_arr[i],
            'net_main_amount': net_main_arr[i],
            'net_elg_amount': net_elg_arr[i],
            'cyq_concentration': cyq_arr[i],
            'cost_50th': cost50_arr[i],
            'hk_vol': hk_vol_arr[i],
            'rz_net_buy': rz_arr[i],
            'winner_rate': wr_arr[i],
            'cost_5th': cost5_arr[i],
            'cost_95th': cost95_arr[i],
            'avg_cost': avg_cost_arr[i],
            'limit_times': limit_times_arr[i],
            'forecast_type': forecast_arr[i],
            'circ_mv': circ_mv_arr[i],
            'total_mv': total_mv_arr[i],
            'is_backtest': True
        }

        sub_df = df.iloc[:i+1]

        try:
            hits = []
            # 1. 智能路由运行常规主引擎
            res = {}
            if hasattr(engine, 'run_all'):
                res = engine.run_all(sub_df, mock_rt)
            elif hasattr(engine, 'evaluate'):
                res = engine.evaluate(sub_df, mock_rt)

            if isinstance(res, dict): hits.extend(res.get('strategies', []))
            elif isinstance(res, list): hits.extend(res)

            # 2. 强力运行 GoldenTen (共振引擎)
            ind_rank = 1
            hk_vol_val = mock_rt.get('hk_vol', 0)
            net_elg_val = mock_rt.get('net_elg_amount', 0)
            cyq_val = mock_rt.get('cyq_concentration', 999)

            gold_hits, _, _ = golden_engine.evaluate(sub_df, mock_rt, pool_key='p4', ind_rank=ind_rank, hk_vol=hk_vol_val, net_elg=net_elg_val, cyq=cyq_val)
            if gold_hits: hits.extend(gold_hits)

            # 3. 匹配信号
            if any(strategy_key in hit for hit in hits):
                signals.append(i)
                dates.append(td_arr[i])
        except Exception as e:
            logging.error(f'回测推演异常 {ts_code} [{engine_name}]: {traceback.format_exc()}')

    if not signals: return None, "未触发该战法信号"

    hold_days = 5
    buy_delay = 0 if any(k in strategy_key for k in ["01_一进二", "02_黄金缺口", "29_量比异动"]) else 1

    sig_arr = np.array(signals)
    buy_idx = sig_arr + buy_delay
    sell_idx = sig_arr + hold_days
    
    valid_mask = (buy_idx < len(df)) & (sell_idx < len(df))
    valid_buy = buy_idx[valid_mask]
    valid_sell = sell_idx[valid_mask]
    total_trades = len(valid_buy)
    
    if total_trades == 0:
        return None, "触发信号后持仓天数不足，无法计算盈亏"
        
    open_arr = df['open'].values
    close_arr = df['close'].values
    
    buy_prices = open_arr[valid_buy] * (1 + 0.002 + 0.00025)
    sell_prices = close_arr[valid_sell] * (1 - 0.002 - 0.00025 - 0.0005)
    
    returns_array = np.where(buy_prices > 0, (sell_prices - buy_prices) / buy_prices * 100, 0)
    
    win_mask = returns_array > 0
    loss_mask = returns_array <= 0
    
    win_rate = (np.sum(win_mask) / total_trades) * 100
    avg_win = np.mean(returns_array[win_mask]) if np.any(win_mask) else 0
    avg_loss = np.mean(returns_array[loss_mask]) if np.any(loss_mask) else 0
    pnl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else (99.9 if avg_win > 0 else 0)
    mdd = calculate_mdd(returns_array)

    fig = None
    if generate_plot:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(df['trade_date'], df['close'], label='股价', color='#333333', linewidth=1)
        ax.scatter(df.iloc[signals]['trade_date'], df.iloc[signals]['close'], color='red', marker='^', s=100, label='战法触发', zorder=5)
        title_str = f"{ts_code} - {strategy_key} ({engine_name}) (去水胜率: {win_rate:.1f}%, 盈亏比: {pnl_ratio:.2f})"
        ax.set_title(title_str)
        ax.legend(); plt.xticks(rotation=45); plt.tight_layout()
        
    return {"metrics": {"win_rate": win_rate, "pnl_ratio": pnl_ratio, "mdd": mdd, "total": total_trades}, "fig": fig}, "OK"

def run_batch_backtest(codes, strategy_key, progress_callback=None):
    """对底仓标的进行批量并行推演"""
    results = []
    
    test_codes = codes 
    total_codes = len(test_codes)
    completed = 0
    
    if progress_callback: progress_callback(0.0)
    
    with ThreadPoolExecutor(max_workers=constants.MAX_WORKERS) as executor:
        future_to_code = {executor.submit(run_strategy_backtest, code, strategy_key, False): code for code in test_codes}
        
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            completed += 1
            if progress_callback: progress_callback(completed / total_codes)
            
            try:
                res, msg = future.result()
                if res: 
                    results.append({
                        "代码": code, 
                        "触发次数": res['metrics']['total'], 
                        "去水胜率%": round(res['metrics']['win_rate'], 1), 
                        "真实盈亏比": round(res['metrics']['pnl_ratio'], 2),
                        "最大回撤%": round(res['metrics']['mdd'], 1)
                    })
                elif "🚫" in str(msg):
                    logging.warning(msg)
            except Exception as e:
                logging.error(f"批量回测异常 {code}: {str(e)}")
            
    if progress_callback: progress_callback(1.0)
    
    return pd.DataFrame(results) if results else pd.DataFrame(columns=["代码", "触发次数", "去水胜率%", "真实盈亏比", "最大回撤%"])


def _vol_z_at_index(df: pd.DataFrame, pos: int, vol_col: str) -> float:
    """与 scan_engine 类似的当日量相对近窗 Z 分数，供 danger_sell 止损分支使用。"""
    try:
        lo = max(0, pos - 59)
        tail = df.iloc[lo : pos + 1]
        if vol_col not in tail.columns:
            return 0.0
        s = pd.to_numeric(tail[vol_col], errors="coerce")
        if len(s) < 5:
            return 0.0
        vm = float(s.mean())
        vs = float(s.std())
        if not np.isfinite(vs) or vs <= 0:
            return 0.0
        cur = float(pd.to_numeric(df.iloc[pos][vol_col], errors="coerce") or 0.0)
        return float((cur - vm) / vs)
    except Exception:
        return 0.0


def _build_mock_rt_painpoint_row(row: pd.Series, vr_val: float, vol_col: str) -> Dict[str, Any]:
    vol_raw = float(row.get(vol_col, row.get("vol", 0)) or 0)
    circ_wan = float(row.get("circ_mv", row.get("total_mv", 10000000)) or 0)
    close_px = float(row.get("close") or 0)
    trf = float(row.get("turnover_rate_f") or 0)
    if trf <= 0 and vol_raw > 0 and close_px > 0 and circ_wan > 0:
        trf = infer_turnover_rate_f_pct(vol_raw / 100.0, close_px, circ_wan)
    return {
        "price": row["close"],
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "pre_close": row["pre_close"],
        "volume": vol_raw,
        "amount": row.get("amount", 0),
        "vol_ratio": vr_val,
        "turnover_rate_f": trf,
        "up_limit": row.get("up_limit", 0),
        "net_main_amount": row.get("net_main_amount", 0),
        "net_elg_amount": row.get("net_elg_amount", 0),
        "cyq_concentration": row.get("cyq_concentration", 999),
        "cost_50th": row.get("cost_50th", row["close"]),
        "hk_vol": row.get("hk_vol", 0),
        "rz_net_buy": row.get("rz_net_buy", 0),
        "winner_rate": row.get("winner_rate", 0),
        "cost_5th": row.get("cost_5th", 0),
        "cost_95th": row.get("cost_95th", 0),
        "avg_cost": row.get("avg_cost", 0),
        "limit_times": row.get("limit_times", 0),
        "forecast_type": row.get("forecast_type", 0),
        "circ_mv": circ_wan,
        "total_mv": row.get("total_mv", 10000000),
        "is_backtest": True,
    }


def _any_engine_signal_painpoint(sub_df: pd.DataFrame, mock_rt: Dict[str, Any]) -> bool:
    """P2–P5 任一引擎出非空 strategies 即视为「上车」信号。"""
    for eng in (p2_engine, p3_engine, p4_engine, p5_engine):
        try:
            if hasattr(eng, "run_all"):
                res = eng.run_all(sub_df, mock_rt)
                if isinstance(res, dict):
                    st = res.get("strategies")
                    if isinstance(st, list) and len(st) > 0:
                        return True
        except Exception as ex:
            logging.debug("painpoint run_all 跳过 %s: %s", getattr(eng, "name", eng), ex)
    return False


def run_painpoint_backtest(
    period: str,
    legacy_mode: bool = False,
    max_codes: Optional[int] = None,
) -> Dict[str, Any]:
    """
    痛点时段专项回测。

    统计口径：
    - 标的池：痛点窗口「最后一个交易日」流通市值落在 [100,500] 亿元（万元见 backtest_painpoint_config）。
    - 命中率：上述标的中，窗口内任一日触发 P2–P5 任一引擎信号的标的占比。
    - 平均持仓时长：对每个命中日，取计划持仓 min(5, 剩余可交易日至序列末尾) 的样本均值。
    - danger_sell：窗口内逐日调用 would_trigger_danger_sell 的累计次数（全样本）。
    - 主力连红>8 日：窗口内以当日为尾的连续 net_main_amount>0 天数≥8 时，T+5 收盘相对当日收盘涨幅%。
    """
    set_backtest_legacy_mode(bool(legacy_mode))
    try:
        win: PainpointWindow = parse_pain_period(period)
        codes = get_all_stock_codes()
        if not codes:
            return {"error": "daily_data 无股票代码或未建库", "period": period}

        if max_codes is not None:
            codes = codes[: int(max_codes)]

        warmup_period = 130
        eligible: List[str] = []
        hit_stocks: Set[str] = set()
        hold_days_samples: List[float] = []
        danger_sell_count = 0
        nm_streak_returns: List[float] = []
        first_hit_forward5d_returns: List[float] = []

        vol_col_guess = "vol"
        processed = 0

        for ts_code in codes:
            df = get_stock_data_qfq(ts_code, limit=800)
            processed += 1
            if df is None or df.empty or "trade_date" not in df.columns:
                continue

            df = df.copy()
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            vol_col = "vol" if "vol" in df.columns else "volume"
            vol_col_guess = vol_col

            win_mask = (df["trade_date"] >= win.start) & (df["trade_date"] <= win.end)
            if not win_mask.any():
                continue

            wdf = df.loc[win_mask]
            last_w = wdf.iloc[-1]
            circ_last = float(
                pd.to_numeric(
                    last_w.get("circ_mv", last_w.get("total_mv", 0)),
                    errors="coerce",
                )
                or 0.0
            )
            if not circ_mv_in_horse_elephant_band(circ_last):
                continue
            eligible.append(ts_code)

            vma5_col = (
                "vol_ma5"
                if "vol_ma5" in df.columns
                else ("vma5" if "vma5" in df.columns else vol_col)
            )
            df["vol_ma5_shifted"] = df[vma5_col].shift(1).fillna(0)
            df["vr_sim"] = np.where(
                df["vol_ma5_shifted"] > 0,
                pd.to_numeric(df[vol_col], errors="coerce") / df["vol_ma5_shifted"],
                0.0,
            )

            # 【性能优化 V3】预计算疼痛点回测循环所需的所有向量化数组
            # 替代方案：循环外一次性向量化预计算，循环内直接下标访问，零拷贝 O(1)
            nm_main_arr = pd.to_numeric(df.get('net_main_amount', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            close_pain_arr = pd.to_numeric(df['close'], errors='coerce').fillna(0).values
            circ_mv_pain_arr = pd.to_numeric(df.get('circ_mv', df['total_mv']), errors='coerce').fillna(10000000).values
            total_mv_pain_arr = pd.to_numeric(df['total_mv'], errors='coerce').fillna(0).values
            vr_sim_arr = pd.to_numeric(df['vr_sim'], errors='coerce').fillna(0).values
            open_pain_arr = pd.to_numeric(df['open'], errors='coerce').fillna(0).values
            high_pain_arr = pd.to_numeric(df['high'], errors='coerce').fillna(0).values
            low_pain_arr = pd.to_numeric(df['low'], errors='coerce').fillna(0).values
            pre_close_pain_arr = pd.to_numeric(df['pre_close'], errors='coerce').fillna(0).values
            vol_pain_arr = pd.to_numeric(df[vol_col], errors='coerce').fillna(0).values
            amount_pain_arr = pd.to_numeric(df['amount'], errors='coerce').fillna(0).values
            trf_pain_arr = pd.to_numeric(df.get('turnover_rate_f', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            up_limit_pain_arr = pd.to_numeric(df.get('up_limit', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            net_elg_pain_arr = pd.to_numeric(df.get('net_elg_amount', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            cyq_pain_arr = pd.to_numeric(df.get('cyq_concentration', pd.Series(999, index=df.index)), errors='coerce').fillna(999).values
            cost50_pain_arr = pd.to_numeric(df.get('cost_50th', close_pain_arr), errors='coerce').fillna(0).values
            hk_vol_pain_arr = pd.to_numeric(df.get('hk_vol', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            rz_pain_arr = pd.to_numeric(df.get('rz_net_buy', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            wr_pain_arr = pd.to_numeric(df.get('winner_rate', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            cost5_pain_arr = pd.to_numeric(df.get('cost_5th', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            cost95_pain_arr = pd.to_numeric(df.get('cost_95th', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            avg_cost_pain_arr = pd.to_numeric(df.get('avg_cost', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            limit_times_pain_arr = pd.to_numeric(df.get('limit_times', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            forecast_pain_arr = pd.to_numeric(df.get('forecast_type', pd.Series(0, index=df.index)), errors='coerce').fillna(0).values
            # 主力连红预计算：用 O(n) 累加替代 O(n^2) 内层回溯循环
            nm_pos_flag = (nm_main_arr > 0).astype(int)
            streak_cum = np.zeros(len(nm_main_arr), dtype=int)
            streak_len = 0
            for idx in range(len(nm_main_arr)):
                if nm_pos_flag[idx]:
                    streak_len += 1
                else:
                    streak_len = 0
                streak_cum[idx] = streak_len
            nm_streak_ge8 = streak_cum >= 8
            # T+5 收益预计算（主力连红触发时使用）
            n_pain = len(close_pain_arr)
            c5_arr = np.full(n_pain, np.nan)
            c5_arr[:n_pain-5] = close_pain_arr[5:]
            nm_ret_arr = np.where(
                (close_pain_arr > 0) & np.isfinite(c5_arr) & nm_streak_ge8,
                (c5_arr - close_pain_arr) / close_pain_arr * 100.0,
                np.nan
            )

            stock_any_hit = False
            first_hit_forward_done = False
            for pos in range(len(df)):
                td = df["trade_date"].iloc[pos]
                if td.normalize() < win.start.normalize() or td.normalize() > win.end.normalize():
                    continue
                if pos < warmup_period:
                    continue

                sub_df = df.iloc[: pos + 1]
                # 【性能优化 V3】用预计算数组替代 df.iloc[pos] 逐行读取
                vr_val = vr_sim_arr[pos]
                vol_raw = vol_pain_arr[pos]
                circ_wan_row = circ_mv_pain_arr[pos]
                close_val = close_pain_arr[pos]
                trf = trf_pain_arr[pos]
                if trf <= 0 and vol_raw > 0 and close_val > 0 and circ_wan_row > 0:
                    trf = infer_turnover_rate_f_pct(vol_raw / 100.0, close_val, circ_wan_row)
                mock_rt = {
                    'price': close_val, 'open': open_pain_arr[pos],
                    'high': high_pain_arr[pos], 'low': low_pain_arr[pos],
                    'pre_close': pre_close_pain_arr[pos], 'volume': vol_raw,
                    'amount': amount_pain_arr[pos], 'vol_ratio': vr_val,
                    'turnover_rate_f': trf,
                    'up_limit': up_limit_pain_arr[pos],
                    'net_main_amount': nm_main_arr[pos],
                    'net_elg_amount': net_elg_pain_arr[pos],
                    'cyq_concentration': cyq_pain_arr[pos],
                    'cost_50th': cost50_pain_arr[pos],
                    'hk_vol': hk_vol_pain_arr[pos],
                    'rz_net_buy': rz_pain_arr[pos],
                    'winner_rate': wr_pain_arr[pos],
                    'cost_5th': cost5_pain_arr[pos],
                    'cost_95th': cost95_pain_arr[pos],
                    'avg_cost': avg_cost_pain_arr[pos],
                    'limit_times': limit_times_pain_arr[pos],
                    'forecast_type': forecast_pain_arr[pos],
                    'circ_mv': circ_wan_row,
                    'total_mv': total_mv_pain_arr[pos],
                    'is_backtest': True,
                }
                sz_emoji = size_emoji_from_circ_mv_wan(circ_wan_row)
                vz = _vol_z_at_index(df, pos, vol_col)
                tr_ds, _ = would_trigger_danger_sell(sub_df, mock_rt, sz_emoji, 0, vz, 999)
                if tr_ds:
                    danger_sell_count += 1

            if stock_any_hit:
                hit_stocks.add(ts_code)

            if "net_main_amount" in df.columns:
                nm = pd.to_numeric(df["net_main_amount"], errors="coerce").fillna(0.0)
                for pos in range(len(df)):
                    td = df["trade_date"].iloc[pos]
                    if td.normalize() < win.start.normalize() or td.normalize() > win.end.normalize():
                        continue
                    streak = 0
                    # 【性能优化 V3】主力连红：用预计算的 nm_streak_ge8[pos] 替代 O(n^2) 内层回溯循环
                    if nm_streak_ge8[pos] and pos + 5 < len(df):
                        nm_ret = nm_ret_arr[pos]
                        if np.isfinite(nm_ret):
                            nm_streak_returns.append(float(nm_ret))


        ne = len(eligible)
        nh = len(hit_stocks)
        hit_rate_pct = (nh / ne * 100.0) if ne > 0 else 0.0
        avg_hold = float(np.mean(hold_days_samples)) if hold_days_samples else 0.0
        avg_nm_ret = float(np.mean(nm_streak_returns)) if nm_streak_returns else 0.0
        fwd_arr = np.array(first_hit_forward5d_returns, dtype=float)
        mean_fwd5 = float(np.mean(fwd_arr)) if fwd_arr.size > 0 else 0.0
        mdd_simple = float(calculate_mdd(fwd_arr)) if fwd_arr.size > 0 else 0.0

        return {
            "period_key": win.key,
            "start": str(win.start.date()),
            "end": str(win.end.date()),
            "legacy_mode": bool(legacy_mode),
            "circ_mv_band_wan_min": CIRC_MV_WAN_MIN,
            "circ_mv_band_wan_max": CIRC_MV_WAN_MAX,
            "eligible_universe_n": ne,
            "hit_stocks_n": nh,
            "hit_rate_pct": round(hit_rate_pct, 4),
            "avg_hold_days_effective": round(avg_hold, 4),
            "danger_sell_triggers_total": int(danger_sell_count),
            "net_main_streak_gt8_sample_n": int(len(nm_streak_returns)),
            "net_main_streak_gt8_avg_return_pct": round(avg_nm_ret, 4),
            "processed_codes": processed,
            "vol_col_used": vol_col_guess,
            "hold_samples_n": int(len(hold_days_samples)),
            "KPI_1_100_500亿命中率_pct": round(hit_rate_pct, 4),
            "KPI_1_平均持仓天数_计划值": round(avg_hold, 4),
            "KPI_2_danger_sell_触发总次数": int(danger_sell_count),
            "KPI_3_主力连红大于8日_T5平均收益_pct": round(avg_nm_ret, 4),
            "approx_mean_forward5d_return_pct": round(mean_fwd5, 4),
            "approx_mdd_pct_on_forward5d_samples": round(mdd_simple, 4),
            "forward5d_samples_n": int(fwd_arr.size),
        }
    finally:
        reset_backtest_context()


def _parse_bool_cli(s: str) -> bool:
    sl = (s or "").strip().lower()
    if sl in ("1", "true", "yes", "y", "on"):
        return True
    if sl in ("0", "false", "no", "n", "off"):
        return False
    return bool(s)


def main(argv: Optional[List[str]] = None) -> int:
    """
    命令行入口。

    交钥匙示例（在项目根目录执行）：

    python -m core.backtest_runner --mode=painpoint --legacy_mode=false --period=tech_rally

    python -m core.backtest_runner --mode=painpoint --legacy_mode=true --period=20241008_20241120 --max_codes=200

    python -m core.backtest_runner --mode=painpoint --legacy_mode=False --period=bluechip_accel
    """
    parser = argparse.ArgumentParser(
        description="小杰AI选股系统 Pro V26.6 回测引擎 CLI（痛点专项已实现）"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="painpoint",
        help="painpoint=痛点专项；其它模式请用 Python API",
    )
    parser.add_argument(
        "--legacy_mode",
        type=str,
        default="false",
        help="true/false：是否启用 legacy 风控/筛选分支（AB 对比）",
    )
    parser.add_argument(
        "--period",
        type=str,
        default="tech_rally",
        help="tech_rally | bluechip_accel | YYYYMMDD_YYYYMMDD",
    )
    parser.add_argument(
        "--max_codes",
        type=int,
        default=None,
        help="最多遍历股票数（省略则全市场，可能较慢）",
    )
    args = parser.parse_args(argv)

    if args.mode.strip().lower() == "painpoint":
        out = run_painpoint_backtest(
            period=args.period,
            legacy_mode=_parse_bool_cli(args.legacy_mode),
            max_codes=args.max_codes,
        )
        print("--- 痛点专项回测报告 ---")
        for k in sorted(out.keys()):
            print(f"{k}: {out[k]}")
        return 0 if "error" not in out else 1

    print("提示：当前 CLI 仅实现 --mode=painpoint。单股/批量请 from core.backtest_runner import run_strategy_backtest, run_batch_backtest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())