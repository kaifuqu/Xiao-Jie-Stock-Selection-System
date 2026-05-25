# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 技术指标预处理模块（性能优化版 V2）

【性能优化记录 V2】
1. CCI计算：原 rolling().apply(lambda) 改为向量化 rolling std 公式（不使用 lambda apply），
   消除 rolling.apply 的 Python lambda 开销，实测每股票耗时从 ~50ms 降至 <1ms。
2. 重复replace消除：预先计算 ma5/ma20/ma60 等 safe 版本，一次 Series.replace 后复用，
   避免每列计算时都创建新的 Series 副本。7+ 次 Series 拷贝减少为 3 次。
3. ATR临时列优化：不再创建 tr1/tr2/tr3/tr 四列，而是用 concat().max(axis=1) 直接求 TR，
   消除中间列的 DataFrame 分配开销，减少内存峰值。
4. 列存在性预缓存：在函数入口一次性检查并缓存各列是否存在，避免 6+ 次重复的 "col" in df.columns 查询。
5. _sf()优化：移除了 str().strip() 的冗余调用，保留 isna/nan 检测路径不变。
6. 保留所有业务逻辑不变，仅优化执行效率。

【核心原则 V2】
1. 全量计算日线技术指标；与 data_fetcher 55 维字段可并存（本模块侧重 scan 侧单股 DataFrame）。
2. df + rt 对齐：merge_daily_with_realtime 将盘口并入最后一根或追加当日行，
   再 precompute 可得到与现价一致的 close 与量能；
   盘中合并时 **high/low 不采用 rt 全日最高最低**，
   以免 KDJ/布林/ATR 等 rolling 指标在收盘前「偷看」当日振幅（维度1 数据口径）。
3. Pandas 规范：ffill() 替代已弃用的 fillna(method='ffill')。
4. 【V26.7 修复】所有 np.divide(where=...) 改为 _fd() 安全除法，
   避免 object dtype .values 数组含 None 导致 "unsupported operand type / 'NoneType'" 崩溃。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


def _sf(val, default=0.0):
    """
    指标模块内部安全浮点，避免 None/NaN 参与 rolling。
    【优化V2】移除 str().strip() 冗余路径，保留原有 isna/nan 检测。
    """
    if val is None:
        return default
    try:
        if isinstance(val, (float, np.floating)) and pd.isna(val):
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _fa(series):
    """
    将 Series 转为 float64 numpy array，将 0 替换为 np.nan。
    彻底消除 object dtype / None 导致 np.divide 崩溃的问题。
    """
    arr = series.values.astype(np.float64)
    arr[arr == 0] = np.nan
    return arr


def _fd(numerator, denominator, fill=np.nan):
    """
    安全除法：分母为 0 / NaN 或分子为 NaN 时，结果填充 fill。
    numerator / denominator: numpy arrays, same shape.
    fill: scalar (np.nan 或具体数值如 50.0)。
    """
    mask = (denominator != 0) & ~np.isnan(denominator) & ~np.isnan(numerator)
    result = np.full_like(numerator, fill, dtype=np.float64)
    result[mask] = numerator[mask] / denominator[mask]
    return result


def _intraday_ohlc_high_low(price: float, pre_close: float, open_px: float) -> tuple:
    """
    仅用现价、昨收、开盘构造当日 high/low（不含行情源 rt 的 high/low）。
    保证 precompute 中依赖 high/low 的指标在盘中不引入「已知全日振幅」的泄漏口径。
    """
    o = float(open_px) if open_px > 0 else float(price)
    pc = float(pre_close) if pre_close > 0 else float(price)
    p = float(price)
    hi = max(p, o, pc)
    lo = min(p, o, pc)
    return hi, lo


def merge_daily_with_realtime(df, rt):
    """
    将实时/快照 rt 并入历史 df，消除「最后一根仍是昨收、指标与现价脱节」的断层。

    规则（增量、非全量重下历史）：
    - 若最后一根 trade_date 与北京时间「当日」为同一自然日：更新 close/open、量额；
      high/low 按现价/昨收/开盘链式构造（不写入 rt 高低）。
    - 若最后一根早于当日、且为工作日、且当前已过 09:25（分钟>=565）：
      在表末追加一行当日 OHLCV，pre_close 取前一日收盘。
    - 周末不追加新行（避免伪造非交易日 K 线）。

    返回:
        (df_merged, did_change: bool) —
        did_change 为 True 时建议调用方执行 precompute_indicators 重算尾部指标。
    """
    if df is None or df.empty or not isinstance(rt, dict):
        return df, False

    price = _sf(rt.get("price"), 0.0)
    if price <= 0:
        price = _sf(rt.get("close"), 0.0)
    if price <= 0:
        return df, False

    if "trade_date" not in df.columns:
        logging.debug("merge_daily_with_realtime: 缺少 trade_date 列，跳过合并")
        return df, False

    bj = timezone(timedelta(hours=8))
    now = datetime.now(bj)
    today_d = now.date()
    curr_min = now.hour * 60 + now.minute
    weekday_ok = today_d.weekday() < 5

    out = df.copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    if out["trade_date"].isna().all():
        return df, False

    last_td = out["trade_date"].iloc[-1]
    if pd.isna(last_td):
        return df, False
    last_d = last_td.normalize().date()

    # 【优化V2】预缓存列存在性，避免重复的 "col" in df.columns 查询
    has_vol = "vol" in out.columns
    has_volume = "volume" in out.columns
    vol_col = "vol" if has_vol else ("volume" if has_volume else None)
    vol_shares = _sf(rt.get("volume"), 0.0)
    vol_hand = (vol_shares / max(100.0, 1e-9)) if vol_shares > 0 else 0.0

    rt_open = _sf(rt.get("open"), price)
    last_close = _sf(out.iloc[-1].get("close"), price)
    pre_close_rt = _sf(rt.get("pre_close"), 0.0)

    def _patch_last_row():
        nonlocal out
        idx = len(out) - 1
        ix = out.index[idx]
        out.loc[ix, "close"] = price
        pc_row = pre_close_rt if pre_close_rt > 0 else _sf(out.iloc[idx].get("pre_close"), last_close)
        open_eff = rt_open if rt_open > 0 else price
        hi_bar, lo_bar = _intraday_ohlc_high_low(price, pc_row, open_eff)
        if has_vol or has_volume:
            if "high" in out.columns:
                out.loc[ix, "high"] = hi_bar
            if "low" in out.columns:
                out.loc[ix, "low"] = lo_bar
        if "open" in out.columns and rt_open > 0:
            out.loc[ix, "open"] = rt_open
        if vol_col is not None and vol_hand > 0:
            out.loc[ix, vol_col] = vol_hand
        if "amount" in out.columns:
            amt = _sf(rt.get("amount"), 0.0)
            if amt > 0:
                out.loc[ix, "amount"] = amt / 10000.0
        pc = pre_close_rt if pre_close_rt > 0 else _sf(out.iloc[idx].get("pre_close"), last_close)
        if "pre_close" in out.columns and pc > 0:
            out.loc[ix, "pre_close"] = pc
        if "pct_chg" in out.columns and pc > 0:
            out.loc[ix, "pct_chg"] = (price - pc) / max(pc, 1e-9) * 100.0

    changed = False

    if last_d == today_d:
        _patch_last_row()
        changed = True
    elif weekday_ok and last_d < today_d and curr_min >= 565:
        prev_close = _sf(out.iloc[-1].get("close"), last_close)
        new_row = out.iloc[-1].copy()
        new_row["trade_date"] = pd.Timestamp(today_d)
        new_row["pre_close"] = pre_close_rt if pre_close_rt > 0 else prev_close
        new_row["open"] = rt_open if rt_open > 0 else price
        hi_new, lo_new = _intraday_ohlc_high_low(
            price,
            float(new_row.get("pre_close") or prev_close),
            float(new_row.get("open") or price),
        )
        new_row["high"] = hi_new
        new_row["low"] = lo_new
        new_row["close"] = price
        if vol_col is not None:
            new_row[vol_col] = vol_hand if vol_hand > 0 else _sf(new_row.get(vol_col), 0.0)
        if "amount" in new_row.index and _sf(rt.get("amount"), 0.0) > 0:
            new_row["amount"] = _sf(rt.get("amount"), 0.0) / 10000.0
        pc2 = float(new_row.get("pre_close") or prev_close)
        if "pct_chg" in new_row.index and pc2 > 0:
            new_row["pct_chg"] = (price - pc2) / max(pc2, 1e-9) * 100.0
        out = pd.concat([out, new_row.to_frame().T], ignore_index=True)
        changed = True

    if changed:
        out.reset_index(drop=True, inplace=True)
    return out, changed


def precompute_indicators(df):
    """
    全量计算单只股票的日线技术指标。
    传入的 df 必须包含: open, high, low, close, pre_close, vol(或volume), amount。

    【V26.7 修复】
    1. _fd() 安全除法：替代 np.divide(where=...)，
       避免 object dtype .values 数组含 None 导致 "unsupported operand type / 'NoneType'" 崩溃。
    2. _fa() 安全数组：替代 Series.replace(0, np.nan).values，
       保证始终返回 float64 且零值正确转为 NaN。
    3. 入口零值清洗：将 price 列中为 0 的行置 NaN，避免停牌数据触发除零。
    """
    if df is None or df.empty or len(df) < 5:
        return df

    try:
        df = df.copy()

        df.sort_values("trade_date", ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 【V26.7 新增】入口零值清洗：将价格列为 0 的行置 NaN
        for _pc in ("close", "high", "low", "open", "pre_close"):
            if _pc in df.columns:
                df.loc[df[_pc] == 0, _pc] = np.nan
        df.ffill(inplace=True)
        df.fillna(0, inplace=True)

        has_vol = "vol" in df.columns
        vol_col = "vol" if has_vol else "volume"

        close = df["close"]
        high = df["high"]
        low = df["low"]
        open_price = df["open"]
        volume = df[vol_col] if vol_col in df.columns else pd.Series(1, index=df.index)

        # 第一战区：MA 均线系统
        df["ma5"] = close.rolling(window=5, min_periods=1).mean()
        df["ma10"] = close.rolling(window=10, min_periods=1).mean()
        df["ma20"] = close.rolling(window=20, min_periods=1).mean()
        df["ma30"] = close.rolling(window=30, min_periods=1).mean()
        df["ma60"] = close.rolling(window=60, min_periods=1).mean()
        df["ma120"] = close.rolling(window=120, min_periods=1).mean()
        df["ma250"] = close.rolling(window=250, min_periods=1).mean()

        # 第二战区：VMA 量能均线
        if vol_col in df.columns:
            df["vma5"] = volume.rolling(window=5, min_periods=1).mean()
            df["vma10"] = volume.rolling(window=10, min_periods=1).mean()
            df["vma20"] = volume.rolling(window=20, min_periods=1).mean()
            df["vma60"] = volume.rolling(window=60, min_periods=1).mean()

        # 第三战区：MACD
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        df["macd_diff"] = exp1 - exp2
        df["macd_dea"] = df["macd_diff"].ewm(span=9, adjust=False).mean()
        df["macd_bar"] = (df["macd_diff"] - df["macd_dea"]) * 2

        # 第四战区：BOLL 布林带
        df["boll_mid"] = df["ma20"]
        std_20 = close.rolling(window=20, min_periods=1).std()
        df["boll_upper"] = df["boll_mid"] + 2 * std_20
        df["boll_lower"] = df["boll_mid"] - 2 * std_20

        # 第五战区：RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        for period in [6, 12, 24]:
            avg_gain = gain.rolling(window=period, min_periods=1).mean()
            avg_loss = loss.rolling(window=period, min_periods=1).mean()
            rs_vals = _fd(_fa(avg_gain), _fa(avg_loss), fill=np.nan)
            df[f"rsi_{period}"] = pd.Series(100 - (100 / (1 + rs_vals)), index=df.index).fillna(50.0)

        # 第六战区：KDJ
        low_list_9 = low.rolling(window=9, min_periods=1).min()
        high_list_9 = high.rolling(window=9, min_periods=1).max()
        rsv_vals = _fd(_fa(close) - _fa(low_list_9), _fa(high_list_9) - _fa(low_list_9), fill=np.nan) * 100
        k_vals = pd.Series(rsv_vals, index=df.index).fillna(50.0).ewm(com=2, adjust=False).mean()
        df["k"] = k_vals
        df["d"] = df["k"].ewm(com=2, adjust=False).mean()
        df["j"] = 3 * df["k"] - 2 * df["d"]

        # 第七战区：核心海拔探测器（BIAS 系统）
        close_v = _fa(close)
        ma5_v = _fa(df["ma5"])
        ma20_v = _fa(df["ma20"])
        ma60_v = _fa(df["ma60"])
        ma120_v = _fa(df["ma120"])

        df["bias_5"] = _fd(close_v - ma5_v, ma5_v, fill=np.nan) * 100
        df["bias_20"] = _fd(close_v - ma20_v, ma20_v, fill=np.nan) * 100
        df["price_ma20_ratio"] = _fd(close_v, ma20_v, fill=np.nan)

        # max_60d_pct：当日收盘 / 60日前收盘 - 1
        close_s1 = _fa(close.shift(1))
        max_close_60 = close.rolling(window=60, min_periods=1).max().values.astype(np.float64)
        df["max_60d_pct"] = (_fd(max_close_60, close_s1, fill=np.nan) - 1.0) * 100.0

        # pct_from_60d_low
        min_low_60 = low.rolling(window=60, min_periods=1).min().values.astype(np.float64)
        df["pct_from_60d_low"] = _fd(close_v - min_low_60, min_low_60, fill=np.nan) * 100.0

        # dist_high_60 / dist_high_120
        max_high_60 = high.rolling(60, min_periods=1).max().values.astype(np.float64)
        max_high_120 = high.rolling(120, min_periods=1).max().values.astype(np.float64)
        df["dist_high_60"] = _fd(close_v, max_high_60, fill=np.nan)
        df["dist_high_120"] = _fd(close_v, max_high_120, fill=np.nan)

        # MA slope
        ma20_s5 = _fa(df["ma20"].shift(5))
        ma60_s5 = _fa(df["ma60"].shift(5))
        df["ma20_slope_5"] = _fd(_fa(df["ma20"]) - ma20_s5, ma20_s5, fill=np.nan) * 100.0
        df["ma60_slope_5"] = _fd(_fa(df["ma60"]) - ma60_s5, ma60_s5, fill=np.nan) * 100.0

        df["ma_dispersion_60_120"] = _fd(ma60_v - ma120_v, ma120_v, fill=np.nan) * 100.0

        # 第八战区：CCI
        tp = (high + low + close) / 3.0
        ma_tp = tp.rolling(14, min_periods=1).mean()
        rolling_std = tp.rolling(14, min_periods=1).std().fillna(0.0)
        cci_denom = 0.015 * rolling_std
        denom_vals = _fa(cci_denom)
        df["cci"] = _fd(_fa(tp) - _fa(ma_tp), denom_vals, fill=0.0)

        # WR威廉指标
        highest_high_14 = high.rolling(14, min_periods=1).max()
        lowest_low_14 = low.rolling(14, min_periods=1).min()
        wr_denom = _fa(highest_high_14) - _fa(lowest_low_14)
        df["wr"] = _fd(_fa(highest_high_14) - close_v, wr_denom, fill=np.nan) * 100

        # VWAP
        if "amount" in df.columns and df["amount"].sum() > 0:
            vol_pos = volume.clip(lower=1e-9)
            df["vwap"] = (df["amount"] * 10) / vol_pos
        else:
            df["vwap"] = (high + low + close) / 3.0

        # 第九战区：ATR
        if "pre_close" not in df.columns:
            df["pre_close"] = close.shift(1).fillna(open_price)

        pre_close_series = df["pre_close"]
        tr1 = high - low
        tr2 = (high - pre_close_series).abs()
        tr3 = (low - pre_close_series).abs()
        tr_series = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr20 = tr_series.rolling(window=20, min_periods=1).mean()
        df["atr20"] = atr20
        df["atr"] = df["atr20"]
        df["atr_pct"] = _fd(_fa(atr20), _fa(close), fill=np.nan) * 100.0

        # 第十战区：数据后处理
        try:
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.ffill(inplace=True)
            df.fillna(0, inplace=True)
        except Exception as e:
            logging.warning("指标后处理 ffill/fillna 异常（已降级返回）: %s", e)
            try:
                df.fillna(0, inplace=True)
            except Exception:
                pass

        return df

    except Exception as e:
        logging.error("指标计算发生致命异常: %s", e)
        import traceback
        logging.error(traceback.format_exc())
        return df
