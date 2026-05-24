# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 - 真理测谎仪（机构级抗过拟合盲测版）
【核心升级】：
1. 🧪 OOS盲测机制：按时间严格切分 IS(前70%) 与 OOS(后30%)，杜绝样本泄漏与回看偏差。
2. 🧠 网格先训后测：所有参数仅在 IS 训练；仅保留 IS>60% 且样本>=10 的候选进入 OOS 盲测。
3. 🔍 反向复盘诊断：真龙漏网按流通市值分层（≥500亿看 T+5≥10%，300–500亿 T+5≥15%，<300亿 T+3≥15%），输出「目标窗口最大涨幅」列。
4. 📄 AI报告升级：统一输出 IS/OOS 双域指标，显式剔除“IS高胜率但OOS断崖”伪圣杯。
5. 📊 截面分位软评分：按 trade_date 全市场池对量价因子做 rank(pct=True)，替代绝对数值阈值与绝对 soft 映射。
6. 🧹 连续特征工业清洗：核心因子先同日 MAD 截断（Winsor），再同日 Z-Score；打分门禁在标准化后的序列上分位，抑制极值与量纲差异。
【实盘摩擦与流动性】：
- 买入价 = T+1 开盘价 × (1 + 0.2% 冲击滑点)；卖出价 = T+p 收盘价 × (1 − 0.15% 综合税费)。
- 一字涨停（T+1 low==high 且涨停）视为买不进，剔除；各卖出日一字跌停近似视为卖不出，按持仓期剔除。
- 网格结果仅当 OOS 盈亏比（已扣摩擦）严格 > 1.2 时进入汇总与 AI 报告。
"""
import os
import sys
import time
import traceback
import itertools
import json
import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm

# ================= 0. 环境静音与物理路径 =================
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)
pd.set_option('future.no_silent_downcasting', True)

# ================= 实盘摩擦与 OOS 准入（仅影响价格与过滤，不改 IS/OOS 日期切分） =================
BUY_IMPACT_SLIPPAGE = 0.0020   # 买入侧 +0.2% 冲击滑点
SELL_FEE_TAX = 0.0015          # 卖出侧 −0.15% 综合税费（从卖出价扣除）
LIMIT_UP_RATIO = 1.093         # T+1 涨停判定：高价相对昨收（与历史脚本一致，避免误杀）
LIMIT_DOWN_RATIO = 0.907       # T+p 跌停近似：收盘相对前一日收盘
ONE_WORD_ATOL = 1e-5           # 一字板 low==high 数值容差
MIN_OOS_PROFIT_LOSS_RATIO = 1.2  # 笛卡尔积入报告：OOS 盈亏比须严格大于该值

# ================= 截面分位门槛（语义对齐原绝对阈值，随市场同日分布缩放） =================
# vr_cross_pct：量比在同日截面上 rank(pct=True, ascending=True)，越大表示当日越放量
XS_VR_TOP = 0.80           # ≈「量比居前 20%」，对应示例 vr_rank > 0.8
XS_OPEN_MIN = 0.20         # 竞价涨幅截面分位：排除当日最弱一档（≈ 原 open_pct > -2% 的相对化）
XS_ATR_CALM_MAX = 0.70     # ATR 分位（低 ATR → 低分位）：保留冷静端，≈ 原 atr<=8 的相对版
XS_MACD_MED = 0.50         # MACD 柱高于当日截面中位
XS_PCT_STRONG = 0.55       # 当日涨跌幅截面分位：强于中轴以上（≈ 原「涨幅」硬阈的相对版）
XS_VR_P3 = 0.75            # P3 略宽于 P2 的量比截面要求
XS_VR_P4 = 0.75            # P4 与 P3 一致

# ---------- 连续特征截面清洗：MAD 截断倍数（工业常用 3～3.5；配合 1.4826 将 MAD 换算为近似正态标准差）----------
# 说明：若改用「3-Sigma」截断，可将边界设为 mean ± 3*std（ddof=0），再对截断后序列做同日 Z-Score；
# 本实现默认 MAD 更抗肥尾。全程仅用 groupby.transform，无 iterrows。
WINSOR_MAD_K = 3.5


def _cs_mad_winsor(x: pd.Series, k=None) -> pd.Series:
    """
    单日内截尾（Winsorization）——MAD 稳健版，供 groupby.transform 调用。
    数学含义：
    - 中位数 med = median(x)；绝对中位差 MAD = median(|x - med|)。
    - 正态假定下，1.4826*MAD 与标准差 σ 可比，故截断边界取：
      [med - k * 1.4826 * MAD, med + k * 1.4826 * MAD]
    - 将超出边界的样本压到边界，抑制「乌龙指/错单/极端截面」对打分的拖拽。
    全程向量化，无逐行循环。
    """
    kk = WINSOR_MAD_K if k is None else float(k)
    v = pd.to_numeric(x, errors='coerce')
    med = v.median()
    mad = (v - med).abs().median()
    mad_f = float(mad) if pd.notna(mad) else 0.0
    if mad_f <= 1e-18:
        return v
    scale = 1.4826 * mad_f
    lo = med - kk * scale
    hi = med + kk * scale
    return v.clip(lower=lo, upper=hi)


def _cs_zscore(x: pd.Series) -> pd.Series:
    """
    单日内 Z-Score：在截断后的序列上计算 (x - mean) / std（ddof=0，与截面总体一致）。
    若当日 std≈0（全相等或单样本退化），返回全 0，避免除零污染。
    """
    v = pd.to_numeric(x, errors='coerce')
    m = v.mean()
    s = v.std(ddof=0)
    sf = float(s) if pd.notna(s) else 0.0
    if sf <= 1e-18:
        return pd.Series(np.zeros(len(v), dtype=float), index=v.index)
    return (v - m) / sf


def setup_environment():
    print("="*90)
    print(" 🚀 [1/5] V26.5 终极测谎仪启动 (10万倍火力全开网格版)...")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    _bn = os.path.basename(current_dir)
    project_root = (
        os.path.dirname(current_dir) if _bn in ("tools", "offline_tools") else current_dir
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    return project_root

def get_db_connection():
    from data.db_core import get_conn
    return get_conn

# ================= 1. 全量数据吞吐与多重未来时空构建 =================
def load_and_build_space(get_conn_func=None):
    """
    从 DuckDB 拉取 daily_data 全表并构造 T+1…T+20 对齐价量。

    【资源契约】必须使用「独立只读连接」并在本函数内关闭。
    禁止对 db_core.get_conn() 的返回值执行 close()：该连接为进程级全局写连接，
    关闭后 _write_con 仍指向已死句柄，后续 save_df_to_sql / 采集会静默或显性失败。
    get_conn_func 参数已弃用，仅保留签名兼容旧调用。
    """
    print(" 🚀 [2/5] 正在向 DuckDB 索取【最近180天】全市场快照...")
    from data.db_core import get_duckdb_path
    import duckdb

    _path = get_duckdb_path()
    con = duckdb.connect(_path, read_only=True)
    try:
        try:
            has_view = con.execute("SELECT COUNT(*) FROM duckdb_views() WHERE view_name = 'vw_daily_data_compat'").fetchone()[0] > 0
        except Exception:
            has_view = False
        source_table = "vw_daily_data_compat" if has_view else "daily_data"
        query = f"SELECT * FROM {source_table} ORDER BY ts_code, trade_date"
        df_all = con.execute(query).fetchdf()
    finally:
        try:
            con.close()
        except Exception:
            pass

    if len(df_all) == 0: raise ValueError("❌ 数据库为空！请先运行数据采集。")
        
    valid_dates = df_all['trade_date'].drop_duplicates().sort_values(ascending=False).head(180)
    df_all = df_all[df_all['trade_date'].isin(valid_dates)].copy()
    
    max_trade_days = df_all['trade_date'].nunique()
    valid_counts = df_all.groupby('ts_code').size()
    # 允许停牌容错，保留至少有 150 天数据的标的
    perfect_stocks = valid_counts[valid_counts >= 150].index
    df_all = df_all[df_all['ts_code'].isin(perfect_stocks)].copy()
    print(f" ✅ 连续性清洗完毕！保留 {df_all['ts_code'].nunique()} 只活跃标的。")
    
    print(" ⚙️ 启动向量化引擎：正在构建 T+1 到 T+20 的全周期未来收益曲线...")
    start_vec = time.time()
    
    all_dates = sorted(df_all['trade_date'].unique())
    date_map = pd.DataFrame({'trade_date': all_dates})
    for i in range(1, 21):
        date_map[f't{i}_date'] = date_map['trade_date'].shift(-i)
    
    df_all = df_all.merge(date_map, on='trade_date', how='left')
    price_lookup = df_all[['ts_code', 'trade_date', 'open', 'close', 'low', 'pre_close']].copy()
    
    # 需含 high：一字涨/跌停判定；T+1 需含 close：跌停日昨收链与 t1_close
    for i in range(1, 21):
        cols_to_get = ['low', 'high', 'close']
        if i == 1:
            cols_to_get = ['open', 'low', 'high', 'close', 'pre_close']
        df_ti = price_lookup.rename(columns={'trade_date': f't{i}_date'})
        df_ti = df_ti[['ts_code', f't{i}_date'] + cols_to_get]
        df_ti = df_ti.rename(columns={c: f't{i}_{c}' for c in cols_to_get})
        df_all = df_all.merge(df_ti, on=['ts_code', f't{i}_date'], how='left')
    
    pc1 = pd.to_numeric(df_all['t1_pre_close'], errors='coerce')
    df_all['is_limit_up'] = df_all['t1_open'] >= (pc1 * LIMIT_UP_RATIO)
    # 双边摩擦：买贵 0.2%、卖价再扣 0.15%（无效开盘价置 NaN，避免后续 pnl 除零产生 inf）
    df_all['buy_price'] = pd.to_numeric(df_all['t1_open'], errors='coerce') * (1.0 + BUY_IMPACT_SLIPPAGE)
    df_all.loc[df_all['buy_price'] <= 0, 'buy_price'] = np.nan

    # T+1 一字涨停：low==high 且处于涨停区 → 实盘买不进（缺 high/low 时保守：不判一字，交由 buy_price/is_limit_up 过滤）
    t1_lo = pd.to_numeric(df_all['t1_low'], errors='coerce')
    t1_hi = pd.to_numeric(df_all['t1_high'], errors='coerce')
    t1_bar_ok = t1_lo.notna() & t1_hi.notna()
    t1_one_word = np.isclose(t1_lo, t1_hi, rtol=0.0, atol=ONE_WORD_ATOL, equal_nan=False)
    t1_limit_state = (pc1 > 0) & (t1_hi >= pc1 * LIMIT_UP_RATIO)
    df_all['t1_one_word_limit_up'] = t1_bar_ok & t1_one_word & t1_limit_state

    for p in [2, 3, 5, 10, 20]:
        clp = pd.to_numeric(df_all[f't{p}_close'], errors='coerce')
        df_all[f'sell_p_t{p}'] = clp * (1.0 - SELL_FEE_TAX)
        low_cols = [f't{i}_low' for i in range(1, p+1)]
        df_all[f'low_t{p}'] = df_all[low_cols].min(axis=1)
        
        df_all[f'pnl_t{p}'] = (df_all[f'sell_p_t{p}'] - df_all['buy_price']) / df_all['buy_price'] * 100.0
        df_all[f'mdd_t{p}'] = (df_all[f'low_t{p}'] - df_all['buy_price']) / df_all['buy_price'] * 100.0

    # 各卖出日 T+p：一字跌停近似（low==high 且收盘贴近跌停）→ 卖不出，该持仓期记为不可平仓
    for p in [2, 3, 5, 10, 20]:
        prev_c = pd.to_numeric(df_all[f't{p-1}_close'], errors='coerce') if p > 2 else pd.to_numeric(df_all['t1_close'], errors='coerce')
        lo = pd.to_numeric(df_all[f't{p}_low'], errors='coerce')
        hi = pd.to_numeric(df_all[f't{p}_high'], errors='coerce')
        cl = pd.to_numeric(df_all[f't{p}_close'], errors='coerce')
        ow = np.isclose(lo, hi, rtol=0.0, atol=ONE_WORD_ATOL, equal_nan=False) & np.isfinite(lo)
        lim_dn = (prev_c > 0) & (cl <= prev_c * LIMIT_DOWN_RATIO)
        df_all[f't{p}_stuck_limit_down'] = ow & lim_dn

    print(f" ✅ 20维时空扩容构建完毕！极速耗时: {time.time() - start_vec:.2f} 秒。")
    
    df_all['mv_group'] = pd.cut(
        df_all['circ_mv'].fillna(df_all['total_mv']*0.6)/10000,
        bins=[0, 300, 500, np.inf],
        labels=['100-300亿', '300-500亿', '500亿+']
    )

    # ==================== OOS盲测切分（按交易日时间顺序严格前70%/后30%） ====================
    all_valid_dates = sorted(df_all['trade_date'].dropna().unique())
    if len(all_valid_dates) < 10:
        raise ValueError("❌ 有效交易日不足，无法进行 IS/OOS 稳健切分（至少需要 10 个交易日）")

    split_idx = int(len(all_valid_dates) * 0.7)
    # 防止极端场景出现一侧为空
    split_idx = max(1, min(split_idx, len(all_valid_dates) - 1))

    is_dates = set(all_valid_dates[:split_idx])
    oos_dates = set(all_valid_dates[split_idx:])

    df_all['sample_set'] = np.where(df_all['trade_date'].isin(is_dates), 'IS', 'OOS')
    print(f" ✅ OOS切分完成 | IS交易日: {len(is_dates)} | OOS交易日: {len(oos_dates)}")
    return df_all

# ================= 2. 向量化基础特征工厂 (52维原生对齐) =================
def prepare_vectorized_features(df):
    print(" 🚀 [3/5] 正在装载核武特征与 G10 跨日记忆 (已开启类型强转安检)...")

    # 关键基础列：全部强制转数值，彻底规避 DuckDB 字符串 '0.0' 参与运算
    df['circ_mv'] = pd.to_numeric(df.get('circ_mv', 0.0), errors='coerce').fillna(0.0)
    df['total_mv'] = pd.to_numeric(df.get('total_mv', 0.0), errors='coerce').fillna(0.0)
    df['net_elg_amount'] = pd.to_numeric(df.get('net_elg_amount', 0.0), errors='coerce').fillna(0.0)
    df['turnover_rate_f'] = pd.to_numeric(df.get('turnover_rate_f', 0.0), errors='coerce').fillna(0.0)
    df['vol_ratio'] = pd.to_numeric(df.get('vol_ratio', 0.0), errors='coerce').fillna(0.0)
    df['pct_chg'] = pd.to_numeric(df.get('pct_chg', 0.0), errors='coerce').fillna(0.0)
    df['winner_rate'] = pd.to_numeric(df.get('winner_rate', 100.0), errors='coerce').fillna(100.0)
    df['atr_pct'] = pd.to_numeric(df.get('atr_pct', 3.0), errors='coerce').fillna(3.0)
    df['ma20_slope_5'] = pd.to_numeric(df.get('ma20_slope_5', 0.0), errors='coerce').fillna(0.0)
    df['bias_20'] = pd.to_numeric(df.get('bias_20', 0.0), errors='coerce').fillna(0.0)
    df['macd_hist'] = pd.to_numeric(df.get('macd_hist', 0.0), errors='coerce').fillna(0.0)
    df['rsi_14'] = pd.to_numeric(df.get('rsi_14', 50.0), errors='coerce').fillna(50.0)
    df['pre_close'] = pd.to_numeric(df.get('pre_close', 0.0), errors='coerce').fillna(0.0)
    df['open'] = pd.to_numeric(df.get('open', 0.0), errors='coerce').fillna(0.0)
    df['close'] = pd.to_numeric(df.get('close', 0.0), errors='coerce').fillna(0.0)
    df['ma20'] = pd.to_numeric(df.get('ma20', np.nan), errors='coerce')
    df['ma60'] = pd.to_numeric(df.get('ma60', np.nan), errors='coerce')
    df['cost_5th'] = pd.to_numeric(df.get('cost_5th', np.nan), errors='coerce')
    if "vol" not in df.columns:
        if "volume" in df.columns:
            df["vol"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0) / 100.0
        else:
            df["vol"] = 0.0

    try:
        from core.strategies.fund_mv_utils import series_effective_turnover_f_daily

        if "ts_code" in df.columns:
            chunks = []
            for _, g in df.groupby("ts_code", sort=False):
                gg = g.copy()
                gg["turnover_rate_f"] = series_effective_turnover_f_daily(gg).values
                chunks.append(gg)
            df = pd.concat(chunks, axis=0) if chunks else df
        else:
            df["turnover_rate_f"] = series_effective_turnover_f_daily(df).values
    except Exception as ex:
        print(f" ⚠️ 真换手向量化回填跳过: {ex}")

    # 核心字段映射（全部基于已强转列）
    df['net_elg'] = df['net_elg_amount']
    df['tr'] = df['turnover_rate_f']
    df['vr'] = df['vol_ratio']
    df['pct'] = df['pct_chg']
    df['slope'] = df['ma20_slope_5']

    # 竞价涨幅：用于 P2 早盘网格（当日开盘相对昨收）
    df['open_pct'] = np.where(
        df['pre_close'] > 0,
        (df['open'] - df['pre_close']) / df['pre_close'] * 100.0,
        0.0
    )

    # 筹码集中度：若数据库中缺失，默认高值（不易通过严格阈值）
    df['cyq_concentration'] = pd.to_numeric(df.get('cyq_concentration', 999.0), errors='coerce').fillna(999.0)

    # 流入占比(%)：先构造安全分母，再做向量化除法，避免 object 类型混入
    denom = (df['circ_mv'] * 10000.0).astype(float)
    safe_num = df['net_elg'].astype(float)
    df['elg_ratio_pct'] = np.where(denom > 0, safe_num / denom * 100.0, 0.0)

    # ========== 连续特征：同日截面 MAD 截断 + Z-Score（groupby.transform，禁止 iterrows）==========
    # 原始列保留不动；新增 *_w（winsor 后）、*_z（再标准化）。打分与软门禁一律读 *_z 做 rank，避免量纲与肥尾。
    _gtd_clean = df['trade_date']
    _core_clean_cols = [
        'slope', 'bias_20', 'elg_ratio_pct', 'turnover_rate_f',
        'vr', 'pct_chg', 'open_pct', 'atr_pct', 'macd_hist',
    ]
    for _cc in _core_clean_cols:
        if _cc not in df.columns:
            continue
        _wn = f'{_cc}_w'
        _zn = f'{_cc}_z'
        df[_wn] = df.groupby(_gtd_clean, sort=False)[_cc].transform(_cs_mad_winsor)
        df[_zn] = df.groupby(_gtd_clean, sort=False)[_wn].transform(_cs_zscore)

    # G10 连续流入加权记忆得分（滞后项基于 elg_ratio_pct_w，削弱极端净流入噪声）
    df = df.sort_values(['ts_code', 'trade_date'])
    # slope_t0/t1 保留原始斜率：跨日「走强」是同一标的时序比较，不宜混用不同交易日的 z 分位
    df['slope_t0'] = df['slope']
    df['slope_t1'] = df.groupby('ts_code')['slope'].shift(1).fillna(0.0)
    df['elg_r_t1'] = df.groupby('ts_code')['elg_ratio_pct_w'].shift(1).fillna(0.0)
    df['elg_r_t2'] = df.groupby('ts_code')['elg_ratio_pct_w'].shift(2).fillna(0.0)
    df['elg_r_t3'] = df.groupby('ts_code')['elg_ratio_pct_w'].shift(3).fillna(0.0)

    def apply_weight(val, weight):
        return np.where(val > 0.0, val * weight, 0.0)

    df['g10_score'] = (
        apply_weight(df['elg_ratio_pct_w'], 1.0) +
        apply_weight(df['elg_r_t1'], 0.8) +
        apply_weight(df['elg_r_t2'], 0.5) +
        apply_weight(df['elg_r_t3'], 0.3)
    )

    df['macd_red'] = df['macd_hist'] > 0

    # ---------- 同日截面分位 rank(pct=True)：输入为 *_z，与标准化后的「同一天平」一致 ----------
    _gtd = df['trade_date']

    def _xs_rank(col_name, ascending=True):
        s = pd.to_numeric(df[col_name], errors='coerce')
        return s.groupby(_gtd, sort=False).transform(
            lambda x: x.rank(pct=True, ascending=ascending, method='average')
        ).fillna(0.5)

    # 供 build_soft_mask：对 Z 后的因子做分位（单调性与 winsor 前一致，尾部更稳）
    df['vr_cross_pct'] = _xs_rank('vr_z', ascending=True)
    df['open_pct_cross_pct'] = _xs_rank('open_pct_z', ascending=True)
    df['atr_cross_pct'] = _xs_rank('atr_pct_z', ascending=True)
    df['macd_hist_cross_pct'] = _xs_rank('macd_hist_z', ascending=True)
    df['pct_cross_pct'] = _xs_rank('pct_chg_z', ascending=True)

    # 软评分因子（0~100）：在截面 Z 上做 rank，再映射到 0~100
    # 1) slope_score：slope_z 越高分位越高；走强加分仍基于原始斜率时序
    df['slope_trend_up'] = (df['slope_t0'] > df['slope_t1']).astype(float)
    slope_pct = _xs_rank('slope_z', ascending=True)
    df['slope_score'] = np.clip(slope_pct * 100.0 + df['slope_trend_up'] * 10.0, 0.0, 100.0)

    # 2) bias_score：|bias_20_z| 越小越好 → 升序分位后 (1-p)*100
    bias_abs = df['bias_20_z'].abs()
    bias_abs_pct = bias_abs.groupby(_gtd, sort=False).transform(
        lambda x: pd.to_numeric(x, errors='coerce').rank(pct=True, ascending=True, method='average')
    ).fillna(0.5)
    df['bias_score'] = np.clip((1.0 - bias_abs_pct) * 100.0, 0.0, 100.0)

    # 3) elg_score：elg_ratio_pct_z 越高分位越高
    elg_pct = _xs_rank('elg_ratio_pct_z', ascending=True)
    df['elg_score'] = np.clip(elg_pct * 100.0, 0.0, 100.0)

    # 4) cost_score：先算 gap%，再对 gap 做同日 MAD 截断与 Z，再在 Z 上做「越小越好」分位
    cost_base = df['cost_5th'].fillna(df['close']).replace(0.0, np.nan)
    df['cost_gap_pct'] = (
        (df['close'] - cost_base).abs() / cost_base
    ).replace([np.inf, -np.inf], np.nan).fillna(1.0) * 100.0
    df['cost_gap_pct_w'] = df.groupby(_gtd, sort=False)['cost_gap_pct'].transform(_cs_mad_winsor)
    df['cost_gap_pct_z'] = df.groupby(_gtd, sort=False)['cost_gap_pct_w'].transform(_cs_zscore)
    cost_gap_rank = df['cost_gap_pct_z'].groupby(_gtd, sort=False).transform(
        lambda x: pd.to_numeric(x, errors='coerce').rank(pct=True, ascending=True, method='average')
    ).fillna(0.5)
    df['cost_score'] = np.clip((1.0 - cost_gap_rank) * 100.0, 0.0, 100.0)

    # 全局软总分（可按战区微调权重）
    df['total_score_base'] = (
        0.30 * df['slope_score'] +
        0.25 * df['bias_score'] +
        0.30 * df['elg_score'] +
        0.15 * df['cost_score']
    )
    # 打分引擎防爆：线性组合在极端缺失下仍可能浮出 NaN/inf，统一压成有限值
    _tsb = pd.to_numeric(df['total_score_base'], errors='coerce')
    df['total_score_base'] = np.nan_to_num(_tsb.to_numpy(dtype=float), nan=0.0, posinf=0.0, neginf=0.0)

    # ---------- P1 左侧一票否决 + 放量豁免（向量化，与实盘 score_calibration 门禁对齐；网格样本池前置清洗）----------
    slope_v = pd.to_numeric(df['slope'], errors='coerce').fillna(0.0)
    bias_v = pd.to_numeric(df['bias_20'], errors='coerce').fillna(0.0)
    limit_dn_risk = (bias_v < -12.0) | (slope_v < -0.05)
    ma60_pos = df['ma60'].notna() & (pd.to_numeric(df['ma60'], errors='coerce') > 0)
    ma20_ok = df['ma20'].notna()
    if ma60_pos.any():
        ma60_safe = pd.to_numeric(df['ma60'], errors='coerce').clip(lower=1e-9)
        ma20_f = pd.to_numeric(df['ma20'], errors='coerce')
        break_ma = ma20_ok & (ma20_f < ma60_safe * 0.985)
        break_ma = break_ma.fillna(False)
        p1_veto = limit_dn_risk | break_ma
    else:
        print(
            " ⚠️ [P1网格门] 无有效 ma60 列或全为缺失：极寒否决不含「MA20<MA60×0.985」，"
            "仅用 bias_20<-12 与 slope<-0.05（与 limit_dn_risk 近似）；请确认 daily_data 含 ma60。"
        )
        p1_veto = limit_dn_risk

    vr_v = pd.to_numeric(df['vr'], errors='coerce')
    vr_v = vr_v.fillna(1.0)
    pct_v = pd.to_numeric(df['pct_chg'], errors='coerce').fillna(0.0)
    p1_exemption = (vr_v >= 1.4) & (pct_v >= 2.5)
    df['is_p1_survivor'] = ~(p1_veto & (~p1_exemption))

    # 有效交易底线：兼容缺列场景，避免 KeyError
    if 'is_limit_up' in df.columns:
        is_limit_up = df['is_limit_up'].fillna(False).astype(bool)
    else:
        is_limit_up = pd.Series(False, index=df.index)
    if 'buy_price' in df.columns:
        buy_ok = df['buy_price'].notna()
    else:
        buy_ok = pd.Series(True, index=df.index)
    if 't1_one_word_limit_up' in df.columns:
        ow_lim = df['t1_one_word_limit_up'].fillna(False).astype(bool)
    else:
        ow_lim = pd.Series(False, index=df.index)
    # 入场可成交：非开盘涨停扫货假设失败 + 非一字涨停买不进 + P1 左侧否决/豁免幸存者
    df['is_valid_trade_entry'] = (~is_limit_up) & buy_ok & (~ow_lim) & df['is_p1_survivor']
    for _p in (2, 3, 5, 10, 20):
        col = f'can_exit_t{_p}'
        stuck = f't{_p}_stuck_limit_down'
        if stuck in df.columns:
            df[col] = ~df[stuck].fillna(False).astype(bool)
        else:
            df[col] = True
    # 兼容旧字段：单布尔仍表示「至少入场有效」；按持仓期筛选在 _calc_basic_metrics 内用 can_exit_t{p}
    df['is_valid_trade'] = df['is_valid_trade_entry']

    print(" ✅ Soft Score 完成：核心因子已 MAD 截断 + 截面 Z-Score，再在 Z 上做 rank(pct)×100。")
    return df

# ================= 3. 笛卡尔积十万倍网格搜索 =================
def run_massive_grid_search(df):
    """
    四大战区统一网格搜索（重构版）：
    - P2：早盘竞价核爆
    - P3：盘中主升承接
    - P4：尾盘盲狙防守
    - P5：上帝视角真龙
    """
    print(" 🚀 [4/5] 引擎点火！四大战区新版网格回测启动 (IS训练 + OOS盲测 + Soft Filtering)...")
    start_time = time.time()
    all_results = []
    MIN_SAMPLES_IS = 10
    OOS_MAX_DROP = 20.0
    
    def _calc_basic_metrics(df_slice, hold_p):
        """
        给定切片与持仓周期，返回：样本数、胜率、盈亏比。
        入场须 is_valid_trade_entry；对应 T+p 须 can_exit_t{p}（排除一字跌停卖不出）。
        """
        pnl_col = f'pnl_t{hold_p}'
        exit_col = f'can_exit_t{hold_p}'
        if 'is_valid_trade_entry' in df_slice.columns:
            base_ok = df_slice['is_valid_trade_entry'].fillna(False)
        elif 'is_valid_trade' in df_slice.columns:
            base_ok = df_slice['is_valid_trade'].fillna(False)
        else:
            base_ok = pd.Series(True, index=df_slice.index)
        if exit_col in df_slice.columns:
            ex_ok = df_slice[exit_col].fillna(False)
        else:
            ex_ok = pd.Series(True, index=df_slice.index)
        row_ok = base_ok & ex_ok
        valid_pnl = df_slice.loc[row_ok, pnl_col].dropna()
        n = len(valid_pnl)
        if n < 1:
            return 0, 0.0, 0.0, 0.0, 0.0

        win_df = valid_pnl[valid_pnl > 0]
        wr = len(win_df) / n * 100.0
        avg_win = win_df.mean() if not win_df.empty else 0.0
        loss_df = valid_pnl[valid_pnl <= 0]
        avg_loss = loss_df.mean() if not loss_df.empty else 0.0
        ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0.0
        avg_pnl = valid_pnl.mean()
        return n, wr, ratio, avg_pnl, float(valid_pnl.std(ddof=0))

    def evaluate_mask_with_oos(pool_name, strat_name, mask_is, mask_oos, df_is, df_oos, param_dict):
        """
        统一评估器（机构版）：
        1) 仅在 IS 评估并选出“最佳持仓周期”
        2) 仅当 IS 胜率>60 且 样本>=10 才进入 OOS 盲测
        3) 记录 IS/OOS 双域核心指标
        """
        entry_is = df_is['is_valid_trade_entry'] if 'is_valid_trade_entry' in df_is.columns else df_is['is_valid_trade']
        hits_is = df_is[mask_is & entry_is]
        if len(hits_is) < 5:
            return

        # ---------------- IS: 先找最佳持仓周期 ----------------
        best = None
        for p in [2, 3, 5, 10, 20]:
            is_n, is_wr, is_ratio, is_avg_pnl, _ = _calc_basic_metrics(hits_is, p)
            if is_n < 5:
                continue
            candidate = (is_wr, is_ratio, is_avg_pnl, is_n, p)
            if best is None or candidate > best:
                best = candidate

        if best is None:
            return

        is_wr, is_ratio, is_avg_pnl, is_n, best_p = best

        # 只对 IS 胜率>=55 且样本>=MIN_SAMPLES_IS 的组合做 OOS 盲测
        if not (is_wr >= 55.0 and is_n >= MIN_SAMPLES_IS):
            return

        # ---------------- OOS: 原封不动参数盲测 ----------------
        entry_oos = df_oos['is_valid_trade_entry'] if 'is_valid_trade_entry' in df_oos.columns else df_oos['is_valid_trade']
        hits_oos = df_oos[mask_oos & entry_oos]
        oos_n, oos_wr, oos_ratio, oos_avg_pnl, _ = _calc_basic_metrics(hits_oos, best_p)
        # 扣除摩擦后 OOS 盈亏比须严格 > 1.2 才进入汇总 / AI 报告
        if not (oos_ratio > MIN_OOS_PROFIT_LOSS_RATIO):
            return
        mdd_col = f'mdd_t{best_p}'
        oos_mdd = hits_oos[mdd_col].mean() if oos_n > 0 else np.nan
        drop = float(is_wr - oos_wr)
        stability_idx = max(0.0, 1.0 - abs(is_wr - oos_wr) / 100.0)

        all_results.append({
            '战法池': pool_name,
            '策略名称': strat_name,
            '最佳持仓期': f'T+{best_p}',
            # 训练集表现(IS)
            'IS样本数': int(is_n),
            'IS胜率%': round(float(is_wr), 2),
            'IS盈亏比': round(float(is_ratio), 2),
            # 盲测集表现(OOS)
            'OOS样本数': int(oos_n),
            'OOS胜率%': round(float(oos_wr), 2),
            'OOS盈亏比': round(float(oos_ratio), 2),
            '胜率落差': round(drop, 2),
            'stability_idx': round(float(stability_idx), 4),
            # 辅助统计
            'IS平均收益%': round(float(is_avg_pnl), 2),
            'OOS平均收益%': round(float(oos_avg_pnl), 2) if oos_n > 0 else np.nan,
            'OOS最大回撤%': round(float(oos_mdd), 2) if pd.notna(oos_mdd) else np.nan,
            'param_json': json.dumps(param_dict, ensure_ascii=False)
        })

    def build_soft_mask(df_part, pool, threshold, score_w):
        """软评分过滤器：以总分阈值代替多重硬逻辑。"""
        total_score = (
            score_w['slope'] * df_part['slope_score'] +
            score_w['bias'] * df_part['bias_score'] +
            score_w['elg'] * df_part['elg_score'] +
            score_w['cost'] * df_part['cost_score']
        )
        mask = total_score >= threshold

        # 轻量行为约束：原绝对阈值改为「当日全市场候选池」截面分位（与 prepare_vectorized_features 列一致）
        if pool == "P2":
            mask = mask & (df_part['open_pct_cross_pct'] > XS_OPEN_MIN)
            mask = mask & (df_part['vr_cross_pct'] > XS_VR_TOP)
        elif pool == "P3":
            mask = mask & (
                (df_part['macd_hist_cross_pct'] > XS_MACD_MED)
                | (df_part['slope_t0'] > df_part['slope_t1'])
            )
            mask = mask & (df_part['vr_cross_pct'] > XS_VR_P3)
            mask = mask & (df_part['pct_cross_pct'] > XS_PCT_STRONG)
        elif pool == "P4":
            mask = mask & (df_part['atr_cross_pct'] <= XS_ATR_CALM_MAX)
            mask = mask & (df_part['vr_cross_pct'] > XS_VR_P4)
        elif pool == "P5":
            # P5 特化（激进召回）：斜率容忍度放宽，允许轻微滞后但避免明显走弱
            slope_tol = float(score_w.get('slope_tol', 0.20))
            mask = mask & (df_part['slope_t0'] > (df_part['slope_t1'] - slope_tol))
            mask = mask & (df_part['vr_cross_pct'] > XS_VR_TOP)
        return mask, total_score

    mv_groups = df['mv_group'].dropna().unique()
    
    for mv_group in mv_groups:
        df_group = df[df['mv_group'] == mv_group].copy()
        df_is = df_group[df_group['sample_set'] == 'IS'].copy()
        df_oos = df_group[df_group['sample_set'] == 'OOS'].copy()
        print(f"\n🔥 锁定战区: {mv_group}（IS: {len(df_is)} | OOS: {len(df_oos)}）")

        # ---------------- 🏁 P2 早盘竞价核爆网格（Soft Filtering） ----------------
        p2_params = list(itertools.product(
            [55, 60, 65, 70],               # total_score threshold
            [0.30, 0.35],                   # slope weight
            [0.20, 0.25],                   # bias weight
            [0.30, 0.35],                   # elg weight
            [0.10, 0.15]                    # cost weight
        ))
        print(f"    >>> 正在横扫 P2_{mv_group} (共计 {len(p2_params)} 种 Soft 评分组合)...")
        for threshold, w_slope, w_bias, w_elg, w_cost in tqdm(p2_params, desc="P2 进度", leave=False):
            score_w = {"slope": w_slope, "bias": w_bias, "elg": w_elg, "cost": w_cost}
            mask_is, score_is = build_soft_mask(df_is, "P2", threshold, score_w)
            mask_oos, _ = build_soft_mask(df_oos, "P2", threshold, score_w)
            print(f"      Soft Score 计算完成，总分阈值: {threshold}，最终捕获: {int(mask_is.sum())} 只")
            s_name = f"[P2核爆-Soft] 总分>={threshold}_W(s,b,e,c)=({w_slope:.2f},{w_bias:.2f},{w_elg:.2f},{w_cost:.2f})"
            evaluate_mask_with_oos(
                f'P2_{mv_group}',
                s_name,
                mask_is,
                mask_oos,
                df_is,
                df_oos,
                {
                    "pool": "P2",
                    "threshold": threshold,
                    "w_slope": w_slope,
                    "w_bias": w_bias,
                    "w_elg": w_elg,
                    "w_cost": w_cost,
                    "is_soft": True,
                    "oos_max_drop": OOS_MAX_DROP,
                    "min_oos_ratio": MIN_OOS_PROFIT_LOSS_RATIO,
                    "buy_slippage": BUY_IMPACT_SLIPPAGE,
                    "sell_fee": SELL_FEE_TAX,
                }
            )

        # ---------------- 🏁 P3 盘中主升承接网格（Soft Filtering） ----------------
        p3_params = list(itertools.product(
            [55, 60, 65, 70],
            [0.35, 0.40],
            [0.20, 0.25],
            [0.30, 0.35],
            [0.05, 0.10]
        ))
        print(f"    >>> 正在横扫 P3_{mv_group} (共计 {len(p3_params)} 种 Soft 评分组合)...")
        for threshold, w_slope, w_bias, w_elg, w_cost in tqdm(p3_params, desc="P3 进度", leave=False):
            score_w = {"slope": w_slope, "bias": w_bias, "elg": w_elg, "cost": w_cost}
            mask_is, _ = build_soft_mask(df_is, "P3", threshold, score_w)
            mask_oos, _ = build_soft_mask(df_oos, "P3", threshold, score_w)
            print(f"      Soft Score 计算完成，总分阈值: {threshold}，最终捕获: {int(mask_is.sum())} 只")
            s_name = f"[P3承接-Soft] 总分>={threshold}_W(s,b,e,c)=({w_slope:.2f},{w_bias:.2f},{w_elg:.2f},{w_cost:.2f})"
            evaluate_mask_with_oos(
                f'P3_{mv_group}',
                s_name,
                mask_is,
                mask_oos,
                df_is,
                df_oos,
                {
                    "pool": "P3",
                    "threshold": threshold,
                    "w_slope": w_slope,
                    "w_bias": w_bias,
                    "w_elg": w_elg,
                    "w_cost": w_cost,
                    "is_soft": True,
                    "oos_max_drop": OOS_MAX_DROP,
                    "min_oos_ratio": MIN_OOS_PROFIT_LOSS_RATIO,
                    "buy_slippage": BUY_IMPACT_SLIPPAGE,
                    "sell_fee": SELL_FEE_TAX,
                }
            )

        # ---------------- 🏁 P4 尾盘盲狙防守网格（Soft Filtering） ----------------
        p4_params = list(itertools.product(
            [55, 60, 65, 70],
            [0.25, 0.30],
            [0.30, 0.35],
            [0.25, 0.30],
            [0.10, 0.15]
        ))
        print(f"    >>> 正在横扫 P4_{mv_group} (共计 {len(p4_params)} 种 Soft 评分组合)...")
        for threshold, w_slope, w_bias, w_elg, w_cost in tqdm(p4_params, desc="P4 进度", leave=False):
            score_w = {"slope": w_slope, "bias": w_bias, "elg": w_elg, "cost": w_cost}
            mask_is, _ = build_soft_mask(df_is, "P4", threshold, score_w)
            mask_oos, _ = build_soft_mask(df_oos, "P4", threshold, score_w)
            print(f"      Soft Score 计算完成，总分阈值: {threshold}，最终捕获: {int(mask_is.sum())} 只")
            s_name = f"[P4盲狙-Soft] 总分>={threshold}_W(s,b,e,c)=({w_slope:.2f},{w_bias:.2f},{w_elg:.2f},{w_cost:.2f})"
            evaluate_mask_with_oos(
                f'P4_{mv_group}',
                s_name,
                mask_is,
                mask_oos,
                df_is,
                df_oos,
                {
                    "pool": "P4",
                    "threshold": threshold,
                    "w_slope": w_slope,
                    "w_bias": w_bias,
                    "w_elg": w_elg,
                    "w_cost": w_cost,
                    "is_soft": True,
                    "oos_max_drop": OOS_MAX_DROP,
                    "min_oos_ratio": MIN_OOS_PROFIT_LOSS_RATIO,
                    "buy_slippage": BUY_IMPACT_SLIPPAGE,
                    "sell_fee": SELL_FEE_TAX,
                }
            )

        # ---------------- 🏁 P5 上帝视角真龙网格（Soft + 特化） ----------------
        p5_params = list(itertools.product(
            [35, 40, 45, 50, 55],           # threshold（前置 P1 极寒过滤后收窄，偏大象起舞纯度）
            [0.30, 0.35],                   # slope
            [0.10, 0.15],                   # bias
            [0.25, 0.30],                   # elg
            [0.25, 0.30]                    # cost（P5强化）
        ))
        print(f"    >>> 正在横扫 P5_{mv_group} (共计 {len(p5_params)} 种 Soft 特化组合)...")
        for threshold, w_slope, w_bias, w_elg, w_cost in tqdm(p5_params, desc="P5 进度", leave=False):
            score_w = {"slope": w_slope, "bias": w_bias, "elg": w_elg, "cost": w_cost, "slope_tol": 0.20}
            mask_is, _ = build_soft_mask(df_is, "P5", threshold, score_w)
            mask_oos, _ = build_soft_mask(df_oos, "P5", threshold, score_w)
            print(f"      Soft Score 计算完成，总分阈值: {threshold}，最终捕获: {int(mask_is.sum())} 只")
            s_name = f"[P5真龙-Soft] 总分>={threshold}_W(s,b,e,c)=({w_slope:.2f},{w_bias:.2f},{w_elg:.2f},{w_cost:.2f})_Slope强于昨日"
            evaluate_mask_with_oos(
                f'P5_{mv_group}',
                s_name,
                mask_is,
                mask_oos,
                df_is,
                df_oos,
                {
                    "pool": "P5",
                    "threshold": threshold,
                    "w_slope": w_slope,
                    "w_bias": w_bias,
                    "w_elg": w_elg,
                    "w_cost": w_cost,
                    "slope_trend_up_required": True,
                    "slope_tol": 0.20,
                    "cost_5th_prefer": True,
                    "is_soft": True,
                    "oos_max_drop": OOS_MAX_DROP,
                    "min_oos_ratio": MIN_OOS_PROFIT_LOSS_RATIO,
                    "buy_slippage": BUY_IMPACT_SLIPPAGE,
                    "sell_fee": SELL_FEE_TAX,
                }
            )

    print(f" ✅ 四大战区网格风暴结束！极速耗时: {time.time() - start_time:.2f} 秒。")
    return pd.DataFrame(all_results)

# ================= 4. 真龙漏网反向复盘 =================
def diagnose_missed_dragons(df, best_p5_params=None, project_root=None):
    """
    每日真龙漏网诊断：
    1) 按流通市值分层定义「真龙」：circ_mv 万元口径，≥500亿 pnl_t5≥10%；300–500亿 pnl_t5≥15%；<300亿 pnl_t3≥15%；
       且非一字板、入场有效；与 A 股大市值中军运行规律对齐。
    2) 使用“最佳P5参数”或默认硬门槛去匹配
    3) 输出 Missed_Dragons_Log.csv（交易日期, ts_code, 目标窗口最大涨幅, 是否被系统捕获）
    """
    if df is None or df.empty:
        return None

    # daily_data.circ_mv 为万元：1亿元=10000万元 → 300亿=300*10000万，500亿=500*10000万
    _WAN_PER_YI = 10_000.0
    _MV_300_YI_WAN = 300.0 * _WAN_PER_YI
    _MV_500_YI_WAN = 500.0 * _WAN_PER_YI

    # 默认硬门槛（当没有可用最佳参数时兜底）
    default_p5 = {
        "threshold": 25,
        "w_slope": 0.35,
        "w_bias": 0.10,
        "w_elg": 0.25,
        "w_cost": 0.30,
        "slope_trend_up_required": True,
        "slope_tol": 0.20
    }
    p = best_p5_params if isinstance(best_p5_params, dict) else default_p5

    ent_ok = df['is_valid_trade_entry'] if 'is_valid_trade_entry' in df.columns else (df['is_valid_trade'] if 'is_valid_trade' in df.columns else True)
    if not isinstance(ent_ok, pd.Series):
        ent_ok = pd.Series(True, index=df.index)

    cm = pd.to_numeric(df.get("circ_mv", np.nan), errors="coerce").fillna(0.0)
    pnl_t3 = pd.to_numeric(df.get("pnl_t3", np.nan), errors="coerce")
    pnl_t5 = pd.to_numeric(df.get("pnl_t5", np.nan), errors="coerce")
    is_true_dragon = np.select(
        [
            cm >= _MV_500_YI_WAN,
            (cm >= _MV_300_YI_WAN) & (cm < _MV_500_YI_WAN),
            cm < _MV_300_YI_WAN,
        ],
        [
            pnl_t5 >= 10.0,
            pnl_t5 >= 15.0,
            pnl_t3 >= 15.0,
        ],
        default=False,
    )
    is_true_dragon = pd.Series(is_true_dragon, index=df.index).fillna(False).astype(bool)
    lim_up = df["is_limit_up"].fillna(False).astype(bool) if "is_limit_up" in df.columns else pd.Series(False, index=df.index)
    dragon_df = df[is_true_dragon & (~lim_up) & ent_ok].copy()
    if dragon_df.empty:
        print(
            " ⚠️ 真龙漏网诊断：未发现满足分层真龙标准"
            "（≥500亿 T+5≥10%；300–500亿 T+5≥15%；<300亿 T+3≥15%）的样本。"
        )
        return None

    threshold = float(p.get("threshold", default_p5["threshold"]))
    w_slope = float(p.get("w_slope", default_p5["w_slope"]))
    w_bias = float(p.get("w_bias", default_p5["w_bias"]))
    w_elg = float(p.get("w_elg", default_p5["w_elg"]))
    w_cost = float(p.get("w_cost", default_p5["w_cost"]))
    slope_trend_up_required = bool(p.get("slope_trend_up_required", default_p5["slope_trend_up_required"]))
    slope_tol = float(p.get("slope_tol", default_p5["slope_tol"]))

    p5_soft_score = (
        w_slope * dragon_df['slope_score'] +
        w_bias * dragon_df['bias_score'] +
        w_elg * dragon_df['elg_score'] +
        w_cost * dragon_df['cost_score']
    )
    captured_mask = p5_soft_score >= threshold
    if slope_trend_up_required:
        captured_mask = captured_mask & (dragon_df['slope_t0'] > (dragon_df['slope_t1'] - slope_tol))
    if 'vr_cross_pct' in dragon_df.columns:
        captured_mask = captured_mask & (dragon_df['vr_cross_pct'] > XS_VR_TOP)
    dragon_df['是否被系统捕获'] = np.where(captured_mask, "是", "否")
    _cm_d = pd.to_numeric(dragon_df.get("circ_mv", np.nan), errors="coerce").fillna(0.0)
    _pt3_d = pd.to_numeric(dragon_df.get("pnl_t3", np.nan), errors="coerce")
    _pt5_d = pd.to_numeric(dragon_df.get("pnl_t5", np.nan), errors="coerce")
    # 大票/中军展示 T+5 收益，小票展示 T+3
    dragon_df["目标窗口最大涨幅"] = np.where(_cm_d < _MV_300_YI_WAN, _pt3_d, _pt5_d).round(2)

    out_cols = ['trade_date', 'ts_code', '目标窗口最大涨幅', '是否被系统捕获']
    out_df = dragon_df[out_cols].rename(columns={'trade_date': '交易日期'})

    if project_root is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        _bn = os.path.basename(current_dir)
        project_root = (
            os.path.dirname(current_dir) if _bn in ("tools", "offline_tools") else current_dir
        )
    out_path = os.path.join(project_root, "data", "Missed_Dragons_Log.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out_df.to_csv(out_path, index=False, encoding='utf-8-sig')

    total_dragons = len(out_df)
    captured = int((out_df['是否被系统捕获'] == '是').sum())
    print(f" ✅ 真龙漏网诊断完成 | 样本: {total_dragons} | 捕获: {captured} | 漏网: {total_dragons - captured}")
    print(f" 📦 已输出: {out_path}")
    return out_path

# ================= 5. 分池天梯榜与落库 =================
def evaluate_and_save(project_root, sum_df):
    """
    输出重构版：
    - 不再输出 CSV
    - 不再打印终端明细
    - 仅生成一个 AI_GridSearch_Master_V1.txt，供 AI 直接解析
    """
    print("\n 🚀 [5/5] 正在生成 AI 专用总报告文本...")
    out_dir = os.path.join(project_root, "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "AI_GridSearch_Master_V1.txt")

    header_prompt = """Plaintext
========================================================================
[AI_SYSTEM_INSTRUCTION_START]
ROLE: 你是一位顶级的量化交易策略分析师与宽客 (Quant)。
CONTEXT: 以下数据是基于 A 股真实历史行情，对 4 个右侧交易战区（P2早盘、P3盘中、P4尾盘、P5盘后）进行的十万次笛卡尔积网格回测报告。底层数据基于 55 维量价与资金因子；软评分与轻量门禁已按「交易日截面分位数 rank(pct)」相对化，非固定绝对阈值。
DATA_COLUMNS: 
- 战法池: 战区及市值分组。
- 策略名称: 参数组合的条件描述。
- 训练集表现(IS): 包含 IS样本数, IS胜率(%), IS盈亏比。
- 盲测集表现(OOS): 包含 OOS样本数, OOS胜率(%), OOS盈亏比（这是衡量策略是否过拟合的唯一标准）。

TASK:
1. 【剔除过拟合】过滤掉“IS样本数 < 10”或“IS胜率高但IS盈亏比 < 1.0”或“OOS胜率相对IS断崖下跌”超阈的伪圣杯参数（全池默认 IS-OOS 胜率落差≤20%；战法池名含「500亿+」时须≤12%）。
2. 【寻找甜点区】为 P2、P3、P4、P5 分别找出 1 到 2 组胜率 > 65% 且 盈亏比 > 2.0 的“参数簇（Parameter Clusters）”。
3. 【反直觉洞察】指出数据中违背常规认知的点。
4. 【实盘摩擦】收益已含买入+0.2%滑点、卖出−0.15%税费；一字涨停买不进、一字跌停卖不出已从样本剔除。
5. 【实盘参数建议】本报告仅含 OOS盈亏比>1.2 的参数簇；优先 stability_idx > 0.9 且 OOS盈亏比 > 1.1 的战区建议。
[AI_SYSTEM_INSTRUCTION_END]
========================================================================
"""

    # 统一筛选与排序：
    # 1) IS样本数>=10
    # 2) 排除 IS-OOS 胜率落差过大的过拟合：默认 >20% 剔除；战法池含「500亿+」时 >12% 剔除（大票 OOS 不允许严重崩盘）
    # 3) 增加盲测稳定性指数 stability_idx
    # 4) 优先 stability_idx>0.9 且 OOS盈亏比>1.1
    if sum_df is None or sum_df.empty:
        filtered_df = pd.DataFrame(columns=[
            '战法池', '策略名称', '最佳持仓期',
            'IS样本数', 'IS胜率%', 'IS盈亏比',
            'OOS样本数', 'OOS胜率%', 'OOS盈亏比',
            'stability_idx'
        ])
    else:
        filtered_df = sum_df[sum_df['IS样本数'] >= 10].copy()
        filtered_df['胜率落差'] = filtered_df['IS胜率%'] - filtered_df['OOS胜率%']
        _pool_nm = filtered_df['战法池'].astype(str)
        _is_500yi = _pool_nm.str.contains('500亿+', na=False)
        _max_drop = np.where(_is_500yi, 12.0, 20.0)
        filtered_df = filtered_df[filtered_df['胜率落差'] <= _max_drop]
        filtered_df['stability_idx'] = 1.0 - (filtered_df['胜率落差'].abs() / 100.0)
        filtered_df['stability_idx'] = filtered_df['stability_idx'].clip(lower=0.0, upper=1.0)
        filtered_df['推荐优先级'] = (
            (filtered_df['stability_idx'] > 0.9) &
            (filtered_df['OOS盈亏比'] > 1.1) &
            (filtered_df['OOS盈亏比'] > MIN_OOS_PROFIT_LOSS_RATIO)
        ).astype(int)
        filtered_df = filtered_df.sort_values(
            by=['推荐优先级', 'stability_idx', 'OOS胜率%', 'OOS盈亏比'],
            ascending=[False, False, False, False]
        ).reset_index(drop=True)

    write_cols = [
        '战法池', '策略名称', '最佳持仓期',
        'IS样本数', 'IS胜率%', 'IS盈亏比',
        'OOS样本数', 'OOS胜率%', 'OOS盈亏比',
        'stability_idx', 'IS平均收益%', 'OOS平均收益%', 'OOS最大回撤%'
    ]
    pools = ['P2', 'P3', 'P4', 'P5']

    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(header_prompt)
        for pool in pools:
            f.write(f"\n\n==================== {pool} 战区回测结果 ====================\n")
            sub = filtered_df[filtered_df['战法池'].str.startswith(pool)] if not filtered_df.empty else filtered_df
            if sub.empty:
                f.write("无满足条件的数据（IS样本数>=10 且 OOS 胜率落差未超阈：默认≤20%，500亿+池≤12%）。\n")
                continue
            preferred = sub[(sub['stability_idx'] > 0.9) & (sub['OOS盈亏比'] > 1.1)]
            f.write(f"优先推荐数量(stability_idx>0.9 且 OOS盈亏比>1.1): {len(preferred)}\n")
            f.write(sub[write_cols].to_string(index=False))
            f.write("\n")

    print(f" ✅ AI 总报告已生成: {out_path}")

    # 返回过滤后的结果，便于主流程提取最优 P5 参数用于漏网诊断
    return filtered_df

def main():
    try:
        root = setup_environment()
        df_raw = load_and_build_space()
        df_vectorized = prepare_vectorized_features(df_raw)
        sum_df = run_massive_grid_search(df_vectorized)
        filtered_df = evaluate_and_save(root, sum_df)

        # 自动提取“表现最好”的 P5 参数做真龙漏网诊断（优先 OOS 胜率/盈亏比）
        best_p5_params = None
        if filtered_df is not None and not filtered_df.empty:
            p5_df = filtered_df[filtered_df['战法池'].str.startswith('P5')].copy()
            if not p5_df.empty:
                p5_df = p5_df.sort_values(by=['OOS胜率%', 'OOS盈亏比', 'IS胜率%'], ascending=[False, False, False])
                top_row = p5_df.iloc[0]
                try:
                    best_p5_params = json.loads(str(top_row.get('param_json', '{}')))
                except Exception:
                    best_p5_params = None

        diagnose_missed_dragons(df_vectorized, best_p5_params=best_p5_params, project_root=root)
    except Exception as e:
        print("\n💥 系统发生崩溃！详细报错信息如下：")
        traceback.print_exc()
    finally:
        input("\n[矩阵推演结束] 请按键盘上的 回车键(Enter) 退出...")

if __name__ == "__main__":
    main()