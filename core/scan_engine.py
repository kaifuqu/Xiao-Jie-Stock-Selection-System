# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.7 - 极速分发引擎（P1–P5 全池扫描中枢）

【P1 初筛与多维分项平滑分（与 pool_manager 同口径）】
- 市值：流通市值低于 get_p1_select_min_circ_mv_wan（config/constants）所对应亿元门槛的标的，在战法循环前直接剔除；
  因循环开头已计入漏斗，此处配合 _rollback_scan_funnel_for_skipped_stock 回滚诊断计数，避免虚高。
- 打分：对通过市值的标的，在 rt 补齐量比与真实换手后，调用 score_calibration.compute_p1_multi_dim_smooth_score；
  行业 PE 分位、动态行业贝塔、板块排名与近 5 日均换手等参数与 build_p1_pool 一致，保证扫描侧「基因分」与底仓池同源。
- 放行线：pass_line 来自 get_p1_regime_thresholds(regime)（config.yaml strategies.p1.profiles，默认 50），
  低于及格线则 reason 记为「平滑得分不达标」，跳过 P2–P5 战法匹配；danger_sell / danger_buy 风控仍执行。
- P3–P5 综合分仍可加权 capital_resonance_score；P1 初筛不依赖该列。

【审查要点】
1. df/rt 对齐：merge_daily_with_realtime + precompute_indicators，保证 bias_20、macd_bar 等与现价一致。
2. P4/P5 门禁诊断：_infer_golden_gate_reason 按池拆分（P5 不使用 tail_vol_ratio）。
3. 单票 try/except：个股数据残缺时跳过该票，不拖垮整轮扫描。
4. P3/P4：右侧直通车（Momentum Fast-Lane）可并入非底仓极端动量标的；命中战法时放宽黄金门禁，避免超高乖离龙头被误杀。
5. danger_buy：叠加 breakout_vwap_eps 分时 VWAP 校验；高位回落低于当日 VWAP 超阈值则仅写入「禁买」提示表，**不**自动写入黑名单（底层零干预）。
6. 综合分稳定化：截面 Rank（涨幅/量比/真换手/主力推力）+ burst 软封顶 + surge 对数压缩 + 性格乘子几何融合，
   减轻换手与量比重复放大；入表列仍经 _ensure_pool_table_row_contract，不改 UI 表结构。
"""

# Standard library
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

# Third-party
import numpy as np
import pandas as pd

# Local modules
import constants

def _p1_scan_min_circ_mv_yi() -> float:
    try:
        from core.config_manager import get_p1_select_min_circ_mv_wan

        return float(get_p1_select_min_circ_mv_wan()) / 10000.0
    except Exception:
        return float(getattr(constants, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000)) / 10000.0


def _safe_notify_system_alert(title: str, detail: str, category: str, dedup_key: str) -> None:
    """扫描链统一运维告警入口，避免各处散落重复 import / 重复格式化。"""
    try:
        from core.notification_gateway import notify_wechat_system_alert

        notify_wechat_system_alert(
            title=title,
            detail=detail,
            category=category,
            dedup_key=dedup_key,
        )
    except Exception as e:
        logging.debug("_safe_notify_system_alert 跳过: %s", e)

try:
    from core.strategies.score_calibration import compute_p1_multi_dim_smooth_score
except ImportError:

    def compute_p1_multi_dim_smooth_score(*args, **kwargs):
        return 0.0, False, "平滑得分不达标", {}


try:
    from core.strategies.strat_base import strict_golden_burst_ok
except ImportError as e:
    logging.warning(f"scan_engine 无法导入 strat_base 门禁: {e}")

    def strict_golden_burst_ok(df, rt, pool_key=None):
        return True

try:
    from core.indicator_calc import precompute_indicators, merge_daily_with_realtime
except ImportError as e:
    logging.warning(f"scan_engine 无法导入 indicator_calc: {e}")

    def precompute_indicators(df):
        return df

    def merge_daily_with_realtime(df, rt):
        return df, False

try:
    from core.strategies.score_calibration import (
        percentile_ranks,
        compress_surge_bonus,
        dampen_burst_by_extremes,
        burst_soft_cap,
        build_rank_lookup,
        neutral_ranks,
        personality_liquidity_blend,
    )
except ImportError:

    def percentile_ranks(df, cols):
        out = df.copy()
        for c in cols:
            if c in out.columns:
                out[c + "_r"] = 0.5
        return out

    def compress_surge_bonus(surge, linear_cap=10.0, asymptote=16.0):
        return float(surge or 0.0)

    def dampen_burst_by_extremes(burst, p1_gene, vr_r, pct_r, trf_r, main_r):
        return float(burst or 0.0)

    def burst_soft_cap(burst, soft=94.0, hard=102.0):
        return float(burst or 0.0)

    def build_rank_lookup(cs_df):
        return {}

    def neutral_ranks():
        return {"pct_r": 0.5, "vol_ratio_r": 0.5, "turnover_f_r": 0.5, "main_ratio_r": 0.5}

    def personality_liquidity_blend(trn_multi, vr_rank):
        t = float(np.clip(trn_multi, 0.85, 1.15))
        vr = float(np.clip(vr_rank, 0.0, 1.0))
        v = float(np.interp(vr, [0.05, 0.35, 0.55, 0.75, 0.95], [0.90, 0.97, 1.0, 1.06, 1.12]))
        return float(np.clip(np.sqrt(max(t, 1e-9) * max(v, 1e-9)), 0.85, 1.15))


from core.danger_signal_utils import would_trigger_danger_sell
from core.stock_name_utils import normalize_stock_display_name

try:
    from core.strategies.fund_mv_utils import (
        effective_turnover_rate_f,
        infer_turnover_rate_f_pct,
        series_effective_turnover_f_daily,
        compute_market_contraction_context,
        adaptive_relaxed_golden_gate_ok,
    )
except ImportError:

    def infer_turnover_rate_f_pct(vol_hand, close, circ_mv_wan):
        if vol_hand <= 0 or close <= 0 or circ_mv_wan <= 0:
            return 0.0
        return vol_hand * close / circ_mv_wan

    def effective_turnover_rate_f(rt, y, close_live):
        r = rt or {}
        t = _safe_float(r.get("turnover_rate_f"), 0.0)
        if t > 0:
            return t
        if hasattr(y, "get"):
            t2 = _safe_float(y.get("turnover_rate_f"), 0.0)
            if t2 > 0:
                return t2
        vh = _safe_float(r.get("volume"), 0.0) / 100.0
        if vh <= 0 and hasattr(y, "get"):
            vh = _safe_float(y.get("vol"), 0.0)
        cm = _safe_float(r.get("circ_mv"), 0.0)
        if cm <= 0 and hasattr(y, "get"):
            cm = _safe_float(y.get("circ_mv"), 0.0)
        if cm <= 0 and hasattr(y, "get"):
            cm = _safe_float(y.get("total_mv"), 0.0) * 0.6
        return infer_turnover_rate_f_pct(vh, _safe_float(close_live, 0.0), cm)

    def series_effective_turnover_f_daily(df):
        if df is None or df.empty:
            return pd.Series(dtype=float)
        if "turnover_rate_f" in df.columns:
            return pd.to_numeric(df["turnover_rate_f"], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    def compute_market_contraction_context(base_items, rt_map=None):
        return {"score": 0.0, "adaptive_reason": "fund_mv_utils 不可用"}

    def adaptive_relaxed_golden_gate_ok(pool_key, df, rt, market_contraction_score):
        return False


# ================= 0. 全局物理底座寻址雷达 =================
def get_project_root():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for _ in range(4): 
        if os.path.exists(os.path.join(current_dir, "config.yaml")):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    return os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = get_project_root()

try:
    from core.runtime_data_paths import (
        ensure_runtime_data_layout,
        path_blacklist_json,
        path_intraday_snapshots_json,
        path_wash_metrics_json,
    )
except ImportError:
    def ensure_runtime_data_layout():
        os.makedirs(os.path.join(PROJECT_ROOT, "data"), exist_ok=True)

    def path_blacklist_json():
        return os.path.join(PROJECT_ROOT, "data", "blacklist.json")

    def path_intraday_snapshots_json():
        return os.path.join(PROJECT_ROOT, "data", "intraday_snapshots.json")

    def path_wash_metrics_json():
        return os.path.join(PROJECT_ROOT, "data", "wash_metrics_history.json")

# ==================== 1. 跨部门资源调配 (任督二脉贯通) ====================
import constants
from data.db_core import (
    save_df_to_sql,
    get_latest_sector_ranking,
    get_stock_industry,
    get_all_basic_industry,
    load_p1_cache,
    get_latest_daily_data_trade_date_yyyymmdd,
)
from data.api_fetcher import fetch_realtime_batch

BJ_TZ = timezone(timedelta(hours=8)) 


def _prev_calendar_bizday_yyyymmdd(today_yyyymmdd: str) -> str:
    """无交易所日历时的兜底：按工作日回退到上一个自然工作日。"""
    s = str(today_yyyymmdd or "").strip().replace("-", "")[:8]
    if len(s) != 8 or (not s.isdigit()):
        return ""
    try:
        dt = datetime.strptime(s, "%Y%m%d")
    except ValueError:
        return ""
    dt = dt - timedelta(days=1)
    while dt.weekday() >= 5:  # 5=Sat, 6=Sun
        dt = dt - timedelta(days=1)
    return dt.strftime("%Y%m%d")


def _expected_latest_daily_anchor_for_intraday(today_yyyymmdd: str) -> str:
    """
    盘中/盘后扫描的最低日线新鲜度：
    - 优先使用交易所开市日历得到「今日之前最近一个交易日」；
    - 失败时降级到自然工作日回退（节假日可能偏松，但不会比“空一天”更宽）。
    """
    try:
        from core.p5_morning_validation import sse_prev_open_trade_date_before

        prev_td = str(sse_prev_open_trade_date_before(today_yyyymmdd) or "").strip()
        if len(prev_td) == 8 and prev_td.isdigit():
            return prev_td
    except Exception:
        pass
    return _prev_calendar_bizday_yyyymmdd(today_yyyymmdd)


def _try_auto_sync_recent_days_for_scan(progress_callback=None) -> bool:
    """
    扫描前轻量自愈：尝试执行近期增量同步，成功返回 True（异常/失败返回 False）。
    仅用于扫描入口兜底，不替代 daemon 晚间链。
    """
    try:
        from data import data_fetcher
    except Exception as e:
        logging.warning("扫描前自动增量同步不可用（导入 data_fetcher 失败）: %s", e)
        return False
    if getattr(data_fetcher, "pro", None) is None:
        logging.warning("扫描前自动增量同步跳过：Tushare pro 未初始化")
        return False
    try:
        if progress_callback:
            try:
                progress_callback("♻️ 检测到日线数据可能未对齐，正在自动增量同步最近交易日数据…")
            except Exception:
                pass
        sync_ret = data_fetcher.sync_recent_days(
            days=3,
            status_callback=lambda m: logging.info("[scan:auto-sync] %s", str(m)),
        )
        if isinstance(sync_ret, pd.DataFrame):
            return not sync_ret.empty
        if isinstance(sync_ret, (list, tuple, set, dict)):
            return len(sync_ret) > 0
        if sync_ret is None:
            return False
        return bool(sync_ret)
    except Exception as e:
        logging.warning("扫描前自动增量同步失败: %s", e)
        return False

class DummyEngine:
    def run_all(self, df, rt): return {}
    def evaluate(self, df, rt): return {}

try:
    from core.strategies.strat_p2_auction import P2Auction
    p2_engine = P2Auction()
except Exception as e:
    logging.warning(f"P2战法加载失败: {e}")
    p2_engine = DummyEngine()

try:
    from core.strategies.strat_p3_intraday import P3Intraday
    p3_engine = P3Intraday()
except Exception as e:
    logging.warning(f"P3战法加载失败: {e}")
    p3_engine = DummyEngine()

try:
    from core.strategies.strat_p4_tail import P4Tail
    p4_engine = P4Tail()
except Exception as e:
    logging.warning(f"P4战法加载失败: {e}")
    p4_engine = DummyEngine()

# 【主干解耦】P5 盘后真龙池独立引擎，禁止与 P4 共用同一实例；调度循环须为 ('p5', p5_engine)。
try:
    from core.strategies.strat_p5_postmarket import P5Postmarket
    p5_engine = P5Postmarket()
except Exception as e:
    logging.warning(f"P5战法加载失败: {e}")
    p5_engine = DummyEngine()

try:
    from core.strategies.strat_golden_10 import GoldenTenStrategies
    golden_engine = GoldenTenStrategies()
except Exception as e:
    logging.warning(f"金共振战法加载失败: {e}")
    golden_engine = None

try:
    from core.strategies.risk_control_engine import evaluate_3layer_risk, DEFAULT_RISK_CONFIG
except Exception as _scan_risk_imp:
    logging.debug("scan_engine 未加载 risk_control_engine（观察池二次一层将跳过）: %s", _scan_risk_imp)
    evaluate_3layer_risk = None
    DEFAULT_RISK_CONFIG = None

def _safe_float(val, default=0.0):
    if val is None: return default
    try:
        if pd.isna(val) or str(val).strip() in ['', '-']: return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _sanitize_rt_vol_ratio(rt, df_target, hist, vol_col, tag):
    """
    P2–P5 与 base_info 共用：库中 vol_ratio=0、实时推算为 0 时，用语义一致的 vol/vol_ma5 兜底（与 P1 表一致）。
    返回 (修正后量比, 尾标)。
    """
    vr = _safe_float(rt.get("vol_ratio", 0.0), 0.0)
    if vr > 0 and np.isfinite(vr):
        return float(min(vr, 50.0)), tag
    try:
        yb = df_target.iloc[-1]
        vh = _safe_float(yb.get(vol_col), 0.0)
        vma5 = _safe_float(yb.get("vol_ma5"), 0.0)
        if vma5 <= 0:
            vma5 = _safe_float(yb.get("vol_ma10"), 0.0)
        if vma5 > 0 and vh > 0:
            r = vh / vma5
            if np.isfinite(r) and r > 0:
                return float(min(r, 50.0)), "C"
    except Exception:
        pass
    if isinstance(hist, dict):
        raw = hist.get("vol_ratio", hist.get("vr"))
        rh = _safe_float(raw, float("nan"))
        if rh > 0 and np.isfinite(rh):
            return float(min(rh, 50.0)), "H" if ("vol_ratio" in hist or "vr" in hist) else tag
    return 1.0, "~"


def _intraday_ma5_vol_baseline_hand(df_target, vol_col: str) -> float:
    """
    盘中量比分母：取「最新一根已收盘 K」上的 5 日均量（手/日），与 Tushare vol 口径一致。
    落库列名为 vol_ma5；旧代码用 ('vma' in colname) 匹配不到 vol_ma5，误把单日成交量当分母，
    且误用 iloc[-2]，量比会被放大到触顶 50.0。
    """
    if df_target is None or df_target.empty:
        return 1.0
    last = df_target.iloc[-1]
    for col in ("vol_ma5", "vma5"):
        if col in df_target.columns:
            v = _safe_float(last[col], 0.0)
            if v > 0 and np.isfinite(v):
                return float(max(v, 1.0))
    vc = vol_col if vol_col and vol_col in df_target.columns else None
    if not vc:
        vc = "vol" if "vol" in df_target.columns else ("volume" if "volume" in df_target.columns else None)
    if vc:
        v = _safe_float(last[vc], 0.0)
        if v > 0 and np.isfinite(v):
            return float(max(v, 1.0))
    return 1.0


def _extract_scan_surface_metrics(item, rt_map, curr_min, is_after_hours, today_str, snapshots):
    """
    轻量截面采样：与主循环量价口径尽量一致（不 merge 日线），供当次扫描全样本 Rank 使用。
    失败返回 None；circ_mv_yi<100 与主池宪法一致直接跳过。
    """
    _ = today_str
    try:
        full_code = item.get("code", "")
        s_code = str(full_code).split(".")[0][:6]
        if s_code not in rt_map:
            fallback_hist = item.get("hist", {})
            if isinstance(fallback_hist, dict) and fallback_hist:
                _fb_pc = _safe_float(fallback_hist.get("pre_close"), 0.0)
                if _fb_pc <= 0:
                    _fb_pc = _safe_float(fallback_hist.get("close"), 0.0)
                rt_map[s_code] = {
                    "price": fallback_hist.get("close", 0),
                    "pre_close": _fb_pc,
                    "open": fallback_hist.get("open", 0),
                    "volume": 0,
                    "high": fallback_hist.get("high", 0),
                    "low": fallback_hist.get("low", 0),
                    "name": normalize_stock_display_name(fallback_hist.get("name", s_code)),
                }
            else:
                return None
        rt = dict(rt_map.get(s_code, {}) or {})
        if not isinstance(rt, dict):
            return None
        if isinstance(snapshots, dict) and s_code in snapshots:
            _apply_intraday_snapshots_to_rt(rt, snapshots[s_code])

        df = item.get("df")
        if df is None or df.empty:
            return None
        vol_col = "vol" if "vol" in df.columns else ("volume" if "volume" in df.columns else None)
        if vol_col is None:
            return None

        circ_mv_wan = _safe_float(df["circ_mv"].iloc[-1]) if "circ_mv" in df.columns else 0.0
        if circ_mv_wan <= 0:
            circ_mv_raw = rt.get("circ_mv")
            if circ_mv_raw is None or pd.isna(circ_mv_raw):
                circ_mv_raw = rt.get("total_mv", 10000000) * 0.6
            circ_mv_wan = _safe_float(circ_mv_raw, default=10000000)
        circ_mv_yi = circ_mv_wan / 10000.0
        if circ_mv_yi < _p1_scan_min_circ_mv_yi():
            return None

        hist = item.get("hist", {}) or {}
        now_price = _safe_float(rt.get("price"))
        if now_price <= 0:
            now_price = _safe_float(hist.get("close", 0.0))
        pre_price = _safe_float(rt.get("pre_close"), 0.0)
        if pre_price <= 0:
            pre_price = _safe_float(hist.get("pre_close"), 0.0)
        if pre_price <= 0:
            pre_price = _safe_float(hist.get("close", 0.0))
        if pre_price <= 0:
            return None
        pct = (now_price - pre_price) / pre_price * 100.0

        vol_shares = _safe_float(rt.get("volume", 0.0))
        ma5_vol_hand = _intraday_ma5_vol_baseline_hand(df, vol_col)

        if vol_shares <= 0 or curr_min < 565:
            rt_local_vr = _safe_float(hist.get("vol_ratio", hist.get("vr", 1.0)), default=1.0)
            calc_price = now_price if now_price > 0 else _safe_float(hist.get("close", 10), default=10.0)
            _vh = _safe_float(hist.get("vol", df[vol_col].iloc[-1] if vol_col else 0), 0.0)
            _trf = infer_turnover_rate_f_pct(_vh, calc_price, circ_mv_wan)
        else:
            vol_hand = vol_shares / 100.0
            if is_after_hours:
                rt_local_vr = vol_hand / ma5_vol_hand
            else:
                if 565 <= curr_min < 570:
                    elapsed_mins = 1
                elif 570 <= curr_min <= 900:
                    elapsed_mins = max(1, curr_min - 570)
                else:
                    elapsed_mins = 240
                rt_local_vr = min((vol_hand / elapsed_mins) / (ma5_vol_hand / 240), 50.0)
            # 【V26.6】VWAP均价替代现价计算换手率
            rt_amount = _safe_float(rt.get('amount', 0.0))
            if rt_amount > 0 and vol_shares > 0:
                calc_price = rt_amount / vol_shares
            else:
                calc_price = now_price if now_price > 0 else _safe_float(hist.get("close", 10), default=10.0)
            _trf = min(infer_turnover_rate_f_pct(vol_hand, calc_price, circ_mv_wan), 100.0)

        rt_for_san = {"vol_ratio": rt_local_vr}
        _vr_fix, _ = _sanitize_rt_vol_ratio(rt_for_san, df, hist, vol_col, "S")
        vol_ratio = float(min(_vr_fix, 50.0))

        net3 = 0.0
        for col in ("hk_vol", "net_main_amount"):
            if col in df.columns:
                net3 += _safe_float(df.tail(3)[col].sum())
        circ_base = max(circ_mv_yi * 100000000.0, 1e-9)
        main_ratio = float(net3 / circ_base)

        return {
            "s_code": s_code,
            "pct": float(np.nan_to_num(pct, nan=0.0, posinf=50.0, neginf=-50.0)),
            "vol_ratio": float(np.nan_to_num(vol_ratio, nan=1.0, posinf=50.0, neginf=0.05)),
            "turnover_f": float(np.nan_to_num(_trf, nan=0.0, posinf=100.0, neginf=0.0)),
            "main_ratio": float(np.nan_to_num(main_ratio, nan=0.0, posinf=10.0, neginf=-10.0)),
        }
    except Exception:
        return None


# ==================== ⛓️ 小黑屋机制 (黑名单隔离) ====================
def _add_to_blacklist(ts_code, stock_name, reason):
    blacklist_file = path_blacklist_json()
    today_str = datetime.now(BJ_TZ).strftime("%Y%m%d")
    blacklist = {}
    if os.path.exists(blacklist_file):
        try:
            with open(blacklist_file, 'r', encoding='utf-8') as f:
                blacklist = json.load(f)
        except json.JSONDecodeError as e:
            logging.warning("黑名单 JSON 解析失败，将覆盖写入新内容 [%s]: %s", blacklist_file, e)
        except OSError as e:
            logging.warning("读取黑名单文件 IO 失败 [%s]: %s", blacklist_file, e)
        except Exception as e:
            logging.exception("读取黑名单文件失败（未分类）[%s]: %s", blacklist_file, e)

    blacklist[ts_code] = {
        "name": stock_name,
        "reason": reason,
        "kill_date": today_str
    }
    
    cutoff_date = (datetime.now(BJ_TZ) - timedelta(days=7)).strftime("%Y%m%d")
    keys_to_remove = [k for k, v in blacklist.items() if v.get("kill_date", "") < cutoff_date]
    for k in keys_to_remove: del blacklist[k]
    
    ensure_runtime_data_layout()
    try:
        with open(blacklist_file, 'w', encoding='utf-8') as f:
            json.dump(blacklist, f, ensure_ascii=False)
    except OSError as e:
        logging.error("写入黑名单失败(IO): %s | %s", blacklist_file, e)
    except Exception as e:
        logging.exception("写入黑名单失败(未分类): %s | %s", blacklist_file, e)


# ==================== 🛑 danger_buy：分时 VWAP 钓鱼线熔断 ====================
# 与 P3 物理胸甲共用配置项 breakout_vwap_eps（config.yaml strategies.p3_intraday_screener）；
# 右侧冲高但现价显著低于当日成交量加权均价时，叠加禁买并拉黑，防主力分时出货诱多。
try:
    from core.strategies.p3_intraday_screener import _estimate_vwap_from_rt as _danger_buy_estimate_vwap_from_rt
except Exception:
    _danger_buy_estimate_vwap_from_rt = None

# 「现价很高」下限（%）：避免低开弱票因略低于 VWAP 误触；与 VWAP 缺省时不硬杀策略一致
DANGER_BUY_VWAP_MIN_DAY_PCT = 4.0


def _load_breakout_vwap_eps_for_danger_buy() -> float:
    """读取 P3 配置中的 breakout_vwap_eps，供禁买 VWAP 熔断与策略层对齐。"""
    try:
        from core.config_manager import get_p3_intraday_screener_config

        cfg = get_p3_intraday_screener_config()
        return float(getattr(cfg, "breakout_vwap_eps", 0.004))
    except Exception:
        return 0.004


def _danger_buy_vwap_fish_line_trigger(
    now_px: float,
    day_pct: float,
    rt: dict,
    curr_min: int,
    breakout_vwap_eps: float,
) -> bool:
    """
    硬规则：涨幅已处高位，但现价低于当日可估计的分时 VWAP 超过 breakout_vwap_eps 比例 → 真。
    盘前（无可靠分时）或额量不足以估计 VWAP 时不触发（与 p3_intraday_screener 一致）。
    """
    if breakout_vwap_eps <= 0 or not isinstance(rt, dict):
        return False
    if _danger_buy_estimate_vwap_from_rt is None:
        return False
    # 集合竞价前：分时均价无意义
    if curr_min < 570:
        return False
    if float(day_pct) < float(DANGER_BUY_VWAP_MIN_DAY_PCT):
        return False
    px = float(now_px)
    if px <= 0:
        return False
    vw = float(_danger_buy_estimate_vwap_from_rt(rt, px))
    if vw <= 0:
        return False
    return px < vw * (1.0 - float(breakout_vwap_eps))


# ==================== 🧬 个股性格评估器 (Personality Evaluator) ====================
def _evaluate_personality(df_target, rt, pool_key, vr_rank=None):
    """
    vr_rank: 当次扫描截面量比百分位秩（0~1）。传入时用「历史换手倍数 × 截面量比秩」几何融合，
    降低与战法层量价门槛的重复加权；未传入时回退旧版「分位换手 + 日内量历史百分位」线性混合。
    """
    if pool_key == 'p2':
        return 1.0

    if df_target is None or len(df_target) < 20:
        return 1.0

    df_hist = df_target.tail(60).copy()
    
    try:
        hist_series = series_effective_turnover_f_daily(df_hist)
        hist_trn_median = float(hist_series.median()) if len(hist_series) else 0.0
        y_last = df_hist.iloc[-1]
        now_px = _safe_float(rt.get("price", 0.0))
        if now_px <= 0:
            now_px = _safe_float(y_last.get("close"), 0.0)
        today_trn = effective_turnover_rate_f(rt, y_last, now_px)
        if hist_trn_median > 0.01 and today_trn > 0:
            trn_ratio = today_trn / max(hist_trn_median, 0.5)
            trn_multi = np.interp(trn_ratio, [0.5, 1.0, 2.5, 4.0, 6.0], [0.85, 1.0, 1.05, 1.10, 1.15])
        else:
            trn_multi = 1.0

        if vr_rank is not None and np.isfinite(float(vr_rank)):
            multiplier = personality_liquidity_blend(float(trn_multi), float(vr_rank))
        else:
            today_vol = _safe_float(rt.get('volume', 0))
            _pvc = 'vol' if 'vol' in df_hist.columns else ('volume' if 'volume' in df_hist.columns else None)
            if _pvc and today_vol > 0:
                vol_percentile = (df_hist[_pvc] < today_vol).mean() * 100.0
                vol_multi = np.interp(vol_percentile, [50, 70, 85, 95, 99], [0.90, 1.0, 1.05, 1.10, 1.15])
            else:
                vol_multi = 1.0

            multiplier = trn_multi * 0.5 + vol_multi * 0.5
            multiplier = max(0.85, min(1.15, float(multiplier)))

        # 禁止用默认 1.0 冒充昨收：缺 rt 时回退 T-1 日线 pre_close/close，否则涨跌幅与性格惩罚完全失真
        pre_close = _safe_float(rt.get("pre_close"), 0.0)
        if pre_close <= 0:
            pre_close = _safe_float(y_last.get("pre_close"), 0.0)
        if pre_close <= 0:
            pre_close = _safe_float(y_last.get("close"), 0.0)
        if pre_close <= 0:
            return round(multiplier, 3)
        today_pct = (_safe_float(rt.get("price", 0.0)) - pre_close) / pre_close * 100.0

        if today_pct < 0 and 'atr_pct' in df_hist.columns:
            hist_atr_median = df_hist['atr_pct'].median()
            if hist_atr_median > 0 and abs(today_pct) > hist_atr_median * 2.0:
                multiplier = 0.5

    except Exception as e:
        logging.debug(f"性格评估器计算异常: {e}")
        return 1.0

    return round(multiplier, 3)

# ==================== 2. 核心探测器 ====================
def _calc_safety_factor(df, rt, regime, industry, sorted_sectors, limit_times, is_empty_board):
    """
    综合安全因子：连板梯队、乖离、短期涨幅、筹码距离等，用于下游仓位/排序加权。

    高位乖离说明（实盘修订）：
    - 原先用 bias20>10 一律 *0.55，易误伤 2~4 板核心板块真龙（连板本身必然高乖离）。
    - 现规则：「2<=连板<=4 + 核心板块前三 + 非空板」整体豁免乖离扣分；
      仅在不满足该豁免且 bias20>15 时，乘 0.60 并打 ⚠️[高位乖离]（阈值从 10 放宽到 15，系数从 0.55 略放宽到 0.60）。
    """
    if df is None or len(df) < 6:
        return 1.0, []
    try:
        curr = df.iloc[-1]
        now_price = _safe_float(rt.get('price', 0))
        ma20 = _safe_float(curr.get('ma20', now_price))
        if ma20 <= 0:
            return 1.0, []

        safety = 1.0
        tags = []
        # 行业是否处于当日涨幅榜前三板块（主线辨识度）
        is_core_sector = industry in sorted_sectors[:3]

        bias20 = (now_price - ma20) / ma20 * 100.0
        # 【审计修复】维度2-pct_5d 分母避免 sub-1 元股价时被 max(close,1) 压扁导致乖离误判
        _c5 = _safe_float(df.iloc[-6]['close'])
        pct_5d = (now_price - _c5) / max(_c5, 1e-9) * 100.0
        cost_50th = _safe_float(rt.get('cost_50th', now_price))

        if is_empty_board and limit_times >= 2:
            tags.append("💣[中空假龙]")
            safety *= 0.70
        else:
            if 2 <= limit_times <= 4 and is_core_sector:
                tags.append("👑[2-4板免死]")
            else:
                if limit_times >= 5:
                    safety *= 0.50
                    tags.append("💀[≥5板断头台]")

        # 高乖离惩罚：与连板/板块豁免解耦，单独判断，避免屠龙刀误杀连板龙
        exempt_high_bias = (
            2 <= limit_times <= 4
            and is_core_sector
            and (not is_empty_board)
        )
        if (not exempt_high_bias) and bias20 > 15.0:
            safety *= 0.60
            tags.append("⚠️[高位乖离]")

        if pct_5d > 20.0 and not (2 <= limit_times <= 4 and is_core_sector and not is_empty_board):
            safety *= 0.80
            tags.append("🔥[短期超买]")

        if cost_50th > 0 and now_price > cost_50th * 1.3 and not (2 <= limit_times <= 4 and is_core_sector and not is_empty_board):
            safety *= 0.85
            tags.append("☁️[脱离筹码]")

        if regime in ["趋势市", "主升浪", "趋势主升"]: 
            safety = min(1.0, safety * 1.15)
            
        return safety, tags
    except Exception as e:
        logging.debug(f"_calc_safety_factor 异常: {e}")
        return 1.0, []

def _calc_decay_factor_atr(df, rt):
    try:
        if df is None or len(df) < 5: return 1.0
        now_vr = _safe_float(rt.get('vol_ratio', 0))
        now_price = _safe_float(rt.get('price', 0))
        
        df_tail_60 = df.tail(60).copy()
        recent_60_high = _safe_float(df_tail_60['high'].max())
        atr_pct = _safe_float(df.iloc[-1].get('atr_pct', 3.0), default=3.0)
        
        if atr_pct > 4.5: half_life = 2.0
        elif atr_pct < 2.5: half_life = 10.0
        else: half_life = 5.0

        if now_vr > 1.2 or now_price >= recent_60_high: return 1.0
        
        _vcol = 'vol' if 'vol' in df_tail_60.columns else ('volume' if 'volume' in df_tail_60.columns else None)
        _ma5_col = None
        for _c in ("vol_ma5", "vma5"):
            if _c in df_tail_60.columns:
                _ma5_col = _c
                break
        if _ma5_col and _vcol:
            cond_spike = df_tail_60[_vcol] > 1.2 * df_tail_60[_ma5_col]
        else:
            cond_spike = pd.Series(False, index=df_tail_60.index)
            
        cond_high = df_tail_60['high'] >= df_tail_60['high'].rolling(60, min_periods=1).max()
        combined_cond = (cond_spike | cond_high).values
        true_indices = np.where(combined_cond)[0]
        
        if len(true_indices) > 0: days_since = len(combined_cond) - 1 - true_indices[-1] 
        else: days_since = 60
            
        if days_since <= 1: return 1.0
        return max(0.5, 0.5 ** (days_since / half_life))
    except Exception as e:
        logging.debug(f"_calc_decay_factor_atr 异常: {e}")
        return 1.0

def _calc_position(size_emoji, safety_factor, regime, is_precise_pullback):
    # 退潮期硬性封顶：与市值档位无关，禁止大票重仓幻想
    if regime in ["情绪退潮市", "主跌浪", "退潮防守"]:
        return "⚠️空仓期建议 · 最高10% (试错)"
    if regime in ["趋势市", "主升浪", "趋势主升"]:
        if size_emoji == "🐎": return "🔥主攻: 30%-40%"
        elif size_emoji in ["🦍", "🐘"]: return "🛡️压舱: 15%-20%"
    else:
        if size_emoji == "🐎": return "⚔️游击: 15%-20%"
        elif size_emoji in ["🦍", "🐘"]: return "🛡️均衡: 20%-30%"
    return "🛡️常规: 15%-20%"

def _calc_defense_line(df, rt, size_emoji):
    try:
        if df is None or len(df) < 20: return "--"
        now_price = _safe_float(rt.get('price', 0))
        if size_emoji in ["🦍", "🐘"]:
            ma20 = _safe_float(df.iloc[-1].get('ma20', 0))
            if ma20 > 0: return f"破20日线: {ma20:.2f}"
            return "破20日线止损"
        else:
            return "3日未强化则无条件退出"
    except Exception as e:
        logging.debug(f"_calc_defense_line 异常: {e}")
        return "--"


def _calc_crowding_penalty(pool_key, pct, vol_ratio, turnover_f, upper_shadow_pct, circ_mv_yi=0.0):
    """
    拥挤度惩罚（收益导向）：
    - 目的：降低量化同质化拥挤下的冲高回落概率；
    - 策略：不做硬剔除，仅做分档降权（P2/P3 同档较轻、P4 中、P5 中强）。
    - 流通市值 circ_mv_yi≥500 亿（超级中军）：大幅抬高量比/换手/集中度过热阈值；若仍落入降权档则强制不降权并标注「大票启动豁免拥挤」。
    返回：(crowding_score[0~100], penalty_mult, label)
    """
    pk = str(pool_key or "").lower()
    p = float(_safe_float(pct, 0.0))
    vr = float(_safe_float(vol_ratio, 1.0))
    trf = float(_safe_float(turnover_f, 0.0))
    us = float(_safe_float(upper_shadow_pct, 0.0))
    circ_yi = float(_safe_float(circ_mv_yi, 0.0))
    megacap_relief = circ_yi >= 500.0

    # 各池阈值：越靠后（P4/P5）对“过热拥挤”容忍越低；P2 竞价与 P3 盘中同档（早盘脉冲不应按 P5 最严误杀）
    if pk in ("p2", "p3"):
        vr_hot, trf_hot, pct_hot = 2.8, 18.0, 6.0
    elif pk == "p4":
        vr_hot, trf_hot, pct_hot = 2.4, 14.0, 5.0
    else:  # p5 / 未知 pool_key 回退
        vr_hot, trf_hot, pct_hot = 2.2, 12.0, 4.5

    # 超级中军：脉冲放量与高真换手更偏机构建仓而非游资拥挤出货，显著放宽量比/换手/集中度「过热」锚点
    if megacap_relief:
        vr_hot += 2.65
        trf_hot += 26.0
        pct_hot += 3.0

    score = 0.0
    if vr > vr_hot:
        score += min(35.0, (vr - vr_hot) * 14.0)
    if trf > trf_hot:
        score += min(30.0, (trf - trf_hot) * 2.8)
    if p > pct_hot:
        score += min(25.0, (p - pct_hot) * 4.5)
    if us > 2.2:
        score += min(20.0, (us - 2.2) * 6.0)
    score = float(max(0.0, min(100.0, score)))

    if pk in ("p2", "p3"):
        if score >= 80.0:
            mult, label = 0.90, "重拥挤"
        elif score >= 65.0:
            mult, label = 0.95, "中拥挤"
        elif score >= 50.0:
            mult, label = 0.98, "轻拥挤"
        else:
            mult, label = 1.0, "正常"
    elif pk == "p4":
        if score >= 80.0:
            mult, label = 0.86, "重拥挤"
        elif score >= 65.0:
            mult, label = 0.92, "中拥挤"
        elif score >= 50.0:
            mult, label = 0.97, "轻拥挤"
        else:
            mult, label = 1.0, "正常"
    else:
        if score >= 80.0:
            mult, label = 0.82, "重拥挤"
        elif score >= 65.0:
            mult, label = 0.90, "中拥挤"
        elif score >= 50.0:
            mult, label = 0.96, "轻拥挤"
        else:
            mult, label = 1.0, "正常"

    if megacap_relief and mult < 1.0:
        mult = 1.0
        label = f"{label}豁免"
    return score, mult, label


def _p3_right_side_guard(
    df_target,
    rt,
    *,
    hit_res=None,
    market_contraction_score: float = 0.0,
    crowd_score: float = 0.0,
) -> tuple[bool, float, str, list]:
    """
    P3 防追高判定：硬过滤 + 降权。
    返回 (hard_filter, multiplier, label, tags)。
    """
    tags = []
    hard = False
    mult = 1.0
    label = "正常"
    try:
        if df_target is None or len(df_target) < 6:
            return False, 1.0, "数据不足", ["数据不足"]
        last = df_target.iloc[-1]
        price = _safe_float(rt.get("price", last.get("close", 0.0)), 0.0)
        pre_close = _safe_float(rt.get("pre_close", last.get("pre_close", 0.0)), 0.0)
        open_px = _safe_float(rt.get("open", last.get("open", price)), 0.0)
        high_px = _safe_float(rt.get("high", last.get("high", price)), 0.0)
        ma5 = _safe_float(last.get("ma5", 0.0), 0.0)
        ma20 = _safe_float(last.get("ma20", 0.0), 0.0)
        if price <= 0 or pre_close <= 0:
            return False, 1.0, "价格不足", ["价格不足"]
        vwap = _danger_buy_estimate_vwap_from_rt(rt, price)
        vwap_dev_pct = (price - vwap) / vwap * 100.0 if vwap and vwap > 0 else 0.0
        pct = (price - pre_close) / pre_close * 100.0
        vol_ratio = _safe_float(rt.get("vol_ratio", 0.0), 0.0)
        turnover_f = _safe_float(rt.get("turnover_rate_f", 0.0), 0.0)
        upper_shadow_pct = 0.0
        if pre_close > 0:
            upper_shadow_pct = (max(high_px, price) - max(price, open_px)) / pre_close * 100.0
        cost_50th = _safe_float(last.get("cost_50th", price), 0.0)
        if cost_50th > 0:
            cost_dev_pct = (price - cost_50th) / cost_50th * 100.0
        else:
            cost_dev_pct = 0.0

        sector_beta = 1.0
        stock_memory_score = 0.0
        if isinstance(hit_res, dict):
            detail = hit_res.get("detail") or {}
            if isinstance(detail, dict):
                sector_beta = _safe_float(detail.get("sector_beta", 1.0), 1.0)
                stock_memory_score = _safe_float(detail.get("stock_memory_score", 0.0), 0.0)

        if vwap_dev_pct > 4.0:
            hard = True
            tags.append("VWAP严重偏离(急拉过热)")
        if pct >= 6.0 and vwap_dev_pct > 2.0:
            mult *= 0.82
            tags.append("盘中涨幅偏热(脉冲风险)")
        if ma5 > 0 and price > ma5 * 1.08:
            mult *= 0.85
            tags.append("5日线乖离过热")
        if ma20 > 0 and price > ma20 * 1.15:
            mult *= 0.90
            tags.append("20日线偏离偏高")
        if vol_ratio >= 6.0:
            mult *= 0.82
            tags.append("量比尖刺(诱多风险)")
        if turnover_f >= 15.0 and price < vwap:
            hard = True
            tags.append("均价线下高换手(承接差)")
        elif turnover_f >= 10.0:
            mult *= 0.90
            tags.append("换手偏高(分歧加大)")
        if upper_shadow_pct >= 3.0 and vol_ratio >= 3.0:
            mult *= 0.85
            tags.append("冲高上影(尾随风险)")
        if crowd_score >= 65.0:
            mult *= 0.92
            tags.append("拥挤度偏高(情绪过热)")
        if sector_beta <= 0.95:
            mult *= 0.92
            tags.append("板块跟随不足(孤强)")
        if sector_beta >= 1.15:
            mult *= 1.03
            tags.append("板块共振(主线同步)")
        if stock_memory_score >= 60.0:
            mult *= 1.03
            tags.append("筹码锁定稳")
        if cost_dev_pct >= 8.0:
            mult *= 0.88
            tags.append("筹码乖离偏高")
        if market_contraction_score >= 0.7:
            mult *= 1.03
            tags.append("收缩环境(容错更高)")
        if vwap_dev_pct <= 1.5 and 1.5 <= vol_ratio <= 3.0 and 5.0 <= turnover_f <= 12.0:
            mult *= 1.06
            tags.append("贴线温和放量(稳健)")
        if price >= vwap and price >= ma5 and sector_beta >= 1.0 and stock_memory_score >= 45.0:
            mult *= 1.05
            tags.append("右侧确认(真核)")
        if hard:
            label = "硬过滤"
        elif mult >= 1.08:
            label = "强确认"
        elif mult >= 1.02:
            label = "适度加分"
        elif mult < 1.0:
            label = "降权"
        else:
            label = "正常"
        mult = float(max(0.55, min(1.12, mult)))
        # 仅把明显追高风险票拦下；其余给降权空间。
        if hard:
            return True, mult, label, tags
        return False, mult, label, tags
    except Exception as e:
        logging.debug("_p3_right_side_guard 异常: %s", e)
        return False, 1.0, "异常回退", ["异常回退"]


def _execution_tier_for_pool(pool_key, final_score, crowding_score, relaxed_gate_applied=False):
    """
    P4/P5 执行分层（A/B/C）：
    - A：可主仓候选（仍需按纪律执行）
    - B：跟踪试错
    - C：观察为主
    """
    pk = str(pool_key or "").lower()
    fs = float(_safe_float(final_score, 0.0))
    cs = float(_safe_float(crowding_score, 0.0))

    if pk == "p4":
        if fs >= 90.0 and cs <= 55.0:
            tier = "A"
        elif fs >= 80.0 and cs <= 72.0:
            tier = "B"
        else:
            tier = "C"
    elif pk == "p5":
        if fs >= 88.0 and cs <= 60.0:
            tier = "A"
        elif fs >= 78.0 and cs <= 75.0:
            tier = "B"
        else:
            tier = "C"
    else:
        return "--", ""

    # 缩量辅助放宽门禁进入的票，执行上自动降一档，避免“门禁放宽=仓位放大”
    if relaxed_gate_applied and tier in ("A", "B"):
        tier = "B" if tier == "A" else "C"

    if tier == "A":
        return tier, "主仓候选（按纪律分批）"
    if tier == "B":
        return tier, "跟踪试错（轻仓）"
    return tier, "观察优先（谨慎）"

def _infer_golden_gate_reason(pool_key, df, rt):
    """门禁未通过时给出可读原因（用于漏斗诊断，不参与策略计算）。"""
    try:
        curr = df.iloc[-1] if isinstance(df, pd.DataFrame) and not df.empty else {}
        price = _safe_float(rt.get('price', curr.get('close', 0.0)))
        pre_close = _safe_float(rt.get('pre_close', curr.get('pre_close', 0.0)))
        open_price = _safe_float(rt.get('open', curr.get('open', 0.0)))
        high_price = _safe_float(rt.get('high', curr.get('high', 0.0)))
        vr = _safe_float(rt.get('vol_ratio', 0.0))
        if pre_close <= 0 or price <= 0 or vr <= 0:
            return "基础行情字段缺失"

        pct = (price - pre_close) / pre_close * 100.0
        pk = str(pool_key or '').lower()
        if pk == 'p2':
            open_pct = (open_price - pre_close) / pre_close * 100.0 if pre_close > 0 else 0.0
            winner = _safe_float(rt.get('winner_rate', curr.get('winner_rate', 0.0)))
            cost_50th = _safe_float(rt.get('cost_50th', curr.get('cost_50th', 0.0)))
            if open_pct <= 1.0:
                return "竞价涨幅不足(open_pct<=1)"
            if vr < 1.2:
                return "量比不足(vr<1.2)"
            if winner > 0 and cost_50th > 0 and not (winner > 85.0 and price > cost_50th):
                return "胜率/成本峰不达标"
            return "P2门禁其他条件未满足"

        if pk == 'p3':
            if pct <= 2.0:
                return "盘中涨幅不足(pct<=2)"
            if vr < 1.5:
                return "量比不足(vr<1.5)"
            macd_bar = _safe_float(curr.get('macd_bar', curr.get('macd_hist', 0.0)))
            amount = _safe_float(rt.get('amount', 0.0))
            volume = _safe_float(rt.get('volume', 0.0))
            if amount <= 0 or volume <= 0:
                return "量额缺失且MACD未转正" if macd_bar <= 0 else "量额缺失"
            # 【审计修复】维度2：额量比与 VWAP 分母兜底
            _vol_d = max(volume, 1e-9)
            tentative = amount / _vol_d
            vwap = amount / max(volume * 100.0, 1e-9) if tentative > price * 20 else tentative
            if vwap <= 0:
                return "VWAP不可用"
            if price <= vwap:
                return "未站上VWAP"
            if macd_bar <= 0:
                return "MACD未转正"
            return "P3门禁其他条件未满足"

        # P4：与 strat_base.strict_golden_burst_ok 的 p4 分支同一套数（诊断文案 ≠ 实盘门禁，但阈值/上影口径必须一致，否则漏斗会骗人）
        if pk == "p4":
            tail_vol_ratio = _safe_float(rt.get("tail_vol_ratio", 0.0))
            net_main_gate = _safe_float(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)))
            if not (2.0 < pct < 8.0):
                return "涨幅不在P4尾盘窗口(2~8%)"
            if vr < 1.1:
                return "量比不足(vr<1.1)"
            tail_ok = (tail_vol_ratio > 1.5) if tail_vol_ratio > 0 else True
            if not tail_ok:
                return "尾盘增量不足(tail_vol_ratio<=1.5)"
            if net_main_gate <= 0:
                return "主力净流入<=0"
            hi_shadow = _safe_float(curr.get("high", 0.0))
            if hi_shadow <= 0:
                hi_shadow = _safe_float(rt.get("high", 0.0))
            if hi_shadow <= 0:
                hi_shadow = max(price, open_price)
            upper_shadow_pct = (hi_shadow - max(price, open_price)) / pre_close * 100.0 if pre_close > 0 else 0.0
            if upper_shadow_pct >= 2.8:
                return "上影过长(>=2.8%)"
            return "P4门禁其他条件未满足"

        # P5：盘后全日定档门禁（与 strat_base.strict_golden_burst_ok 的 p5 分支一致，禁止混入 tail_vol_ratio）
        if pk == "p5":
            try:
                from core.config_manager import get_golden_config

                g = get_golden_config()
                p5_vr = float(g.get("p5_golden_vr_min", 1.2))
                p5_lo = float(g.get("p5_golden_pct_low", 2.0))
                p5_hi = float(g.get("p5_golden_pct_high", 7.0))
            except Exception:
                p5_vr, p5_lo, p5_hi = 1.2, 2.0, 7.0
            if vr < p5_vr:
                return f"量比不足(P5全日<{p5_vr})"
            if not (p5_lo < pct < p5_hi):
                return f"涨幅不在P5全日窗口({p5_lo}~{p5_hi})"
            net_main = _safe_float(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)))
            if net_main <= 0:
                return "主力净流入非正(P5)"
            ma5_line = _safe_float(curr.get("ma5", 0.0))
            ma20_line = _safe_float(curr.get("ma20", 0.0))
            if ma20_line <= 0 or not (price > ma20_line and price > ma5_line):
                return "未同时站稳ma5与ma20(P5)"
            return "P5门禁其他条件未满足"

        return "门禁未通过(未分类)"
    except Exception:
        return "门禁未通过(诊断异常)"

def _build_priority_tags(p1_gene, burst_score, surge_bonus, regime_mult, decay_factor, is_stop_loss, penalty, is_core_sector, pool_key, safety_tags, personality_mult):
    tags = safety_tags.copy() 
    if is_core_sector: tags.append("👑[主线保送]")  
    if is_stop_loss: tags.append("🚨[危-破位]")
    elif decay_factor <= 0.85: tags.append("⚠️[衰减严重]")
    if penalty > 0 and not is_core_sector: tags.append(f"🩹[瑕疵-{penalty:.0f}]") 
    
    if personality_mult == 0.5: tags.append("🩸[熔断拦截]")
    elif personality_mult >= 1.10: tags.append("🌟[异动觉醒]")
        
    if p1_gene >= 90.0: tags.append("🛡️[底仓王]")
    if surge_bonus >= 2.0: tags.append("🚀[黄金十绝]") 
    if burst_score >= 80.0: tags.append("⚡[强爆发]")
    if regime_mult > 1.0: tags.append("🌊[顺风增压]")
    return " ".join(tags[:5])

def get_realtime_sector_ranking():
    ind_map = get_all_basic_industry()
    if not ind_map: return get_latest_sector_ranking()
    
    now_time = datetime.now(BJ_TZ)
    curr_min = now_time.hour * 60 + now_time.minute
    # 非交易日或开盘前：固定回退到最新落库板块排名，避免同日多次扫描口径漂移。
    if now_time.weekday() >= 5 or curr_min < 565:
        return get_latest_sector_ranking()
        
    rt_map = fetch_realtime_batch(list(ind_map.keys()))
    if not rt_map: return get_latest_sector_ranking()
    sector_data = {}
    for ts_code, ind in ind_map.items():
        s_code = ts_code.split('.')[0][:6]
        if s_code in rt_map:
            rt = rt_map[s_code]
            if not isinstance(rt, dict): continue
            now_p = float(rt.get('price', 0))
            # 缺昨收时宁可不参与板块涨跌统计，也不用 1.0 伪造分母
            pre_p = float(rt.get('pre_close', 0.0))
            if pre_p > 0 and now_p > 0:
                if ind not in sector_data: sector_data[ind] = []
                sector_data[ind].append((now_p - pre_p) / pre_p * 100.0)
    ranking = {ind: sum(pcts)/len(pcts) for ind, pcts in sector_data.items() if len(pcts) >= 5}
    return dict(sorted(ranking.items(), key=lambda item: item[1], reverse=True))

def save_signal_log(item, pool_key, regime, breakdown_str):
    try:
        if not isinstance(item, dict): return
        df_log = pd.DataFrame([{
            'trade_date': datetime.now(BJ_TZ).strftime('%Y%m%d'),
            'ts_code': item.get('代码', ''), 'name': normalize_stock_display_name(item.get('名称', '')),
            'pool': pool_key, 'strategy': item.get('战法', ''),
            'score': item.get('综合分', 0.0), 'regime': regime,
            'score_breakdown': breakdown_str, 'limit_times': item.get('连板高度', 0), 
            'forecast_type': item.get('机构预测', 0), 'created_at': datetime.now(BJ_TZ)
        }])
        table_name = getattr(constants, 'LOG_TABLE', 'signal_log') or 'signal_log'
        save_df_to_sql(df_log, table_name)
    except Exception as e:
        logging.warning(f"save_signal_log 落库失败: {e}")


def _format_p4_trade_language(hits, stock_memory_score=0.0, sector_beta=1.0, close_vwap_dev=None):
    """
    把 P4 命中翻成更像交易员看盘的短标签。

    【V26.6 优化】原实现对 hits 列表遍历 11 次 any()，每次都从头扫描。
    改为：单次遍历 hits 构建命中关键词集合，再用集合查询判断，
    复杂度从 O(11n) 降为 O(n)。
    """
    hits = [str(h) for h in (hits or []) if str(h).strip()]
    tags = []

    # 【V26.6 优化】单次遍历构建命中词集合，避免11次重复扫描
    hit_set = set()
    for h in hits:
        for w in h.split():
            hit_set.add(w)
    hit_txt = " ".join(hits)

    # P4战法标签（从11次 any() 遍历改为集合查询，每个判断 O(1)）
    if "P4-01" in hit_set or "光头阳线抢筹" in hit_set:
        tags.append("🔥抢筹")
    if "P4-02" in hit_set or "筹码锁死" in hit_set:
        tags.append("🧲锁筹")
    if "P4-03" in hit_set or "机构尾盘潜伏" in hit_set:
        tags.append("🕳️埋伏")
    if "P4-04" in hit_set or "均线缩量低吸" in hit_set:
        tags.append("↘️回踩")
    if "P4-05" in hit_set or "强势洗盘承接" in hit_set:
        tags.append("🧱承接")
    if "P4-06" in hit_set or "动能突破共振" in hit_set:
        tags.append("🚀突破")
    if "P4-07" in hit_set or "底仓不破均线" in hit_set:
        tags.append("🛡️底仓")
    if "P4-08" in hit_set or "温和均线修复" in hit_set:
        tags.append("🔧均线修复")
    if "P4-09" in hit_set or "均线多头回踩企稳" in hit_set:
        tags.append("📈多头回踩")
    if "P4-10" in hit_set or "沿5日线主升缩量" in hit_set:
        tags.append("⚡缩量再攻")
    if "P4-11" in hit_set or "底仓主线共振" in hit_set:
        tags.append("⭐主线底仓")
    if stock_memory_score >= 12.0:
        tags.append("👑股性强")
    elif stock_memory_score >= 6.0:
        tags.append("⭐股性热")
    elif stock_memory_score < -3.0:
        tags.append("🧊股性弱")
    if close_vwap_dev is not None:
        if close_vwap_dev > 2.5:
            tags.append("⚠️尾盘偷袭")
        elif close_vwap_dev < -1.5:
            tags.append("弱收")
    if sector_beta >= 1.25:
        tags.append("🌋板块热")
    elif sector_beta <= 0.9:
        tags.append("🧊板块冷")
    if not tags:
        tags.append("--")
    if hit_txt:
        return " / ".join(tags[:4])
    return "--"


def _p3_hit_tier(hit: str) -> str:
    """
    判断 P3 战法标签级别。
    【V26.6 优化】P3 主战法/辅助标签/观察项仍用 any() 链，
    因单只股票一次 hits 数量有限（通常 <10），any() 开销可忽略。
    """
    s = str(hit or "")
    if any(k in s for k in ["右侧起爆", "均线低吸", "巨头连贯发力", "倍量启动延续"]):
        return "主战法"
    if any(k in s for k in ["单峰跃迁", "平台/二次确认", "水上金叉", "资金逆势托底"]):
        return "辅助标签"
    if any(k in s for k in ["质量趋势底仓", "缩量分歧低吸"]):
        return "观察项"
    return "辅助标签"


def _format_p3_trade_layers(hits):
    hits = [str(h) for h in (hits or []) if str(h).strip()]
    tiers = {"主战法": [], "辅助标签": [], "观察项": []}
    for h in hits:
        tiers[_p3_hit_tier(h)].append(h)
    return {
        "主战法": " / ".join(tiers["主战法"]) if tiers["主战法"] else "--",
        "辅助标签": " / ".join(tiers["辅助标签"]) if tiers["辅助标签"] else "--",
        "观察项": " / ".join(tiers["观察项"]) if tiers["观察项"] else "--",
    }

_INTRADAY_SLOT_IDS = frozenset({"935", "1030", "1125", "1325", "1425", "1440"})


def _normalize_intraday_slot_id(raw):
    """后台六时点槽位：935/1030/1125/1325/1425/1440。"""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s in _INTRADAY_SLOT_IDS:
        return s
    if s.isdigit():
        s2 = str(int(s))
        if s2 in _INTRADAY_SLOT_IDS:
            return s2
    return None


def _intraday_write_slot_to_stock_row(sn: dict, rt_data: dict, curr_min: int, slot: str) -> None:
    """
    单票写入某一瞬态槽位（须 force 路径调用，总是覆盖该槽字段）。
    早盘三档：935/1030/1125；午盘末 1325；尾盘两锚 1425→vol_1435、1440→vol_1440。
    """
    amt = _safe_float(rt_data.get("amount", 0))
    vol = _safe_float(rt_data.get("volume", 0))
    now_p = _safe_float(rt_data.get("price", 0))
    high = _safe_float(rt_data.get("high", 0))
    vr = _safe_float(rt_data.get("vol_ratio", 0))
    curr_vwap = 0.0
    if amt > 0 and vol > 0:
        _vs = max(vol, 1e-9)
        tentative = amt / _vs
        curr_vwap = amt / max(vol * 100.0, 1e-9) if tentative > now_p * 20 else tentative

    sn[f"vr_{slot}"] = vr
    sn[f"vol_{slot}"] = vol
    sn[f"high_{slot}"] = high
    if slot == "1030":
        sn["time_1030"] = curr_min
    else:
        sn[f"tmin_{slot}"] = curr_min
    if curr_vwap > 0:
        sn[f"vwap_{slot}"] = curr_vwap

    if slot == "1325":
        sn["pre_tail_high"] = high
        if curr_vwap > 0:
            sn["pre_tail_vwap"] = curr_vwap
        sn["time_1300"] = curr_min

    if slot == "1425":
        sn["vol_1435"] = vol
        sn["time_1435"] = curr_min

    if slot == "1440":
        sn["vol_1440"] = vol
        sn["time_1440"] = curr_min


def _tail_volume_anchor(sn: dict, curr_min: int):
    """尾盘 tail_vol_ratio：过 14:40 后优先用 1440 锚点，否则 1425（原 vol_1435）。"""
    if not isinstance(sn, dict):
        return None, None
    t1440 = sn.get("time_1440")
    if t1440 is not None and curr_min > int(t1440):
        return sn.get("vol_1440"), int(t1440)
    t1435 = sn.get("time_1435")
    if t1435 is not None and curr_min > int(t1435):
        return sn.get("vol_1435"), int(t1435)
    return None, None


def _apply_intraday_snapshots_to_rt(rt: dict, sn: dict) -> None:
    """JSON 瞬态记忆并入 rt：全槽位字段 + P3/P4 用的早盘量比地板。"""
    if not isinstance(rt, dict) or not isinstance(sn, dict):
        return
    for k in (
        "vr_935",
        "vol_935",
        "high_935",
        "tmin_935",
        "vwap_935",
        "vr_1030",
        "time_1030",
        "vol_1030",
        "high_1030",
        "vwap_1030",
        "vr_1125",
        "vol_1125",
        "high_1125",
        "tmin_1125",
        "vwap_1125",
        "vr_1325",
        "vol_1325",
        "high_1325",
        "tmin_1325",
        "vwap_1325",
        "pre_tail_high",
        "pre_tail_vwap",
        "time_1300",
        "vol_1435",
        "time_1435",
        "vol_1440",
        "time_1440",
    ):
        if k in sn:
            rt[k] = sn[k]
    morning = []
    for _k in ("vr_935", "vr_1030", "vr_1125"):
        if _k in sn:
            v = _safe_float(sn.get(_k), -1.0)
            if v >= 0:
                morning.append(v)
    if morning:
        rt["vr_morning_floor"] = float(min(morning))
        rt["vr_morning_mean"] = float(sum(morning) / len(morning))


def get_intraday_snapshot_status():
    """
    返回当日快照完成情况（供 UI / 诊断）：
    六时点：935 / 1030 / 1125 / 1325（午盘末）/ 1425（尾盘锚1）/ 1440（尾盘锚2）。
    仍附带 done_1030、done_1300、done_1435 与旧 ratio_* 字段名以便兼容展示。
    """
    now_time = datetime.now(BJ_TZ)
    today_str = now_time.strftime("%Y%m%d")
    snapshot_file = path_intraday_snapshots_json()
    snapshots = {}
    if os.path.exists(snapshot_file):
        try:
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if data.get("date") == today_str:
                    snapshots = data.get("stocks", {})
        except Exception:
            snapshots = {}

    total = len(snapshots)
    if total <= 0:
        z = {
            "total": 0,
            "quality_score": 0.0,
            "quality_grade": "C",
            "quality_hint": "未建立快照，建议先捕获关键时点",
        }
        for sid in ("935", "1030", "1125", "1325", "1425", "1440"):
            z[f"done_{sid}"] = 0
            z[f"ratio_{sid}"] = 0.0
        z["done_1030"] = z["done_1300"] = z["done_1435"] = 0
        z["ratio_1030"] = z["ratio_1300"] = z["ratio_1435"] = 0.0
        return z

    def _has_slot(s: dict, sid: str) -> bool:
        if sid == "935":
            return "vr_935" in s
        if sid == "1030":
            return "vr_1030" in s and "time_1030" in s
        if sid == "1125":
            return "vr_1125" in s
        if sid == "1325":
            return ("pre_tail_high" in s) or ("pre_tail_vwap" in s)
        if sid == "1425":
            return ("vol_1435" in s) and ("time_1435" in s)
        if sid == "1440":
            return ("vol_1440" in s) and ("time_1440" in s)
        return False

    done = {sid: 0 for sid in ("935", "1030", "1125", "1325", "1425", "1440")}
    for _, s in snapshots.items():
        if not isinstance(s, dict):
            continue
        for sid in done:
            if _has_slot(s, sid):
                done[sid] += 1

    score = sum((done[sid] / total) * (100.0 / 6.0) for sid in done)
    if score >= 90.0:
        grade = "A"
        hint = "六时点快照齐套，早盘地板与双尾盘锚可用"
    elif score >= 65.0:
        grade = "B"
        hint = "快照部分齐套，P3/P4 早盘与尾盘判定可能偏保守"
    else:
        grade = "C"
        hint = "快照不足，易漏判/误判，请确认后台调度或手动补录"

    out = {
        "total": total,
        "quality_score": round(score, 2),
        "quality_grade": grade,
        "quality_hint": hint,
        "done_1030": done["1030"],
        "done_1300": done["1325"],
        "done_1435": done["1425"],
        "ratio_1030": round(done["1030"] / total * 100.0, 1),
        "ratio_1300": round(done["1325"] / total * 100.0, 1),
        "ratio_1435": round(done["1425"] / total * 100.0, 1),
    }
    for sid in done:
        out[f"done_{sid}"] = done[sid]
        out[f"ratio_{sid}"] = round(done[sid] / total * 100.0, 1)
    return out


def capture_intraday_snapshots(codes=None, capture_mode='auto', force=False, slot_id=None):
    now_time = datetime.now(BJ_TZ)
    curr_min = now_time.hour * 60 + now_time.minute
    today_str = now_time.strftime("%Y%m%d")
    slot = _normalize_intraday_slot_id(slot_id)

    snapshot_file = path_intraday_snapshots_json()
    snapshots = {}
    if os.path.exists(snapshot_file):
        try:
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if data.get("date") == today_str:
                    snapshots = data.get("stocks", {})
        except json.JSONDecodeError as e:
            logging.warning(f"日内快照 JSON 损坏 [{snapshot_file}]: {e}，将重建当日快照容器。")
        except OSError as e:
            logging.warning(f"读取日内快照 IO 失败 [{snapshot_file}]: {e}")
        except Exception as e:
            logging.warning(f"读取日内快照失败（未分类）[{snapshot_file}]: {e}")

    if not codes:
        p1_cache = load_p1_cache(today_str)
        if p1_cache:
            if isinstance(p1_cache, list):
                codes = [x.get('ts_code', x.get('code', '')) for x in p1_cache if isinstance(x, dict)]
            elif isinstance(p1_cache, dict):
                codes = list(p1_cache.keys())
        if not codes:
            codes = list(get_all_basic_industry().keys())
            
    if not codes:
        return "无股票池"
    
    rt_map = fetch_realtime_batch(codes)
    if not rt_map:
        return "获取实时行情失败"

    if slot:
        if not force:
            return f"slot={slot} 捕获须 force=True（由后台调度触发）"
        need_save = False
        update_count = 0
        for s_code, rt_data in rt_map.items():
            if not isinstance(rt_data, dict):
                continue
            if s_code not in snapshots:
                snapshots[s_code] = {}
            _intraday_write_slot_to_stock_row(snapshots[s_code], rt_data, curr_min, slot)
            need_save = True
            update_count += 1
        if need_save:
            ensure_runtime_data_layout()
            try:
                with open(snapshot_file, 'w', encoding='utf-8') as f:
                    json.dump({"date": today_str, "stocks": snapshots}, f, ensure_ascii=False)
                return f"✅ slot {slot} 快照捕获成功（更新 {update_count} 只，{now_time.strftime('%H:%M')}）"
            except Exception as e:
                return f"保存失败: {e}"
        return f"slot {slot} 无有效标的"

    mode = str(capture_mode or 'auto').strip().lower()
    valid_modes = {'auto', '1030', '1300', '1435'}
    if mode not in valid_modes:
        mode = 'auto'

    if mode == 'auto':
        if curr_min >= 875:
            mode = '1435'
        elif curr_min >= 780:
            mode = '1300'
        elif curr_min >= 630:
            mode = '1030'
        else:
            return "尚未到捕获时段"

    # 时间窗约束：默认只允许在目标时段附近捕获；force=True 可用于盘后人工补录
    windows = {
        '1030': (615, 660),   # 10:15~11:00
        '1300': (770, 820),   # 12:50~13:40
        '1435': (860, 895),   # 14:20~14:55
    }
    ws, we = windows.get(mode, (0, 24 * 60))
    if (not force) and not (ws <= curr_min <= we):
        return f"当前时间 {now_time.strftime('%H:%M')} 不在 {mode} 捕获窗口内（{ws//60:02d}:{ws%60:02d}-{we//60:02d}:{we%60:02d}）"
    
    need_save = False
    update_count = 0
    for s_code, rt_data in rt_map.items():
        if not isinstance(rt_data, dict): continue
        if s_code not in snapshots: snapshots[s_code] = {}
        
        amt = _safe_float(rt_data.get('amount', 0))
        vol = _safe_float(rt_data.get('volume', 0))
        now_p = _safe_float(rt_data.get('price', 0))
        
        curr_vwap = 0.0
        if amt > 0 and vol > 0:
            # 【审计修复】维度2：快照 VWAP 换算分母兜底
            _vs = max(vol, 1e-9)
            tentative = amt / _vs
            curr_vwap = amt / max(vol * 100.0, 1e-9) if tentative > now_p * 20 else tentative
            
        if mode == '1030':
            if force or ('vr_1030' not in snapshots[s_code]):
                snapshots[s_code]['vr_1030'] = _safe_float(rt_data.get('vol_ratio', 0))
                snapshots[s_code]['time_1030'] = curr_min
                need_save = True
                update_count += 1

        elif mode == '1300':
            if force or ('pre_tail_high' not in snapshots[s_code]):
                snapshots[s_code]['pre_tail_high'] = _safe_float(rt_data.get('high', 0))
                if curr_vwap > 0:
                    snapshots[s_code]['pre_tail_vwap'] = curr_vwap
                snapshots[s_code]['time_1300'] = curr_min
                need_save = True
                update_count += 1

        elif mode == '1435':
            if force or ('vol_1435' not in snapshots[s_code]):
                snapshots[s_code]['vol_1435'] = vol
                snapshots[s_code]['time_1435'] = curr_min
                need_save = True
                update_count += 1

    if need_save:
        ensure_runtime_data_layout()
        try:
            with open(snapshot_file, 'w', encoding='utf-8') as f:
                json.dump({"date": today_str, "stocks": snapshots}, f, ensure_ascii=False)
            return f"✅ {mode} 快照捕获成功（更新 {update_count} 只，{now_time.strftime('%H:%M')}）"
        except Exception as e:
            return f"保存失败: {e}"
            
    return f"{mode} 快照无需更新"

# ==================== 【多层级池子】与 wash_metrics 共用元数据（不新增文件） ====================
_TIER_META_KEY = "__tier_pool_meta__"


def _tier_wash_path():
    return path_wash_metrics_json()


def _load_prev_scan_empty_streak():
    """# 【多层级池子】说明：读取上一轮落盘的 P2–P5 主池连续无票 streak，供本趟是否允许写入观察池（乐观门控）。"""
    p = _tier_wash_path()
    if not os.path.exists(p):
        return 0
    try:
        with open(p, "r", encoding="utf-8") as f:
            root = json.load(f)
        if not isinstance(root, dict):
            return 0
        meta = root.get(_TIER_META_KEY)
        if not isinstance(meta, dict):
            return 0
        return int(meta.get("scan_empty_streak", 0) or 0)
    except Exception as e:
        logging.debug("【多层级池子】读取 scan streak 失败: %s", e)
        return 0


def _merge_tier_meta_scan(is_main_empty):
    """# 【多层级池子】说明：本次扫描结束后更新「主池(P2–P5)无票」连续计数；有票清零。同日多次扫描不重复加一。"""
    from core.file_utils import atomic_json_update

    p = _tier_wash_path()
    box = {"streak": 0}

    def _upd(root: dict) -> None:
        meta = root.get(_TIER_META_KEY)
        if not isinstance(meta, dict):
            meta = {}
        streak = int(meta.get("scan_empty_streak", 0) or 0)
        today_str = datetime.now(BJ_TZ).strftime("%Y%m%d")
        last_d = str(meta.get("scan_last_empty_date", "") or "")
        if is_main_empty:
            if last_d != today_str:
                streak += 1
                meta["scan_last_empty_date"] = today_str
        else:
            streak = 0
            meta["scan_last_empty_date"] = ""
        meta["scan_empty_streak"] = streak
        root[_TIER_META_KEY] = meta
        box["streak"] = streak

    try:
        ensure_runtime_data_layout()
        atomic_json_update(p, _upd, timeout=5)
    except Exception as e:
        logging.warning("【多层级池子】atomic 写入 scan streak 失败: %s", e)
    return int(box["streak"])


def _parse_crowding_score(row):
    """从 '68(中拥挤)' 解析数值 68.0；解析失败返回 0。"""
    try:
        s = str((row or {}).get("拥挤度", "") or "").strip()
        if not s:
            return 0.0
        n = str(s).split("(", 1)[0].strip()
        return float(_safe_float(n, 0.0))
    except Exception:
        return 0.0


T1_MEMORY_SCHEMA_VERSION = 1


def _default_t1_memory_payload():
    return {
        "schema_version": T1_MEMORY_SCHEMA_VERSION,
        "source": "scan_attribution.t1_settlement",
        "pools": {},
    }


def _normalize_t1_pool_payload(raw):
    payload = {
        "sample_n": 0,
        "avg_ret_t1_pct": 0.0,
        "win_rate_t1_pct": 0.0,
        "by_tier": {},
    }
    if not isinstance(raw, dict):
        return payload
    payload["sample_n"] = int(_safe_float(raw.get("sample_n", 0), 0))
    payload["avg_ret_t1_pct"] = round(float(_safe_float(raw.get("avg_ret_t1_pct", 0.0), 0.0)), 3)
    payload["win_rate_t1_pct"] = round(float(_safe_float(raw.get("win_rate_t1_pct", 0.0), 0.0)), 1)
    by_tier = raw.get("by_tier") if isinstance(raw.get("by_tier"), dict) else {}
    norm_bt = {}
    for tk, tv in by_tier.items():
        if not isinstance(tv, dict):
            continue
        norm_bt[str(tk)] = {
            "n": int(_safe_float(tv.get("n", 0), 0)),
            "avg_ret_t1_pct": round(float(_safe_float(tv.get("avg_ret_t1_pct", 0.0), 0.0)), 3),
            "win_rate_t1_pct": round(float(_safe_float(tv.get("win_rate_t1_pct", 0.0), 0.0)), 1),
        }
    payload["by_tier"] = norm_bt
    return payload


def _collect_t1_memory_for_pools(root: dict, target_pools):
    """从历史 scan_attribution 里汇总 P3/P4/P5 的 T+1 记忆，供实时打分使用。"""
    out = {}
    if not isinstance(root, dict):
        return out
    for pk in target_pools:
        if pk not in out:
            out[pk] = _normalize_t1_pool_payload(None)
    for d8, dnode in root.items():
        if not (isinstance(d8, str) and len(d8) == 8 and d8.isdigit()):
            continue
        sat = dnode.get("scan_attribution") if isinstance(dnode, dict) else None
        if not isinstance(sat, dict):
            continue
        t1 = sat.get("t1_settlement") if isinstance(sat.get("t1_settlement"), dict) else None
        if not isinstance(t1, dict):
            continue
        pools = t1.get("pools") if isinstance(t1.get("pools"), dict) else {}
        for pk in target_pools:
            pdata = pools.get(pk)
            if not isinstance(pdata, dict):
                continue
            n = int(_safe_float(pdata.get("sample_n", 0), 0))
            if n <= 0:
                continue
            cur = out.setdefault(pk, _normalize_t1_pool_payload(None))
            cur_n = int(cur.get("sample_n", 0) or 0)
            cur_avg = float(cur.get("avg_ret_t1_pct", 0.0) or 0.0)
            cur_win = float(cur.get("win_rate_t1_pct", 0.0) or 0.0)
            total_n = cur_n + n
            if total_n > 0:
                cur["avg_ret_t1_pct"] = round(((cur_avg * cur_n) + (float(pdata.get("avg_ret_t1_pct", 0.0) or 0.0) * n)) / total_n, 3)
                cur["win_rate_t1_pct"] = round(((cur_win * cur_n) + (float(pdata.get("win_rate_t1_pct", 0.0) or 0.0) * n)) / total_n, 1)
                cur["sample_n"] = total_n
            by_tier = pdata.get("by_tier") if isinstance(pdata.get("by_tier"), dict) else {}
            cur_bt = cur.setdefault("by_tier", {})
            for tk, tv in by_tier.items():
                if not isinstance(tv, dict):
                    continue
                tn = int(_safe_float(tv.get("n", 0), 0))
                if tn <= 0:
                    continue
                prev = cur_bt.get(tk) if isinstance(cur_bt.get(tk), dict) else {"n": 0, "avg_ret_t1_pct": 0.0, "win_rate_t1_pct": 0.0}
                p_n = int(prev.get("n", 0) or 0)
                t_n = p_n + tn
                if t_n > 0:
                    prev["avg_ret_t1_pct"] = round(((float(prev.get("avg_ret_t1_pct", 0.0)) * p_n) + (float(tv.get("avg_ret_t1_pct", 0.0) or 0.0) * tn)) / t_n, 3)
                    prev["win_rate_t1_pct"] = round(((float(prev.get("win_rate_t1_pct", 0.0)) * p_n) + (float(tv.get("win_rate_t1_pct", 0.0) or 0.0) * tn)) / t_n, 1)
                    prev["n"] = t_n
                cur_bt[tk] = prev
    return out


def _load_t1_memory_for_scan(target_pools, *, fallback=None):
    """安全加载扫描用 T+1 历史记忆；支持版本化协议与旧格式兼容。"""
    pools = list(target_pools or [])
    empty = {pk: _normalize_t1_pool_payload(None) for pk in pools}
    if not pools:
        return {}
    p = _tier_wash_path()
    if not os.path.exists(p):
        return empty
    try:
        with open(p, "r", encoding="utf-8") as f:
            root = json.load(f)
        if not isinstance(root, dict):
            return fallback if isinstance(fallback, dict) else empty

        # 新协议：scan_attribution.t1_memory
        mem_root = None
        meta = root.get("__t1_memory_protocol__")
        if isinstance(meta, dict):
            mem_root = meta.get("payload")
        if isinstance(mem_root, dict):
            schema_version = int(_safe_float(meta.get("schema_version", 0), 0))
            pools_node = mem_root.get("pools") if isinstance(mem_root.get("pools"), dict) else {}
            if schema_version >= T1_MEMORY_SCHEMA_VERSION and pools_node:
                out = {}
                for pk in pools:
                    out[pk] = _normalize_t1_pool_payload(pools_node.get(pk))
                return out

        # 旧协议：直接从 scan_attribution 历史归因中聚合
        mem = _collect_t1_memory_for_pools(root, pools)
        for pk in pools:
            mem.setdefault(pk, _normalize_t1_pool_payload(None))
        return mem
    except Exception as e:
        logging.debug("读取 t1_memory 失败，回退为空: %s", e)
        return fallback if isinstance(fallback, dict) else empty


def _encode_t1_memory_protocol(target_pools, t1_memory):
    pools = list(target_pools or [])
    payload = _default_t1_memory_payload()
    payload["pools"] = {pk: _normalize_t1_pool_payload((t1_memory or {}).get(pk)) for pk in pools}
    return {
        "schema_version": T1_MEMORY_SCHEMA_VERSION,
        "updated_at": datetime.now(BJ_TZ).isoformat(),
        "payload": payload,
    }


def _persist_scan_attribution_snapshot(res, target_pools, regime):
    """
    第3步：收益归因闭环（第一阶段）
    - 先落「当日扫描结构快照」：出票数、均分、均拥挤、执行层级分布；
    - 存入 wash_metrics_history.json 当日键下的 scan_attribution，便于 UI/人工复盘。
    """
    from core.file_utils import atomic_json_update

    p = _tier_wash_path()
    today = datetime.now(BJ_TZ).strftime("%Y%m%d")

    def _apply_attribution(root: dict) -> None:
        day = root.get(today)
        if not isinstance(day, dict):
            day = {}

        attrib = {
            "updated_at": datetime.now(BJ_TZ).isoformat(),
            "regime": str(regime or ""),
            "pools": {},
            "note": "scan结构快照（非收益结算）；用于第3步参数归因与执行复盘。",
        }

        t1_memory = _load_t1_memory_for_scan(target_pools)

        for pk in target_pools:
            rows = res.get(pk) or []
            if not isinstance(rows, list):
                rows = []
            n = len(rows)
            scores = [float(_safe_float((r or {}).get("综合分", 0.0), 0.0)) for r in rows if isinstance(r, dict)]
            crows = [_parse_crowding_score(r) for r in rows if isinstance(r, dict)]
            tier_cnt = {"A": 0, "B": 0, "C": 0, "--": 0}
            for r in rows:
                if not isinstance(r, dict):
                    continue
                t = str(r.get("执行层级", "--") or "--").strip().upper()
                if t not in tier_cnt:
                    t = "--"
                tier_cnt[t] += 1
            attrib["pools"][pk] = {
                "count_main": n,
                "avg_score": round(float(np.mean(scores)) if scores else 0.0, 2),
                "avg_crowding": round(float(np.mean(crows)) if crows else 0.0, 1),
                "tier_counts": tier_cnt,
                "t1_memory": t1_memory.get(pk, _normalize_t1_pool_payload(None)),
                "t1_memory_protocol": {
                    "schema_version": T1_MEMORY_SCHEMA_VERSION,
                    "updated_at": datetime.now(BJ_TZ).isoformat(),
                },
                # 保存轻量明细，供 T+1 结算归因（第二阶段）
                "picks": [
                    {
                        "code": str((r or {}).get("代码", "") or "").strip(),
                        "score": round(float(_safe_float((r or {}).get("综合分", 0.0), 0.0)), 2),
                        "tier": str((r or {}).get("执行层级", "--") or "--").strip().upper(),
                        "signal_px": float(_safe_float((r or {}).get("现价", 0.0), 0.0)),
                        "crowding": round(_parse_crowding_score(r), 1),
                    }
                    for r in rows[:80]
                    if isinstance(r, dict) and str((r or {}).get("代码", "") or "").strip()
                ],
            }

        obs_root = res.get("observation") if isinstance(res, dict) else {}
        if not isinstance(obs_root, dict):
            obs_root = {}
        obs_counts = {}
        for pk in target_pools:
            obs_rows = obs_root.get(pk) or []
            obs_counts[pk] = len(obs_rows) if isinstance(obs_rows, list) else 0
        attrib["observation_counts"] = obs_counts

        day["scan_attribution"] = attrib
        root[today] = day
        root["__t1_memory_protocol__"] = _encode_t1_memory_protocol(target_pools, t1_memory)

        # -------- 第二阶段：T+1 收益结算归因（轻量自动回填）--------
        try:
            from data.db_core import get_read_conn_singleton, table_exists

            if table_exists("daily_data"):
                con = get_read_conn_singleton()
                if con is not None:
                    # 仅结算最近 12 天且尚未结算的归因记录
                    day_keys = sorted([k for k in root.keys() if str(k).isdigit() and len(str(k)) == 8])[-12:]
                    for d8 in day_keys:
                        dnode = root.get(d8) if isinstance(root.get(d8), dict) else {}
                        sat = dnode.get("scan_attribution") if isinstance(dnode.get("scan_attribution"), dict) else None
                        if not isinstance(sat, dict):
                            continue
                        if sat.get("t1_settlement_done"):
                            continue

                        row_next = con.execute(
                            """
                            SELECT REPLACE(CAST(trade_date AS VARCHAR), '-', '') AS d8
                            FROM daily_data
                            WHERE REPLACE(CAST(trade_date AS VARCHAR), '-', '') > ?
                            GROUP BY 1
                            ORDER BY 1 ASC
                            LIMIT 1
                            """,
                            [str(d8)],
                        ).fetchone()
                        if not row_next or not row_next[0]:
                            continue  # 尚无下一交易日，留待下次扫描自动回填
                        next_d8 = str(row_next[0])

                        # 【性能优化 V2】T+1批量结算：将 O(N*2) 次逐行查询优化为 O(1) 批量查询
                        # 原逻辑：picks 列表每条执行 2 次 SQL（今日收盘 + 明日收盘）= O(2N) 次连接
                        # 优化：收集所有 (code6, d8) 和 (code6, next_d8) 对，一次 IN 查询完成
                        all_codes_d8 = []
                        for it in picks:
                            if not isinstance(it, dict):
                                continue
                            code6 = str(it.get("code", "") or "").strip()[:6]
                            if len(code6) != 6:
                                continue
                            all_codes_d8.append((code6, str(d8), str(next_d8)))
                        if not all_codes_d8:
                            continue
                        # 去重，避免 IN 中重复条目
                        unique_pairs = list({(c, d) for c, d, _ in all_codes_d8})
                        all_dates_d8 = list({d for _, d, _ in all_codes_d8})
                        all_dates_next = list({nd for _, _, nd in all_codes_d8})
                        if not unique_pairs or not all_dates_d8:
                            continue
                        codes_in = ",".join([f"'{c}'" for c, _ in unique_pairs])
                        dates_in = ",".join([f"'{d}'" for d in all_dates_d8])
                        dates_next_in = ",".join([f"'{d}'" for d in all_dates_next])
                        price_map = {}
                        try:
                            q = f"""
                                SELECT SUBSTR(CAST(ts_code AS VARCHAR), 1, 6) AS c6,
                                       REPLACE(CAST(trade_date AS VARCHAR), '-', '') AS td,
                                       close
                                FROM daily_data
                                WHERE SUBSTR(CAST(ts_code AS VARCHAR), 1, 6) IN ({codes_in})
                                  AND (REPLACE(CAST(trade_date AS VARCHAR), '-', '') IN ({dates_in})
                                       OR REPLACE(CAST(trade_date AS VARCHAR), '-', '') IN ({dates_next_in}))
                            """
                            rows = con.execute(q).fetchall()
                            for r in rows:
                                c6 = str(r[0] or "").strip()[:6]
                                td = str(r[1] or "").strip()[:8]
                                price_map[(c6, td)] = _safe_float(r[2], 0.0)
                        except Exception as _e_batch:
                            logging.debug("T+1批量查询失败，回退逐行: %s", _e_batch)
                            price_map = {}
                        pools = sat.get("pools") if isinstance(sat.get("pools"), dict) else {}
                        t1 = {"next_trade_date": next_d8, "pools": {}}
                        for pk, pdata in pools.items():
                            if not isinstance(pdata, dict):
                                continue
                            picks = pdata.get("picks") if isinstance(pdata.get("picks"), list) else []
                            rets = []
                            tier_bucket = {"A": [], "B": [], "C": [], "--": []}
                            for it in picks:
                                if not isinstance(it, dict):
                                    continue
                                code6 = str(it.get("code", "") or "").strip()[:6]
                                if len(code6) != 6:
                                    continue
                                c0 = price_map.get((code6, str(d8)), 0.0) if price_map else 0.0
                                c1 = price_map.get((code6, str(next_d8)), 0.0) if price_map else 0.0
                                if c0 <= 0 or c1 <= 0:
                                    # 【优化V2】批量查询失败时使用逐行兜底（保留原有逻辑作为降级路径）
                                    if not price_map:
                                        try:
                                            r0 = con.execute(
                                                """SELECT close FROM daily_data WHERE SUBSTR(CAST(ts_code AS VARCHAR), 1, 6)=?
                                                  AND REPLACE(CAST(trade_date AS VARCHAR), '-', '')=?
                                                  ORDER BY trade_date DESC LIMIT 1""",
                                                [code6, str(d8)],
                                            ).fetchone()
                                            r1 = con.execute(
                                                """SELECT close FROM daily_data WHERE SUBSTR(CAST(ts_code AS VARCHAR), 1, 6)=?
                                                  AND REPLACE(CAST(trade_date AS VARCHAR), '-', '')=?
                                                  ORDER BY trade_date DESC LIMIT 1""",
                                                [code6, str(next_d8)],
                                            ).fetchone()
                                            c0 = _safe_float(r0[0], 0.0) if r0 else 0.0
                                            c1 = _safe_float(r1[0], 0.0) if r1 else 0.0
                                        except Exception:
                                            c0, c1 = 0.0, 0.0
                                    else:
                                        c0, c1 = 0.0, 0.0
                                if c0 <= 0 or c1 <= 0:
                                    continue
                                ret = (c1 - c0) / c0 * 100.0
                                rets.append(ret)
                                tier = str(it.get("tier", "--") or "--").strip().upper()
                                if tier not in tier_bucket:
                                    tier = "--"
                                tier_bucket[tier].append(ret)
                            t1["pools"][pk] = {
                                "sample_n": len(rets),
                                "avg_ret_t1_pct": round(float(np.mean(rets)) if rets else 0.0, 3),
                                "win_rate_t1_pct": round(
                                    100.0 * float(np.mean([1.0 if x > 0 else 0.0 for x in rets])) if rets else 0.0,
                                    1,
                                ),
                                "by_tier": {
                                    tk: {
                                        "n": len(tv),
                                        "avg_ret_t1_pct": round(float(np.mean(tv)) if tv else 0.0, 3),
                                        "win_rate_t1_pct": round(
                                            100.0 * float(np.mean([1.0 if x > 0 else 0.0 for x in tv])) if tv else 0.0,
                                            1,
                                        ),
                                    }
                                    for tk, tv in tier_bucket.items()
                                },
                            }
                        sat["t1_settlement"] = t1
                        sat["t1_settlement_done"] = True
                        sat["t1_settlement_at"] = datetime.now(BJ_TZ).isoformat()
                        dnode["scan_attribution"] = sat
                        root[d8] = dnode
        except Exception as e:
            logging.debug("scan attribution T+1 settlement skipped: %s", e)

    try:
        ensure_runtime_data_layout()
        atomic_json_update(p, _apply_attribution, timeout=5)
    except Exception as e:
        logging.warning("写入 scan attribution 快照失败: %s", e)


def _ensure_pool_table_row_contract(row, hk_column_key="外资(万)"):
    """
    主池 / 观察池入表行契约：仅对缺失键补默认值，已有键顺序与取值一律以 row 为准。
    防止后续改 rt/df 合并或策略返回值时漏带「综合分、量比、真换手」等列导致 UI 表格错位。
    danger_buy / danger_sell 等专用表结构不同，禁止调用本函数。
    """
    defaults = {
        "代码": "",
        "名称": "",
        "行业": "",
        "股性": "",
        "综合分": 0.0,
        "涨幅": "--",
        "量比": "--",
        "建议仓位": "",
        "纪律防线": "",
        "真换手": "--",
        "集中度": "--",
        "现价": "--",
        "战法": "",
        "连板高度": "",
        "机构预测": "",
        "风险标签": "--",
        "建议最低分": "--",
        "操盘提示": "--",
        "pool_tier": "main",
    }
    out = {**defaults, **(row or {})}
    _hk = str(hk_column_key or "").strip() or "外资(万)"
    if _hk not in out:
        out[_hk] = "--"
    return out


def _rollback_scan_funnel_for_skipped_stock(res, target_pools):
    """
    个股在计入漏斗后因市值或 P1 分未达标而 continue 时，回滚各池 total_candidates / enter_strategy_check，
    使诊断面板与真实进入战法层的数量一致。
    """
    if not isinstance(res, dict) or "funnel" not in res:
        return
    fu = res["funnel"]
    for k in target_pools:
        if k not in fu:
            continue
        fk = fu[k]
        fk["total_candidates"] = max(0, int(fk.get("total_candidates", 0) or 0) - 1)
        fk["enter_strategy_check"] = max(0, int(fk.get("enter_strategy_check", 0) or 0) - 1)


def _scan_build_industry_pe_stats_for_p1(items):
    """
    用当前扫描批次的 base_items 估计行业 PE 分布（q75 等），供多维分项中「动态 PE」维使用；
    与 build_p1_pool 中 industry_pe_stats 构造方式一致，不写库表。
    """
    industry_map = get_all_basic_industry()
    pe_records = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ts_code = item.get("code", "")
        ind = industry_map.get(ts_code, "未知")
        hist = item.get("hist", {}) or {}
        if not isinstance(hist, dict):
            hist = {}
        pe_raw = hist.get("pe_ttm")
        if pe_raw is None or pd.isna(pe_raw) or str(pe_raw).strip() in ("", "-"):
            pe_raw = hist.get("pe", 0)
        pe = _safe_float(pe_raw)
        if pe <= 0 or pe > 500:
            continue
        pe_records.append({"ind": ind, "pe": pe})
    df_pe = pd.DataFrame(pe_records)
    global_stats = {
        "median": df_pe["pe"].median() if not df_pe.empty else 20.0,
        "q30": df_pe["pe"].quantile(0.30) if not df_pe.empty else 15.0,
        "q50": df_pe["pe"].quantile(0.50) if not df_pe.empty else 20.0,
        "q75": df_pe["pe"].quantile(0.75) if not df_pe.empty else 30.0,
        "q80": df_pe["pe"].quantile(0.80) if not df_pe.empty else 35.0,
    }
    industry_pe_stats = {}
    if not df_pe.empty:
        for ind, group in df_pe.groupby("ind"):
            if len(group) >= 10:
                industry_pe_stats[ind] = {
                    "median": group["pe"].median(),
                    "q30": group["pe"].quantile(0.30),
                    "q50": group["pe"].quantile(0.50),
                    "q75": group["pe"].quantile(0.75),
                    "q80": group["pe"].quantile(0.80),
                }
            else:
                industry_pe_stats[ind] = global_stats
    return industry_map, industry_pe_stats, global_stats


# ==================== 5. 主中枢调度与打分引擎 ====================
def run_scan_engine(target_pools, base_items, regime="震荡市", progress_callback=None):
    # 策略阈值：P2–P5 引擎 run_all 内每轮从 config_manager 读取（见 strat_p*_*.py）
    if not base_items: return {}
    p1_min_circ_mv_yi = _p1_scan_min_circ_mv_yi()
    res = {k: [] for k in target_pools}
    
    res['danger_buy'] = []
    res['danger_sell'] = []
    # 诊断漏斗：按池子统计每一层剩余数量（只读诊断，不参与策略计算）
    res['funnel'] = {
        k: {
            "total_candidates": 0,
            "enter_strategy_check": 0,
            "pass_golden_gate": 0,
            "hit_strategy": 0,
            "pass_score": 0,
            "gate_block_reasons": {},
        } for k in target_pools
    }
    # P1 初筛统计：市值回滚次数、平滑分拦截次数（与 pool_manager 淘汰文案对齐）
    res["p1_prescreen"] = {
        "pass_line": 50.0,
        "smooth_blocked": 0,
        "mv_skipped_rollbacks": 0,
        "gate_reasons": {},
    }
    # 【多层级池子】说明：P2–P5 震荡观察池（缩量期备选），与主池分列；最终是否保留由文末 real_fill 再校验
    res["observation"] = {k: [] for k in target_pools}
    active_target_pools = list(target_pools)

    # 扫描前数据新鲜度规则：
    # - P2/P3/P4：若落后于上一交易日，先自动增量同步；仍落后则停用。
    # - P5：允许任意时刻扫描，但结果必须基于「最后一个交易日盘后已落库」的日线；若未对齐则先自动增量同步，仍不对齐则停用。
    today_cal = datetime.now(BJ_TZ).strftime("%Y%m%d")
    is_trading_day = True
    try:
        from core.master_control import is_maintenance_mode_enabled
        from data import data_fetcher as _scan_data_fetcher

        if not is_maintenance_mode_enabled():
            is_trading_day = bool(_scan_data_fetcher.check_data_completeness(days=1, static_mode=True)[0])
    except Exception:
        is_trading_day = True
    expected_prev_td = _expected_latest_daily_anchor_for_intraday(today_cal)
    latest_trade_date = str(get_latest_daily_data_trade_date_yyyymmdd() or "").replace("-", "")[:8]

    p234_requested = [k for k in ("p2", "p3", "p4") if k in target_pools]
    p5_requested = "p5" in target_pools

    p234_need_sync = (
        bool(p234_requested)
        and len(expected_prev_td) == 8
        and expected_prev_td.isdigit()
        and (
            len(latest_trade_date) != 8
            or (not latest_trade_date.isdigit())
            or latest_trade_date < expected_prev_td
        )
    )
    p5_required_trade_date = expected_prev_td or today_cal
    p5_need_sync = bool(p5_requested) and (
        (len(latest_trade_date) != 8)
        or (not latest_trade_date.isdigit())
        or (len(p5_required_trade_date) == 8 and p5_required_trade_date.isdigit() and latest_trade_date < p5_required_trade_date)
    )

    if p234_need_sync or p5_need_sync:
        _ = _try_auto_sync_recent_days_for_scan(progress_callback=progress_callback)
        latest_trade_date = str(get_latest_daily_data_trade_date_yyyymmdd() or "").replace("-", "")[:8]

    if p234_requested:
        p234_stale = (
            (len(expected_prev_td) != 8)
            or (not expected_prev_td.isdigit())
            or (len(latest_trade_date) != 8)
            or (not latest_trade_date.isdigit())
            or (latest_trade_date < expected_prev_td)
        )
        if p234_stale:
            active_target_pools = [k for k in active_target_pools if k not in p234_requested]
            msg = (
                "P2~P4停用：日线未对齐到扫描基准日"
                f"（latest={latest_trade_date or 'N/A'}，required>={expected_prev_td or 'N/A'}）"
            )
            logging.warning(msg)
            if progress_callback:
                try:
                    progress_callback(f"⏸️ {msg}")
                except Exception:
                    pass
            for pk in p234_requested:
                if pk in res.get("funnel", {}):
                    gr = res["funnel"][pk]["gate_block_reasons"]
                    gr[msg] = int(gr.get(msg, 0)) + 1

    if p5_requested and p5_need_sync:
        active_target_pools = [k for k in active_target_pools if k != "p5"]
        now_bj = datetime.now()
        curr_min = now_bj.hour * 60 + now_bj.minute
        if curr_min < 15 * 60:
            hint = "盘前/盘中启动：等待收盘日落库"
        else:
            hint = "盘后启动：可直接扫描，无需再判日线是否落后"
        msg = (
            f"P5停用：{hint}"
            f"（latest={latest_trade_date or 'N/A'}，required>={p5_required_trade_date or 'N/A'}；收盘日={p5_required_trade_date or 'N/A'}）"
        )
        logging.warning(msg)
        if progress_callback:
            try:
                progress_callback(f"⏸️ {msg}")
            except Exception:
                pass
        if "p5" in res.get("funnel", {}):
            gr = res["funnel"]["p5"]["gate_block_reasons"]
            gr[msg] = int(gr.get(msg, 0)) + 1

    try:
        from core.sop_v11 import evaluate_market_circuit_breaker

        res["sop_market_breaker"] = evaluate_market_circuit_breaker()
    except Exception:
        res["sop_market_breaker"] = {}

    # 综合分参考线 60：仅用于排序、漏斗「质量档」与 signal_log 写入；命中战法后一律进主池表（见下方入池逻辑）
    # 交易纪律：UI 提示综合分 ≥85 再动手，不作为硬门槛
    # P2–P5 统一底线：综合分 >= 30 才允许企微推送；< 30 或负分仅保留在扫描结果里供观察/复盘。
    min_pass_map = {'p2': 30.0, 'p3': 30.0, 'p4': 30.0, 'p5': 30.0}
    # 🐉 核心修改区：加入 P5 引擎的权重映射，P5 与 P4 共用 0.95 极高优选权重
    pool_weight_map = {'p2': 0.92, 'p3': 1.0, 'p4': 0.95, 'p5': 0.95}
    
    holding_days_map = {}
    try:
        from data.db_core import get_read_conn, table_exists

        if table_exists("signal_log"):
            recent_date = (datetime.now(BJ_TZ) - timedelta(days=15)).strftime("%Y-%m-%d")
            query = """
                SELECT ts_code, COUNT(DISTINCT trade_date) AS sig_days
                FROM signal_log
                WHERE trade_date >= ?
                GROUP BY ts_code
            """
            # 短连接查库：仅在 with 内取 DataFrame，立即释放底层读句柄；不得在内层做 iterrows 等耗时遍历
            df_sig = None
            with get_read_conn(read_only=True) as con:
                df_sig = con.execute(query, [recent_date]).df()
            # 空表 / 缺列时不得 iterrows 盲取，避免静默 KeyError 中断整轮扫描
            if df_sig is not None and not df_sig.empty and "ts_code" in df_sig.columns and "sig_days" in df_sig.columns:
                holding_days_map = {
                    str(row.ts_code): row.sig_days
                    for row in df_sig.itertuples(index=False)
                    if getattr(row, "ts_code", None)
                }
            elif df_sig is not None and not df_sig.empty:
                logging.warning("signal_log 聚合结果缺 ts_code/sig_days 列，跳过持仓日数映射")
    except Exception as e:
        logging.error(f"批量获取信号日志失败: {e}")

    if progress_callback: progress_callback("📡 正在探测全市场板块共振信号...")
    if is_trading_day:
        sector_ranking_dict = get_realtime_sector_ranking()
    else:
        try:
            from core.regime_analyzer import get_latest_sector_ranking

            sector_ranking_dict = get_latest_sector_ranking() or {}
        except Exception:
            sector_ranking_dict = {}
    sorted_sectors = list(sector_ranking_dict.keys())
    sector_rank_map = {ind: idx + 1 for idx, ind in enumerate(sorted_sectors)}
    total_sectors = len(sorted_sectors)

    # ---------- P3/P4 右侧直通车（Momentum Fast-Lane）----------
    # 在单次 fetch_realtime_batch 中合并「底仓代码 + 非底仓探测列表」，避免对 DuckDB daily 全表扫描；
    # 直通车标的仅走增量 API 快照筛选 + 逐只 get_stock_data_qfq（与主循环一致）。
    base_items_for_scan = list(base_items)
    rt_map_mfl = None
    if is_trading_day and ("p3" in active_target_pools or "p4" in active_target_pools):
        try:
            from core.pool_manager import (
                build_momentum_fast_lane_base_items,
                compute_momentum_fast_lane_probe_codes,
                extend_fast_lane_with_limit_streak_hits,
                merge_scan_base_with_fast_lane_items,
                select_momentum_fast_lane_ts_codes,
            )

            _bc = [
                str(x["code"]).strip()
                for x in base_items
                if isinstance(x, dict) and x.get("code")
            ]
            _probe = compute_momentum_fast_lane_probe_codes(_bc, sorted_sectors)
            if progress_callback and _probe:
                progress_callback(
                    f"🚀 P3/P4 右侧直通车：增量探测 {len(_probe)} 只非底仓标的（并行快照，无 daily 全表扫描）…"
                )
            _fetch_union = list(dict.fromkeys(_bc + _probe))
            rt_map_mfl = fetch_realtime_batch(_fetch_union) or {}
            _base_frozen = frozenset(_bc)
            _picked = select_momentum_fast_lane_ts_codes(_probe, rt_map_mfl, _base_frozen)
            _extra_streak = extend_fast_lane_with_limit_streak_hits(
                _probe,
                rt_map_mfl,
                _base_frozen,
                frozenset(_picked),
                max_add=12,
            )
            _picked_all = list(dict.fromkeys(list(_picked) + list(_extra_streak)))
            _fl_items = build_momentum_fast_lane_base_items(_picked_all)
            for _it in _fl_items:
                if isinstance(_it, dict):
                    _it.setdefault("pool_tier", "fastlane")
                    _it.setdefault("pool_source", "直通车")
            base_items_for_scan = merge_scan_base_with_fast_lane_items(base_items, _fl_items)
        except Exception as _e_mfl:
            logging.warning("P3/P4 右侧直通车合并失败，回退为纯底仓扫描: %s", _e_mfl)
            base_items_for_scan = list(base_items)
            rt_map_mfl = None

    try:
        from core.config_manager import get_p1_regime_thresholds

        p1_pass_line_scan = float(get_p1_regime_thresholds(regime).get("pass_line", 50.0))
    except Exception:
        p1_pass_line_scan = 50.0
    res["p1_prescreen"]["pass_line"] = float(p1_pass_line_scan)
    industry_map_scan, industry_pe_stats_scan, global_stats_scan = _scan_build_industry_pe_stats_for_p1(
        base_items_for_scan
    )
    try:
        from core.pool_manager import _get_dynamic_strategic_industries

        dynamic_industries_scan = _get_dynamic_strategic_industries(sector_ranking_dict)
    except Exception as _e_dyn:
        logging.debug("扫描引擎动态行业贝塔失败: %s", _e_dyn)
        dynamic_industries_scan = {}

    codes = [
        x["code"]
        for x in base_items_for_scan
        if isinstance(x, dict) and x.get("code")
    ]
    
    global_hk_label = "外资(万)"
    if base_items and isinstance(base_items[0], dict):
        first_hist = base_items[0].get('hist', {}) or {}
        if not isinstance(first_hist, dict): first_hist = {}
        first_date = str(first_hist.get('trade_date', ''))
        if '-' in first_date: global_hk_label = f"{first_date[5:]}外资(万)"
        elif len(first_date) == 8: global_hk_label = f"{first_date[4:6]}-{first_date[6:8]}外资(万)"
            
    if progress_callback: progress_callback(f"⚡ 定向并发拉取盘口中 (调用异步高并发引擎)...")
    if rt_map_mfl is not None:
        rt_map = rt_map_mfl
    else:
        rt_map = fetch_realtime_batch(codes) or {}
    # 【自适应优化】主扫描入口：全样本市场情绪收缩度（无 rt 时仍可根据日线样本估计，便于日志复盘）
    _mctx = compute_market_contraction_context(base_items, rt_map)
    market_contraction_score = float(_mctx.get("score", 0.0) or 0.0)
    if not is_trading_day:
        market_contraction_score = 0.0
    res["market_contraction_score"] = market_contraction_score
    res["adaptive_reason"] = str(_mctx.get("adaptive_reason", "") or "")
    # 【全局审计修复】复盘：透出有效换手样本数，便于核对是否达到 30 只门槛
    res["adaptive_sample_count"] = int(_mctx.get("sample_count", 0) or 0)
    # 【多层级池子】文末仍可按缩量语境清理 observation（当前命中战法已统一进主池）
    prev_scan_streak = _load_prev_scan_empty_streak()
    if not is_trading_day:
        prev_scan_streak = 0
    # 【全局审计·rt 断流】原先 rt_map 为空则整段 return，P2–P5 完全无输出；与下方「单票不在 rt_map 则用 hist 合成最小 rt」语义矛盾。
    # 全市场接口偶发 {} 时改为降级为空 dict，由循环内逐只补齐，避免静默空扫描；缩量语境仍以上文 base_items+（可能为空的）rt 已估计完毕。
    if not rt_map:
        rt_map = {}

    snapshot_file = path_intraday_snapshots_json()
    now_time = datetime.now(BJ_TZ)
    today_str = now_time.strftime("%Y%m%d")
    curr_min = now_time.hour * 60 + now_time.minute

    try:
        _breakout_vwap_eps_danger = _load_breakout_vwap_eps_for_danger_buy()
    except Exception:
        _breakout_vwap_eps_danger = 0.004
    
    snapshots = {}
    if os.path.exists(snapshot_file):
        try:
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                saved_date = data.get("date", "")
                if saved_date == today_str or curr_min < 565:
                    snapshots = data.get("stocks", {})
        except json.JSONDecodeError as e:
            logging.warning(f"扫描引擎加载快照 JSON 失败 [{snapshot_file}]: {e}")
        except OSError as e:
            logging.warning(f"扫描引擎加载快照 IO 失败 [{snapshot_file}]: {e}")
        except Exception as e:
            logging.warning(f"扫描引擎加载快照失败（未分类）[{snapshot_file}]: {e}")

    # T+1 记忆必须在主循环前显式初始化，避免 P2 分支引用未定义局部变量导致整票跳过。
    t1_memory = _load_t1_memory_for_scan(active_target_pools)
    if not is_trading_day:
        t1_memory = {pk: _normalize_t1_pool_payload(None) for pk in active_target_pools}

    # ---------- 综合分·截面 Rank：同一趟扫描内对涨幅/量比/真换手/主力推力做百分位秩，供性格乘子与爆发压制 ----------
    rank_lookup = {}
    try:
        _iah_cs = (curr_min > 900 or curr_min < 565)
        _surf_rows = []
        for _sit in base_items_for_scan:
            if not isinstance(_sit, dict):
                continue
            _sx = _extract_scan_surface_metrics(_sit, rt_map, curr_min, _iah_cs, today_str, snapshots)
            if _sx:
                _surf_rows.append(_sx)
        if _surf_rows:
            _csdf = pd.DataFrame(_surf_rows)
            for _cc in ("pct", "vol_ratio", "turnover_f", "main_ratio"):
                if _cc in _csdf.columns:
                    _csdf[_cc] = pd.to_numeric(_csdf[_cc], errors="coerce").replace([np.inf, -np.inf], np.nan)
                    if _csdf[_cc].notna().any():
                        _csdf[_cc] = _csdf[_cc].fillna(_csdf[_cc].median())
                    else:
                        _csdf[_cc] = _csdf[_cc].fillna(0.0)
            _csdf = percentile_ranks(_csdf, ["vol_ratio", "turnover_f", "pct", "main_ratio"])
            rank_lookup = build_rank_lookup(_csdf)
            if not is_trading_day:
                rank_lookup = {k: {"pct_r": 0.5, "vol_ratio_r": 0.5, "turnover_f_r": 0.5, "main_ratio_r": 0.5} for k in rank_lookup.keys()}
    except Exception as _rke:
        logging.debug("综合分·截面 Rank 构建失败(退化为中性分位): %s", _rke)
        rank_lookup = {}

    total_items = len(base_items_for_scan)
    funnel = res.get("funnel", {})
    active_funnel_keys = [k for k in active_target_pools if k in funnel]
    funnel_total_candidates = {k: funnel[k]["total_candidates"] for k in active_funnel_keys}
    funnel_enter_strategy_check = {k: funnel[k]["enter_strategy_check"] for k in active_funnel_keys}
    for i, item in enumerate(base_items_for_scan):
        if not isinstance(item, dict): continue
        for k in active_funnel_keys:
            funnel_total_candidates[k] += 1
            funnel_enter_strategy_check[k] += 1
        if progress_callback and i % max(1, total_items // 10) == 0: 
            progress_callback(f"🚀 运算与战法匹配: [{i}/{total_items}]...")
            
        try:
            full_code = item.get('code', '')
            s_code = str(full_code).split('.')[0][:6]
            
            if s_code not in rt_map: 
                fallback_hist = item.get('hist', {})
                if isinstance(fallback_hist, dict) and fallback_hist:
                    _fb_pc = _safe_float(fallback_hist.get("pre_close"), 0.0)
                    if _fb_pc <= 0:
                        _fb_pc = _safe_float(fallback_hist.get("close"), 0.0)
                    rt_map[s_code] = {
                        'price': fallback_hist.get('close', 0),
                        'pre_close': _fb_pc,
                        'open': fallback_hist.get('open', 0),
                        'volume': 0,
                        'high': fallback_hist.get('high', 0),
                        'low': fallback_hist.get('low', 0),
                        'name': normalize_stock_display_name(fallback_hist.get('name', s_code)),
                    }
                else: continue
                    
            rt = rt_map.get(s_code, {})
            if not isinstance(rt, dict): rt = {}
            
            sn_row = snapshots.get(s_code, {})
            if isinstance(sn_row, dict):
                _apply_intraday_snapshots_to_rt(rt, sn_row)

            anchor_vol, anchor_t = _tail_volume_anchor(sn_row, curr_min)
            if (
                anchor_vol is not None
                and anchor_t is not None
                and curr_min > anchor_t
            ):
                delta_vol = _safe_float(rt.get("volume", 0)) - _safe_float(anchor_vol, 0)
                delta_time = curr_min - anchor_t
                if delta_time > 0 and delta_vol > 0:
                    tail_vol_per_min = delta_vol / delta_time
                    # 【V26.6 A股尾盘优化】14:30后用"尾盘前段均量"作为基准，更精准识别尾盘异动
                    if 870 <= curr_min <= 900:
                        # 尾盘前段(13:00-14:30)均量作为基准
                        vol_1330 = _safe_float(sn_row.get("vol_1325", 0), 0.0)
                        t_1330 = _safe_float(sn_row.get("tmin_1325", 795), 0.0)  # 13:25 ≈ 805
                        if vol_1330 > 0 and t_1330 > 0:
                            pre_tail_mins = curr_min - int(t_1330)
                            if pre_tail_mins > 30:
                                pre_tail_avg = vol_1330 / pre_tail_mins
                                if pre_tail_avg > 1e-9:
                                    rt["tail_vol_ratio"] = tail_vol_per_min / max(pre_tail_avg, 1e-9)
                                    # 标注基准类型，便于诊断
                                    rt["_tail_baseline_type"] = "pretail"
                                else:
                                    _elapsed_safe = max(float(max(1, curr_min - 780)), 1e-9)
                                    avg_vol_per_min = _safe_float(rt.get("volume", 0)) / _elapsed_safe
                                    if avg_vol_per_min > 1e-9:
                                        rt["tail_vol_ratio"] = tail_vol_per_min / max(avg_vol_per_min, 1e-9)
                                        rt["_tail_baseline_type"] = "dayavg"
                            else:
                                _elapsed_safe = max(float(max(1, curr_min - 780)), 1e-9)
                                avg_vol_per_min = _safe_float(rt.get("volume", 0)) / _elapsed_safe
                                if avg_vol_per_min > 1e-9:
                                    rt["tail_vol_ratio"] = tail_vol_per_min / max(avg_vol_per_min, 1e-9)
                                    rt["_tail_baseline_type"] = "dayavg"
                        else:
                            # 无1325快照时退化为全日均量
                            elapsed = max(1, curr_min - 780)
                            avg_vol_per_min = _safe_float(rt.get("volume", 0)) / max(float(elapsed), 1e-9)
                            if avg_vol_per_min > 1e-9:
                                rt["tail_vol_ratio"] = tail_vol_per_min / max(avg_vol_per_min, 1e-9)
                                rt["_tail_baseline_type"] = "dayavg"
                    else:
                        if 570 <= curr_min <= 690:
                            elapsed = curr_min - 570
                        elif 780 <= curr_min <= 900:
                            elapsed = 120 + (curr_min - 780)
                        elif curr_min > 900:
                            elapsed = 240
                        else:
                            elapsed = 1
                        _elapsed_safe = max(float(elapsed), 1e-9)
                        avg_vol_per_min = _safe_float(rt.get("volume", 0)) / _elapsed_safe
                        if avg_vol_per_min > 1e-9:
                            rt["tail_vol_ratio"] = tail_vol_per_min / max(avg_vol_per_min, 1e-9)
                            rt["_tail_baseline_type"] = "dayavg"
            
            hist = item.get('hist', {})
            if not isinstance(hist, dict):
                hist = {}

            df_target = item.get('df')
            if df_target is None or df_target.empty:
                continue

            merged_flag = False
            try:
                df_m, merged_flag = merge_daily_with_realtime(df_target, rt)
                if merged_flag:
                    df_target = df_m
                    item["df"] = df_target
            except Exception as e:
                logging.debug("merge_daily_with_realtime 跳过 %s: %s", s_code, e)

            # daily_data 宽表不落库 max_60d_pct（见 data_fetcher.ALL_55_COLS）；该列由 precompute_indicators 补算。
            # 旧版底仓 JSON 或仅 DB 列时若跳过 precompute，P2–P4 的 max_60d_pct 门槛会误读为 0。
            _need_pre = (
                merged_flag
                or ("ma20" not in df_target.columns)
                or ("max_60d_pct" not in df_target.columns)
            )
            if _need_pre:
                try:
                    df_target = precompute_indicators(df_target)
                    item["df"] = df_target
                except Exception as e:
                    logging.warning("指标计算失败 %s: %s", s_code, e)
                    continue

            df_tail_60 = df_target.tail(60).copy()
            df_tail_5 = df_target.tail(5).copy()

            vol_col = 'vol' if 'vol' in df_target.columns else ('volume' if 'volume' in df_target.columns else None)
            if vol_col is None:
                logging.debug(f"扫描跳过 {full_code}: 无成交量列")
                continue
    
            circ_mv_wan = 0.0
            if 'circ_mv' in df_target.columns:
                circ_mv_wan = _safe_float(df_target['circ_mv'].iloc[-1])
            else:
                circ_mv_raw = rt.get('circ_mv')
                if circ_mv_raw is None or pd.isna(circ_mv_raw): circ_mv_raw = rt.get('total_mv', 10000000) * 0.6
                circ_mv_wan = _safe_float(circ_mv_raw, default=10000000)
                
            circ_mv_yi = circ_mv_wan / 10000.0
    
            if circ_mv_yi >= 2000.0:
                size_emoji, size_label = "🦍", "巨无霸"
                size_pool_boost, size_safety_mult, size_penalty_factor = 1.08, 1.06, 0.82
            elif circ_mv_yi >= 1000.0:
                size_emoji, size_label = "🐘", "千亿中军"
                size_pool_boost, size_safety_mult, size_penalty_factor = 1.05, 1.04, 0.86
            elif circ_mv_yi >= 500.0:
                size_emoji, size_label = "🐘", "超级中军"
                size_pool_boost, size_safety_mult, size_penalty_factor = 1.03, 1.02, 0.90
            elif circ_mv_yi >= _p1_scan_min_circ_mv_yi():
                size_emoji, size_label = "🐎", "核心中盘"
                size_pool_boost, size_safety_mult, size_penalty_factor = 1.00, 1.00, 1.00
            else: size_emoji, size_label = "🐥", "袖珍盘"
            # 与 P1 一致：流通市值低于选股下限不进入战法与结果池；漏斗已在循环头计数，此处回滚以免诊断虚高
            if circ_mv_yi < p1_min_circ_mv_yi:
                _rollback_scan_funnel_for_skipped_stock(res, active_target_pools)
                res["p1_prescreen"]["mv_skipped_rollbacks"] = int(
                    res["p1_prescreen"].get("mv_skipped_rollbacks", 0) or 0
                ) + 1
                continue

            if regime in ["情绪退潮市", "主跌浪", "退潮防守"]: 
                w_gene, w_burst = 0.60, 0.40
                if size_emoji == "🐎": regime_mult = 0.70  
                elif size_emoji in ["🦍", "🐘"]: regime_mult = 1.20 
                else: regime_mult = 0.85
            elif regime in ["趋势市", "主升浪", "趋势主升"]: 
                w_gene, w_burst = 0.30, 0.70
                if size_emoji == "🐎": regime_mult = 1.20 
                elif size_emoji in ["🦍", "🐘"]: regime_mult = 0.90 
                else: regime_mult = 1.15
            else: 
                w_gene, w_burst = 0.40, 0.60
                regime_mult = 1.0
                
            industry = get_stock_industry(full_code)
            
            sector_mult = 1.0
            is_core_sector = False
            ind_rank_str = "未上榜"
            ind_rank = 999
            is_bottom_3 = False
            
            rank = sector_rank_map.get(industry)
            if rank is not None:
                ind_rank = rank
                if rank <= 3: 
                    sector_mult = 1.15
                    is_core_sector = True
                    ind_rank_str = f"实时第{rank}名"
                elif rank <= 8: 
                    sector_mult = 1.05
                    ind_rank_str = f"实时第{rank}名"
                elif total_sectors >= 3 and rank > total_sectors - 3:
                    sector_mult = 0.60 
                    is_bottom_3 = True
                    ind_rank_str = f"领跌倒数{total_sectors - rank + 1}"
                elif total_sectors >= 8 and rank > total_sectors - 8:
                    sector_mult = 0.85 
                    ind_rank_str = f"弱势倒数{total_sectors - rank + 1}"
                else:
                    ind_rank_str = f"实时第{rank}名"
            
            stock_name = normalize_stock_display_name(
                rt.get("name") or hist.get("name") or str(s_code)
            )
            
            now_price = _safe_float(rt.get('price'))
            if now_price <= 0 and curr_min < 565:
                 now_price = _safe_float(hist.get('close', 0.0))
            elif now_price <= 0:
                 now_price = _safe_float(hist.get('close', 0.0))
    
            pre_price = _safe_float(rt.get("pre_close"), 0.0)
            if pre_price <= 0:
                pre_price = _safe_float(hist.get("pre_close"), 0.0)
            if pre_price <= 0:
                pre_price = _safe_float(hist.get("close"), 0.0)
            if pre_price <= 0:
                continue
            pct = (now_price - pre_price) / pre_price * 100.0
            
            # 【V26.6 A股特殊场景】涨跌停识别 + 集合竞价时段优化
            limit_up = pre_price > 0 and pct >= 9.9
            limit_down = pre_price > 0 and pct <= -9.9
            if limit_up or limit_down:
                rt['_is_limit'] = 'UP' if limit_up else 'DOWN'
                rt['_limit_tag'] = '🔴涨停' if limit_up else '🟢跌停'
            else:
                rt['_is_limit'] = None
                rt['_limit_tag'] = ''

            # 集合竞价时段(9:25-9:30)修正：竞价期成交量已并入当日数据，但量比基准应特殊处理
            is_auction = 565 <= curr_min < 570
            if is_auction:
                # 竞价期参考昨日均量（而非5日均量），竞价成交量通常只占全日1-3%
                yesterday_vol = _safe_float(hist.get('vol'), 0.0)
                if yesterday_vol > 0:
                    rt['_auction_vol_ref'] = yesterday_vol / 100.0  # 转手
                    rt['_auction_tag'] = '集合竞价'
            
            vol_shares = _safe_float(rt.get('volume', 0.0))
            is_after_hours = (curr_min > 900 or curr_min < 565)
            
            ma5_vol_hand = _intraday_ma5_vol_baseline_hand(df_target, vol_col)

            vol_ratio_tag = "F"
            if vol_shares <= 0 or curr_min < 565:
                rt['vol_ratio'] = _safe_float(hist.get('vol_ratio', hist.get('vr', 1.0)), default=1.0)
                _trf = _safe_float(hist.get('turnover_rate_f'), 0.0)
                if _trf <= 0:
                    _vh = _safe_float(hist.get('vol', df_target[vol_col].iloc[-1] if vol_col else 0), 0.0)
                    _cl = _safe_float(hist.get('close', now_price), 0.0)
                    _trf = infer_turnover_rate_f_pct(_vh, _cl, circ_mv_wan)
                rt['turnover_rate_f'] = _trf
                vol_ratio_tag = "H" if ('vol_ratio' in hist or 'vr' in hist) else "F"
            else:
                vol_hand = vol_shares / 100.0
                vol_wan_shares = vol_shares / 10000.0
                
                if is_after_hours:
                    rt['vol_ratio'] = vol_hand / ma5_vol_hand
                else:
                    # 【V26.6 A股集合竞价优化】竞价期用昨日均量替代5日均量作为基准
                    if is_auction and yesterday_vol > 0:
                        # 竞价期成交量已并入当日，用昨日均量作为基准
                        auction_vol_baseline = rt.get('_auction_vol_ref', ma5_vol_hand)
                        rt['vol_ratio'] = min(vol_hand / auction_vol_baseline, 50.0)
                    elif 570 <= curr_min <= 900:
                        elapsed_mins = max(1, curr_min - 570)
                        rt['vol_ratio'] = min((vol_hand / elapsed_mins) / (ma5_vol_hand / 240), 50.0)
                    else:
                        elapsed_mins = 240
                        rt['vol_ratio'] = min((vol_hand / elapsed_mins) / (ma5_vol_hand / 240), 50.0)
                vol_ratio_tag = "R"
                
                # 【V26.6 优化】优先使用成交额/成交量计算VWAP均价，而非当前价
                # VWAP更能反映当日平均成交成本，对换手率计算更准确
                rt_amount = _safe_float(rt.get('amount', 0.0))
                if rt_amount > 0 and vol_shares > 0:
                    # VWAP = 成交额(元) / 成交量(股) — 注意amount单位与volume单位匹配
                    # amount从东财接口获取，单位为元；volume单位为股
                    vwap_avg_price = rt_amount / vol_shares
                    rt['vwap'] = vwap_avg_price  # 供 P3/P4 screener 的 rt.get('vwap') 直接读取
                    calc_price = vwap_avg_price
                else:
                    rt['vwap'] = now_price if now_price > 0 else _safe_float(hist.get('close', 10), default=10.0)
                    calc_price = rt['vwap']
                # 【审计修复】维度2：用价作分母时兜底，避免 circ/price 异常
                _cp = max(calc_price, 1e-9)
                float_shares_wan = circ_mv_wan / _cp

                if float_shares_wan <= 0:
                    float_shares_wan = 1.0
                # 与 fund_mv_utils 一致：真实换手 = vol(手)*close/circ_mv(万)，不用总股本换手
                rt["turnover_rate_f"] = min(infer_turnover_rate_f_pct(vol_hand, calc_price, circ_mv_wan), 100.0)

            _vr_fix, vol_ratio_tag = _sanitize_rt_vol_ratio(rt, df_target, hist, vol_col, vol_ratio_tag)
            rt["vol_ratio"] = _vr_fix
            
            rt['price'] = now_price
            rt['open'] = _safe_float(rt.get('open', now_price))
            # 昨收 K 线末行：北向/筹码等优先与 df 对齐
            y_bar = df_target.iloc[-1]
            if "capital_resonance_score" in df_target.columns:
                rt["capital_resonance_score"] = _safe_float(y_bar.get("capital_resonance_score", 0.0), 0.0)
            else:
                rt["capital_resonance_score"] = _safe_float(hist.get("capital_resonance_score", 0.0), 0.0)
            for key in ['net_elg_amount', 'cost_50th', 'cost_95th']:
                rt[key] = _safe_float(hist.get(key, 0.0))
            # 北向持股(股)：优先实时；实时为 0 或未推送时用昨收 K / hist，表格加注「昨」提示
            _hk_rt_raw = max(0.0, _safe_float(rt.get("hk_vol", 0.0), 0.0))
            _hk_y = (
                max(0.0, _safe_float(y_bar.get("hk_vol"), 0.0))
                if "hk_vol" in df_target.columns
                else 0.0
            )
            _hk_h = max(0.0, _safe_float(hist.get("hk_vol", 0.0), 0.0))
            if _hk_rt_raw > 0:
                rt["hk_vol"] = _hk_rt_raw
                _hk_disp_suffix = ""
            elif _hk_y > 0:
                rt["hk_vol"] = _hk_y
                _hk_disp_suffix = "昨"
            elif _hk_h > 0:
                rt["hk_vol"] = _hk_h
                _hk_disp_suffix = "昨"
            else:
                rt["hk_vol"] = 0.0
                _hk_disp_suffix = ""
            # 昨收 K 线筹码字段：实时包里常缺失，必须从 df 末行补全，否则黄金门禁误杀
            if 'winner_rate' in df_target.columns:
                rt['winner_rate'] = _safe_float(y_bar.get('winner_rate'), rt.get('winner_rate', 0.0))
            if 'avg_cost' in df_target.columns and _safe_float(y_bar.get('avg_cost'), 0.0) > 0:
                rt['avg_cost'] = _safe_float(y_bar.get('avg_cost'), 0.0)
            rz_net_buy_hist = _safe_float(hist.get('rz_net_buy', 0.0))
                
            cyq_raw = hist.get('cyq_concentration')
            rt['cyq_concentration'] = _safe_float(cyq_raw, default=999.0) if _safe_float(cyq_raw) > 0 else 999.0
            
            limit_times_val = int(_safe_float(hist.get('limit_times', 0)))
            rt['limit_times'] = limit_times_val
            rt['forecast_type'] = int(_safe_float(hist.get('forecast_type', 0)))
            
            cyq_str = f"{rt['cyq_concentration']:.1f}" if rt['cyq_concentration'] < 900 else "--"

            # 【V26.6 优化】在行 2601-2602 已预计算 df_tail_60 和 df_tail_5，
            # 此处 df_tail5_p1 复用 df_tail_5 而非重新 tail(5) 调用，
            # 避免主扫描循环中每只股票重复执行 .tail() 切片操作
            df_tail5_p1 = df_tail_5
            try:
                vol5_p = pd.to_numeric(df_tail5_p1["vol"], errors="coerce").fillna(0)
                close5_p = pd.to_numeric(df_tail5_p1["close"], errors="coerce").fillna(0)
                cm5_p = pd.to_numeric(df_tail5_p1["circ_mv"], errors="coerce").fillna(0)
                tr_f5_p = pd.to_numeric(df_tail5_p1["turnover_rate_f"], errors="coerce").fillna(0)
                inferred_tr5_p = np.where(
                    (tr_f5_p > 0) | (cm5_p <= 0) | (close5_p <= 0),
                    tr_f5_p,
                    vol5_p * 100 / np.maximum(cm5_p, 1e-9),
                )
                _tr5_arr = np.array(inferred_tr5_p, dtype=float)
                _tr5_arr = _tr5_arr[np.isfinite(_tr5_arr)]
                avg_trn_scan_row = float(np.mean(_tr5_arr)) if len(_tr5_arr) > 0 else 0.0
            except Exception:
                # 【性能优化 V3】向量化替代 iterrows fallback
                # 主逻辑已完成向量化计算，此处为异常兜底
                try:
                    vol5_arr = pd.to_numeric(df_tail5_p1["vol"], errors="coerce").fillna(0).to_numpy()
                    close5_arr = pd.to_numeric(df_tail5_p1["close"], errors="coerce").fillna(0).to_numpy()
                    cm5_arr = pd.to_numeric(df_tail5_p1["circ_mv"], errors="coerce").fillna(0).to_numpy()
                    tr_f5_arr = pd.to_numeric(df_tail5_p1["turnover_rate_f"], errors="coerce").fillna(0).to_numpy()
                    # 向量化计算换手率
                    with np.errstate(divide='ignore', invalid='ignore'):
                        inferred_tr5_vec = np.where(
                            (tr_f5_arr > 0) | (cm5_arr <= 0) | (close5_arr <= 0),
                            tr_f5_arr,
                            vol5_arr * 100 / np.maximum(cm5_arr, 1e-9),
                        )
                    inferred_tr5_vec = np.where(np.isfinite(inferred_tr5_vec), inferred_tr5_vec, np.nan)
                    valid_tr5 = inferred_tr5_vec[~np.isnan(inferred_tr5_vec)]
                    avg_trn_scan_row = float(np.mean(valid_tr5)) if len(valid_tr5) > 0 else 0.0
                except Exception:
                    # 终极兜底：返回默认值
                    avg_trn_scan_row = 0.0

            pe_raw_p1 = hist.get("pe_ttm")
            if pe_raw_p1 is None or pd.isna(pe_raw_p1) or str(pe_raw_p1).strip() in ("", "-"):
                pe_raw_p1 = hist.get("pe", 0)
            pe_p1 = _safe_float(pe_raw_p1)
            ind_stats_p1 = industry_pe_stats_scan.get(industry, global_stats_scan)

            p1_strategies_allowed = True
            try:
                p1_smooth, p1_ok, p1_reason, _p1_det_scan = compute_p1_multi_dim_smooth_score(
                    df_target,
                    rt,
                    circ_mv_yi,
                    ind_rank,
                    pe_p1,
                    ind_stats_p1,
                    industry,
                    dynamic_industries_scan,
                    avg_trn_scan_row,
                    float(p1_pass_line_scan),
                )
            except Exception as _ex_p1:
                p1_smooth, p1_ok = 0.0, False
                p1_reason = "平滑得分不达标"
                logging.debug("P1多维分项初筛异常 %s: %s", s_code, _ex_p1)
            if not p1_ok:
                p1_strategies_allowed = False
                _rollback_scan_funnel_for_skipped_stock(res, active_target_pools)
                res["p1_prescreen"]["smooth_blocked"] = int(res["p1_prescreen"].get("smooth_blocked", 0) or 0) + 1
                _gpre = res["p1_prescreen"]["gate_reasons"]
                _gpre["平滑得分不达标"] = int(_gpre.get("平滑得分不达标", 0) or 0) + 1
                logging.debug("P1初筛拦截 %s: %s score=%.2f", s_code, p1_reason, p1_smooth)
            p1_gene = float(p1_smooth)
            item["p1_score"] = float(p1_smooth)

            # 【V26.6 优化】整理代码块：移除重复的嵌套 try，合并为主逻辑 + 统一异常兜底
            empty_board_count = 0
            is_empty_board = False
            try:
                pct_chg_5 = pd.to_numeric(df_tail_5["pct_chg"], errors="coerce").fillna(0)
                high_5 = pd.to_numeric(df_tail_5["high"], errors="coerce").fillna(0)
                low_5 = pd.to_numeric(df_tail_5["low"], errors="coerce").fillna(0)
                empty_board_count = int(((high_5 == low_5) & (pct_chg_5 >= 9.0)).sum())
                is_empty_board = empty_board_count >= 2
            except Exception:
                # 终极兜底：默认无涨停板
                empty_board_count = 0
                is_empty_board = False
            decay_factor = _calc_decay_factor_atr(df_target, rt)
            
            vol_mean_pre = df_tail_60[vol_col].mean()
            vol_std_pre = df_tail_60[vol_col].std()
            curr_vol_pre = _safe_float(rt.get('vol', df_target[vol_col].iloc[-1] if not df_target.empty else 0))
            vol_z = (curr_vol_pre - vol_mean_pre) / vol_std_pre if pd.notna(vol_std_pre) and vol_std_pre > 0 else 0.0
            holding_days = holding_days_map.get(full_code, 0)
            
            trigger_ds, final_reason = would_trigger_danger_sell(
                df_target, rt, size_emoji, holding_days, vol_z, ind_rank
            )
            # 与 danger_sell 展示同源，供 _build_priority_tags 等展示「危-破位」标签；**不**再自动拉黑（纯 UI 预警）
            is_stop_loss = bool(trigger_ds)
            if trigger_ds:
                res['danger_sell'].append({
                    "代码": s_code, "名称": stock_name, "现价": f"{now_price:.2f}",
                    "涨幅": f"{pct:.2f}%", "斩仓指令": final_reason
                })
            
            safety_factor, safety_tags = _calc_safety_factor(df_target, rt, regime, industry, sorted_sectors, limit_times_val, is_empty_board)
            safety_factor = max(0.5, min(1.2, safety_factor * size_safety_mult))
            
            _trf_disp = _safe_float(rt.get("turnover_rate_f", 0.0), 0.0)
            if not np.isfinite(_trf_disp) or _trf_disp < 0:
                _trf_disp = 0.0
            _hk_shares = max(0.0, _safe_float(rt.get("hk_vol", 0.0), 0.0))
            _hk_wan_str = f"{_hk_shares / 10000.0:.0f}"
            if _hk_disp_suffix:
                _hk_wan_str = f"{_hk_wan_str}({_hk_disp_suffix})"
            _vr_show = _safe_float(rt.get("vol_ratio", 1.0), 1.0)
            if not np.isfinite(_vr_show) or _vr_show <= 0:
                _vr_show = 1.0
            cs_ranks = rank_lookup.get(s_code)
            if not isinstance(cs_ranks, dict):
                cs_ranks = neutral_ranks()
            base_info = {
                "代码": s_code, "名称": stock_name, "行业": f"{industry}({ind_rank_str})", 
                "股性": size_label, "综合分": 0.0, "涨幅": f"{pct:.2f}%", 
                "量比": f"{min(_vr_show, 50.0):.1f}({vol_ratio_tag})", "建议仓位": "", "纪律防线": "",  
                "真换手": f"{_trf_disp:.1f}%", "集中度": cyq_str, 
                global_hk_label: _hk_wan_str,
                "现价": f"{now_price:.2f}", "战法": "", "连板高度": f"{limit_times_val:.0f}", 
                "机构预测": f"{int(_safe_float(rt.get('forecast_type', 0), 0))}"
            }

            scan_vwap_fish = False
            if _danger_buy_estimate_vwap_from_rt is not None and _breakout_vwap_eps_danger > 0:
                scan_vwap_fish = _danger_buy_vwap_fish_line_trigger(
                    now_price,
                    pct,
                    rt,
                    curr_min,
                    _breakout_vwap_eps_danger,
                )
            if scan_vwap_fish:
                # 原此处写入黑名单已移除：VWAP 钓鱼线仅依赖下方 danger_buy 表与操盘提示展示
                pass

            is_danger_buy_added = False
            
            # P2–P5 各用独立引擎；P4 尾盘、P5 盘后物理胸甲已拆分
            for pool_key, engine_obj in [('p2', p2_engine), ('p3', p3_engine), ('p4', p4_engine), ('p5', p5_engine)]:
                if not p1_strategies_allowed:
                    continue
                if pool_key in active_target_pools:
                    # rz_net_buy：仅 P4/P5 尾盘与验尸可信；P2/P3 及盘中一律不传融资字段
                    if pool_key == 'p3':
                        rt['rz_net_buy'] = 0.0
                    elif pool_key in ('p4', 'p5'):
                        rt['rz_net_buy'] = rz_net_buy_hist
                    else:
                        rt['rz_net_buy'] = 0.0
                    # 传入当前池子标识，供 P4/P5 共用引擎区分验尸逻辑
                    rt['_pool_key'] = pool_key
                    # 【自适应优化】各池战法 run_all/evaluate 可读此字段（仅三文件内约定，不新增策略文件）
                    rt["_market_contraction_score"] = market_contraction_score
                    rt["_regime_state"] = regime
                    rt["regime"] = regime
                    if pool_key == "p2":
                        rt["_t1_memory"] = (t1_memory.get("p2") if isinstance(t1_memory, dict) else {}) or {}
                    else:
                        rt.pop("_t1_memory", None)
                    try:
                        if hasattr(engine_obj, "run_all"): 
                            hit_res = engine_obj.run_all(df_target, rt) 
                        elif hasattr(engine_obj, "evaluate"): 
                            hit_res = engine_obj.evaluate(df_target, rt)
                        else: 
                            continue
                    except Exception as e:
                        logging.warning("战法单池跳过 %s - %s: %s", s_code, pool_key, e)
                        continue
                    
                    if hit_res is None: hit_res = {}
                        
                    if isinstance(hit_res, dict): orig_hits = hit_res.get("strategies", [])
                    elif isinstance(hit_res, list): orig_hits = hit_res
                    else: orig_hits = []
                        
                    if not isinstance(orig_hits, list): orig_hits = []
                    
                    if orig_hits:
                        if isinstance(hit_res, dict):
                            burst_score = _safe_float(hit_res.get("burst_score", 90.0), default=90.0)
                            surge_bonus = _safe_float(hit_res.get("surge_bonus", 0.0))
                            penalty = _safe_float(hit_res.get("penalty", 0.0))
                        else:
                            burst_score = 90.0
                            surge_bonus = 0.0
                            penalty = 0.0
                    else:
                        burst_score = 0.0
                        surge_bonus = 0.0
                        penalty = 0.0
                    
                    hits = orig_hits.copy()
                    
                    if golden_engine is not None:
                        hk_vol = _safe_float(rt.get('hk_vol', hist.get('hk_vol', 0.0)))
                        net_main_for_gold = _safe_float(rt.get("net_main_amount", hist.get("net_main_amount", 0.0)))
                        cyq = _safe_float(rt.get('cyq_concentration', hist.get('cyq_concentration', 999.0)))
                        
                        gold_hits, gold_burst, gold_bonus = golden_engine.evaluate(df_target, rt, pool_key, ind_rank, hk_vol, net_main_for_gold, cyq)
                        if gold_hits:
                            hits.extend(gold_hits)
                            burst_score = max(burst_score, gold_burst) 
                            surge_bonus += gold_bonus
    
                    # 分发层二次门禁：黄金起爆点（P2 用开盘价涨幅，其余用现价涨幅）
                    strict_gate = strict_golden_burst_ok(df_target, rt, pool_key)
                    gate_ok = strict_gate
                    core_screener_bypass = False
                    # P2：物理胸甲四大主策略命中时放宽黄金门禁（否则 0.5%~1% 高开与低量比策略会被误杀）
                    if pool_key == "p2" and isinstance(hit_res, dict) and hit_res.get("p2_core_screener_pass"):
                        gate_ok = True
                        core_screener_bypass = True
                    # P3：八大策略硬阈值命中时放宽黄金门禁（否则 0~3% 低吸/托底与 P3 门禁冲突）
                    if pool_key == "p3" and isinstance(hit_res, dict) and hit_res.get("p3_core_screener_pass"):
                        gate_ok = True
                        core_screener_bypass = True
                    # P4：尾盘八策略命中时放宽黄金门禁
                    if pool_key == "p4" and isinstance(hit_res, dict) and hit_res.get("p4_core_screener_pass"):
                        gate_ok = True
                        core_screener_bypass = True
                    # P5：盘后策略体系命中时放宽黄金门禁（含资金/趋势/结构/缩量低吸等确认项）
                    if pool_key == "p5" and isinstance(hit_res, dict) and hit_res.get("p5_core_screener_pass"):
                        gate_ok = True
                        core_screener_bypass = True
                    # 【自适应优化】核心主池仍走 strict_gate/胸甲直通；仅当未过严格门禁且收缩度≥0.7 时启用辅助观察门禁（量比/换手约-25%，P4资金OR）
                    relaxed_gate_applied = False
                    if (not gate_ok) and market_contraction_score >= 0.7:
                        if adaptive_relaxed_golden_gate_ok(pool_key, df_target, rt, market_contraction_score):
                            gate_ok = True
                            relaxed_gate_applied = True
                    # 直通车：已豁免 P1 入池形态；若 P3/P4 物理胸甲已命中战法，则放宽黄金门禁（避免 strict_gate 误杀超高乖离龙头）
                    if (
                        hits
                        and (not gate_ok)
                        and item.get("_momentum_fast_lane")
                        and pool_key in ("p3", "p4")
                    ):
                        gate_ok = True
                        core_screener_bypass = True
                    if gate_ok and pool_key in res['funnel']:
                        res['funnel'][pool_key]["pass_golden_gate"] += 1
                    if hits and not gate_ok:
                        if pool_key in res['funnel']:
                            r = _infer_golden_gate_reason(pool_key, df_target, rt)
                            gr = res['funnel'][pool_key]["gate_block_reasons"]
                            gr[r] = int(gr.get(r, 0)) + 1
                        hits = []
                        burst_score = 0.0
                        surge_bonus = 0.0
                        penalty = 0.0
                    
                    if hits and burst_score > 0:
                        if pool_key in res['funnel']:
                            res['funnel'][pool_key]["hit_strategy"] += 1
                        # 最终构建前二次保险：确保低于 P1 流通下限不会进入任何 pool_key 结果列表
                        if circ_mv_yi < _p1_scan_min_circ_mv_yi():
                            continue
                        if pool_key in ("p4", "p5"):
                            uniq_hits = []
                            seen = set()
                            for h in hits:
                                if h not in seen:
                                    uniq_hits.append(h)
                                    seen.add(h)
                            if pool_key == "p5":
                                if any("P5-05C·★趋势确认" in h for h in uniq_hits):
                                    uniq_hits = [h for h in uniq_hits if "P5-05·★绝对趋势雷达" not in h]
                                if any("P5-13C·★均线发散确认" in h for h in uniq_hits):
                                    uniq_hits = [h for h in uniq_hits if "P5-13·★均线粘合发散" not in h]
                                if any("P5-12A·★箱体突破" in h or "P5-12B·★箱体回踩" in h for h in uniq_hits):
                                    uniq_hits = [h for h in uniq_hits if "P5-12·★箱体突破回踩" not in h]
                            # P4/P5：保留优先级最高的少量战法说明；P5 先做语义去重，避免总标签与子标签重复堆叠。
                            prioritized = [h for h in uniq_hits if (
                                "金·" in h or "实锤" in h or "九转序列" in h or "龙虎榜机构" in h
                                or "布林CCI" in h or "融资杠杆主力" in h or "主力真金" in h or "机构实锤" in h
                                or "★" in h
                            )]
                            hits = (prioritized[:3] if prioritized else uniq_hits[:3]) or hits
    
                        pw = pool_weight_map.get(pool_key, 1.0) * size_pool_boost
                        actual_penalty = 0.0 if is_core_sector else (penalty * size_penalty_factor)
                        has_golden = any("金·" in h for h in hits)
                        
                        surge_eff = compress_surge_bonus(surge_bonus)
                        burst_adj = burst_soft_cap(float(burst_score))
                        burst_adj = dampen_burst_by_extremes(
                            burst_adj,
                            p1_gene,
                            float(cs_ranks.get("vol_ratio_r", 0.5)),
                            float(cs_ranks.get("pct_r", 0.5)),
                            float(cs_ranks.get("turnover_f_r", 0.5)),
                            float(cs_ranks.get("main_ratio_r", 0.5)),
                        )
                        personality_multiplier = _evaluate_personality(
                            df_target,
                            rt,
                            pool_key,
                            vr_rank=float(cs_ranks.get("vol_ratio_r", 0.5)),
                        )
                        adjusted_burst = burst_adj * personality_multiplier
                                    
                        if not hits: 
                            continue 
                        
                        if not is_danger_buy_added:
                            fatal_reasons = []
                            if scan_vwap_fish:
                                fatal_reasons.append(
                                    f"📉VWAP钓鱼线(现价低于分时均价>breakout_vwap_eps="
                                    f"{_breakout_vwap_eps_danger:.4f})"
                                )
                            if is_bottom_3: fatal_reasons.append("💀身处领跌板块")
                            if safety_factor <= 0.65: fatal_reasons.append("⚠️高位乖离严重")
                            if is_empty_board and has_golden: fatal_reasons.append("💣中空假龙诱多")
                            if personality_multiplier == 0.5: fatal_reasons.append("🩸暴跌熔断(破ATR极限)")
                            
                            if fatal_reasons:
                                res['danger_buy'].append({
                                    "代码": s_code, "名称": stock_name, "现价": f"{now_price:.2f}",
                                    "涨幅": f"{pct:.2f}%", "致死原因": " | ".join(fatal_reasons)
                                })
                                is_danger_buy_added = True
                        
                        if is_empty_board and has_golden:
                            current_w_gene = w_gene
                            current_w_burst = w_burst
                            adjusted_burst -= 20.0  
                            hits.append("💣[假龙重罚]")
                        elif has_golden:
                            current_w_gene = 0.35
                            current_w_burst = 0.65
                        else:
                            current_w_gene = w_gene
                            current_w_burst = w_burst
                        
                        # 资金共振复合分：P2 略高权重，P3–P5 递减（均低于 P1 排序 18% 量级，避免重复放大）
                        _crs_live = _safe_float(
                            rt.get("capital_resonance_score", y_bar.get("capital_resonance_score", 0.0)),
                            0.0,
                        )
                        if pool_key == "p2":
                            _crs_w = 0.05
                        elif pool_key == "p3":
                            _crs_w = 0.04
                        elif pool_key == "p4":
                            _crs_w = 0.035
                        else:
                            _crs_w = 0.03
                        _crs_add = _crs_live * _crs_w * float(pw)
                        _sector_beta = _safe_float((hit_res or {}).get("detail", {}).get("sector_beta", 1.0), 1.0)
                        if pool_key == "p4":
                            if _sector_beta >= 1.2:
                                sector_mult *= min(1.5, _sector_beta)
                            elif _sector_beta <= 0.9:
                                sector_mult *= max(0.7, _sector_beta)
                        raw_score = (
                            (p1_gene * current_w_gene)
                            + (adjusted_burst * pw * current_w_burst)
                            + surge_eff
                            - actual_penalty
                            + _crs_add
                        )
                        final_score = raw_score * regime_mult * sector_mult * decay_factor * safety_factor
                        # 拥挤度降权：只影响分数与标签，不改变“命中战法即可入池”的基本语义
                        hi_shadow_live = _safe_float(rt.get("high", now_price), now_price)
                        upper_shadow_pct_live = 0.0
                        pre_close_live = _safe_float(rt.get("pre_close", 0.0), 0.0)
                        if pre_close_live > 0:
                            upper_shadow_pct_live = (
                                (max(hi_shadow_live, now_price) - max(now_price, _safe_float(rt.get("open", now_price), now_price)))
                                / pre_close_live
                                * 100.0
                            )
                        crowd_score, crowd_mult, crowd_label = _calc_crowding_penalty(
                            pool_key=pool_key,
                            pct=pct,
                            vol_ratio=_safe_float(rt.get("vol_ratio", 1.0), 1.0),
                            turnover_f=_safe_float(rt.get("turnover_rate_f", 0.0), 0.0),
                            upper_shadow_pct=upper_shadow_pct_live,
                            circ_mv_yi=circ_mv_yi,
                        )
                        p3_hard_filter = False
                        p3_guard_mult = 1.0
                        p3_guard_label = ""
                        p3_guard_tags: list = []
                        if pool_key == "p3":
                            p3_hard_filter, p3_guard_mult, p3_guard_label, p3_guard_tags = _p3_right_side_guard(
                                df_target,
                                rt,
                                hit_res=hit_res,
                                market_contraction_score=market_contraction_score,
                                crowd_score=crowd_score,
                            )
                            if p3_hard_filter:
                                if pool_key in res['funnel']:
                                    res['funnel'][pool_key]["gate_block_reasons"][p3_guard_label or "P3硬过滤"] = int(
                                        res['funnel'][pool_key]["gate_block_reasons"].get(p3_guard_label or "P3硬过滤", 0)
                                    ) + 1
                                hits = []
                                burst_score = 0.0
                                surge_bonus = 0.0
                                penalty = 0.0
                                continue
                        final_score = final_score * crowd_mult * p3_guard_mult
                        
                        # 【自适应优化】综合分参考线 60：缩量放宽门禁且收缩度高 → 再降 5 分（仅影响 signal_log 与提示，不拦截入表）
                        min_pass = float(min_pass_map.get(pool_key, 60.0))
                        if relaxed_gate_applied and (not strict_gate) and (not core_screener_bypass) and market_contraction_score >= 0.7:
                            min_pass = 55.0
                        # 命中战法且已算综合分：一律写入主池；60/85 不作为硬门槛（漏斗「入池」与命中战法对齐）
                        if pool_key in res['funnel']:
                            res['funnel'][pool_key]["pass_score"] += 1
                        b = base_info.copy()
                        if pool_key == "p3":
                            b["主战法"] = "--"
                            b["辅助标签"] = "--"
                            b["观察项"] = "--"
                        b["综合分"] = round(final_score, 2)
                        if final_score < 85.0:
                            b["操盘提示"] = "综合分未达85，建议谨慎操作（仅供参考）"
                        else:
                            b["操盘提示"] = "--"

                        # 三层风控展示：风险标签 + 建议最低买入综合分（来自各池引擎 run_all）
                        if isinstance(hit_res, dict):
                            _rtags = hit_res.get("risk_tags") or []
                            if isinstance(_rtags, list):
                                b["风险标签"] = " ".join(str(t) for t in _rtags) if _rtags else "--"
                            else:
                                b["风险标签"] = str(_rtags) if _rtags else "--"
                            b["建议最低分"] = f"{_safe_float(hit_res.get('suggested_min_entry_score', 0.0), 0.0):.1f}"
                            p4_md = _format_p4_trade_language(
                                hits,
                                stock_memory_score=_safe_float(hit_res.get("detail", {}).get("stock_memory_score", 0.0), 0.0),
                                sector_beta=_safe_float(hit_res.get("detail", {}).get("sector_beta", 1.0), 1.0),
                                close_vwap_dev=hit_res.get("detail", {}).get("close_vwap_dev_pct"),
                            )
                            p3_layers = _format_p3_trade_layers(hits) if pool_key == "p3" else None
                            if pool_key == "p4" and p4_md and p4_md != "--":
                                b["战法"] = p4_md
                            if pool_key == "p3" and isinstance(p3_layers, dict):
                                b["主战法"] = p3_layers.get("主战法", "--")
                                b["辅助标签"] = p3_layers.get("辅助标签", "--")
                                b["观察项"] = p3_layers.get("观察项", "--")
                            if pool_key == "p2":
                                b["主战法"] = " / ".join([h for h in hits if h and "✈️" not in h][:3]) or "--"
                                b["辅助标签"] = "--"
                                b["观察项"] = "--"
                        else:
                            b["风险标签"] = "--"
                            b["建议最低分"] = "--"
                        if pool_key == "p3":
                            extra_tags = []
                            if p3_guard_label:
                                extra_tags.append(f"P3:{p3_guard_label}")
                            if p3_guard_tags:
                                extra_tags.extend([str(x) for x in p3_guard_tags if str(x).strip()])
                            if extra_tags:
                                _risk = str(b.get("风险标签", "") or "").strip()
                                _extra = " ".join(dict.fromkeys(extra_tags))
                                b["风险标签"] = f"{_risk} {_extra}".strip() if _risk and _risk != "--" else _extra
                                if p3_guard_label in ("强确认", "适度加分"):
                                    _pt = str(b.get("操盘提示", "") or "").strip()
                                    _add = "P3右侧确认"
                                    b["操盘提示"] = f"{_pt} | {_add}" if _pt and _pt != "--" else _add
                        # 【V26.7 增强】P3 告警补充四项关键数据：VWAP偏离、均线位置、主力净额、MACD状态
                        if pool_key == "p3":
                            # 1) VWAP 偏离
                            _vw = _safe_float(rt.get("vwap"), 0.0)
                            _now_p = _safe_float(rt.get("price"), 0.0)
                            if _vw > 0 and _now_p > 0:
                                _vw_dev = (_now_p - _vw) / _vw * 100.0
                                b["vwap_dev_pct"] = round(_vw_dev, 2)
                                b["vwap"] = round(_vw, 3)
                            # 2) 均线位置（从 df_target 读取）
                            if df_target is not None and len(df_target) > 0:
                                _curr = df_target.iloc[-1]
                                for _ma in ("ma5", "ma10", "ma20", "ma60"):
                                    if _ma in _curr and not pd.isna(_curr[_ma]):
                                        _px_vs_ma = (_now_p - float(_curr[_ma])) / max(float(_curr[_ma]), 1e-9) * 100.0
                                        b[f"{_ma}_dev_pct"] = round(_px_vs_ma, 2)
                                        b[_ma] = round(float(_curr[_ma]), 3)
                            # 3) 主力净额（昨日结算，非实时）
                            _net_main = _safe_float(rt.get("net_main_amount", hist.get("net_main_amount", 0.0)), 0.0)
                            if _net_main != 0.0:
                                b["net_main_amount"] = round(_net_main / 10000.0, 2)  # 转为万元
                            # 4) MACD 状态
                            if df_target is not None and len(df_target) > 0:
                                _curr = df_target.iloc[-1]
                                for _mkey in ("macd_dif", "macd_dea", "macd_bar"):
                                    if _mkey in _curr and not pd.isna(_curr[_mkey]):
                                        b[_mkey] = round(float(_curr[_mkey]), 4)
                        b["拥挤度"] = f"{crowd_score:.0f}({crowd_label})"
                        if crowd_mult < 1.0:
                            _tag_c = str(b.get("风险标签", "") or "").strip()
                            _crowd_suffix = f"【拥挤降权x{crowd_mult:.2f}】"
                            b["风险标签"] = (
                                f"{_tag_c} {_crowd_suffix}".strip()
                                if _tag_c and _tag_c != "--"
                                else _crowd_suffix
                            )
                        # 第2步：P4/P5 执行分层（A/B/C），把“选得出”升级为“怎么做”
                        exec_tier, exec_hint = _execution_tier_for_pool(
                            pool_key=pool_key,
                            final_score=final_score,
                            crowding_score=crowd_score,
                            relaxed_gate_applied=bool(relaxed_gate_applied and (not strict_gate) and (not core_screener_bypass)),
                        )
                        b["执行层级"] = exec_tier
                        if exec_hint:
                            b["操盘提示"] = exec_hint if b.get("操盘提示", "--") == "--" else f"{b.get('操盘提示')} | {exec_hint}"
                            if pool_key in ("p4", "p5"):
                                if exec_tier == "A":
                                    b["建议仓位"] = "主仓候选: 20%-30%（分批）"
                                elif exec_tier == "B":
                                    b["建议仓位"] = "试错仓位: 8%-15%"
                                elif exec_tier == "C":
                                    b["建议仓位"] = "观察仓位: 0%-8%"
                        # 【自适应优化】缩量放宽门禁标记（仍进主池，仅标签区分）
                        if relaxed_gate_applied and (not strict_gate) and (not core_screener_bypass):
                            _tag0 = str(b.get("风险标签", "") or "").strip()
                            _suffix = "【缩量辅助观察】"
                            b["风险标签"] = f"{_tag0} {_suffix}".strip() if _tag0 and _tag0 != "--" else _suffix

                        is_precise_pullback = "精准回踩" in " ".join(hits)
                        b["建议仓位"] = _calc_position(size_emoji, safety_factor, regime, is_precise_pullback)

                        defense_line = "跌破20日线就先观察，不急着动"
                        b["纪律防线"] = defense_line

                        tags_str = _build_priority_tags(p1_gene, burst_score, surge_bonus, regime_mult, decay_factor, is_stop_loss, actual_penalty, is_core_sector, pool_key, safety_tags, personality_multiplier)

                        golden_hits = [h for h in hits if "金·短共振" in h or "金·月共振" in h]
                        other_hits = [h for h in hits if h not in golden_hits]

                        parts = []
                        if golden_hits:
                            parts.append(" + ".join(golden_hits))
                        if tags_str:
                            parts.append(tags_str)
                        if other_hits:
                            parts.append(" + ".join(other_hits))

                        b["战法"] = " | ".join(parts)
                        if pool_key == "p5" and isinstance(hit_res, dict):
                            p5_action = str(hit_res.get("primary_action", "") or hit_res.get("detail", {}).get("主动作", "") or "").strip()
                            p5_status = str(hit_res.get("market_status", "") or hit_res.get("detail", {}).get("市场状态", "") or "").strip()
                            p5_reason = str(hit_res.get("detail", {}).get("入池理由", "") or hit_res.get("detail", {}).get("买入原因", "") or "").strip()
                            p5_hint = str(hit_res.get("buy_hint", "") or hit_res.get("wechat_hint", "") or "").strip()
                            p5_prefix = " / ".join([x for x in (p5_action, p5_status, p5_reason) if x and x != "--"])
                            if p5_prefix:
                                b["战法"] = f"{p5_prefix} | {b['战法']}" if b.get("战法") else p5_prefix
                            if p5_hint:
                                old_tip = str(b.get("操盘提示", "") or "").strip()
                                b["操盘提示"] = f"{old_tip} | {p5_hint}" if old_tip and old_tip != "--" else p5_hint
                        b["pool_tier"] = "main"
                        b["名称"] = normalize_stock_display_name(b.get("名称", ""))
                        b["股性"] = normalize_stock_display_name(b.get("股性", ""))
                        b["战法"] = normalize_stock_display_name(b.get("战法", ""))
                        res[pool_key].append(_ensure_pool_table_row_contract(b, global_hk_label))
                        if final_score >= min_pass:
                            save_signal_log(b, pool_key, regime, f"S({safety_factor:.2f})*P({actual_penalty:.0f})*M({personality_multiplier:.2f})")

            # 未命中任何战法时仍要进入禁买表（黑名单已在上方写入）
            if scan_vwap_fish and (not is_danger_buy_added) and _danger_buy_estimate_vwap_from_rt is not None:
                _vw_only = float(_danger_buy_estimate_vwap_from_rt(rt, now_price))
                _reason_vwap_only = (
                    f"📉VWAP钓鱼线熔断：涨幅≥{DANGER_BUY_VWAP_MIN_DAY_PCT:.1f}%"
                    f"且现价低于当日VWAP超过 breakout_vwap_eps={_breakout_vwap_eps_danger:.4f}"
                    f"（VWAP≈{_vw_only:.2f}）"
                )
                res["danger_buy"].append({
                    "代码": s_code,
                    "名称": stock_name,
                    "现价": f"{now_price:.2f}",
                    "涨幅": f"{pct:.2f}%",
                    "致死原因": _reason_vwap_only,
                })
    
        except Exception as scan_item_err:
            logging.warning("扫描引擎单票处理跳过 code=%s: %s", item.get('code', '?'), scan_item_err)
            continue
    for k in active_target_pools:
        res[k] = sorted(res[k], key=lambda x: float(x.get("综合分", 0) or 0), reverse=True)
    for k in active_funnel_keys:
        res["funnel"][k]["total_candidates"] = int(funnel_total_candidates.get(k, 0) or 0)
        res["funnel"][k]["enter_strategy_check"] = int(funnel_enter_strategy_check.get(k, 0) or 0)
        if res["funnel"][k]["total_candidates"] == 0 and len(base_items_for_scan) > 0:
            res["funnel"][k]["total_candidates"] = len(base_items_for_scan)
    # 【多层级池子】说明：若本趟主池合计已有票、且非缩量高压，则丢弃观察池候选，避免「有主池仍显示备选」的心理干扰
    total_main = sum(len(res.get(k) or []) for k in active_target_pools)
    real_fill_obs = (market_contraction_score >= 0.7) or (prev_scan_streak >= 1 and total_main == 0)
    if not real_fill_obs:
        res["observation"] = {k: [] for k in active_target_pools}
    else:
        for k in active_target_pools:
            if k in res.get("observation", {}):
                res["observation"][k] = sorted(res["observation"][k], key=lambda x: x.get("综合分", 0), reverse=True)
    _merge_tier_meta_scan(total_main == 0)
    _persist_scan_attribution_snapshot(res, active_target_pools, regime)

    return res