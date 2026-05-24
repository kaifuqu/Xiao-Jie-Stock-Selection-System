# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 动态打分与策略基类（CSM 双核 + 黄金门禁路由）
【V26.6 新增资金记忆体系】fund_memory_score 在 P1 路径由 score_calibration 按权重可选融合；本模块战法链仍不直接读该列。
【V26.6 第一阶段】资金共振 composite：P3–P5 路径下 crs_ui 以 constants.CAPITAL_RESONANCE_WEIGHT_P345（默认 8%）线性注入动态分（P1 初筛不使用该列）。
【要点】
1. strict_golden_burst_ok 按 pool_key 固定分支，避免仅靠系统时钟误伤 P2–P5。
2. P4 使用 tail_vol_ratio 等尾盘特征；P5 使用全日定档条件，与 tail_vol_ratio 脱钩。
3. get_project_root / P1 基因缓存 / config 战法分 与 UI 共存。
4. calculate_dynamic_score 与 scan_engine 对齐：burst_soft_cap + compress_surge_bonus，避免孤立路径上的极值过拟合。
5. 仅使用 rt_dict 中的日线及合法衍生字段；不依赖任何已废弃的分时特征列。
"""

# Standard library
import glob
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta

# Third-party
import pandas as pd
import streamlit as st

# Local utilities


def _safe_session_state_get(key, default=None):
    try:
        return st.session_state.get(key, default)
    except Exception:
        return default


def _safe_session_state_contains(key) -> bool:
    try:
        return key in st.session_state
    except Exception:
        return False


def _safe_session_state_set(key, value) -> None:
    try:
        st.session_state[key] = value
    except Exception:
        pass


def _p1_min_circ_mv_yi_strat() -> float:
    """与 pool_manager / scan_engine / get_p1_candidate_codes 的流通下限一致（亿元）。"""
    try:
        from core.config_manager import get_p1_select_min_circ_mv_wan

        return float(get_p1_select_min_circ_mv_wan()) / 10000.0
    except Exception:
        try:
            import constants as c

            return float(getattr(c, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000)) / 10000.0
        except Exception:
            return 100.0


try:
    from core.strategies.score_calibration import burst_soft_cap, compress_surge_bonus
except ImportError:

    def burst_soft_cap(burst, soft=94.0, hard=102.0):
        try:
            return float(burst)
        except (TypeError, ValueError):
            return 0.0

    def compress_surge_bonus(surge, linear_cap=10.0, asymptote=16.0):
        try:
            return float(surge)
        except (TypeError, ValueError):
            return 0.0


def __getattr__(name: str):
    """兼容旧代码对模块级 GOLDEN_* / P5_GOLDEN_* 的引用；实际值来自 config.yaml strategies.golden_burst。"""
    _aliases = {
        "GOLDEN_BURST_PCT_LOW": "golden_burst_pct_low",
        "GOLDEN_BURST_PCT_HIGH": "golden_burst_pct_high",
        "GOLDEN_BURST_VR_LOW": "golden_burst_vr_low",
        "GOLDEN_BURST_VR_HIGH": "golden_burst_vr_high",
        "P5_GOLDEN_VR_MIN": "p5_golden_vr_min",
        "P5_GOLDEN_PCT_LOW": "p5_golden_pct_low",
        "P5_GOLDEN_PCT_HIGH": "p5_golden_pct_high",
    }
    if name in _aliases:
        from core.config_manager import get_golden_config

        return float(get_golden_config()[_aliases[name]])
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# ================= 0.5 回踩统一约束（全战法公共函数） =================
def strict_pullback_shrink_ok(vr, threshold=0.8):
    """
    统一回踩约束：所有“回踩类”战法必须叠加缩量条件。
    默认规则：vr < 0.8。
    """
    try:
        return float(vr) < float(threshold)
    except (TypeError, ValueError):
        return False


def _resolve_pre_close_rt_y(rt: dict, y) -> float:
    """
    与 scan_engine 主循环口径一致：rt['pre_close'] → 日线 y['pre_close'] → y['close']。
    禁止用 1.0 冒充昨收；无效时返回 0，由调用方判断。
    """
    if not isinstance(rt, dict):
        rt = {}
    try:
        pc = float(rt.get("pre_close", 0.0) or 0.0)
    except (TypeError, ValueError):
        pc = 0.0
    if pc > 0:
        return pc
    if y is not None and hasattr(y, "get"):
        try:
            pc = float(y.get("pre_close", 0.0) or 0.0)
        except (TypeError, ValueError):
            pc = 0.0
        if pc > 0:
            return pc
        try:
            pc = float(y.get("close", 0.0) or 0.0)
        except (TypeError, ValueError):
            pc = 0.0
    return float(pc or 0.0)


def strict_golden_burst_ok(df, rt, pool_key=None):
    """
    四阶段动态起爆门槛（统一底层门禁）：
    Stage1 早盘核爆期(09:25-09:45)：pct>2.0, vr>=3.0, winner_rate>85, 现价>cost_50th
    Stage2 盘中趋势期(09:45-11:20)：pct>3.0, vr>=1.8, 现价>分时VWAP, macd_bar>0
    Stage3 午后沉淀期(13:00-14:00)：pct>4.0, vr>=1.5, ma20_slope_5>1.0, 近5日阳量>阴量
    Stage4 尾盘确权期(14:00后/盘后复盘)：3.0<pct<7.0, vr>=1.2, tail_vol_ratio>1.5,
                                      上影占比<2.5%, net_main_amount>0
    按 pool_key 固定口径：P4 使用尾盘 tail_vol_ratio/上影；P5 使用全日 vr/涨幅甜点/主力为正/站稳均线（见 P5_GOLDEN_*）。
    """
    def _safe_float(val, default=0.0):
        try:
            if val is None:
                return default
            if pd.isna(val):
                return default
            s = str(val).strip()
            if s in ("", "-"):
                return default
            return float(val)
        except Exception:
            return default

    try:
        if df is None or rt is None:
            return False

        if isinstance(df, pd.DataFrame):
            if df.empty:
                return False
            curr = df.iloc[-1]
            recent5 = df.tail(5).copy()
        else:
            return False

        bj_tz = timezone(timedelta(hours=8))
        now_dt = datetime.now(bj_tz)
        curr_min = now_dt.hour * 60 + now_dt.minute
        is_after_hours = (curr_min < 565) or (curr_min > 900)

        price = _safe_float(rt.get("price", curr.get("close", 0.0)))
        pre_close = float(_resolve_pre_close_rt_y(rt, curr))
        open_price = _safe_float(rt.get("open", curr.get("open", 0.0)))
        high_price = _safe_float(rt.get("high", curr.get("high", 0.0)))
        vr = _safe_float(rt.get("vol_ratio", 0.0))
        if vr <= 0:
            # 行情未推量比时用昨收 vol_ratio 兜底，禁止「vr==0」误杀全池
            vr = max(_safe_float(curr.get("vol_ratio", 0.0)), 0.05)
        if vr <= 0:
            vr = 1.0

        if price <= 0 or pre_close <= 0:
            return False
        pct_chg = (price - pre_close) / pre_close * 100.0

        macd_bar = _safe_float(curr.get("macd_bar", curr.get("macd_hist", 0.0)))
        ma20_slope_5 = _safe_float(curr.get("ma20_slope_5", 0.0))

        # ---------------------------------------------------------
        # 先按 pool_key 固定门禁口径（避免“当前时钟”误伤）
        # P2: 竞价门禁；P3: 盘中门禁；P4: 尾盘快照门禁；P5: 盘后全日定档门禁（已拆分）
        # 仅当未提供 pool_key 时，才回退到旧的按时间自动分阶段。
        # ---------------------------------------------------------
        pk = str(pool_key or "").strip().lower()

        if pk == "p2":
            winner_rate = _safe_float(rt.get("winner_rate", curr.get("winner_rate", 0.0)))
            # 缺省必须用 0：旧版 999999 会让「price > cost_50th」永假，竞价池全灭
            cost_50th = _safe_float(rt.get("cost_50th", curr.get("cost_50th", 0.0)))
            open_pct = (open_price - pre_close) / pre_close * 100.0 if pre_close > 0 else 0.0
            if not (open_pct > 1.0 and vr >= 1.2):
                return False
            # 仅当筹码字段落在合理区间才做强约束；缺失或脏值不拦截（由物理胸甲策略兜底）
            chip_ok = (
                winner_rate > 0
                and cost_50th > 0
                and cost_50th < price * 20.0
                and cost_50th > price * 0.05
            )
            if chip_ok and not (winner_rate > 85.0 and open_price > cost_50th):
                return False
            return True

        if pk == "p3":
            amount = _safe_float(rt.get("amount", 0.0))
            volume = _safe_float(rt.get("volume", 0.0))
            # 略放宽：与「防守反击」区间一致，避免轻微上攻即被门禁挡在金叉战法外
            if not (pct_chg > 1.2 and vr >= 1.2):
                return False
            if amount <= 0 or volume <= 0:
                # 无量额时退化为动能门禁（仅要求 MACD 为正）
                return macd_bar > 0
            else:
                vol_safe = max(volume, 1e-9)
                tentative = amount / vol_safe
                vwap = amount / max(volume * 100.0, 1e-9) if tentative > price * 20 else tentative
                if vwap <= 0:
                    return macd_bar > 0
                if not (price > vwap and macd_bar > 0):
                    return False
            return True

        # P4：尾盘抢筹快照门禁（保留 tail_vol_ratio、上影线、特大单等「临近收盘」特征）
        if pk == "p4":
            tail_vol_ratio = _safe_float(rt.get("tail_vol_ratio", 0.0))
            net_main_amt = _safe_float(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)))
            if not (2.0 < pct_chg < 8.0 and vr >= 1.1):
                return False
            if pre_close <= 0:
                return False
            # 上影：优先用「合并后日线最后一根」的 high，与 precompute/macd 同源；rt['high'] 往往是全日最高，和盘中合成 K 线错位会误伤门禁。
            hi_shadow = _safe_float(curr.get("high", 0.0))
            if hi_shadow <= 0:
                hi_shadow = _safe_float(rt.get("high", 0.0))
            if hi_shadow <= 0:
                hi_shadow = max(price, open_price)
            upper_shadow_pct = (hi_shadow - max(price, open_price)) / pre_close * 100.0
            # 未写入 tail_vol_ratio（未跑 14:35 快照）时不卡尾盘占比，避免 P4 恒为 0
            tail_ok = (tail_vol_ratio > 1.5) if tail_vol_ratio > 0 else True
            if not (tail_ok and upper_shadow_pct < 2.8 and net_main_amt > 0):
                return False
            return True

        # P5：盘后全日定档门禁（不使用 tail_vol_ratio；阈值来自 config.yaml strategies.golden_burst）
        if pk == "p5":
            from core.config_manager import get_golden_config

            gb = get_golden_config()
            p5_vr_min = float(gb.get("p5_golden_vr_min", 1.2))
            p5_lo = float(gb.get("p5_golden_pct_low", 2.0))
            p5_hi = float(gb.get("p5_golden_pct_high", 7.0))
            if vr < p5_vr_min:
                return False
            if not (p5_lo < pct_chg < p5_hi):
                return False
            if pre_close <= 0:
                return False
            net_main_amt = _safe_float(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)))
            if net_main_amt <= 0:
                return False
            ma5_line = _safe_float(curr.get("ma5", 0.0))
            ma20_line = _safe_float(curr.get("ma20", 0.0))
            if ma20_line <= 0:
                return False
            # 收盘价站稳短期均线：同时站上 ma5 与 ma20（price 为 rt 现价/收盘语义）
            if not (price > ma20_line and price > ma5_line):
                return False
            return True

        # ---------------------------------------------------------
        # 兼容旧路径：未提供 pool_key 时，仍按当前时钟自动分阶段
        # 【V26.6 A股时段精细化】
        #   Stage1 09:30-09:45 早盘确认期（排除竞价虚假繁荣）
        #   Stage2 09:45-10:30 强势股趋势确立
        #   Stage3 10:30-14:00 主力控盘/洗盘期（含10:30第二波识别）
        #   Stage4 14:00后/盘后 尾盘确权/异动期
        # ---------------------------------------------------------
        stage = 4 if is_after_hours else None
        if stage is None:
            if curr_min < 570:
                stage = 0  # 集合竞价期，不评分
            elif 570 <= curr_min < 585:
                stage = 1
            elif 585 <= curr_min < 630:
                stage = 2
            elif 630 <= curr_min < 840:
                stage = 3
            else:
                stage = 4

        if stage == 1:
            winner_rate = _safe_float(rt.get("winner_rate", curr.get("winner_rate", 0.0)))
            cost_50th = _safe_float(rt.get("cost_50th", curr.get("cost_50th", 0.0)))
            if not (pct_chg > 2.0 and vr >= 3.0):
                return False
            if cost_50th > 0 and cost_50th < price * 20.0:
                if not (winner_rate > 85.0 and price > cost_50th):
                    return False
            return True

        if stage == 2:
            amount = _safe_float(rt.get("amount", 0.0))
            volume = _safe_float(rt.get("volume", 0.0))
            if not (pct_chg > 2.5 and vr >= 1.8):
                return False
            if amount <= 0 or volume <= 0:
                return False
            tentative = amount / volume
            vwap = amount / (volume * 100.0) if tentative > price * 20 else tentative
            if vwap <= 0:
                return False
            if not (price > vwap and macd_bar > 0):
                return False
            return True

        if stage == 3:
            if not (pct_chg > 3.0 and vr >= 1.5):
                return False
            if not (ma20_slope_5 > 1.0):
                return False

            vol_col = "vol" if "vol" in recent5.columns else ("volume" if "volume" in recent5.columns else None)
            if vol_col is None:
                return False

            up_mask = recent5["close"] > recent5["open"] if "open" in recent5.columns else (recent5["pct_chg"] > 0 if "pct_chg" in recent5.columns else None)
            if up_mask is None:
                return False
            down_mask = ~up_mask
            up_vol = recent5.loc[up_mask, vol_col].astype(float).sum()
            down_vol = recent5.loc[down_mask, vol_col].astype(float).sum()
            if not (up_vol > down_vol):
                return False
            return True

        if stage == 4:
            tail_vol_ratio = _safe_float(rt.get("tail_vol_ratio", 0.0))
            net_main_amt = _safe_float(rt.get("net_main_amount", curr.get("net_main_amount", 0.0)))
            if not (2.5 < pct_chg < 7.5 and vr >= 1.2):
                return False
            if pre_close <= 0:
                return False
            hi_s4 = _safe_float(curr.get("high", 0.0))
            if hi_s4 <= 0:
                hi_s4 = _safe_float(rt.get("high", 0.0))
            if hi_s4 <= 0:
                hi_s4 = max(price, open_price)
            upper_shadow_pct = (hi_s4 - max(price, open_price)) / pre_close * 100.0
            if not (tail_vol_ratio > 1.5 and upper_shadow_pct < 2.5 and net_main_amt > 0):
                return False
            return True

        return False
    except Exception as e:
        logging.debug(f"strict_golden_burst_ok 动态门槛异常: {e}")
        return False

# ================= 0. 全局物理底座寻址雷达 =================
def get_project_root():
    """动态向上探测根目录，确保跨设备、跨层级部署时路径绝对安全"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for _ in range(4):  
        if os.path.exists(os.path.join(current_dir, "config.yaml")):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    return os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = get_project_root()

try:
    from core.runtime_data_paths import glob_p1_gene_json_paths, path_p1_gene_json
except ImportError:
    def path_p1_gene_json(yyyymmdd):
        return os.path.join(PROJECT_ROOT, "data", f"p1_gene_{yyyymmdd}.json")

    def glob_p1_gene_json_paths():
        return sorted(glob.glob(os.path.join(PROJECT_ROOT, "data", "p1_gene_*.json")))

# ================= 1. 数据对齐与预处理基建 (全量恢复) =================

def align_market_data(df, rt):
    if df is None or df.empty or not rt:
        return df
        
    try:
        latest_trade_date = str(rt.get('trade_date', datetime.now().strftime("%Y%m%d"))).replace('-', '')
        df_last_date = str(df.iloc[-1].get('trade_date', '')).replace('-', '')
        
        if latest_trade_date != df_last_date:
            y_row = df.iloc[-1]
            new_row = {
                'trade_date': latest_trade_date,
                'open': float(rt.get('open', 0)),
                'high': float(rt.get('high', 0)),
                'low': float(rt.get('low', 0)),
                'close': float(rt.get('price', 0)),
                'vol': float(rt.get('volume', 0)) / 100.0,
                'amount': float(rt.get('amount', 0)) / 10000.0,
                'pre_close': float(_resolve_pre_close_rt_y(rt, y_row)),
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
    except Exception as e:
        logging.debug(f"市场数据对齐异常: {e}")
        
    return df

# ================= 2. 原版 Config 加载器 =================

def _load_base_scores_from_config():
    default_scores = {
        "01_一进二": 95.0, "59_强势接力": 95.0, "93_234板接力": 85.0,
        "95_北向隔夜抢跑": 110.0, "96_筹码单峰突破": 110.0,
        "91_强势阴线回踩": 100.0, "60_旱地拔葱": 95.0, "30_朱雀突破": 95.0,
        "58_龙头首板": 95.0, "02_黄金缺口": 95.0, "94_两融错杀反核": 95.0,
        "L1_极端错杀": 95.0, "L2_VWAP极度乖离": 95.0, "L3_均线深海探针": 95.0,
        "L5_极度背离": 105.0, "L8_布林下轨防守": 105.0, "L6_三连阴缩量冰点": 95.0,
        "82_尾盘强势加速": 95.0, "99_筹码护盘收官": 105.0, "100_北向尾盘抢筹": 105.0,
        "81_尾盘温和抢筹": 95.0, "83_尾盘均线突破": 95.0, "86_特大单护盘": 95.0, "L4_底背离雏形": 95.0
    }
    
    try:
        config_path = os.path.join(PROJECT_ROOT, 'config.yaml')
        
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
                if config_data and 'strategy_scores' in config_data:
                    default_scores.update(config_data['strategy_scores'])
    except Exception as e:
        logging.debug(f"读取 config.yaml 战法分数失败，已切回内置安全底座: {e}")
        
    return default_scores

BASE_SCORES_MAP = _load_base_scores_from_config()

# ================= 3. P1 基因缓存提取器 =================

def get_cached_p1_gene(ts_code):
    today_str = datetime.now().strftime("%Y%m%d")
    clean_code = str(ts_code).split('.')[0] if ts_code else ""
    
    if _safe_session_state_contains('p1_gene_cache') and _safe_session_state_get('p1_cache_date') == today_str:
        cache_dict = _safe_session_state_get('p1_gene_cache') or {}
        for k, v in cache_dict.items():
            if str(k).startswith(clean_code):
                return float(v)

    cache_file = path_p1_gene_json(today_str)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_dict = json.load(f)
            _safe_session_state_set('p1_gene_cache', cache_dict)
            _safe_session_state_set('p1_cache_date', today_str)
            for k, v in cache_dict.items():
                if str(k).startswith(clean_code):
                    return float(v)
            return 80.0
        except Exception as e:
            logging.warning(f"读取当日 P1 JSON 缓存遭遇脏数据: {e}")
            
    cache_files = glob_p1_gene_json_paths()
    if cache_files:
        latest_file = max(cache_files)
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                cache_dict = json.load(f)
            _safe_session_state_set('p1_gene_cache', cache_dict)
            m = re.search(r"p1_gene_(\d{8})\.json$", latest_file.replace("\\", "/"))
            _safe_session_state_set('p1_cache_date', m.group(1) if m else today_str)
            for k, v in cache_dict.items():
                if str(k).startswith(clean_code):
                    return float(v)
        except Exception as e:
            logging.warning(f"读取历史 P1 JSON 缓存失败: {e}")

    return 80.0

# ================= 4. 双核打分大脑 =================

def calculate_dynamic_score(strategy_string=None, rt_dict=None, is_market_bad=False, is_market_strong=False, 
                            ts_code=None, real_time_burst_score=0.0, surge_bonus=0.0, decay_factor=1.0, pool_weight=1.0):
    try:
        if ts_code is not None:
            p1_gene_score = get_cached_p1_gene(ts_code)
            adjusted_burst = real_time_burst_score * pool_weight
            # 与 scan_engine 一致：战法爆发软封顶，减轻 config 里高分战法名的线性过冲
            adjusted_burst = burst_soft_cap(adjusted_burst)

            if p1_gene_score < 85.0:
                adjusted_burst = adjusted_burst * 0.8

            # 🚀 核心修复：市场情绪动态权重漂移
            regime = _safe_session_state_get('market_regime', '震荡市')
            if regime in ["情绪退潮市", "主跌浪"]:
                w_gene, w_burst, regime_multiplier = 0.60, 0.40, 0.85
            elif regime in ["趋势市", "主升浪"]:
                w_gene, w_burst, regime_multiplier = 0.30, 0.70, 1.15
            else:
                w_gene, w_burst, regime_multiplier = 0.40, 0.60, 1.0

            # 副线情绪（regime_analyzer.sentiment_key）：冰点略收紧，高潮略防追；默认 1.0
            sentiment = _safe_session_state_get("market_sentiment", "平稳")
            sentiment_mult = 1.0
            if sentiment == "冰点":
                sentiment_mult = 0.94
            elif sentiment == "高潮":
                sentiment_mult = 0.98

            surge_c = compress_surge_bonus(surge_bonus)
            # 资金共振分：经 rt_dict 注入日线 capital_resonance_score（0~100）；P3–P5 使用 CAPITAL_RESONANCE_WEIGHT_P345（默认 8%），与 P1 排序 18% 分离
            crs_ui = 0.0
            if isinstance(rt_dict, dict):
                try:
                    crs_ui = float(rt_dict.get("capital_resonance_score") or 0.0)
                except (TypeError, ValueError):
                    crs_ui = 0.0
            try:
                import constants as _crs_w

                _wc345 = float(getattr(_crs_w, "CAPITAL_RESONANCE_WEIGHT_P345", 0.08))
            except Exception:
                _wc345 = 0.08
            crs_bonus = crs_ui * _wc345 * float(pool_weight)
            base_total = (p1_gene_score * w_gene) + (adjusted_burst * w_burst) + surge_c + crs_bonus
            final_score = base_total * regime_multiplier * decay_factor * sentiment_mult
            # 强过滤：默认总分<55直接降级；震荡/退潮环境抬升到58
            score_floor = 58.0 if regime in ["震荡市", "情绪退潮市", "主跌浪", "退潮防守"] else 55.0
            if final_score < score_floor:
                return 30.0
            return round(min(max(final_score, 0.0), 100.0), 2)

        elif strategy_string is not None:
            score = 80.0 
            strat_list = [s.strip() for s in strategy_string.split('+')]
            base_scores = []
            
            for strat in strat_list:
                matched_score = 80.0 
                for key, val in BASE_SCORES_MAP.items():
                    if key in strat:
                        matched_score = float(val)
                        break
                base_scores.append(matched_score)
                
            score = max(base_scores) if base_scores else 80.0
            
            if "👑" in strategy_string: 
                score = 150.0
            if "北向" in strategy_string or "外资" in strategy_string or "hk_vol" in strategy_string: 
                score += 10.0
            if "筹码" in strategy_string or "单峰" in strategy_string or "底牌" in strategy_string: 
                score += 8.0
            if "大单" in strategy_string or "机构" in strategy_string: 
                score += 8.0
            
            if "+" in strategy_string and ("👑" in strategy_string or "北向" in strategy_string or "筹码" in strategy_string):
                score += 5.0

            regime = _safe_session_state_get('market_regime', '震荡市')
            if regime in ["情绪退潮市", "主跌浪"] or is_market_bad:
                if "👑" not in strategy_string and "L" not in strategy_string and "回踩" not in strategy_string:
                    score -= 10.0
                if "93_234板接力" in strategy_string:
                    score *= 0.7
            elif regime in ["趋势市", "主升浪"] or is_market_strong:
                if "95_" in strategy_string or "96_" in strategy_string or "99_" in strategy_string or "100_" in strategy_string or "01_" in strategy_string or "60_" in strategy_string or "82_" in strategy_string:
                    score += 10.0

            if "⭐自选" in strategy_string: 
                score += 5.0
            regime_for_floor = _safe_session_state_get('market_regime', '震荡市')
            score_floor = 58.0 if regime_for_floor in ["震荡市", "情绪退潮市", "主跌浪", "退潮防守"] else 55.0
            if score < score_floor:
                return 30.0
            return round(score, 2)

    except Exception as e:
        logging.error(f"大一统打分大脑异常: {e}")
        return 80.0
        
    return 80.0

# ================= 5. 战法基类与分析兵器库 (全量恢复) =================

class StrategyBase:
    def __init__(self):
        self.name = "BaseStrategy"

    def run_all(self, df, rt):
        return {"burst_score": 0.0, "detail": {}}

    def get_basic_info(self, rt, y=None):
        """
        y: 可选昨日日线 pd.Series；若传入则用 effective_turnover_rate_f 统一真实换手，禁止误读 total turnover_rate。
        """
        if not isinstance(rt, dict):
            rt = {}
        raw_cm = rt.get("circ_mv")
        if raw_cm is None or (isinstance(raw_cm, float) and pd.isna(raw_cm)):
            tm = rt.get("total_mv", 0.0)
            try:
                tmf = float(tm) if tm is not None and str(tm).strip() not in ("", "-") else 0.0
            except (TypeError, ValueError):
                tmf = 0.0
            raw_cm = tmf * 0.6 if tmf > 0 else 0.0
        try:
            if raw_cm is None or (isinstance(raw_cm, float) and pd.isna(raw_cm)):
                circ_mv_yi = 0.0
            else:
                circ_mv_yi = float(raw_cm) / 10000.0
        except (TypeError, ValueError):
            circ_mv_yi = 0.0
        # 流通低于 P1 下限（与 pool_manager/scan_engine 同源，默认 60 亿）则 skip_scan。
        if circ_mv_yi < _p1_min_circ_mv_yi_strat():
            return {
                'price': 0.0,
                'open': 0.0,
                # 跳过扫描：不设假昨收 1.0，避免下游误算涨跌幅
                'pre_close': 0.0,
                'high': 0.0,
                'low': 0.0,
                'vr': 0.0,
                'turnover_f': 0.0,
                'circ_mv_yi': circ_mv_yi,
                'skip_scan': True
            }
        px = float(rt.get('price', 0.0))
        if y is not None:
            try:
                from core.strategies.fund_mv_utils import effective_turnover_rate_f
                cl = px if px > 0 else float(y.get('close', 0.0))
                tf = float(effective_turnover_rate_f(rt, y, cl))
            except Exception:
                tf = float(rt.get('turnover_rate_f', 0.0))
        else:
            tf = float(rt.get('turnover_rate_f', 0.0))
        return {
            'price': px,
            'open': float(rt.get('open', 0.0)),
            'pre_close': float(_resolve_pre_close_rt_y(rt, y)),
            'high': float(rt.get('high', 0.0)),
            'low': float(rt.get('low', 0.0)),
            'vr': float(rt.get('vol_ratio', 0.0)),
            'turnover_f': tf,
            'circ_mv_yi': circ_mv_yi,
            'skip_scan': False
        }

    def is_strong_trend(self, df):
        if df is None or len(df) < 60: 
            return False
        curr = df.iloc[-1]
        m5, m20, m60 = curr.get('ma5', 0), curr.get('ma20', 0), curr.get('ma60', 0)
        return m5 > m20 > m60

    def is_weak_trend(self, df):
        if df is None or len(df) < 60:
            return True
        curr = df.iloc[-1]
        return curr.get('ma20', 0) < curr.get('ma60', 0)

    def check_limit_up(self, pct_chg, limit_times):
        return pct_chg > 9.5 and limit_times > 0

    def is_vol_breakout(self, vr, threshold=2.0):
        return vr >= threshold

    def is_strict_pullback(self, vr, threshold=0.8):
        return strict_pullback_shrink_ok(vr, threshold)
        
    def calculate_price_distance(self, current_price, target_price):
        if target_price <= 0: return 999.0
        return (current_price - target_price) / target_price * 100.0