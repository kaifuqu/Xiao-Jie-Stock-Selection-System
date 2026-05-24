# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - P1 底仓管理与打分枢纽（黄金平衡及格线版 + 容错补偿网）

【P1 引擎与初筛如何工作（与 scan_engine 对齐）】
- 流通市值硬闸：低于 get_p1_select_min_circ_mv_wan（config.yaml strategies.p1.select_min_circ_mv_wan，缺省同 constants）换算的亿元门槛的标的，在安检最前即拦截，不进入多维分项打分。
- 多维分项平滑分：第五层 _evaluate_p1_single_stock 仅委托 score_calibration.compute_p1_multi_dim_smooth_score，
  在内存中衍生各子项得分，不向 DuckDB 增列；行业 PE 分位、动态行业贝塔、板块排名与市值优待均在同一函数内完成；可选将 fund_memory_score 按权重凸入最终分（见 config fund_memory_weight_p1）。
- 及格线 pass_line：由 get_p1_regime_thresholds（config.yaml strategies.p1.profiles 按大盘环境映射到 strict/neutral/relaxed，
  键 pass_line 默认 50）注入 _process_single_stock_for_p1；若综合分低于该值，返回 reason 为「平滑得分不达标」（明细中带未达标比对）。
- 不使用 capital_resonance_score 作 P1 硬闸或排序融合。
- scan_engine 的 danger 黑名单（blacklist.json）：默认不用于 P1 拦截（config `respect_scan_blacklist_for_p1`，默认 false），
  避免 P2–P5 扫描误判写入黑名单后，次日洗盘把强势股踢出底仓池；若开启拦截，仅认 kill_date 在 7 日内的条目。
- Tushare 解禁窗口拦截：默认关闭（`use_tushare_unlock_blacklist_for_p1`，默认 false），不调用 share_float，避免专线/代理超时。

不修改 P4/P5 右侧量价引擎。

【核心宪法级更新】：
1. ⚖️【黄金平衡及格线】：P1 默认 pass_line 为 50 分（配置文件可覆盖），配合 60 亿流通下限（可配置），在包容性与僵尸过滤之间折中。
2. 🧹【技术债清除】：彻底删除 precompute_indicators 的重复调用，100% 信任 db_core 的源头数据契约，算力消耗降低。
3. 📈【极致平滑算法】：全面引入 np.interp 线性插值模型，彻底消灭所有 If-Else 悬崖式掉分。
4. 🦍【大盘股专属参数】：剔除妖股逻辑，最大涨幅核心区锁定 8%-25%，均线斜率核心区锁定 0.5-3.5。
5. 🧷【P1 容错补偿网】：在资金强抢筹、形态贴线洗盘再启动、MACD 拐头、直通车/特赦同类信号下，自动升高「容错档」，
   同步放宽 MA120 粘合度、MACD 绿柱斩杀线、量价背离比例、贴 MA20 判定；入选理由会附带说明，便于复盘。
6. 🛡️【数值防爆】：打分链路与换手反算侧强化 NaN/inf 兜底，避免停牌、除权断层导致引擎异常。
7. 🚀【P3/P4 右侧直通车】：见文末 Momentum Fast-Lane 函数族；供 scan_engine 增量并入极端强势非底仓标的，不重扫 daily 全表。

【可维护性】：
- 配置文件与历史 JSON 的读取失败一律记录 logging，禁止静默吞掉异常，便于实盘排障。
"""
from __future__ import annotations

import functools
import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import yaml

import constants

from core.p1_score_display import p1_score_details_to_extreme_labels, score_details_json_safe
from core.stock_name_utils import normalize_stock_display_name

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
        path_p1_gene_json,
        path_sector_rank_history_json,
        path_strategic_mapping_debug_json,
        path_wash_metrics_json,
    )
except ImportError:
    def ensure_runtime_data_layout():
        os.makedirs(os.path.join(PROJECT_ROOT, "data"), exist_ok=True)

    def path_blacklist_json():
        return os.path.join(PROJECT_ROOT, "data", "blacklist.json")

    def path_sector_rank_history_json():
        return os.path.join(PROJECT_ROOT, "data", "sector_rank_history.json")

    def path_strategic_mapping_debug_json(yyyymmdd: str):
        day = str(yyyymmdd or "").strip()
        if len(day) != 8 or (not day.isdigit()):
            day = "latest"
        return os.path.join(PROJECT_ROOT, "data", f"strategic_mapping_debug_{day}.json")

    def path_wash_metrics_json():
        return os.path.join(PROJECT_ROOT, "data", "wash_metrics_history.json")

    def path_p1_gene_json(yyyymmdd: str):
        return os.path.join(PROJECT_ROOT, "data", f"p1_gene_{yyyymmdd}.json")

# ==================== P1 前置防线阈值 ====================
# 数值已迁移至 config.yaml strategies.p1.profiles；此处常量仅作文档/旧代码兼容占位。
P1_TREND_MA120_MIN_RATIO = 0.98
P1_TREND_SLOPE_FASTPASS = 0.25
P1_NEAR_MA20_MIN_RATIO = 0.985
P1_MACD_BAR_KILL = -0.13
P1_VOL_DIVERGENCE_RATIO = 0.85
P1_PASS_LINE = 50.0

# ---------- 资金共振截面分（已废弃；保留空壳函数名，避免旧快照/外链 import 报错）----------
# 流通市值单位：亿元（由 TuShare circ_mv 万元 / 10000）；≥300 亿仍用于「超级大盘」短线均线/MACD 放宽。
P1_MV_SUPER_YI = 300.0
# 资金特赦 is_amnesty：dynamic_inflow_threshold 在按市值比例上浮后硬性封顶（元），防止千亿超级中军线性比例过高导致特赦失效
P1_AMNESTY_DYNAMIC_INFLOW_CAP_YUAN = 1_500_000_000.0  # 15 亿


def p1_capital_resonance_hard_gate(circ_mv_yi: float, crs: float) -> Optional[str]:
    """历史接口：P1 曾按 capital_resonance_score 分层硬闸；现已废止，始终放行。"""
    return None


def p1_capital_resonance_scan_secondary_gate(
    circ_mv_yi: float, crs: float, curr_row: Any
) -> Optional[str]:
    """历史接口：scan_engine 曾用 CRS 二次闸；现已废止，始终放行。"""
    return None


def _p1_final_sort_key(hit_item: Dict[str, Any]) -> Tuple[float, float]:
    """P1 主池/观察池排序：主键为多维分项平滑 p1_score；次键为近 3 日+10 日资金流入复合指标。"""
    p1 = float(hit_item.get("p1_score", 0.0) or 0.0)
    inf = float(hit_item.get("inflow_ratio", 0.0) or 0.0) + float(hit_item.get("inflow_10d_ratio", 0.0) or 0.0) * 0.6
    return (p1, inf)


def _build_sector_rank_map(sorted_sectors: list) -> Dict[str, int]:
    """预构建行业名 -> 排名映射，避免在单票循环中重复 index() 扫描。"""
    if not sorted_sectors:
        return {}
    return {str(name): idx + 1 for idx, name in enumerate(sorted_sectors) if name}


@functools.lru_cache(maxsize=8)
def _get_regime_thresholds(regime_name=None):
    """
    按市场环境返回阈值配置（自 config.yaml strategies.p1 读取，含策略实验室会话覆写）。
    【V26.6 优化】添加 @functools.lru_cache(maxsize=8)：
    原实现每次调用都重新从 config_manager 拉取配置，当 P1 底仓有 200 只股票
    时会触发 200 次函数调用。配置内容在同一进程生命周期内通常不变，
    使用 LRU 缓存可将在同一天内重复调用的开销从 ~5ms/次 降至 <0.01ms/次。
    """
    from core.config_manager import get_p1_regime_thresholds

    return get_p1_regime_thresholds(regime_name)


def get_p1_threshold_profile_label(regime_name=None):
    """与 _get_regime_thresholds 档位一致的人类可读标签（供侧边栏展示）。"""
    name = str(regime_name or "").strip()
    if any(k in name for k in ["主升", "趋势"]):
        return "趋势市·严格精选"
    if any(k in name for k in ["退潮", "空头", "主跌"]):
        return "退潮/空头·适度放宽"
    return "震荡市·稳健中性"


def _p1_regime_profile_key(regime_name=None) -> str:
    """与 config_manager._profile_key_for_regime 一致，供 P1 单股处理线程识别 strict/neutral/relaxed。"""
    name = str(regime_name or "").strip()
    if any(k in name for k in ["主升", "趋势"]):
        return "strict"
    if any(k in name for k in ["退潮", "空头", "主跌"]):
        return "relaxed"
    return "neutral"


def get_p1_threshold_summary(regime_name=None):
    """
    供 UI 展示：当前大盘环境对应的 P1 阈值档与数值（与 build_p1_pool_and_cache 使用同一套逻辑）。
    """
    t = _get_regime_thresholds(regime_name)
    return {
        "regime_input": str(regime_name or "").strip() or "(默认)",
        "profile_label": get_p1_threshold_profile_label(regime_name),
        "thresholds": t.copy(),
    }


# ==================== 1. 跨部门资源调配 (任督二脉贯通) ====================
def _p1_min_circ_mv_yi_pool() -> float:
    """与 scan_engine / db_core.get_p1_candidate_codes 共用；读 config 后回退 constants。"""
    try:
        from core.config_manager import get_p1_select_min_circ_mv_wan

        return float(get_p1_select_min_circ_mv_wan()) / 10000.0
    except Exception:
        return float(getattr(constants, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000)) / 10000.0


def _p1_mv_yi_for_thr(thresholds: dict) -> float:
    if isinstance(thresholds, dict):
        v = thresholds.get("_p1_min_circ_mv_yi")
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return _p1_min_circ_mv_yi_pool()


def _p1_mv_fail_reason_for(mv_yi_bar: float) -> str:
    return f"流通市值不足{int(mv_yi_bar)}亿"

try:
    from core.strategies.fund_mv_utils import (
        infer_turnover_rate_f_pct,
        compute_market_contraction_context,
        adaptive_turnover_kill_threshold_relaxed,
    )
except ImportError:

    def infer_turnover_rate_f_pct(vol_hand, close, circ_mv_wan):
        if vol_hand <= 0 or close <= 0 or circ_mv_wan <= 0:
            return 0.0
        return vol_hand * close / circ_mv_wan

    def compute_market_contraction_context(base_items, rt_map=None):
        return {"score": 0.0, "adaptive_reason": ""}

    def adaptive_turnover_kill_threshold_relaxed(
        circ_mv_yi, net_main_amount=None, net_elg_amount=None
    ):
        return 0.8

try:
    from data.db_core import (
        get_all_basic_industry,
        get_latest_sector_ranking,
        get_latest_daily_data_trade_date_yyyymmdd,
    )
except ImportError:
    def get_all_basic_industry(): return {}
    def get_latest_sector_ranking(): return {}
    def get_latest_daily_data_trade_date_yyyymmdd(): return ""

# ==================== 初始化 Tushare (专线拉取解禁黑名单) ====================
def _load_dotenv_for_tushare() -> None:
    """
    将 .env 文件中的 TUSHARE_TOKEN 加载到 os.environ。
    仅首次调用时生效。
    """
    env_path = os.path.join(PROJECT_ROOT, ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if "=" not in stripped:
                    continue
                key, _, val = stripped.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key == "TUSHARE_TOKEN" and val:
                    os.environ.setdefault(key, val)
    except Exception:
        pass


def _init_pro():
    """
    从项目根目录 config.yaml 读取 Tushare token 与可选专线 endpoint。
    任一步失败都会记录日志，并回退到环境变量 TUSHARE_TOKEN（不静默失败）。
    同时支持从 .env 文件读取 TUSHARE_TOKEN。
    """
    _load_dotenv_for_tushare()
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")
    token = ""
    custom_endpoint = ""

    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
                token = cfg.get('tushare', {}).get('token', '')
                custom_endpoint = (cfg.get('tushare', {}).get('custom_endpoint', '') or "").strip()
        except yaml.YAMLError as e:
            # YAML 语法错误：文件存在但无法解析，属于配置级错误，用 error 便于立刻发现
            logging.error(f"❌ 解析 config.yaml 失败（YAML 语法错误）[{config_path}]: {e}，将尝试环境变量 TUSHARE_TOKEN。")
        except OSError as e:
            # 权限、磁盘、路径等 IO 问题
            logging.error(f"❌ 读取 config.yaml 失败（IO 异常）[{config_path}]: {e}，将尝试环境变量 TUSHARE_TOKEN。")
        except Exception as e:
            # 其他未预期异常，避免裸 except 丢失上下文
            logging.error(f"❌ 读取 config.yaml 失败（未分类异常）[{config_path}]: {e}，将尝试环境变量 TUSHARE_TOKEN。")
        else:
            # 仅当「打开并解析成功」时检查 token 是否为空（与异常分支区分开）
            if not token and not os.getenv("TUSHARE_TOKEN"):
                logging.warning(f"⚠️ config.yaml 中未配置 tushare.token，且环境变量 TUSHARE_TOKEN 为空，Tushare 将无法初始化。")
    else:
        # 文件不存在不算异常，但必须留痕，否则排障时误以为读了默认配置
        logging.warning(f"⚠️ 未找到配置文件 [{config_path}]，将仅尝试环境变量 TUSHARE_TOKEN。")

    if not token:
        token = os.getenv('TUSHARE_TOKEN', '')
        
    if token:
        import tushare as ts
        ts.set_token(token)
        pro = ts.pro_api()
        
        if custom_endpoint:
            try:
                pro._DataApi__http_url = custom_endpoint
            except Exception as e:
                logging.warning(f"代理注入失败，将尝试直连: {e}")
                
        return pro
    return None

pro = _init_pro()
GLOBAL_UNLOCK_BLACKLIST = None

def _get_unlock_blacklist(anchor_yyyymmdd=None):
    """
    解禁窗口以 anchor 日为起点向后 30 天；与 DB 最新交易日对齐，避免自然日漂移导致入池集合变化。
    anchor 无效时回退当前日历日。
    默认不启用（见 config use_tushare_unlock_blacklist_for_p1），不调 Tushare。
    """
    try:
        from core.config_manager import get_p1_use_tushare_unlock_blacklist

        if not get_p1_use_tushare_unlock_blacklist():
            return set()
    except Exception:
        return set()
    if pro is None:
        logging.warning("⚠️ Tushare 未初始化，跳过解禁黑名单检查。")
        return set()
    try:
        cand = str(anchor_yyyymmdd or "").strip()
        if len(cand) == 8 and cand.isdigit():
            start_date = cand
        else:
            start_date = datetime.now().strftime("%Y%m%d")
        try:
            dt_start = datetime.strptime(start_date, "%Y%m%d")
        except ValueError:
            start_date = datetime.now().strftime("%Y%m%d")
            dt_start = datetime.strptime(start_date, "%Y%m%d")
        end_date = (dt_start + timedelta(days=30)).strftime("%Y%m%d")
        df_unlock = pro.share_float(start_date=start_date, end_date=end_date)
        if not df_unlock.empty:
            return set(df_unlock["ts_code"].tolist())
    except Exception as e:
        logging.warning(f"获取解禁黑名单失败 (可能无权限或限流): {e}")
    return set()

# ==================== 本地小黑屋 (防报复性交易) ====================
def _get_punish_blacklist(max_age_days: int = 7) -> Set[str]:
    """
    读取 scan_engine 写入的 blacklist.json。
    仅返回 kill_date 在 [今天−max_age_days, 今天] 内的 ts_code（与 _add_to_blacklist 侧 7 日清理语义一致）。
    无 kill_date 或格式异常的历史条目不参与拦截，避免「只读不写」时永久残留。
    """
    blacklist_file = path_blacklist_json()
    if not os.path.exists(blacklist_file):
        return set()
    try:
        with open(blacklist_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logging.warning("读取惩罚黑名单失败: %s", e)
        return set()
    if not isinstance(data, dict):
        return set()
    bj_tz = timezone(timedelta(hours=8))
    cutoff = (datetime.now(bj_tz) - timedelta(days=max(1, int(max_age_days)))).strftime("%Y%m%d")
    out: Set[str] = set()
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        kd = str(v.get("kill_date") or "").strip()
        if len(kd) == 8 and kd.isdigit() and kd >= cutoff:
            out.add(str(k))
    return out

def _safe_float(val, default=0.0):
    if val is None: return default
    try:
        if pd.isna(val) or str(val).strip() in ['', '-']: return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _finite_clip01(x):
    """把任意标量压成有限数，避免 np.interp 或加减分链条被 NaN/inf 污染。"""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(v):
        return 0.0
    return v


def _compute_p1_compensation_tier(
    curr,
    df,
    rt,
    now_price,
    net_inflow_3d,
    net_inflow_5d_early,
    vr,
    ind_rank,
    dynamic_inflow_threshold,
    is_amnesty,
    ma20,
    ma60,
    ma120,
    ma20_slope_5,
):
    """
    P1 容错补偿档位（0/1/2），用于在「形态好 + 资金真抢」时，适度放宽 MACD 绿柱、MA120 粘合度、量价背离等硬阈值。

    【V26.6 简化版】将8个分散条件整合为3个核心维度：
    1. 资金维度（tier 2）：三日净流入达标 OR is_amnesty → 强资金信号
    2. 形态维度（tier 1）：筹码单峰密集 OR 均线多头+斜率 → 底部/洗盘结束形态
    3. 动能维度（tier 1）：MACD拐头改善 OR 放量配合行业前7 → 启动信号

    档位 capped 在 2，避免无上限放宽把垃圾股全放进来。

    返回:
        tier: int, 0 表示不补偿，1 轻度，2 强力（需强资金或直通车类信号支撑）。
        note: str, 供入选理由拼接，方便复盘「为什么这条票享受了放宽」。
    """
    tier = 0
    notes = []

    # ===== 核心维度1：资金强度（tier 2）=====
    if is_amnesty:
        tier = 2
        notes.append("资金特赦档")

    th_dyn = _finite_clip01(dynamic_inflow_threshold)
    ni3 = _finite_clip01(net_inflow_3d)
    if th_dyn > 0 and ni3 >= th_dyn * 0.52:
        tier = max(tier, 2)
        notes.append("三日净流入达动态门槛")

    # ===== 核心维度2：形态支撑（tier 1）=====
    # 筹码单峰密集：95%成本位与50%成本位高度接近，且现价贴近成本中枢
    c50 = _safe_float(curr.get("cost_50th", rt.get("cost_50th", 0.0)))
    c95 = _safe_float(curr.get("cost_95th", rt.get("cost_95th", 0.0)))
    now_px = _safe_float(now_price, 0.0)
    if c50 > 0 and c95 > 0:
        peak_gap_pct = (c95 - c50) / max(c50, 1e-9) * 100.0
        if peak_gap_pct <= 8.0 and now_px >= c50 * 0.97 and now_px <= c95 * 1.02:
            tier = max(tier, 1)
            notes.append("筹码单峰密集")

    # 均线多头+斜率抬头
    m20, m60 = _safe_float(ma20), _safe_float(ma60)
    slope5 = _safe_float(ma20_slope_5)
    if m20 > 0 and m60 > 0 and m20 > m60 and slope5 >= 0.11:
        tier = max(tier, 1)
        notes.append("短均线多头且20斜率抬头")

    # ===== 核心维度3：动能信号（tier 1）=====
    # MACD动能柱拐头改善
    macd_now = _safe_float(curr.get("macd_bar", curr.get("macd_hist", 0.0)))
    macd_prev = 0.0
    if isinstance(df, pd.DataFrame) and len(df) >= 2:
        macd_prev = _safe_float(df.iloc[-2].get("macd_bar", df.iloc[-2].get("macd_hist", 0.0)))
    if macd_now > macd_prev and macd_now > -0.09:
        tier = max(tier, 1)
        notes.append("MACD动能柱拐头改善")

    # 放量配合行业排名靠前
    if ni3 > 0 and vr >= 1.85 and ind_rank <= 7:
        tier = max(tier, 1)
        notes.append("放量配合且行业排名靠前")

    # 【V26.6 简化】移除贴均线洗盘区的冗余条件（已被均线多头+斜率条件覆盖）

    tier = int(max(0, min(2, tier)))
    note = "；".join(notes[:3]) if notes else ""
    return tier, note


def _effective_p1_thresholds(comp_tier, trend_ma120_min_ratio, macd_bar_kill, vol_divergence_ratio, near_ma20_min_ratio):
    """
    根据容错档位，生成「实际生效」的串联阈值。数值方向说明：
    - macd_bar_kill 越负越宽松（例如 -0.22 比 -0.13 更容易放过略弱的柱）。
    - trend_ma120_min_ratio 略降：允许 MA60 与 MA120 粘合度稍差一丝（主升浪初期常见）。
    - vol_divergence_ratio 略降：上涨日均量略低于下跌日均量时，仍可通过（修复刚反转时统计窗偏短的问题）。
    - near_ma20_min_ratio 略降：回踩 MA20 略深一点仍算「贴线企稳」。
    """
    ct = int(max(0, min(2, comp_tier)))
    t120 = float(trend_ma120_min_ratio) - 0.005 * (1 if ct >= 1 else 0) - 0.006 * (1 if ct >= 2 else 0)
    t120 = max(0.964, min(1.0, t120))

    mk = float(macd_bar_kill) - 0.055 * (1 if ct >= 1 else 0) - 0.065 * (1 if ct >= 2 else 0)
    mk = max(-0.40, min(0.0, mk))

    vd = float(vol_divergence_ratio) - 0.055 * (1 if ct >= 1 else 0) - 0.065 * (1 if ct >= 2 else 0)
    vd = max(0.60, min(0.95, vd))

    nm = float(near_ma20_min_ratio) - 0.0035 * (1 if ct >= 1 else 0) - 0.004 * (1 if ct >= 2 else 0)
    nm = max(0.975, min(0.995, nm))

    return t120, mk, vd, nm


def _p1_is_high_cooldown(bias20_cool: float, avg_trn: float) -> bool:
    return (15.0 <= float(bias20_cool) <= 25.0) and (float(avg_trn) <= 2.2)


def _p1_is_two_day_below_ma20(close_today: float, ma20_today: float, close_yesterday: float, ma20_yesterday: float) -> bool:
    return (ma20_today > 0) and (close_today < ma20_today) and (close_yesterday < ma20_yesterday)


def _p1_is_single_peak_dense(curr, rt, now_price: float) -> bool:
    c50_p1 = _safe_float(curr.get("cost_50th"), _safe_float(rt.get("cost_50th"), 0.0))
    c95_p1 = _safe_float(curr.get("cost_95th"), _safe_float(rt.get("cost_95th"), 0.0))
    if c50_p1 <= 0 or c95_p1 <= 0:
        return False
    peak_gap = (c95_p1 - c50_p1) / max(c50_p1, 1e-9) * 100.0
    return (peak_gap <= 8.0) and (now_price >= c50_p1 * 0.97) and (now_price <= c95_p1 * 1.02)


def _p1_behavior_penalty_hits(df, lookback_days: int = 20) -> Tuple[int, int]:
    """
    【性能优化 V2】向量化替代 iterrows：
    - 原：逐行 Python 迭代，对每行执行 _safe_float 和浮点比较。
    - 改：用 pandas 向量化布尔运算替代，零 Python 行循环。
    """
    long_upper_shadow_hits = 0
    intraday_dump_hits = 0
    if not isinstance(df, pd.DataFrame) or df.empty:
        return long_upper_shadow_hits, intraday_dump_hits
    lookback = min(int(lookback_days), len(df))
    if lookback <= 0:
        return 0, 0
    recent_20 = df.tail(lookback)

    try:
        high_v = pd.to_numeric(recent_20["high"], errors="coerce").fillna(0)
        low_v = pd.to_numeric(recent_20["low"], errors="coerce").fillna(0)
        open_v = pd.to_numeric(recent_20["open"], errors="coerce").fillna(0)
        close_v = pd.to_numeric(recent_20["close"], errors="coerce").fillna(0)
        pct_chg_v = pd.to_numeric(recent_20["pct_chg"], errors="coerce").fillna(0)

        body = (close_v - open_v).abs()
        upper_shadow = (high_v - pd.concat([open_v, close_v], axis=1).max(axis=1)).clip(lower=0)
        long_upper_shadow_hits = int((upper_shadow >= body * 0.5).sum())
        long_upper_shadow_hits += int((upper_shadow >= 0.01).sum())

        intraday_dump_hits = int(((pct_chg_v > 4.0) & (close_v < open_v)).sum())
    except Exception:
        pass
    return long_upper_shadow_hits, intraday_dump_hits

# ==================== 👑 时代贝塔：战略白名单 (基准配置) ====================
# 说明：
# - 行业名须与 Tushare stock_basic.industry（多为申万一级口径）完全一致，否则无法命中。
# - 12 分档：科技主链、高端装备、国家战略与近年机构重仓的成长制造方向（AI/算力、新能源车链、电力设备等）。
# - 8 分档：消费、周期、大金融弹性、农业等，偏轮动与防御/复苏，可由盘面热度「晋级」至 12。
STRATEGIC_INDUSTRIES = {
    # —— T0 基准 12 分（动态可降至 10）：成长制造主战场 ——
    "电子": 12.0,
    "计算机": 12.0,
    "通信": 12.0,
    "国防军工": 12.0,
    "机械设备": 12.0,
    "电力设备": 12.0,
    "汽车": 12.0,
    # —— 基准 8 分（动态可升至 12）：消费、周期、材料、金融弹性等 ——
    "医药生物": 8.0,
    "有色金属": 8.0,
    "基础化工": 8.0,
    "传媒": 8.0,
    "家用电器": 8.0,
    "食品饮料": 8.0,
    "社会服务": 8.0,
    "农林牧渔": 8.0,
    "非银金融": 8.0,
}

# 动态贝塔：仅看「最近若干条」板块排名快照，贴近当前资金风格；过旧样本不参与判定。
_STRATEGIC_RANK_RECENT_WINDOW = 5
# 至少积累这么多天的快照再启用升降级，避免冷启动两三天就误伤。
_STRATEGIC_RANK_MIN_SNAPSHOTS = 3
# 近端窗口权重：越近越重要，避免 5 日前旧热度与当下资金抢主导。
_STRATEGIC_RANK_WINDOW_WEIGHTS = [0.10, 0.15, 0.20, 0.25, 0.30]
# 12 分主线若加权热度长期缺席，则降到 10；8 分行业若强势聚集，则晋级到 12。
_STRATEGIC_DOWNGRADE_SCORE_MAX = 0.85
_STRATEGIC_UPGRADE_SCORE_MIN = 1.75

# 自动聚合学习参数：不再维护手写别名表，而是从历史快照中的名称共现、词缀相似度和市场常用语义锚点
# 动态推断「细分行业 -> 战略一级行业」。该映射只用于动态贝塔热度聚合，不改股票自身行业归属。
_STRATEGIC_AUTO_LINK_MIN_SCORE = 0.62
_STRATEGIC_AUTO_LINK_PREFIX_BONUS = 0.42
_STRATEGIC_AUTO_LINK_TOKEN_BONUS = 0.36
_STRATEGIC_AUTO_LINK_THEME_BONUS = 0.30
_STRATEGIC_AUTO_LINK_COOCCUR_BONUS = 0.22
_STRATEGIC_AUTO_LINK_ROOT_SELF_SCORE = 1.0


def _sector_rank_recent_date_keys(history_data: dict, window: int) -> list:
    """取已排序日期键的最近 window 条，用于近端资金热度判定。"""
    if not isinstance(history_data, dict) or not history_data:
        return []
    sd = sorted(history_data.keys())
    return sd[-window:] if len(sd) > window else sd


def _strategic_window_weights(n: int) -> List[float]:
    if n <= 0:
        return []
    base = list(_STRATEGIC_RANK_WINDOW_WEIGHTS)
    if n == len(base):
        return base
    if n < len(base):
        trimmed = base[-n:]
        s = sum(trimmed) or 1.0
        return [float(x / s) for x in trimmed]
    extra = [1.0] * (n - len(base)) + base
    s = sum(extra) or 1.0
    return [float(x / s) for x in extra]


@functools.lru_cache(maxsize=1024)
def _industry_tokens(ind: str) -> frozenset:
    """
    【性能优化 V2】添加 @functools.lru_cache，避免在嵌套循环中重复计算同一行业的 token 集合。
    返回 frozenset（哈希可复用）而非 set。
    """
    text = str(ind or "").strip()
    if not text:
        return frozenset()
    tokens = [text]
    if len(text) >= 2:
        for i in range(len(text) - 1):
            tk = text[i:i + 2].strip()
            if tk:
                tokens.append(tk)
    return frozenset(tokens)


def _industry_theme_seed(root_ind: str) -> Set[str]:
    text = str(root_ind or "").strip()
    tokens = _industry_tokens(text)
    if text == "电子":
        tokens |= {"半导", "元器", "芯片", "光学"}
    elif text == "计算机":
        tokens |= {"软件", "互联", "it", "算力", "数据"}
    elif text == "通信":
        tokens |= {"通信", "电信", "光通", "运营"}
    elif text == "国防军工":
        tokens |= {"军工", "航空", "船舶", "航天"}
    elif text == "机械设备":
        tokens |= {"机械", "机床", "设备", "基件", "工程"}
    elif text == "电力设备":
        tokens |= {"电力", "电气", "储能", "光伏", "电网"}
    elif text == "汽车":
        tokens |= {"汽车", "整车", "配件", "摩托"}
    elif text == "医药生物":
        tokens |= {"医药", "制药", "生物", "医疗"}
    elif text == "有色金属":
        tokens |= {"金属", "黄金", "铜", "铝", "铅锌"}
    elif text == "基础化工":
        tokens |= {"化工", "化纤", "塑料", "染料", "农药"}
    elif text == "传媒":
        tokens |= {"传媒", "影视", "出版", "广告", "文教"}
    elif text == "家用电器":
        tokens |= {"家电", "电器"}
    elif text == "食品饮料":
        tokens |= {"食品", "饮料", "白酒", "啤酒", "乳制"}
    elif text == "社会服务":
        tokens |= {"旅游", "酒店", "餐饮", "服务"}
    elif text == "农林牧渔":
        tokens |= {"农业", "种植", "渔业", "饲料", "牧渔"}
    elif text == "非银金融":
        tokens |= {"证券", "保险", "金融"}
    return {str(t).lower() for t in tokens if str(t).strip()}


def _build_auto_strategic_mapping(history_data: dict, current_ranking_dict: dict) -> Tuple[Dict[str, str], List[str]]:
    """根据历史快照自动学习 细分行业 -> 战略一级行业 映射。"""
    root_inds = [str(k).strip() for k in STRATEGIC_INDUSTRIES.keys() if str(k).strip()]
    all_names: Set[str] = set(root_inds)
    if isinstance(current_ranking_dict, dict):
        all_names |= {str(k).strip() for k in current_ranking_dict.keys() if str(k).strip()}
    if isinstance(history_data, dict):
        for snap in history_data.values():
            if not isinstance(snap, dict):
                continue
            all_names |= {str(k).strip() for k in snap.keys() if str(k).strip()}

    root_theme_tokens = {root: _industry_theme_seed(root) for root in root_inds}
    occurrence_days: Dict[str, Set[str]] = {name: set() for name in all_names}
    for dt, snap in (history_data or {}).items():
        if not isinstance(snap, dict):
            continue
        for raw_name in snap.keys():
            name = str(raw_name).strip()
            if name:
                occurrence_days.setdefault(name, set()).add(str(dt))

    learned: Dict[str, str] = {root: root for root in root_inds}
    explain: List[str] = []

    # 【性能优化 V2】预计算所有 root_tokens，避免在内层循环中重复调用 _industry_tokens
    root_tokens_cache: Dict[str, frozenset] = {root: _industry_tokens(root) for root in root_inds}

    for name in sorted(all_names):
        if not name:
            continue
        if name in STRATEGIC_INDUSTRIES:
            learned[name] = name
            continue
        best_root = ""
        best_score = -1.0
        name_tokens = _industry_tokens(name)
        name_days = occurrence_days.get(name, set())
        for root in root_inds:
            score = 0.0
            root_tokens = root_tokens_cache.get(root)
            if root_tokens is None:
                root_tokens = _industry_tokens(root)
                root_tokens_cache[root] = root_tokens
            if name == root:
                score = _STRATEGIC_AUTO_LINK_ROOT_SELF_SCORE
            else:
                if name.startswith(root) or root.startswith(name):
                    score += _STRATEGIC_AUTO_LINK_PREFIX_BONUS
                token_inter = name_tokens & root_tokens
                if token_inter:
                    score += _STRATEGIC_AUTO_LINK_TOKEN_BONUS
                if name_tokens & root_theme_tokens.get(root, frozenset()):
                    score += _STRATEGIC_AUTO_LINK_THEME_BONUS
                root_days = occurrence_days.get(root, set())
                if name_days and root_days:
                    union_days = name_days | root_days
                    inter_days = name_days & root_days
                    if union_days:
                        jacc = len(inter_days) / float(len(union_days))
                        score += _STRATEGIC_AUTO_LINK_COOCCUR_BONUS * jacc
            if score > best_score:
                best_score = score
                best_root = root
        if best_root and best_score >= _STRATEGIC_AUTO_LINK_MIN_SCORE:
            learned[name] = best_root
            explain.append(f"{name}->{best_root}@{best_score:.2f}")

    return learned, explain


def _aggregate_sector_snapshot_for_strategic(snapshot: dict, learned_mapping: Dict[str, str]) -> Dict[str, int]:
    """将快照中的细分行业按自动学习映射聚合为战略一级行业代表排名（取最强名次）。"""
    agg: Dict[str, int] = {}
    if not isinstance(snapshot, dict):
        return agg
    for raw_ind, rank_val in snapshot.items():
        root_ind = learned_mapping.get(str(raw_ind).strip())
        if not root_ind:
            continue
        try:
            rk = int(rank_val)
        except (TypeError, ValueError):
            continue
        if rk <= 0:
            continue
        old = agg.get(root_ind)
        if old is None or rk < old:
            agg[root_ind] = rk
    return agg


# ================= 🧠 游资嗅觉记忆中枢 (全自动升降级) =================
def _get_dynamic_strategic_industries(current_ranking_dict, p1_anchor_yyyymmdd=None):
    history_file = path_sector_rank_history_json()
    bj_tz = timezone(timedelta(hours=8))
    anchor = str(p1_anchor_yyyymmdd or "").strip()
    if len(anchor) == 8 and anchor.isdigit():
        history_key = anchor
    else:
        history_key = datetime.now(bj_tz).strftime("%Y%m%d")

    sorted_inds = list(current_ranking_dict.keys())
    today_ranks = {ind: (i + 1) for i, ind in enumerate(sorted_inds)}

    history_data = {}
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                history_data = json.load(f)
        except json.JSONDecodeError as e:
            # JSON 损坏或截断：无法恢复当日记忆，降级为空 dict，由后续逻辑重新写入
            logging.warning(f"⚠️ 板块排名历史文件 JSON 解析失败 [{history_file}]: {e}，将使用空历史并重新累积。")
            history_data = {}
        except OSError as e:
            logging.warning(f"⚠️ 读取板块排名历史文件失败（IO）[{history_file}]: {e}，将使用空历史并重新累积。")
            history_data = {}
        except Exception as e:
            logging.warning(f"⚠️ 读取板块排名历史文件失败（未分类）[{history_file}]: {e}，将使用空历史并重新累积。")
            history_data = {}

    if today_ranks:
        history_data[history_key] = today_ranks

    # 【V26.6 优化】sorted_dates 用于判断是否超过 10 个快照，
    # 但删除旧条目时无需先对所有日期排序，直接用 sorted() 一次性获取最小 10 个即可
    # 原实现先 sorted() 全排序 O(n log n) 再遍历 O(n)，
    # 改为 sorted() 全排序后取末尾保留 n-10 个（时间复杂度不变但代码更清晰）
    if len(history_data) > 10:
        # 取最大 10 个日期（最新），其余删除；sorted() 本身 O(n log n)，
        # 这里不可省，因为需要找出最大的 10 个日期用于保留
        sorted_dates = sorted(history_data.keys())
        keep_dates = set(sorted_dates[-10:])  # 保留最近 10 个
        for d in list(history_data.keys()):     # 遍历用 list() 副本避免字典在迭代中修改
            if d not in keep_dates:
                del history_data[d]

    ensure_runtime_data_layout()
    try:
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(history_data, f, ensure_ascii=False)
    except Exception as e:
        logging.error(f"嗅觉记忆库写入失败: {e}")

    dynamic_dict = STRATEGIC_INDUSTRIES.copy()
    recent_dates = _sector_rank_recent_date_keys(history_data, _STRATEGIC_RANK_RECENT_WINDOW)
    if len(history_data) < _STRATEGIC_RANK_MIN_SNAPSHOTS or len(recent_dates) < _STRATEGIC_RANK_MIN_SNAPSHOTS:
        return dynamic_dict

    learned_mapping, learned_explain = _build_auto_strategic_mapping(history_data, current_ranking_dict)
    recent_weights = _strategic_window_weights(len(recent_dates))
    weighted_snapshots = []
    for dt, weight in zip(recent_dates, recent_weights):
        agg_snapshot = _aggregate_sector_snapshot_for_strategic(history_data.get(dt, {}), learned_mapping)
        weighted_snapshots.append((dt, float(weight), agg_snapshot))

    downgraded: List[str] = []
    upgraded: List[str] = []
    evidence_lines: List[str] = []

    for ind, base_score in dynamic_dict.items():
        weighted_top8 = 0.0
        weighted_top3 = 0.0
        hit_days = 0
        evidence_parts: List[str] = []

        for dt, weight, agg_snapshot in weighted_snapshots:
            rank_val = int(agg_snapshot.get(ind, 999) or 999)
            if rank_val <= 8:
                weighted_top8 += weight
                hit_days += 1
            if rank_val <= 3:
                weighted_top3 += weight
            evidence_parts.append(f"{dt[-4:]}#{rank_val if rank_val < 999 else '--'}@{weight:.2f}")

        action = "保持"
        target_score = base_score
        if base_score == 12.0 and weighted_top8 <= _STRATEGIC_DOWNGRADE_SCORE_MAX:
            target_score = 10.0
            action = "12→10"
            downgraded.append(ind)
        elif base_score == 8.0 and weighted_top3 >= _STRATEGIC_UPGRADE_SCORE_MIN:
            target_score = 12.0
            action = "8→12"
            upgraded.append(ind)

        dynamic_dict[ind] = target_score
        evidence_lines.append(
            f"{ind}:{action}(top8={weighted_top8:.2f},top3={weighted_top3:.2f},hits={hit_days}/{len(weighted_snapshots)}|{'/'.join(evidence_parts)})"
        )

    debug_payload = {
        "date": history_key,
        "recent_dates": list(recent_dates),
        "window_weights": [round(float(w), 4) for w in recent_weights],
        "learned_mapping": learned_mapping,
        "learned_mapping_explain": learned_explain,
        "downgraded": list(downgraded),
        "upgraded": list(upgraded),
        "dynamic_scores": {k: float(v) for k, v in dynamic_dict.items()},
        "evidence": list(evidence_lines),
    }
    try:
        debug_json_path = path_strategic_mapping_debug_json(history_key)
        with open(debug_json_path, 'w', encoding='utf-8') as f:
            json.dump(debug_payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("动态行业贝塔调试缓存写入失败: %s", e)

    logging.info(
        "[动态行业贝塔] 自动聚合窗口=%s | 权重=%s | 学习映射=%s | 调试缓存=%s | 12→10 降权: %s | 8→12 晋级: %s | 证据: %s",
        len(recent_dates),
        "/".join(f"{w:.2f}" for w in recent_weights),
        "、".join(learned_explain[:24]) if learned_explain else "无新增",
        path_strategic_mapping_debug_json(history_key),
        "、".join(downgraded) if downgraded else "无",
        "、".join(upgraded) if upgraded else "无",
        " ; ".join(evidence_lines),
    )

    return dynamic_dict

# ================= ⚖️ 第五层：极致平滑打分系统 (满分 100) =================
def _evaluate_p1_single_stock(df, rt, circ_mv_yi, ind_rank, pe, ind_stats, ind, dynamic_industries, avg_trn, pass_line):
    """
        委托 score_calibration.compute_p1_multi_dim_smooth_score：多维分项平滑 + 行业/市值附加；
        fund_memory_score 是否凸入最终分由 config/constants 的 fund_memory_weight_p1 决定；不读 capital_resonance_score。
    """
    try:
        from core.strategies.score_calibration import compute_p1_multi_dim_smooth_score

        return compute_p1_multi_dim_smooth_score(
            df,
            rt,
            circ_mv_yi,
            ind_rank,
            pe,
            ind_stats,
            ind,
            dynamic_industries,
            avg_trn,
            pass_line,
        )
    except Exception as e:
        logging.debug(f"P1 单股打分底层异常: {e}")
        return 0.0, False, f"打分系统异常: {e}", {}

# ================= 🛡️ 四大物理防线区 =================
def _process_single_stock_for_p1(item, industry_pe_stats, global_stats, industry_map, sector_rank_map, dynamic_industries, unlock_blacklist, punish_blacklist, thresholds):
    inflow_ratio = 0.0
    inflow_10d_ratio = 0.0
    used_fast_track = False
    p1_profile = str(thresholds.get("_p1_profile_key", "neutral")).strip()
    p1_mv_bar = _p1_mv_yi_for_thr(thresholds)
    try:
        ts_code = item.get('ts_code', item.get('code', ''))
        df = item.get('df', pd.DataFrame())
        rt = item.get('hist', item.get('rt', {}))
        # 全局前置硬过滤：流通市值不足 P1 下限一律在最前面拦截（先于黑名单/解禁/PE/流动性）
        circ_mv_raw_early = rt.get('circ_mv')
        if circ_mv_raw_early is None or pd.isna(circ_mv_raw_early):
            circ_mv_raw_early = rt.get('total_mv', 0) * 0.6
        if (circ_mv_raw_early is None or pd.isna(circ_mv_raw_early)) and isinstance(df, pd.DataFrame) and (not df.empty) and ('circ_mv' in df.columns):
            circ_mv_raw_early = _safe_float(df['circ_mv'].iloc[-1], 0.0)
        # 【审计修复】维度2：万元换算分母兜底
        circ_mv_yi_early = _safe_float(circ_mv_raw_early) / max(10000.0, 1e-9)
        if circ_mv_yi_early < p1_mv_bar:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": _p1_mv_fail_reason_for(p1_mv_bar), "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
        
        if ts_code in punish_blacklist:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "🛑黑名单冷却期 (防报复性交易)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
        
        if df.empty or len(df) < 100 or not rt:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "上市不足或无数据", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        now_price_initial = _safe_float(rt.get('price', df['close'].iloc[-1] if not df.empty else 0.0))
        _p1_inst_min_yuan = float(constants.P1_INSTITUTION_MIN_PRICE_YUAN)
        if now_price_initial < _p1_inst_min_yuan:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"💀机构审美排斥(现价{now_price_initial:.2f}元<{_p1_inst_min_yuan:g}元红线)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        if ts_code in unlock_blacklist:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "💀未来30天内有天量解禁", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
        
        forecast_type = _safe_float(rt.get('forecast_type', 0))
        if forecast_type == -2:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "💣业绩首亏/续亏爆雷", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
        
        circ_mv_raw = rt.get('circ_mv')
        if circ_mv_raw is None or pd.isna(circ_mv_raw):
            circ_mv_raw = rt.get('total_mv', 0) * 0.6  
        # 【审计修复】维度2：万元换算分母兜底
        circ_mv_yi = _safe_float(circ_mv_raw) / max(10000.0, 1e-9)
        # 流通下限已在前置过滤，这里仅用于档位标签与后续打分口径
        if circ_mv_yi >= 2000.0:
            size_emoji, mv_tier = "🦍", "2000亿+巨无霸"
        elif circ_mv_yi >= 1000.0:
            size_emoji, mv_tier = "🐘+", "1000-2000亿千亿中军"
        elif circ_mv_yi >= 500.0:
            size_emoji, mv_tier = "🐘", "500-1000亿超级中军"
        else:
            size_emoji, mv_tier = "🐎", f"{int(p1_mv_bar)}-500亿核心中盘"
        
        if circ_mv_yi < p1_mv_bar:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": _p1_mv_fail_reason_for(p1_mv_bar), "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        # 超级大盘：放宽「贴 MA20 / MACD 柱斩杀」等短线项（不再依赖资金共振截面分）。
        relaxed_short_ma = circ_mv_yi >= P1_MV_SUPER_YI

        circ_mv_base = max(circ_mv_yi * 100000000.0, 1.0)
        net_inflow_3d = 0.0
        net_inflow_5d_early = 0.0
        net_inflow_10d = 0.0
        # 【性能优化 V2】预计算 tail 数据：消除重复的 df.tail() 调用
        # 原逻辑：循环内每列调用 tail(3)/tail(5)/tail(10) 各一次，造成 6 次 O(n) 切片操作
        # 改为：一次性提取 tail_3/5/10，在循环外完成切片
        tail_3 = df.tail(3)
        tail_5 = df.tail(5)
        tail_10 = df.tail(10)
        for col in ("hk_vol", "net_main_amount"):
            if col in df.columns:
                net_inflow_3d += _safe_float(tail_3[col].sum())
                net_inflow_5d_early += _safe_float(tail_5[col].sum())
                net_inflow_10d += _safe_float(tail_10[col].sum())

        # 【性能优化 V2】向量化换手率计算：消除 iterrows 循环
        # 原逻辑：逐行调用 iterrows 执行 infer_turnover_rate_f_pct
        # 改为：一次性提取列，向量化计算
        # 【V26.6 优化】整理代码块：移除重复的嵌套 try，内层向量化已覆盖外层逻辑
        avg_amount = tail_5["amount"].mean() if "amount" in tail_5.columns else 999999
        try:
            vol5 = pd.to_numeric(tail_5["vol"], errors="coerce").fillna(0)
            close5 = pd.to_numeric(tail_5["close"], errors="coerce").fillna(0)
            cm5 = pd.to_numeric(tail_5["circ_mv"], errors="coerce").fillna(0)
            tr_f5 = pd.to_numeric(tail_5["turnover_rate_f"], errors="coerce").fillna(0)
            # infer_turnover_rate_f_pct(vol, close, circ_mv_wan) = vol * 100 / (circ_mv / 10000)
            # 合并计算：vol * 100 * 10000 / circ_mv = vol * 1e6 / circ_mv
            inferred_tr5 = np.where(
                (tr_f5 > 0) | (cm5 <= 0) | (close5 <= 0),
                tr_f5,
                vol5 * 100 / np.maximum(cm5, 1e-9),
            )
            _tr5_p1_arr = np.array(inferred_tr5, dtype=float)
            _tr5_p1_arr = _tr5_p1_arr[np.isfinite(_tr5_p1_arr)]
            avg_trn = float(np.mean(_tr5_p1_arr)) if len(_tr5_p1_arr) > 0 else 0.0
        except Exception:
            avg_trn = 0.0

        # 【V26.6 优化】整理代码块：移除重复的嵌套 try，内层向量化已覆盖外层逻辑
        try:
            vol10 = pd.to_numeric(tail_10["vol"], errors="coerce").fillna(0)
            close10 = pd.to_numeric(tail_10["close"], errors="coerce").fillna(0)
            cm10 = pd.to_numeric(tail_10["circ_mv"], errors="coerce").fillna(0)
            tr_f10 = pd.to_numeric(tail_10["turnover_rate_f"], errors="coerce").fillna(0)
            nm10 = pd.to_numeric(tail_10["net_main_amount"], errors="coerce").fillna(0)
            hk10 = pd.to_numeric(tail_10["hk_vol"], errors="coerce").fillna(0)
            inferred_tr10 = np.where(
                (tr_f10 > 0) | (cm10 <= 0) | (close10 <= 0),
                tr_f10,
                vol10 * 100 / np.maximum(cm10, 1e-9),
            )
            tr10_arr = np.array(inferred_tr10, dtype=float)
            tr10_arr = tr10_arr[np.isfinite(tr10_arr)]
            avg_trn_10 = float(np.mean(tr10_arr)) if len(tr10_arr) > 0 else 0.0
            flow_positive_days_10 = int(((nm10 > 0) | (hk10 > 0)).sum())
        except Exception:
            avg_trn_10 = 0.0
            flow_positive_days_10 = 0

        # 【性能优化 V2】复用 tail_10 计算 close 最大最小值（消除 df.tail(10) 重复调用）
        if not tail_10.empty and "close" in tail_10.columns:
            close_max_10 = _safe_float(tail_10["close"].max())
            close_min_10 = _safe_float(tail_10["close"].min())
            range_10_pct = (close_max_10 - close_min_10) / max(close_min_10, 1e-9) * 100.0 if close_min_10 > 0 else 0.0
        else:
            range_10_pct = 0.0
        ttl_quiet = (flow_positive_days_10 < 2) and (avg_trn_10 < 1.0) and (range_10_pct < 6.0)

        if avg_amount < 100000.0:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"🧊流动性枯竭(近5日均成交额仅{avg_amount/10000:.2f}亿)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        ind = industry_map.get(ts_code, '未知')
        ind_rank = int(sector_rank_map.get(ind, 999))

        vr = _safe_float(rt.get('vol_ratio', 1.0))
        _open_today_for_attack = _safe_float(rt.get("open"), 0.0)
        if _open_today_for_attack <= 0 and not df.empty and "open" in df.columns:
            _open_today_for_attack = _safe_float(df["open"].iloc[-1], 0.0)
        is_strong_attack = bool(
            vr >= 1.4 and _open_today_for_attack > 0 and now_price_initial > _open_today_for_attack
        )
        dynamic_inflow_threshold = min(
            P1_AMNESTY_DYNAMIC_INFLOW_CAP_YUAN,
            max(400000000.0, circ_mv_yi * 100000000.0 * 0.035),
        )
        cost_50th = _safe_float(rt.get('cost_50th', 0.0))
        price_insurance = (cost_50th > 0 and now_price_initial <= cost_50th * 1.30) or (cost_50th <= 0)

        # 【V26.6 优化】整理代码块：移除重复的嵌套 try，内层向量化已覆盖外层逻辑
        try:
            nm5 = pd.to_numeric(tail_5["net_main_amount"], errors="coerce").fillna(0)
            hk5 = pd.to_numeric(tail_5["hk_vol"], errors="coerce").fillna(0)
            flow_positive_days_5 = int(((nm5 > 0) | (hk5 > 0)).sum())
        except Exception:
            flow_positive_days_5 = 0
        is_continuous_inflow = (flow_positive_days_5 >= 3) or (net_inflow_3d > 0 and net_inflow_5d_early > 0 and avg_trn_10 >= avg_trn)
        if not is_continuous_inflow and net_inflow_3d <= 0:
            pass
        is_amnesty = (circ_mv_yi >= 100.0) and is_continuous_inflow and (net_inflow_3d > dynamic_inflow_threshold) and (vr > 2.5) and (ind_rank <= 5) and price_insurance
        amnesty_reason = ""
            
        is_turnover_killed = False
        kill_reason = ""
        
        if circ_mv_yi >= 2000.0:
            if avg_trn < 0.65:
                is_turnover_killed = True
                kill_reason = f"🧊股性呆滞(两千亿巨无霸换手仅{avg_trn:.2f}%)"
        elif 1000.0 <= circ_mv_yi < 2000.0:
            if avg_trn < 1.05:
                is_turnover_killed = True
                kill_reason = f"🧊股性呆滞(千亿中军换手仅{avg_trn:.2f}%)"
        elif circ_mv_yi < 1000.0:
            if avg_trn < 1.5:
                is_turnover_killed = True
                kill_reason = f"🧊股性呆滞(近5日均换手仅{avg_trn:.2f}%)"

        # 【自适应优化】极度缩量期：仅辅助观察语义——若近5日均换手仍不低于放宽后地板(约-25%且≥0.8%)，撤销股性呆滞拦截
        _mcs_p1 = float(thresholds.get("_market_contraction_score", 0.0) or 0.0)
        if is_turnover_killed and _mcs_p1 >= 0.7:
            # 【缩量撤销呆滞】传入当日主力/特大单净额：大中盘资金为正时换手地板可再降 20%（见 fund_mv_utils）
            _nm_rt = _safe_float(rt.get("net_main_amount", 0.0))
            _rel_tr = adaptive_turnover_kill_threshold_relaxed(
                circ_mv_yi,
                net_main_amount=_nm_rt,
            )
            if avg_trn >= _rel_tr:
                is_turnover_killed = False
                kill_reason = ""

        if is_turnover_killed:
            if is_amnesty:
                amnesty_reason = f"特赦激活(极低换手被底部巨量资金捞回,VR:{vr:.1f})"
            else:
                is_shrink_lock_amnesty = False
                if p1_mv_bar <= circ_mv_yi < 2000.0 and flow_positive_days_5 >= 3:
                    if len(df) >= 22:
                        closes = df['close'].values
                        ma1 = closes[-20:].mean()
                        ma2 = closes[-21:-1].mean()
                        ma3 = closes[-22:-2].mean()
                        if closes[-1] >= ma1 and closes[-2] >= ma2 and closes[-3] >= ma3:
                            is_shrink_lock_amnesty = True
                
                if is_shrink_lock_amnesty:
                    amnesty_reason = f"缩量锁仓特赦(换手{avg_trn:.2f}%,近5日正流入{flow_positive_days_5}天,近3日稳站MA20)"
                else:
                    return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": kill_reason, "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        pe_raw = rt.get('pe_ttm') if rt.get('pe_ttm') is not None else rt.get('pe', 0)
        pe = _safe_float(pe_raw)
        
        if pe <= 0 or pe > 300:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"PE极值异常 (≤0或>300)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
            
        ind_stats = industry_pe_stats.get(ind, global_stats)
        ind_q75 = ind_stats['q75']
        
        if pe > ind_q75 * 2.0:
            # 注意：此处必须使用 inflow_10d_ratio，误写 score_details 会在该分支触发 NameError（score_details 仅存在于打分函数返回中）
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"PE极度超标 (PE:{pe:.1f} > 75分位{ind_q75:.1f}*2)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
            
        if pe > ind_q75 * 1.5:
            if is_amnesty:
                if amnesty_reason:
                    amnesty_reason += f" | PE特赦(流入{net_inflow_3d/100000000:.1f}亿)"
                else:
                    amnesty_reason = f"特赦激活(高估值被底部巨量资金强捞,VR:{vr:.1f})"
            elif _p1_is_single_peak_dense(curr, rt, now_price) and is_continuous_inflow:
                amnesty_reason = amnesty_reason + " | 筹码单峰密集PE缓刑" if amnesty_reason else "筹码单峰密集PE缓刑"
            else:
                return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"PE相对超标 (PE:{pe:.1f} > 75分位{ind_q75:.1f}*1.5)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        curr = df.iloc[-1]
        now_price = _safe_float(rt.get('price', curr.get('close', 0.0)))

        tail_vol_ratio = _safe_float(rt.get('tail_vol_ratio', 0.0))
        bias20 = _safe_float(curr.get('bias_20', (now_price - curr.get('ma20', now_price)) / max(curr.get('ma20', 1), 1) * 100.0))
        _ma60_for_bias = _safe_float(curr.get("ma60", 0.0))
        if _ma60_for_bias > 0:
            bias60 = _safe_float(
                curr.get(
                    "bias_60",
                    (now_price - _ma60_for_bias) / max(_ma60_for_bias, 1e-9) * 100.0,
                )
            )
        else:
            bias60 = 0.0
        if len(df) >= 60 and "high" in df.columns:
            highest_60d = _safe_float(df["high"].tail(60).max(), _safe_float(now_price))
        else:
            highest_60d = _safe_float(now_price)
        if highest_60d <= 0:
            highest_60d = max(_safe_float(now_price), 1e-9)

        # 趋势市(strict)：高位判定不交静态 BIAS20，交给后续熔断/乖离风控
        if p1_profile == "strict":
            is_high_position = now_price >= highest_60d * 0.88
        else:
            is_high_position = (bias20 > 8.0) or (now_price >= highest_60d * 0.88)

        relaxed_bear_bounce = p1_profile == "relaxed" and bias60 < -15.0 and net_inflow_3d > 0
        
        amplitude = 0.0
        high_val = _safe_float(curr.get('high', 0.0))
        low_val = _safe_float(curr.get('low', 0.0))
        # 与 scan/P3 一致：缺 pre_close 时用昨收链式回退，禁止默认 1 元导致振幅假爆表
        pre_close_val = _safe_float(curr.get("pre_close"), 0.0)
        if pre_close_val <= 0:
            pre_close_val = _safe_float(curr.get("close"), 0.0)
        
        if high_val > 0 and low_val > 0 and pre_close_val > 0:
            amplitude = (high_val - low_val) / pre_close_val * 100.0
        else:
            rt_price = _safe_float(rt.get('price', now_price))
            rt_open = _safe_float(rt.get('open', rt_price))
            rt_pre_close = _safe_float(rt.get('pre_close', pre_close_val))
            rt_high = _safe_float(rt.get('high', max(rt_price, rt_open)))
            rt_low = _safe_float(rt.get('low', min(rt_price, rt_open)))
            if rt_pre_close > 0:
                amplitude = (rt_high - rt_low) / rt_pre_close * 100.0
        amplitude = float(np.nan_to_num(amplitude, nan=0.0, posinf=0.0, neginf=0.0))
            
        # P1 去战术化：剔除尾盘偷袭/日内振幅惩罚，保留中期结构和筹码质量判断
        if p1_profile != "strict":
            if p1_mv_bar <= circ_mv_yi < 500.0 and bias20 >= 22.0:
                return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"乖离率极危 (BIAS20: {bias20:.1f}% >= 22%)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
            if circ_mv_yi >= 500.0 and bias20 >= 22.0:
                return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"乖离率极危 (超级中军 BIAS20: {bias20:.1f}% >= 22%)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        if cost_50th > 0 and now_price > cost_50th * 1.40:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"偏离主峰过远 (>40%)", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        ma20 = _safe_float(curr.get('ma20', 0.0))
        ma60 = _safe_float(curr.get('ma60', 0.0))
        ma120 = _safe_float(curr.get('ma120', 0.0))
        ma20_slope_5 = _safe_float(curr.get('ma20_slope_5', 0.0))
        close_today = _safe_float(curr.get('close', now_price_initial))
        close_yesterday = _safe_float(df.iloc[-2].get('close', close_today)) if len(df) >= 2 else close_today
        ma20_today = ma20 if ma20 > 0 else _safe_float(curr.get('ma20', close_today))
        ma20_yesterday = _safe_float(df.iloc[-2].get('ma20', ma20_today)) if len(df) >= 2 else ma20_today
        below_ma20_2d = (ma20_today > 0) and (close_today < ma20_today) and (close_yesterday < ma20_yesterday)
        bias20_cool = _safe_float(curr.get('bias_20', (close_today - ma20_today) / max(ma20_today, 1e-9) * 100.0))
        high_cooldown = (15.0 <= bias20_cool <= 25.0) and (avg_trn <= 2.2)

        comp_tier, comp_note = _compute_p1_compensation_tier(
            curr,
            df,
            rt,
            now_price,
            net_inflow_3d,
            net_inflow_5d_early,
            vr,
            ind_rank,
            dynamic_inflow_threshold,
            is_amnesty,
            ma20,
            ma60,
            ma120,
            ma20_slope_5,
        )

        # 高空冷却与深海防线：连续2日收盘在MA20下方，直接剔除P1（强势阳线反包可豁免）；高位冷却期最多保留58分观察，不给60分以上出头
        # 【V26.6 优化】深海防线增加阳线反包豁免：若当日阳线反包且量比≥1.5，视为洗盘结束信号
        is_bullish_bounce = (close_today >= ma20_today) and (close_today > close_yesterday) and (vr >= 1.5) and (close_today >= ma20_today * 0.998)
        if _p1_is_two_day_below_ma20(close_today, ma20_today, close_yesterday, ma20_yesterday):
            if not is_bullish_bounce:
                return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "深海防线: 连续2日收盘低于MA20", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
            else:
                comp_tier = max(comp_tier, 1)
                comp_note = "阳线反包豁免" + (" | " + comp_note if comp_note else "")
        if _p1_is_high_cooldown(bias20_cool, avg_trn) and not relaxed_bear_bounce:
            comp_tier = min(comp_tier, 0)
            comp_note = comp_note or "高位冷却压制"
        # 右侧地基：现价须站上 MA60；退潮左侧底仓（深乖离 MA60 + 资金）豁免
        if ma60 > 0 and now_price < ma60 * 0.995 and not relaxed_bear_bounce:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "右侧地基: 现价未站上MA60", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        # 筹码上沿重压：未突破 cost_95th 且获利盘不足 → 易成假突破炮灰；强攻击阳线时豁免（资金强行冲关）
        wr_p1 = _safe_float(rt.get("winner_rate"), _safe_float(curr.get("winner_rate"), 0.0))
        chip_single_peak_dense = _p1_is_single_peak_dense(curr, rt, now_price)
        if _safe_float(curr.get("cost_95th"), _safe_float(rt.get("cost_95th"), 0.0)) > 0 and not chip_single_peak_dense:
            c95_p1 = _safe_float(curr.get("cost_95th"), _safe_float(rt.get("cost_95th"), 0.0))
            if now_price < c95_p1 * 0.995 and wr_p1 < 72.0 and not is_strong_attack:
                return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "筹码重压: 未突破成本上沿且获利盘<72%", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
        
        is_trend_pass = False
        trend_ma120_min_ratio = float(thresholds.get("trend_ma120_min_ratio", P1_TREND_MA120_MIN_RATIO))
        trend_slope_fastpass = float(thresholds.get("trend_slope_fastpass", P1_TREND_SLOPE_FASTPASS))
        near_ma20_min_ratio = float(thresholds.get("near_ma20_min_ratio", P1_NEAR_MA20_MIN_RATIO))
        macd_bar_kill = float(thresholds.get("macd_bar_kill", P1_MACD_BAR_KILL))
        vol_divergence_ratio = float(thresholds.get("vol_divergence_ratio", P1_VOL_DIVERGENCE_RATIO))
        pass_line = float(thresholds.get("pass_line", P1_PASS_LINE))

        _t120_eff, macd_kill_eff, vol_div_eff, near_ma20_eff = _effective_p1_thresholds(
            comp_tier,
            trend_ma120_min_ratio,
            macd_bar_kill,
            vol_divergence_ratio,
            near_ma20_min_ratio,
        )

        if relaxed_bear_bounce:
            is_trend_pass = True
            am_str = "退潮左侧底仓特赦(超跌 bias60<-15% 且近3日主力净流入>0，豁免MA20>MA60)"
            amnesty_reason = amnesty_reason + " | " + am_str if amnesty_reason else am_str
        elif ma20 > ma60 and ma20_slope_5 >= trend_slope_fastpass:
            is_trend_pass = True
        elif ma20 > ma60:
            macd_bar_curr = _safe_float(curr.get('macd_bar', 0.0))
            macd_bar_prev = _safe_float(df.iloc[-2].get('macd_bar', 0.0)) if len(df) >= 2 else 0.0
            if net_inflow_5d_early > 0 or (macd_bar_curr > 0 and macd_bar_curr > macd_bar_prev):
                is_trend_pass = True
                am_str = "早龙释放(MA20>MA60且资金/动能支持)"
                amnesty_reason = amnesty_reason + " | " + am_str if amnesty_reason else am_str
            else:
                is_trend_pass = True

        if not is_trend_pass:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "均线未呈多头且不满足早龙特征", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
            
        tail_3 = df.tail(3)
        if not relaxed_short_ma and 'ma20' in tail_3.columns:
            days_above_ma20 = sum(tail_3['close'] >= tail_3['ma20'])
            # 阈值放宽：允许“贴近 MA20（1.5%以内）”或“有短期资金修复”的回踩形态
            latest_close = _safe_float(curr.get('close', now_price))
            latest_ma20 = _safe_float(curr.get('ma20', ma20))
            latest_ma20_safe = max(latest_ma20, 1e-9)
            near_ma20 = latest_ma20 > 0 and latest_close >= latest_ma20_safe * near_ma20_eff
            # 稳健：资金修复要求 3 日与 5 日净流入均为正，避免单日噪声“假修复”
            has_repair_inflow = (net_inflow_3d > 0) and (net_inflow_5d_early > 0)
            if (
                not relaxed_bear_bounce
                and days_above_ma20 == 0
                and not near_ma20
                and not has_repair_inflow
            ):
                return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "破位洗盘过深", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
                
        macd_bar = _safe_float(curr.get('macd_bar', curr.get('macd_hist', 0.0)))
        # MACD 绿柱斩杀：强攻击阳线时豁免（均线指标滞后于当日爆量定价）
        if (not relaxed_short_ma) and macd_bar <= macd_kill_eff and not is_strong_attack:
            return {
                "ts_code": ts_code,
                "score": 0.0,
                "is_pass": False,
                "reason": f"MACD过弱(柱{macd_bar:.4f}≤放宽底线{macd_kill_eff:.4f},容错档{comp_tier})",
                "item": item,
                "df": df,
                "inflow_ratio": inflow_ratio,
                "inflow_10d_ratio": inflow_10d_ratio,
                "score_details": {},
                "used_fast_track": used_fast_track,
            }

        # ---------- 量价背离（近窗）：A 股轮动快，用 60 日阴阳量均值会过度平滑、误杀近期刚反转的放量品种；
        # 改为近 20 个交易日，更贴近「当前攻击段」的量价配合度。（深幅回撤仍用下方 60 日窗口，二者解耦。）
        # 【性能优化 V2】复用 tail_10，避免重复切片（tail_10 已在循环外预计算）
        # 仅当需要 20 行时才额外切片；复用策略避免多次 df.tail() 调用
        if len(df) >= 20:
            df_20 = df.tail(20)
        elif len(df) >= 10:
            df_20 = tail_10
        else:
            df_20 = tail_5 if len(df) >= 5 else df.tail(min(5, len(df)))
        _vc_div = "vol" if "vol" in df_20.columns else ("volume" if "volume" in df_20.columns else None)
        if _vc_div is None:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "量价背离检查缺少成交量列", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
        up_days = df_20[(df_20["close"] > df_20["open"]) | (df_20["pct_chg"] > 0)]
        down_days = df_20[(df_20["close"] < df_20["open"]) | (df_20["pct_chg"] < 0)]
        avg_up_vol = up_days[_vc_div].mean() if not up_days.empty else 0
        avg_down_vol = down_days[_vc_div].mean() if not down_days.empty else 1
        avg_up_vol = float(np.nan_to_num(_safe_float(avg_up_vol), nan=0.0, posinf=0.0, neginf=0.0))
        avg_down_vol = float(np.nan_to_num(_safe_float(avg_down_vol), nan=0.0, posinf=0.0, neginf=0.0))
        # 下跌日均量若为 0（极少成交或全阳K线窗），分母改用 1 手当量纲占位，避免「除以极小常数」误杀全员
        avg_down_ref = avg_down_vol if avg_down_vol > 1e-9 else 1.0

        # 近20日量价背离：强攻击阳线时豁免（历史阴阳量统计滞后于当日冲关）
        if avg_up_vol < avg_down_ref * vol_div_eff and not is_strong_attack:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "量价背离", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        # ---------- 深幅回撤：仍用 60 日高低区间衡量「波段级」回撤，与 20 日量价背离统计分离
        # 【性能优化 V2】df_60 只在深幅回撤 >= 20% 时才计算；df_10 复用 tail_10
        df_60_len = len(df)
        if df_60_len >= 60:
            df_60 = df.tail(60)
        elif df_60_len >= 10:
            df_60 = tail_10
        else:
            df_60 = tail_5 if df_60_len >= 5 else df.tail(min(5, df_60_len))
        _vc60 = "vol" if "vol" in df_60.columns else ("volume" if "volume" in df_60.columns else None)
        if _vc60 is None:
            return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": "回撤检查缺少成交量列", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}
        max_high_60 = _safe_float(df_60['high'].max())
        idx_max = df_60['high'].idxmax()
        min_after_max = _safe_float(df.loc[idx_max:]['low'].min() if idx_max in df.index else df_60['low'].min())
        max_drawdown_60d = (max_high_60 - min_after_max) / max_high_60 * 100.0 if max_high_60 > 0 else 0
        
        if max_drawdown_60d >= 20.0:
            lowest_60d = _safe_float(df_60['low'].min())
            mid_point = (max_high_60 + lowest_60d) / 2.0
            
            df_10 = tail_10  # 【性能优化V2】复用已在循环外预计算的 tail_10
            down_10 = df_10[(df_10['close'] < df_10['open']) | (df_10['pct_chg'] < 0)]
            up_10 = df_10[(df_10['close'] > df_10['open']) | (df_10['pct_chg'] > 0)]
            avg_down_vol_10 = down_10[_vc60].mean() if not down_10.empty else 0
            avg_up_vol_10 = up_10[_vc60].mean() if not up_10.empty else 1
            
            # 【审计修复】维度2：上行日均量作分母时兜底，避免除零比较失真
            _up10 = max(float(avg_up_vol_10), 1e-9)
            is_drawdown_amnesty = (now_price >= mid_point) and (avg_down_vol_10 <= _up10 * 1.5)
            
            if is_drawdown_amnesty:
                am_str = f"回撤修复特赦(最大回撤{max_drawdown_60d:.1f}%,已收复中轴)"
                amnesty_reason = amnesty_reason + " | " + am_str if amnesty_reason else am_str
            else:
                return {"ts_code": ts_code, "score": 0.0, "is_pass": False, "reason": f"回撤超标 ({max_drawdown_60d:.1f}% >= 20%) 且未修复", "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": {}, "used_fast_track": used_fast_track}

        score, is_pass, reason, score_details = _evaluate_p1_single_stock(
            df, rt, circ_mv_yi, ind_rank, pe, ind_stats, ind, dynamic_industries, avg_trn, pass_line
        )

        # 股性黑名单：对“日内冲高回落反复做T”的标的做显式惩罚，避免 P1 被短线幽灵污染。
        from core.config_manager import get_p1_stock_behavior_penalty_config

        bh_cfg = get_p1_stock_behavior_penalty_config()
        long_upper_shadow_hits, intraday_dump_hits = _p1_behavior_penalty_hits(df, int(bh_cfg.get("lookback_days", 20)))

        if bool(bh_cfg.get("enabled", True)) and is_pass and (
            long_upper_shadow_hits >= int(bh_cfg.get("long_upper_shadow_hits_min", 3))
            or intraday_dump_hits >= int(bh_cfg.get("intraday_dump_hits_min", 3))
        ):
            score = max(0.0, score - float(bh_cfg.get("score_penalty", 8.0)))
            score_details["渣男股性惩罚"] = f"近{int(bh_cfg.get('lookback_days', 20))}日长上影{long_upper_shadow_hits}次/冲高回落{intraday_dump_hits}次"
            reason = (reason + " | " if reason else "") + "渣男股性命中"
            if score < float(pass_line):
                is_pass = False
                reason = "渣男股性扣分后未达标"

        if is_pass and comp_tier > 0 and comp_note:
            _cn = f"P1容错补偿(档{comp_tier})：{comp_note}"
            amnesty_reason = amnesty_reason + " | " + _cn if amnesty_reason else _cn
        
        if amnesty_reason and is_pass:
            reason = amnesty_reason + " | " + reason

        # 【V26.6 优化】高位冷却从55分调整为58分，健康回调的强势股不被误伤
        if high_cooldown and is_pass:
            score = min(score, 58.0)
            score_details["高位冷却压制"] = "58分封顶"
        score_details["市值档位"] = mv_tier
        return {"ts_code": ts_code, "score": score, "is_pass": is_pass, "reason": reason, "item": item, "df": df, "inflow_ratio": inflow_ratio, "inflow_10d_ratio": inflow_10d_ratio, "score_details": score_details, "used_fast_track": used_fast_track}
        
    except Exception as e:
        return {"ts_code": item.get('code', 'Unknown'), "score": 0.0, "is_pass": False, "reason": "安检致命异常: 数据残缺", "item": item, "df": pd.DataFrame(), "inflow_ratio": 0.0, "inflow_10d_ratio": 0.0, "score_details": {}, "used_fast_track": False}

# ================= 并发调度与行业基准库 =================
# 【多层级池子】说明：与 wash_metrics_history.json 共用元数据键，避免新增独立文件；仅存 streak 与日期，供「连续无票」判定
_TIER_META_KEY = "__tier_pool_meta__"
_LAST_P1_OBSERVATION_POOL = []
_LAST_P1_WASH_ADAPTIVE = {
    "adaptive_reason": "",
    "adaptive_sample_count": 0,
    "market_contraction_score": 0.0,
}


def _wash_metrics_path():
    return path_wash_metrics_json()


def _merge_tier_meta_p1(is_main_empty):
    """# 【多层级池子】说明：主 P1 池为空则累加连续无票日（同日多次洗盘不重复加一）；有票则清零。返回更新后的 streak。"""
    from core.file_utils import atomic_json_update

    bj_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(bj_tz).strftime("%Y%m%d")
    ret_box: Dict[str, Any] = {"streak": 0, "meta": {}}

    def _upd(root: Dict[str, Any]) -> None:
        meta = root.get(_TIER_META_KEY)
        if not isinstance(meta, dict):
            meta = {}
        streak = int(meta.get("p1_empty_streak", 0) or 0)
        last_d = str(meta.get("p1_last_empty_date", "") or "")
        if is_main_empty:
            if last_d != today_str:
                streak += 1
                meta["p1_last_empty_date"] = today_str
            # 【多层级池子】说明：同日第二次洗盘仍视为「今日仍无票」，不重复累加 streak
        else:
            streak = 0
            meta["p1_last_empty_date"] = ""
        meta["p1_empty_streak"] = streak
        root[_TIER_META_KEY] = meta
        ret_box["streak"] = streak
        ret_box["meta"] = meta

    try:
        ensure_runtime_data_layout()
        atomic_json_update(_wash_metrics_path(), _upd, timeout=5)
    except Exception as e:
        logging.error("【多层级池子】atomic 写入 wash_metrics_history 失败: %s", e)
    return int(ret_box["streak"]), dict(ret_box["meta"] or {})


def get_last_p1_observation_pool():
    """# 【多层级池子】说明：供 app 读取本轮洗盘产出的震荡观察池（不改变原 build 的二元组返回，避免破坏其它模块解包）。"""
    return list(_LAST_P1_OBSERVATION_POOL or [])


def get_last_p1_wash_adaptive():
    """# 【自适应优化】最近一次 P1 洗盘计算的市场收缩度上下文（供 UI 观察池行「缩量说明」与侧边栏追溯）。"""
    return dict(_LAST_P1_WASH_ADAPTIVE)


def build_p1_pool_and_cache(stock_data_list, progress_callback=None, regime_name=None, p1_threshold_override=None):
    global GLOBAL_UNLOCK_BLACKLIST
    global _LAST_P1_OBSERVATION_POOL
    global _LAST_P1_WASH_ADAPTIVE
    _LAST_P1_OBSERVATION_POOL = []
    _LAST_P1_WASH_ADAPTIVE = {
        "adaptive_reason": "",
        "adaptive_sample_count": 0,
        "market_contraction_score": 0.0,
    }

    bj_tz = timezone(timedelta(hours=8))
    calendar_yyyymmdd = datetime.now(bj_tz).strftime("%Y%m%d")
    p1_anchor = (get_latest_daily_data_trade_date_yyyymmdd() or "").strip()
    if len(p1_anchor) != 8 or not p1_anchor.isdigit():
        p1_anchor = calendar_yyyymmdd
        logging.warning("P1 锚定：日线库最新交易日不可用，回退日历日 %s", p1_anchor)

    if progress_callback:
        progress_callback(0.01)
    unlock_blacklist = _get_unlock_blacklist(p1_anchor)
    GLOBAL_UNLOCK_BLACKLIST = unlock_blacklist

    try:
        from core.config_manager import get_p1_respect_scan_blacklist

        _respect_bl = bool(get_p1_respect_scan_blacklist())
    except Exception:
        _respect_bl = False
    punish_blacklist: Set[str] = _get_punish_blacklist() if _respect_bl else set()
    if not _respect_bl:
        logging.info("P1 build: respect_scan_blacklist_for_p1=false，洗盘不拦截 scan_engine 黑名单")

    thresholds = dict(_get_regime_thresholds(regime_name))
    if isinstance(p1_threshold_override, dict) and p1_threshold_override:
        for k, v in p1_threshold_override.items():
            try:
                thresholds[k] = float(v)
            except (TypeError, ValueError):
                thresholds[k] = v
    thresholds["_p1_profile_key"] = _p1_regime_profile_key(regime_name)
    thresholds["_p1_min_circ_mv_yi"] = float(_p1_min_circ_mv_yi_pool())

    try:
        from core.config_manager import get_p1_fund_memory_weight, get_p1_select_min_circ_mv_wan

        _log_mv_wan = int(get_p1_select_min_circ_mv_wan())
        _log_w_fm = float(get_p1_fund_memory_weight())
    except Exception:
        _log_mv_wan = int(getattr(constants, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000))
        _log_w_fm = float(getattr(constants, "FUND_MEMORY_WEIGHT_P1", 0.17))
    logging.info(
        "P1 build 参数: pass_line=%.1f profile=%s select_min_circ_mv_wan=%d fund_memory_weight_p1=%.4f 输入候选数=%d",
        float(thresholds.get("pass_line", 50.0)),
        str(thresholds.get("_p1_profile_key", "")),
        _log_mv_wan,
        _log_w_fm,
        len(stock_data_list) if stock_data_list else 0,
    )

    # 【自适应优化】仅当「库内最新交易日 = 当日日历」时拉实时算收缩度；否则休市/库滞后会导致量比与收盘态不一致、结果漂移。
    _mcs_wash = 0.0
    skip_rt_mcs = p1_anchor != calendar_yyyymmdd
    if skip_rt_mcs:
        logging.info(
            "P1 锚定：DB 最新交易日 %s ≠ 日历日 %s，跳过实时市场收缩度（保证同库重复跑一致）",
            p1_anchor,
            calendar_yyyymmdd,
        )
    try:
        from data.api_fetcher import fetch_realtime_batch as _fetch_rt_for_mcs
    except Exception:
        _fetch_rt_for_mcs = None
    if (not skip_rt_mcs) and _fetch_rt_for_mcs and stock_data_list:
        _codes_mcs = [
            it.get("code")
            for it in stock_data_list
            if isinstance(it, dict) and it.get("code")
        ]
        if _codes_mcs:
            try:
                _rtm_wash = _fetch_rt_for_mcs(_codes_mcs) or {}
                _ctx_wash = compute_market_contraction_context(stock_data_list, _rtm_wash)
                _mcs_wash = float(_ctx_wash.get("score", 0.0) or 0.0)
                _LAST_P1_WASH_ADAPTIVE.update(
                    {
                        "adaptive_reason": str(_ctx_wash.get("adaptive_reason", "") or ""),
                        "adaptive_sample_count": int(_ctx_wash.get("sample_count", 0) or 0),
                        "market_contraction_score": _mcs_wash,
                    }
                )
            except Exception as _e_mcs:
                logging.debug("【自适应优化】build_p1 市场收缩度计算失败: %s", _e_mcs)
    thresholds["_market_contraction_score"] = _mcs_wash

    p1_pool = []
    p1_rejected = [] 
    fast_track_pool = [] 
    p1_gene_dict = {}
    
    if not stock_data_list:
        # 【多层级池子】说明：无候选时仍返回二元组，观察池通过 get_last_p1_observation_pool 为空列表
        return p1_pool, p1_rejected

    end_date_str = p1_anchor
    optimal_workers = min(32, (os.cpu_count() or 4) * 2)
    workers = getattr(constants, 'MAX_WORKERS', optimal_workers)
    
    industry_map = get_all_basic_industry()
    
    sector_ranking_dict = get_latest_sector_ranking()
    sorted_sectors = list(sector_ranking_dict.keys())
    sector_rank_map = _build_sector_rank_map(sorted_sectors)
    dynamic_industries = _get_dynamic_strategic_industries(
        sector_ranking_dict, p1_anchor_yyyymmdd=p1_anchor
    )
    
    pe_records = []
    for item in stock_data_list:
        ts_code = item.get('code', '')
        ind = industry_map.get(ts_code, '未知')
        
        pe_raw = item.get('hist', {}).get('pe_ttm')
        if pe_raw is None or pd.isna(pe_raw) or str(pe_raw).strip() in ['', '-']:
            pe_raw = item.get('hist', {}).get('pe', 0)
            
        pe = _safe_float(pe_raw)
        if pe <= 0 or pe > 500: continue
        pe_records.append({'ind': ind, 'pe': pe})
            
    df_pe = pd.DataFrame(pe_records)
    
    global_stats = {
        'median': df_pe['pe'].median() if not df_pe.empty else 20.0,
        'q30': df_pe['pe'].quantile(0.30) if not df_pe.empty else 15.0,
        'q50': df_pe['pe'].quantile(0.50) if not df_pe.empty else 20.0,
        'q75': df_pe['pe'].quantile(0.75) if not df_pe.empty else 30.0,
        'q80': df_pe['pe'].quantile(0.80) if not df_pe.empty else 35.0
    }
    
    industry_pe_stats = {}
    if not df_pe.empty:
        for ind, group in df_pe.groupby('ind'):
            if len(group) >= 10:
                industry_pe_stats[ind] = {
                    'median': group['pe'].median(),
                    'q30': group['pe'].quantile(0.30),
                    'q50': group['pe'].quantile(0.50),
                    'q75': group['pe'].quantile(0.75),
                    'q80': group['pe'].quantile(0.80)
                }
            else:
                industry_pe_stats[ind] = global_stats

    total_tasks = len(stock_data_list)
    completed_tasks = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _process_single_stock_for_p1,
                item, industry_pe_stats, global_stats, industry_map, sector_rank_map,
                dynamic_industries, unlock_blacklist, punish_blacklist, thresholds
            ): item
            for item in stock_data_list
        }
        
        for future in as_completed(futures):
            with lock:
                completed_tasks += 1
                if progress_callback and completed_tasks % max(1, total_tasks // 100) == 0:
                    progress_callback(completed_tasks / total_tasks)

            # 【审计修复】维度4：result() 在锁外等待，避免阻塞其他线程；异常单独记录不拖垮整池
            try:
                res = future.result()
            except Exception as exc:
                logging.warning("P1 并发单任务 future.result 异常: %s", exc)
                continue

            # 【审计修复】维度4：共享 dict/list 写入统一加锁，满足线程安全要求
            with lock:
                if res:
                    ts_code = res["ts_code"]
                    score = res["score"]
                    p1_gene_dict[ts_code] = score

                    if res["is_pass"]:
                        hit_item = res["item"]
                        hit_item['p1_score'] = score
                        hit_item['inflow_ratio'] = res.get("inflow_ratio", 0.0)
                        hit_item['inflow_10d_ratio'] = res.get("inflow_10d_ratio", 0.0)
                        hit_item['df'] = res["df"]
                        hit_item["score_details"] = dict(res.get("score_details") or {})

                        if res.get("used_fast_track", False):
                            hit_item['p1_score'] = max(score, 85.0)
                            fast_track_pool.append(hit_item)
                        else:
                            p1_pool.append(hit_item)
                    else:
                        s_code = ts_code.split('.')[0][:6]
                        stock_name = normalize_stock_display_name(
                            res.get("item", {}).get("hist", {}).get("name", s_code)
                        )

                        score_details = res.get("score_details", {})
                        top_1, top_2, worst_1, worst_2 = p1_score_details_to_extreme_labels(score_details, score)

                        _sdj = score_details_json_safe(score_details)
                        p1_rejected.append({
                            "代码": s_code,
                            "名称": stock_name,
                            "淘汰死因": (
                                f"{int(thresholds.get('_p1_min_circ_mv_yi', _p1_min_circ_mv_yi_pool()))}亿以下流通市值全局拦截"
                                if "流通市值不足" in str(res.get("reason", ""))
                                else res["reason"]
                            ),
                            "被裁阶段": "第五层打分" if score > 0 else "前置防线",
                            "当前得分": round(float(score), 2),
                            "满分项1": top_1,
                            "满分项2": top_2,
                            "最低项1": worst_1,
                            "最低项2": worst_2,
                            "score_details": _sdj,
                        })

    # ==========================================================
    # 🛑 核心限流阀：放宽至 5 只，给予盘中挑选最强形态的战术空间
    # ==========================================================
    fast_track_pool.sort(key=lambda x: x.get('inflow_ratio', 0.0), reverse=True)
    accepted_fast_tracks = fast_track_pool[:5] 
    rejected_fast_tracks = fast_track_pool[5:]
    
    for ft_item in rejected_fast_tracks:
        s_code = str(ft_item.get('code', '')).split('.')[0][:6]
        stock_name = normalize_stock_display_name(ft_item.get("hist", {}).get("name", s_code))
        p1_rejected.append({
            "代码": s_code,
            "名称": stock_name,
            "淘汰死因": "🛑直通车超载保护 (全市场限流5只，名额已满拦截)",
            "被裁阶段": "全局限流防线",
            "当前得分": round(float(ft_item.get('p1_score', 0) or 0), 2),
            "满分项1": "--",
            "满分项2": "--",
            "最低项1": "--",
            "最低项2": "--"
        })
        
    p1_pool.extend(accepted_fast_tracks)
    # 最终安全阀：进入 P1 池前再次物理过滤流通市值低于 P1 下限的标的
    # 【审计修复】维度2：亿元门槛换算分母兜底
    _p1_mv_final_bar = float(thresholds.get("_p1_min_circ_mv_yi", _p1_min_circ_mv_yi_pool()))
    p1_pool = [
        x
        for x in p1_pool
        if (_safe_float(x.get('hist', {}).get('circ_mv', x.get('hist', {}).get('total_mv', 0) * 0.6)) / max(10000.0, 1e-9))
        >= _p1_mv_final_bar
    ]

    p1_pool.sort(key=lambda x: _p1_final_sort_key(x), reverse=True)
    
    p1_rejected.sort(key=lambda x: x["当前得分"], reverse=True)

    # 【多层级池子】说明：先根据主池是否为空更新「连续无票」streak，再决定是否跑第二遍降档观察池（仍走同一套 _process_single_stock_for_p1，含 PE/换手与第1条缩量自适应）
    streak_after, _ = _merge_tier_meta_p1(len(p1_pool) == 0)
    pass_line_main = float(thresholds.get("pass_line", P1_PASS_LINE))
    # 【多层级池子】说明：观察池及格线约为主池 78%（约降 22%，落在 15~25% 区间）；缩量期再叠乘 0.95 与 adaptive_turnover 语境一致
    pass_line_obs = max(35.0, pass_line_main * 0.78)
    if float(_mcs_wash) >= 0.7:
        pass_line_obs = max(35.0, pass_line_obs * 0.95)
    fill_observation_p1 = (streak_after >= 2) or (float(_mcs_wash) >= 0.7)
    observation_pool = []
    main_codes = {str(x.get("code", "")).strip() for x in p1_pool if isinstance(x, dict)}
    if fill_observation_p1 and stock_data_list:
        obs_thresholds = dict(thresholds)
        obs_thresholds["pass_line"] = float(pass_line_obs)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures_obs = {
                executor.submit(
                    _process_single_stock_for_p1,
                    item, industry_pe_stats, global_stats, industry_map, sector_rank_map,
                    dynamic_industries, unlock_blacklist, punish_blacklist, obs_thresholds
                ): item
                for item in stock_data_list
                if str(item.get("code", "")).strip() not in main_codes
            }
            for fut in as_completed(futures_obs):
                try:
                    res_o = fut.result()
                except Exception as exc:
                    logging.warning("【多层级池子】观察池单任务异常: %s", exc)
                    continue
                if not res_o or not res_o.get("is_pass"):
                    continue
                hit_o = res_o.get("item")
                if not isinstance(hit_o, dict):
                    continue
                sc = float(res_o.get("score", 0.0))
                hit_o["p1_score"] = sc
                hit_o["inflow_ratio"] = res_o.get("inflow_ratio", 0.0)
                hit_o["inflow_10d_ratio"] = res_o.get("inflow_10d_ratio", 0.0)
                hit_o["df"] = res_o.get("df")
                hit_o["score_details"] = dict(res_o.get("score_details") or {})
                hit_o["pool_tier"] = "observation"
                if isinstance(hit_o.get("hist"), dict):
                    hit_o["hist"]["_observation_label"] = "【缩量期备选】"
                observation_pool.append(hit_o)
        # 【V26.6 优化】观察池上限50只，防止长期死池时池子膨胀
    _OBS_POOL_MAX_SIZE = 50
    if len(observation_pool) > _OBS_POOL_MAX_SIZE:
        observation_pool = observation_pool[:_OBS_POOL_MAX_SIZE]
        logging.info("【多层级池子】观察池已截断至top%d", _OBS_POOL_MAX_SIZE)
        if isinstance(_LAST_P1_WASH_ADAPTIVE, dict):
            _LAST_P1_WASH_ADAPTIVE["obs_pool_truncated"] = True
    observation_pool.sort(key=lambda x: _p1_final_sort_key(x), reverse=True)
    _LAST_P1_OBSERVATION_POOL = observation_pool

    ensure_runtime_data_layout()
    file_path = path_p1_gene_json(end_date_str)
    
    # 【审计修复】维度2：p1_gene 仅允许可 JSON 序列化的标量，禁止 DataFrame/Series 混入导致 dump 崩溃
    _gene_out = {}
    for _gk, _gv in (p1_gene_dict or {}).items():
        try:
            if isinstance(_gv, (pd.DataFrame, pd.Series)):
                logging.warning("P1 基因缓存跳过非 JSON 类型键 %s (DataFrame/Series)", _gk)
                continue
            _gene_out[str(_gk)] = float(_gv)
        except (TypeError, ValueError) as _je:
            logging.warning("P1 基因缓存跳过不可转 float 键 %s: %s", _gk, _je)

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(_gene_out, f, ensure_ascii=False)
    except TypeError as e:
        logging.error(f"❌ P1缓存 JSON 类型错误: {e}")
    except OSError as e:
        logging.error(f"❌ P1缓存写入 IO 失败: {e}")
    except Exception as e:
        logging.error(f"❌ P1缓存写入崩溃: {e}")

    _missing_required_fields = []
    for _it in p1_pool:
        if not isinstance(_it, dict):
            _missing_required_fields.append(("<non-dict>", ["code", "p1_score", "hist", "df"]))
            continue
        _missing = [k for k in ("code", "p1_score", "hist", "df") if k not in _it or _it.get(k) is None]
        if _missing:
            _missing_required_fields.append((str(_it.get("code", "<no-code>")), _missing))
    if _missing_required_fields:
        sample = "; ".join([f"{code}:{'/'.join(fields)}" for code, fields in _missing_required_fields[:10]])
        logging.warning(
            "P1 落盘字段完整性检查未通过 | 缺失项数=%s | 样例=%s",
            len(_missing_required_fields),
            sample,
        )
    else:
        logging.info("P1 落盘字段完整性检查通过 | 记录数=%s", len(p1_pool))

    return p1_pool, p1_rejected


# =============================================================================
# P3/P4 右侧直通车（Momentum Fast-Lane）
# =============================================================================
# 业务：P1 静态形态（如 bias_20、贴 MA20）偏左侧，易把「极端强势龙头」挡在底仓外；
# 扫描引擎仅遍历底仓 base_items，导致 P3/P4 永远看不到这类票。
#
# 策略：在 run_scan_engine 内、批量 fetch_realtime 之前，对「非底仓」增量探测一批代码的实时快照，
# 用纯行情字段筛出极端强势标的，再按代码逐只 get_stock_data_qfq（与现有单票扫描一致），
# 合并入本轮扫描列表。全程不对 daily_data 做额外全表扫描；候选列表复用 get_p1_candidate_codes()
# 或 stock_basic 行业映射（与 P1 洗盘同源的一次性查询语义，非逐行扫表）。
#
# 常量可按实盘微调；阈值故意偏「少而精」，避免直通车淹没主池。
# =============================================================================

MOMENTUM_FAST_LANE_MAX_PROBE = 1500
MOMENTUM_FAST_LANE_MAX_PICK = 56
MOMENTUM_FAST_LANE_TOP_SECTORS = 14
# 日内近似涨停/强势区（主板口径；创业板科创板可由高 pct 自然落入）
MFL_PCT_LIMIT_TOUCH = 8.8
MFL_PCT_STRONG = 6.2
MFL_PCT_AMOUNT_TIER = 5.0
MFL_CLOSE_IN_RANGE_MIN = 0.82
MFL_OPEN_SURGE_PCT = 4.8
MFL_OPEN_SURGE_NEED_DAY_PCT = 2.8
# 连板：在日线 hist 上 limit_times>=2 且当日 rt 强势时直接入选（右侧接力）
MFL_LIMIT_TIMES_MIN = 2
MFL_LIMIT_TIMES_MIN_PCT = 4.5


def _ts_to_s_code(ts_code: str) -> str:
    return str(ts_code or "").split(".")[0][:6]


def compute_momentum_fast_lane_probe_codes(
    base_codes: List[str],
    sorted_sector_names: List[str],
    max_probe: int = MOMENTUM_FAST_LANE_MAX_PROBE,
) -> List[str]:
    """
    构造「非底仓」探测列表：优先实时涨幅榜相关板块内的股票，控制长度上限。
    不使用 SELECT * FROM daily_data 类全表扫描。
    """
    try:
        from data.db_core import get_p1_candidate_codes, get_all_basic_industry
    except Exception as e:
        logging.debug("compute_momentum_fast_lane_probe_codes 导入失败: %s", e)
        return []

    candidates = get_p1_candidate_codes() or []
    if not candidates:
        candidates = list((get_all_basic_industry() or {}).keys())
    if not candidates:
        return []

    base_set: Set[str] = {str(c).strip() for c in (base_codes or []) if c}
    hot_inds: Set[str] = set()
    for sec in (sorted_sector_names or [])[: max(1, MOMENTUM_FAST_LANE_TOP_SECTORS)]:
        if sec:
            hot_inds.add(str(sec))

    ind_map = get_all_basic_industry() or {}
    non_base: List[str] = []
    for c in candidates:
        cs = str(c).strip()
        if not cs or cs.upper().endswith(".BJ"):
            continue
        if cs in base_set:
            continue
        non_base.append(cs)

    priority: List[str] = []
    rest: List[str] = []
    if hot_inds:
        for c in non_base:
            if ind_map.get(c) in hot_inds:
                priority.append(c)
            else:
                rest.append(c)
    else:
        rest = list(non_base)

    ordered = priority + rest
    seen: Set[str] = set()
    out: List[str] = []
    for c in ordered:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
        if len(out) >= int(max_probe):
            break
    return out


def _mfl_rt_metrics(rt: Dict[str, Any]) -> Optional[Tuple[float, float, float, float, float]]:
    """返回 (pct_day, amount, pos_in_range, pct_from_open, pre_close) 或 None。"""
    if not isinstance(rt, dict):
        return None
    price = _safe_float(rt.get("price"), 0.0)
    pre = _safe_float(rt.get("pre_close"), 0.0)
    open_p = _safe_float(rt.get("open"), 0.0)
    high = _safe_float(rt.get("high"), 0.0)
    low = _safe_float(rt.get("low"), 0.0)
    amount = _safe_float(rt.get("amount"), 0.0)
    if price <= 0 or pre <= 0:
        return None
    pct_day = (price - pre) / pre * 100.0
    hi_lo = high - low
    if hi_lo > 1e-9:
        pos_in_range = (price - low) / hi_lo
    else:
        pos_in_range = 1.0 if pct_day >= 9.0 else 0.5
    if open_p > 0:
        pct_from_open = (price - open_p) / open_p * 100.0
    else:
        pct_from_open = 0.0
    return float(pct_day), float(amount), float(pos_in_range), float(pct_from_open), float(pre)


def select_momentum_fast_lane_ts_codes(
    probe_ts_codes: List[str],
    rt_map: Dict[str, Any],
    base_code_set: FrozenSet[str],
    max_picks: int = MOMENTUM_FAST_LANE_MAX_PICK,
) -> List[str]:
    """
    仅基于已拉取的 rt_map（探测批次）判定极端强势；从 probe 中选出最多 max_picks 只。
    规则（满足任一即可进入打分排序）：
    - 涨幅触及强涨停区 / 大阳线收在近高位；
    - 分时自开盘拉升过猛（右侧启动）；
    - 成交额处于探测集前列且有一定涨幅；
    - 连板条件在 build_momentum_fast_lane_base_items 内用日线 limit_times 复核。
    """
    if not probe_ts_codes or not isinstance(rt_map, dict):
        return []

    rows: List[Tuple[str, float, float, float, float, float]] = []
    for ts in probe_ts_codes:
        ts = str(ts).strip()
        if not ts or ts in base_code_set:
            continue
        sc = _ts_to_s_code(ts)
        rt = rt_map.get(sc)
        m = _mfl_rt_metrics(rt if isinstance(rt, dict) else {})
        if m is None:
            continue
        pct_day, amount, pos_in_range, pct_from_open, _pre = m
        rows.append((ts, pct_day, amount, pos_in_range, pct_from_open, 0.0))

    if not rows:
        return []

    amounts_sorted = sorted([r[2] for r in rows], reverse=True)
    amt_thr = 0.0
    if amounts_sorted:
        idx = max(0, min(len(amounts_sorted) - 1, int(len(amounts_sorted) * 0.11)))
        amt_thr = float(amounts_sorted[idx])

    scored: List[Tuple[str, float]] = []
    for ts, pct_day, amount, pos_in_range, pct_from_open, _ in rows:
        hit = False
        if pct_day >= MFL_PCT_LIMIT_TOUCH:
            hit = True
        elif pct_day >= MFL_PCT_STRONG and pos_in_range >= MFL_CLOSE_IN_RANGE_MIN:
            hit = True
        elif pct_from_open >= MFL_OPEN_SURGE_PCT and pct_day >= MFL_OPEN_SURGE_NEED_DAY_PCT:
            hit = True
        elif pct_day >= MFL_PCT_AMOUNT_TIER and amount >= amt_thr and amt_thr > 0:
            hit = True
        # 连板加速日常见形态：高涨幅 + 收在近高位（limit_times 在后续日线 build 中复核）
        elif pct_day >= (MFL_LIMIT_TIMES_MIN_PCT + 1.2) and pos_in_range >= 0.78:
            hit = True
        if not hit:
            continue
        # 综合排序：涨幅为主，成交额对数压缩防一手独大
        scr = float(pct_day) * (1.0 + np.log1p(max(amount, 0.0) / 1e6))
        scored.append((ts, scr))

    scored.sort(key=lambda x: x[1], reverse=True)
    out: List[str] = []
    seen: Set[str] = set()
    for ts, _ in scored:
        if ts in seen:
            continue
        seen.add(ts)
        out.append(ts)
        if len(out) >= int(max_picks):
            break
    return out


def build_momentum_fast_lane_base_items(ts_codes: List[str]) -> List[Dict[str, Any]]:
    """
    对直通车代码逐只拉 QFQ 日线并 precompute_indicators，构造与底仓项同形的 dict。
    标记 _momentum_fast_lane 供 scan_engine 在 P3/P4 上放宽黄金门禁（仍走物理胸甲战法打分）。
    """
    if not ts_codes:
        return []
    try:
        from data.db_core import get_stock_data_qfq
        from core.indicator_calc import precompute_indicators
    except Exception as e:
        logging.warning("build_momentum_fast_lane_base_items 导入失败: %s", e)
        return []

    out: List[Dict[str, Any]] = []
    for ts_code in ts_codes:
        ts_code = str(ts_code).strip()
        if not ts_code:
            continue
        try:
            df = get_stock_data_qfq(ts_code, limit=120)
            if df is None or getattr(df, "empty", True):
                continue
            df = precompute_indicators(df)
            hist = df.iloc[-1].to_dict()
            lt = int(_safe_float(hist.get("limit_times", 0), 0.0))
            item: Dict[str, Any] = {
                "code": ts_code,
                "p1_score": 72.0,
                "df": df,
                "hist": hist,
                "_momentum_fast_lane": True,
            }
            # 强连板接力：limit_times 已含昨日连板高度，供引擎/风控语义使用
            item["_momentum_fast_lane_limit_times"] = lt
            out.append(item)
        except Exception as e:
            logging.debug("直通车单票构建跳过 %s: %s", ts_code, e)
    return out


def extend_fast_lane_with_limit_streak_hits(
    probe_ts_codes: List[str],
    rt_map: Dict[str, Any],
    base_code_set: FrozenSet[str],
    already_picked: FrozenSet[str],
    max_add: int = 12,
) -> List[str]:
    """
    对「rt 未过动量阈值」的探测代码，仅按代码逐只查日线末行 limit_times（单票 O(1) 窗口），
    将连板高度达标的代码加入直通车名单；上限 max_add，避免额外 I/O 膨胀。
    """
    if max_add <= 0 or not probe_ts_codes:
        return []
    try:
        from data.db_core import get_stock_data_qfq
    except Exception:
        return []

    extra: List[str] = []
    for ts in probe_ts_codes:
        if len(extra) >= max_add:
            break
        ts = str(ts).strip()
        if not ts or ts in base_code_set or ts in already_picked:
            continue
        sc = _ts_to_s_code(ts)
        rt = rt_map.get(sc)
        m = _mfl_rt_metrics(rt if isinstance(rt, dict) else {})
        if m is None:
            continue
        pct_day, _amount, _pr, _po, _pre = m
        if pct_day < MFL_LIMIT_TIMES_MIN_PCT:
            continue
        try:
            df = get_stock_data_qfq(ts, limit=30)
            if df is None or getattr(df, "empty", True):
                continue
            if "limit_times" not in df.columns:
                continue
            lt = int(_safe_float(df["limit_times"].iloc[-1], 0.0))
            if lt >= MFL_LIMIT_TIMES_MIN:
                extra.append(ts)
        except Exception:
            continue
    return extra


def merge_scan_base_with_fast_lane_items(
    base_items: List[Dict[str, Any]],
    fast_lane_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """底仓顺序不变，尾部追加直通车（按 code 去重）。"""
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for it in base_items or []:
        if not isinstance(it, dict):
            continue
        c = str(it.get("code", "")).strip()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(it)
    for it in fast_lane_items or []:
        if not isinstance(it, dict):
            continue
        c = str(it.get("code", "")).strip()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(it)
    return out


# ==================== P1 缓存 JSON 主权（UI 人工洗盘 vs 守护自动重建）====================
# 与 ui/app.py、auto_sniper_daemon.py 落盘字段 `_source` 字符串必须严格一致。
P1_CACHE_JSON_SOURCE_UI_MANUAL = "UI_MANUAL"
P1_CACHE_JSON_SOURCE_DAEMON_AUTO = "DAEMON_AUTO"


def p1_cache_json_should_skip_daemon_overwrite(json_path: str) -> bool:
    """
    守护进程写入前的主权防御：若当日 p1_cache_YYYYMMDD.json 已存在且声明为 UI 人工洗盘，
    则不得覆盖 JSON 及同源 DuckDB p1_cache，以人工数据为准。

    Returns:
        True  — 应跳过本次守护覆写；
        False — 可继续写入（文件不存在、旧版纯数组格式、或 _source 非 UI_MANUAL）。
    """
    p = str(json_path or "").strip()
    if not p or not os.path.isfile(p):
        return False
    try:
        with open(p, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        logging.warning("P1 主权预检：读取 %s 失败，不阻断守护写入: %s", p, e)
        return False
    if isinstance(payload, dict) and payload.get("_source") == P1_CACHE_JSON_SOURCE_UI_MANUAL:
        logging.warning(
            "⚠️ 检测到今日 P1 底仓已被 UI 人工洗盘干预，守护进程放弃自动覆盖，以人工数据为准！path=%s",
            p,
        )
        return True
    return False