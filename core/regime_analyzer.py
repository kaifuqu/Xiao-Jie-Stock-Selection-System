# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 双轨行情雷达 (Regime Analyzer)
============================================
【职责】
- 主 Regime：用「最近 N 日上涨家数占比」的均值刻画战略环境（主升 / 震荡 / 退潮）。
- 副 Regime：用「最近一日」宽度 + 均涨刻画短期情绪（高潮 / 冰点 / 回暖 / 平稳）。

【统计口径】
- 仅使用 DuckDB；日线聚合前在 SQL 层过滤：沪深 A（.SH/.SZ）、剔除北交所、剔除常见指数代码、
  可选剔除 ST（依赖 stock_basic.name）、剔除无量与涨跌幅极端脏样本。
- 阈值全部来自 config.yaml 的 regime 段，避免魔法数散落。

【下游】
- 返回 dict 含 sentiment_key（高潮/冰点/回暖/平稳），由 ui/app 写入 st.session_state['market_sentiment']。
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from data.db_core import config as _db_config
    from data.db_core import get_read_conn_singleton, table_exists
except ImportError as e:
    logging.warning("regime_analyzer 无法导入 db_core，盘面雷达将恒返回默认状态: %s", e)
    _db_config = {}

    def get_read_conn_singleton():
        raise RuntimeError("db_core 不可用")

    def table_exists(_name):
        return False


def _regime_config() -> Dict[str, Any]:
    """合并 config.yaml 的 regime 段与内置默认，避免缺键崩溃。"""
    base = (_db_config or {}).get("regime") or {}
    defaults: Dict[str, Any] = {
        "lookback_days": 20,
        "primary_window_days": 10,
        "min_sample_days": 5,
        "primary": {"trend_up_ratio": 0.52, "trend_down_ratio": 0.42},
        "secondary": {
            "climax_up_ratio": 0.65,
            "climax_avg_pct_chg": 1.0,
            "freeze_up_ratio": 0.35,
            "freeze_avg_pct_chg": -1.0,
            "rebound_delta": 0.1,
        },
        "sql_filter": {
            "max_abs_pct_chg": 30.0,
            "min_vol": 0.0,
            "exclude_index_ts_codes": ["000001.SH", "399001.SZ", "399006.SZ", "000300.SH"],
        },
    }
    out = {**defaults, **base}
    out["primary"] = {**defaults["primary"], **(base.get("primary") or {})}
    out["secondary"] = {**defaults["secondary"], **(base.get("secondary") or {})}
    out["sql_filter"] = {**defaults["sql_filter"], **(base.get("sql_filter") or {})}
    return out


def _escape_sql_literal(s: str) -> str:
    return str(s).replace("'", "''")


def _build_regime_daily_aggregate_sql(rcfg: Dict[str, Any], has_stock_basic: bool) -> str:
    """
    构造按 trade_date 聚合的 SQL：在 WHERE 中完成 A 股样本净化。
    说明：北交所 .BJ 直接排除；指数代码由配置列表排除；ST 依赖 stock_basic 表中的 name。
    """
    sf = rcfg.get("sql_filter") or {}
    lookback = int(rcfg.get("lookback_days", 20))
    max_abs_pct = float(sf.get("max_abs_pct_chg", 30.0))
    min_vol = float(sf.get("min_vol", 0.0))
    excludes: List[str] = list(sf.get("exclude_index_ts_codes") or [])

    excl_parts: List[str] = []
    for raw in excludes:
        code = _escape_sql_literal(str(raw).strip())
        if code:
            excl_parts.append(f"d.ts_code <> '{code}'")
    excl_clause = " AND ".join(excl_parts) if excl_parts else "1=1"

    join_sql = ""
    st_clause = ""
    if has_stock_basic:
        join_sql = "LEFT JOIN stock_basic b ON d.ts_code = b.ts_code"
        # 名称以 ST、*ST、SST 开头视为风险 ST 样本，剔除；无基础信息行保留（LEFT JOIN 可能 name 为空）
        st_clause = """
          AND (
            b.name IS NULL
            OR (
              b.name NOT LIKE 'ST%'
              AND b.name NOT LIKE '*ST%'
              AND b.name NOT LIKE 'SST%'
            )
          )
        """

    # 涨跌幅绝对值过滤：去掉明显脏数据；30cm 注册制极端日仍落在常见涨跌停以内时可调大 max_abs_pct_chg
    sql = f"""
SELECT
  d.trade_date,
  AVG(d.pct_chg) AS avg_chg,
  CAST(COUNT(CASE WHEN d.pct_chg > 0 THEN 1 END) AS DOUBLE)
    / CAST(NULLIF(COUNT(*), 0) AS DOUBLE) AS up_ratio
FROM daily_data d
{join_sql}
WHERE
  (d.ts_code LIKE '%.SH' OR d.ts_code LIKE '%.SZ')
  AND d.ts_code NOT LIKE '%.BJ'
  AND ({excl_clause})
  AND d.vol IS NOT NULL AND d.vol > {min_vol}
  AND d.pct_chg IS NOT NULL AND ABS(d.pct_chg) <= {max_abs_pct}
  {st_clause}
GROUP BY d.trade_date
ORDER BY d.trade_date DESC
LIMIT {lookback}
"""
    return re.sub(r"\s+", " ", sql).strip()


def _default_regime() -> Dict[str, Any]:
    return {
        "primary": {
            "status": "🔀 数据不足",
            "color": "#94a3b8",
            "advice": "大盘数据样本不足，系统默认开启震荡市防守策略。",
        },
        "secondary": {
            "status": "⚖️ 等待接入",
            "desc": "等待底层数据流入，以激活短线前线吹哨人系统。",
        },
        "sentiment_key": "平稳",
    }


# 进程内短缓存：降低 Streamlit 每次交互重复扫 daily_data 聚合的开销
_REGIME_CACHE_TS: float = 0.0
_REGIME_CACHE_DATA: Optional[Dict[str, Any]] = None
REGIME_CACHE_TTL_SEC = 60.0


def get_market_regime() -> Dict[str, Any]:
    """
    双轨市场状态识别：返回 primary / secondary 展示字段，以及 sentiment_key 供下游与 session 使用。

    sentiment_key 取值：'高潮' | '冰点' | '回暖' | '平稳'
    60 秒内重复调用直接返回缓存结果（同一进程）。
    """
    global _REGIME_CACHE_TS, _REGIME_CACHE_DATA
    now = time.monotonic()
    if _REGIME_CACHE_DATA is not None and (now - _REGIME_CACHE_TS) < REGIME_CACHE_TTL_SEC:
        return _REGIME_CACHE_DATA
    out = _compute_market_regime_uncached()
    _REGIME_CACHE_DATA = out
    _REGIME_CACHE_TS = now
    return out


def _compute_market_regime_uncached() -> Dict[str, Any]:
    rcfg = _regime_config()
    pcfg = rcfg.get("primary") or {}
    scfg = rcfg.get("secondary") or {}
    primary_window = int(rcfg.get("primary_window_days", 10))
    min_sample = int(rcfg.get("min_sample_days", 5))

    tr_up = float(pcfg.get("trend_up_ratio", 0.52))
    tr_dn = float(pcfg.get("trend_down_ratio", 0.42))

    sx_climax_r = float(scfg.get("climax_up_ratio", 0.65))
    sx_climax_avg = float(scfg.get("climax_avg_pct_chg", 1.0))
    sx_freeze_r = float(scfg.get("freeze_up_ratio", 0.35))
    sx_freeze_avg = float(scfg.get("freeze_avg_pct_chg", -1.0))
    sx_rebound = float(scfg.get("rebound_delta", 0.1))

    try:
        if not table_exists("daily_data"):
            return _default_regime()

        con = get_read_conn_singleton()
        if con is None:
            return _default_regime()

        has_sb = bool(table_exists("stock_basic"))
        query = _build_regime_daily_aggregate_sql(rcfg, has_stock_basic=has_sb)
        if not has_sb:
            logging.info(
                "regime_analyzer: 未检测到 stock_basic，ST 名称过滤已跳过；可在数据底座同步 stock_basic 后收紧口径。"
            )

        df = con.execute(query).fetchdf()
        if df is None or df.empty or len(df) < min_sample:
            return _default_regime()

        df = df.sort_values("trade_date", ascending=True).reset_index(drop=True)

        use_tail = min(primary_window, len(df))
        recent_up_mean = float(df["up_ratio"].tail(use_tail).mean())

        if recent_up_mean > tr_up:
            primary_status = "📈 趋势主升"
            primary_color = "#ef4444"
            primary_advice = "重兵出击，70%仓位向【核心中盘】倾斜，把握主升浪。"
        elif recent_up_mean < tr_dn:
            primary_status = "📉 退潮防守"
            primary_color = "#10b981"
            primary_advice = "防守反击，严控仓位，拥抱【巨无霸】压舱石避险。"
        else:
            primary_status = "🔀 震荡博弈"
            primary_color = "#f59e0b"
            primary_advice = "高抛低吸，大票小票均衡配置，不见兔子不撒鹰。"

        latest_up_ratio = float(df["up_ratio"].iloc[-1])
        latest_avg_chg = float(df["avg_chg"].iloc[-1])

        sentiment_key = "平稳"
        if latest_up_ratio > sx_climax_r and latest_avg_chg > sx_climax_avg:
            sec_status = "🔥 情绪高潮"
            sec_desc = "谨防一致性转分歧，切勿无脑追高，适时兑现。"
            sentiment_key = "高潮"
        elif latest_up_ratio < sx_freeze_r and latest_avg_chg < sx_freeze_avg:
            sec_status = "🧊 情绪冰点"
            sec_desc = "留意老龙反抽与错杀修复，适合轻仓试错博弈。"
            sentiment_key = "冰点"
        elif latest_up_ratio > recent_up_mean + sx_rebound:
            sec_status = "⚡ 情绪回暖"
            sec_desc = "短线情绪异动向上，可小仓位测试先锋突击票。"
            sentiment_key = "回暖"
        else:
            sec_status = "⚖️ 情绪平稳"
            sec_desc = "短线无极端异动，请严格遵循主战略方向操作。"
            sentiment_key = "平稳"

        return {
            "primary": {
                "status": primary_status,
                "color": primary_color,
                "advice": primary_advice,
            },
            "secondary": {"status": sec_status, "desc": sec_desc},
            "sentiment_key": sentiment_key,
        }
    except Exception as e:
        logging.error("Regime 雷达计算异常: %s", e)
        return _default_regime()


def invalidate_market_regime_cache() -> None:
    """配置热更新或测试时可调用，强制下一帧重算 Regime。"""
    global _REGIME_CACHE_TS, _REGIME_CACHE_DATA
    _REGIME_CACHE_DATA = None
    _REGIME_CACHE_TS = 0.0
