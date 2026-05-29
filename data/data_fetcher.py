# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 工业级数据采集引擎（57 维物理胸甲：55 基础字段 + 资金共振 + 股性记忆 fund_memory_score + 增量同步）

【P1 十一维平滑分 · 数据契约与增量原则】
打分逻辑在 core/strategies/score_calibration.py（运算期衍生分，不改表结构）。日线层必须稳定提供下列来源列，
均由「缺失日拉取生肉 → 与库合并 → calc_indicators_in_memory 向量化重算」写入 daily_data；禁止为打分单独全市场重复请求 Tushare。

| 维度（业务名） | 主要依赖列 / 来源 |
|----------------|-------------------|
| 筹码真空 | turnover_rate_f（及 pool/scan 侧用近5日换手均值兜底，列：turnover_rate_f、vol、close、circ_mv） |
| 趋势距离 / 均线成熟 | ma20, ma60 |
| 资金攻击 | amount, hk_vol, net_main_amount（近5日窗口） |
| 波段涨幅 | max_60d_pct（indicator_calc 由 60 日高价与收盘价滚动生成） |
| 黄金起爆 | pct_chg, vol_ratio（合并后 rt/df 对齐） |
| 趋势健康 / 高位熔断 | bias_20 |
| 主升斜率 | ma20_slope_5 |
| 动态 PE | pe_ttm 或 pe（行业 q75 分位在 pool_manager/scan_engine 对候选截面统计，非单列） |
| 行业/板块/市值加分 | 行业映射、板块排名、流通市值 circ_mv（亿元在引擎内换算） |

增量分层（减轻 I/O 与 API）：
1. **生肉下载**：`sync_history` / `sync_recent_days` / `sync_single_day` 仅对「日历缺失日」或「近端强制重扫尾窗」调用 `_core_pipeline`；
   若近端交易日已在库且无尾窗强制，**跳过重复下载**，直接 `_sync_daily_features`（零 API）。
2. **指标与 P1 相关列**：每次有新生肉合并后，仅通过 `_rebuild_daily_table_from_full_df` → `calc_indicators_in_memory`
   **一次性**重算含 max_60d_pct、均线、乖离、斜率等，不在打分线程里逐票重拉全表。
3. **capital_resonance_score / fund_memory_score**：若在步骤 1 已走全量重铸，则已在 `calc_indicators_in_memory` 内写入；
   若步骤 1 跳过下载，则由 `_sync_daily_features_*` 从本地 DuckDB **UPDATE** 两列（全表向量化重算、无 Tushare），
   属于「特征热修」而非「行情重复下载」。

【V26.6 资金记忆 / 共振】夜间增量管道尾部 `_sync_daily_features()` 串联重算 capital_resonance_score 与 fund_memory_score；
资金记忆算法见 fund_memory_score.py；共振见 capital_resonance_features.py。
日线主表列集合保持与历史迁移兼容。
- sync_history / sync_recent_days / sync_single_day：仅拉取缺失交易日生肉后合并落库，禁止无差别全历史重下。
- 【稳定性】DuckDB 按交易日 + ts_code 断点续传；API 指数退避 + 抖动；分块/按日失败平滑跳过。
"""
# Standard library
import functools
import logging
import os
import random
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional
from zoneinfo import ZoneInfo

# Third-party
import numpy as np
import pandas as pd
import tushare as ts
import yaml

# Local modules
import constants
from data.capital_resonance_features import compute_capital_resonance_score
from data.db_core import (
    duckdb_checkpoint,
    duckdb_disk_bytes_total,
    duckdb_drop_table_if_exists_resolved,
    duckdb_resolve_table_sql_id,
    duckdb_storage_snapshot,
    duckdb_storage_snapshot_text,
    ensure_v26_compat_view,
    ensure_v26_tables,
    get_conn,
    get_duckdb_path,
    get_existing_trade_dates,
    get_read_conn_singleton,
    save_df_to_sql,
    table_exists,
)
from data.fund_memory_score import compute_fund_memory_score

pd.set_option('future.no_silent_downcasting', True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [数据管道] %(message)s')

_BJ_TZ = ZoneInfo("Asia/Shanghai")


def _now_bj_naive() -> datetime:
    """
    北京时间（naive datetime），与 auto_sniper_daemon 的 Asia/Shanghai 调度锚点一致。
    用于 trade_cal 的 end_dt 与「hour>=16 则日历含当日」判断；勿用系统本地时区的 datetime.now()。
    """
    return datetime.now(_BJ_TZ).replace(tzinfo=None)


# ==================== 实盘熔断：禁止「空表静默」继续选股 ====================
class DataFetchCriticalError(RuntimeError):
    """
    行情数据拉取不可用：无 Token、鉴权失败、或网络/API 在退避重试后仍失败。
    抛出前已通过企微发送「实盘熔断」强告警；调用方应捕获并终止本轮流水线，勿吞掉后继续打分。
    """


# 企微正文固定话术（与产品要求一致，便于值班识别）
_FUSE_ALERT_BODY = (
    "🚨【实盘熔断】行情数据拉取彻底失败！Tushare 无响应或 Token 失效，为防错算，"
    "系统已强行终止本轮扫描，请立刻检查服务器网络！"
)


def raise_data_fetch_critical(reason: str, cause: Optional[BaseException] = None) -> None:
    """
    先推企微强告警（走 notify_wechat_system_alert，带 dedup），再抛 DataFetchCriticalError。
    懒加载网关，避免 import 环；告警失败不影响抛错。
    """
    detail = _FUSE_ALERT_BODY
    if reason:
        detail = f"{_FUSE_ALERT_BODY}\n详情：{reason}"
    if cause is not None:
        detail = f"{detail}\n末次异常：{cause!s}"
    detail = f"{detail}\n根因摘要：{_classify_data_sync_root_cause(reason, cause)}"
    try:
        from core.notification_gateway import notify_wechat_system_alert

        notify_wechat_system_alert(
            title="【实盘熔断】行情数据拉取失败",
            detail=detail[:2000],
            category="data_fetch_critical",
            dedup_key="data_fetch_tushare_fuse",
        )
    except Exception:
        pass
    if cause is not None:
        raise DataFetchCriticalError(reason) from cause
    raise DataFetchCriticalError(reason)


# ==================== 0) 日线宽表字段契约（产品与 P2–P5 对齐）====================
# 业务口径「52 维底层」指：除 ts_code、trade_date、及本地派生调试列外，行情+指标+资金+筹码+双评分的
# 核心物理列集合；本处 ALL_55_COLS 为落库列名列表（含 capital_resonance_score / fund_memory_score），
# P2–P5 仅允许引用本列表及 indicator_calc 派生的 max_60d_pct、macd_diff 等，禁止引用已废弃字段。
# 说明：55 个基础字段 + capital_resonance_score（0~100）
#      + fund_memory_score（0~200，半衰期见 constants.FUND_MEMORY_HALF_LIFE_DAYS，见 fund_memory_score.py）。
# 落库：['ts_code', 'trade_date'] + ALL_55_COLS => 共 59 列。
ALL_55_COLS = [
    'open', 'high', 'low', 'close', 'pre_close', 'pct_chg', 'vol', 'amount', 'turnover_rate_f', 'vol_ratio',
    'pe_ttm', 'pb', 'ps_ttm', 'dv_ratio', 'total_mv', 'circ_mv', 'adj_factor',
    'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'ma250',
    'vol_ma5', 'vol_ma10', 'vol_ma20',
    'ma20_slope_5', 'high_20', 'low_60',
    'macd', 'macd_signal', 'macd_hist', 'rsi_14', 'kdj_k', 'kdj_d', 'boll_upper', 'boll_lower', 'cci', 'bias_20', 'atr_pct',
    'net_elg_amount', 'net_main_amount', 'inst_net_buy', 'hk_vol', 'rz_net_buy',
    'cost_5th', 'cost_50th', 'cost_95th', 'avg_cost', 'winner_rate', 'cyq_concentration',
    'nineturn_signal', 'limit_times', 'strth', 'forecast_type',
    'capital_resonance_score',
    'fund_memory_score',
]

# P1 十一维在日线侧的核心物理列（含指标产物）；与模块 docstring 表格一致，供运维检索 grep，非运行时代码依赖。
P1_ELEVEN_DIM_SOURCE_COLUMNS = (
    "turnover_rate_f",
    "vol",
    "close",
    "circ_mv",
    "amount",
    "ma20",
    "ma60",
    "hk_vol",
    "net_main_amount",
    "max_60d_pct",
    "pct_chg",
    "vol_ratio",
    "bias_20",
    "ma20_slope_5",
    "pe_ttm",
    "pe",
)

# 近端最后 K 个交易日：即使库中已有该 trade_date，仍入队重扫（新股/半日补洞）。
# 注意：若 K ≥ sync_recent_days 的 days（守护进程为 5），则窗口内每日都会入队；而断点续传在「已齐」时
# 会返回空生肉 → 易被误判为「全部拉取失败」。故 K 宜小于常见 days，仅强制刷新最近 1～2 根。
RECENT_FORCE_RESYNC_TAIL = 2

# 指数退避：封顶等待（秒）
_BACKOFF_CAP_SEC = 90.0
_BACKOFF_BASE_SEC = 0.55

# 系统/终端常设 HTTP(S)_PROXY=127.0.0.1:7890 等；Clash 未启动时 requests 会卡满 read timeout 才失败。
_PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


@contextmanager
def _no_proxy_env():
    """临时清除进程内代理环境变量，供 Tushare/requests 直连（finally 恢复原值）。"""
    saved: dict = {}
    try:
        for k in _PROXY_ENV_KEYS:
            if k in os.environ:
                saved[k] = os.environ.pop(k)
        yield
    finally:
        for k, v in saved.items():
            os.environ[k] = v


def _proxy_env_snapshot() -> str:
    parts = []
    for k in _PROXY_ENV_KEYS:
        v = os.environ.get(k)
        if v:
            parts.append(f"{k}={v}")
    return "; ".join(parts) if parts else "(none)"


def _error_suggests_dead_loopback_proxy(exc: BaseException) -> bool:
    s = str(exc).lower()
    if "timed out" not in s and "timeout" not in s:
        return False
    if "127.0.0.1" in s or "localhost" in s:
        return True
    # 部分栈信息不含 host，但环境变量指向本机代理端口
    for k in _PROXY_ENV_KEYS:
        v = (os.environ.get(k) or "").lower()
        if not v:
            continue
        if "127.0.0.1" in v or "localhost" in v:
            return True
    return False


def _classify_data_sync_root_cause(detail: str = "", cause: Optional[BaseException] = None) -> str:
    """
    将同步失败归因为可执行摘要，便于企微告警快速定位。
    """
    tail = str(cause) if cause is not None else ""
    text = f"{detail}\n{tail}".lower()
    if any(k in text for k in ("token", "pro is none", "未初始化", "permission", "auth", "鉴权", "401", "403", "权限")):
        return "Token/权限异常（请核对 tushare.token、接口权限与额度）"
    if any(
        k in text
        for k in (
            "getaddrinfo",
            "name resolution",
            "nameresolution",
            "failed to resolve",
            "temporary failure in name resolution",
            "11001",
            "could not resolve host",
            "nodename nor servname",
        )
    ):
        return "DNS 解析失败（无法解析 Tushare 域名；可在 config.yaml 配置 tushare.custom_endpoint 专线 URL 或修正服务器 DNS/代理）"
    if ("127.0.0.1" in tail or "localhost" in tail) and any(
        k in text for k in ("timed out", "timeout", "read timed out", "connection")
    ):
        return (
            "本机 HTTP 代理无响应（请求经 127.0.0.1/localhost 代理；请启动 Clash/V2 等或清空 "
            "HTTP_PROXY/HTTPS_PROXY；程序会在检测到此类错误时自动尝试一次直连）"
        )
    if any(k in text for k in ("timeout", "timed out", "connection", "network", "无法连接", "read timed out", "connect")):
        return "网络/超时异常（请检查服务器出网、DNS、代理与接口可达性）"
    if any(k in text for k in ("生肉为空", "无数据", "为空", "all_days_failed", "trade_cal 返回空表", "未产出有效生肉", "empty")):
        return "空数据/过滤后为空（请检查交易日历、接口返回与市值过滤口径）"
    if any(k in text for k in ("duckdb", "file is already open", "cannot open file", "进程无法访问", "lock")):
        return "本地数据库锁冲突（请确保仅单实例写库，关闭重复 python 进程）"
    return "未分类异常（请查看 data/runtime/sniper.log 末条栈追踪）"


def _norm_cal_date_8(s) -> str:
    """统一为 8 位 YYYYMMDD 字符串，便于与交易日历比对。"""
    if s is None:
        return ""
    try:
        if pd.isna(s):
            return ""
    except Exception:
        pass
    if isinstance(s, (np.integer, np.floating, int, float)):
        try:
            if isinstance(s, float) and pd.isna(s):
                return ""
            x = str(int(s))
            return x[:8] if len(x) >= 8 else x
        except Exception:
            pass
    x = str(s).replace("-", "").strip()
    if "." in x:
        x = x.split(".", 1)[0]
    return x[:8] if len(x) >= 8 else x


def _api_trade_date_str(date_str) -> str:
    """Tushare daily_basic / daily 等接口要求的 trade_date：八位 YYYYMMDD 字符串。"""
    d8 = _norm_cal_date_8(date_str)
    return d8 if len(d8) == 8 and d8.isdigit() else str(date_str).replace("-", "").replace(".", "")[:8]


def _normalize_ts_code_val(v) -> str:
    """
    统一 ts_code 口径到 6位+交易所后缀（如 600000.SH / 000001.SZ）。
    部分接口会返回无后缀纯 6 位代码，若不标准化会在 merge/filter 时被误丢弃。
    """
    if v is None:
        return ""
    s = str(v).strip().upper()
    if not s:
        return ""
    if "." in s:
        return s
    if len(s) == 6 and s.isdigit():
        return f"{s}.SH" if s.startswith("6") else f"{s}.SZ"
    return s


def _normalize_ts_code_series(sr: pd.Series) -> pd.Series:
    try:
        return sr.map(_normalize_ts_code_val)
    except Exception:
        return sr.astype(str).str.strip().str.upper()


def _trade_date_for_sql(date_str: str) -> str:
    """daily_data.trade_date 落库为 YYYY-MM-DD 字符串；用于 DuckDB 等值查询。"""
    s = _norm_cal_date_8(date_str)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return str(date_str)[:10]


def _existing_ts_codes_for_trade_date(trade_date_yyyymmdd: str) -> set:
    """
    查询 DuckDB：该交易日已存在的 ts_code 集合。
    用于断点续传：仅对缺失代码发起分块下载，避免重复拉取已落库个股。
    """
    try:
        if not table_exists("daily_data"):
            return set()
        con = get_read_conn_singleton()
        sql_dt = _trade_date_for_sql(trade_date_yyyymmdd)
        rows = con.execute(
            "SELECT DISTINCT ts_code FROM daily_data WHERE CAST(trade_date AS VARCHAR) = ?",
            [sql_dt],
        ).fetchall()
        return {str(r[0]).strip() for r in rows if r and r[0] is not None}
    except Exception as ex:
        logging.warning("【断点续传】查询已落库 ts_code 失败，当日将按全量候选拉取: %s", ex)
        return set()


def _count_rows_for_trade_date(trade_date_yyyymmdd: str) -> int:
    """统计 daily_data 中该交易日行数（与 _existing_ts_codes_for_trade_date 同一日期匹配语义）。"""
    if not table_exists("daily_data"):
        return 0
    try:
        con = get_read_conn_singleton()
        if con is None:
            return 0
        sql_dt = _trade_date_for_sql(trade_date_yyyymmdd)
        r = con.execute(
            "SELECT COUNT(*) FROM daily_data WHERE CAST(trade_date AS VARCHAR) = ?",
            [sql_dt],
        ).fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    except Exception as ex:
        logging.debug("_count_rows_for_trade_date: %s", ex)
        return 0


def _to_sync_dates_all_have_daily_rows(to_sync_raw_dates) -> bool:
    """
    本轮计划同步的每个交易日，在 daily_data 中是否都已有行。
    用于区分「接口真失败」与「断点续传：候选已在库故无新生肉」。
    """
    if not to_sync_raw_dates:
        return True
    for d in to_sync_raw_dates:
        d8 = _norm_cal_date_8(d)
        if len(d8) != 8 or not d8.isdigit():
            return False
        if _count_rows_for_trade_date(d8) <= 0:
            return False
    return True


def _is_rate_or_network_backoff(e: Exception) -> bool:
    """判断是否适合退避重试（限速 / 网络抖动）。"""
    try:
        import requests

        if isinstance(
            e,
            (
                requests.exceptions.Timeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ),
        ):
            return True
    except Exception:
        pass
    msg = str(e).lower()
    keys = (
        "timeout", "timed out", "connection", "reset", "refused", "broken pipe",
        "429", "too many", "频率", "限速", "限流", "每分钟", "访问过快", "请稍后",
        "remote end closed", "ssl", "handshake",
        "getaddrinfo", "name resolution", "nameresolution", "failed to resolve",
        "temporary failure in name resolution", "11001", "could not resolve host",
    )
    return any(k in msg for k in keys)


def _fatal_token_error(e: Exception) -> bool:
    msg = str(e)
    return ("token不对" in msg) or ("权限" in msg) or ("积分" in msg and "不足" in msg)


def _load_dotenv_for_tushare() -> None:
    """
    将 .env 文件中的 TUSHARE_TOKEN 加载到 os.environ。
    仅首次调用时生效，后续直接读 os.environ。
    """
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
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


def init_pro():
    """
    初始化 Tushare Pro API。
    Token 读取优先级：os.environ['TUSHARE_TOKEN'] > .env 文件 > config.yaml > custom_endpoint。
    首次运行时会通过 config_manager 的引导流程引导用户输入 key。
    """
    _load_dotenv_for_tushare()
    token = ""
    endpoint = ""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg_path = os.path.join(base_dir, "config.yaml")
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            token = (cfg.get('tushare', {}) or {}).get('token', '')
            endpoint = ((cfg.get('tushare', {}) or {}).get('custom_endpoint', '') or "").strip()
        except Exception as e:
            logging.warning(f"读取 config.yaml 失败: {e}")
    if not token:
        token = os.getenv('TUSHARE_TOKEN', '')
    if not token:
        logging.error("❌ 严重错误：未找到 Tushare Token！请检查 config.yaml 或设置 TUSHARE_TOKEN 环境变量。")
        return None
    ts.set_token(token)
    pro_api = ts.pro_api(token)
    if endpoint:
        pro_api._DataApi__http_url = endpoint
        logging.info(f"🔗 已挂载 Tushare VIP 共享专线: {endpoint}")
    return pro_api


pro = init_pro()


def retry_api(func):
    """
    Tushare 调用包装：指数退避 + 均匀抖动；区分致命鉴权错误与可重试网络/限速异常。

    【实盘安全】不再在「无 pro / Token 死 / 重试耗尽」时返回空 DataFrame。
    上述情况一律先企微熔断告警，再抛 DataFetchCriticalError，由上层终止本轮下载或扫描。
    正常 HTTP 返回空表（例如当日无数据）仍原样返回，不视为熔断。
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if pro is None:
            logging.error("❌ retry_api：Tushare pro 未初始化，拒绝静默空表")
            raise_data_fetch_critical("retry_api：Tushare 未初始化（pro is None），请配置 config.yaml 或环境变量 TUSHARE_TOKEN")
        max_attempts = max(6, int(getattr(constants, "API_RETRY_TIMES", 3)) + 4)
        last_err: Optional[BaseException] = None
        tried_no_proxy = False
        for attempt in range(max_attempts):
            try:
                res = func(*args, **kwargs)
                if res is not None:
                    return res
            except Exception as e:
                last_err = e
                if (not tried_no_proxy) and _error_suggests_dead_loopback_proxy(e):
                    tried_no_proxy = True
                    proxy_snapshot = _proxy_env_snapshot()
                    logging.warning(
                        "【API】检测到本机代理疑似失联，先发出直连告警并临时切换直连 | error=%s | proxies=%s",
                        str(e)[:220],
                        proxy_snapshot,
                    )
                    try:
                        with _no_proxy_env():
                            res = func(*args, **kwargs)
                        if res is not None:
                            logging.warning(
                                "【API】直连重试成功；当前进程已自动绕过代理。若需长期直连，请清理系统代理或环境变量。"
                            )
                            return res
                    except Exception as e2:
                        last_err = e2
                        logging.warning(
                            "【API】直连重试仍失败 | error=%s | proxies=%s",
                            str(e2)[:220],
                            proxy_snapshot,
                        )
                        if _is_rate_or_network_backoff(e2):
                            try:
                                from core.notification_gateway import notify_wechat_system_alert

                                notify_wechat_system_alert(
                                    title="【API】本机代理疑似失联，已自动切换直连",
                                    detail=(
                                        f"接口: {getattr(func, '__name__', 'api')}\n"
                                        f"错误: {str(e2)[:900]}\n"
                                        f"当前代理环境: {proxy_snapshot}\n"
                                        f"处理: 已自动清理 HTTP(S)_PROXY 并重试一次直连"
                                    ),
                                    category="api_proxy_fallback",
                                    dedup_key=f"api_proxy_fallback_{getattr(func, '__name__', 'api')}",
                                )
                            except Exception:
                                pass
                if _fatal_token_error(last_err):
                    logging.error("❌ 接口权限/Token 报错，停止重试并熔断: %s", last_err)
                    raise_data_fetch_critical(
                        f"接口 {getattr(func, '__name__', 'api')}：鉴权或 Token 无效",
                        cause=last_err,
                    )
                if not _is_rate_or_network_backoff(last_err) and attempt > 1:
                    logging.warning("【API】非网络类异常，减少重试: %s", last_err)
                exp = min(_BACKOFF_CAP_SEC, _BACKOFF_BASE_SEC * (2 ** attempt))
                jitter = random.uniform(0.0, 0.45)
                delay = exp + jitter
                logging.debug(
                    "API [%s] 第 %s/%s 次失败后退避 %.2fs: %s",
                    getattr(func, "__name__", "api"),
                    attempt + 1,
                    max_attempts,
                    delay,
                    str(last_err)[:160],
                )
                time.sleep(delay)
        logging.error(
            "❌ 接口 %s 已达最大重试仍失败，拒绝返回空表",
            getattr(func, "__name__", "api"),
        )
        raise_data_fetch_critical(
            f"接口 {getattr(func, '__name__', 'api')}：连续 {max_attempts} 次调用失败（网络/限速或接口异常）",
            cause=last_err,
        )

    return wrapper


def fetch_chunked(api_func, codes, date_str, chunk_size=500, **kwargs):
    """分块拉取：单块失败仅跳过该块，不拖垮整日任务。"""
    res_list = []
    if not codes:
        return pd.DataFrame()
    for i in range(0, len(codes), chunk_size):
        chunk = ",".join(codes[i:i + chunk_size])
        try:
            df = retry_api(api_func)(ts_code=chunk, trade_date=date_str, **kwargs)
            if df is not None and not df.empty:
                res_list.append(df)
        except DataFetchCriticalError:
            # 网络/鉴权级失败：整块日任务必须失败，不能靠「跳过该块」假装成功
            raise
        except Exception as e:
            logging.warning("【平滑降级】分块拉取跳过 (offset=%s): %s", i, e)
            continue
    return pd.concat(res_list, ignore_index=True) if res_list else pd.DataFrame()


def fetch_trade_date_fallback(api_func, date_str, codes=None, **kwargs):
    """
    回退整日拉取：不传 ts_code，仅按 trade_date 请求，再按 codes 过滤。
    适用于部分高阶接口在 ts_code 分块模式下易返回空表的场景。
    """
    try:
        df = retry_api(api_func)(trade_date=date_str, **kwargs)
    except DataFetchCriticalError:
        raise
    except Exception as e:
        logging.debug("trade_date 回退拉取失败 [%s]: %s", getattr(api_func, "__name__", "api"), e)
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    if "ts_code" in df.columns:
        df["ts_code"] = _normalize_ts_code_series(df["ts_code"])
    if codes and "ts_code" in df.columns:
        code_set = {_normalize_ts_code_val(x) for x in codes}
        df = df[df["ts_code"].astype(str).isin(code_set)].copy()
    return df


def fetch_prefer_trade_date(api_func, codes, date_str, *, chunk_size=500, **kwargs):
    """
    优先按 trade_date 整日拉取；若为空再回退 ts_code 分块。
    适用于 cyq_perf / hk_hold / margin_detail 等在 ts_code 模式下易空表的接口。
    """
    df_all = fetch_trade_date_fallback(api_func, date_str, codes=codes, **kwargs)
    if df_all is not None and not df_all.empty:
        return df_all
    return fetch_chunked(api_func, codes, date_str, chunk_size=chunk_size, **kwargs)


def _map_forecast_type(x):
    if pd.isna(x):
        return np.nan
    s = str(x)
    if '首亏' in s or '续亏' in s:
        return -2.0
    if '减' in s or '降' in s:
        return -1.0
    if '扭亏' in s:
        return 0.5
    if '增' in s or '盈' in s:
        return 1.0
    return 0.0


def get_p0_codes():
    if not os.path.exists(constants.P0_FILE_PATH):
        return set()
    with open(constants.P0_FILE_PATH, 'r', encoding='utf-8') as f:
        return {f"{c}.SH" if c.startswith('6') else f"{c}.SZ" for c in re.findall(r'\d{6}', f.read())}


def check_data_completeness(days=150, *, static_mode: bool = False):
    if pro is None:
        raise_data_fetch_critical("check_data_completeness：Tushare 未初始化，无法比对交易日历")
    now = _now_bj_naive()
    end_dt = (now if now.hour >= 16 else now - timedelta(days=1)).strftime('%Y%m%d')
    start_dt = (now - timedelta(days=days * 2 + 30)).strftime('%Y%m%d')
    if static_mode:
        existing_raw = get_existing_trade_dates()
        existing_norm = {_norm_cal_date_8(d) for d in existing_raw}
        required_8 = sorted(existing_norm)[-days:]
        missing = [d8 for d8 in required_8 if d8 not in existing_norm]
        return True, list(dict.fromkeys(missing))
    cal = retry_api(pro.trade_cal)(exchange='SSE', is_open='1', start_date=start_dt, end_date=end_dt)
    if cal.empty:
        return False, []
    required_dates = cal.sort_values('cal_date')['cal_date'].tolist()[-days:]
    required_8 = [_norm_cal_date_8(d) for d in required_dates]

    existing_raw = get_existing_trade_dates()
    existing_norm = {_norm_cal_date_8(d) for d in existing_raw}

    # 半残交易日识别：仅看「日期存在」不够，若某日仅写入了异常低行数（中断/锁冲突/接口波动），
    # 也应纳入补洞。阈值采用近期中位数的 70% 与绝对下限 500 的较大值，避免误伤。
    sparse_dates = set()
    try:
        if table_exists("daily_data"):
            con = get_read_conn_singleton()
            if con is not None:
                need_sql_dates = list(dict.fromkeys(required_8))
                if need_sql_dates:
                    date_literals = ",".join([f"'{d}'" for d in need_sql_dates])
                    q = f"""
                        SELECT REPLACE(CAST(trade_date AS VARCHAR), '-', '') AS d8, COUNT(*) AS n
                        FROM daily_data
                        WHERE REPLACE(CAST(trade_date AS VARCHAR), '-', '') IN ({date_literals})
                        GROUP BY 1
                    """
                    cnt_df = con.execute(q).fetchdf()
                    if cnt_df is not None and not cnt_df.empty:
                        cnt_df["d8"] = cnt_df["d8"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
                        cnt_df["n"] = pd.to_numeric(cnt_df["n"], errors="coerce").fillna(0).astype(int)
                        counts = [int(x) for x in cnt_df["n"].tolist() if int(x) > 0]
                        if counts:
                            baseline = int(np.median(np.asarray(counts, dtype=np.float64)))
                            floor_n = max(500, int(0.7 * baseline))
                            # 【性能优化 V2】向量化替代 iterrows：避免逐行 Python 迭代
                            sparse_dates = set(
                                str(d8) for d8 in cnt_df.loc[
                                    pd.to_numeric(cnt_df["n"], errors="coerce").fillna(0) < floor_n, "d8"
                                ].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
                            )
                            if sparse_dates:
                                logging.warning(
                                    "【完整性检查】检测到疑似半残交易日（低行数），将纳入补洞: %s | 阈值=%s",
                                    sorted(sparse_dates),
                                    floor_n,
                                )
    except Exception as e:
        logging.debug("check_data_completeness 稀疏日检查失败（已降级忽略）: %s", e)

    # 【UI/缺失查询】仅报告「库内 DISTINCT trade_date 中尚不存在」的交易日。
    # 近端 N 日强制重扫由 sync_recent_days 内部 tail_set 单独处理；若此处也把 tail 标为「缺失」，
    # 则库中已有该日时 fetch_raw_day_data 会因断点续传直接返回空 → 用户看到「生肉为空」却永远补不齐。
    missing = [d8 for d8 in required_8 if (d8 not in existing_norm) or (d8 in sparse_dates)]
    missing = list(dict.fromkeys(missing))
    return True, missing


def _fetch_daily_basic_for_date(trade_date: str, max_empty_retries: int = 6) -> pd.DataFrame:
    """
    拉取 daily_basic。空表可能由：限速/网络抖动/接口短暂无数据/未收盘。
    对「空表」做有限次重试，避免静默吞掉整日。
    """
    if pro is None:
        raise_data_fetch_critical("_fetch_daily_basic_for_date：Tushare 未初始化")
    td = _api_trade_date_str(trade_date)
    last_empty = 0
    for attempt in range(max_empty_retries):
        df = retry_api(pro.daily_basic)(
            trade_date=td,
            fields='ts_code,pe_ttm,pb,ps_ttm,dv_ratio,total_mv,circ_mv,turnover_rate_f,volume_ratio',
        )
        if df is not None and not df.empty:
            return df
        last_empty = int(len(df)) if df is not None else 0
        if attempt < max_empty_retries - 1:
            delay = 0.6 * (2 ** attempt) + random.uniform(0.0, 0.35)
            logging.warning(
                "【daily_basic】%s 返回空表，%s/%s 次后重试 %.2fs",
                td,
                attempt + 1,
                max_empty_retries,
                delay,
            )
            time.sleep(delay)
    logging.error(
        "【daily_basic】%s 连续 %s 次仍为空（最后行数=%s）。常见原因：限速/积分/专线未收盘或 trade_date 格式错误。",
        td,
        max_empty_retries,
        last_empty,
    )
    return pd.DataFrame()


def _latest_report_period_label(end_date: str) -> str:
    s = str(end_date or "").strip().replace("-", "")[:8]
    if len(s) != 8:
        return "未知报告期"
    return f"{s[:4]}年{s[4:6]}月"


def _safe_numeric_series(df: pd.DataFrame, col: str, default=0.0) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series([default] * (0 if df is None else len(df)))
    return pd.to_numeric(df[col], errors="coerce")


def _build_financial_risk_flags_vectorized(out: pd.DataFrame) -> pd.Series:
    """
    【V26.6 性能优化】真向量化版本：
    原实现名为"vectorized"但内部使用 Python for 循环 + .iloc[i] 标量提取，
    这是 pandas 中最慢的反模式之一，比纯 NumPy 慢 50–100 倍。
    改为 np.select 链实现真正的向量化计算，单次 DataFrame 扫描完成所有判断。
    """
    if out is None or out.empty:
        return pd.Series(["未见明显财务硬伤"])

    try:
        # 批量向量化数值转换（一次 Series 操作替代多次 to_numeric）
        net_profit_yoy = pd.to_numeric(out.get("net_profit_yoy"), errors="coerce")
        revenue_yoy = pd.to_numeric(out.get("revenue_yoy"), errors="coerce")
        deduct_net_profit_yoy = pd.to_numeric(out.get("deduct_net_profit_yoy"), errors="coerce")
        op_cash_flow = pd.to_numeric(out.get("op_cash_flow"), errors="coerce")
        asset_liab_rate = pd.to_numeric(out.get("asset_liab_rate"), errors="coerce")
        goodwill = pd.to_numeric(out.get("goodwill"), errors="coerce")

        # 六个布尔 mask 向量化计算
        mask_profit = net_profit_yoy < -20
        mask_revenue = revenue_yoy < -10
        mask_deduct = deduct_net_profit_yoy < -20
        mask_cash_flow = op_cash_flow < 0
        mask_asset_liab = asset_liab_rate > 70
        mask_goodwill = goodwill > 1e9

        # 【V26.6 核心优化】使用 np.select 替代 Python for 循环 + .iloc[i]
        # np.select 内部以 C 速度执行条件匹配，避免逐行 Python 对象操作
        # conditions 顺序即优先级（首次 match 优先）
        conditions = [mask_profit, mask_revenue, mask_deduct, mask_cash_flow, mask_asset_liab, mask_goodwill]
        choices = ["净利同比下滑", "营收同比下滑", "扣非承压", "经营现金流为负", "资产负债率偏高", "商誉较高"]
        flag_labels = np.select(conditions, choices, default="")

        # 将空标签替换为默认提示
        final_flags = np.where(flag_labels == "", "未见明显财务硬伤", flag_labels)
        return pd.Series(final_flags, index=out.index)
    except Exception:
        pass

    # fallback：全走默认
    return pd.Series(["未见明显财务硬伤"] * (len(out) if out is not None and not out.empty else 1),
                     index=(out.index if out is not None and not out.empty else None))


def _build_financial_risk_flags(row: pd.Series) -> str:
    """
    【保留兼容】逐行版本，供非热点路径（如独立调用）使用。
    热点路径（apply(axis=1)）已被 _build_financial_risk_flags_vectorized 替代。
    """
    flags = []
    try:
        if pd.notna(row.get("net_profit_yoy")) and float(row.get("net_profit_yoy")) < -20:
            flags.append("净利同比下滑")
        if pd.notna(row.get("revenue_yoy")) and float(row.get("revenue_yoy")) < -10:
            flags.append("营收同比下滑")
        if pd.notna(row.get("deduct_net_profit_yoy")) and float(row.get("deduct_net_profit_yoy")) < -20:
            flags.append("扣非承压")
        if pd.notna(row.get("op_cash_flow")) and float(row.get("op_cash_flow")) < 0:
            flags.append("经营现金流为负")
        if pd.notna(row.get("asset_liab_rate")) and float(row.get("asset_liab_rate")) > 70:
            flags.append("资产负债率偏高")
        if pd.notna(row.get("goodwill")) and float(row.get("goodwill")) > 1e9:
            flags.append("商誉较高")
    except Exception:
        pass
    return "；".join(flags) if flags else "未见明显财务硬伤"


def _format_financial_amount_cn(val: Any) -> str:
    try:
        x = float(val)
        if not np.isfinite(x):
            return ""
        ax = abs(x)
        if ax >= 1e8:
            return f"{x / 1e8:.2f}亿"
        if ax >= 1e4:
            return f"{x / 1e4:.2f}万"
        return f"{x:.2f}"
    except Exception:
        return ""


def _build_financial_summary_text(row: pd.Series) -> str:
    period = _latest_report_period_label(str(row.get("end_date") or ""))
    pieces = [period]
    for key, label in (("revenue", "营收"), ("net_profit", "净利")):
        txt = _format_financial_amount_cn(row.get(key))
        if txt:
            pieces.append(f"{label}{txt}")
    for key, label, suffix in (
        ("revenue_yoy", "营收同比", "%"),
        ("net_profit_yoy", "净利同比", "%"),
        ("deduct_net_profit_yoy", "扣非同比", "%"),
        ("asset_liab_rate", "负债率", "%"),
    ):
        val = row.get(key)
        try:
            if pd.notna(val):
                pieces.append(f"{label}{float(val):.2f}{suffix}")
        except Exception:
            continue
    risk = str(row.get("risk_flags") or "").strip()
    if risk:
        pieces.append(f"风险：{risk}")
    return "，".join(pieces)[:500]


def _merge_financial_side_table(out: pd.DataFrame, side: pd.DataFrame, value_cols: List[str]) -> pd.DataFrame:
    if out is None or out.empty or side is None or side.empty or "ts_code" not in side.columns:
        return out
    side = side.copy()
    side["ts_code"] = _normalize_ts_code_series(side["ts_code"])
    keys = [c for c in ("ts_code", "ann_date", "end_date") if c in out.columns and c in side.columns]
    if "ts_code" not in keys:
        return out
    keep = keys + [c for c in value_cols if c in side.columns]
    side = side[keep].drop_duplicates(subset=keys, keep="last")
    if not [c for c in value_cols if c in side.columns]:
        return out
    merged = pd.merge(out, side, on=keys, how="left", suffixes=("", "_side"))
    for col in value_cols:
        scol = f"{col}_side"
        if scol in merged.columns:
            if col in merged.columns:
                merged[col] = merged[col].combine_first(merged[scol])
            else:
                merged[col] = merged[scol]
            merged = merged.drop(columns=[scol])
    return merged


def _fetch_financial_chunked(api_func, codes, *, start_date: str, end_date: str, fields: str = "", chunk_size: int = 120) -> pd.DataFrame:
    """财报类接口通常要求 ts_code，且不认 trade_date；这里按 ts_code 分块拉。"""
    if pro is None or not codes:
        return pd.DataFrame()
    res_list = []
    code_list = [_normalize_ts_code_val(c) for c in codes if str(c).strip()]
    for i in range(0, len(code_list), max(1, int(chunk_size))):
        chunk = ",".join(code_list[i:i + max(1, int(chunk_size))])
        try:
            kwargs = {"ts_code": chunk, "start_date": start_date, "end_date": end_date}
            if fields:
                kwargs["fields"] = fields
            df = retry_api(api_func)(**kwargs)
            if df is not None and not df.empty:
                res_list.append(df)
        except DataFetchCriticalError:
            raise
        except Exception as e:
            logging.warning("【财报增强】%s 分块拉取跳过 offset=%s: %s", getattr(api_func, "__name__", "api"), i, e)
            continue
    return pd.concat(res_list, ignore_index=True) if res_list else pd.DataFrame()


def _fetch_financial_report_snapshot(codes, start_date: str, end_date: str, status_callback=print) -> pd.DataFrame:
    """拉取近年财报/财务指标快照，落 fact_financial_reports；供 DeepSeek 直接读取年报季报关键字段。"""
    if pro is None or not codes:
        return pd.DataFrame()
    code_set = {_normalize_ts_code_val(c) for c in codes if str(c).strip()}
    if not code_set:
        return pd.DataFrame()
    start8 = _api_trade_date_str(start_date)
    end8 = _api_trade_date_str(end_date)
    try:
        ind = _fetch_financial_chunked(pro.fina_indicator, list(code_set), start_date=start8, end_date=end8, chunk_size=120)
    except Exception as e:
        logging.warning("【财报增强】fina_indicator 拉取失败: %s", e)
        ind = pd.DataFrame()
    if ind is None or ind.empty or "ts_code" not in ind.columns:
        return pd.DataFrame()
    ind["ts_code"] = _normalize_ts_code_series(ind["ts_code"])
    ind = ind[ind["ts_code"].isin(code_set)].copy()
    if ind.empty:
        return pd.DataFrame()

    keep_map = {
        "ts_code": "ts_code",
        "ann_date": "ann_date",
        "end_date": "end_date",
        "or_yoy": "revenue_yoy",
        "revenue_yoy": "revenue_yoy",
        "netprofit_yoy": "net_profit_yoy",
        "net_profit_yoy": "net_profit_yoy",
        "dt_netprofit_yoy": "deduct_net_profit_yoy",
        "deduct_net_profit_yoy": "deduct_net_profit_yoy",
        "debt_to_assets": "asset_liab_rate",
    }
    cols = [c for c in keep_map if c in ind.columns]
    out = ind[cols].rename(columns={c: keep_map[c] for c in cols}).copy()
    for col in ("revenue_yoy", "net_profit_yoy", "deduct_net_profit_yoy", "asset_liab_rate"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "ann_date" not in out.columns:
        out["ann_date"] = ""
    if "end_date" not in out.columns:
        out["end_date"] = ""

    try:
        income = _fetch_financial_chunked(
            pro.income,
            list(code_set),
            start_date=start8,
            end_date=end8,
            fields="ts_code,ann_date,end_date,revenue,n_income_attr_p,n_income",
            chunk_size=120,
        )
    except Exception as e:
        logging.debug("【财报增强】income 拉取失败: %s", e)
        income = pd.DataFrame()
    if income is not None and not income.empty and "ts_code" in income.columns:
        income = income[income["ts_code"].notna()].copy()
        income["ts_code"] = _normalize_ts_code_series(income["ts_code"])
        income = income[income["ts_code"].isin(code_set)].copy()
        if "net_profit" not in income.columns:
            if "n_income_attr_p" in income.columns:
                income["net_profit"] = income["n_income_attr_p"]
            elif "n_income" in income.columns:
                income["net_profit"] = income["n_income"]
        out = _merge_financial_side_table(out, income, ["revenue", "net_profit"])

    try:
        bs = _fetch_financial_chunked(
            pro.balancesheet,
            list(code_set),
            start_date=start8,
            end_date=end8,
            fields="ts_code,ann_date,end_date,goodwill,accounts_receiv,inventories,total_liab,total_assets",
            chunk_size=120,
        )
    except Exception as e:
        logging.debug("【财报增强】balancesheet 拉取失败: %s", e)
        bs = pd.DataFrame()
    if bs is not None and not bs.empty and "ts_code" in bs.columns:
        bs["ts_code"] = _normalize_ts_code_series(bs["ts_code"])
        bs = bs[bs["ts_code"].isin(code_set)].copy()
        bs = bs.rename(columns={"accounts_receiv": "accounts_receivable"})
        if "asset_liab_rate" not in bs.columns and {"total_liab", "total_assets"}.issubset(bs.columns):
            bs["asset_liab_rate"] = np.where(
                pd.to_numeric(bs["total_assets"], errors="coerce") > 0,
                pd.to_numeric(bs["total_liab"], errors="coerce") / pd.to_numeric(bs["total_assets"], errors="coerce") * 100.0,
                np.nan,
            )
        out = _merge_financial_side_table(out, bs, ["goodwill", "accounts_receivable", "inventories", "asset_liab_rate"])

    try:
        cf = _fetch_financial_chunked(
            pro.cashflow,
            list(code_set),
            start_date=start8,
            end_date=end8,
            fields="ts_code,ann_date,end_date,n_cashflow_act",
            chunk_size=120,
        )
    except Exception as e:
        logging.debug("【财报增强】cashflow 拉取失败: %s", e)
        cf = pd.DataFrame()
    if cf is not None and not cf.empty and "ts_code" in cf.columns:
        cf["ts_code"] = _normalize_ts_code_series(cf["ts_code"])
        cf = cf[cf["ts_code"].isin(code_set)].copy()
        cf = cf.rename(columns={"n_cashflow_act": "op_cash_flow"})
        out = _merge_financial_side_table(out, cf, ["op_cash_flow"])

    for col in ("revenue", "net_profit", "op_cash_flow", "goodwill", "accounts_receivable", "inventories"):
        if col not in out.columns:
            out[col] = np.nan
    out["report_type"] = np.where(out["end_date"].astype(str).str.endswith("1231"), "annual", "quarter")
    out["report_period"] = out["end_date"].apply(_latest_report_period_label)
    # 【性能优化 V2】向量化替代 apply(axis=1)：消除 pandas 最慢的反模式
    out["risk_flags"] = _build_financial_risk_flags_vectorized(out)
    out["summary_text"] = out.apply(_build_financial_summary_text, axis=1)
    out["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    final_cols = [
        "ts_code", "ann_date", "end_date", "report_type", "report_period", "revenue", "revenue_yoy",
        "net_profit", "net_profit_yoy", "deduct_net_profit_yoy", "op_cash_flow", "asset_liab_rate",
        "goodwill", "accounts_receivable", "inventories", "risk_flags", "summary_text", "updated_at",
    ]
    final_cols = [c for c in final_cols if c in out.columns]
    out = out[final_cols].copy()
    for key_col in ("ts_code", "ann_date", "end_date"):
        if key_col in out.columns:
            out[key_col] = out[key_col].astype(str).str.strip()
            out = out[~out[key_col].isin(("", "NaT", "nan", "None"))]
    out = out.drop_duplicates(subset=["ts_code", "ann_date", "end_date"], keep="last")
    if not out.empty:
        ensure_v26_tables()
        save_df_to_sql(out, "fact_financial_reports")
        status_callback(f"✅ 财报增强字段入库: {len(out)} 条")
    return out


def sync_financial_reports_recent(years: int = 3, status_callback=print) -> pd.DataFrame:
    """独立同步近 N 年财报增强表；重下数据库后可先跑此函数补齐 DeepSeek 财报字段。"""
    if pro is None:
        raise_data_fetch_critical("sync_financial_reports_recent：Tushare 未初始化（pro is None）")
    try:
        ensure_v26_tables()
        codes = []
        try:
            if table_exists("daily_data"):
                con = get_read_conn_singleton(max_wait_sec=30.0)
                if con is not None:
                    dfc = con.execute("SELECT DISTINCT ts_code FROM daily_data").fetchdf()
                    codes = dfc["ts_code"].astype(str).tolist() if dfc is not None and not dfc.empty else []
        except Exception:
            codes = []
        if not codes:
            try:
                dfb = retry_api(pro.stock_basic)(exchange="", list_status="L", fields="ts_code")
                codes = dfb["ts_code"].astype(str).tolist() if dfb is not None and not dfb.empty else []
            except Exception:
                codes = []
        end8 = _now_bj_naive().strftime("%Y%m%d")
        start8 = (_now_bj_naive() - timedelta(days=max(1, int(years)) * 370)).strftime("%Y%m%d")
        return _fetch_financial_report_snapshot(codes, start8, end8, status_callback=status_callback)
    except Exception as e:
        logging.warning("sync_financial_reports_recent 失败: %s", e, exc_info=True)
        return pd.DataFrame()


def _attach_limit_list_features(df: pd.DataFrame, codes, td: str, _dt_merge: str) -> pd.DataFrame:
    """
    科室6：连板高度 limit_times、涨停强度 strth（万元口径，与策略层 fund_mv_utils 对齐）。
    - 优先 limit_list_d(limit_type=U)：`limit_times` + `fd_amount`(元)→`strth`(万)
    - 回退 pro.limit_list：兼容历史列名别名
    """
    if df is None or df.empty or not codes:
        return df

    limit_df = pd.DataFrame()
    api_label = ""

    if pro is not None and hasattr(pro, "limit_list_d"):
        try:
            limit_df = retry_api(pro.limit_list_d)(trade_date=td, limit_type="U")
            api_label = "limit_list_d(U)"
        except DataFetchCriticalError:
            raise
        except Exception as e:
            logging.warning("【limit_list】limit_list_d 失败，将回退 limit_list: %s", e)
            limit_df = pd.DataFrame()

    if limit_df is None or limit_df.empty:
        try:
            limit_df = retry_api(pro.limit_list)(trade_date=td)
            api_label = "limit_list"
        except DataFetchCriticalError:
            raise
        except Exception as e:
            logging.debug("【limit_list】limit_list 拉取失败: %s", e)
            return df

    if limit_df.empty:
        return df

    if "ts_code" not in limit_df.columns:
        logging.warning("【limit_list】%s 返回无 ts_code 列，跳过合并", api_label or "unknown")
        return df

    work = limit_df.copy()
    work["ts_code"] = _normalize_ts_code_series(work["ts_code"])

    # 旧版别名：连板 / 封单强度
    rename_map = {
        "lu_times": "limit_times",
        "continuous_times": "limit_times",
        "l_times": "limit_times",
        "limit_up_times": "limit_times",
        "seal_amount": "strth",
        "fd_amt": "strth",
        "first_time_amount": "strth",
    }
    for old, new in rename_map.items():
        if old in work.columns and new not in work.columns:
            work = work.rename(columns={old: new})

    if "limit_times" in work.columns:
        lt = pd.to_numeric(work["limit_times"], errors="coerce").fillna(0)
    else:
        lt = pd.Series(0.0, index=work.index)

    if "strth" in work.columns:
        st = pd.to_numeric(work["strth"], errors="coerce").fillna(0)
    elif "fd_amount" in work.columns:
        # Tushare：fd_amount 为元 → 策略 strth 为万元
        st = pd.to_numeric(work["fd_amount"], errors="coerce").fillna(0) / 10000.0
    elif "limit_amount" in work.columns:
        st = pd.to_numeric(work["limit_amount"], errors="coerce").fillna(0) / 10000.0
    elif "amount" in work.columns and api_label.startswith("limit_list_d"):
        st = pd.to_numeric(work["amount"], errors="coerce").fillna(0) / 10000.0
    else:
        st = pd.Series(0.0, index=work.index)

    agg = (
        pd.DataFrame({"ts_code": work["ts_code"].astype(str), "limit_times": lt, "strth": st})
        .groupby("ts_code", as_index=False)
        .agg({"limit_times": "max", "strth": "max"})
    )
    agg["trade_date"] = _dt_merge
    code_set = {_normalize_ts_code_val(c) for c in codes}
    agg = agg[agg["ts_code"].isin(code_set)]
    logging.info("【limit_list】%s 原始=%s条 合并涨停=%s只", api_label, len(work), len(agg))
    return pd.merge(df, agg, on=["ts_code", "trade_date"], how="left")


# ==================== 1) 原始数据抓取：生肉提取器（不做滚动指标） ====================
def fetch_raw_day_data(date_str):
    if pro is None:
        raise_data_fetch_critical("fetch_raw_day_data：Tushare 未初始化")

    td = _api_trade_date_str(date_str)

    df_basic = _fetch_daily_basic_for_date(td)
    if df_basic.empty:
        return pd.DataFrame()
    df_basic = df_basic.rename(columns={'volume_ratio': 'vol_ratio'})

    p0_set = get_p0_codes()
    df_basic['ts_code'] = _normalize_ts_code_series(df_basic['ts_code'])
    # 市值/流通市值单位：万元；必须数值化，否则全 NaN 会「过滤后 0 只」导致生肉静默为空
    for _mv_col in ('total_mv', 'circ_mv'):
        if _mv_col in df_basic.columns:
            df_basic[_mv_col] = pd.to_numeric(df_basic[_mv_col], errors='coerce')
    _mv_sync_min = float(getattr(constants, "DAILY_BASIC_MIN_MV_WAN", 1_000_000))
    mv_ok = df_basic['total_mv'] >= _mv_sync_min
    if 'circ_mv' in df_basic.columns:
        mv_ok = mv_ok | (df_basic['circ_mv'] >= _mv_sync_min)
    mv_mask = mv_ok | (df_basic['ts_code'].isin(p0_set))
    n_before = len(df_basic)
    df_basic = df_basic[mv_mask].copy()
    if df_basic.empty and n_before > 0:
        logging.warning(
            "【市值过滤】%s 过滤前 %s 行，过滤后 0 只（total_mv/circ_mv 可能全为 NaN）。本次放宽为保留原始候选集。",
            td,
            n_before,
        )
        df_basic = retry_api(pro.daily_basic)(
            trade_date=td,
            fields='ts_code,pe_ttm,pb,ps_ttm,dv_ratio,total_mv,circ_mv,turnover_rate_f,volume_ratio',
        )
        if df_basic is None or df_basic.empty:
            return pd.DataFrame()
        df_basic = df_basic.rename(columns={'volume_ratio': 'vol_ratio'})
        df_basic['ts_code'] = _normalize_ts_code_series(df_basic['ts_code'])
        for _mv_col in ('total_mv', 'circ_mv'):
            if _mv_col in df_basic.columns:
                df_basic[_mv_col] = pd.to_numeric(df_basic[_mv_col], errors='coerce')

    codes = df_basic['ts_code'].unique().tolist()
    if not codes:
        return pd.DataFrame()

    # 【断点续传】仅下载库中尚不存在 (ts_code, trade_date) 的标的
    already = _existing_ts_codes_for_trade_date(td)
    need_codes = [c for c in codes if str(c) not in already]
    if not need_codes:
        logging.info("【断点续传】%s 共 %s 只候选已在库，跳过重复下载", td, len(codes))
        return pd.DataFrame()
    if len(need_codes) < len(codes):
        logging.info(
            "【断点续传】%s 需补票 %s / %s 只，仅拉取缺失代码",
            td,
            len(need_codes),
            len(codes),
        )
    codes = need_codes
    df_basic = df_basic[df_basic['ts_code'].isin(codes)].copy()

    df_daily = fetch_chunked(pro.daily, codes, td, chunk_size=800)
    if df_daily.empty:
        return pd.DataFrame()
    if 'ts_code' in df_daily.columns:
        df_daily['ts_code'] = _normalize_ts_code_series(df_daily['ts_code'])
    if 'trade_date' not in df_daily.columns:
        df_daily['trade_date'] = td

    # 统一主键列
    df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')
    df_basic['trade_date'] = _trade_date_for_sql(td)

    # 量价主表：daily + daily_basic（严格切片，避免误删 ps_ttm/dv_ratio）
    basic_keep = ['ts_code', 'trade_date', 'pe_ttm', 'pb', 'ps_ttm', 'dv_ratio', 'total_mv', 'circ_mv', 'turnover_rate_f', 'vol_ratio']
    basic_keep = [c for c in basic_keep if c in df_basic.columns]
    df_basic = df_basic[basic_keep].copy()
    df = pd.merge(df_daily, df_basic, on=['ts_code', 'trade_date'], how='left')

    _dt_merge = _trade_date_for_sql(td)

    # 【V26.6 优化】高阶接口并行拉取（cyq_perf / hk_hold / margin_detail / top_inst / forecast / limit_list）
    # 以下 6 个 API 之间无数据依赖（均只依赖 codes 和 td），
    # 原来串行执行时每增一个接口增加约 0.5–1.5 秒延迟，
    # 并行后总耗时接近最慢单个接口的耗时（节省 5~8 秒/次）。
    # 注意：adj_factor 需保证在主表 join 后 left merge，故单独串行；
    # limit_list 依赖已合并的主表 df，故需在主表就绪后单独执行；
    # 其余 4 个（cyq / hk / margin / top_inst / forecast）可完全并行。
    def _safe_fetch_df(callable_fn, **kwargs):
        """安全包装 API 调用，失败时返回空 DataFrame，避免并行组内单个失败影响全局。"""
        try:
            result = callable_fn(**kwargs)
            if result is None:
                return pd.DataFrame()
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    with ThreadPoolExecutor(max_workers=5) as _par_ex:
        _fut_cyq = _par_ex.submit(_safe_fetch_df, fetch_prefer_trade_date, api_func=pro.cyq_perf, codes=codes, date_str=td, chunk_size=500)
        _fut_hk = _par_ex.submit(_safe_fetch_df, fetch_prefer_trade_date, api_func=pro.hk_hold, codes=codes, date_str=td, chunk_size=500)
        _fut_margin = _par_ex.submit(_safe_fetch_df, fetch_prefer_trade_date, api_func=pro.margin_detail, codes=codes, date_str=td, chunk_size=500)
        _fut_inst = _par_ex.submit(_safe_fetch_df, retry_api(pro.top_inst), trade_date=td)
        # forecast 不支持 trade_date；改用近30日内任意公告日回溯（取最新财报季公告窗口）
        _end = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=8)).strftime("%Y%m%d")
        _start = (datetime.now(timezone(timedelta(hours=8))) - timedelta(days=30)).strftime("%Y%m%d")
        _fut_fc = _par_ex.submit(_safe_fetch_df, retry_api(pro.forecast), ann_date=_end, start_date=_start)

        # 统一等待结果
        cyq_df, hk_df, margin_df, inst_df, fc_df = (
            _fut_cyq.result(),
            _fut_hk.result(),
            _fut_margin.result(),
            _fut_inst.result(),
            _fut_fc.result(),
        )

    # 前复权因子（必须串行，在主表 join 后 left merge）
    adj_df = retry_api(pro.adj_factor)(trade_date=td)
    if not adj_df.empty:
        if 'ts_code' in adj_df.columns:
            adj_df['ts_code'] = _normalize_ts_code_series(adj_df['ts_code'])
        adj_df['trade_date'] = _dt_merge
        adj_df = adj_df[adj_df['ts_code'].isin(codes)].drop_duplicates('ts_code')[['ts_code', 'adj_factor']]
        adj_df['trade_date'] = _dt_merge
        df = pd.merge(df, adj_df, on=['ts_code', 'trade_date'], how='left')

    # 筹码分布浓度（并行结果）
    logging.info(f"获取cyq_perf数据: {len(cyq_df)}条")
    if not cyq_df.empty:
        if 'ts_code' in cyq_df.columns:
            cyq_df['ts_code'] = _normalize_ts_code_series(cyq_df['ts_code'])
        rename_map = {'cost_5pct': 'cost_5th', 'cost_50pct': 'cost_50th', 'cost_95pct': 'cost_95th', 'weight_avg': 'avg_cost'}
        cyq_df = cyq_df.rename(columns={k: v for k, v in rename_map.items() if k in cyq_df.columns}).drop_duplicates('ts_code')
        if {'cost_95th', 'cost_5th'}.issubset(set(cyq_df.columns)):
            cyq_df['cyq_concentration'] = np.where(
                (cyq_df['cost_95th'] + cyq_df['cost_5th']) > 0,
                (cyq_df['cost_95th'] - cyq_df['cost_5th']) / (cyq_df['cost_95th'] + cyq_df['cost_5th']) * 100.0, 0.0
            )
        keep = ['ts_code', 'cost_5th', 'cost_50th', 'cost_95th', 'avg_cost', 'winner_rate', 'cyq_concentration']
        keep = [c for c in keep if c in cyq_df.columns]
        cyq_df = cyq_df[keep].copy()
        cyq_df['trade_date'] = _dt_merge
        df = pd.merge(df, cyq_df, on=['ts_code', 'trade_date'], how='left')

    # 北向资金（并行结果）
    logging.info(f"获取hk_hold数据: {len(hk_df)}条")
    if not hk_df.empty and 'vol' in hk_df.columns:
        if 'ts_code' in hk_df.columns:
            hk_df['ts_code'] = _normalize_ts_code_series(hk_df['ts_code'])
        hk_df = hk_df.drop_duplicates('ts_code')[['ts_code', 'vol']].rename(columns={'vol': 'hk_vol'})
        hk_df['trade_date'] = _dt_merge
        df = pd.merge(df, hk_df, on=['ts_code', 'trade_date'], how='left')

    # 两融数据（并行结果）
    logging.info(f"获取margin_detail数据: {len(margin_df)}条")
    if not margin_df.empty and {'rzmre', 'rzche'}.issubset(set(margin_df.columns)):
        if 'ts_code' in margin_df.columns:
            margin_df['ts_code'] = _normalize_ts_code_series(margin_df['ts_code'])
        margin_df = margin_df.drop_duplicates('ts_code')
        margin_df['rz_net_buy'] = margin_df['rzmre'] - margin_df['rzche']
        # rzmre：融资买入额（元），供两融「近 5 日环比」复合分使用；与 rz_net_buy 一并落生肉宽表
        margin_df = margin_df[['ts_code', 'rz_net_buy', 'rzmre']].copy()
        margin_df['trade_date'] = _dt_merge
        df = pd.merge(df, margin_df, on=['ts_code', 'trade_date'], how='left')

    # 大单资金
    mf_df = fetch_chunked(pro.moneyflow, codes, td, chunk_size=800)
    if not mf_df.empty and {'buy_elg_amount', 'sell_elg_amount', 'buy_lg_amount', 'sell_lg_amount'}.issubset(set(mf_df.columns)):
        if 'ts_code' in mf_df.columns:
            mf_df['ts_code'] = _normalize_ts_code_series(mf_df['ts_code'])
        mf_df = mf_df.drop_duplicates('ts_code')
        mf_df['net_elg_amount'] = (mf_df['buy_elg_amount'] - mf_df['sell_elg_amount']) * 10000.0
        mf_df['net_main_amount'] = ((mf_df['buy_elg_amount'] + mf_df['buy_lg_amount']) - (mf_df['sell_elg_amount'] + mf_df['sell_lg_amount'])) * 10000.0
        mf_df = mf_df[['ts_code', 'net_elg_amount', 'net_main_amount']].copy()
        mf_df['trade_date'] = _dt_merge
        df = pd.merge(df, mf_df, on=['ts_code', 'trade_date'], how='left')

    # 机构龙虎榜（并行结果）
    if not inst_df.empty and 'net_buy' in inst_df.columns:
        if 'ts_code' in inst_df.columns:
            inst_df['ts_code'] = _normalize_ts_code_series(inst_df['ts_code'])
        inst_df['inst_net_buy'] = inst_df['net_buy'] * 10000.0
        inst_df = inst_df.groupby('ts_code', as_index=False)['inst_net_buy'].sum()
        inst_df['trade_date'] = _dt_merge
        inst_df = inst_df[inst_df['ts_code'].isin(codes)]
        df = pd.merge(df, inst_df, on=['ts_code', 'trade_date'], how='left')

    # 涨停队列：连板高度 / 强度（_attach_limit_list_features 内部自行拉取）
    df = _attach_limit_list_features(df, codes, td, _dt_merge)

    # 业绩预告映射（并行结果）
    if not fc_df.empty and 'type' in fc_df.columns:
        if 'ts_code' in fc_df.columns:
            fc_df['ts_code'] = _normalize_ts_code_series(fc_df['ts_code'])
        fc_df['forecast_type'] = fc_df['type'].apply(_map_forecast_type)
        fc_df = fc_df[['ts_code', 'forecast_type']].drop_duplicates('ts_code').copy()
        # 业绩预告以公告日发布，存在周末错位；强制对齐到当前交易日
        fc_df['trade_date'] = _dt_merge
        fc_df = fc_df[fc_df['ts_code'].isin(codes)]
        df = pd.merge(df, fc_df, on=['ts_code', 'trade_date'], how='left')

    # 时间统一
    df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')
    return df.drop_duplicates(subset=['ts_code', 'trade_date'])


# ==================== 2) 指标补全与 59 列强对齐 ====================
def calc_indicators_in_memory(df):
    if df is None or df.empty:
        final_cols = ['ts_code', 'trade_date'] + ALL_55_COLS
        return pd.DataFrame(columns=final_cols)

    df = df.copy()
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values(['ts_code', 'trade_date'])

    # 两融融资买入额：历史库可能无此列，缺省按 0（全向量路径安全）
    if 'rzmre' not in df.columns:
        df['rzmre'] = 0.0
    else:
        df['rzmre'] = pd.to_numeric(df['rzmre'], errors='coerce').fillna(0.0)

    # ---------- 资金共振复合分（全向量化；在 MACD 分组循环之前写入，随切片原样保留）----------
    crs_series = compute_capital_resonance_score(df)
    if not isinstance(crs_series, pd.Series):
        crs_series = pd.Series([crs_series] * len(df), index=df.index)
    else:
        crs_series = crs_series.reindex(df.index)
    df['capital_resonance_score'] = pd.to_numeric(crs_series, errors='coerce').fillna(0.0).astype('float64')

    # 基础兜底列
    for c in ['open', 'high', 'low', 'close', 'pre_close', 'vol']:
        if c not in df.columns:
            df[c] = 0.0

    # 价格均线
    for n in [5, 10, 20, 60, 120, 250]:
        df[f'ma{n}'] = df.groupby('ts_code')['close'].transform(lambda s: s.rolling(n, min_periods=1).mean())

    # 专属量能均线（强制新增）
    df['vol_ma5'] = df.groupby('ts_code')['vol'].transform(lambda s: s.rolling(5, min_periods=1).mean())
    df['vol_ma10'] = df.groupby('ts_code')['vol'].transform(lambda s: s.rolling(10, min_periods=1).mean())
    df['vol_ma20'] = df.groupby('ts_code')['vol'].transform(lambda s: s.rolling(20, min_periods=1).mean())

    # ---------- 资金活跃度记忆（依赖 vol_ma20 / vol_ratio / 涨停字段；0~200，见模块内长注释）----------
    cam_series = compute_fund_memory_score(df)
    if not isinstance(cam_series, pd.Series):
        cam_series = pd.Series([cam_series] * len(df), index=df.index)
    else:
        cam_series = cam_series.reindex(df.index)
    df["fund_memory_score"] = pd.to_numeric(cam_series, errors="coerce").fillna(0.0).astype("float64")

    # ma20斜率 / 区间极值 / 乖离率
    ma20_prev5 = df.groupby('ts_code')['ma20'].shift(5)
    df['ma20_slope_5'] = np.where(ma20_prev5 > 0, (df['ma20'] - ma20_prev5) / ma20_prev5 * 100.0, 0.0)
    df['high_20'] = df.groupby('ts_code')['high'].transform(lambda s: s.rolling(20, min_periods=1).max())
    df['low_60'] = df.groupby('ts_code')['low'].transform(lambda s: s.rolling(60, min_periods=1).min())
    df['bias_20'] = np.where(df['ma20'] > 0, (df['close'] - df['ma20']) / df['ma20'] * 100.0, 0.0)

    # pct_chg 兜底
    if 'pct_chg' not in df.columns:
        df['pct_chg'] = np.where(df['pre_close'] > 0, (df['close'] - df['pre_close']) / df['pre_close'] * 100.0, 0.0)

    # ================= 真实技术指标（纯 pandas + numpy，禁止 TA-Lib）=================
    def _calc_group_ind(g: pd.DataFrame) -> pd.DataFrame:
        """单只股票时间序列指标；调用方保证 g 含 ts_code、trade_date。"""
        g = g.sort_values("trade_date").copy()
        close = g['close'].astype(float)
        high = g['high'].astype(float)
        low = g['low'].astype(float)
        pre_close = g['pre_close'].astype(float)

        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        hist = dif - dea
        g['macd'] = dif
        g['macd_signal'] = dea
        g['macd_hist'] = hist

        # RSI14（Wilder）
        delta = close.diff()
        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        g['rsi_14'] = (100 - (100 / (1 + rs))).fillna(50.0)

        # KDJ(9,3,3)
        llv = low.rolling(9, min_periods=1).min()
        hhv = high.rolling(9, min_periods=1).max()
        rsv = np.where((hhv - llv) > 0, (close - llv) / (hhv - llv) * 100, 50.0)
        k = pd.Series(rsv, index=g.index).ewm(alpha=1/3, adjust=False).mean()
        d = k.ewm(alpha=1/3, adjust=False).mean()
        g['kdj_k'] = k
        g['kdj_d'] = d

        # Bollinger(20,2)
        mid = close.rolling(20, min_periods=1).mean()
        std = close.rolling(20, min_periods=1).std(ddof=0).fillna(0.0)
        g['boll_upper'] = mid + 2 * std
        g['boll_lower'] = mid - 2 * std

        # CCI(14)
        tp = (high + low + close) / 3.0
        tp_ma = tp.rolling(14, min_periods=1).mean()
        md = (tp - tp_ma).abs().rolling(14, min_periods=1).mean()
        g['cci'] = np.where(md > 0, (tp - tp_ma) / (0.015 * md), 0.0)

        # ATR% (14)
        tr1 = high - low
        tr2 = (high - pre_close).abs()
        tr3 = (low - pre_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr14 = tr.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        g['atr_pct'] = np.where(close > 0, atr14 / close * 100.0, 0.0)
        return g

    # 不用 groupby.apply：新版 pandas 下 apply 可能返回空表或破坏索引；按组循环并显式写入 ts_code。
    _chunks = []
    for _code, _g in df.groupby("ts_code", sort=False):
        _g = _g.copy()
        _g["ts_code"] = str(_code)
        _chunks.append(_calc_group_ind(_g))
    if not _chunks:
        final_cols = ["ts_code", "trade_date"] + ALL_55_COLS
        return pd.DataFrame(columns=final_cols)
    df = pd.concat(_chunks, ignore_index=True)

    # nineturn_signal 兜底（后续可替换为完整九转逻辑）
    if 'nineturn_signal' not in df.columns:
        df['nineturn_signal'] = 0.0

    # 强制 59 列出厂：['ts_code','trade_date'] + 57 维特征
    final_cols = ['ts_code', 'trade_date'] + ALL_55_COLS
    df = df.reindex(columns=final_cols)
    # 主键列禁止 fillna(0.0)：否则空值会被写成 '0.0' / 'nan'，造成全库主键坍缩
    if "ts_code" in df.columns:
        df["ts_code"] = df["ts_code"].astype(str).str.strip()
        df = df[df["ts_code"].notna() & (df["ts_code"] != "") & (df["ts_code"].str.lower() != "nan")]
    if "trade_date" in df.columns:
        td = pd.to_datetime(df["trade_date"], errors="coerce")
        keep = td.notna()
        df = df.loc[keep].copy()
        df["trade_date"] = td.loc[keep].dt.strftime("%Y-%m-%d")
    # 数值列统一用 0.0 兜底
    num_cols = [c for c in ALL_55_COLS if c in df.columns]
    if num_cols:
        df[num_cols] = df[num_cols].fillna(0.0)
    return df


def _core_pipeline(date_str, status_callback=print):
    """
    核心生肉提取器：
    - 仅负责抓取某个交易日原始数据，不在这里计算滚动指标。
    - 返回 raw_df 给上层聚合后统一重算，避免单日计算导致均线失真。
    """
    d8 = _api_trade_date_str(date_str)
    try:
        raw_df = fetch_raw_day_data(d8)
    except DataFetchCriticalError:
        # 熔断级错误：必须上浮，禁止当「本日跳过」吞掉
        raise
    except Exception as e:
        logging.exception("【平滑降级】%s 生肉提取异常，已跳过本日", d8)
        status_callback(f"⚠️ {d8} 抓取失败已跳过: {e}")
        return pd.DataFrame()
    if raw_df.empty:
        status_callback(
            f"⚠️ {d8} 生肉为空（daily_basic 无数据/限速/市值过滤后无标的/本地已齐）。请查看控制台 [daily_basic] 日志。"
        )
        return pd.DataFrame()
    status_callback(f"✅ {d8} 生肉提取完成: {len(raw_df)} 只")
    return raw_df


def _load_existing_daily_data():
    con = get_conn()
    try:
        tables = con.execute("SHOW TABLES").fetchdf()
        has_daily = 'daily_data' in tables['name'].tolist() if not tables.empty else False
        if not has_daily:
            return pd.DataFrame(columns=['ts_code', 'trade_date'] + ALL_55_COLS)
        df_old = con.execute("SELECT * FROM daily_data").fetchdf()
        if df_old is None or df_old.empty:
            return pd.DataFrame(columns=['ts_code', 'trade_date'] + ALL_55_COLS)
        df_old = df_old.reindex(columns=['ts_code', 'trade_date'] + ALL_55_COLS).fillna(0.0)
        return df_old
    except Exception as e:
        logging.warning(f"读取历史 daily_data 失败，按空库处理: {e}")
        return pd.DataFrame(columns=['ts_code', 'trade_date'] + ALL_55_COLS)


def _normalize_daily_merge_keys(df):
    """
    与 db_core.save_df_to_sql 日线 trade_date 规则对齐，避免库内读出的日期与接口生肉
    （int/str/datetime）混用导致 concat 后 drop_duplicates 漏网、重复主键或库文件膨胀。
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "ts_code" in out.columns:
        out["ts_code"] = out["ts_code"].astype(str).str.strip()
    if "trade_date" in out.columns:
        out["trade_date"] = pd.to_datetime(
            out["trade_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8],
            format="%Y%m%d",
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")
        out = out[out["trade_date"].notna()]
    return out


def _merge_daily_full(df_old, raw_frames):
    """合并历史 + 多段生肉，主键规范化后去重（keep last）。"""
    df_old = _normalize_daily_merge_keys(df_old)
    frames = []
    if df_old is not None and not df_old.empty:
        frames.append(df_old)
    for rf in raw_frames:
        if rf is not None and not rf.empty:
            frames.append(_normalize_daily_merge_keys(rf))
    if not frames:
        return pd.DataFrame(columns=["ts_code", "trade_date"] + ALL_55_COLS)
    if len(frames) == 1:
        full_df = frames[0]
    else:
        full_df = pd.concat(frames, ignore_index=True)
    full_df = _normalize_daily_merge_keys(full_df)
    full_df = full_df.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return full_df


def _drop_daily_data_rebuild_backup_tables(con) -> int:
    """
    重铸时旧 daily_data 会改名为 daily_data__backup_<时间戳>，体积与日线主表相当；
    新表已切换为 daily_data 后若保留备份，库文件约翻倍。此处删除所有此类备份表。
    返回删除数量，供日志与维护判断。
    """
    rows = None
    for sql in (
        "SELECT database_name, schema_name, table_name FROM duckdb_tables() WHERE table_name LIKE 'daily_data__backup_%'",
        "SELECT database_name, schema_name, name AS table_name FROM duckdb_tables() WHERE name LIKE 'daily_data__backup_%'",
    ):
        try:
            rows = con.execute(sql).fetchall()
            if rows is not None:
                break
        except Exception:
            rows = None
    if not rows:
        return 0
    deleted = 0
    for db, sch, tbl in rows:
        try:
            tid = f'"{db}"."{sch}"."{tbl}"'
            con.execute(f"DROP TABLE {tid}")
            deleted += 1
            logging.info("已删除重铸备份表以回收空间: %s", tbl)
        except Exception as e:
            logging.warning("删除重铸备份表失败 %s: %s", tbl, e)
    return deleted


def _compact_duckdb_after_rebuild(status_callback=print, max_attempts: int = 3) -> None:
    """
    重铸后立即做空间整理，抑制「单日补齐后物理体积翻倍」：

    安全整理流程（按序执行，全部使用 DuckDB 官方 SQL，不丢失任何数据）：
    1) CHECKPOINT          强制 WAL 落盘合并
    2) ANALYZE            更新表统计信息（帮助 VACUUM 更精准压缩）
    3) VACUUM             回收列式存储碎片（依赖 ANALYZE 提供统计信息）
    4) CHECKPOINT         VACUUM 后再次强制落盘
    5) ANALYZE            第二轮统计更新
    6) VACUUM             第二轮碎片回收（基于更精准统计）
    7) CHECKPOINT         最终落盘

    若整理后体积仍 > 1.3x（宽松阈值）：
    → 再执行一轮 ANALYZE + VACUUM + CHECKPOINT（第三轮深度整理）
    → 记录警告日志但不中断业务，不删除任何数据

    异常处理：任何步骤失败均记录日志并继续，不中断业务流程。
    与 duckdb_vacuum_silent 的区别：此处已持有写连接，传入 status_callback；
    duckdb_vacuum_silent 用于独立维护进程（需要重新获取连接）。
    """
    snap_before = duckdb_storage_snapshot()
    before = int(snap_before.get("db_bytes", 0) or 0)
    status_callback(f"📦 重铸前存储：{duckdb_storage_snapshot_text()}")

    def _analyze_vacuum_checkpoint(con, label: str = "") -> bool:
        """ANALYZE + VACUUM + CHECKPOINT 组合执行，帮助 VACUUM 更好压缩列式存储"""
        try:
            con.execute("ANALYZE")
            con.execute("VACUUM")
            duckdb_checkpoint(force=True)
            return True
        except Exception as e:
            logging.warning("ANALYZE+VACUUM 步骤异常（不阻断）%s: %s", label, e)
            return False

    def _one_compact_pass(con, attempt: int) -> bool:
        """执行一轮完整的 CHECKPOINT + ANALYZE + VACUUM + CHECKPOINT"""
        try:
            duckdb_checkpoint(force=True)
            _analyze_vacuum_checkpoint(con, f"(第{attempt}轮)")
            return True
        except Exception as e:
            logging.warning("重铸后空间整理第 %s 轮异常: %s", attempt, e)
            return False

    # 第一轮完整整理
    for attempt in range(1, max_attempts + 1):
        try:
            con = get_conn()
            if _one_compact_pass(con, attempt):
                break
        except Exception as e:
            logging.warning("重铸后空间整理第 %s/%s 轮异常: %s", attempt, max_attempts, e)
            if attempt == max_attempts:
                status_callback(f"⚠️ 重铸后空间整理(VACUUM)未完成: {e}")
                return

    # 第二轮：ANALYZE 帮助压缩后再次 VACUUM
    try:
        con = get_conn()
        _analyze_vacuum_checkpoint(con, "(第二轮)")
    except Exception as e:
        logging.warning("第二轮 ANALYZE+VACUUM 异常: %s", e)

    snap_after = duckdb_storage_snapshot()
    after = int(snap_after.get("db_bytes", 0) or 0)
    ratio = (after / before) if before > 0 else 0.0
    status_callback(
        f"🧹 重铸后空间整理完成（ANALYZE+VACUUM+CHECKPOINT 多轮）："
        f"{before / 1024 / 1024:.2f}MB -> {after / 1024 / 1024:.2f}MB "
        f"(膨胀率 {ratio:.2f}x) | {duckdb_storage_snapshot_text()}"
    )

    # 若膨胀率仍 > 1.3x，执行第三轮深度 ANALYZE+VACUUM+CHECKPOINT
    if before > 0 and after > int(before * 1.3):
        logging.warning(
            "重铸后库体积膨胀 %.2fx（before=%.2fMB after=%.2fMB），执行第三轮深度 ANALYZE+VACUUM",
            ratio, before / 1024 / 1024, after / 1024 / 1024,
        )
        status_callback(f"⚠️ 库体积膨胀 {ratio:.2f}x，执行第三轮深度整理...")
        try:
            con = get_conn()
            for _depth_round in range(3):
                if _analyze_vacuum_checkpoint(con, f"(深度{_depth_round + 1})"):
                    break
            snap_final = duckdb_storage_snapshot()
            final_mb = float(snap_final.get("db_bytes", 0) or 0) / 1024 / 1024
            final_ratio = final_mb / (before / 1024 / 1024) if before > 0 else 0.0
            status_callback(
                f"🧹 第三轮深度整理后: {final_mb:.2f}MB (膨胀率 {final_ratio:.2f}x)"
            )
        except Exception as e:
            logging.warning("第三轮深度整理异常: %s", e)

    # 软保护告警：膨胀率 > 1.5x（比之前 1.6x 更严格，但仍保留安全余量）
    try:
        if before > 0 and after > int(before * 1.5):
            _notify_data_sync_alert(
                "数据库体积异常放大（已自动维护）",
                (
                    "本次重铸后已执行多轮 ANALYZE+VACUUM+CHECKPOINT，库体积仍显著增长。\n"
                    f"before={before / 1024 / 1024:.2f}MB, after={after / 1024 / 1024:.2f}MB "
                    f"(膨胀率 {ratio:.2f}x)。\n"
                    f"storage={duckdb_storage_snapshot_text()}\n"
                    "已保持业务继续运行（未做离线替换/删数据）。建议在低峰期观察下一次维护是否回落。"
                ),
                dedup_key="data_sync_db_growth_after_rebuild",
            )
    except Exception:
        pass


def _rebuild_daily_table_from_full_df(full_df, status_callback=print):
    """
    全量重算后重铸落库：
    1) 计算全部指标
    2) DROP TABLE daily_data
    3) 全量写回（59 列固定）
    """
    if full_df is None or full_df.empty:
        logging.error("❌ 全量重铸收到空 full_df，已跳过落库")
        status_callback("❌ 合并数据为空，无法重铸 daily_data（请检查生肉与历史合并）。")
        return pd.DataFrame()

    logging.info(f"全量 full_df 形状: {full_df.shape}，准备开始计算指标...")
    logging.info("⏳ 开始执行 57 维指标的全量向量化计算，请耐心等待...")
    out_df = calc_indicators_in_memory(full_df)
    if out_df is None or out_df.empty:
        logging.error(
            "❌ 指标重算后 out_df 为空，合并行数=%s；已中止写入，避免误报「表不存在」",
            len(full_df),
        )
        status_callback(
            "❌ 指标重算结果为空，无法写入 daily_data。若已显示「生肉提取完成」，请尝试升级 pandas 或检查控制台。"
        )
        return pd.DataFrame()
    # 写库前做“主键健康检查”，防止一次同步把全库主键写成同一个 ts_code
    try:
        n_rows = int(len(out_df))
        n_codes = int(out_df["ts_code"].nunique()) if ("ts_code" in out_df.columns and n_rows > 0) else 0
        if n_rows > 0 and n_codes <= 1:
            raise RuntimeError(f"重铸产物异常：ts_code 去重={n_codes}，将拒绝覆盖 daily_data（疑似主键丢失/污染）")
    except Exception as e:
        logging.error("❌ daily_data 重铸验数失败，已中止覆盖以保护旧库: %s", e)
        status_callback(f"❌ 重铸验数失败，已中止覆盖以保护旧库: {e}")
        return pd.DataFrame()

    con = get_conn()
    snap_before = duckdb_storage_snapshot()
    status_callback(f"📦 重铸前存储快照：{duckdb_storage_snapshot_text()}")
    # 最稳妥落库：先写入新表，再验数后原子替换（旧表保留为备份）
    new_table = "daily_data__rebuild_new"
    backup_table = f"daily_data__backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    duckdb_drop_table_if_exists_resolved(con, new_table)
    save_df_to_sql(out_df, new_table)
    # 强制 checkpoint：近期/全量重铸 daily_data 属于大事务
    duckdb_checkpoint(force=True)

    if not table_exists(new_table):
        logging.error("❌ save_df_to_sql 后仍检测不到表 %s（写库失败或仍为空）", new_table)
        status_callback("❌ 新表 daily_data__rebuild_new 未创建，请检查 DuckDB 锁与磁盘空间。")
        return pd.DataFrame()

    try:
        qnew = duckdb_resolve_table_sql_id(con, new_table)
        new_rows = int(con.execute(f"SELECT COUNT(*) FROM {qnew}").fetchone()[0] or 0)
        new_codes = int(
            con.execute(f"SELECT COUNT(DISTINCT ts_code) FROM {qnew}").fetchone()[0] or 0
        )
        if new_rows <= 0 or new_codes <= 1:
            raise RuntimeError(f"新表验数失败：rows={new_rows}, codes={new_codes}")

        if table_exists("daily_data"):
            qdaily = duckdb_resolve_table_sql_id(con, "daily_data")
            con.execute(f'ALTER TABLE {qdaily} RENAME TO "{backup_table}"')
        con.execute(f'ALTER TABLE {qnew} RENAME TO "daily_data"')
        # 旧库已改名为 daily_data__backup_*，与主表同量级；必须删除以免库文件约翻倍。
        deleted = _drop_daily_data_rebuild_backup_tables(con)
        duckdb_checkpoint(force=True)
        status_callback(f"🧹 重铸替换完成，已清理备份表 {deleted} 个。")
    except Exception as e:
        logging.error("❌ daily_data 原子替换失败，将保留旧表/备份表: %s", e)
        status_callback(f"❌ daily_data 替换失败：{e}")
        try:
            duckdb_drop_table_if_exists_resolved(con, new_table)
        except Exception:
            pass
        return pd.DataFrame()
    try:
        total = con.execute("SELECT COUNT(*) FROM daily_data").fetchone()[0]
        distinct = con.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT ts_code, trade_date FROM daily_data) t"
        ).fetchone()[0]
        if total != distinct:
            logging.warning(
                "daily_data 存在重复 (ts_code, trade_date): 行数=%s 去重键=%s",
                total,
                distinct,
            )
    except Exception as e:
        logging.debug("daily_data 去重校验: %s", e)
    # 关键：daily_data 替换后先整理一次，得到“宽表阶段”体积；随后再同步 V26 分层表，避免日志口径混淆。
    _compact_duckdb_after_rebuild(status_callback=status_callback)
    snap_after_daily = duckdb_storage_snapshot()
    daily_db_mb = float(snap_after_daily.get("db_bytes", 0) or 0) / 1024 / 1024
    daily_total_mb = float(snap_after_daily.get("total_bytes", 0) or 0) / 1024 / 1024
    status_callback(
        f"✅ daily_data 重铸完成: {len(out_df)} 行 | 固定列数: {out_df.shape[1]} | "
        f"主库={daily_db_mb:.2f}MB | 总占用={daily_total_mb:.2f}MB | {duckdb_storage_snapshot_text()}"
    )
    try:
        # 兜底：确保兼容视图在新库上存在，避免重建后 inspector 只见分层表、看不到视图。
        from data.db_core import ensure_v26_tables, ensure_v26_compat_view, verify_v26_consistency
        ensure_v26_tables()
        ensure_v26_compat_view(force=True)
        _save_v26_layer_tables(out_df, status_callback=status_callback)
        duckdb_checkpoint(force=True)
        v26_check = verify_v26_consistency(sample_rows=200)
        v26_view_ok = bool(v26_check.get("coverage", {}).get("ok", False)) and bool(v26_check.get("ok", False))
        status_callback(
            f"🔎 最终自检：vw_daily_data_compat={'存在' if table_exists('vw_daily_data_compat') else '不存在'} | "
            f"V26闭环={'通过' if v26_view_ok else '未通过'} | "
            f"建议={'可停止测试' if v26_view_ok else '继续排查'}"
        )
    except Exception as e:
        logging.warning("重铸后 V26 兜底同步失败: %s", e)
    snap_after = duckdb_storage_snapshot()
    try:
        if int(snap_before.get("db_bytes", 0) or 0) > 0 and int(snap_after.get("db_bytes", 0) or 0) > int((snap_before.get("db_bytes", 0) or 0) * 1.6):
            _notify_data_sync_alert(
                "数据库体积异常放大（重铸后）",
                f"重铸后仍显著放大：before={snap_before.get('db_bytes', 0)}, after={snap_after.get('db_bytes', 0)}; {duckdb_storage_snapshot_text()}",
                dedup_key="data_sync_db_growth_after_rebuild_final",
            )
    except Exception:
        pass

    try:
        snap_final = duckdb_storage_snapshot()
        status_callback(
            "✅ V26 分层同步后最终存储："
            f"主库={float(snap_final.get('db_bytes', 0) or 0) / 1024 / 1024:.2f}MB | "
            f"总占用={float(snap_final.get('total_bytes', 0) or 0) / 1024 / 1024:.2f}MB | "
            f"raw/bars/core/capital/memory 已落盘"
        )
    except Exception:
        pass
    return out_df


def _sync_daily_features_capital_resonance(status_callback=print) -> bool:
    """
    增量特征修补（零 Tushare 流量）【V26.6 新增资金记忆体系】与 fund_memory 修补并列由 _sync_daily_features 调度。
    从本地 DuckDB 读取 daily_data 全表，仅向量化重算 capital_resonance_score（FLOAT），
    通过 UPDATE…FROM 写回原表主键行。

    典型触发：sync_recent_days 发现「近端交易日已在库」跳过下载时，仍可能因算法升级或历史列缺省
    需要刷新该列；本函数不拉取生肉、不重下全市场日线 API。
    """
    try:
        con = get_conn()
        if not table_exists("daily_data"):
            status_callback("【特征增量】daily_data 不存在，跳过 capital_resonance_score 修补。")
            return False
        try:
            pragma = con.execute("PRAGMA table_info('daily_data')").fetchdf()
            _pk = (
                "name"
                if pragma is not None and "name" in getattr(pragma, "columns", [])
                else (
                    "column_name"
                    if pragma is not None and "column_name" in getattr(pragma, "columns", [])
                    else None
                )
            )
            colnames = (
                set(pragma[_pk].astype(str).str.lower().tolist())
                if _pk and pragma is not None and not getattr(pragma, "empty", True)
                else set()
            )
        except Exception:
            colnames = set()
        if "capital_resonance_score" not in colnames:
            try:
                con.execute("ALTER TABLE daily_data ADD COLUMN capital_resonance_score DOUBLE")
                status_callback("【特征增量】已 ADD COLUMN capital_resonance_score")
            except Exception as _ae:
                logging.warning("ADD COLUMN capital_resonance_score: %s", _ae)

        df = con.execute("SELECT * FROM daily_data").fetchdf()
        if df is None or df.empty:
            status_callback("【特征增量】daily_data 为空，跳过。")
            return False

        work = _normalize_daily_merge_keys(df.copy())
        work = work.sort_values(["ts_code", "trade_date"])
        if "rzmre" not in work.columns:
            work["rzmre"] = 0.0
        else:
            work["rzmre"] = pd.to_numeric(work["rzmre"], errors="coerce").fillna(0.0)

        work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce")
        work = work.loc[work["trade_date"].notna()].copy()
        work["trade_date"] = work["trade_date"].dt.strftime("%Y-%m-%d")

        crs_series = compute_capital_resonance_score(work)
        if not isinstance(crs_series, pd.Series):
            # 单行样本时上游可能返回 numpy.float64 标量；统一升格为与 work 等长的 Series
            crs_series = pd.Series([crs_series] * len(work), index=work.index)
        patch = pd.DataFrame(
            {
                "ts_code": work["ts_code"].astype(str),
                "trade_date": work["trade_date"].astype(str),
                "capital_resonance_score": pd.to_numeric(crs_series, errors="coerce").fillna(0.0).astype("float64"),
            }
        )

        try:
            con.unregister("crs_patch")
        except Exception:
            pass
        con.register("crs_patch", patch)
        con.execute(
            """
            UPDATE daily_data AS d
            SET capital_resonance_score = CAST(p.capital_resonance_score AS DOUBLE)
            FROM crs_patch AS p
            WHERE CAST(d.ts_code AS VARCHAR) = CAST(p.ts_code AS VARCHAR)
              AND CAST(d.trade_date AS DATE) = CAST(p.trade_date AS DATE)
            """
        )
        try:
            con.unregister("crs_patch")
        except Exception:
            pass
        duckdb_checkpoint(force=True)
        status_callback(
            f"【特征增量】capital_resonance_score 已就地重算并写回（{len(patch)} 行补丁键）。"
        )
        return True
    except Exception as e:
        logging.exception("_sync_daily_features_capital_resonance: %s", e)
        status_callback(f"【特征增量】capital_resonance_score 修补失败: {e}")
        return False


def _sync_daily_features_fund_memory(status_callback=print) -> bool:
    """
    【股性记忆 fund_memory_score】DuckDB 增量修补（零 Tushare 流量）
    【V26.6 新增资金记忆体系】

    自然语言说明（与 fund_memory_score.py 顶部长注释一致，此处强调落库形态）：
    - 本列刻画大流通市值标的在「近 60 日有过放量痕迹」前提下，由涨停/天量事件驱动、
      以 21 个交易日为半衰期的指数衰减记忆分（0~200）。
    - 夜间管道若**没有**走「合并生肉 + 全表 calc_indicators 重铸」（例如近端交易日已在库、
      仅做特征热修），仍需保证该列与算法版本一致；故从本地 daily_data 读出**当前全表**，
      在 Python 中重算整列，再写回。

    DuckDB 增量更新策略（滚动历史安全）：
    - 该分数按每只股票的时间序列**状态机**定义，单日补丁从中间截断会错链；
      因此即使只「新增了一天」日线，也必须基于库内**已有全部历史行**重算 memory，
      再按主键 (ts_code, trade_date) 批量 UPDATE。列数、行数、其它字段均不变。
    - 使用 ``UPDATE daily_data SET … FROM cam_patch``：cam_patch 为内存 DataFrame
      ``con.register`` 后的临时表，与主表等值连接两行键；单事务、顺序 IO，避免逐行点更新。

    与「不破坏既有列」：
    - 仅 ADD COLUMN（若老库无此列）或 UPDATE 该列浮点值；绝不 DROP/重建 daily_data，
      也不修改 55 个基础行情列及 capital_resonance_score 的既有语义。
    """
    try:
        con = get_conn()
        if not table_exists("daily_data"):
            status_callback("【特征增量·资金记忆】daily_data 不存在，跳过。")
            return False
        try:
            pragma = con.execute("PRAGMA table_info('daily_data')").fetchdf()
            _pk = (
                "name"
                if pragma is not None and "name" in getattr(pragma, "columns", [])
                else (
                    "column_name"
                    if pragma is not None and "column_name" in getattr(pragma, "columns", [])
                    else None
                )
            )
            colnames = (
                set(pragma[_pk].astype(str).str.lower().tolist())
                if _pk and pragma is not None and not getattr(pragma, "empty", True)
                else set()
            )
        except Exception:
            colnames = set()
        col_name = "fund_memory_score"
        if col_name not in colnames:
            try:
                con.execute(f'ALTER TABLE daily_data ADD COLUMN "{col_name}" DOUBLE')
                status_callback(f"【特征增量·资金记忆】已 ADD COLUMN {col_name}")
            except Exception as _ae:
                logging.warning("ADD COLUMN %s: %s", col_name, _ae)

        df = con.execute("SELECT * FROM daily_data").fetchdf()
        if df is None or df.empty:
            status_callback("【特征增量·资金记忆】daily_data 为空，跳过。")
            return False

        work = _normalize_daily_merge_keys(df.copy())
        work = work.sort_values(["ts_code", "trade_date"])
        if "trade_date" in work.columns:
            work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce")
            work = work.loc[work["trade_date"].notna()].copy()
            work["trade_date"] = work["trade_date"].dt.strftime("%Y-%m-%d")

        cam_series = compute_fund_memory_score(work)
        if not isinstance(cam_series, pd.Series):
            # 单行样本时上游可能返回 numpy.float64 标量；统一升格为与 work 等长的 Series
            cam_series = pd.Series([cam_series] * len(work), index=work.index)
        patch = pd.DataFrame(
            {
                "ts_code": work["ts_code"].astype(str),
                "trade_date": work["trade_date"].astype(str),
                col_name: pd.to_numeric(cam_series, errors="coerce").fillna(0.0).astype("float64"),
            }
        )

        try:
            con.unregister("cam_patch")
        except Exception:
            pass
        con.register("cam_patch", patch)
        con.execute(
            f"""
            UPDATE daily_data AS d
            SET "{col_name}" = CAST(p.{col_name} AS DOUBLE)
            FROM cam_patch AS p
            WHERE CAST(d.ts_code AS VARCHAR) = CAST(p.ts_code AS VARCHAR)
              AND CAST(d.trade_date AS DATE) = CAST(p.trade_date AS DATE)
            """
        )
        try:
            con.unregister("cam_patch")
        except Exception:
            pass
        duckdb_checkpoint(force=True)
        status_callback(
            f"【特征增量·资金记忆】{col_name} 已全表重算并写回（{len(patch)} 行补丁键）。"
        )
        return True
    except Exception as e:
        logging.exception("_sync_daily_features_fund_memory: %s", e)
        status_callback(f"【特征增量·资金记忆】修补失败: {e}")
        return False


def _sync_daily_features(status_callback=print) -> bool:
    """
    对外统一入口：晚间/早盘增量管道尾部，零 API 重算两条复合特征并 UPDATE 回 daily_data。
    【V26.6 新增资金记忆体系】依次执行 capital_resonance_score、fund_memory_score；
    任一步成功即视为管道有有效写回（返回值 OR）。
    """
    a = _sync_daily_features_capital_resonance(status_callback)
    b = _sync_daily_features_fund_memory(status_callback)
    return bool(a or b)


def run_post_fetch_verification(expected_dates, status_callback):
    """
    同步后轻量验数：仅 SELECT COUNT，不做 DDL/DML。
    【关键】禁止使用 get_conn() 再 close()：get_conn 返回进程级单例写连接，
    一旦 close，全局 _write_con 仍非 None 但连接已死，后续 save_df_to_sql 将全部失败。
    故改用 get_read_conn_singleton()（只读或复用已打开的写连接），且绝不在此关闭连接。
    """
    try:
        con = get_read_conn_singleton()
        if con is None:
            status_callback("📊 数据库载弹: (只读连接暂不可用，跳过计数)")
            return
        total_rows = con.execute("SELECT COUNT(*) FROM daily_data").fetchone()[0]
        status_callback(f"📊 数据库载弹: {total_rows} 条")
    except Exception as e:
        logging.debug(f"run_post_fetch_verification: {e}")


def _notify_data_sync_alert(
    title: str,
    detail: str,
    *,
    category: str = "data_sync",
    dedup_key=None,
    cause: Optional[BaseException] = None,
) -> None:
    """数据管道异常 → 企微运维提示（懒加载，避免与 notification_gateway 循环 import）。"""
    try:
        from core.notification_gateway import notify_wechat_system_alert

        msg = f"{detail}\n根因摘要：{_classify_data_sync_root_cause(detail, cause)}"
        notify_wechat_system_alert(title=title, detail=msg, category=category, dedup_key=dedup_key)
    except Exception as ex:
        logging.debug("企微数据同步告警发送失败（已忽略）: %s", ex)


# ==================== V26 兼容分层输出 ====================
def _v26_normalize_trade_date(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "trade_date" in out.columns:
        out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        out = out[out["trade_date"].notna()].copy()
    return out


def _save_v26_layer_tables(df: pd.DataFrame, *, status_callback=print) -> None:
    """将 daily_data 的当前结果全量覆盖同步到 V26 分层表，保持旧表兼容同时为新架构提供落盘。"""
    if df is None or df.empty:
        return
    try:
        from data.db_core import save_df_to_sql, ensure_v26_tables, ensure_v26_compat_view
        ensure_v26_tables()
        ensure_v26_compat_view(force=True)
    except Exception as e:
        logging.debug("V26 分层表保存入口不可用: %s", e)
        return

    work = _v26_normalize_trade_date(df)
    if work is None or work.empty:
        return

    common = [c for c in ("ts_code", "trade_date") if c in work.columns]
    if len(common) < 2:
        return

    layer_written = []

    # 原始层：保留事实行情字段，方便后续分层迁移与校验
    raw_cols = [
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount",
        "adj_factor", "turnover_rate_f", "vol_ratio", "pe_ttm", "pb", "ps_ttm", "dv_ratio", "total_mv", "circ_mv",
        "net_elg_amount", "net_main_amount", "inst_net_buy", "hk_vol", "rz_net_buy", "limit_times", "strth",
        "forecast_type",
    ]
    raw_df = work[[c for c in raw_cols if c in work.columns]].drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    if not raw_df.empty:
        save_df_to_sql(raw_df, "raw_daily_quotes", if_exists="replace")
        layer_written.append(f"raw_daily_quotes={len(raw_df)}")

    bars_cols = [
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount",
        "adj_factor", "turnover_rate_f", "vol_ratio", "pe_ttm", "pb", "ps_ttm", "dv_ratio", "total_mv", "circ_mv",
    ]
    bars_df = work[[c for c in bars_cols if c in work.columns]].drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    if not bars_df.empty:
        save_df_to_sql(bars_df, "bars_daily", if_exists="replace")
        layer_written.append(f"bars_daily={len(bars_df)}")

    feat_core_cols = [
        "ts_code", "trade_date", "ma5", "ma10", "ma20", "ma30", "ma60", "ma120", "vma5", "vma10", "vma20",
        "high_20", "low_60", "ma20_slope_5", "bias_20", "macd", "macd_signal", "macd_hist", "rsi_14",
        "kdj_k", "kdj_d", "boll_upper", "boll_lower", "cci", "atr_pct", "capital_resonance_score", "fund_memory_score",
    ]
    feat_core_df = work[[c for c in feat_core_cols if c in work.columns]].drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    if not feat_core_df.empty:
        save_df_to_sql(feat_core_df, "feat_daily_core", if_exists="replace")
        layer_written.append(f"feat_daily_core={len(feat_core_df)}")

    feat_cap_cols = ["ts_code", "trade_date", "capital_resonance_score", "net_elg_amount", "net_main_amount", "inst_net_buy", "hk_vol", "rz_net_buy"]
    feat_cap_df = work[[c for c in feat_cap_cols if c in work.columns]].drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    if not feat_cap_df.empty:
        save_df_to_sql(feat_cap_df, "feat_daily_capital", if_exists="replace")
        layer_written.append(f"feat_daily_capital={len(feat_cap_df)}")

    feat_mem_cols = ["ts_code", "trade_date", "fund_memory_score", "limit_times", "strth"]
    feat_mem_df = work[[c for c in feat_mem_cols if c in work.columns]].drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    if not feat_mem_df.empty:
        save_df_to_sql(feat_mem_df, "feat_daily_memory", if_exists="replace")
        layer_written.append(f"feat_daily_memory={len(feat_mem_df)}")

    try:
        from data.db_core import sync_stock_basic

        sync_stock_basic()
    except Exception as e:
        logging.debug("V26 stock_basic 同步跳过: %s", e)

    status_callback(
        "✅ V26 分层表已同步：raw_daily_quotes / bars_daily / feat_daily_core / feat_daily_capital / feat_daily_memory"
        + (f" | rows: {', '.join(layer_written)}" if layer_written else "")
    )


def sync_history(days=150, status_callback=print, progress_callback=None):
    if pro is None:
        # 与 retry_api 一致：无 Token 时绝不当「空跑成功」
        raise_data_fetch_critical("sync_history：Tushare 未初始化（pro is None）")

    # 【V26.7 修复】在数据拉取前预先创建写连接，避免断点续传只读连接与后续写连接冲突。
    # 与 sync_missing_days_batch 相同的修复逻辑：确保 get_read_conn_singleton() 返回写连接。
    try:
        from data.db_core import get_conn
        _ = get_conn()
        logging.debug("历史同步前已预创建写连接")
    except Exception as e_preconn:
        logging.warning("历史同步预创建写连接失败: %s", e_preconn)

    success, missing_dates = check_data_completeness(days)
    if not success:
        status_callback("⚠️ 无法获取交易日历或接口异常。")
        _notify_data_sync_alert(
            "历史数据：交易日历或完整性检查失败",
            "check_data_completeness 未成功，可能网络、Tushare 接口或本地库异常。",
            dedup_key="data_sync_history_cal_check_fail",
        )
        return
    if not missing_dates:
        status_callback("✨ 本地数据已是最新，无缺失交易日。")
        _sync_daily_features(status_callback)
        try:
            from data.db_core import duckdb_vacuum_silent
            duckdb_vacuum_silent(log=status_callback)
            status_callback(f"📦 历史同步数据完整，仅做特征修补+压缩，storage={duckdb_storage_snapshot_text()}")
        except Exception as e:
            logging.debug("历史同步数据完整分支 VACUUM 跳过: %s", e)
            status_callback(f"📦 历史同步数据完整，仅做特征修补，storage={duckdb_storage_snapshot_text()}")
        return

    # 【性能优化 V2】并行生肉拉取（替代顺序 for 循环）
    # 每日生肉获取相互独立，使用 ThreadPoolExecutor 并行化，节省 70-90% 时间
    # 注意：_core_pipeline 内部无共享状态，线程安全
    raw_list = []
    total = len(missing_dates)
    _MAX_PARALLEL_DAYS = min(8, max(1, os.cpu_count() or 4))
    _daily_lock = threading.Lock()
    _daily_results = []

    def _fetch_one_day(date_str):
        """并行拉取单日生肉的包装函数（线程安全收集结果）。"""
        try:
            df = _core_pipeline(date_str, status_callback=status_callback)
            return (date_str, df)
        except DataFetchCriticalError:
            raise
        except Exception as e:
            logging.exception("【并行拉取】%s 跳过: %s", date_str, e)
            return (date_str, None)

    try:
        with ThreadPoolExecutor(max_workers=_MAX_PARALLEL_DAYS) as executor:
            futures = {executor.submit(_fetch_one_day, d): d for d in missing_dates}
            done = 0
            for future in as_completed(futures):
                done += 1
                try:
                    ds, df = future.result()
                    if df is not None and not df.empty:
                        with _daily_lock:
                            raw_list.append(df)
                except DataFetchCriticalError:
                    raise
                except Exception as e:
                    logging.warning("并行拉取 future.result 异常: %s", e)
                if progress_callback:
                    progress_callback(done / total)
    except DataFetchCriticalError:
        raise
    except Exception as e:
        logging.warning("【并行拉取】线程池异常，回退顺序拉取: %s", e)
        # 顺序回退路径（保底）
        for i, date_str in enumerate(missing_dates):
            try:
                raw_df = _core_pipeline(date_str, status_callback=status_callback)
                if raw_df is not None and not raw_df.empty:
                    raw_list.append(raw_df)
            except DataFetchCriticalError:
                raise
            except Exception as ex:
                status_callback(f"⚠️ {date_str} 已跳过: {ex}")
            if progress_callback:
                progress_callback((i + 1) / total)

    if missing_dates and not raw_list:
        # 与 sync_recent_days 对称：断点续传或重复入队时可能「无新生肉」但库内已有各日数据
        if _to_sync_dates_all_have_daily_rows(missing_dates):
            status_callback(
                f"✨ 历史补缺 {len(missing_dates)} 日：库内各日已有日线、无增量生肉（断点续传），跳过重铸；已执行特征修补。"
            )
            logging.info(
                "sync_history: missing=%s 均无新生肉但库内各日均有记录，视为成功",
                missing_dates,
            )
            _sync_daily_features(status_callback)
            try:
                from data.db_core import duckdb_vacuum_silent
                duckdb_vacuum_silent(log=status_callback)
            except Exception as e:
                logging.debug("历史同步空生肉分支 VACUUM 跳过: %s", e)
            try:
                con = get_read_conn_singleton(max_wait_sec=30.0)
                if con is not None:
                    current_df = con.execute("SELECT * FROM daily_data").fetchdf()
                    _save_v26_layer_tables(current_df, status_callback=status_callback)
                    duckdb_checkpoint(force=True)
            except Exception as e:
                logging.debug("V26 兼容层同步跳过: %s", e)
            run_post_fetch_verification(missing_dates, status_callback)
            return
        msg = (
            f"待补缺交易日 {len(missing_dates)} 天，但均未产出有效生肉；请查日志、接口额度与市值过滤。"
        )
        _notify_data_sync_alert(
            "历史同步：补缺日全部无有效生肉",
            msg,
            dedup_key="data_sync_history_all_days_no_raw",
        )
        status_callback(f"❌ {msg}")

    if raw_list:
        df_old = _load_existing_daily_data()
        full_df = _merge_daily_full(df_old, raw_list)
        rebuilt = _rebuild_daily_table_from_full_df(full_df, status_callback=status_callback)
        if rebuilt is None or rebuilt.empty:
            status_callback("❌ 历史同步：日线全量重铸失败，请查看日志。")
            _notify_data_sync_alert(
                "历史同步：日线全量重铸失败",
                "已拉取生肉但 _rebuild_daily_table_from_full_df 未产出有效表，请查日志与 DuckDB。",
                dedup_key="data_sync_history_rebuild_empty",
            )
        else:
            try:
                sync_financial_reports_recent(years=3, status_callback=status_callback)
            except Exception as e:
                logging.warning("历史同步后财报增强补齐失败: %s", e)
    run_post_fetch_verification(missing_dates, status_callback)


def sync_recent_days(
    days=3,
    status_callback=print,
    progress_callback=None,
    raise_on_all_days_failed: bool = False,
):
    if pro is None:
        raise_data_fetch_critical("sync_recent_days：Tushare 未初始化（pro is None）")

    # 【V26.7 修复】在数据拉取前预先创建写连接，避免断点续传只读连接与后续写连接冲突。
    # 与 sync_missing_days_batch 相同的修复逻辑：确保 get_read_conn_singleton() 返回写连接。
    try:
        from data.db_core import get_conn
        _ = get_conn()
        logging.debug("近期同步前已预创建写连接")
    except Exception as e_preconn:
        logging.warning("近期同步预创建写连接失败: %s", e_preconn)

    now = _now_bj_naive()
    end_dt = (now if now.hour >= 16 else now - timedelta(days=1)).strftime('%Y%m%d')
    start_dt = (now - timedelta(days=days * 2 + 10)).strftime('%Y%m%d')
    cal = retry_api(pro.trade_cal)(exchange='SSE', is_open='1', start_date=start_dt, end_date=end_dt)
    if cal.empty:
        status_callback("⚠️ 交易日历为空，跳过近期同步。")
        _notify_data_sync_alert(
            "近期同步：交易日历为空",
            f"SSE trade_cal 返回空表（start={start_dt} end={end_dt}），请检查接口与网络。",
            dedup_key="data_sync_recent_cal_empty",
        )
        return
    target_dates = cal.sort_values('cal_date')['cal_date'].tolist()[-days:]

    existing_set = {_norm_cal_date_8(d) for d in get_existing_trade_dates()}
    target_8 = [_norm_cal_date_8(d) for d in target_dates]
    tail_set = set(target_8[-RECENT_FORCE_RESYNC_TAIL:]) if target_8 else set()
    to_sync = []
    for d_raw, d8 in zip(target_dates, target_8):
        if d8 not in existing_set or d8 in tail_set:
            to_sync.append(d_raw)
    to_sync = list(dict.fromkeys(to_sync))
    if not to_sync:
        status_callback(f"✨ 近 {days} 个交易日在库中已齐，跳过重复下载。")
        _sync_daily_features(status_callback)
        try:
            from data.db_core import duckdb_vacuum_silent
            duckdb_vacuum_silent(log=status_callback)
            status_callback(f"📦 近期同步仅做特征修补+压缩，storage={duckdb_storage_snapshot_text()}")
        except Exception as e:
            logging.debug("特征修补后 VACUUM 跳过: %s", e)
            status_callback(f"📦 近期同步仅做特征修补，storage={duckdb_storage_snapshot_text()}")
        run_post_fetch_verification(target_dates, status_callback)
        return

    # 【性能优化 V2】并行生肉拉取（替代顺序 for 循环）
    raw_list = []
    total = len(to_sync)
    _MAX_PARALLEL_DAYS = min(8, max(1, os.cpu_count() or 4))
    _daily_lock = threading.Lock()

    def _fetch_one_day_recent(date_str):
        try:
            df = _core_pipeline(date_str, status_callback=status_callback)
            return (date_str, df)
        except DataFetchCriticalError:
            raise
        except Exception as e:
            logging.exception("【并行拉取】%s 跳过: %s", date_str, e)
            return (date_str, None)

    try:
        with ThreadPoolExecutor(max_workers=_MAX_PARALLEL_DAYS) as executor:
            futures = {executor.submit(_fetch_one_day_recent, d): d for d in to_sync}
            done = 0
            for future in as_completed(futures):
                done += 1
                try:
                    ds, df = future.result()
                    if df is not None and not df.empty:
                        with _daily_lock:
                            raw_list.append(df)
                except DataFetchCriticalError:
                    raise
                except Exception as e:
                    logging.warning("并行拉取 future.result 异常: %s", e)
                if progress_callback:
                    progress_callback(done / total)
    except DataFetchCriticalError:
        raise
    except Exception as e:
        logging.warning("【并行拉取】近期同步线程池异常，回退顺序拉取: %s", e)
        for i, date_str in enumerate(to_sync):
            try:
                raw_df = _core_pipeline(date_str, status_callback=status_callback)
                if raw_df is not None and not raw_df.empty:
                    raw_list.append(raw_df)
            except DataFetchCriticalError:
                raise
            except Exception as ex:
                status_callback(f"⚠️ {date_str} 已跳过: {ex}")
            if progress_callback:
                progress_callback((i + 1) / total)

    if to_sync and not raw_list:
        # 数据已在库且断点续传跳过 → 不产出生肉是预期行为，禁止当失败告警（否则夜间守护进程误报）
        if _to_sync_dates_all_have_daily_rows(to_sync):
            status_callback(
                f"✨ 近端 {len(to_sync)} 个交易日：库内已有日线、无增量生肉（断点续传），跳过重铸；已执行特征修补。"
            )
            logging.info(
                "sync_recent_days: to_sync=%s 均无新生肉但库内各日均有记录，视为成功",
                [_norm_cal_date_8(x) for x in to_sync],
            )
            _sync_daily_features(status_callback)
            try:
                from data.db_core import duckdb_vacuum_silent
                duckdb_vacuum_silent(log=status_callback)
            except Exception as e:
                logging.debug("近期空生肉分支 VACUUM 跳过: %s", e)
            try:
                con = get_read_conn_singleton(max_wait_sec=30.0)
                if con is not None:
                    current_df = con.execute("SELECT * FROM daily_data").fetchdf()
                    _save_v26_layer_tables(current_df, status_callback=status_callback)
                    duckdb_checkpoint(force=True)
            except Exception as e:
                logging.debug("V26 兼容层同步跳过: %s", e)
            run_post_fetch_verification(target_dates, status_callback)
            return pd.DataFrame()
        msg = (
            f"待同步交易日 {len(to_sync)} 天，但单日管道均未产出有效生肉，请查日志与接口配额。"
        )
        _notify_data_sync_alert(
            "近期同步：需同步日全部拉取失败",
            msg,
            dedup_key="data_sync_recent_all_days_failed",
        )
        status_callback(f"❌ {msg}")
        if raise_on_all_days_failed:
            raise RuntimeError(msg)

    if raw_list:
        df_old = _load_existing_daily_data()
        full_df = _merge_daily_full(df_old, raw_list)
        out_df = _rebuild_daily_table_from_full_df(full_df, status_callback=status_callback)
        if out_df is None or out_df.empty:
            status_callback("❌ 近期同步：日线全量重铸失败，请查看日志。")
            _notify_data_sync_alert(
                "近期同步：日线全量重铸失败",
                "生肉已合并但重铸后表为空，请查 DuckDB 与指标管道日志。",
                dedup_key="data_sync_recent_rebuild_empty",
            )
            run_post_fetch_verification(target_dates, status_callback)
            return pd.DataFrame()
        try:
            sync_financial_reports_recent(years=3, status_callback=status_callback)
        except Exception as e:
            logging.warning("近期同步后财报增强补齐失败: %s", e)
        return out_df

    run_post_fetch_verification(target_dates, status_callback)
    return pd.DataFrame()


def sync_missing_days_batch(missing_days, status_callback=print) -> list:
    """
    批量修补缺失日期（Phase 1: 先批量下载所有生肉，Phase 2: 最后一次合并重铸）。
    相比逐日调用 sync_single_day，避免每天做一次全表重铸，大幅节省时间和写入开销。

    参数 missing_days: 日期字符串列表，如 ['20260506', '20260507', '20260508']
    返回 failed_days: 下载失败的日期列表
    """
    total = len(missing_days)
    if total == 0:
        return []

    status_callback(f"📥 批量下载 {total} 个缺失日期的生肉...")
    raw_frames = []
    failed_days = []

    # 【V26.7 关键修复】在 Phase 1 开始前激进地清理所有只读连接并预创建写连接。
    # 原因：
    # 1. UI 侧边栏可能在调用本函数前已通过 get_read_conn_singleton() 创建了只读连接
    # 2. 这些只读连接保存在 _readonly_conns 列表中，通过 _close_all_readonly_conns() 统一关闭
    # 3. 必须使用垃圾回收 + 重试策略来确保写连接成功创建
    try:
        import gc
        from data.db_core import get_conn, _close_all_readonly_conns, _thread_local
        
        # 最多重试 3 次，每次清理后重新尝试
        for _retry_attempt in range(3):
            try:
                # 清理所有只读连接（使用新增的追踪机制）
                _close_all_readonly_conns()
                # 关闭线程本地连接
                if hasattr(_thread_local, 'conn') and _thread_local.conn is not None:
                    try:
                        _thread_local.conn.close()
                    except Exception:
                        pass
                    _thread_local.conn = None
                # 强制垃圾回收，关闭不可达的对象
                gc.collect()
                # 尝试预创建写连接
                _ = get_conn()
                logging.debug("批量下载 Phase 1 前已预创建写连接")
                break  # 成功，跳出重试循环
            except Exception as e:
                if _retry_attempt < 2:
                    logging.warning(
                        "预创建写连接第 %d 次尝试失败（%s），将清理后重试...", 
                        _retry_attempt + 1, e
                    )
                    gc.collect()
                    continue
                else:
                    logging.warning("预创建写连接失败，将在后续重试: %s", e)
    except Exception as e_preconn:
        logging.warning("预创建写连接时异常（将在后续重试）: %s", e_preconn)

    # Phase 1: 批量下载所有生肉
    for i, d in enumerate(missing_days):
        status_callback(f"📥 下载生肉 [{i + 1}/{total}]: {d}")
        try:
            raw_df = _core_pipeline(d, status_callback=status_callback)
        except DataFetchCriticalError:
            raise
        except Exception as e:
            logging.exception("【批量下载】%s 生肉提取异常", d)
            status_callback(f"⚠️ {d} 下载失败: {e}")
            failed_days.append(d)
            continue

        if raw_df is not None and not raw_df.empty:
            raw_frames.append(raw_df)
            status_callback(f"✅ {d} 生肉提取完成: {len(raw_df)} 只")
        else:
            status_callback(f"⚠️ {d} 生肉为空，已跳过")
            failed_days.append(d)

        progress = (i + 1) / total
        status_callback(f"📊 生肉下载进度: {progress:.0%}")

    if failed_days:
        status_callback(f"⚠️ 有 {len(failed_days)} 天下载失败: {failed_days}")

    if not raw_frames:
        status_callback("❌ 所有缺失日期的生肉均为空，无法重铸。")
        return failed_days

    # 【V26.7 修复】防御性清理：只关闭只读连接，保留写连接。
    # 原因：
    # 1. Phase 1 前已预创建写连接，该连接在整个 Phase 1 期间可用
    # 2. 防御性清理应该只清理可能残留的只读连接（来自其他代码路径）
    # 3. close_db() 会关闭 _write_con，导致 Phase 2 前失去写连接
    # 4. 保留写连接可避免 DuckDB "different configuration" 错误
    # 5. 使用 _close_all_readonly_conns() 统一关闭所有追踪的只读连接
    try:
        import gc
        from data.db_core import get_conn, _close_all_readonly_conns, close_thread_local_conn, _thread_local
        
        # 最多重试 3 次，每次清理后重新尝试
        for _retry_attempt in range(3):
            try:
                # 清理只读连接
                close_thread_local_conn()
                _close_all_readonly_conns()
                # 强制垃圾回收
                gc.collect()
                # 确保写连接可用
                _ = get_conn()
                logging.debug("Phase 2 开始前已确保写连接可用")
                break  # 成功，跳出重试循环
            except Exception as e:
                if _retry_attempt < 2:
                    logging.warning(
                        "Phase 2 前确保写连接第 %d 次尝试失败（%s），将清理后重试...", 
                        _retry_attempt + 1, e
                    )
                    gc.collect()
                    continue
                else:
                    logging.warning("Phase 2 前确保写连接失败: %s", e)
    except Exception as e_conn_cleanup:
        logging.debug("连接清理异常（不影响主流程）: %s", e_conn_cleanup)

    # Phase 2: 一次性加载历史 + 合并所有生肉 + 一次重铸
    status_callback(f"🔨 合并 {len(raw_frames)} 段生肉 + 历史数据，执行单次全量重铸...")
    df_old = _load_existing_daily_data()
    full_df = _merge_daily_full(df_old, raw_frames)
    rebuilt = _rebuild_daily_table_from_full_df(full_df, status_callback=status_callback)

    if rebuilt is not None and not rebuilt.empty:
        try:
            sync_financial_reports_recent(years=3, status_callback=status_callback)
        except Exception as e:
            logging.warning("批量补齐后财报增强补齐失败: %s", e)
        _save_v26_layer_tables(rebuilt, status_callback=status_callback)
        duckdb_checkpoint(force=True)
        
        # 【V26.7 新增】批量补缺完成后执行最安全、最稳妥的数据库压缩
        # 使用完整的多轮 VACUUM 策略：
        # 1. 先执行 CHECKPOINT 确保 WAL 落盘
        # 2. 执行多轮 ANALYZE + VACUUM + CHECKPOINT
        # 3. 确保数据库体积最小化，避免膨胀
        try:
            from data.db_core import duckdb_vacuum_silent, duckdb_storage_snapshot
            status_callback("🗜️ 开始执行数据库深度压缩...")
            before_snapshot = duckdb_storage_snapshot()
            status_callback(
                f"📦 压缩前存储：db={before_snapshot['db_bytes']/1024/1024:.2f}MB, "
                f"wal={before_snapshot['wal_bytes']/1024/1024:.2f}MB, "
                f"tmp={before_snapshot['tmp_bytes']/1024/1024:.2f}MB, "
                f"total={before_snapshot['total_bytes']/1024/1024:.2f}MB"
            )
            duckdb_vacuum_silent(log=status_callback)
            after_snapshot = duckdb_storage_snapshot()
            status_callback(
                f"📦 压缩后存储：db={after_snapshot['db_bytes']/1024/1024:.2f}MB, "
                f"wal={after_snapshot['wal_bytes']/1024/1024:.2f}MB, "
                f"tmp={after_snapshot['tmp_bytes']/1024/1024:.2f}MB, "
                f"total={after_snapshot['total_bytes']/1024/1024:.2f}MB"
            )
            if before_snapshot['total_bytes'] > 0 and after_snapshot['total_bytes'] > 0:
                ratio = after_snapshot['total_bytes'] / before_snapshot['total_bytes']
                if ratio < 0.95:
                    status_callback(f"🗜️ 压缩优化完成：节省 {(1 - ratio) * 100:.1f}% 空间")
                else:
                    status_callback(f"✅ 压缩检查完成：存储效率良好 ({(1 - ratio) * 100:.1f}% 空间已优化)")
            status_callback("✅ 数据库压缩完成，数据安全存储")
        except Exception as e_vacuum:
            logging.warning("批量补齐后数据库压缩失败（不影响数据完整性）: %s", e_vacuum)
            status_callback(f"⚠️ 数据库压缩跳过: {e_vacuum}")
        
        # 收集成功日期的 trade_date 做验证
        success_dates = [_norm_cal_date_8(d) for d in missing_days if d not in failed_days]
        run_post_fetch_verification(success_dates, status_callback)
        status_callback(f"✅ 批量补齐完成：成功 {len(missing_days) - len(failed_days)} 天，共 {len(rebuilt)} 行。")
    else:
        status_callback("❌ 批量补齐全量重铸失败，缺失数据可能仍未写入。")

    return failed_days


def sync_single_day(trade_date_str, status_callback=print) -> bool:
    """
    同步单个交易日（YYYYMMDD）：拉取生肉 → 与库内历史合并 → 全量重算 57 维指标后落库。
    与 sync_recent_days 单步逻辑一致，供侧边栏「缺失数据下载」等调用；定时增量以 auto_sniper_daemon 晚间/早盘链为准。

    参数 trade_date_str：支持 20260323 或 2026-03-23，内部规范为 8 位数字串。
    若 SSE 交易日历显示该日非开市，则跳过（避免周末/节假日空跑）。

    返回 True 表示该日无需报警：已写入新数据、或本地已有该日数据（断点无新增）、或非交易日跳过。
    返回 False 表示交易日但拉取后库中仍无该日记录（接口无数据/过滤等），供 UI 勿误报「修补成功」。
    """
    if pro is None:
        raise_data_fetch_critical("sync_single_day：Tushare 未初始化（pro is None）")

    d8 = _norm_cal_date_8(trade_date_str)
    if len(d8) != 8 or not d8.isdigit():
        status_callback(f"❌ 非法交易日格式: {trade_date_str!r}，需 YYYYMMDD。")
        return False

    # 【V26.7 修复】在生肉提取前预先创建写连接，避免断点续传只读连接与后续写连接冲突。
    # 与 sync_missing_days_batch 相同的修复逻辑：确保 get_read_conn_singleton() 返回写连接。
    try:
        from data.db_core import get_conn
        _ = get_conn()
        logging.debug("单日同步前已预创建写连接")
    except Exception as e_preconn:
        logging.warning("单日同步预创建写连接失败: %s", e_preconn)

    try:
        cal = retry_api(pro.trade_cal)(exchange="SSE", is_open="1", start_date=d8, end_date=d8)
        if cal is None or cal.empty:
            status_callback(f"📅 {d8} 非交易日或日历无记录，跳过同步。")
            return True
    except Exception as e:
        logging.warning("【单日同步】trade_cal 校验异常，仍尝试拉数: %s", e)

    try:
        raw_df = _core_pipeline(d8, status_callback=status_callback)
    except DataFetchCriticalError:
        raise
    except Exception as e:
        logging.exception("【平滑降级】单日同步异常 %s: %s", d8, e)
        status_callback(f"⚠️ {d8} 同步异常: {e}")
        _notify_data_sync_alert(
            f"单日同步异常 {d8}",
            str(e)[:500],
            dedup_key=f"data_sync_single_exc_{d8}",
        )
        return False

    if raw_df is None or raw_df.empty:
        run_post_fetch_verification([d8], status_callback)
        n = _count_rows_for_trade_date(d8)
        if n > 0:
            status_callback(f"✅ {d8} 本地已有 {n} 条记录（断点续传无新增，无需全量重算）。")
            _sync_daily_features(status_callback)
            try:
                from data.db_core import duckdb_vacuum_silent
                duckdb_vacuum_silent(log=status_callback)
            except Exception as e:
                logging.debug("单日无新增分支 VACUUM 跳过: %s", e)
            try:
                con = get_read_conn_singleton(max_wait_sec=30.0)
                if con is not None:
                    current_df = con.execute("SELECT * FROM daily_data WHERE trade_date = ?", [_trade_date_for_sql(d8)]).fetchdf()
                    if current_df is not None and not current_df.empty:
                        _save_v26_layer_tables(current_df, status_callback=status_callback)
                        duckdb_checkpoint(force=True)
            except Exception as e:
                logging.debug("V26 单日兼容层同步跳过: %s", e)
            return True
        status_callback(
            f"❌ {d8} 未能入库：接口未返回 daily_basic 有效数据、或全部被市值/P0 规则过滤。"
            "请检查 Tushare 专线、积分及该日是否已收盘落库。"
        )
        _notify_data_sync_alert(
            f"单日 {d8} 未能入库",
            "接口无有效生肉或全部被规则过滤，库中该日仍无记录。",
            dedup_key=f"data_sync_single_no_raw_{d8}",
        )
        return False

    df_old = _load_existing_daily_data()
    full_df = _merge_daily_full(df_old, [raw_df])
    rebuilt = _rebuild_daily_table_from_full_df(full_df, status_callback=status_callback)
    if rebuilt is not None and not rebuilt.empty:
        try:
            sync_financial_reports_recent(years=3, status_callback=status_callback)
        except Exception as e:
            logging.warning("单日同步后财报增强补齐失败: %s", e)
    if rebuilt is None or rebuilt.empty:
        status_callback("❌ 日线全量重铸失败，缺失数据可能仍未写入；请查看上方日志。")
        _notify_data_sync_alert(
            f"单日 {d8} 重铸失败",
            "合并生肉后日线全量重铸为空，请查日志。",
            dedup_key=f"data_sync_single_rebuild_{d8}",
        )
        return False
    _save_v26_layer_tables(rebuilt, status_callback=status_callback)
    duckdb_checkpoint(force=True)
    run_post_fetch_verification([d8], status_callback)
    return True


if __name__ == "__main__":
    try:
        df_last = sync_recent_days(days=3)
        try:
            if df_last is not None and not df_last.empty:
                print(df_last.tail(1).to_string(index=False))
            else:
                print("⚠️ df_last 为空，未打印尾行样本。")
        except Exception as e:
            print(f"⚠️ 打印尾行样本失败: {e}")
    except DataFetchCriticalError as e:
        print(f"❌ 实盘熔断（CLI）：{e}")
        raise SystemExit(2) from e
