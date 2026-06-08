# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.7 - 高并发异步行情中枢（仅实时快照，不承担历史全量下载）

【V26.7 瀑布式数据源战略 — 基于交易日实测结果】

实测数据（2026-05-26 周二，14只股票池）:
  腾讯 (qt.gtimg.cn):           100% → 字段最全（price/pre_close/open/high/low/vol/amount/vol_ratio/turnover_rate_f/limit_up/limit_down/amplitude_pct）
  新浪 (hq.sinajs.cn):         100% → 仅基础字段，缺6个关键字段
  东方财富 (push2.eastmoney.com): → DDE 实时资金流（主力净流入/占比），独立第3.5层

瀑布策略（顺序执行，任意步骤足够好即返回）:
  1. 腾讯主力   → 覆盖主力全字段，含腾讯独有 vol_ratio/turnover_rate_f/limit_up/limit_down/amplitude_pct
  2. 新浪备用   → 补充腾讯失败的标的（基础行情）
  3. Tushare    → 补充 PE_TTM / PB / circ_mv（历史日线，盘中有旧数据可用）
  3.5. 东方财富DDE → 盘中主力净流入(万元) / 主力净流入占比(%)，覆盖 realtime_main_inflow_rate
  4. 推算补全   → pct_chg = (price - pre_close) / pre_close * 100，涨跌停从 pre_close 推算

【性能】单源并行 50 只/批，单次请求 5s 超时，任意源失败即用下一源补充，绝不卡死。
"""
# __file__ aware path setup so `python api_fetcher.py` can self-test
import sys as _sys, os as _os
_SELF_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _SELF_DIR not in _sys.path:
    _sys.path.insert(0, _SELF_DIR)
if _os.path.dirname(_SELF_DIR) not in _sys.path:
    _sys.path.insert(0, _os.path.dirname(_SELF_DIR))

# Standard library
import asyncio
import json
import logging
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

# Third-party
import aiohttp

# Local modules
from core.stock_name_utils import normalize_stock_display_name

# 🛡️ 并发安全锁
MAX_CONCURRENT_REQUESTS = 20
TIMEOUT_SECONDS = 5.0
_HTTP_RETRY_ATTEMPTS = 1  # 只试一次，失败即换备用源
_BACKOFF_BASE = 0.2
_BACKOFF_CAP = 2.0

# ──────────────────────────────────────────────
# 工具函数
# ──────────────────────────────────────────────

def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        if isinstance(val, str) and val.strip() in ('', '-', '--'):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _is_retriable(e: BaseException) -> bool:
    if isinstance(e, (asyncio.TimeoutError, TimeoutError, aiohttp.ServerDisconnectedError)):
        return True
    if isinstance(e, aiohttp.ClientError):
        return True
    msg = str(e).lower()
    return any(k in msg for k in ("timeout", "connection", "reset", "refused", "broken pipe", "429", "disconnected"))


async def _async_backoff(attempt: int) -> None:
    await asyncio.sleep(min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** attempt)) + random.uniform(0, 0.3))


async def _http_get_text(session, url, semaphore, encoding="gbk", headers=None) -> str | None:
    """带 1 次重试的 HTTP GET → text；失败返回 None。"""
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    req_h = headers or {}
    last_exc = None
    for attempt in range(_HTTP_RETRY_ATTEMPTS):
        async with semaphore:
            try:
                async with session.get(url, timeout=timeout, headers=req_h) as resp:
                    if resp.status == 429 or resp.status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, tuple(resp.history),
                            status=resp.status, headers=resp.headers,
                        )
                    return await resp.text(encoding=encoding)
            except BaseException as e:
                last_exc = e
                if attempt < _HTTP_RETRY_ATTEMPTS - 1 and _is_retriable(e):
                    await _async_backoff(attempt)
                else:
                    break
    if last_exc is not None:
        logging.debug("api_fetcher GET 放弃: %s", last_exc)
    return None


# ──────────────────────────────────────────────
# HTTP 请求头（各源需要不同的 Referer）
# ──────────────────────────────────────────────

_TC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.qq.com/",
    "Accept": "*/*",
}
_SINA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.sina.com.cn/",
    "Accept": "*/*",
}
_EMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
    "Accept": "*/*",
}


# ──────────────────────────────────────────────
# 数据源抓取函数（各源独立，互不依赖）
# ──────────────────────────────────────────────

async def _fetch_eastmoney_dde_chunk(session, chunk, semaphore) -> Dict:
    """
    东方财富实时 DDE 资金流接口 — 获取盘中主力/大单净流入与占比。
    接口: push2.eastmoney.com/api/qt/stock/fflow/daykline/get
    字段: f62=主力净流入额(元), f184=超大单净流入, f66=大单净流入,
          f69=中单净流入, f72=小单净流入, f58=股票名称
    返回: {code: {dde_main_net(万元), dde_main_rate(%)} }
    """
    result = {}
    for code in chunk:
        code6 = str(code).split(".")[0][:6]
        # secid: 沪市=1.6xxxxx, 深市=0.0/0/3/4xxxxx
        if code6.startswith("6"):
            secid = f"1.{code6}"
        elif code6.startswith(("0", "3")):
            secid = f"0.{code6}"
        else:
            continue
        url = (
            "http://push2.eastmoney.com/api/qt/stock/fflow/daykline/get"
            f"?lmt=0&klt=1&secid={secid}"
            f"&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63"
        )
        try:
            text = await _http_get_text(session, url, semaphore, encoding="utf-8", headers=_EMONEY_HEADERS)
            if text is None:
                continue
            data = json.loads(text)
            klines = (data.get("data") or {}).get("klines") or []
            if not klines:
                continue
            # 取最后一根（当日最新累计）
            last = klines[-1].split(",")
            if len(last) < 7:
                continue
            # f51=时间, f52=主力净流入, f53=小单, f54=中单, f55=大单, f56=超大单
            main_net_yuan = _safe_float(last[1], 0.0)   # 元
            amount_yuan = _safe_float(last[6], 0.0)      # 成交额(元)，f63
            if main_net_yuan == 0.0 and amount_yuan == 0.0:
                continue
            result[code6] = {
                "dde_main_net": round(main_net_yuan / 10000.0, 2),   # 万元
                "dde_main_rate": round(main_net_yuan / max(amount_yuan, 1) * 100.0, 2) if amount_yuan > 0 else 0.0,
            }
        except Exception:
            pass
    return result


async def _fetch_tencent_chunk(session, chunk, semaphore) -> Dict:
    """
    腾讯行情接口 — 主力数据源（2026-05-26 实测 14/14 命中，字段最全）
    字段: price/pre_close/open/high/low/vol/amount/vol_ratio/turnover_rate_f/limit_up/limit_down/amplitude_pct
    腾讯独有: vol_ratio(量比) / turnover_rate_f(换手率) / limit_up / limit_down / amplitude_pct
    """
    tc_list = ",".join(
        f"{'sh' if str(c).split('.')[0][:6].startswith('6') else 'sz'}{str(c).split('.')[0][:6]}"
        for c in chunk
    )
    url = f"http://qt.gtimg.cn/q={tc_list}"
    result = {}
    try:
        text = await _http_get_text(session, url, semaphore, encoding="gbk", headers=_TC_HEADERS)
        if text is None:
            return {}
        for line in text.strip().split("\n"):
            if '="' not in line:
                continue
            parts = line.split('="')[1].strip('";').split("~")
            if len(parts) <= 49 or _safe_float(parts[3]) <= 0:
                continue
            code = parts[2]  # 6位纯数字
            price = _safe_float(parts[3])
            pre_close = _safe_float(parts[4])
            result[code] = {
                "name": normalize_stock_display_name(parts[1]),
                "price": price,
                "pre_close": pre_close,
                "open": _safe_float(parts[5]),
                "volume": _safe_float(parts[36]) * 100,
                "amount": _safe_float(parts[37]) * 10000,
                "high": _safe_float(parts[33]),
                "low": _safe_float(parts[34]),
                "vol_ratio": _safe_float(parts[49]) if len(parts) > 49 else 0.0,
                "turnover_rate_f": _safe_float(parts[38]) if len(parts) > 38 else 0.0,
                "limit_up": _safe_float(parts[47]) if len(parts) > 47 else 0.0,
                "limit_down": _safe_float(parts[48]) if len(parts) > 48 else 0.0,
                "amplitude_pct": _safe_float(parts[43]) if len(parts) > 43 else 0.0,
            }
    except Exception as e:
        logging.debug("Tencent Fetch Error: %s", e)
    return result


async def _fetch_sina_chunk(session, chunk, semaphore) -> Dict:
    """
    新浪行情接口 — 备用数据源（2026-05-26 实测 14/14 命中，仅基础字段）
    字段: price/pre_close/open/high/low/volume/amount
    缺: vol_ratio / turnover_rate_f / limit_up / limit_down / amplitude_pct
    """
    sina_list = ",".join(
        f"{'sh' if str(c).split('.')[0][:6].startswith('6') else 'sz'}{str(c).split('.')[0][:6]}"
        for c in chunk
    )
    url = f"http://hq.sinajs.cn/list={sina_list}"
    result = {}
    try:
        text = await _http_get_text(session, url, semaphore, encoding="gbk", headers=_SINA_HEADERS)
        if text is None:
            return {}
        for line in text.strip().split("\n"):
            if '="' not in line:
                continue
            left, right = line.split('="')
            code = left[-6:]  # 6位纯数字
            parts = right.strip('";').split(",")
            if len(parts) <= 30 or _safe_float(parts[3]) <= 0:
                continue
            result[code] = {
                "name": normalize_stock_display_name(parts[0]),
                "price": _safe_float(parts[3]),
                "pre_close": _safe_float(parts[2]),
                "open": _safe_float(parts[1]),
                "volume": _safe_float(parts[8]),
                "amount": _safe_float(parts[9]),
                "high": _safe_float(parts[4]),
                "low": _safe_float(parts[5]),
                # 新浪不提供以下字段，设为 0
                "vol_ratio": 0.0,
                "turnover_rate_f": 0.0,
                "limit_up": 0.0,
                "limit_down": 0.0,
                "amplitude_pct": 0.0,
            }
    except Exception as e:
        logging.debug("Sina Fetch Error: %s", e)
    return result


# ──────────────────────────────────────────────
# 瀑布式合并
# ──────────────────────────────────────────────

async def _fetch_realtime_batch_async(codes: List[str]) -> Dict[str, Dict]:
    """
        【V26.7 瀑布式数据源战略 — 基于交易日实测结果】

    执行顺序（任意步骤成功即可能提前返回）：
    1. 腾讯主力 → 全部 14 个字段（主力）
    2. 新浪备用 → 7 个基础字段（腾讯失败的标的）
    3. Tushare 补充 → PE_TTM / PB / circ_mv（历史日线，盘中有旧数据）
    4. 推算补全 → pct_chg / limit_up / limit_down（从 price + pre_close 推算）

    性能保证：单源失败不触发重试退避，最坏情况 = 各源顺序执行，总耗时 ≤ 单源耗时的叠加。
    """
    clean = [c for c in codes if not c.endswith(".BJ")]
    if not clean:
        return {}

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ssl=False)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # ── 第 1 层：腾讯主力 ─────────────────────────────
    rt_map: Dict[str, Dict] = {}
    chunks = [clean[i : i + 50] for i in range(0, len(clean), 50)]

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_fetch_tencent_chunk(session, ch, semaphore) for ch in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, dict):
                rt_map.update(res)

    # ── 第 2 层：新浪备用（腾讯命中率低，用新浪补漏）───
    sina_map: Dict[str, Dict] = {}
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_fetch_sina_chunk(session, ch, semaphore) for ch in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, dict):
                sina_map.update(res)

    # 腾讯失败的标的 → 用新浪补充
    for code, sina_data in sina_map.items():
        if code not in rt_map:
            rt_map[code] = sina_data

    # ── 第 3 层：Tushare 补充财务字段 ─────────────────
    await _fill_tushare_supplemental_fields(rt_map, clean)

    # ── 第 3.5 层：东方财富 DDE 实时资金流（盘中主力净流入）────
    # 【V26.7 修复】必须在两条路径（腾讯≥50% 和 <50%）都执行
    dde_map: Dict[str, Dict] = {}
    dde_chunks = [clean[i : i + 30] for i in range(0, len(clean), 30)]
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ssl=False)) as session:
        sem2 = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks2 = [_fetch_eastmoney_dde_chunk(session, ch, sem2) for ch in dde_chunks]
        dde_results = await asyncio.gather(*tasks2, return_exceptions=True)
        for res in dde_results:
            if isinstance(res, dict):
                dde_map.update(res)
    for code6, dde_data in dde_map.items():
        if code6 in rt_map:
            rt_map[code6]["dde_main_net"] = dde_data.get("dde_main_net")
            rt_map[code6]["dde_main_rate"] = dde_data.get("dde_main_rate")

    # ── 第 4 层：推算派生字段 ──────────────────────────
    _compute_derived_fields(rt_map)
    return rt_map


def _compute_derived_fields(rt_map: Dict[str, Dict]) -> None:
    """
    推算派生字段（从已有字段计算，不依赖任何网络请求）：
    - pct_chg     : (price - pre_close) / pre_close * 100
    - limit_up    : pre_close * 1.10（涨停价，精确值依赖腾讯接口）
    - limit_down  : pre_close * 0.90（跌停价，精确值依赖腾讯接口）
    """
    for snap in rt_map.values():
        if not isinstance(snap, dict):
            continue
        price = snap.get("price", 0)
        pre_close = snap.get("pre_close", 0)
        # pct_chg：若源有值则保留（腾讯无此字段但推算总比 0 好）
        if snap.get("pct_chg", 0) == 0 and pre_close > 0 and price > 0:
            snap["pct_chg"] = round((price - pre_close) / pre_close * 100, 2)
        # 涨跌停：腾讯有精确值则保留，没有则推算
        if snap.get("limit_up", 0) <= 0 and pre_close > 0:
            snap["limit_up"] = round(pre_close * 1.10, 2)
        if snap.get("limit_down", 0) <= 0 and pre_close > 0:
            snap["limit_down"] = round(pre_close * 0.90, 2)


async def _fill_tushare_supplemental_fields(rt_map: Dict, clean_targets: List[str]) -> None:
    """
    用 Tushare daily_basic 补充 rt_map 中缺失的财务字段（pe_ttm / pb / circ_mv）。
    daily_basic 返回的是历史日线数据，盘中有旧数据可用（上一个交易日收盘后更新）。
    注意：这是日线基础数据，非严格实时；但 PE/PB/circ_mv 在盘中变化极小，够用。
    """
    token = os.getenv("TUSHARE_TOKEN", "")
    if not token:
        return
    try:
        import tushare as ts
        ts.set_token(token)
        pro = ts.pro_api()

        for i in range(0, len(clean_targets), 500):
            chunk = clean_targets[i : i + 500]
            try:
                df = pro.daily_basic(ts_code=",".join(chunk))
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    code_key = str(row.get("ts_code", "")).split(".")[0][:6]
                    if code_key not in rt_map:
                        continue
                    merged = rt_map[code_key]
                    for field, col in [
                        ("pe_ttm", "pe_ttm"),
                        ("pb", "pb"),
                        ("circ_mv", "circ_mv"),
                    ]:
                        val = _safe_float(row.get(col, 0))
                        if val > 0 and not merged.get(field):
                            merged[field] = val
            except Exception as e:
                logging.debug("Tushare daily_basic 补充失败: %s", e)
    except Exception as e:
        logging.debug("Tushare 初始化失败: %s", e)


# ──────────────────────────────────────────────
# 同步包装器（供 scan_engine / UI 调用）
# ──────────────────────────────────────────────

def fetch_realtime_batch(codes):
    """
    👑 老法师封装：同步包装器（Drop-in Replacement）
    外部调用方无需任何修改，直接调用此函数。
    内部自动建立独立事件循环，全速压榨异步并发性能。
    """
    if not codes:
        return {}

    # Daemon 后台路径：严格不碰 Streamlit ScriptRunContext
    if os.environ.get("XIAOJIE_DAEMON_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
        try:
            return asyncio.run(_fetch_realtime_batch_async(codes))
        except Exception:
            logging.exception("fetch_realtime_batch: daemon 模式失败，返回空 dict")
            return {}

    try:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(_fetch_realtime_batch_async(codes))

        # UI 环境：子线程内 asyncio.run，挂载 ScriptRunContext
        parent_ctx = None
        try:
            from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx
            parent_ctx = get_script_run_ctx(suppress_warning=True)
        except Exception:
            pass

        def _run():
            if parent_ctx is not None:
                try:
                    from streamlit.runtime.scriptrunner import add_script_run_ctx
                    add_script_run_ctx(threading.current_thread(), parent_ctx)
                except Exception:
                    pass
            return asyncio.run(_fetch_realtime_batch_async(codes))

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_run)
            try:
                return future.result(timeout=20)
            except TimeoutError:
                logging.warning("fetch_realtime_batch: 20s 超时放弃（批次大小=%s）", len(codes))
                return {}
    except Exception:
        logging.exception("fetch_realtime_batch: 失败，返回空 dict")
        return {}


# ──────────────────────────────────────────────
# 自测入口
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import time
    test_codes = ["600519.SH", "000858.SZ", "300750.SZ", "002594.SZ", "300308.SZ"]
    t1 = time.time()
    res = fetch_realtime_batch(test_codes)
    elapsed = time.time() - t1
    print(f"\nOK fetch_realtime_batch self-test done")
    print(f"   耗时: {elapsed:.3f}秒")
    print(f"   命中: {len(res)} / {len(test_codes)}")
    for code, snap in list(res.items())[:5]:
        print(f"   {code}: price={snap.get('price')}, vol_ratio={snap.get('vol_ratio')}, "
              f"turnover_f={snap.get('turnover_rate_f')}, limit_up={snap.get('limit_up')}, "
              f"pct_chg={snap.get('pct_chg')}, name={snap.get('name')}")
