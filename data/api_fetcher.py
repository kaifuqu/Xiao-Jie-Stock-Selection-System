# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 高并发异步行情中枢（仅实时快照，不承担历史全量下载）
【能力】aiohttp 多源容灾、fetch_realtime_batch 同步包装供 scan_engine / UI 调用。
【稳定性】单次 HTTP 指数退避 + 抖动；超时/断连/429 可重试；单 chunk 失败不拖垮全批。
"""
# Standard library
import asyncio
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

# 🛡️ 并发安全锁：最大同时发起的网络连接数 (控制在20-30最安全)
MAX_CONCURRENT_REQUESTS = 25
TIMEOUT_SECONDS = 4.0
# 每个 chunk 最大重试次数（含首次请求）
_HTTP_RETRY_ATTEMPTS = 6
_BACKOFF_BASE = 0.5
_BACKOFF_CAP = 45.0


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        if isinstance(val, str) and val.strip() in ['', '-']:
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _is_retriable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return True
    if isinstance(exc, aiohttp.ClientConnectionError):
        return True
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status in (408, 425, 429, 500, 502, 503, 504)
    if isinstance(exc, aiohttp.ClientError):
        return True
    msg = str(exc).lower()
    return any(k in msg for k in ("timeout", "connection", "reset", "refused", "broken pipe", "429"))


async def _async_backoff_sleep(attempt: int) -> None:
    exp = min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** attempt))
    jitter = random.uniform(0.0, 0.4)
    await asyncio.sleep(exp + jitter)


async def _session_get_text(session, url, semaphore, encoding="gbk"):
    """带指数退避的 GET → text；失败返回 None。"""
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    last_exc = None
    for attempt in range(_HTTP_RETRY_ATTEMPTS):
        async with semaphore:
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 429 or resp.status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            tuple(resp.history),
                            status=resp.status,
                            headers=resp.headers,
                        )
                    return await resp.text(encoding=encoding)
            except BaseException as e:
                last_exc = e
                if attempt < _HTTP_RETRY_ATTEMPTS - 1 and _is_retriable_http_error(e):
                    logging.debug(
                        "api_fetcher GET 重试 %s/%s: %s",
                        attempt + 1,
                        _HTTP_RETRY_ATTEMPTS,
                        str(e)[:120],
                    )
                    await _async_backoff_sleep(attempt)
                else:
                    break
    if last_exc is not None:
        logging.debug("api_fetcher GET 放弃: %s", last_exc)
    return None


async def _session_get_json(session, url, semaphore):
    """带指数退避的 GET → json；失败返回 None。"""
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
    last_exc = None
    for attempt in range(_HTTP_RETRY_ATTEMPTS):
        async with semaphore:
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 429 or resp.status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            tuple(resp.history),
                            status=resp.status,
                            headers=resp.headers,
                        )
                    return await resp.json(content_type=None)
            except BaseException as e:
                last_exc = e
                if attempt < _HTTP_RETRY_ATTEMPTS - 1 and _is_retriable_http_error(e):
                    logging.debug(
                        "api_fetcher JSON 重试 %s/%s: %s",
                        attempt + 1,
                        _HTTP_RETRY_ATTEMPTS,
                        str(e)[:120],
                    )
                    await _async_backoff_sleep(attempt)
                else:
                    break
    if last_exc is not None:
        logging.debug("api_fetcher JSON 放弃: %s", last_exc)
    return None


async def _fetch_tencent_chunk(session, chunk, semaphore):
    """腾讯接口异步抓取（含量比、换手率字段）"""
    tc_codes = [f"{'sh' if str(c).split('.')[0][:6].startswith('6') else 'sz'}{str(c).split('.')[0][:6]}" for c in chunk]
    url = "http://qt.gtimg.cn/q=" + ",".join(tc_codes)

    try:
        text = await _session_get_text(session, url, semaphore, encoding="gbk")
        if text is None:
            return None
        results = {}
        for line in text.strip().split('\n'):
            if not line or '="' not in line:
                continue
            parts = line.split('="')[1].strip('";').split('~')
            if len(parts) > 49 and _safe_float(parts[3]) > 0:
                results[parts[2]] = {
                    'name': parts[1],
                    'price': _safe_float(parts[3]),
                    'pre_close': _safe_float(parts[4]),
                    'open': _safe_float(parts[5]),
                    'volume': _safe_float(parts[36]) * 100,
                    'high': _safe_float(parts[33]),
                    'low': _safe_float(parts[34]),
                    'amount': _safe_float(parts[37]) * 10000,  # 腾讯 amount 单位: 万
                    # 腾讯原生量比（相对5日均量），parts[49]（parts[39]=PE(TTM)，勿混用）
                    'vol_ratio': _safe_float(parts[49]) if len(parts) > 49 else 0.0,
                    # 腾讯原生换手率(%)，parts[38]
                    'turnover_rate_f': _safe_float(parts[38]) if len(parts) > 38 else 0.0,
                    # 额外提取：涨停价 parts[47]、跌停价 parts[48]、振幅 parts[43]、PE(TTM) parts[39]
                    'limit_up': _safe_float(parts[47]) if len(parts) > 47 else 0.0,
                    'limit_down': _safe_float(parts[48]) if len(parts) > 48 else 0.0,
                    'amplitude_pct': _safe_float(parts[43]) if len(parts) > 43 else 0.0,
                }
        return results
    except Exception as e:
        logging.debug(f"Tencent Fetch Error: {e}")
        return None


async def _fetch_em_chunk(session, chunk, semaphore):
    """东方财富接口异步抓取 (含 f6 成交额字段)"""
    em_secids = [f"{'1' if str(c).startswith('6') else '0'}.{str(c).split('.')[0][:6]}" for c in chunk]
    url = f"http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&fields=f2,f5,f6,f12,f14,f15,f16,f17,f18&secids={','.join(em_secids)}"

    try:
        data_json = await _session_get_json(session, url, semaphore)
        if data_json is None:
            return None
        results = {}
        if 'data' in data_json and data_json['data'] and 'diff' in data_json['data']:
            for item in data_json['data']['diff']:
                if not isinstance(item, dict):
                    continue
                code_key = item.get('f12', '')
                price = item.get('f2', 0)
                if not code_key or str(price) == '-' or _safe_float(price) <= 0:
                    continue
                results[code_key] = {
                    'name': item.get('f14', ''),
                    'price': _safe_float(price),
                    'pre_close': _safe_float(item.get('f18', 0)),
                    'open': _safe_float(item.get('f17', 0)),
                    'volume': _safe_float(item.get('f5', 0)) * 100,
                    'amount': _safe_float(item.get('f6', 0)),
                    'high': _safe_float(item.get('f15', 0)),
                    'low': _safe_float(item.get('f16', 0))
                }
        return results
    except Exception as e:
        logging.debug(f"EastMoney Fetch Error: {e}")
        return None


async def _fetch_sina_chunk(session, chunk, semaphore):
    """新浪接口异步抓取"""
    tc_codes = [f"{'sh' if str(c).split('.')[0][:6].startswith('6') else 'sz'}{str(c).split('.')[0][:6]}" for c in chunk]
    url = "http://hq.sinajs.cn/list=" + ",".join(tc_codes)

    try:
        text = await _session_get_text(session, url, semaphore, encoding="gbk")
        if text is None:
            return None
        results = {}
        for line in text.strip().split('\n'):
            if not line or '="' not in line:
                continue
            left, right = line.split('="')
            code_key = left[-6:]
            parts = right.strip('";').split(',')
            if len(parts) > 30 and _safe_float(parts[3]) > 0:
                results[code_key] = {
                    'name': parts[0],
                    'price': _safe_float(parts[3]),
                    'pre_close': _safe_float(parts[2]),
                    'open': _safe_float(parts[1]),
                    'volume': _safe_float(parts[8]),
                    'amount': _safe_float(parts[9]),
                    'high': _safe_float(parts[4]),
                    'low': _safe_float(parts[5])
                }
        return results
    except Exception as e:
        logging.debug(f"Sina Fetch Error: {e}")
        return None


async def _fetch_realtime_batch_async(codes):
    """
    【V26.6 数据源并行+字段合并优化】
    - 三源（东财/腾讯/新浪）全部并行发起请求，总耗时由最慢源决定
    - 东财优先：price/volume/pre_close/open/high/low/amount
    - 腾讯补充：vol_ratio / turnover_rate_f（东财不提供这两个字段）
    - 新浪兜底：基础行情兜底
    """
    clean_targets = [c for c in codes if not c.endswith('.BJ')]
    if not clean_targets:
        return {}

    # code -> [dict_from_em, dict_from_tc, dict_from_sina]
    _all_source_results: Dict[str, List[Dict]] = {}
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def _fetch_all_sources_for_chunk(session, chunk):
        """三个源全部并行，返回 {code: [snap_em, snap_tc, snap_sina]}"""
        em_task = _fetch_em_chunk(session, chunk, semaphore)
        tc_task = _fetch_tencent_chunk(session, chunk, semaphore)
        sina_task = _fetch_sina_chunk(session, chunk, semaphore)
        em_res, tc_res, sina_res = await asyncio.gather(em_task, tc_task, sina_task, return_exceptions=True)
        for res in (em_res, tc_res, sina_res):
            if isinstance(res, dict):
                for code, snap in res.items():
                    if code not in _all_source_results:
                        _all_source_results[code] = []
                    _all_source_results[code].append(snap)

    # TCP 连接池优化
    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        chunks = [clean_targets[i:i + 50] for i in range(0, len(clean_targets), 50)]
        tasks = [_fetch_all_sources_for_chunk(session, chunk) for chunk in chunks]
        await asyncio.gather(*tasks, return_exceptions=True)

    # 字段合并：东财为主，腾讯补充 vol_ratio/turnover_rate_f
    rt_map = {}
    for code, snaps in _all_source_results.items():
        merged = {}
        em_snap = next((s for s in snaps if s is not None and s.get('price', 0) > 0), {})
        tc_snap = next((s for s in snaps if s is not None and 'turnover_rate_f' in s), {})
        if em_snap:
            merged.update(em_snap)
        else:
            merged.update({k: v for s in snaps if s for k, v in s.items() if v})

        # 腾讯补充：vol_ratio / turnover_rate_f / limit_up / limit_down / amplitude_pct
        if tc_snap:
            if not merged.get('vol_ratio'):
                merged['vol_ratio'] = tc_snap.get('vol_ratio', 0.0)
            if not merged.get('turnover_rate_f'):
                merged['turnover_rate_f'] = tc_snap.get('turnover_rate_f', 0.0)
            # 涨跌停价：腾讯直接提供（东财/新浪均无此字段），优先使用
            merged.setdefault('limit_up', 0.0)
            merged.setdefault('limit_down', 0.0)
            if merged['limit_up'] <= 0:
                merged['limit_up'] = tc_snap.get('limit_up', 0.0)
            if merged['limit_down'] <= 0:
                merged['limit_down'] = tc_snap.get('limit_down', 0.0)
            # 振幅
            if not merged.get('amplitude_pct'):
                merged['amplitude_pct'] = tc_snap.get('amplitude_pct', 0.0)

        if merged.get('name'):
            merged['name'] = normalize_stock_display_name(merged['name'])
        rt_map[code] = merged

    return rt_map


def fetch_realtime_batch(codes):
    """
    👑 老法师封装：同步包装器 (Drop-in Replacement)
    外部调用方(如旧版引擎或UI)无需任何修改，直接调用此函数。
    内部会自动建立独立的事件循环，全速压榨异步并发性能。
    """
    if not codes:
        return {}

    # Daemon/后台路径：严格不碰 Streamlit ScriptRunContext，直接走纯后端异步执行。
    if os.environ.get("XIAOJIE_DAEMON_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
        try:
            return asyncio.run(_fetch_realtime_batch_async(codes))
        except Exception:
            logging.exception("fetch_realtime_batch: daemon 模式异步抓取失败，返回空 dict")
            return {}

    # 【审计修复】维度5-实时批量抓取顶层兜底，异常时返回空映射避免调用链中断
    try:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # 当前线程无运行中的事件循环：直接用 asyncio.run（Python 3.7+ 推荐路径，避免弃用 get_event_loop）
            return asyncio.run(_fetch_realtime_batch_async(codes))

        # UI 等环境已有运行中的 loop：子线程内 asyncio.run，避免与主线程 loop 冲突。
        # 仅在明确非 daemon 模式下尝试挂载 ScriptRunContext。
        parent_ctx = None
        try:
            from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx

            parent_ctx = get_script_run_ctx(suppress_warning=True)
        except Exception:
            parent_ctx = None

        def _run_async_in_worker():
            if parent_ctx is not None:
                try:
                    from streamlit.runtime.scriptrunner import add_script_run_ctx

                    add_script_run_ctx(threading.current_thread(), parent_ctx)
                except Exception:
                    pass
            return asyncio.run(_fetch_realtime_batch_async(codes))

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_run_async_in_worker)
            return future.result()
    except Exception:
        logging.exception("fetch_realtime_batch: 异步抓取失败，返回空 dict")
        return {}


if __name__ == "__main__":
    # 极速测试小跑
    test_codes = ["600519.SH", "000858.SZ", "300308.SZ"]
    import time
    t1 = time.time()
    res = fetch_realtime_batch(test_codes)
    print(f"✅ 异步雷达测试完毕, 耗时: {time.time()-t1:.3f}秒, 抓取数据: {len(res)} 条")
