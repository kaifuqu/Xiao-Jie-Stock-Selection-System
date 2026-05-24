# -*- coding: utf-8 -*-
"""
大盘指数熔断探测（原 SOP 模块残留名 sop_v11，与 config.yaml 节名兼容）。

供指挥舱 P4 扫描前预检、`service.async_scan_bridge`、`core.scan_engine` 元数据使用。
已移除：交易纪律日志、背书池、宽松档底线校验、疲劳日记录等与 UI 截图三功能相关的全部逻辑。
"""
from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import yaml

_BJ_TZ = timezone(timedelta(hours=8))
_BREAKER_LOCK = threading.Lock()
_BREAKER_CACHE: Dict[str, Any] = {"t": 0.0, "data": None}
_BREAKER_TTL_SEC = 45.0
_TUSHARE_INDEX_TIMEOUT_SEC = 10.0


def _project_root() -> str:
    d = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        if os.path.exists(os.path.join(d, "config.yaml")):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


_CONFIG_PATH = None


def _config_path() -> str:
    global _CONFIG_PATH
    if _CONFIG_PATH is None:
        _CONFIG_PATH = os.path.join(_project_root(), "config.yaml")
    return _CONFIG_PATH


def load_sop_v11_config() -> dict:
    """读取 config.yaml 的 sop_v11 节（熔断、observation_pool_relax 等）。"""
    try:
        with open(_config_path(), "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        node = raw.get("sop_v11")
        return dict(node) if isinstance(node, dict) else {}
    except Exception as e:
        logging.debug("load_sop_v11_config: %s", e)
        return {}


def _parse_hhmm(s: str) -> Optional[tuple]:
    import re

    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(s or "").strip())
    if not m:
        return None
    h, mm = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 23 and 0 <= mm <= 59:
        return h, mm
    return None


def _fetch_em_index_pct(secid: str) -> Optional[float]:
    """东财 f170 一般为涨跌幅 * 100（例如 -1.25% → -125）；结构异常时返回 None，不抛。"""
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={urllib.parse.quote(secid)}&fields=f170,f58,f60"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4.0) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        d = data.get("data") if isinstance(data, dict) else None
        if not isinstance(d, dict):
            return None
        f170 = d.get("f170")
        if f170 is None or f170 == "-":
            return None
        try:
            v = float(f170) / 100.0
        except (TypeError, ValueError):
            return None
        return v
    except Exception as e:
        logging.debug("_fetch_em_index_pct %s: %s", secid, e)
        return None


def _fetch_index_pct_tushare_fallback(ts_code: str) -> Optional[float]:
    """
    东财不可达时，用 Tushare index_daily 最近一根 K 线的 pct_chg（非盘中实时，仅作熔断参考）。
    整段在子线程执行并带硬超时，避免专线/断网无限阻塞。
    """

    def _worker() -> Optional[float]:
        try:
            import tushare as ts

            with open(_config_path(), "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            tcfg = raw.get("tushare") or {}
            token = str(tcfg.get("token", "") or "").strip()
            if not token:
                return None
            ts.set_token(token)
            pro = ts.pro_api()
            ep = str(tcfg.get("custom_endpoint", "") or "").strip()
            if ep:
                pro._DataApi__http_url = ep
            end_d = datetime.now(_BJ_TZ).strftime("%Y%m%d")
            start_d = (datetime.now(_BJ_TZ) - timedelta(days=20)).strftime("%Y%m%d")
            df = pro.index_daily(ts_code=str(ts_code).strip(), start_date=start_d, end_date=end_d)
            if df is None or df.empty:
                return None
            df = df.sort_values("trade_date").reset_index(drop=True)
            last = df.iloc[-1]
            if "pct_chg" not in last.index:
                return None
            return float(last["pct_chg"])
        except Exception:
            return None

    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_worker)
            return fut.result(timeout=_TUSHARE_INDEX_TIMEOUT_SEC)
    except FuturesTimeout:
        logging.debug("_fetch_index_pct_tushare_fallback: timeout %s", ts_code)
        return None
    except Exception as e:
        logging.debug("_fetch_index_pct_tushare_fallback outer %s: %s", ts_code, e)
        return None


def evaluate_market_circuit_breaker(
    now: Optional[datetime] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """
    返回 advisory 结构：
      active: 是否满足「时间窗 + 跌幅阈值」
      details: 各指数涨跌幅
      message: 人类可读
    use_cache: 为 True 时 45s 内复用上次数值，避免扫描引擎每票重复拉指数。
    """
    if use_cache:
        t_now = time.monotonic()
        with _BREAKER_LOCK:
            prev = _BREAKER_CACHE.get("data")
            prev_t = float(_BREAKER_CACHE.get("t") or 0.0)
        if isinstance(prev, dict) and (t_now - prev_t) < _BREAKER_TTL_SEC:
            return dict(prev)

    cfg = load_sop_v11_config()
    out: Dict[str, Any] = {
        "active": False,
        "details": {},
        "message": "",
        "enforce_block_p4": bool((cfg.get("circuit_breaker") or {}).get("enforce_block_p4", False)),
        "skipped": "",
    }
    if not cfg.get("enabled", True):
        out["skipped"] = "sop_v11.disabled"
        return out

    cb = cfg.get("circuit_breaker")
    if not isinstance(cb, dict) or not cb.get("enabled", True):
        out["skipped"] = "circuit_breaker.disabled"
        return out

    now = now or datetime.now(_BJ_TZ)
    hhmm = _parse_hhmm(str(cb.get("active_after_hhmm", "14:30")))
    if hhmm:
        h, m = hhmm
        if now.hour < h or (now.hour == h and now.minute < m):
            out["skipped"] = "before_active_window"
            return out

    thr = cb.get("index_thresholds_pct")
    if not isinstance(thr, dict):
        out["skipped"] = "no_thresholds"
        return out

    sec_map = cfg.get("index_secid_map")
    if not isinstance(sec_map, dict):
        sec_map = {"000300.SH": "1.000300", "399006.SZ": "0.399006"}

    triggered = []
    details: Dict[str, Any] = {}
    for ts_code, limit in thr.items():
        sid = sec_map.get(str(ts_code).strip())
        if not sid:
            continue
        try:
            lim = float(limit)
        except (TypeError, ValueError):
            continue
        pct = _fetch_em_index_pct(str(sid))
        feed = "eastmoney"
        if pct is None:
            pct = _fetch_index_pct_tushare_fallback(str(ts_code))
            feed = "tushare_daily" if pct is not None else "unavailable"
        details[str(ts_code)] = {"pct": pct, "threshold": lim, "secid": sid, "feed": feed}
        if pct is None:
            continue
        if pct <= lim:
            triggered.append(f"{ts_code} {pct:.2f}% ≤ {lim}%")

    if triggered:
        out["active"] = True
        out["details"] = details
        out["message"] = "系统级熔断（指数）：" + "；".join(triggered) + "。建议 14:30 后暂停宽松捞票 / 慎做 P4。"
    else:
        out["details"] = details

    if use_cache:
        with _BREAKER_LOCK:
            _BREAKER_CACHE["t"] = time.monotonic()
            _BREAKER_CACHE["data"] = out

    return out
