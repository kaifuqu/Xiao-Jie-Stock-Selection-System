# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 — 物理总控台跨进程状态（Streamlit UI ↔ auto_sniper_daemon）

【性能优化 V2】
1. TTL缓存：read_master_control() 添加 5 秒 TTL 内存缓存。
   每次扫描调用时（如 P2/P3 每 2.5 分钟），5 个 get_* 函数不再各自执行一次
   文件锁+JSON解析，而是共享缓存结果，将 2-5 次/扫描 的 I/O+锁竞争降至 0。
2. 写失效：write_master_control() 写入后主动清除缓存，下次读取时重新加载。
3. 写入锁内清除：确保写入锁释放前缓存已失效，避免读写竞态。

持久化：data/runtime/state/master_control.json
并发：独立 .lock 文件 + 平台文件锁；写入采用临时文件 + os.replace 原子替换。

缺省（文件不存在或字段缺失）：企微推送、全自动巡航、P1≥75 推送均为 True（全开）；
若需关闭高分底仓摘要可在总控取消勾选。
维护模式 maintenance_mode 缺省为 False；读写合并时始终保留该键，避免被其它字段写入冲掉。
"""
from __future__ import annotations

# Standard library
import json
import logging
import os
import sys
import tempfile
import threading
import time
from typing import Any, Dict

# Local modules
logger = logging.getLogger(__name__)

_DEFAULTS: Dict[str, bool] = {
    "wechat_push_enabled": True,
    "daemon_auto_cruise_enabled": True,
    "push_p1_high_score_enabled": True,
    "wechat_system_alert_enabled": True,
    "maintenance_mode": False,
}

# ---------------------------------------------------------------------------
# 【性能优化 V2】TTL 缓存：避免每次 get_* 调用都执行文件锁+JSON解析
# ---------------------------------------------------------------------------
# 缓存条目：{"data": {...}, "expires_at": time.time() + TTL}
_MC_CACHE: Dict[str, Any] = {}
_MC_CACHE_TTL_SEC: float = 5.0  # 5 秒缓存，过期后重新读取
_MC_CACHE_LOCK = threading.Lock()


def _mc_cache_get() -> tuple:
    """读取缓存（线程安全），返回 (data, valid) 元组。"""
    with _MC_CACHE_LOCK:
        entry = _MC_CACHE.get("entry")
        if entry is None:
            return None, False
        expires_at = entry.get("expires_at", 0.0)
        if time.time() < expires_at:
            return entry.get("data"), True
        return None, False


def _mc_cache_set(data: Dict[str, Any]) -> None:
    """设置缓存（线程安全），写入锁后清除缓存。"""
    with _MC_CACHE_LOCK:
        _MC_CACHE["entry"] = {
            "data": dict(data),
            "expires_at": time.time() + _MC_CACHE_TTL_SEC,
        }


def _mc_cache_invalidate() -> None:
    """使缓存失效（写入后调用，确保下次读取重新加载）。"""
    with _MC_CACHE_LOCK:
        _MC_CACHE.clear()


# ---------------------------------------------------------------------------
# 全局缺省配置
# ---------------------------------------------------------------------------
maintenance_mode: bool = False


def _state_path() -> str:
    from core.runtime_data_paths import STATE_DIR, ensure_runtime_data_layout

    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "master_control.json")


def get_master_control_state_path() -> str:
    return _state_path()


def _lock_file_path() -> str:
    return _state_path() + ".lock"


def _ensure_lock_region(fp: Any) -> None:
    fp.seek(0, os.SEEK_END)
    if fp.tell() == 0:
        fp.write(b"\0")
        fp.flush()
    fp.seek(0)


def _lock_acquire(fp: Any) -> None:
    if sys.platform == "win32":
        import msvcrt

        _ensure_lock_region(fp)
        fp.seek(0)
        msvcrt.locking(fp.fileno(), msvcrt.LK_LOCK, 1)
    else:
        import fcntl

        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)


def _lock_release(fp: Any) -> None:
    try:
        if sys.platform == "win32":
            import msvcrt

            fp.seek(0)
            msvcrt.locking(fp.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
    except OSError as e:
        logger.debug("master_control 释放锁: %s", e)


def read_master_control() -> Dict[str, Any]:
    """
    【性能优化 V2】
    - 先查 TTL 缓存（5 秒内多次调用共享结果，消除重复文件锁+JSON解析）
    - 缓存命中时直接返回，不再执行任何 I/O 操作
    - 缓存未命中时执行原逻辑（加锁、读文件、解析），结果存入缓存

    返回包含 wechat_push_enabled、daemon_auto_cruise_enabled、maintenance_mode
    及可选元数据的 dict。
    """
    # 【优化V2】快速路径：TTL 缓存命中
    cached_data, valid = _mc_cache_get()
    if valid and cached_data is not None:
        return cached_data

    # 慢速路径：缓存未命中，执行原逻辑
    path = _state_path()
    lk = _lock_file_path()
    os.makedirs(os.path.dirname(lk), exist_ok=True)
    out: Dict[str, Any] = {**_DEFAULTS}
    try:
        with open(lk, "a+b") as lf:
            _lock_acquire(lf)
            try:
                if os.path.isfile(path):
                    try:
                        with open(path, "r", encoding="utf-8") as jf:
                            raw = json.load(jf)
                        if isinstance(raw, dict):
                            for k in _DEFAULTS:
                                if k in raw:
                                    out[k] = bool(raw[k])
                            ua = raw.get("updated_at")
                            if ua is not None:
                                out["updated_at"] = ua
                    except Exception as e:
                        logger.warning("读取 master_control.json 异常，使用缺省: %s", e)
                # 【优化V2】结果写入缓存（每次重新读取后更新）
                _mc_cache_set(out)
                return out
            finally:
                _lock_release(lf)
    except Exception as e:
        logger.warning("master_control 读锁失败，返回缺省配置: %s", e)
        return {**_DEFAULTS}


def write_master_control(**kwargs: Any) -> Dict[str, Any]:
    """
    合并写入；仅识别 _DEFAULTS 中的键（含 maintenance_mode）。
    写前完整读回磁盘，再与 kwargs 合并，确保不会把未出现在 kwargs 里的 maintenance_mode 等键抹掉。

    【性能优化 V2】
    - 写入后主动清除缓存（_mc_cache_invalidate），
      确保下次 read_master_control 重新读取最新值。
    """
    path = _state_path()
    lk = _lock_file_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    os.makedirs(os.path.dirname(lk), exist_ok=True)
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        ts = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
    except Exception:
        ts = str(time.time())

    with open(lk, "a+b") as lf:
        _lock_acquire(lf)
        try:
            data = {**_DEFAULTS}
            if os.path.isfile(path):
                try:
                    with open(path, "r", encoding="utf-8") as jf:
                        raw = json.load(jf)
                    if isinstance(raw, dict):
                        for k in _DEFAULTS:
                            if k in raw:
                                data[k] = bool(raw[k])
                except Exception as e:
                    logger.warning("写前读 master_control 失败，将覆盖为缺省+patch: %s", e)
            for k, v in kwargs.items():
                if k in _DEFAULTS and v is not None:
                    data[k] = bool(v)
            payload = dict(data)
            payload["updated_at"] = ts

            dname = os.path.dirname(path) or "."
            fd, tmp = tempfile.mkstemp(prefix=".mc_", suffix=".json", dir=dname, text=True)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as wf:
                    json.dump(payload, wf, ensure_ascii=False, indent=0)
                os.replace(tmp, path)
            except Exception:
                try:
                    if os.path.isfile(tmp):
                        os.unlink(tmp)
                except OSError:
                    pass
                raise
            # 【优化V2】写入成功后清除缓存，下次读取时重新加载最新值
            _mc_cache_invalidate()
            return payload
        finally:
            _lock_release(lf)


def is_wechat_push_master_enabled() -> bool:
    return bool(read_master_control().get("wechat_push_enabled", True))


def is_daemon_auto_cruise_enabled() -> bool:
    return bool(read_master_control().get("daemon_auto_cruise_enabled", True))


def is_push_p1_high_score_enabled() -> bool:
    return bool(read_master_control().get("push_p1_high_score_enabled", False))


def is_wechat_system_alert_enabled() -> bool:
    return bool(read_master_control().get("wechat_system_alert_enabled", True))


def is_maintenance_mode_enabled() -> bool:
    try:
        return bool(read_master_control().get("maintenance_mode", False))
    except Exception:
        return False
