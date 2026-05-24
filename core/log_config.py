# -*- coding: utf-8 -*-
"""
守护进程 / 工具脚本共用的日志落盘配置。

默认落盘路径：``data/runtime/sniper.log``（与 ``core.runtime_data_paths.RUNTIME_DIR`` 对齐）。
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo


class _MissingScriptRunContextFilter(logging.Filter):
    """屏蔽 Streamlit 后台线程里已知无害的 ScriptRunContext 噪音。"""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            return "missing ScriptRunContext" not in record.getMessage()
        except Exception:
            return True

_SNIPER_LOG_SETUP_DONE = False
_BJ_TZ = ZoneInfo("Asia/Shanghai")


class _ShanghaiFormatter(logging.Formatter):
    """强制按北京时间格式化日志时间，避免系统时区显示为 UTC。"""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=_BJ_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


def path_sniper_rotating_log() -> str:
    """返回 ``data/runtime/sniper.log`` 绝对路径。"""
    try:
        from core.runtime_data_paths import RUNTIME_DIR, ensure_runtime_data_layout

        ensure_runtime_data_layout()
        return os.path.join(RUNTIME_DIR, "sniper.log")
    except Exception:
        here = os.path.dirname(os.path.abspath(__file__))
        root = os.path.dirname(here)
        d = os.path.join(root, "data", "runtime")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, "sniper.log")


def setup_sniper_rotating_file_logging(
    level: int = logging.INFO,
    *,
    also_stream: bool = True,
) -> None:
    """
    为 root logger 配置 ``sniper.log`` 的 RotatingFileHandler，并可选附加 stderr StreamHandler。

    开启日志轮转，防止 7x24 纯后台模式下日志文件无限膨胀撑爆磁盘。
    单文件上限 50MB、最多 10 个历史备份、UTF-8 编码。
    """
    global _SNIPER_LOG_SETUP_DONE
    if _SNIPER_LOG_SETUP_DONE:
        return

    log_path = path_sniper_rotating_log()
    parent = os.path.dirname(log_path) or "."
    os.makedirs(parent, exist_ok=True)

    fmt = _ShanghaiFormatter(
        "%(asctime)s [SNIPER] %(levelname)s %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    # 开启日志轮转，防止 7x24 纯后台模式下日志文件无限膨胀撑爆磁盘
    rh = RotatingFileHandler(
        log_path,
        maxBytes=50 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    rh.setFormatter(fmt)

    root = logging.getLogger()
    for h in list(root.handlers):
        try:
            root.removeHandler(h)
        except Exception:
            pass
    root.setLevel(level)
    root.addHandler(rh)
    root.addFilter(_MissingScriptRunContextFilter())
    if also_stream:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        sh.addFilter(_MissingScriptRunContextFilter())
        root.addHandler(sh)

    _SNIPER_LOG_SETUP_DONE = True
