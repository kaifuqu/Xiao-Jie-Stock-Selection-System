# -*- coding: utf-8 -*-
"""
Streamlit 后台线程 ScriptRunContext 注入
========================================
在 UI 进程内通过 threading.Thread 启动的守护线程，若未携带 ScriptRunContext，
部分 Streamlit 内部路径会刷屏警告「missing ScriptRunContext」。
在 t.start() 之前对本模块的封装调用一次即可（非 Streamlit 环境静默跳过）。
"""
from __future__ import annotations

import os
import threading


def _is_daemon_or_headless() -> bool:
    try:
        if os.environ.get("XIAOJIE_DAEMON_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
            return True
        return bool(os.environ.get("XIAOJIE_DAEMON_MODE_ONLY", "").strip().lower() in ("1", "true", "yes", "on"))
    except Exception:
        return False


def attach_script_run_ctx(thread: threading.Thread) -> threading.Thread:
    """
    将当前脚本运行上下文附加到子线程（等价于官方推荐的 add_script_run_ctx(thread)）。
    必须在创建线程的父线程中、在 thread.start() 之前调用。
    """
    if _is_daemon_or_headless():
        return thread
    try:
        from streamlit.runtime.scriptrunner import add_script_run_ctx

        add_script_run_ctx(thread)
    except Exception:
        pass
    return thread


__all__ = ["attach_script_run_ctx"]
