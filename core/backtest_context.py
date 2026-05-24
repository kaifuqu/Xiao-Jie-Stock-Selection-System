# -*- coding: utf-8 -*-
"""
回测专用全局上下文（与实盘 Streamlit / 扫描引擎隔离）。

用途：
- 专项痛点回测（backtest_runner --mode=painpoint）在批次开始前调用
  set_backtest_legacy_mode(True)，使 P3/P5/风控中注册的 legacy 分支生效；
- 默认 False，保证日常实盘与未设标志的 CLI 行为与历史一致。

注意：本模块不得被 data_fetcher / DuckDB 路径导入，避免隐式副作用。
"""
from __future__ import annotations

import threading
from typing import Optional

_lock = threading.RLock()
_legacy: bool = False


def set_backtest_legacy_mode(enabled: Optional[bool]) -> None:
    """设置是否启用「旧版硬性否决/扣分」回测分支；None 视为 False。"""
    global _legacy
    with _lock:
        _legacy = bool(enabled) if enabled is not None else False


def is_backtest_legacy_mode() -> bool:
    """当前进程是否处于 legacy 回测模式。"""
    with _lock:
        return bool(_legacy)


def reset_backtest_context() -> None:
    """测试或脚本结束时清零（可选调用）。"""
    set_backtest_legacy_mode(False)
