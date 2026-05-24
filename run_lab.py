# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 策略实验室独立入口（与实盘 ui/app.py 进程隔离，避免重型科研组件与盯盘大屏争用内存）。

运行：在项目根目录执行
  streamlit run run_lab.py
"""
from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import streamlit as st

try:
    import constants
except ImportError:

    class _ConstShim:
        APP_VERSION = "V26.5"

    constants = _ConstShim()

st.set_page_config(
    page_title="小杰AI选股系统 Pro V26.5 - 策略实验室",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

try:
    import ui.ui_components as ui_components

    ui_components.inject_custom_css()
except Exception:
    pass

from core.regime_analyzer import get_market_regime

regime_data = get_market_regime()
prim = regime_data["primary"]
raw_status = prim["status"]
if "主升" in raw_status:
    st.session_state["market_regime"] = "主升浪"
elif "退潮" in raw_status or "防守" in raw_status:
    st.session_state["market_regime"] = "情绪退潮市"
else:
    st.session_state["market_regime"] = "震荡市"

curr_regime = st.session_state.get("market_regime", "震荡市")
st.session_state["market_sentiment"] = str(regime_data.get("sentiment_key") or "平稳")

progress_placeholder = st.empty()
progress_bar = st.empty()

from ui.strategy_lab import render_strategy_lab

render_strategy_lab(
    curr_regime=curr_regime,
    progress_placeholder=progress_placeholder,
    progress_bar=progress_bar,
)
