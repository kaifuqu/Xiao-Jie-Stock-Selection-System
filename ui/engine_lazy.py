# -*- coding: utf-8 -*-
"""
指挥舱重型依赖延迟加载：首次调用时再 import，缩短 Streamlit 冷启动首包时间。
sys.modules 会缓存已加载模块，后续调用无额外开销。
"""
from __future__ import annotations

from typing import Any

_scan_engine_mod = None


def _scan_engine():
    global _scan_engine_mod
    if _scan_engine_mod is None:
        import core.scan_engine as m

        _scan_engine_mod = m
    return _scan_engine_mod


def run_scan_engine(*args: Any, **kwargs: Any):
    return _scan_engine().run_scan_engine(*args, **kwargs)


def get_realtime_sector_ranking(*args: Any, **kwargs: Any):
    return _scan_engine().get_realtime_sector_ranking(*args, **kwargs)


def fetch_realtime_batch(*args: Any, **kwargs: Any):
    from data.api_fetcher import fetch_realtime_batch as f

    return f(*args, **kwargs)


def run_batch_backtest(*args: Any, **kwargs: Any):
    from core.backtest_runner import run_batch_backtest as f

    return f(*args, **kwargs)


def build_p1_pool_and_cache(*args: Any, **kwargs: Any):
    from core.pool_manager import build_p1_pool_and_cache as f

    return f(*args, **kwargs)


def get_last_p1_observation_pool(*args: Any, **kwargs: Any):
    from core.pool_manager import get_last_p1_observation_pool as f

    return f(*args, **kwargs)


def get_last_p1_wash_adaptive(*args: Any, **kwargs: Any):
    from core.pool_manager import get_last_p1_wash_adaptive as f

    return f(*args, **kwargs)


def get_all_stock_codes(*args: Any, **kwargs: Any):
    from data.db_core import get_all_stock_codes as f

    return f(*args, **kwargs)


def get_stock_data_qfq(*args: Any, **kwargs: Any):
    from data.db_core import get_stock_data_qfq as f

    return f(*args, **kwargs)


def get_p1_candidate_codes(*args: Any, **kwargs: Any):
    from data.db_core import get_p1_candidate_codes as f

    return f(*args, **kwargs)


def save_p1_cache(*args: Any, **kwargs: Any):
    from data.db_core import save_p1_cache as f

    return f(*args, **kwargs)


def load_p1_cache(*args: Any, **kwargs: Any):
    from data.db_core import load_p1_cache as f

    return f(*args, **kwargs)


def get_stock_industry(*args: Any, **kwargs: Any):
    from data.db_core import get_stock_industry as f

    return f(*args, **kwargs)


def get_latest_sector_ranking(*args: Any, **kwargs: Any):
    from data.db_core import get_latest_sector_ranking as f

    return f(*args, **kwargs)


def get_all_basic_industry(*args: Any, **kwargs: Any):
    from data.db_core import get_all_basic_industry as f

    return f(*args, **kwargs)


def precompute_indicators(*args: Any, **kwargs: Any):
    from core.indicator_calc import precompute_indicators as f

    return f(*args, **kwargs)


def dehydrate_base_items_list(*args: Any, **kwargs: Any):
    from ui.session_cache_dehydrate import dehydrate_base_items_list as f

    return f(*args, **kwargs)


def dehydrate_scan_nested_fragment(*args: Any, **kwargs: Any):
    from ui.session_cache_dehydrate import dehydrate_scan_nested_fragment as f

    return f(*args, **kwargs)


def dehydrate_scan_results_list(*args: Any, **kwargs: Any):
    from ui.session_cache_dehydrate import dehydrate_scan_results_list as f

    return f(*args, **kwargs)


def rehydrate_base_items_for_scan_engine(*args: Any, **kwargs: Any):
    from ui.session_cache_dehydrate import rehydrate_base_items_for_scan_engine as f

    return f(*args, **kwargs)
