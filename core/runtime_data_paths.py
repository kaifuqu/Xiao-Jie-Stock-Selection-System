# -*- coding: utf-8 -*-
"""
运行时数据文件路径：按类型分目录，避免 data/ 根目录堆积按日生成的缓存。

布局（DuckDB 等仍留在 data/ 根目录）：
  data/runtime/pool_cache/   — p0/p1 按日缓存 *.json / *.pkl
  data/runtime/p1_gene/      — p1_gene_YYYYMMDD.json
  data/runtime/state/        — 黑名单、板块史、洗盘日报、快照、阵亡名单、实验室候选元数据等
"""
from __future__ import annotations

import glob
import logging
import os
import shutil
from typing import List

_migrated: bool = False


def project_root() -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        if os.path.exists(os.path.join(current_dir, "config.yaml")):
            return current_dir
        parent = os.path.dirname(current_dir)
        if parent == current_dir:
            break
        current_dir = parent
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


DATA_DIR = os.path.join(project_root(), "data")
RUNTIME_DIR = os.path.join(DATA_DIR, "runtime")
POOL_CACHE_DIR = os.path.join(RUNTIME_DIR, "pool_cache")
P1_GENE_DIR = os.path.join(RUNTIME_DIR, "p1_gene")
STATE_DIR = os.path.join(RUNTIME_DIR, "state")
SCAN_ASYNC_DIR = os.path.join(RUNTIME_DIR, "scan_async")


def _mkdirs() -> None:
    os.makedirs(POOL_CACHE_DIR, exist_ok=True)
    os.makedirs(P1_GENE_DIR, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(SCAN_ASYNC_DIR, exist_ok=True)


def _move_if_needed(src: str, dst: str) -> None:
    if not os.path.isfile(src):
        return
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.isfile(dst):
        try:
            os.remove(src)
        except OSError:
            pass
        return
    try:
        shutil.move(src, dst)
    except OSError as e:
        logging.warning("迁移运行时文件失败 %s -> %s: %s", src, dst, e)


def _migrate_legacy_from_data_root() -> None:
    """将原先落在 data/ 根目录的文件迁入 runtime/（仅搬一次/进程）。"""
    static_map = {
        "blacklist.json": STATE_DIR,
        "sector_rank_history.json": STATE_DIR,
        "wash_metrics_history.json": STATE_DIR,
        "intraday_snapshots.json": STATE_DIR,
        "p1_rejected_cache.json": STATE_DIR,
        "p1_rejected_cache.pkl": STATE_DIR,
        "p0_rejected_cache.json": STATE_DIR,
        "p0_rejected_cache.pkl": STATE_DIR,
        "p1_last_wash_input_codes.json": STATE_DIR,
    }
    for name, dest_dir in static_map.items():
        _move_if_needed(os.path.join(DATA_DIR, name), os.path.join(dest_dir, name))

    for pattern, dest_dir in (
        ("p1_cache_*.json", POOL_CACHE_DIR),
        ("p1_cache_*.pkl", POOL_CACHE_DIR),
        ("p0_cache_*.json", POOL_CACHE_DIR),
        ("p0_cache_*.pkl", POOL_CACHE_DIR),
        ("p1_gene_*.json", P1_GENE_DIR),
    ):
        for src in glob.glob(os.path.join(DATA_DIR, pattern)):
            if not os.path.isfile(src):
                continue
            base = os.path.basename(src)
            _move_if_needed(src, os.path.join(dest_dir, base))


def ensure_runtime_data_layout() -> None:
    global _migrated
    _mkdirs()
    if not _migrated:
        _migrate_legacy_from_data_root()
        _migrated = True


# ---------- 对外路径（均先 ensure）----------


def path_wash_metrics_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "wash_metrics_history.json")


def path_blacklist_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "blacklist.json")


def path_sector_rank_history_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "sector_rank_history.json")


def path_strategic_mapping_debug_json(yyyymmdd: str) -> str:
    """动态行业贝塔自动学习映射调试缓存（按日覆盖）。"""
    ensure_runtime_data_layout()
    day = str(yyyymmdd or "").strip()
    if len(day) != 8 or (not day.isdigit()):
        day = "latest"
    return os.path.join(STATE_DIR, f"strategic_mapping_debug_{day}.json")


def path_intraday_snapshots_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "intraday_snapshots.json")


def path_alert_dedup_cache_json() -> str:
    """
    企微「系统运维告警」跨进程去重账本（notify_wechat_system_alert）。
    与内存无关，守护进程 / Streamlit / 工具脚本共享同一 JSON，由 core.file_utils.atomic_json_update 更新。
    """
    ensure_runtime_data_layout()
    return os.path.join(RUNTIME_DIR, "alert_dedup_cache.json")


def path_wechat_signal_dedup_cache_json() -> str:
    """
    企微股票信号跨进程去重账本。
    用于 P3/P4 「同池同代码当日仅推一次」硬去重，避免 daemon 与 UI 同时扫描时重复刷屏。
    """
    ensure_runtime_data_layout()
    return os.path.join(RUNTIME_DIR, "wechat_signal_dedup_cache.json")


def path_p1_rejected_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "p1_rejected_cache.json")


def path_p1_rejected_pkl() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "p1_rejected_cache.pkl")


def path_p0_rejected_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "p0_rejected_cache.json")


def path_p0_rejected_pkl() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "p0_rejected_cache.pkl")


def path_p1_last_wash_input_codes_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(STATE_DIR, "p1_last_wash_input_codes.json")


def path_p1_cache_json(yyyymmdd: str) -> str:
    ensure_runtime_data_layout()
    return os.path.join(POOL_CACHE_DIR, f"p1_cache_{yyyymmdd}.json")


def path_p1_cache_pkl(yyyymmdd: str) -> str:
    ensure_runtime_data_layout()
    return os.path.join(POOL_CACHE_DIR, f"p1_cache_{yyyymmdd}.pkl")


def path_p0_cache_json(yyyymmdd: str) -> str:
    ensure_runtime_data_layout()
    return os.path.join(POOL_CACHE_DIR, f"p0_cache_{yyyymmdd}.json")


def path_p0_cache_pkl(yyyymmdd: str) -> str:
    ensure_runtime_data_layout()
    return os.path.join(POOL_CACHE_DIR, f"p0_cache_{yyyymmdd}.pkl")


def path_p1_gene_json(yyyymmdd: str) -> str:
    ensure_runtime_data_layout()
    return os.path.join(P1_GENE_DIR, f"p1_gene_{yyyymmdd}.json")


def glob_p1_gene_json_paths() -> List[str]:
    ensure_runtime_data_layout()
    return sorted(glob.glob(os.path.join(P1_GENE_DIR, "p1_gene_*.json")))


def path_scan_async_dir() -> str:
    """P3/P4 异步扫描：队列与结果目录（指挥舱 UI 与 auto_sniper_daemon 共用）。"""
    ensure_runtime_data_layout()
    return SCAN_ASYNC_DIR


def path_scan_async_pending_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(SCAN_ASYNC_DIR, "pending.json")


def path_scan_async_running_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(SCAN_ASYNC_DIR, "running.json")


def path_scan_async_status_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(SCAN_ASYNC_DIR, "status.json")


def path_scan_async_latest_result_json() -> str:
    ensure_runtime_data_layout()
    return os.path.join(SCAN_ASYNC_DIR, "latest_result.json")


def path_scan_async_queue_consumer_filelock() -> str:
    """
    P3/P4 pending 队列消费互斥：filelock.FileLock 使用的锁文件路径（OS 级锁，进程崩溃/杀进程后自动释放）。
    auto_sniper_daemon 与 Streamlit 内嵌 ScanAsyncWorker 通过争抢此锁实现二选一。
    """
    ensure_runtime_data_layout()
    return os.path.join(SCAN_ASYNC_DIR, "pending_queue_consumer.filelock")
