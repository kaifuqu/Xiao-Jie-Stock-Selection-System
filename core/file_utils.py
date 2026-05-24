# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 跨进程安全的 JSON 落盘工具（原子更新 + 自旋文件锁）。

用于 UI 与守护进程并发写同一 JSON（如 wash_metrics_history.json）时避免「读-改-写」丢更新。
"""
from __future__ import annotations

import json
import logging
import os
import pickle
import shutil
import time
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

_SAFE_PICKLE_ROOTS = (os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"),)


def is_safe_pickle_path(filepath: str) -> bool:
    """仅允许读取项目 data 目录下本进程生成的 pickle 缓存。"""
    try:
        abspath = os.path.abspath(filepath)
        return any(abspath.startswith(os.path.abspath(root) + os.sep) or abspath == os.path.abspath(root) for root in _SAFE_PICKLE_ROOTS)
    except Exception:
        return False


def safe_pickle_load(filepath: str) -> Any:
    """带路径白名单检查的 pickle 读取，仅用于兼容旧缓存。"""
    if not is_safe_pickle_path(filepath):
        raise PermissionError(f"拒绝读取非白名单 pickle 路径: {filepath}")
    with open(filepath, "rb") as f:
        return pickle.load(f)


def atomic_json_update(filepath: str, update_func: Callable[[Dict[str, Any]], None], timeout: int = 5) -> None:
    """
    在文件级自旋锁保护下，原子地读取 JSON → 内存修改 → 临时文件写入 → os.replace 覆盖。

    工业级流程（与 UI / 守护进程并发写 wash_metrics_history.json 等场景对齐）：
    1. 自旋锁：独占创建 ``filepath + '.lock'``；若已存在则 ``time.sleep(0.1)`` 重试，超过 ``timeout`` 秒抛出 ``TimeoutError``。
    2. 若检测到陈旧锁（默认超过 10 分钟且锁文件可用），自动清理后继续。
    3. 获锁后读取 ``filepath`` 的 JSON；文件不存在、为空或非法则视为 ``{}``。
    4. 将 dict 传入 ``update_func(data)``，由调用方在内存中原地合并/修改。
    5. 将结果写入 ``filepath + '.tmp'``（UTF-8，与项目其余 JSON 一致 ``ensure_ascii=False, indent=2``）。
    6. ``os.replace(filepath + '.tmp', filepath)`` 原子覆盖目标文件。
    7. ``finally`` 中无条件尝试删除 ``filepath + '.lock'``，释放锁。

    :param filepath: 目标 JSON 绝对路径或相对路径（与调用方 cwd 一致）。
    :param update_func: 接收已加载的 dict（可能为空），**必须在原地修改**该 dict；不得替换为其它类型。
    :param timeout: 等待锁文件独占创建的最大秒数。

    注意：获锁进程崩溃可能导致 ``.lock`` 残留，现会在超时后按 stale lock 自动恢复一次。
    """
    lock_path = filepath + ".lock"
    tmp_path = filepath + ".tmp"
    deadline = time.time() + float(timeout)
    stale_after_sec = 10 * 60
    lock_stale_checked = False

    while True:
        if time.time() > deadline:
            if not lock_stale_checked and os.path.isfile(lock_path):
                try:
                    age_sec = time.time() - os.path.getmtime(lock_path)
                except OSError:
                    age_sec = 0.0
                if age_sec >= stale_after_sec:
                    try:
                        os.remove(lock_path)
                        logger.warning("atomic_json_update: 已清理陈旧锁文件并重试: %s (age=%.0fs)", lock_path, age_sec)
                    except OSError as e:
                        logger.warning("atomic_json_update: 清理陈旧锁失败: %s | %s", lock_path, e)
                    lock_stale_checked = True
                    deadline = time.time() + float(timeout)
                    continue
            raise TimeoutError(
                f"atomic_json_update: 无法在 {timeout} 秒内获取锁（请确认无死锁进程或手动删除陈旧文件）: {lock_path}"
            )
        try:
            with open(lock_path, "x", encoding="utf-8") as lf:
                lf.write("1")
            break
        except FileExistsError:
            time.sleep(0.1)

    try:
        data: Dict[str, Any] = {}
        if os.path.isfile(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as rf:
                    raw = rf.read()
                if raw and str(raw).strip():
                    loaded = json.loads(raw)
                    if isinstance(loaded, dict):
                        data = loaded
                    else:
                        data = {}
            except json.JSONDecodeError as e:
                logger.warning("atomic_json_update: JSON 解析失败，按空 dict 处理: %s | %s", filepath, e)
                try:
                    if os.path.isfile(filepath):
                        shutil.copy2(filepath, filepath + ".corrupt.bak")
                except OSError:
                    logger.debug("atomic_json_update: 备份损坏 JSON 失败(忽略): %s", filepath, exc_info=True)
                data = {}
            except OSError as e:
                logger.warning("atomic_json_update: 读取失败，按空 dict 处理: %s | %s", filepath, e)
                data = {}
            except Exception as e:
                logger.warning("atomic_json_update: 非法 JSON 读取异常，按空 dict 处理: %s | %s", filepath, e, exc_info=True)
                data = {}

        update_func(data)

        if not isinstance(data, dict):
            raise TypeError("atomic_json_update: update_func 须保持 data 为 dict 类型")

        parent = os.path.dirname(filepath) or "."
        os.makedirs(parent, exist_ok=True)

        with open(tmp_path, "w", encoding="utf-8") as wf:
            json.dump(data, wf, ensure_ascii=False, indent=2)

        os.replace(tmp_path, filepath)
    except Exception:
        try:
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        raise
    finally:
        try:
            if os.path.isfile(lock_path):
                os.remove(lock_path)
        except OSError as e:
            logger.debug("atomic_json_update: 释放锁文件失败(忽略): %s | %s", lock_path, e)
