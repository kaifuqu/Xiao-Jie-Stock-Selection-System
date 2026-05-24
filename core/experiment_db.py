# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 策略实验记录 DuckDB 访问层

【设计说明】
- 主行情库由 data.db_core 管理（quant_data.duckdb）；实验记录单独落在 data/experiments.duckdb，
  避免与全市场 K 线写入争用同一文件锁，也便于备份与清理实验数据。
- 每次读写使用「短连接」：connect → 执行 → close；写入前始终 CREATE TABLE IF NOT EXISTS，不依赖进程内缓存标志。
- parameters / top_reasons 使用 NumpyEncoder，避免 numpy / pandas 标量导致 json.dumps 崩溃或结构破坏。
"""
from __future__ import annotations

import json
import logging
import math
import os
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# 项目根目录：与 data/db_core.py 一致，从 core/ 上溯一级
# ---------------------------------------------------------------------------
_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_CORE_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logger = logging.getLogger(__name__)

_EXPERIMENTS_DB_NAME = "experiments.duckdb"
TABLE_NAME = "strategy_experiments"

_STRATEGY_EXPERIMENTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    exp_id VARCHAR PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    pool_key VARCHAR NOT NULL,
    regime VARCHAR,
    parameters VARCHAR NOT NULL,
    pass_count INTEGER NOT NULL,
    max_score DOUBLE NOT NULL,
    top_reasons VARCHAR,
    note VARCHAR
);
"""


class NumpyEncoder(json.JSONEncoder):
    """将 numpy / pandas / datetime 等转为 JSON 可编码类型，禁止依赖非结构化 str 兜底。"""

    def default(self, o: Any) -> Any:
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            # 【全局审计修复】维度2：inf/NaN 会导致 json.dumps 抛错或非标准 JSON，实验记录须可序列化
            fv = float(o)
            if not math.isfinite(fv):
                logger.warning("NumpyEncoder: 非有限浮点 %s 已降级为 null", fv)
                return None
            return fv
        if isinstance(o, np.bool_):
            return bool(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        if isinstance(o, pd.Timestamp):
            return o.isoformat()
        # 【审计修复】维度2：禁止将 DataFrame/Series 原样进 JSON，改为列名/长度摘要，避免 dump 崩溃
        if isinstance(o, pd.DataFrame):
            logger.warning("NumpyEncoder: parameters 中含 DataFrame，已替换为列名摘要")
            return {"__truncated_dataframe__": [str(c) for c in o.columns]}
        if isinstance(o, pd.Series):
            logger.warning("NumpyEncoder: parameters 中含 Series，已替换为摘要")
            return {"__truncated_series__": str(o.name), "len": int(len(o))}
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        # 实验参数里若混入 set / Decimal / Path / bytes，原生 json 会直接 TypeError
        if isinstance(o, (set, frozenset)):
            return list(o)
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, Path):
            return str(o)
        if isinstance(o, bytes):
            try:
                return o.decode("utf-8")
            except UnicodeDecodeError:
                return o.hex()
        return super().default(o)


def get_experiment_db_path() -> str:
    """实验专用 DuckDB 绝对路径：`<项目根>/data/experiments.duckdb`。"""
    path = os.path.abspath(os.path.join(_PROJECT_ROOT, "data", _EXPERIMENTS_DB_NAME))
    data_dir = os.path.dirname(path)
    try:
        os.makedirs(data_dir, exist_ok=True)
    except OSError as e:
        logger.warning("创建实验库目录失败（将继续尝试连接）: %s — %s", data_dir, e)
    return path


def _connect_experiment_db(*, read_only: bool = False):
    path = get_experiment_db_path()
    return duckdb.connect(path, read_only=read_only)


def _ensure_strategy_experiments_table(con: Any) -> None:
    con.execute(_STRATEGY_EXPERIMENTS_DDL)


def _serialize_json(obj: Any) -> str:
    if obj is None:
        return "{}"
    if isinstance(obj, str):
        return obj
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), cls=NumpyEncoder)
    except (TypeError, ValueError) as e:
        # 【审计修复】维度4：序列化失败须打日志，禁止静默吞掉
        logger.warning("实验记录 json.dumps 失败，回退空对象: %s", e)
        return "{}"


def init_experiment_table() -> bool:
    """
    创建策略实验表（若不存在）。无进程内缓存：每次调用都执行 IF NOT EXISTS，保证与磁盘一致。
    """
    con = None
    try:
        con = _connect_experiment_db(read_only=False)
        _ensure_strategy_experiments_table(con)
        logger.debug("策略实验表已就绪: %s @ %s", TABLE_NAME, get_experiment_db_path())
        return True
    except Exception as e:
        logger.exception("初始化策略实验表失败: %s", e)
        return False
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def save_experiment_record(
    pool_key: str,
    regime: str,
    parameters: Union[Dict[str, Any], List[Any], str],
    pass_count: int,
    max_score: float,
    top_reasons: Union[Dict[str, Any], List[Any], str, None],
    note: str = "",
    exp_id: Optional[str] = None,
) -> Optional[str]:
    eid = (exp_id or "").strip() or str(uuid.uuid4())
    pk = (pool_key or "").strip()
    if not pk:
        logger.error("save_experiment_record: pool_key 不能为空")
        return None

    try:
        param_s = _serialize_json(parameters)
        reasons_s = _serialize_json(top_reasons if top_reasons is not None else {})
    except TypeError as e:
        logger.warning("实验记录 JSON 序列化失败: %s", e)
        return None

    note_s = (note or "").strip()
    reg = (regime or "").strip()

    sql = f"""
    INSERT INTO {TABLE_NAME} (
        exp_id, pool_key, regime, parameters, pass_count, max_score, top_reasons, note
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """

    con = None
    try:
        con = _connect_experiment_db(read_only=False)
        _ensure_strategy_experiments_table(con)
        con.execute(
            sql,
            [
                eid,
                pk,
                reg,
                param_s,
                int(pass_count),
                float(max_score),
                reasons_s,
                note_s,
            ],
        )
        logger.debug("实验记录已保存 exp_id=%s pool_key=%s pass_count=%s", eid, pk, pass_count)
        return eid
    except Exception as e:
        logger.warning("保存实验记录失败 exp_id=%s: %s", eid, e)
        return None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def get_experiment_history(pool_key: Optional[str] = None) -> pd.DataFrame:
    columns = [
        "exp_id",
        "created_at",
        "pool_key",
        "regime",
        "parameters",
        "pass_count",
        "max_score",
        "top_reasons",
        "note",
    ]
    empty = pd.DataFrame(columns=columns)

    con = None
    try:
        con = _connect_experiment_db(read_only=True)

        if pool_key is not None and str(pool_key).strip() != "":
            pk = str(pool_key).strip()
            df = con.execute(
                f"""
                SELECT * FROM {TABLE_NAME}
                WHERE pool_key = ?
                ORDER BY created_at DESC
                """,
                [pk],
            ).fetchdf()
        else:
            df = con.execute(
                f"""
                SELECT * FROM {TABLE_NAME}
                ORDER BY created_at DESC
                """
            ).fetchdf()

        if df is None or df.empty:
            return empty
        return df
    except Exception as e:
        logger.debug("查询实验历史失败 pool_key=%s: %s", pool_key, e)
        return empty.copy()
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
