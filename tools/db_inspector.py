# -*- coding: utf-8 -*-
"""
V26 DuckDB 数据库全量体检脚本

目标
- 检查当前主库、兼容视图、V26 分层表、维表是否齐全
- 检查字段覆盖是否完整
- 检查所有日期是否连续、各表日期是否一致、各主表主键是否一致
- 检查 daily_data 与 vw_daily_data_compat 的抽样一致性
- 输出可读报告，并返回结构化结果

说明
- 该脚本尽量只读，不做写入性修复
- 若兼容视图不存在，会先尝试在当前主库上补建一次，但不会改动业务数据
- 适配 Windows 控制台输出，避免使用特殊符号造成编码问题
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb
import pandas as pd


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if os.path.basename(os.path.dirname(os.path.abspath(__file__))) == "tools" else os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from data.db_core import (
    ensure_v26_compat_view,
    get_duckdb_path,
    get_read_conn_singleton,
)


CORE_TABLES = [
    "daily_data",
    "vw_daily_data_compat",
    "raw_daily_quotes",
    "bars_daily",
    "feat_daily_core",
    "feat_daily_capital",
    "feat_daily_memory",
    "dim_security",
    "stock_basic",
]

LEGACY_EXPECTED_COLUMNS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "pct_chg",
    "vol",
    "amount",
    "turnover_rate_f",
    "vol_ratio",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dv_ratio",
    "total_mv",
    "circ_mv",
    "adj_factor",
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "ma120",
    "ma250",
    "vol_ma5",
    "vol_ma10",
    "vol_ma20",
    "ma20_slope_5",
    "high_20",
    "low_60",
    "macd",
    "macd_signal",
    "macd_hist",
    "rsi_14",
    "kdj_k",
    "kdj_d",
    "boll_upper",
    "boll_lower",
    "cci",
    "bias_20",
    "atr_pct",
    "net_elg_amount",
    "net_main_amount",
    "inst_net_buy",
    "hk_vol",
    "rz_net_buy",
    "cost_5th",
    "cost_50th",
    "cost_95th",
    "avg_cost",
    "winner_rate",
    "cyq_concentration",
    "nineturn_signal",
    "limit_times",
    "strth",
    "forecast_type",
    "capital_resonance_score",
    "fund_memory_score",
]

DATE_TABLES = ["daily_data", "vw_daily_data_compat", "raw_daily_quotes", "bars_daily", "feat_daily_core", "feat_daily_capital", "feat_daily_memory"]


def _print_block(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def _safe_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        row = con.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?", [table_name]).fetchone()
        return bool(row and _safe_int(row[0]) > 0)
    except Exception:
        return False


def _view_exists(con: duckdb.DuckDBPyConnection, view_name: str) -> bool:
    try:
        row = con.execute("SELECT COUNT(*) FROM duckdb_views() WHERE view_name = ?", [view_name]).fetchone()
        return bool(row and _safe_int(row[0]) > 0)
    except Exception:
        return False


def _get_columns(con: duckdb.DuckDBPyConnection, obj_name: str) -> List[str]:
    try:
        if not (_table_exists(con, obj_name) or _view_exists(con, obj_name)):
            return []
        df = con.execute(f"DESCRIBE {obj_name}").fetchdf()
        if df is None or df.empty:
            return []
        if "column_name" in df.columns:
            return [str(x) for x in df["column_name"].tolist()]
        if "name" in df.columns:
            return [str(x) for x in df["name"].tolist()]
    except Exception:
        return []
    return []


def _get_column_types(con: duckdb.DuckDBPyConnection, obj_name: str) -> Dict[str, str]:
    try:
        if not (_table_exists(con, obj_name) or _view_exists(con, obj_name)):
            return {}
        df = con.execute(f"DESCRIBE {obj_name}").fetchdf()
        if df is None or df.empty:
            return {}
        name_col = "column_name" if "column_name" in df.columns else "name"
        type_col = "column_type" if "column_type" in df.columns else ("type" if "type" in df.columns else None)
        if type_col is None:
            return {}
        return {str(r[name_col]): str(r[type_col]) for _, r in df.iterrows()}
    except Exception:
        return {}


def _safe_count(con: duckdb.DuckDBPyConnection, sql: str, params: Optional[Sequence[Any]] = None) -> int:
    try:
        row = con.execute(sql, list(params or [])).fetchone()
        return _safe_int(row[0]) if row else 0
    except Exception:
        return 0


def _load_df(con: duckdb.DuckDBPyConnection, sql: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    try:
        return con.execute(sql, list(params or [])).fetchdf()
    except Exception:
        return pd.DataFrame()


def _sorted_unique(values: Iterable[Any]) -> List[str]:
    out = []
    seen = set()
    for v in values:
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return sorted(out)


def _normalize_date_series(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    s = series.astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    return s[s.str.len() == 8]


def _date_range_report(dates: List[str]) -> Dict[str, Any]:
    if not dates:
        return {"count": 0, "start": "", "end": "", "gaps": []}
    d = pd.to_datetime(pd.Series(dates), format="%Y%m%d", errors="coerce").dropna().sort_values().reset_index(drop=True)
    if d.empty:
        return {"count": 0, "start": "", "end": "", "gaps": []}
    expected = pd.date_range(d.iloc[0], d.iloc[-1], freq="B")
    missing = sorted(set(expected.strftime("%Y%m%d")) - set(d.dt.strftime("%Y%m%d")))
    return {
        "count": int(d.shape[0]),
        "start": d.iloc[0].strftime("%Y-%m-%d"),
        "end": d.iloc[-1].strftime("%Y-%m-%d"),
        "gaps": missing,
    }


def verify_coverage(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    legacy_cols = _get_columns(con, "daily_data")
    view_cols = _get_columns(con, "vw_daily_data_compat")
    legacy_set = set(LEGACY_EXPECTED_COLUMNS if legacy_cols else [])
    view_set = set(view_cols)
    missing = sorted([c for c in legacy_set if c not in view_set]) if view_set else sorted(list(legacy_set))
    extra = sorted([c for c in view_set if c not in legacy_set]) if legacy_set else []
    return {
        "ok": len(missing) == 0 and bool(view_cols),
        "missing_columns": missing,
        "extra_columns": extra,
        "legacy_columns": legacy_cols,
        "view_columns": view_cols,
    }


def verify_dim_security_consistency(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "ok": False,
        "rows_dim": 0,
        "rows_stock_basic": 0,
        "missing_in_dim": 0,
        "missing_in_stock_basic": 0,
        "reason": "",
    }
    if not _table_exists(con, "dim_security") and not _table_exists(con, "stock_basic"):
        result["reason"] = "both_missing"
        return result
    if _table_exists(con, "dim_security"):
        result["rows_dim"] = _safe_count(con, "SELECT COUNT(*) FROM dim_security")
    if _table_exists(con, "stock_basic"):
        result["rows_stock_basic"] = _safe_count(con, "SELECT COUNT(*) FROM stock_basic")
    if _table_exists(con, "dim_security") and _table_exists(con, "stock_basic"):
        try:
            row = con.execute(
                """
                WITH d AS (SELECT ts_code FROM dim_security),
                     s AS (SELECT ts_code FROM stock_basic)
                SELECT
                    (SELECT COUNT(*) FROM (SELECT ts_code FROM s EXCEPT SELECT ts_code FROM d)) AS missing_in_dim,
                    (SELECT COUNT(*) FROM (SELECT ts_code FROM d EXCEPT SELECT ts_code FROM s)) AS missing_in_stock_basic
                """
            ).fetchone()
            result["missing_in_dim"] = _safe_int(row[0])
            result["missing_in_stock_basic"] = _safe_int(row[1])
        except Exception as e:
            result["reason"] = str(e)
            return result
    result["ok"] = (
        result["rows_dim"] > 0
        and result["rows_stock_basic"] > 0
        and result["missing_in_dim"] == 0
        and result["missing_in_stock_basic"] == 0
    )
    return result


def verify_date_integrity(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    for tbl in DATE_TABLES:
        if not (_table_exists(con, tbl) or _view_exists(con, tbl)):
            report[tbl] = {"exists": False, "rows": 0, "date_count": 0, "start": "", "end": "", "gaps": []}
            continue
        df = _load_df(con, f"SELECT DISTINCT trade_date FROM {tbl}")
        dates = _sorted_unique(_normalize_date_series(df["trade_date"]).tolist()) if not df.empty and "trade_date" in df.columns else []
        rr = _date_range_report(dates)
        report[tbl] = {
            "exists": True,
            "rows": _safe_count(con, f"SELECT COUNT(*) FROM {tbl}"),
            "date_count": rr["count"],
            "start": rr["start"],
            "end": rr["end"],
            "gaps": rr["gaps"],
        }
    # 主表之间日期差集
    base_dates = set(_sorted_unique(_normalize_date_series(_load_df(con, "SELECT DISTINCT trade_date FROM daily_data")["trade_date"]).tolist())) if _table_exists(con, "daily_data") else set()
    for tbl in ["raw_daily_quotes", "bars_daily", "feat_daily_core", "feat_daily_capital", "feat_daily_memory"]:
        if not _table_exists(con, tbl):
            continue
        tbl_dates = set(_sorted_unique(_normalize_date_series(_load_df(con, f"SELECT DISTINCT trade_date FROM {tbl}")["trade_date"]).tolist()))
        report[tbl]["missing_vs_daily_data"] = sorted(list(base_dates - tbl_dates))[:200]
        report[tbl]["extra_vs_daily_data"] = sorted(list(tbl_dates - base_dates))[:200]
    return report


def verify_schema_alignment(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    tables = {}
    for tbl in CORE_TABLES:
        tables[tbl] = {
            "exists": _table_exists(con, tbl) or _view_exists(con, tbl),
            "columns": _get_columns(con, tbl),
            "types": _get_column_types(con, tbl),
            "rows": _safe_count(con, f"SELECT COUNT(*) FROM {tbl}") if (_table_exists(con, tbl) or _view_exists(con, tbl)) else 0,
        }
    coverage = verify_coverage(con)
    return {"tables": tables, "coverage": coverage}


def verify_v26_consistency(con: duckdb.DuckDBPyConnection, sample_rows: int = 500) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "ok": False,
        "legacy_rows": 0,
        "view_rows": 0,
        "raw_rows": 0,
        "bars_rows": 0,
        "feat_core_rows": 0,
        "feat_cap_rows": 0,
        "feat_mem_rows": 0,
        "sample_diff_count": -1,
        "coverage": {},
        "dim_security": {},
        "date_integrity": {},
        "reason": "",
    }
    legacy_exists = _table_exists(con, "daily_data")
    view_exists = _view_exists(con, "vw_daily_data_compat")
    if legacy_exists:
        result["legacy_rows"] = _safe_count(con, "SELECT COUNT(*) FROM daily_data")
    if view_exists:
        result["view_rows"] = _safe_count(con, "SELECT COUNT(*) FROM vw_daily_data_compat")
    for tbl, key in [
        ("raw_daily_quotes", "raw_rows"),
        ("bars_daily", "bars_rows"),
        ("feat_daily_core", "feat_core_rows"),
        ("feat_daily_capital", "feat_cap_rows"),
        ("feat_daily_memory", "feat_mem_rows"),
    ]:
        if _table_exists(con, tbl):
            result[key] = _safe_count(con, f"SELECT COUNT(*) FROM {tbl}")

    result["coverage"] = verify_coverage(con)
    result["dim_security"] = verify_dim_security_consistency(con)
    result["date_integrity"] = verify_date_integrity(con)

    if legacy_exists and view_exists:
        try:
            q = f"""
                WITH l AS (
                    SELECT ts_code, trade_date, close, pct_chg, ma20, capital_resonance_score, fund_memory_score
                    FROM daily_data
                    ORDER BY ts_code, trade_date
                    LIMIT {int(sample_rows)}
                ), v AS (
                    SELECT ts_code, trade_date, close, pct_chg, ma20, capital_resonance_score, fund_memory_score
                    FROM vw_daily_data_compat
                    ORDER BY ts_code, trade_date
                    LIMIT {int(sample_rows)}
                )
                SELECT COUNT(*)
                FROM (
                    SELECT * FROM l
                    EXCEPT
                    SELECT * FROM v
                ) t
            """
            result["sample_diff_count"] = _safe_count(con, q)
        except Exception as e:
            result["reason"] = str(e)
            return result

    result["ok"] = bool(
        (not legacy_exists or result["legacy_rows"] == result["view_rows"])
        and result["coverage"].get("ok", False)
        and result["dim_security"].get("ok", False)
        and result["sample_diff_count"] == 0
    )
    return result


def _print_table_summary(con: duckdb.DuckDBPyConnection) -> None:
    _print_block("2) V26 分层表与维表概览")
    for tbl in CORE_TABLES:
        exists = _table_exists(con, tbl) or _view_exists(con, tbl)
        if exists:
            cnt = _safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
            print(f"[OK]   {tbl:<24} {cnt:,}")
        else:
            print(f"[MISS] {tbl:<24} 不存在")


def _print_coverage(coverage: Dict[str, Any]) -> None:
    _print_block("3) 字段覆盖检查")
    print(f"覆盖结论: {'通过' if coverage.get('ok') else '未通过'}")
    missing = coverage.get("missing_columns", [])
    extra = coverage.get("extra_columns", [])
    print(f"缺失字段: {', '.join(missing) if missing else '无'}")
    print(f"额外字段: {', '.join(extra) if extra else '无'}")


def _print_dim_check(dim_check: Dict[str, Any]) -> None:
    _print_block("4) dim_security 一致性检查")
    print(json.dumps(dim_check, ensure_ascii=False, indent=2, default=str))


def _print_consistency(consistency: Dict[str, Any]) -> None:
    _print_block("5) V26 一致性检查")
    print(json.dumps(consistency, ensure_ascii=False, indent=2, default=str))


def _print_date_integrity(date_integrity: Dict[str, Any]) -> None:
    _print_block("6) 日期完整性检查")
    for tbl, info in date_integrity.items():
        if not isinstance(info, dict):
            continue
        if not info.get("exists"):
            print(f"[MISS] {tbl:<24} 不存在")
            continue
        print(
            f"[OK]   {tbl:<24} rows={info.get('rows', 0):,} | dates={info.get('date_count', 0)} | "
            f"{info.get('start', '')} -> {info.get('end', '')}"
        )
        gaps = info.get("gaps", []) or []
        if gaps:
            print(f"       gaps={len(gaps)} 例如: {', '.join(gaps[:10])}")
        missing_vs = info.get("missing_vs_daily_data", [])
        extra_vs = info.get("extra_vs_daily_data", [])
        if missing_vs or extra_vs:
            print(f"       vs daily_data missing={len(missing_vs)} extra={len(extra_vs)}")


def _print_field_completeness(field_report: Dict[str, Any]) -> None:
    _print_block("7) 字段逐列完整性列表")
    if not field_report.get("exists"):
        print("[MISS] daily_data 不存在")
        return
    rows = field_report.get("rows", []) or []
    print(f"表名: {field_report.get('table', 'daily_data')}")
    print(f"总记录数: {field_report.get('total_rows', 0):,} | 交易日数: {field_report.get('total_days', 0)}")
    print("-" * 100)
    print(f"{'字段':<24} {'完整天数':>10} {'总天数':>10} {'完整率':>10} {'状态':>10}")
    print("-" * 100)
    for r in rows:
        print(
            f"{r.get('column',''):<24} {int(r.get('valid_days',0)):>10,} {int(r.get('total_days',0)):>10,} "
            f"{float(r.get('ratio',0.0)):>9.2f}% {r.get('status',''):>10}"
        )
    print("-" * 100)


def _print_final(result: Dict[str, Any]) -> None:
    _print_block("8) 结论")
    coverage_ok = result.get("coverage", {}).get("ok")
    dim_ok = result.get("dim_security", {}).get("ok")
    consistency_ok = result.get("ok")
    print(f"verify_v26_coverage().ok = {coverage_ok}")
    print(f"verify_dim_security_consistency().ok = {dim_ok}")
    print(f"verify_v26_consistency().ok = {consistency_ok}")
    if consistency_ok:
        print("[OK] V26 闭环完成：旧表、兼容视图、分层表、维表一致。")
    else:
        print("[WARN] V26 仍存在不一致项，请根据上方报告修复。")
    date_integrity = result.get("date_integrity", {})
    if date_integrity:
        print("[INFO] 已输出日期完整性检查；如有 gaps，请优先修复。")


def _open_connection() -> duckdb.DuckDBPyConnection:
    # 优先复用 db_core 的只读连接池/单例，减少不同配置的同库连接冲突。
    con = get_read_conn_singleton(max_wait_sec=30.0)
    if con is not None:
        return con
    db_path = get_duckdb_path()
    return duckdb.connect(db_path, read_only=True)


def _build_field_completeness_report(con: duckdb.DuckDBPyConnection, table_name: str = "daily_data") -> Dict[str, Any]:
    """逐字段按交易日统计完整性，返回可用于展示的数据列表。"""
    if not _table_exists(con, table_name):
        return {"table": table_name, "exists": False, "total_days": 0, "total_rows": 0, "rows": []}

    total_days = _safe_count(con, f"SELECT COUNT(DISTINCT trade_date) FROM {table_name}")
    total_rows = _safe_count(con, f"SELECT COUNT(*) FROM {table_name}")
    cols = _get_columns(con, table_name)
    rows: List[Dict[str, Any]] = []
    for col in cols:
        if col in {"ts_code", "trade_date"}:
            continue
        valid_days = _safe_count(
            con,
            f"""
            SELECT COUNT(DISTINCT trade_date)
            FROM {table_name}
            WHERE {col} IS NOT NULL
              AND TRIM(CAST({col} AS VARCHAR)) != ''
            """,
        )
        ratio = round((valid_days / total_days * 100.0), 2) if total_days else 0.0
        if ratio >= 99.0:
            status = "FULL"
        elif ratio >= 90.0:
            status = "GOOD"
        elif ratio > 0:
            status = "PART"
        else:
            status = "MISS"
        rows.append(
            {
                "column": col,
                "valid_days": valid_days,
                "total_days": total_days,
                "ratio": ratio,
                "status": status,
            }
        )
    rows.sort(key=lambda x: (x["status"] != "FULL", x["status"] != "GOOD", x["ratio"], x["column"]))
    return {"table": table_name, "exists": True, "total_days": total_days, "total_rows": total_rows, "rows": rows}


def run_full_inspection(sample_rows: int = 500) -> Dict[str, Any]:
    db_path = get_duckdb_path()
    if not os.path.exists(db_path):
        result = {"ok": False, "reason": "db_missing", "db_path": db_path}
        _print_block("DuckDB 数据库全景体检报告 (V26)")
        print(f"[MISS] 找不到数据库: {db_path}")
        return result

    try:
        # 只做结构兜底，不改业务数据。
        ensure_v26_compat_view(force=True)
    except Exception:
        pass

    con = _open_connection()
    try:
        _print_block("DuckDB 数据库全景体检报告 (V26)")
        print(f"数据库路径: {db_path}")

        _print_block("1) 基础范围与规模")
        if _table_exists(con, "daily_data"):
            date_row = con.execute(
                "SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) FROM daily_data"
            ).fetchone()
            stat_row = con.execute("SELECT COUNT(*), COUNT(DISTINCT ts_code) FROM daily_data").fetchone()
            print(f"日期范围: {date_row[0]} -> {date_row[1]} | 交易日数: {_safe_int(date_row[2])}")
            print(f"总行数: {_safe_int(stat_row[0]):,} | 标的数: {_safe_int(stat_row[1]):,}")
        else:
            print("[MISS] daily_data 不存在")

        _print_table_summary(con)
        coverage = verify_coverage(con)
        dim_check = verify_dim_security_consistency(con)
        consistency = verify_v26_consistency(con, sample_rows=sample_rows)
        date_integrity = verify_date_integrity(con)

        _print_coverage(coverage)
        _print_dim_check(dim_check)
        _print_consistency(consistency)
        _print_date_integrity(date_integrity)
        field_report = _build_field_completeness_report(con, "daily_data")
        _print_field_completeness(field_report)
        consistency["coverage"] = coverage
        consistency["dim_security"] = dim_check
        consistency["date_integrity"] = date_integrity
        consistency["field_completeness"] = field_report
        _print_final(consistency)

        return {
            "ok": bool(consistency.get("ok")),
            "coverage": coverage,
            "dim_security": dim_check,
            "consistency": consistency,
            "date_integrity": date_integrity,
            "field_completeness": field_report,
            "db_path": db_path,
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="V26 DuckDB database inspector")
    parser.add_argument("--sample-rows", type=int, default=500, help="sample rows for consistency check")
    parser.add_argument("--json", action="store_true", help="print structured json result")
    args = parser.parse_args(argv)

    result = run_full_inspection(sample_rows=max(10, int(args.sample_rows)))
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
