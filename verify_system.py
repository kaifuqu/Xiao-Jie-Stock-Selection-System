# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 轻量级系统自检（DuckDB、异步扫描目录权限等）
【V26 数据底座验收】
- capital_resonance_score：最新交易日截面上，流通市值 ≥100 亿（与 constants.DAILY_BASIC_MIN_MV_WAN 一致）
  的活跃标的中，非零有效分占比须 ≥85%。
- fund_memory_score：优先检查 vw_daily_data_compat，必要时回退 daily_data，且 PRAGMA 类型应为浮点类（DOUBLE/FLOAT/REAL）。

用法（在项目根目录）:
    python verify_system.py
退出码：0 全部通过，1 存在失败项。
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile


def _root() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _fail(msg: str) -> None:
    print(f"[FAIL] {msg}")


def _ok(msg: str) -> None:
    print(f"[OK]   {msg}")


def check_config_readable() -> bool:
    p = os.path.join(_root(), "config.yaml")
    if not os.path.isfile(p):
        _fail(f"config.yaml 不存在: {p}")
        return False
    try:
        import yaml

        with open(p, "r", encoding="utf-8") as f:
            yaml.safe_load(f)
    except Exception as e:
        _fail(f"config.yaml 解析失败: {e}")
        return False
    _ok("config.yaml 可读且 YAML 语法正确")
    return True


def _duckdb_looks_like_file_lock(err: BaseException) -> bool:
    """Windows 上多进程同开时只读连接也可能抢不到锁，属环境态而非库损坏。"""
    msg = str(err).lower()
    if "另一个程序正在使用" in str(err) or "进程无法访问" in str(err):
        return True
    if "being used by another process" in msg:
        return True
    if "could not set lock" in msg:  # DuckDB / OS 锁相关英文提示
        return True
    return False


def check_duckdb_health() -> bool:
    cfg_path = os.path.join(_root(), "config.yaml")
    try:
        import yaml

        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        fn = (cfg.get("database") or {}).get("file") or "quant_data.duckdb"
    except Exception as e:
        _fail(f"读取数据库配置失败: {e}")
        return False

    dbp = os.path.join(_root(), "data", str(fn))
    if not os.path.isfile(dbp):
        _fail(f"DuckDB 文件不存在（首次部署属正常，请先同步数据）: {dbp}")
        return False
    try:
        import duckdb

        con = duckdb.connect(dbp, read_only=True)
        try:
            one = con.execute("SELECT 1").fetchone()
            if one is None or int(one[0]) != 1:
                _fail("DuckDB SELECT 1 结果异常")
                return False
        finally:
            con.close()
    except Exception as e:
        if _duckdb_looks_like_file_lock(e):
            print(
                f"[WARN] DuckDB 被其他进程占用，无法建立只读连接（关闭占用进程后重试 verify）。"
                f" 文件: {dbp}"
            )
            return True
        _fail(f"DuckDB 连接或查询失败: {e}")
        return False
    _ok(f"DuckDB 健康: {dbp}")
    return True


def check_scan_async_dir_rw() -> bool:
    sys.path.insert(0, _root())
    try:
        from core.runtime_data_paths import path_scan_async_dir, ensure_runtime_data_layout

        ensure_runtime_data_layout()
        d = path_scan_async_dir()
    except Exception as e:
        _fail(f"解析 scan_async 路径失败: {e}")
        return False

    if not os.path.isdir(d):
        _fail(f"scan_async 目录不存在: {d}")
        return False

    try:
        fd, tmp = tempfile.mkstemp(prefix=".verify_", suffix=".tmp", dir=d)
        os.close(fd)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("xiaojie_verify")
        os.remove(tmp)
    except Exception as e:
        _fail(f"data/runtime/scan_async 无写权限或不可写: {d} — {e}")
        return False
    _ok(f"异步扫描目录可写: {d}")
    return True


def check_embed_switch_readable() -> bool:
    sys.path.insert(0, _root())
    try:
        from service.async_scan_bridge import should_embed_ui_scan_worker

        v = should_embed_ui_scan_worker()
    except Exception as e:
        _fail(f"扫描队列嵌入开关检测失败: {e}")
        return False
    _ok(
        "scan_async 嵌入开关可读: should_embed_ui_scan_worker()="
        + ("True（UI 可内嵌消费者）" if v else "False（应由 auto_sniper_daemon 等外部消费）")
    )
    return True


def _ensure_utf8_stdout() -> None:
    """Windows 控制台默认 GBK，打印含 emoji 的 JSON 会报错，自检输出统一走 UTF-8。"""
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass


def check_regime_config() -> bool:
    """
    校验 config.yaml 中 regime 段：键齐全、类型合理、阈值在可解释范围内。
    不全则 [FAIL]；若主阈值顺序反常则 [WARN] 但仍算通过。
    """
    cfg_path = os.path.join(_root(), "config.yaml")
    try:
        import yaml

        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        _fail(f"读取 config 失败（regime 校验）: {e}")
        return False

    rg = cfg.get("regime")
    if rg is None:
        _fail("config.yaml 缺少 regime 段（双轨雷达阈值与 SQL 过滤）")
        return False
    if not isinstance(rg, dict):
        _fail("regime 必须是映射(dict)")
        return False

    try:
        ld = int(rg.get("lookback_days", 0))
        pwd = int(rg.get("primary_window_days", 0))
        msd = int(rg.get("min_sample_days", 0))
        if ld < 5 or ld > 500:
            _fail(f"regime.lookback_days 应在合理区间，当前: {ld}")
            return False
        if pwd < 1 or pwd > ld:
            _fail(f"regime.primary_window_days 应在 1..lookback_days 内，当前: {pwd} (lookback={ld})")
            return False
        if msd < 1 or msd > ld:
            _fail(f"regime.min_sample_days 应在 1..lookback_days 内，当前: {msd}")
            return False

        prim = rg.get("primary") or {}
        if not isinstance(prim, dict):
            _fail("regime.primary 必须是映射")
            return False
        tr_up = float(prim.get("trend_up_ratio", 0))
        tr_dn = float(prim.get("trend_down_ratio", 0))
        if not (0.0 < tr_up < 1.0 and 0.0 < tr_dn < 1.0):
            _fail(f"regime.primary 趋势阈值应在 (0,1)，当前 up={tr_up} down={tr_dn}")
            return False
        if tr_up <= tr_dn:
            print(
                f"[WARN] regime.primary: trend_up_ratio({tr_up}) 应大于 trend_down_ratio({tr_dn})，否则主升/退潮语义异常"
            )

        sec = rg.get("secondary") or {}
        if not isinstance(sec, dict):
            _fail("regime.secondary 必须是映射")
            return False
        for k, lo, hi in [
            ("climax_up_ratio", 0.0, 1.0),
            ("freeze_up_ratio", 0.0, 1.0),
        ]:
            v = float(sec.get(k, float("nan")))
            if not (lo < v < hi):
                _fail(f"regime.secondary.{k} 应在 ({lo},{hi})，当前: {v}")
                return False
        for k in ("climax_avg_pct_chg", "freeze_avg_pct_chg", "rebound_delta"):
            float(sec.get(k, 0.0))  # 仅校验可解析为 float

        sf = rg.get("sql_filter") or {}
        if not isinstance(sf, dict):
            _fail("regime.sql_filter 必须是映射")
            return False
        max_abs = float(sf.get("max_abs_pct_chg", 0))
        min_vol = float(sf.get("min_vol", -1))
        if max_abs <= 0 or max_abs > 100:
            _fail(f"regime.sql_filter.max_abs_pct_chg 应在 (0,100]，当前: {max_abs}")
            return False
        if min_vol < 0:
            _fail(f"regime.sql_filter.min_vol 应 >= 0，当前: {min_vol}")
            return False
        excl = sf.get("exclude_index_ts_codes")
        if excl is not None:
            if not isinstance(excl, list) or not all(isinstance(x, str) for x in excl):
                _fail("regime.sql_filter.exclude_index_ts_codes 应为字符串列表")
                return False
    except (TypeError, ValueError) as e:
        _fail(f"regime 配置数值解析失败: {e}")
        return False

    _ok("regime 配置段结构、类型与阈值范围校验通过")
    return True


def check_regime_analyzer_call() -> bool:
    """
    可选连库：调用 get_market_regime()。库不存在或只读连接失败时降级为提示，不视为硬失败。
    DuckDB 被其它进程独占时可能无法打开只读连接，此时 [WARN] 并跳过。
    """
    sys.path.insert(0, _root())
    _ensure_utf8_stdout()
    cfg_path = os.path.join(_root(), "config.yaml")
    try:
        import yaml

        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        fn = (cfg.get("database") or {}).get("file") or "quant_data.duckdb"
    except Exception as e:
        _fail(f"读取数据库配置失败（regime 连库）: {e}")
        return False

    dbp = os.path.join(_root(), "data", str(fn))
    if not os.path.isfile(dbp):
        print("[SKIP] get_market_regime：DuckDB 文件不存在，跳过连库调用")
        return True

    try:
        from core.regime_analyzer import get_market_regime

        # 文件被占用时 db_core 会打 ERROR，自检场景下改为静默，结论仍由下方 WARN 说明
        _prev_root = logging.root.level
        logging.root.setLevel(logging.CRITICAL)
        try:
            out = get_market_regime()
        finally:
            logging.root.setLevel(_prev_root)
    except Exception as e:
        _fail(f"get_market_regime() 异常: {e}")
        return False

    if not isinstance(out, dict):
        _fail("get_market_regime() 返回值类型异常")
        return False

    sk = out.get("sentiment_key")
    prim = (out.get("primary") or {}).get("status", "")
    if sk not in ("高潮", "冰点", "回暖", "平稳"):
        _fail(f"sentiment_key 非预期: {sk!r}")
        return False

    # 若底层返回默认「数据不足」，说明未拉到足够样本或连接为 None，给 WARN 便于排查
    if "数据不足" in str(prim) or "等待接入" in str((out.get("secondary") or {}).get("status", "")):
        print(
            f"[WARN] get_market_regime 返回默认/降级状态（可能兼容视图/日线不足、只读连接失败或表缺失）。"
            f" sentiment_key={sk}, primary={prim!r}"
        )
        return True

    _ok(
        f"get_market_regime() 连库成功: sentiment_key={sk}, primary={prim!r}"
    )
    return True


def check_capital_resonance_coverage() -> bool:
    """
    【V26.5 新增资金记忆体系】与 fund_memory 列校验并列；特征均由 data_fetcher._sync_daily_features() 维护。

    分母为「活跃标的」：最新交易日 circ_mv ≥ constants.DAILY_BASIC_MIN_MV_WAN（默认 100 亿市值门槛，万元口径），
    与日线下载候选及资金记忆双重过滤的规模闸对齐。分子为其中 capital_resonance_score 非零有效的行数；占比须 ≥85%。
    """
    cfg_path = os.path.join(_root(), "config.yaml")
    try:
        import yaml

        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        fn = (cfg.get("database") or {}).get("file") or "quant_data.duckdb"
    except Exception as e:
        _fail(f"读取数据库配置失败（资金共振覆盖率）: {e}")
        return False

    try:
        import constants as _constants

        mv_min_wan = float(getattr(_constants, "DAILY_BASIC_MIN_MV_WAN", 1_000_000))
    except Exception:
        mv_min_wan = 1_000_000.0

    dbp = os.path.join(_root(), "data", str(fn))
    if not os.path.isfile(dbp):
        print("[SKIP] capital_resonance_score 覆盖率：DuckDB 不存在，跳过")
        return True

    try:
        import duckdb

        con = duckdb.connect(dbp, read_only=True)
        try:
            source_table, table_hint = _resolve_compatible_source_table(con)
            if source_table is None:
                print(f"[SKIP] {table_hint}")
                return True
            if table_hint:
                print(f"[INFO] 资金共振覆盖率: {table_hint}")
            cols = con.execute(f"PRAGMA table_info('{source_table}')").fetchdf()
            if cols is None or cols.empty:
                print(f"[SKIP] {source_table} 无 PRAGMA 信息，跳过资金共振覆盖率")
                return True
            _cn = "name" if "name" in cols.columns else ("column_name" if "column_name" in cols.columns else None)
            if _cn is None:
                print(f"[SKIP] {source_table} PRAGMA table_info 列名非预期，跳过资金共振覆盖率")
                return True
            names = {str(x).lower() for x in cols[_cn].tolist()}
            if "capital_resonance_score" not in names:
                _fail(f"{source_table} 缺少列 capital_resonance_score（请先跑日线同步或特征修补）")
                return False
            if "circ_mv" not in names:
                _fail(f"{source_table} 缺少列 circ_mv，无法计算活跃标的资金共振覆盖率")
                return False
            mx = con.execute(
                f"SELECT MAX(CAST(trade_date AS DATE)) AS md FROM {source_table}"
            ).fetchone()
            if mx is None or mx[0] is None:
                print(f"[SKIP] {source_table} 无有效 trade_date，跳过资金共振覆盖率")
                return True
            md = mx[0]
            row = con.execute(
                f"""
                SELECT
                  COUNT(*) AS n_all,
                  SUM(CASE
                    WHEN capital_resonance_score IS NOT NULL
                     AND ABS(CAST(capital_resonance_score AS DOUBLE)) > 1e-9
                    THEN 1 ELSE 0 END) AS n_ok
                FROM {source_table}
                WHERE CAST(trade_date AS DATE) = ?
                  AND CAST(circ_mv AS DOUBLE) >= ?
                """,
                [md, mv_min_wan],
            ).fetchone()
        finally:
            con.close()
    except Exception as e:
        if _duckdb_looks_like_file_lock(e):
            print(
                f"[WARN] DuckDB 占用中，跳过 capital_resonance_score 覆盖率检查: {dbp}"
            )
            return True
        _fail(f"资金共振覆盖率查询失败: {e}")
        return False

    n_all = int(row[0] or 0)
    n_ok = int(row[1] or 0)
    if n_all <= 0:
        print(
            f"[SKIP] 最新交易日无 circ_mv≥{mv_min_wan:.0f}(万) 的活跃行，跳过资金共振覆盖率"
        )
        return True
    ratio = n_ok / float(n_all)
    if ratio < 0.85:
        _fail(
            f"capital_resonance_score 活跃覆盖率不足: 最新日 {md}、流通≥100亿样本中有效 {n_ok}/{n_all} "
            f"({ratio*100:.1f}%)，阈值 85%。请执行 sync_recent_days / _sync_daily_features()。"
        )
        return False
    _ok(
        f"capital_resonance_score 活跃覆盖率: 最新日 {md} 流通≥100亿 有效 {n_ok}/{n_all} ({ratio*100:.1f}%)"
    )
    return True


def _has_table_or_view(con, name: str) -> bool:
    """兼容 DuckDB / 旧表结构的存在性判断。"""
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            LIMIT 1
            """,
            [name],
        ).fetchone()
        return row is not None
    except Exception:
        try:
            row = con.execute(
                """
                SELECT 1
                FROM duckdb_views()
                WHERE lower(view_name) = lower(?)
                LIMIT 1
                """,
                [name],
            ).fetchone()
            return row is not None
        except Exception:
            return False


def _resolve_compatible_source_table(con, preferred="vw_daily_data_compat", fallback="daily_data"):
    """统一解析可用的数据源表/视图，并给出更明确的降级信息。"""
    if _has_table_or_view(con, preferred):
        return preferred, None
    if _has_table_or_view(con, fallback):
        return fallback, f"未找到 {preferred}，已回退到 {fallback}"
    return None, f"未找到 {preferred} 或 {fallback}，请先完成日线同步/特征重铸"


def _pragma_col_type(df, col_name: str) -> str:
    """从 PRAGMA table_info DataFrame 取列类型字符串（小写）。"""
    if df is None or df.empty:
        return ""
    _cn = "name" if "name" in df.columns else ("column_name" if "column_name" in df.columns else None)
    _ct = "type" if "type" in df.columns else None
    if _cn is None or _ct is None:
        return ""
    for _, r in df.iterrows():
        if str(r[_cn]).lower() == col_name.lower():
            return str(r[_ct] or "").strip().lower()
    return ""


def check_fund_memory_score_column() -> bool:
    """
    【V26.5 新增资金记忆体系】结构验收：列 fund_memory_score 存在，且 PRAGMA 声明为浮点存储。
    不做数值全表覆盖率（记忆分为稀疏事件驱动）；算法见 data/fund_memory_score.py。
    """
    cfg_path = os.path.join(_root(), "config.yaml")
    try:
        import yaml

        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        fn = (cfg.get("database") or {}).get("file") or "quant_data.duckdb"
    except Exception as e:
        _fail(f"读取数据库配置失败（fund_memory_score 列校验）: {e}")
        return False

    dbp = os.path.join(_root(), "data", str(fn))
    if not os.path.isfile(dbp):
        print("[SKIP] fund_memory_score：DuckDB 不存在，跳过列校验")
        return True

    try:
        import duckdb

        con = duckdb.connect(dbp, read_only=True)
        try:
            source_table, table_hint = _resolve_compatible_source_table(con)
            if source_table is None:
                print(f"[SKIP] {table_hint}")
                return True
            if table_hint:
                print(f"[INFO] fund_memory_score 列校验: {table_hint}")
            cols = con.execute(f"PRAGMA table_info('{source_table}')").fetchdf()
            if cols is None or cols.empty:
                print(f"[SKIP] {source_table} 无 PRAGMA 信息，跳过 fund_memory_score 列校验")
                return True
            _cn = "name" if "name" in cols.columns else ("column_name" if "column_name" in cols.columns else None)
            if _cn is None:
                print(f"[SKIP] {source_table} PRAGMA table_info 列名非预期，跳过 fund_memory_score 列校验")
                return True
            names = {str(x).lower() for x in cols[_cn].tolist()}
            if "fund_memory_score" not in names:
                _fail(
                    f"{source_table} 缺少列 fund_memory_score（请执行日线全量重铸或 _sync_daily_features() 特征修补）"
                )
                return False
            t = _pragma_col_type(cols, "fund_memory_score")
            if t and not any(k in t for k in ("double", "float", "real", "decimal")):
                _fail(
                    f"{source_table}.fund_memory_score 类型非浮点类（PRAGMA type={t!r}），请检查迁移/重铸"
                )
                return False
        finally:
            con.close()
    except Exception as e:
        if _duckdb_looks_like_file_lock(e):
            print(f"[WARN] DuckDB 占用中，跳过 fund_memory_score 列校验: {dbp}")
            return True
        _fail(f"fund_memory_score 列校验查询失败: {e}")
        return False

    _ok(
        f"{source_table} 已包含 fund_memory_score 列且类型为浮点（V26.5 新增资金记忆体系）"
    )
    return True


def check_filelock_installed() -> bool:
    try:
        from filelock import FileLock, Timeout
    except ImportError as e:
        _fail(f"未安装 filelock（请执行: pip install filelock>=3.13.0）: {e}")
        return False
    try:
        import tempfile

        fd, tmp = tempfile.mkstemp(prefix=".fl_", suffix=".lock", dir=_root())
        os.close(fd)
        fl = FileLock(tmp)
        fl.acquire(timeout=0)
        fl.release()
        try:
            os.remove(tmp)
        except OSError:
            pass
    except Exception as e:
        _fail(f"filelock 功能自检失败: {e}")
        return False
    _ok("filelock 已安装且可正常获取/释放 OS 文件锁")
    return True


def main() -> int:
    os.chdir(_root())
    _ensure_utf8_stdout()
    print("小杰AI选股系统 Pro V26.5 — verify_system.py")
    print("项目根:", _root())
    print("-" * 60)
    all_ok = True
    all_ok = check_config_readable() and all_ok
    all_ok = check_regime_config() and all_ok
    all_ok = check_filelock_installed() and all_ok
    all_ok = check_scan_async_dir_rw() and all_ok
    all_ok = check_embed_switch_readable() and all_ok
    all_ok = check_duckdb_health() and all_ok
    all_ok = check_capital_resonance_coverage() and all_ok
    all_ok = check_fund_memory_score_column() and all_ok
    all_ok = check_regime_analyzer_call() and all_ok
    print("-" * 60)
    if all_ok:
        print("结论: 全部检查通过。")
        return 0
    print("结论: 存在失败项，请根据上述 [FAIL] 修复后再启动。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
