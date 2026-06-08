# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 - 数据库核心驱动 (DuckDB UPSERT 防重装甲版)

【模块职责】
- 统一管理 DuckDB 连接、表结构演进、按主键 UPSERT 落库。
- 提供 QFQ 前复权读取链路，并与技术指标预计算模块解耦绑定。
- 提供股票基础信息（行业）同步与板块统计查询。

【本次优化要点】
0. 短时独占连接：`get_write_conn` / `get_read_conn` 为 contextmanager（finally 强制 close）；进程内长生命周期读写仍用 `get_conn` / `get_read_conn_singleton`。`duckdb_vacuum_silent` 走 ``with get_write_conn()`` 并对 ``duckdb.IOException`` / ``duckdb.CatalogException`` 最多重试 3 次（间隔 2s）。
1. 修复 sync_stock_basic：必须先 register 再 CREATE TABLE AS；合并 L+D+P 并补 daily_data 缺码，避免简称/行业映射不全。
2. config.yaml 路径：改为相对本文件定位到项目根目录，避免从非项目根启动时找不到配置。
3. 异常与日志：关键路径补充异常信息，避免静默失败；不改变对外 DataFrame 列顺序语义（落库仍按传入 df 的列顺序）。
4. 数据库路径固定为 `<项目根>/data/<文件名>`，避免误配到其它目录。
5. QFQ 与指标兜底逻辑保持原行为，仅在注释上澄清契约。
6. daily_data 列集随 data_fetcher.ALL_55_COLS 演进；【V26.6 新增资金记忆体系】含 capital_resonance_score（DOUBLE，0~100）、
   fund_memory_score（DOUBLE，0~200，半衰期见 constants.FUND_MEMORY_HALF_LIFE_DAYS）。
   全量重铸时 CREATE 随 DataFrame；增量 UPSERT 时 _ensure_table_schema 对缺失列执行 ALTER TABLE ADD COLUMN。
   手工修补示例：ALTER TABLE daily_data ADD COLUMN capital_resonance_score DOUBLE;
   ALTER TABLE daily_data ADD COLUMN fund_memory_score DOUBLE;
"""
from __future__ import annotations

# Standard library
import atexit
import json
import logging
import os
import random
import re
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Set, Tuple

# Third-party
import duckdb
import pandas as pd
import tushare as ts
import yaml

# ---------------------------------------------------------------------------
# 指标模块：动态导入，保证从任意工作目录启动时均可找到 core 包
# ---------------------------------------------------------------------------
try:
    from core.indicator_calc import precompute_indicators
except ImportError as e:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)
    try:
        from core.indicator_calc import precompute_indicators
    except ImportError as e2:
        logging.exception("db_core 无法导入 precompute_indicators，已降级为恒等映射: %s", e2)
        # 恒等映射：下游仍可运行，但无衍生指标（由 get_stock_data_qfq 内兜底补列）
        precompute_indicators = lambda x: x

try:
    from core.stock_name_utils import normalize_stock_display_name
except ImportError as e:
    logging.exception("db_core 无法导入 normalize_stock_display_name，已降级为基础字符串清洗: %s", e)
    def normalize_stock_display_name(name):
        return str(name or "").strip()

# ---------------------------------------------------------------------------
# 配置加载：固定从「项目根目录/config.yaml」读取，避免依赖当前 shell 的 cwd
# 项目根通过「自本文件向上查找 config.yaml」解析，避免仅依赖「data 的父目录」在目录结构变化时误判。
# ---------------------------------------------------------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def _resolve_project_root():
    """自 data/db_core.py 所在目录向上查找包含 config.yaml 的目录，作为项目根。"""
    d = _THIS_DIR
    for _ in range(16):
        cfg = os.path.join(d, "config.yaml")
        if os.path.isfile(cfg):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    fallback = os.path.dirname(_THIS_DIR)
    logging.warning("未在父目录链上找到 config.yaml，回退项目根（旧逻辑）: %s", fallback)
    return fallback


_PROJECT_ROOT = os.path.abspath(_resolve_project_root())
_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config.yaml")
_SECTOR_RANK_LAST_SB_SYNC_TRY_TS = 0.0
_SECTOR_RANK_LAST_SB_WARN_TS = 0.0
_SECTOR_RANK_SB_SYNC_COOLDOWN_SEC = 15 * 60
_SECTOR_RANK_WARN_COOLDOWN_SEC = 10 * 60

try:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    logging.error(f"❌ 未找到配置文件: {_CONFIG_PATH}，将使用内置默认数据库路径。")
    config = {}
except Exception as e:
    logging.error(f"❌ 读取 config.yaml 失败: {e}，将使用内置默认数据库路径。")
    config = {}

# DuckDB 文件始终落在「项目根目录/data/<文件名>」；config 里只写文件名，禁止 data/xxx 造成重复嵌套
_raw_db = (config.get("database") or {}).get("file") or "quant_data.duckdb"
_db_name = os.path.basename(str(_raw_db).replace("\\", "/"))
if not _db_name.lower().endswith(".duckdb"):
    _db_name = "quant_data.duckdb"
db_path = os.path.abspath(os.path.join(_PROJECT_ROOT, "data", _db_name))
os.makedirs(os.path.dirname(db_path), exist_ok=True)
logging.info("DuckDB 主库绝对路径（请在此路径查找 quant_data.duckdb）: %s", db_path)


def get_duckdb_path():
    """与 get_conn 使用同一解析后的绝对路径，供热成像等直连 DuckDB 的模块复用。"""
    return db_path


def duckdb_checkpoint(force: bool = False, max_attempts: int = 5):
    """
    大事务（如全表 DROP + 全量 INSERT）后执行，将 WAL 落盘合并，抑制 .duckdb + .wal 体积无谓翻倍。

    - 默认使用 `CHECKPOINT`（尽量不阻塞）。
    - 若外部仍有活跃连接导致 checkpoint 不能及时完成，可传 `force=True` 使用 `FORCE CHECKPOINT`。
    """
    last_err = None
    for attempt in range(max_attempts):
        try:
            con = get_conn()
            # FORCE CHECKPOINT 在存在并发连接时通常更可靠（但可能更阻塞）。
            con.execute("FORCE CHECKPOINT" if force else "CHECKPOINT")
            return
        except Exception as e:
            last_err = e
            # 锁/并发类错误需要稍等后再试
            if attempt < max_attempts - 1 and _duckdb_is_lock_error(e):
                delay = 0.2 * (2 ** attempt) + random.uniform(0, 0.1)
                time.sleep(delay)
                continue
            break
    if last_err is not None:
        # 提升可观测性：WAL 残留会导致 UI 里物理体积暴涨
        logging.warning("V26 duckdb_checkpoint(%s) 失败（%s/%s）: %s", force, attempt + 1, max_attempts, last_err)


def duckdb_disk_bytes_total():
    """
    主库文件 + 可能存在的侧车文件（.wal / .tmp）字节数，供 UI 展示「真实占用」。
    """
    total = 0
    base = db_path
    for p in (base, base + ".wal", base + ".tmp"):
        try:
            if os.path.isfile(p):
                total += os.path.getsize(p)
        except OSError:
            continue
    return total


def duckdb_storage_snapshot() -> Dict[str, Any]:
    """返回主库/侧车/备份表的轻量体积快照，供 V26 维护日志与 UI 诊断使用。"""
    snap = {
        "db_path": db_path,
        "db_bytes": 0,
        "wal_bytes": 0,
        "tmp_bytes": 0,
        "backup_tables": 0,
        "total_bytes": 0,
    }
    try:
        if os.path.isfile(db_path):
            snap["db_bytes"] = os.path.getsize(db_path)
        if os.path.isfile(db_path + ".wal"):
            snap["wal_bytes"] = os.path.getsize(db_path + ".wal")
        if os.path.isfile(db_path + ".tmp"):
            snap["tmp_bytes"] = os.path.getsize(db_path + ".tmp")
        snap["total_bytes"] = int(snap["db_bytes"] + snap["wal_bytes"] + snap["tmp_bytes"])
    except OSError:
        pass
    try:
        con = get_read_conn_singleton(max_wait_sec=5.0)
        if con is not None:
            rows = con.execute(
                """
                SELECT COUNT(*)
                FROM duckdb_tables()
                WHERE table_name LIKE '%__backup_%'
                   OR table_name LIKE '%__rebuild_%'
                   OR table_name LIKE '%__tmp_%'
                """
            ).fetchone()
            snap["backup_tables"] = int(rows[0]) if rows and rows[0] is not None else 0
    except Exception:
        pass
    return snap


def duckdb_storage_snapshot_text() -> str:
    s = duckdb_storage_snapshot()
    return (
        f"db={s['db_bytes'] / 1024 / 1024:.2f}MB, "
        f"wal={s['wal_bytes'] / 1024 / 1024:.2f}MB, "
        f"tmp={s['tmp_bytes'] / 1024 / 1024:.2f}MB, "
        f"backup_tables={s['backup_tables']}, total={s['total_bytes'] / 1024 / 1024:.2f}MB"
    )


def duckdb_vacuum_silent(log: Optional[Any] = None) -> None:
    """
    在业务侧读写任务结束后执行的静默碎片整理：多轮 CHECKPOINT + ANALYZE + VACUUM，
    **必须**经 ``with get_write_conn() as con:`` 短时独占写连接，不删除业务数据、不改表结构。

    多轮整理策略：
    Round 1: CHECKPOINT → ANALYZE → VACUUM → CHECKPOINT
    Round 2: ANALYZE → VACUUM → CHECKPOINT（基于第一轮统计信息再次压缩）
    Round 3（膨胀率 > 1.2x 时触发）: ANALYZE → VACUUM → CHECKPOINT

    与「增量 UPSERT / ALL_55_COLS 列契约」无关：仅压缩存储碎片并写回主文件，供 P5 盘后扫描链等单点调用。
    重试容错：仅对 ``duckdb.IOException``、``duckdb.CatalogException`` 间隔 ``time.sleep(2)`` 最多 3 次；
    彻底失败则 ``logger.error`` 记录并退出本函数（不向外抛）。
    """
    lg = log if log is not None else logging.getLogger(__name__)
    t0 = time.perf_counter()
    t_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    _IOExc = getattr(duckdb, "IOException", None)
    _CatExc = getattr(duckdb, "CatalogException", None)

    def _is_vacuum_retryable(exc: BaseException) -> bool:
        if _IOExc is not None and isinstance(exc, _IOExc):
            return True
        if _CatExc is not None and isinstance(exc, _CatExc):
            return True
        return False

    def _one_round(con) -> bool:
        try:
            con.execute("ANALYZE")
            con.execute("VACUUM")
            return True
        except Exception as e:
            lg.debug("ANALYZE+VACUUM 步骤异常（不阻断）: %s", e)
            return False

    def _get_db_bytes() -> int:
        try:
            return int(duckdb_disk_bytes_total())
        except Exception:
            return 0

    lg.info("DuckDB VACUUM 开始 | %s", t_start)

    before_bytes = _get_db_bytes()
    for attempt in range(3):
        try:
            with get_write_conn() as con:
                con.execute("CHECKPOINT")
                for _round_num in range(2):
                    _one_round(con)
                con.execute("CHECKPOINT")
            break
        except Exception as e:
            if _is_vacuum_retryable(e) and attempt < 2:
                lg.warning(
                    "DuckDB CHECKPOINT/VACUUM 遇 IOException/CatalogException (%s/3)，2s 后重试: %s",
                    attempt + 1,
                    e,
                )
                time.sleep(2.0)
                continue
            lg.error(
                "DuckDB CHECKPOINT/VACUUM 失败（已达 %s 次或非可重试异常）: %s",
                attempt + 1,
                e,
                exc_info=True,
            )
            return

    after_bytes = _get_db_bytes()
    ratio = (after_bytes / before_bytes) if before_bytes > 0 else 0.0

    # 膨胀率 > 1.2x 时执行第三轮深度压缩
    if before_bytes > 0 and after_bytes > int(before_bytes * 1.2):
        lg.warning(
            "DuckDB VACUUM Round 1+2 后体积膨胀 %.2fx，执行第三轮深度 ANALYZE+VACUUM",
            ratio,
        )
        for attempt in range(3):
            try:
                with get_write_conn() as con:
                    for _r in range(3):
                        if not _one_round(con):
                            break
                    con.execute("CHECKPOINT")
                after_bytes = _get_db_bytes()
                break
            except Exception as e:
                if _is_vacuum_retryable(e) and attempt < 2:
                    time.sleep(1.5)
                    continue
                lg.debug("第三轮深度 VACUUM 异常（不阻断）: %s", e)
                break

    final_bytes = _get_db_bytes()
    final_ratio = (final_bytes / before_bytes) if before_bytes > 0 else 0.0
    elapsed = time.perf_counter() - t0
    t_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    lg.info(
        "DuckDB VACUUM 结束 | %s | 耗时 %.2f 秒 | "
        "db: %.2fMB -> %.2fMB (%.2fx) | V26 静默瘦身完成",
        t_end,
        elapsed,
        before_bytes / 1024.0 / 1024.0,
        final_bytes / 1024.0 / 1024.0,
        final_ratio,
    )


def _duckdb_sanitize_orphan_wal():
    """
    若主库 .duckdb 不存在但存在同名 .wal（误删主文件、崩溃或路径不一致遗留），
    DuckDB 可能无法生成可见的主库文件。删除孤立侧车文件后，下次 connect 会创建新主库。
    """
    if os.path.isfile(db_path):
        return
    # 另一进程可能正在创建主库文件：短暂等待后再确认，避免误删刚写入的 WAL
    for _ in range(15):
        if os.path.isfile(db_path):
            return
        time.sleep(0.05)
    for suffix in (".wal", ".tmp"):
        side = db_path + suffix
        if not os.path.isfile(side):
            continue
        logging.warning(
            "检测到无主库文件的孤立侧车文件（期望主库: %s），将删除: %s",
            db_path,
            side,
        )
        try:
            os.remove(side)
        except OSError as e:
            logging.error("无法删除孤立文件 %s: %s", side, e)


def _duckdb_recover_from_corrupt_wal_connect_error(err: BaseException) -> bool:
    """
    DuckDB 在重放损坏 WAL 时可能抛出 INTERNAL Error。
    这类错误通常意味着该 `.wal` 侧车不可用：为保证服务可启动，
    将其先备份（重命名）再重连。

    返回 True 表示已触发恢复动作；False 表示非目标错误或恢复失败。
    """
    msg = str(err)
    if "Failure while replaying WAL file" not in msg:
        return False

    # 尝试从异常文本中提取 WAL 路径（异常里常带 Windows `\\?\` 前缀）。
    m = re.search(r'replaying WAL file\s+"([^"]+\.wal)"', msg)
    wal_path = m.group(1) if m else (db_path + ".wal")
    wal_path = wal_path.replace("\\\\?\\", "").replace("\\\\?", "")

    # 兜底：确保 wal_path 是我们期望的同一主库同名侧车。
    if not wal_path.lower().endswith(".wal"):
        wal_path = db_path + ".wal"

    ts = int(time.time())
    if os.path.isfile(wal_path):
        bak_path = f"{wal_path}.bak_corrupt_{ts}"
        try:
            os.replace(wal_path, bak_path)
            logging.warning(
                "DuckDB WAL 重放失败已恢复：已备份并丢弃损坏 WAL（%s -> %s），错误: %s",
                wal_path,
                bak_path,
                err,
            )
        except OSError as e:
            logging.error("无法备份损坏 WAL（%s）：%s", wal_path, e)
            return False
    else:
        # 若异常文本里的路径和本地实际不一致，仍允许走继续重连逻辑。
        logging.warning(
            "DuckDB WAL 重放失败但未找到待恢复的 WAL 文件: %s（仍将尝试重连）。错误: %s",
            wal_path,
            err,
        )

    # 侧车 tmp 在部分异常场景下也可能需要清理（但不强制）。
    tmp_path = db_path + ".tmp"
    if os.path.isfile(tmp_path):
        try:
            os.replace(tmp_path, f"{tmp_path}.bak_corrupt_{ts}")
        except OSError:
            pass

    return True


def get_project_root():
    """与主库路径一致的项目根目录（绝对路径），供诊断与其它模块复用。"""
    return _PROJECT_ROOT


# 模块加载时尽早清理孤立 WAL，避免「仅有 .wal 无主库」时无法生成可见的 quant_data.duckdb
_duckdb_sanitize_orphan_wal()


def _duckdb_err_pid(err: str):
    m = re.search(r"PID\s+(\d+)", err, flags=re.IGNORECASE)
    return m.group(1) if m else None


def probe_duckdb_lock():
    """
    启动前锁检测：必须复用本进程已有连接执行 SELECT 1。
    若此处再 duckdb.connect 到同一文件，且与已有连接的 read_only 配置不一致，
    DuckDB 会报：Can't open a connection to same database file with a different configuration...
    """
    try:
        con = get_read_conn_singleton()
        if con is None:
            con = get_conn()
        con.execute("SELECT 1").fetchone()
        return {"ok": True, "msg": f"DuckDB 可用: {db_path}", "pid": None}
    except duckdb.IOException as e:
        err = str(e)
        return {"ok": False, "msg": err, "pid": _duckdb_err_pid(err)}
    except duckdb.CatalogException as e:
        err = str(e)
        return {"ok": False, "msg": err, "pid": _duckdb_err_pid(err)}
    except Exception as e:
        err = str(e)
        logging.debug("probe_duckdb_lock 未分类异常: %s", e, exc_info=True)
        return {"ok": False, "msg": err, "pid": _duckdb_err_pid(err)}


def init_tushare_pro():
    """
    初始化 Tushare Pro 接口。
    Token 优先级：os.environ['TUSHARE_TOKEN'] > .env > config.yaml。
    """
    # 1. 先尝试从环境变量 / .env 读取
    _dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(_dotenv_path):
        try:
            with open(_dotenv_path, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    if "=" not in stripped:
                        continue
                    k, _, v = stripped.partition("=")
                    k, v = k.strip(), v.strip().strip('"').strip("'")
                    if k == "TUSHARE_TOKEN" and v:
                        os.environ.setdefault(k, v)
        except Exception:
            pass

    # 2. 环境变量优先级最高
    token = os.environ.get("TUSHARE_TOKEN", "")
    endpoint = ""

    # 3. 其次从 config.yaml 读取（支持覆盖）
    if not token:
        token = config.get("tushare", {}).get("token", "")
        endpoint = (config.get("tushare", {}).get("custom_endpoint", "") or "").strip()

    if not token:
        logging.error("未在 .env / 环境变量 / config.yaml 中找到 Tushare Token。")
        return None

    try:
        ts.set_token(token)
        pro = ts.pro_api(token)
        if endpoint:
            pro._DataApi__http_url = endpoint
        return pro
    except Exception as e:
        logging.error(f"Tushare 专线初始化失败: {e}")
        return None


# ==================== 全局连接（写 / 只读）====================
# Windows 下多进程各开一个「写连接」会争用同一 .duckdb 文件导致 IO Error；
# 纯查询路径使用 read_only=True，可与守护进程/另一 Python 进程的写连接并存。
_conn_lock = threading.RLock()
_write_con = None
_read_con = None
# 【V26.7 新增】追踪所有创建的只读连接，以便在需要时关闭它们
# 解决 "different configuration" 错误：确保在创建写连接前关闭所有只读连接
_readonly_conns = []

# 【V26.8 新增】只读模式标志
# 当设置为 True 时，本进程只能创建只读连接，禁止创建写连接
# 用于 Streamlit UI 等只查询不写入的场景，与 sniper daemon 的写操作共存
_READONLY_MODE = os.environ.get("XIAOJIE_READONLY_DB", "0") == "1"


def set_readonly_mode(enabled: bool = True):
    """设置只读模式。启用后 get_conn() 将拒绝创建写连接，只能创建只读连接。"""
    global _READONLY_MODE
    _READONLY_MODE = bool(enabled)
    if _READONLY_MODE:
        logging.info("DuckDB 只读模式已启用（XIAOJIE_READONLY_DB=1）")
    else:
        logging.info("DuckDB 只读模式已禁用")


def is_readonly_mode() -> bool:
    """返回当前是否为只读模式。"""
    return _READONLY_MODE


def _register_readonly_conn(con):
    """注册只读连接以便后续追踪。"""
    global _readonly_conns
    if con not in _readonly_conns:
        _readonly_conns.append(con)


def _close_all_readonly_conns():
    """
    关闭所有追踪的只读连接。

    用于解决 "different configuration" 错误：
    当需要创建写连接时，先调用此函数关闭所有只读连接。
    """
    global _readonly_conns, _read_con
    # 关闭所有追踪的只读连接
    for con in _readonly_conns:
        try:
            con.close()
        except Exception:
            pass
    _readonly_conns = []
    # 关闭全局只读连接
    if _read_con is not None:
        try:
            _read_con.close()
        except Exception:
            pass
        _read_con = None


def _duckdb_connect_write():
    """统一写连接创建入口，便于后续收口连接策略。"""
    return duckdb.connect(db_path)


def _duckdb_connect_readonly():
    """统一只读连接创建入口，便于后续收口连接策略。"""
    con = duckdb.connect(db_path, read_only=True)
    _register_readonly_conn(con)
    return con


# 线程本地只读连接池：每个调用线程持有一个独立 DuckDB 只读连接
# 解决 ThreadPoolExecutor 并行调用 get_stock_data_qfq 时共享单例连接导致 result closed 崩溃
_thread_local = threading.local()


def close_thread_local_conn():
    """
    清理当前线程的本地只读连接。

    用于解决以下场景：
    1. Phase 1 批量下载时断点续传调用 get_read_conn_singleton() 创建了只读连接
    2. Phase 2 全量重铸需要 get_conn() 创建写连接
    3. DuckDB 在 Windows 上同一进程内不能同时存在 read_only=True 和 read_only=False 连接

    在 Phase 1 结束后、Phase 2 开始前调用此函数，清理线程本地只读连接，
    避免与后续写连接产生 "different configuration" 错误。
    """
    if hasattr(_thread_local, 'conn') and _thread_local.conn is not None:
        try:
            _thread_local.conn.close()
        except Exception:
            pass
        _thread_local.conn = None
    # 重置线程本地状态
    if hasattr(_thread_local, '_query_count'):
        _thread_local._query_count = 0


def close_temp_read_connections():
    """
    关闭通过 get_read_conn_singleton() 创建的临时只读连接。

    当进程内存在只读连接时，后续调用 get_conn() 创建写连接会触发
    DuckDB "Can't open a connection to same database file with a different configuration" 错误。

    此函数通过关闭所有可能的连接源来清除残留的只读连接：
    1. 清理线程本地连接
    2. 关闭全局只读连接（如果存在）

    注意：此函数不会关闭写连接（_write_con），因为那是业务连接。
    """
    import gc
    # 先清理线程本地连接
    close_thread_local_conn()
    # 强制垃圾回收，关闭不可达的只读连接对象
    gc.collect()
    # 最后清理全局只读连接
    global _read_con
    with _conn_lock:
        if _read_con is not None:
            try:
                _read_con.close()
            except Exception:
                pass
            _read_con = None

def _get_thread_local_read_conn():
    """
    返回当前线程独立的 DuckDB 连接（线程内复用）。

    【V26.7 最终修复版】
    核心策略：
    1. 若 _write_con 已建立 → 直接复用写连接（写连接可读，无配置冲突）
    2. 若 _write_con 未建立 → 创建非只读连接（可读可写，不会有配置冲突）
    3. 绝对不创建 read_only=True 连接（会与后续 get_conn() 的写连接冲突）
    
    【V26.8 修复】在创建新连接前，必须清理所有只读连接，
    避免 "Can't open a connection to same database file with a different configuration" 错误。
    """
    import time as _time_module

    if not hasattr(_thread_local, 'conn') or _thread_local.conn is None:
        _thread_local.conn = None
        _thread_local._query_count = 0

        # 【V26.7 核心策略】检查全局写连接
        if _write_con is not None:
            _thread_local.conn = _write_con
            _thread_local._source_table = "daily_data"
            return _thread_local.conn

        # 【V26.8 关键修复】在创建新连接前，清理所有只读连接
        # 避免 "different configuration" 错误：同一进程内不能同时存在只读和读写连接
        _close_all_readonly_conns()
        if hasattr(_thread_local, 'conn') and _thread_local.conn is not None:
            try:
                _thread_local.conn.close()
            except Exception:
                pass
            _thread_local.conn = None

        # 【V26.8 关键修复】不创建 read_only=True 连接！
        # 只创建普通连接（可读可写），避免与后续 get_conn() 的写连接冲突
        try:
            _thread_local.conn = duckdb.connect(db_path)
            _thread_local._query_count = 0
            try:
                _thread_local._source_table = (
                    "vw_daily_data_compat"
                    if _table_exists_via_conn(_thread_local.conn, "vw_daily_data_compat")
                    else "daily_data"
                )
            except Exception:
                _thread_local._source_table = "daily_data"
        except Exception as _conn_err:
            logging.warning("线程本地连接创建失败: %s", _conn_err)
            # 降级：尝试使用全局写连接（如果可用）
            if _write_con is not None:
                _thread_local.conn = _write_con
                _thread_local._source_table = "daily_data"
            else:
                return None

    _thread_local._query_count += 1
    # 每 500 次查询强制重建连接
    if _thread_local._query_count > 500:
        _thread_local._query_count = 0
        # 关闭旧连接
        try:
            if _thread_local.conn is not None and _thread_local.conn != _write_con:
                _thread_local.conn.close()
        except Exception:
            pass
        _thread_local.conn = None
        # 【V26.8 关键修复】重连前清理所有只读连接
        _close_all_readonly_conns()
        # 重新获取
        if _write_con is not None:
            _thread_local.conn = _write_con
        else:
            try:
                _thread_local.conn = duckdb.connect(db_path)
            except Exception:
                return None
        try:
            _thread_local._source_table = (
                "vw_daily_data_compat"
                if _table_exists_via_conn(_thread_local.conn, "vw_daily_data_compat")
                else "daily_data"
            )
        except Exception:
            _thread_local._source_table = "daily_data"

    return _thread_local.conn


def _table_exists_via_conn(conn, table_name):
    """用已有连接判断表是否存在（不走全局锁）。"""
    try:
        df = conn.execute("SHOW TABLES").fetchdf()
        if df is not None and not df.empty and 'name' in df.columns:
            return table_name in df['name'].astype(str).values
        return False
    except Exception:
        return False

def _safe_thread_local_query(ts_code, limit, offset):
    """用线程本地连接执行查询，失败时自动重连一次。"""
    conn = _get_thread_local_read_conn()
    if conn is None:
        return pd.DataFrame()
    # 使用线程内缓存的表名（避免每次查询都调 table_exists 争抢全局锁）
    source_table = getattr(_thread_local, '_source_table', None) or 'daily_data'
    try:
        return conn.execute(
            f"SELECT * FROM {source_table} WHERE ts_code = ? ORDER BY trade_date DESC LIMIT ? OFFSET ?",
            [str(ts_code), limit, offset]
        ).fetchdf()
    except Exception as e:
        err_msg = str(e).lower()
        # 【V26.7 检测配置冲突错误，触发重连修复】
        if any(k in err_msg for k in ("result closed", "wal", "writefile", "replay", "different configuration")):
            # 关闭旧连接并重建
            try:
                if _thread_local.conn is not None and _thread_local.conn != _write_con:
                    _thread_local.conn.close()
            except Exception:
                pass
            _thread_local.conn = None
            # 重新获取连接
            conn = _get_thread_local_read_conn()
            if conn is not None:
                try:
                    return conn.execute(
                        f"SELECT * FROM {source_table} WHERE ts_code = ? ORDER BY trade_date DESC LIMIT ? OFFSET ?",
                        [str(ts_code), limit, offset]
                    ).fetchdf()
                except Exception:
                    return pd.DataFrame()
            return pd.DataFrame()
        raise


def _duckdb_is_lock_error(exc: BaseException) -> bool:
    """识别 DuckDB 文件锁 / 并发写导致的可重试错误（INSERT/UPDATE/DDL 常见）。"""
    msg = str(exc).lower()
    if "database is locked" in msg:
        return True
    if "could not set lock" in msg:
        return True
    if "serialization conflict" in msg and "transaction" in msg:
        return True
    return False


def _duckdb_transient_connect_error(exc: BaseException) -> bool:
    """
    只读 connect 阶段：另一进程（如 Streamlit）占写锁 / Windows 文件锁时的可重试错误。
    非瞬时类（配置冲突等）返回 False，避免无限空转。
    """
    raw = str(exc or "")
    low = raw.lower()
    if "different configuration" in low and "same database file" in low:
        return False
    needles = (
        "cannot open file",
        "io error",
        "already open",
        "another program",
        "being used by another",
        "进程无法访问",
        "database is locked",
        "could not set lock",
        "conflicting lock",
        "could not obtain",
        "latch",
        "temporarily unavailable",
    )
    if any(x in low for x in needles):
        return True
    if "quant_data.duckdb" in low and ("lock" in low or "error" in low or "busy" in low):
        return True
    return False


def _duckdb_write_with_lock_retry(operation_label, write_fn, max_attempts=5):
    """
    写库自适应避让：捕获 database is locked 等锁冲突时 Sleep 后退避重试。
    max_attempts=5 表示最多尝试 5 次（含首次），用尽仍失败则向上抛出最后一次异常。
    write_fn 为无参数可调用对象，其返回值将作为本函数返回值。
    """
    for attempt in range(max_attempts):
        try:
            return write_fn()
        except Exception as e:
            if attempt < max_attempts - 1 and _duckdb_is_lock_error(e):
                delay = 0.15 * (2 ** attempt) + random.uniform(0, 0.12)
                logging.warning(
                    "【写库避让】%s 遇锁 (%s/%s)，等待 %.2fs 后重试: %s",
                    operation_label,
                    attempt + 1,
                    max_attempts,
                    delay,
                    e,
                )
                time.sleep(delay)
                continue
            raise


def get_conn():
    """获取（懒创建）全局写连接。

    规范：
    - 适合需要长期复用、承担写事务的路径（同步、落库、维护任务）。
    - 同一进程内始终只保留一个写单例；若此前仅有只读连接，则会先关闭只读再建写。
    - 长事务结束后，优先显式 commit/rollback；需要物理整理时再调用短连接的 ``get_write_conn()``。
    """
    global _write_con, _read_con, _READONLY_MODE
    
    # 【V26.8 只读模式检查】
    if _READONLY_MODE:
        raise RuntimeError(
            "DuckDB 只读模式已启用，禁止创建写连接。"
            "如需写入操作，请在启动时设置 XIAOJIE_READONLY_DB=0 或调用 set_readonly_mode(False)"
        )
    
    with _conn_lock:
        if _write_con is None:
            if _read_con is not None:
                try:
                    _read_con.close()
                except Exception:
                    pass
                _read_con = None
            _duckdb_sanitize_orphan_wal()
            try:
                _write_con = _duckdb_connect_write()
            except Exception as e:
                err_msg = str(e).lower()
                # 【V26.8 智能降级】检测是否被其他进程锁定
                _is_locked = (
                    "another program" in err_msg or
                    "process cannot access" in err_msg or
                    "used by another process" in err_msg or
                    "拒绝访问" in str(e) or
                    ("canada" in err_msg and "open" in err_msg)
                )
                if _is_locked:
                    # 数据库被其他进程锁定，自动降级为只读模式
                    _READONLY_MODE = True
                    logging.warning(
                        "DuckDB 写连接失败（另一进程占库），自动降级为只读模式。写入操作将不可用。"
                    )
                    raise RuntimeError(
                        "DuckDB 写连接失败（另一进程占库），自动降级为只读模式。"
                    )
                # 【V26.8 优化】处理 "different configuration" 错误
                # 这种错误表示进程内已存在只读连接，创建写连接会冲突
                # 尝试关闭所有可能的只读连接，再重试
                if "different configuration" in err_msg and "same database file" in err_msg:
                    # 关闭所有只读连接（这是正常的重试场景，不算错误）
                    _close_all_readonly_conns()
                    # 关闭线程本地连接
                    if hasattr(_thread_local, 'conn') and _thread_local.conn is not None:
                        try:
                            _thread_local.conn.close()
                        except Exception:
                            pass
                        _thread_local.conn = None
                    # 强制垃圾回收，关闭不可达的对象
                    import gc
                    gc.collect()
                    try:
                        _write_con = _duckdb_connect_write()
                        # 重试成功后记录 debug 级别日志，避免警告干扰
                        logging.debug("DuckDB 写连接在关闭只读连接后重试成功")
                    except Exception as e2:
                        logging.error("DuckDB 写连接重试仍失败: %s", e2)
                        raise
                elif _duckdb_recover_from_corrupt_wal_connect_error(e):
                    try:
                        _write_con = _duckdb_connect_write()
                    except Exception as e2:
                        logging.error("DuckDB WAL 恢复后重连仍失败: %s", e2)
                        raise
                else:
                    raise
            # WAL 残留阈值：避免大量写入后 `.wal` 长时间不被合并
            try:
                _write_con.execute("PRAGMA wal_autocheckpoint='64 MB'")
            except Exception:
                # 部分版本/配置不支持该 pragma：忽略但不阻断
                pass
            logging.info(f"DuckDB 写连接已创建: {db_path}")
        return _write_con


def get_read_conn_singleton(*, max_wait_sec: float = 0.0):
    """
    只读查询用连接（进程内单例）：优先使用 read_only 打开，避免与占用同一文件的其它进程写连接冲突。
    若本进程已建立写连接，则直接复用写连接读数据。

    max_wait_sec:
        0  — 默认约 60s 内退避重试（适合 UI 启动场景，与 sniper daemon 并发时需要更长等待）；
        >0 — 最长等待秒数，用于守护进程在 Streamlit 占锁时仍能读到代码列表。

    【V26.7 关键修复】禁止缓存只读连接到 _read_con 全局变量。
    若 _read_con 被缓存后，同一进程的另一个路径调用 get_conn() 创建了写连接，
    DuckDB 在 Windows 上会报 "Can't open a connection to same database file with a different configuration"，
    因为同一进程内同时存在 read_only=True 和 read_only=False 连接。
    修复：只读连接每次按需创建，用完不缓存；写连接仍在 _write_con 全局单例（可读可写）。
    """
    global _read_con, _write_con
    budget = float(max_wait_sec) if float(max_wait_sec) > 0 else 60.0
    deadline = time.time() + budget
    last_err: Optional[BaseException] = None
    attempt = 0
    while time.time() < deadline:
        with _conn_lock:
            if _write_con is not None:
                return _write_con
            # 不再从 _read_con 缓存读取，避免与后续 get_conn() 的写连接产生配置冲突
            # if _read_con is not None:
            #     return _read_con
            if not os.path.exists(db_path):
                return get_conn()
            try:
                # 【V26.8 关键修复】先关闭所有只读连接，避免配置冲突
                _close_all_readonly_conns()
                # 尝试创建只读连接
                _fresh_read_con = _duckdb_connect_readonly()
                logging.debug(f"DuckDB 只读连接已创建: {db_path}")
                return _fresh_read_con
            except Exception as e:
                last_err = e
                err_msg = str(e).lower()
                # 【V26.8 关键修复】"different configuration" 错误表示本进程内已有读写连接
                # 此时应该复用写连接而不是继续创建只读连接
                if "different configuration" in err_msg and "same database file" in err_msg:
                    if _write_con is not None:
                        logging.debug("检测到 different configuration 错误，复用已有写连接")
                        return _write_con
                if _duckdb_recover_from_corrupt_wal_connect_error(e):
                    try:
                        _close_all_readonly_conns()
                        _fresh_read_con = _duckdb_connect_readonly()
                        logging.info(f"DuckDB 只读连接已恢复: {db_path}")
                        return _fresh_read_con
                    except Exception as e2:
                        last_err = e2
                if not _duckdb_transient_connect_error(last_err):
                    logging.error("DuckDB 只读连接失败（非占锁类，不重试）: %s", last_err)
                    # 【V26.8 修复】fallback：尝试使用写连接
                    if _write_con is not None:
                        return _write_con
                    return None
        attempt += 1
        # 【V26.7 优化】改用 debug 级别，避免与 sniper daemon 并发时警告刷屏
        if attempt == 1 or attempt % 10 == 0:
            logging.debug(
                "DuckDB 只读连接受阻（另一进程可能占库），退避中 attempt=%s: %s",
                attempt,
                last_err,
            )
        time.sleep(min(8.0, 0.4 * (1.45 ** min(attempt, 14))))
    logging.warning("DuckDB 只读在 %.0fs 窗口内仍无法连接，返回空连接: %s", budget, last_err)
    return None


@contextmanager
def get_write_conn():
    """
    获取 read_only=False 的独占短连接。

    规范：
    - 仅用于 VACUUM / CHECKPOINT / 一次性维护等短时独占场景。
    - 不要在业务主循环里长期持有该连接。
    - 退出时强制 close，适合与全局写单例隔离。
    """
    con = None
    try:
        _duckdb_sanitize_orphan_wal()
        try:
            con = _duckdb_connect_write()
        except Exception as e:
            if _duckdb_recover_from_corrupt_wal_connect_error(e):
                con = _duckdb_connect_write()
            else:
                raise
        yield con
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


@contextmanager
def get_read_conn(read_only: bool = True, *, max_wait_sec: float = 0.0):
    """
    获取只读（或显式 read_only=False 的直连）连接。

    参数：
    - read_only: 是否只读模式
    - max_wait_sec: 只读连接最大等待秒数
      - 0（默认）：默认约 18s 内退避重试（适合 UI 高频查询）；
      - >0：最长等待秒数，用于守护进程在 Streamlit 占锁时仍能读到代码列表。

    说明：
    - read_only=True 时优先复用进程内单例读/写连接，避免同库不同配置重复建连。
    - 主库文件尚不存在时委托 ``get_write_conn`` 创建写连接并 yield。
    - read_only=False 时保留短连接语义，finally 中强制 close。
    """
    con = None
    try:
        if read_only:
            con = get_read_conn_singleton(max_wait_sec=max_wait_sec)
            if con is not None:
                yield con
                return
            if not os.path.exists(db_path):
                with get_write_conn() as wc:
                    yield wc
                return
            # get_read_conn_singleton 返回 None 时走此 fallback
            # 【V26.7 修复】fallback 也必须检查写连接，避免配置冲突
            if _write_con is not None:
                yield _write_con
                return
            con = _duckdb_connect_readonly()
            yield con
            return
        if not os.path.exists(db_path):
            with get_write_conn() as wc:
                yield wc
            return
        con = _duckdb_connect_write()
        yield con
    finally:
        # 【V26.8 关键修复】绝对不能关闭单例写连接 _write_con！
        # 当 get_read_conn_singleton() 内部检测到 _write_con 已建立时，
        # 会直接返回 _write_con（该连接由 get_conn() 的单例生命周期管理）。
        # 若在 finally 中关闭，将导致同进程所有后续数据库操作遇到 "Connection already closed"。
        if con is not None and con is not _write_con:
            try:
                con.close()
            except Exception:
                pass


def table_exists(table_name):
    """判断表是否存在；异常时返回 False，避免上层崩溃。"""
    if not os.path.exists(db_path):
        return False
    try:
        con = get_read_conn_singleton(max_wait_sec=35.0)
        if con is None:
            return False
        df = con.execute("SHOW TABLES").fetchdf()
        if df is not None and not df.empty and 'name' in df.columns:
            return table_name in df['name'].astype(str).values
        tables = con.execute("SHOW TABLES").fetchall()
        for row in tables:
            if row and row[-1] == table_name:
                return True
        return False
    except Exception as e:
        logging.debug(f"table_exists 查询失败 [{table_name}]: {e}")
        return False


def duckdb_resolve_table_sql_id(con, table_name: str) -> str:
    """
    将已存在的物理表解析为 DuckDB 可绑定的 SQL 标识符（\"catalog\".\"schema\".\"name\"）。

    打开 ``quant_data.duckdb`` 时，表往往挂在与文件名同名的 database（catalog）下；仅写
    ``\"daily_data__rebuild_new\"`` 会在部分版本/上下文中按默认 search_path 解析失败，
    出现 ``Catalog Error: Table ... does not exist! Did you mean \"quant_data....\"?``。
    """
    if not table_name:
        return '""'
    safe = str(table_name).strip().replace('"', '""')
    for sql in (
        "SELECT database_name, schema_name, table_name FROM duckdb_tables() WHERE table_name = ?",
        "SELECT database_name, schema_name, name FROM duckdb_tables() WHERE name = ?",
    ):
        try:
            row = con.execute(sql, [safe]).fetchone()
            if row:
                return f'"{row[0]}"."{row[1]}"."{row[2]}"'
        except Exception:
            continue
    try:
        row = con.execute(
            "SELECT table_catalog, table_schema, table_name FROM information_schema.tables "
            "WHERE table_name = ?",
            [safe],
        ).fetchone()
        if row:
            return f'"{row[0]}"."{row[1]}"."{row[2]}"'
    except Exception:
        pass
    raise RuntimeError(
        f"DuckDB 中未找到物理表 {table_name!r}（可能尚未 CREATE，或 save_df_to_sql 因空数据跳过建表）。"
    )


def duckdb_drop_table_if_exists_resolved(con, table_name: str) -> None:
    """DROP TABLE IF EXISTS：尝试 catalog.schema.table 与常见别名，避免残留未限定名的旧表。"""
    if not table_name:
        return
    safe = str(table_name).strip()
    stem = os.path.splitext(os.path.basename(db_path))[0]
    candidates: list = []
    try:
        q = duckdb_resolve_table_sql_id(con, safe)
        if q and q != '""':
            candidates.append(q)
    except Exception:
        pass
    if stem:
        candidates.append(f'"{stem}"."{safe}"')
        candidates.append(f'"{stem}"."main"."{safe}"')
    candidates.append(f'"{safe}"')
    seen = set()
    for frag in candidates:
        if frag in seen:
            continue
        seen.add(frag)
        try:
            con.execute(f"DROP TABLE IF EXISTS {frag}")
        except Exception:
            pass


# ==================== 表结构热修复系统 ====================
def _fix_ts_code_column(table_name):
    """
    若 ts_code 被误建成数值类型，会导致主键与字符串代码不一致。
    检测到则整表删除，由后续 UPSERT 按正确类型重建（数据需重新同步）。
    """
    con = get_conn()
    try:
        info = con.execute(f"PRAGMA table_info({table_name})").fetchdf()
        ts_col = info[info['name'] == 'ts_code']
        if not ts_col.empty and ts_col.iloc[0]['type'] in ('INTEGER', 'INT32', 'BIGINT'):
            logging.info(f"检测到 ts_code 列类型错误，正在自动修复表 {table_name}")
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            return True
    except Exception as e:
        logging.debug(f"_fix_ts_code_column 异常: {e}")
    return False


def _check_and_fix_primary_key(table_name, pk_cols):
    """
    校验主键是否与期望 pk_cols 一致；不一致则尝试迁移到新主键定义。
    返回值语义：True 表示需要按 df 全量重建空表；False 表示无需因 PK 问题触发重建。
    """
    con = get_conn()
    try:
        info = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
        if info.empty:
            return True

        existing_pks = []
        if 'pk' in info.columns:
            existing_pks = info.loc[info['pk'].astype(bool), 'name'].tolist()

        if set(existing_pks) != set(pk_cols):
            temp_table = f"{table_name}_migration"
            con.execute(f"DROP TABLE IF EXISTS {temp_table}")

            cols = []
            cols = ['"' + str(row['name']) + '" ' + str(row['type']) for _, row in info.iterrows()]

            create_sql = f"""
            CREATE TABLE "{temp_table}" (
                {', '.join(cols)},
                PRIMARY KEY ({', '.join(pk_cols)})
            )
            """
            con.execute(create_sql)
            con.execute(f'INSERT INTO "{temp_table}" SELECT * FROM "{table_name}"')
            con.execute(f'DROP TABLE "{table_name}"')
            con.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
            return False

        return False
    except Exception as e:
        logging.error(f"_check_and_fix_primary_key 失败，将删除表 {table_name} 以待重建: {e}")
        try:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as drop_e:
            logging.error(f"删除表 {table_name} 失败: {drop_e}")
        return True


def _ensure_performance_indexes(con, table_name: str) -> None:
    """
    【V26.6 新增】确保关键索引存在，避免全表扫描拖慢查询。

    背景：DuckDB 对 WHERE ts_code = ? 和 WHERE trade_date BETWEEN ? AND ?
    条件过滤高度依赖索引。在 daily_data（约数百万行）上进行这类过滤时，
    无索引会导致全表扫描，查询耗时从 <100ms 升至 1-10s。

    索引策略：
    - daily_data(ts_code, trade_date)：复合索引，同时加速「某股历史」和「某日全市场」查询
    - daily_data(trade_date)：单独索引，加速按日期过滤全市场数据
    - stock_basic(industry)：加速行业统计 groupby
    - signal_log(ts_code, trade_date)：加速信号查询

    使用 IF NOT EXISTS 防止重复创建报错（DuckDB 不支持 OR REPLACE）。
    此函数在每次表结构变更后调用，安全幂等。
    """
    _INDEX_DEFINITIONS = {
        "daily_data": [
            ("idx_daily_ts_code", "ts_code"),
            ("idx_daily_trade_date", "trade_date"),
            ("idx_daily_ts_trade", "ts_code, trade_date"),
        ],
        "stock_basic": [
            ("idx_basic_industry", "industry"),
        ],
        "signal_log": [
            ("idx_signal_ts_code", "ts_code"),
            ("idx_signal_ts_trade", "ts_code, trade_date"),
        ],
    }

    indexes = _INDEX_DEFINITIONS.get(table_name, [])
    for idx_name, columns in indexes:
        try:
            con.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ({columns})')
            logging.debug("索引 %s ON %s(%s) 已就绪", idx_name, table_name, columns)
        except Exception as e:
            logging.debug("创建索引 %s 失败（可能已存在或其他原因）: %s", idx_name, e)


def _ensure_table_schema(table_name, df, pk_cols):
    """
    确保目标表存在且主键/列与当前 DataFrame 对齐。
    新增列时使用 ALTER ADD COLUMN，不擅自删列，避免破坏历史库结构。
    """
    con = get_conn()
    rebuild_needed = False

    if table_exists(table_name):
        if _fix_ts_code_column(table_name):
            rebuild_needed = True
        else:
            rebuild_needed = _check_and_fix_primary_key(table_name, pk_cols)

    if rebuild_needed or not table_exists(table_name):
        logging.info(f"🔨 重建表 {table_name}，当前列数: {len(df.columns)}，主键: {pk_cols}")
        con.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        cols = []
        for col in df.columns:
            if col in pk_cols:
                dtype = 'VARCHAR'
            elif str(df[col].dtype).startswith('int'):
                dtype = 'BIGINT'
            elif str(df[col].dtype).startswith('float'):
                dtype = 'DOUBLE'
            else:
                dtype = 'VARCHAR'
            cols.append(f'"{col}" {dtype}')

        create_sql = f"""
        CREATE TABLE "{table_name}" (
            {', '.join(cols)},
            PRIMARY KEY ({', '.join(pk_cols)})
        )
        """
        con.execute(create_sql)
        logging.info(f"✅ {table_name} 表重建完成，共 {len(df.columns)} 列")

    # 【V26.6 优化】建表后确保关键索引存在（DuckDB 对于频繁 WHERE 过滤的列非常依赖索引）
    # 索引在 PRIMARY KEY 之外独立创建（DuckDB 的主键索引与普通索引分开管理）
    # 对 daily_data：trade_date（日期范围过滤）、industry（板块联表查询）是最常用过滤列
    # 对 stock_basic：industry（行业统计）、ts_code（股票查找）高频使用
    _ensure_performance_indexes(con, table_name)

    existing = con.execute(f"PRAGMA table_info({table_name})").fetchdf()
    existing_cols = set(existing['name'])
    for col in df.columns:
        if col not in existing_cols:
            dtype = 'VARCHAR' if df[col].dtype == 'object' else 'DOUBLE'
            con.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {dtype}')
            logging.info(f"新增列: {col}")


# ==================== 核心落库系统 ====================
def _infer_pk_cols_for_df(df_local: pd.DataFrame) -> List[str]:
    """按列集合推断主键，兼容日线、分钟线、财报表、AI 日志表等非 trade_date 表。"""
    cols = set(str(c) for c in df_local.columns)
    if "id" in cols:
        return ["id"]
    if {"ts_code", "ann_date", "end_date"}.issubset(cols):
        return ["ts_code", "ann_date", "end_date"]
    if {"exchange", "trade_date"}.issubset(cols) and "ts_code" not in cols:
        return ["exchange", "trade_date"]
    if {"ts_code", "trade_time"}.issubset(cols):
        return ["ts_code", "trade_time"]
    if {"ts_code", "trade_date"}.issubset(cols):
        return ["ts_code", "trade_date"]
    if "ts_code" in cols:
        return ["ts_code"]
    return [str(df_local.columns[0])]


def save_df_to_sql(df, table_name, if_exists='append'):
    """
    将 DataFrame 以 UPSERT 方式写入 DuckDB。
    - 主键按列集合自动推断：兼容日线、分钟线、财报增强、AI 日志、维表等。
    - 不在此函数内重排列顺序；仅做日期规范化与 ts_code 字符串化，列集合与顺序保持与传入 df 一致。
    - 写事务包在「遇锁重试」内，避免并发读写下直接崩进程。
    """
    if df is None or df.empty:
        logging.warning(f"⚠️ {table_name} 数据为空，跳过")
        return

    def _write_once():
        con = get_conn()
        df_local = df.copy()
        pk_cols = _infer_pk_cols_for_df(df_local)
        is_minute_data = 'trade_time' in df_local.columns

        if not is_minute_data and 'trade_date' in df_local.columns:
            df_local['trade_date'] = pd.to_datetime(
                df_local['trade_date'].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8],
                format='%Y%m%d', errors='coerce'
            ).dt.strftime('%Y-%m-%d')
            today = pd.Timestamp.now().strftime('%Y-%m-%d')
            df_local = df_local[(df_local['trade_date'].notna()) & (df_local['trade_date'] <= today)]

        for dcol in ('ann_date', 'end_date'):
            if dcol in df_local.columns:
                df_local[dcol] = pd.to_datetime(
                    df_local[dcol].astype(str).str.replace(r'[^0-9]', '', regex=True).str[:8],
                    format='%Y%m%d', errors='coerce'
                ).dt.strftime('%Y-%m-%d')

        if 'ts_code' in df_local.columns:
            df_local['ts_code'] = df_local['ts_code'].astype(str)

        for pk in pk_cols:
            if pk in df_local.columns:
                df_local[pk] = df_local[pk].fillna("").astype(str).str.strip()
                bad_mask = df_local[pk].str.lower().isin({"", "nat", "nan", "none", "<na>", "null"})
                df_local = df_local[~bad_mask]

        _ensure_table_schema(table_name, df_local, pk_cols)

        if if_exists == 'replace':
            con.execute(f"DELETE FROM \"{table_name}\"")

        cols_str = ", ".join([f'"{c}"' for c in df_local.columns])

        update_cols = [c for c in df_local.columns if c not in pk_cols]
        pk_str = ", ".join(pk_cols)
        if update_cols:
            update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            conflict_clause = f" ON CONFLICT ({pk_str}) DO UPDATE SET {update_set}"
        else:
            conflict_clause = f" ON CONFLICT ({pk_str}) DO NOTHING"

        con.execute("BEGIN TRANSACTION")
        con.register('temp_df', df_local)
        con.execute(f'INSERT INTO "{table_name}" ({cols_str}) SELECT * FROM temp_df{conflict_clause}')
        con.unregister('temp_df')
        con.commit()

    try:
        _duckdb_write_with_lock_retry(f"save_df_to_sql:{table_name}", _write_once, max_attempts=5)
    except Exception as e:
        logging.error(f"❌ {table_name} 落库失败: {e}")
        try:
            get_conn().rollback()
        except Exception:
            pass
    finally:
        try:
            get_conn().unregister('temp_df')
        except Exception:
            pass


def get_existing_trade_dates():
    """返回日线库中已存在的全部交易日期（降序列表，元素为库内存储格式）。"""
    if not table_exists('daily_data'):
        logging.debug("get_existing_trade_dates: daily_data 表不存在，返回空列表")
        return []
    con = get_read_conn_singleton(max_wait_sec=45.0)
    if con is None:
        logging.warning("get_existing_trade_dates: 无法获取只读连接，返回空列表")
        return []
    try:
        dates = con.execute("SELECT DISTINCT trade_date FROM daily_data ORDER BY trade_date DESC").fetchall()
        return [d[0] for d in dates]
    except Exception as e:
        logging.debug("get_existing_trade_dates 查询失败: %s", e, exc_info=True)
        return []


def get_all_stock_codes():
    """返回日线库中出现过的全部 ts_code。"""
    con = get_read_conn_singleton(max_wait_sec=15.0)
    if con is None:
        logging.error("get_all_stock_codes: 无法建立 DuckDB 只读连接（占锁超时或WAL损坏），返回空列表")
        return []
    try:
        codes = con.execute("SELECT DISTINCT ts_code FROM daily_data").fetchall()
        return [c[0] for c in codes]
    except Exception as e:
        logging.debug("get_all_stock_codes 查询失败: %s", e, exc_info=True)
        return []


def _trade_date_value_to_yyyymmdd(raw):
    """将库内 trade_date 单元格规范为 8 位 YYYYMMDD；无法解析则返回空串。"""
    if raw is None:
        return ""
    s = re.sub(r"[^0-9]", "", str(raw).strip())[:8]
    return s if len(s) == 8 else ""


def _resolve_p1_daily_anchor_trade_date(con):
    """
    选择 P1 初筛 / 洗盘锚定用的 trade_date。

    盘中若已写入「当日」部分日线，MAX(trade_date) 会前移到今日，但行数常远少于上一完整交易日；
    此时按 circ_mv/PE 截面的候选数会暴减，且 hist 末行易落在不完整当日。若检测到「最新日」行数
    明显少于次新日（< max(500, 85%×次新日)），则回退到次新交易日，与「上一完整收盘日」语义一致。
    """
    try:
        base_table = "vw_daily_data_compat" if table_exists("vw_daily_data_compat") else "daily_data"
        rows = con.execute(
            f"""
            SELECT trade_date, COUNT(*) AS n
            FROM {base_table}
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 2
            """
        ).fetchall()
    except Exception as e:
        logging.debug("_resolve_p1_daily_anchor_trade_date: %s", e)
        return None, ""
    if not rows:
        return None, ""
    d_anchor = rows[0][0]
    if len(rows) >= 2:
        d0, n0 = rows[0][0], int(rows[0][1] or 0)
        d1, n1 = rows[1][0], int(rows[1][1] or 0)
        if n1 > 0 and n0 < max(500, int(0.85 * n1)):
            logging.info(
                "P1 日线锚定：采用 %s（%d 行）替代 MAX 日 %s（%d 行），避免日内未完整同步导致截面漂移",
                d1,
                n1,
                d0,
                n0,
            )
            d_anchor = d1
    ymd = _trade_date_value_to_yyyymmdd(d_anchor)
    return d_anchor, ymd


def get_latest_daily_data_trade_date_yyyymmdd():
    """
    返回用于 P1 锚定的 trade_date，规范为 8 位 YYYYMMDD 字符串。

    优先使用 vw_daily_data_compat；若不存在则回退 daily_data。
    与「裸 MAX(trade_date)」不同：若库内最新日仅为日内部分同步，会回退到上一完整交易日，
    保证休市重复跑、与 P1 初筛候选截面一致。
    """
    if not (table_exists('vw_daily_data_compat') or table_exists('daily_data')):
        return ""
    con = get_read_conn_singleton(max_wait_sec=60.0)
    if con is None:
        return ""
    try:
        _, ymd = _resolve_p1_daily_anchor_trade_date(con)
        return ymd
    except Exception as e:
        logging.debug("get_latest_daily_data_trade_date_yyyymmdd 失败: %s", e)
        return ""


def is_max_trade_date_daily_rows_sparse() -> Tuple[bool, str, int, int]:
    """
    判断「库内最新交易日」相对「次新交易日」行数是否异常偏少（半残/错日/未收全）。

    口径与 _resolve_p1_daily_anchor_trade_date 一致：最新日 n0 < max(500, 0.85×次新日 n1) 视为 sparse。
    仅 1 个有数据交易日时：n0 < 500 视为 sparse。

    返回:
        (is_sparse, max_day_yyyymmdd, n_max_day, n_prev_day)
        max_day 为按 trade_date 排序 DESC 的第一行（即最新一日）。
    """
    empty = (False, "", 0, 0)
    if not table_exists("daily_data"):
        return empty
    con = get_read_conn_singleton(max_wait_sec=60.0)
    if con is None:
        return empty
    try:
        base_table = "vw_daily_data_compat" if table_exists("vw_daily_data_compat") else "daily_data"
        rows = con.execute(
            f"""
            SELECT trade_date, COUNT(*) AS n
            FROM {base_table}
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 2
            """
        ).fetchall()
    except Exception as e:
        logging.debug("is_max_trade_date_daily_rows_sparse: %s", e)
        return empty
    if not rows:
        return empty
    d0, n0 = rows[0][0], int(rows[0][1] or 0)
    ymd0 = _trade_date_value_to_yyyymmdd(d0)
    if len(rows) < 2:
        sparse = n0 < 500
        return sparse, ymd0, n0, 0
    n1 = int(rows[1][1] or 0)
    if n1 <= 0:
        sparse = n0 < 500
        return sparse, ymd0, n0, n1
    floor_n = max(500, int(0.85 * n1))
    sparse = n0 < floor_n
    return sparse, ymd0, n0, n1


def get_p1_candidate_codes(min_mv_wan=None):
    """在锚定交易日上按流通市值与估值区间筛选 P1 候选代码；优先使用 vw_daily_data_compat，回退 daily_data。"""
    if min_mv_wan is None:
        try:
            from core.config_manager import get_p1_select_min_circ_mv_wan

            min_mv_wan = int(get_p1_select_min_circ_mv_wan())
        except Exception:
            try:
                import constants

                min_mv_wan = int(getattr(constants, "P1_SELECT_MIN_CIRC_MV_WAN", 1_000_000))
            except Exception:
                min_mv_wan = 1_000_000
    try:
        min_mv_wan = max(0, int(min_mv_wan))
    except (TypeError, ValueError):
        min_mv_wan = 600_000
    con = get_read_conn_singleton(max_wait_sec=15.0)
    if con is None:
        logging.error("get_p1_candidate_codes: 无法建立 DuckDB 只读连接（占锁超时或WAL损坏），返回空候选")
        return []
    try:
        if table_exists("vw_daily_data_compat"):
            anchor_sql = "vw_daily_data_compat"
        elif table_exists("daily_data"):
            anchor_sql = "daily_data"
        else:
            return []
        con.execute(f"SELECT 1 FROM {anchor_sql} LIMIT 1").fetchone()
    except Exception as e:
        logging.debug("get_p1_candidate_codes: 日线存在性探测失败，返回空候选: %s", e, exc_info=True)
        return []
    try:
        anchor_date, _ = _resolve_p1_daily_anchor_trade_date(con)
        if anchor_date is None:
            return []

        schema_df = con.execute(f"DESCRIBE {anchor_sql}").fetchdf()
        cols = set(schema_df['column_name'].astype(str).tolist()) if ('column_name' in schema_df.columns) else set(schema_df['name'].astype(str).tolist())
        has_pe_ttm = 'pe_ttm' in cols
        has_pe = 'pe' in cols
        if has_pe_ttm and has_pe:
            pe_filter = "((pe > 0 AND pe <= 300) OR (pe_ttm > 0 AND pe_ttm <= 300))"
        elif has_pe_ttm:
            pe_filter = "(pe_ttm > 0 AND pe_ttm <= 300)"
        elif has_pe:
            pe_filter = "(pe > 0 AND pe <= 300)"
        else:
            pe_filter = "1=1"

        circ_col = "circ_mv" if "circ_mv" in cols else ("total_mv" if "total_mv" in cols else None)
        mv_clause = f"{circ_col} >= ?" if circ_col else "1=1"
        query = f"""
            SELECT DISTINCT ts_code
            FROM {anchor_sql}
            WHERE trade_date = ?
              AND {mv_clause}
              AND {pe_filter}
        """
        params = [anchor_date]
        if circ_col:
            params.append(min_mv_wan)
        codes = con.execute(query, params).fetchall()
        return [c[0] for c in codes]
    except Exception as e:
        logging.error(f"🚨 [初筛崩溃] 提取基础池时异常: {e}")
        return get_all_stock_codes()


# ==================== QFQ 前复权与指标绑定 ====================
def get_stock_data_qfq(ts_code, limit=400, offset=0, use_thread_local=True):
    """
    取出历史数据 -> 动态执行前复权 -> 绑定计算全量技术指标 -> 返回。
    行顺序：按 trade_date 升序；列顺序：以库表 SELECT * 顺序为基底，后续仅追加指标列（不主动重排原列）。

    【V26.7 连接安全修复】默认使用 use_thread_local=True：
    - 线程本地连接会自动检测写连接并复用，避免 read_only + 读写 连接配置冲突
    - 解决 "Can't open a connection to same database file with a different configuration" 错误
    - 避免 Windows 环境下 WAL replay 导致的连接问题
    """
    try:
        # 【V26.7 统一使用线程本地连接】避免 read_only + 读写 连接配置冲突
        con = _get_thread_local_read_conn()
        if con is None:
            logging.error(f"🚨 [连接阻断] 无法获取线程本地 DuckDB 连接，跳过 {ts_code} K线读取")
            return pd.DataFrame()

        limit = max(1, min(int(limit), 5000))
        offset = max(0, int(offset))
        df = _safe_thread_local_query(str(ts_code), limit, offset)

        if not df.empty and 'trade_date' in df.columns:
            # 关键防爆：DuckDB 某些缺失兜底字段会以字符串落库，先做统一数值安检
            # turnover_rate：仅作历史/接口落库数值清洗，策略与打分一律用 turnover_rate_f（见下方回填）
            numeric_cols = [
                'open', 'high', 'low', 'close', 'pre_close', 'vol', 'volume', 'amount',
                'adj_factor', 'pe', 'pe_ttm', 'pb', 'ps_ttm', 'circ_mv', 'total_mv',
                'turnover_rate', 'turnover_rate_f', 'vol_ratio'
            ]
            for c in numeric_cols:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')

            df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce').dt.strftime('%Y%m%d')
            df = df.dropna(subset=['trade_date'])

            # 时间升序：复权与指标均依赖正确时间轴
            df = df.sort_values('trade_date').reset_index(drop=True)

            # 统一真实换手：仅 turnover_rate_f 或 vol×close/circ_mv 反算，业务侧禁止再用 total turnover_rate
            try:
                from core.strategies.fund_mv_utils import series_effective_turnover_f_daily

                if not df.empty and "close" in df.columns and ("vol" in df.columns or "volume" in df.columns):
                    df["turnover_rate_f"] = series_effective_turnover_f_daily(df)
            except Exception as e:
                logging.debug("turnover_rate_f 日线回填跳过 [%s]: %s", ts_code, e)

            is_qfq_done = False
            if 'adj_factor' in df.columns:
                df['adj_factor'] = df['adj_factor'].ffill().bfill()
                latest_factor = df['adj_factor'].iloc[-1]

                if pd.notna(latest_factor) and latest_factor > 0:
                    df['qfq_factor'] = df['adj_factor'] / latest_factor
                    for col in ['open', 'high', 'low', 'close', 'pre_close']:
                        if col in df.columns:
                            df[col] = df[col] * df['qfq_factor']
                    df.drop(columns=['qfq_factor'], inplace=True, errors='ignore')
                    is_qfq_done = True

            try:
                df = precompute_indicators(df)
                logging.info(f"[{ts_code}] 已完成 QFQ({is_qfq_done}) + 指标绑定，返回 {len(df)} 行")
            except Exception as e:
                logging.error(f"🚨 [{ts_code}] 绑定的衍生指标计算失败: {e}，启动极简滚动兜底防崩矩阵")

                if 'close' in df.columns:
                    close_s = df['close']
                    ma_windows = {'ma5': 5, 'ma10': 10, 'ma20': 20, 'ma30': 30, 'ma60': 60, 'ma120': 120, 'ma250': 250}
                    for col, w in ma_windows.items():
                        if col not in df.columns:
                            df[col] = close_s.rolling(window=w, min_periods=1).mean().ffill().bfill()

                    if 'boll_mid' not in df.columns:
                        df['boll_mid'] = df['ma20']
                    if 'boll_upper' not in df.columns:
                        df['boll_upper'] = df['boll_mid'] * 1.1
                    if 'boll_lower' not in df.columns:
                        df['boll_lower'] = df['boll_mid'] * 0.9
                    if 'vwap' not in df.columns:
                        df['vwap'] = close_s

                if 'vma5' not in df.columns and 'vol' in df.columns:
                    df['vma5'] = df['vol'].rolling(window=5, min_periods=1).mean().ffill().bfill()

                other_cols = [
                    'macd_diff', 'macd_dea', 'macd_bar', 'atr', 'atr_pct', 'atr20',
                    'bias_20', 'rsi_6', 'max_60d_pct', 'ma20_slope_5'
                ]
                for col in other_cols:
                    if col not in df.columns:
                        df[col] = 0.0

                logging.warning(f"⚠️ [{ts_code}] 兜底完毕，已启用极简滚动矩阵，保证下游策略不断流！")

            return df

        return pd.DataFrame()
    except Exception as e:
        logging.error(f"🚨 [致命断层] 提取 {ts_code} K线时报错: {e}")
        return pd.DataFrame()


def get_name_map():
    """名称映射别名，与 get_stock_names 一致。"""
    return get_stock_names()


_STOCK_BASIC_BACKFILL_CAP = 400


def _fetch_merged_stock_basic_tushare(pro) -> pd.DataFrame:
    """
    合并 list_status=L/D/P 的全市场 stock_basic，去重时保留先出现的行（上市 > 退市 > 暂停）。
    仅拉 L 会漏掉已退市但仍出现在历史日线中的代码，导致简称映射失败。
    """
    fields = "ts_code,name,industry"
    parts: List[pd.DataFrame] = []
    for st in ("L", "D", "P"):
        try:
            df = pro.stock_basic(exchange="", list_status=st, fields=fields)
            if df is not None and not df.empty:
                parts.append(df)
        except Exception as e:
            logging.warning("stock_basic list_status=%s 拉取失败（跳过该桶）: %s", st, e)
    if not parts:
        return pd.DataFrame()
    merged = pd.concat(parts, ignore_index=True)
    merged = merged.copy()
    merged["ts_code"] = merged["ts_code"].astype(str)
    merged = merged.drop_duplicates(subset=["ts_code"], keep="first")
    return merged


def _backfill_stock_basic_from_daily_gaps(pro, base_df: pd.DataFrame) -> pd.DataFrame:
    """
    daily_data 中存在但 L+D+P 主表未覆盖的 ts_code，按只请求补行（新股/北交所/边界代码）。
    """
    fields = "ts_code,name,industry"
    have: Set[str] = {
        str(x).strip().upper() for x in base_df["ts_code"].tolist() if str(x).strip()
    }
    if not table_exists("daily_data"):
        return pd.DataFrame()
    try:
        con = get_read_conn_singleton()
        dfc = con.execute("SELECT DISTINCT ts_code FROM daily_data").fetchdf()
    except Exception as e:
        logging.debug("_backfill_stock_basic_from_daily_gaps: distinct 失败: %s", e)
        return pd.DataFrame()
    if dfc is None or dfc.empty:
        return pd.DataFrame()
    missing: List[str] = []
    for raw in dfc["ts_code"].tolist():
        tc = str(raw).strip()
        if not tc or tc.upper() in have:
            continue
        missing.append(tc)
    if not missing:
        return pd.DataFrame()
    cap = int(_STOCK_BASIC_BACKFILL_CAP)
    if len(missing) > cap:
        logging.warning(
            "stock_basic 日线兜底：未命中主表的 distinct 代码数=%s，超过 cap=%s，仅补前 %s 只",
            len(missing),
            cap,
            cap,
        )
        missing = missing[:cap]
    out_parts: List[pd.DataFrame] = []
    for i, tc in enumerate(missing):
        try:
            df1 = pro.stock_basic(ts_code=tc, fields=fields)
            if df1 is not None and not df1.empty:
                out_parts.append(df1)
        except Exception as e:
            logging.debug("stock_basic 单只补全失败 ts_code=%s: %s", tc, e)
        if i > 0 and (i + 1) % 40 == 0:
            time.sleep(0.15)
    if not out_parts:
        return pd.DataFrame()
    extra = pd.concat(out_parts, ignore_index=True)
    extra["ts_code"] = extra["ts_code"].astype(str)
    return extra


def _read_security_dim_df() -> pd.DataFrame:
    """优先读取 dim_security；不存在则回退 stock_basic。"""
    con = get_read_conn_singleton()
    if con is None:
        return pd.DataFrame()
    for tbl in ("dim_security", "stock_basic"):
        try:
            if table_exists(tbl):
                df = con.execute(f"SELECT ts_code, name, industry FROM {tbl}").fetchdf()
                if df is not None and not df.empty:
                    return df
        except Exception:
            continue
    return pd.DataFrame()


def get_stock_names():
    """
    从本地维表优先拉取 ts_code->name 映射；失败再回退 Tushare。

    【V26.6 优化】新增第一优先路径：直接从 data/stock_names.json 读取，
    避免访问 DuckDB（可能涉及文件锁）和网络（Tushare API）。
    """
    # 【V26.6 优化】最快路径：JSON 文件（单次文件读取，无 DB 无网络）
    try:
        json_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "stock_names.json")
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as _f:
                import json as _json
                _names = _json.load(_f)
                if _names:
                    return _names
    except Exception:
        pass

    try:
        df = _read_security_dim_df()
        if df is not None and not df.empty and "ts_code" in df.columns and "name" in df.columns:
            out = dict(zip(df["ts_code"].astype(str), df["name"].astype(str)))
            if out:
                return out
        pro = init_tushare_pro()
        if not pro:
            logging.debug("get_stock_names: Tushare Pro 初始化失败，返回空映射")
            return {}
        df = _fetch_merged_stock_basic_tushare(pro)
        if df is None or df.empty:
            logging.debug("get_stock_names: stock_basic 拉取结果为空，返回空映射")
            return {}
        return dict(zip(df["ts_code"], df["name"]))
    except Exception as e:
        logging.debug("get_stock_names 异常: %s", e, exc_info=True)
        return {}


# ==================== P1 底仓存储 ====================
def save_p1_cache(trade_date_str, p1_list):
    """按交易日覆盖写入 P1 缓存表（先删后插同一 trade_date）。"""

    def _write_once():
        con = get_conn()
        con.execute("CREATE TABLE IF NOT EXISTS p1_cache (trade_date VARCHAR, ts_code VARCHAR, p1_score DOUBLE)")
        con.execute("DELETE FROM p1_cache WHERE trade_date = ?", [str(trade_date_str)])
        if not p1_list:
            con.commit()
            return

        data = [{'trade_date': trade_date_str, 'ts_code': x['code'], 'p1_score': x.get('p1_score', 0.0)} for x in p1_list]
        df_cache = pd.DataFrame(data)

        con.register('temp_p1', df_cache)
        con.execute("INSERT INTO p1_cache SELECT * FROM temp_p1")
        con.unregister('temp_p1')
        con.commit()

    try:
        _duckdb_write_with_lock_retry("save_p1_cache", _write_once, max_attempts=5)
    except Exception as e:
        logging.error("P1底仓落库失败: %s", e, exc_info=True)
        try:
            get_conn().rollback()
        except Exception as rb_e:
            logging.debug("P1底仓回滚失败: %s", rb_e, exc_info=True)
        try:
            get_conn().unregister('temp_p1')
        except Exception as unreg_e:
            logging.debug("P1底仓解绑 temp_p1 失败: %s", unreg_e, exc_info=True)


def ensure_v26_tables():
    """创建 V26 兼容表骨架，允许先落库再由 data_fetcher 分层同步写入。"""
    con = get_conn()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_daily_quotes (
            ts_code VARCHAR,
            trade_date VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            pct_chg DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            adj_factor DOUBLE,
            turnover_rate_f DOUBLE,
            vol_ratio DOUBLE,
            pe_ttm DOUBLE,
            pb DOUBLE,
            ps_ttm DOUBLE,
            dv_ratio DOUBLE,
            total_mv DOUBLE,
            circ_mv DOUBLE,
            source VARCHAR,
            data_version VARCHAR,
            is_valid BOOLEAN,
            ingest_time TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS bars_daily (
            ts_code VARCHAR,
            trade_date VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            pct_chg DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            adj_factor DOUBLE,
            turnover_rate_f DOUBLE,
            vol_ratio DOUBLE,
            pe_ttm DOUBLE,
            pb DOUBLE,
            ps_ttm DOUBLE,
            dv_ratio DOUBLE,
            total_mv DOUBLE,
            circ_mv DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feat_daily_core (
            ts_code VARCHAR,
            trade_date VARCHAR,
            ma5 DOUBLE,
            ma10 DOUBLE,
            ma20 DOUBLE,
            ma30 DOUBLE,
            ma60 DOUBLE,
            ma120 DOUBLE,
            vma5 DOUBLE,
            vma10 DOUBLE,
            vma20 DOUBLE,
            high_20 DOUBLE,
            low_60 DOUBLE,
            ma20_slope_5 DOUBLE,
            bias_20 DOUBLE,
            macd DOUBLE,
            macd_signal DOUBLE,
            macd_hist DOUBLE,
            rsi_14 DOUBLE,
            kdj_k DOUBLE,
            kdj_d DOUBLE,
            boll_upper DOUBLE,
            boll_lower DOUBLE,
            cci DOUBLE,
            atr_pct DOUBLE,
            capital_resonance_score DOUBLE,
            fund_memory_score DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feat_daily_capital (
            ts_code VARCHAR,
            trade_date VARCHAR,
            capital_resonance_score DOUBLE,
            net_elg_amount DOUBLE,
            net_main_amount DOUBLE,
            inst_net_buy DOUBLE,
            hk_vol DOUBLE,
            rz_net_buy DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feat_daily_memory (
            ts_code VARCHAR,
            trade_date VARCHAR,
            fund_memory_score DOUBLE,
            limit_times DOUBLE,
            strth DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_security (
            ts_code VARCHAR PRIMARY KEY,
            symbol VARCHAR,
            exchange VARCHAR,
            market VARCHAR,
            name VARCHAR,
            area VARCHAR,
            industry VARCHAR,
            fullname VARCHAR,
            list_date VARCHAR,
            delist_date VARCHAR,
            is_active BOOLEAN,
            concept_tags_json VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_trade_calendar (
            exchange VARCHAR,
            trade_date VARCHAR,
            is_open BOOLEAN,
            pretrade_date VARCHAR,
            next_trade_date VARCHAR,
            week_no INTEGER,
            month_no INTEGER,
            quarter_no INTEGER,
            year_no INTEGER,
            updated_at TIMESTAMP,
            PRIMARY KEY (exchange, trade_date)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_financial_reports (
            ts_code VARCHAR,
            ann_date VARCHAR,
            end_date VARCHAR,
            report_type VARCHAR,
            report_period VARCHAR,
            revenue DOUBLE,
            revenue_yoy DOUBLE,
            net_profit DOUBLE,
            net_profit_yoy DOUBLE,
            deduct_net_profit_yoy DOUBLE,
            op_cash_flow DOUBLE,
            asset_liab_rate DOUBLE,
            goodwill DOUBLE,
            accounts_receivable DOUBLE,
            inventories DOUBLE,
            risk_flags VARCHAR,
            summary_text VARCHAR,
            updated_at TIMESTAMP,
            PRIMARY KEY (ts_code, ann_date, end_date)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_ai_analysis_log (
            id VARCHAR PRIMARY KEY,
            ts_code VARCHAR,
            trade_date VARCHAR,
            pool_key VARCHAR,
            prompt_hash VARCHAR,
            input_json VARCHAR,
            advice VARCHAR,
            model VARCHAR,
            latency_ms DOUBLE,
            success BOOLEAN,
            error_msg VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW vw_ai_analysis_debug AS
        SELECT
            id,
            ts_code,
            trade_date,
            pool_key,
            prompt_hash,
            advice,
            model,
            latency_ms,
            success,
            error_msg,
            created_at,
            json_extract_string(input_json, '$.pool_key') AS payload_pool_key,
            json_extract_string(input_json, '$.pool_focus') AS payload_pool_focus,
            json_extract_string(input_json, '$.pool_semantics.name') AS pool_name,
            json_extract_string(input_json, '$.pool_semantics.core_question') AS pool_core_question,
            json_extract_string(input_json, '$.tactic_context.matched_keywords[0]') AS tactic_kw_1,
            json_extract_string(input_json, '$.tactic_context.matched_keywords[1]') AS tactic_kw_2,
            json_extract_string(input_json, '$.stock.code') AS stock_code,
            json_extract_string(input_json, '$.stock.name') AS stock_name,
            TRY_CAST(json_extract_string(input_json, '$.stock.score') AS DOUBLE) AS stock_score,
            TRY_CAST(json_extract_string(input_json, '$.stock.price') AS DOUBLE) AS stock_price,
            TRY_CAST(json_extract_string(input_json, '$.stock.pct_chg') AS DOUBLE) AS stock_pct_chg,
            json_extract_string(input_json, '$.stock.tactic') AS stock_tactic,
            json_extract_string(input_json, '$.stock.entry_reason') AS stock_entry_reason,
            json_extract_string(input_json, '$.stock.risk') AS stock_risk,
            json_extract_string(input_json, '$.stock.exec_tier') AS stock_exec_tier,
            json_extract_string(input_json, '$.stock.position') AS stock_position,
            json_extract_string(input_json, '$.stock.market_emotion') AS market_emotion,
            json_extract_string(input_json, '$.stock.top_industry') AS top_industry,
            json_extract_string(input_json, '$.stock.top_concept') AS top_concept,
            json_extract_string(input_json, '$.stock.deepseek_fin_text') AS deepseek_fin_text,
            json_extract_string(input_json, '$.stock.annual_report') AS annual_report,
            json_extract_string(input_json, '$.stock.quarter_report') AS quarter_report,
            json_extract_string(input_json, '$.stock.financial.summary_text') AS fin_summary_text,
            json_extract_string(input_json, '$.stock.financial.risk_flags') AS fin_risk_flags,
            json_extract_string(input_json, '$.stock.financial.report_period') AS fin_report_period,
            TRY_CAST(json_extract_string(input_json, '$.stock.financial.revenue_yoy') AS DOUBLE) AS fin_revenue_yoy,
            TRY_CAST(json_extract_string(input_json, '$.stock.financial.net_profit_yoy') AS DOUBLE) AS fin_net_profit_yoy,
            json_extract_string(input_json, '$.stock.missing_fields[0]') AS missing_field_1,
            json_extract_string(input_json, '$.stock.missing_fields[1]') AS missing_field_2,
            LENGTH(COALESCE(input_json, '')) AS input_json_len,
            input_json
        FROM fact_ai_analysis_log
        """
    )
    con.commit()


def ensure_v26_compat_view(force: bool = False) -> None:
    """创建/刷新 vw_daily_data_compat，将 V26 分层拼成旧 daily_data 风格宽表。"""
    con = get_conn()
    if force:
        try:
            con.execute("DROP VIEW IF EXISTS vw_daily_data_compat")
        except Exception:
            pass
    con.execute(
        """
        CREATE OR REPLACE VIEW vw_daily_data_compat AS
        SELECT
            b.ts_code,
            b.trade_date,
            b.open,
            b.high,
            b.low,
            b.close,
            b.pre_close,
            b.pct_chg,
            b.vol,
            b.amount,
            b.adj_factor,
            b.turnover_rate_f,
            b.vol_ratio,
            b.pe_ttm,
            b.pb,
            b.ps_ttm,
            b.dv_ratio,
            b.total_mv,
            b.circ_mv,
            c.ma5,
            c.ma10,
            c.ma20,
            c.ma30,
            c.ma60,
            c.ma120,
            c.vma5,
            c.vma10,
            c.vma20,
            c.high_20,
            c.low_60,
            c.ma20_slope_5,
            c.bias_20,
            c.macd,
            c.macd_signal,
            c.macd_hist,
            c.rsi_14,
            c.kdj_k,
            c.kdj_d,
            c.boll_upper,
            c.boll_lower,
            c.cci,
            c.atr_pct,
            CAST(NULL AS DOUBLE) AS ma250,
            c.vma5 AS vol_ma5,
            c.vma10 AS vol_ma10,
            c.vma20 AS vol_ma20,
            CAST(NULL AS DOUBLE) AS avg_cost,
            CAST(NULL AS DOUBLE) AS cost_5th,
            CAST(NULL AS DOUBLE) AS cost_50th,
            CAST(NULL AS DOUBLE) AS cost_95th,
            CAST(NULL AS DOUBLE) AS cyq_concentration,
            CAST(NULL AS DOUBLE) AS winner_rate,
            CAST(NULL AS DOUBLE) AS nineturn_signal,
            CAST(NULL AS DOUBLE) AS forecast_type,
            COALESCE(cap.capital_resonance_score, c.capital_resonance_score) AS capital_resonance_score,
            COALESCE(mem.fund_memory_score, c.fund_memory_score) AS fund_memory_score,
            cap.net_elg_amount,
            cap.net_main_amount,
            cap.inst_net_buy,
            cap.hk_vol,
            cap.rz_net_buy,
            mem.limit_times,
            mem.strth
        FROM bars_daily b
        LEFT JOIN feat_daily_core c
            ON b.ts_code = c.ts_code AND b.trade_date = c.trade_date
        LEFT JOIN feat_daily_capital cap
            ON b.ts_code = cap.ts_code AND b.trade_date = cap.trade_date
        LEFT JOIN feat_daily_memory mem
            ON b.ts_code = mem.ts_code AND b.trade_date = mem.trade_date
        """
    )


def _df_schema_set(df: pd.DataFrame) -> set:
    if df is None or df.empty:
        return set()
    return {str(c) for c in df.columns}


def verify_v26_coverage() -> Dict[str, Any]:
    """检查 V26 兼容视图相对 daily_data 的字段覆盖情况。"""
    con = get_read_conn_singleton(max_wait_sec=30.0)
    if con is None:
        return {"ok": False, "reason": "no_read_conn"}
    result: Dict[str, Any] = {"ok": False, "missing_columns": [], "extra_columns": [], "view_columns": [], "legacy_columns": []}
    try:
        legacy_tbl = "daily_data"
        view_tbl = "vw_daily_data_compat"
        if not table_exists(legacy_tbl):
            result["reason"] = "legacy_missing"
            return result
        if not table_exists(view_tbl):
            result["reason"] = "view_missing"
            return result

        legacy_df = con.execute(f"DESCRIBE {legacy_tbl}").fetchdf()
        view_df = con.execute(f"DESCRIBE {view_tbl}").fetchdf()
        legacy_cols = set(legacy_df["column_name"].astype(str).tolist()) if "column_name" in legacy_df.columns else set(legacy_df["name"].astype(str).tolist())
        view_cols = set(view_df["column_name"].astype(str).tolist()) if "column_name" in view_df.columns else set(view_df["name"].astype(str).tolist())
        result["legacy_columns"] = sorted(legacy_cols)
        result["view_columns"] = sorted(view_cols)
        result["missing_columns"] = sorted([c for c in legacy_cols if c not in view_cols])
        result["extra_columns"] = sorted([c for c in view_cols if c not in legacy_cols])
        result["ok"] = len(result["missing_columns"]) == 0
        return result
    except Exception as e:
        result["reason"] = str(e)
        return result


def verify_dim_security_consistency(sample_limit: int = 5000) -> Dict[str, Any]:
    """检查 dim_security 与 stock_basic 的关键字段一致性与覆盖率。"""
    con = get_read_conn_singleton(max_wait_sec=30.0)
    if con is None:
        return {"ok": False, "reason": "no_read_conn"}
    out: Dict[str, Any] = {"ok": False, "rows_dim": 0, "rows_stock_basic": 0, "missing_in_dim": 0, "missing_in_stock_basic": 0}
    try:
        if not table_exists("dim_security") and not table_exists("stock_basic"):
            out["reason"] = "both_missing"
            return out
        if table_exists("dim_security"):
            out["rows_dim"] = int(con.execute("SELECT COUNT(*) FROM dim_security").fetchone()[0] or 0)
        if table_exists("stock_basic"):
            out["rows_stock_basic"] = int(con.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0] or 0)
        if table_exists("dim_security") and table_exists("stock_basic"):
            q = f"""
                WITH d AS (SELECT ts_code, name, industry FROM dim_security LIMIT {int(sample_limit)}),
                     s AS (SELECT ts_code, name, industry FROM stock_basic LIMIT {int(sample_limit)})
                SELECT
                    (SELECT COUNT(*) FROM (SELECT ts_code FROM s EXCEPT SELECT ts_code FROM d)) AS missing_in_dim,
                    (SELECT COUNT(*) FROM (SELECT ts_code FROM d EXCEPT SELECT ts_code FROM s)) AS missing_in_stock_basic
            """
            row = con.execute(q).fetchone()
            out["missing_in_dim"] = int(row[0] or 0)
            out["missing_in_stock_basic"] = int(row[1] or 0)
        out["ok"] = (out["rows_dim"] > 0) and (out["rows_stock_basic"] > 0)
        return out
    except Exception as e:
        out["reason"] = str(e)
        return out


def get_ai_analysis_debug_rows(limit: int = 20, success_only: Optional[bool] = None, pool_key: str = "") -> pd.DataFrame:
    """读取 AI 分析调试视图，便于快速排查 DeepSeek 输入与建议质量。"""
    con = get_read_conn_singleton(max_wait_sec=30.0)
    if con is None:
        return pd.DataFrame()
    if not table_exists("vw_ai_analysis_debug"):
        ensure_v26_tables()
    where = []
    args: List[Any] = []
    if success_only is not None:
        where.append("success = ?")
        args.append(bool(success_only))
    if str(pool_key or "").strip():
        where.append("LOWER(pool_key) = ?")
        args.append(str(pool_key).strip().lower())
    sql = "SELECT * FROM vw_ai_analysis_debug"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY created_at DESC LIMIT {max(1, int(limit))}"
    try:
        return con.execute(sql, args).fetchdf()
    except Exception:
        return pd.DataFrame()


def get_latest_ai_analysis_payload_pretty() -> str:
    """返回最近一条 AI 分析日志的美化 JSON 文本，便于肉眼检查实际发送载荷。"""
    con = get_read_conn_singleton(max_wait_sec=30.0)
    if con is None or not table_exists("fact_ai_analysis_log"):
        return ""
    try:
        row = con.execute(
            "SELECT input_json FROM fact_ai_analysis_log ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row or not row[0]:
            return ""
        try:
            obj = json.loads(str(row[0]))
            return json.dumps(obj, ensure_ascii=False, indent=2)
        except Exception:
            return str(row[0])
    except Exception:
        return ""


def verify_v26_consistency(sample_rows: int = 5000) -> Dict[str, Any]:
    """执行 V26 一致性校验：行数、主键覆盖、字段覆盖、抽样差异。"""
    con = get_read_conn_singleton(max_wait_sec=30.0)
    result: Dict[str, Any] = {
        "ok": False,
        "legacy_rows": 0,
        "view_rows": 0,
        "raw_rows": 0,
        "bars_rows": 0,
        "feat_core_rows": 0,
        "feat_cap_rows": 0,
        "feat_mem_rows": 0,
        "sample_diff_count": 0,
        "coverage": {},
        "dim_security": {},
        "reason": "",
    }
    if con is None:
        result["reason"] = "no_read_conn"
        return result
    try:
        legacy_exists = table_exists("daily_data")
        view_exists = table_exists("vw_daily_data_compat")
        if legacy_exists:
            result["legacy_rows"] = int(con.execute("SELECT COUNT(*) FROM daily_data").fetchone()[0] or 0)
        if view_exists:
            result["view_rows"] = int(con.execute("SELECT COUNT(*) FROM vw_daily_data_compat").fetchone()[0] or 0)
        if table_exists("raw_daily_quotes"):
            result["raw_rows"] = int(con.execute("SELECT COUNT(*) FROM raw_daily_quotes").fetchone()[0] or 0)
        if table_exists("bars_daily"):
            result["bars_rows"] = int(con.execute("SELECT COUNT(*) FROM bars_daily").fetchone()[0] or 0)
        if table_exists("feat_daily_core"):
            result["feat_core_rows"] = int(con.execute("SELECT COUNT(*) FROM feat_daily_core").fetchone()[0] or 0)
        if table_exists("feat_daily_capital"):
            result["feat_cap_rows"] = int(con.execute("SELECT COUNT(*) FROM feat_daily_capital").fetchone()[0] or 0)
        if table_exists("feat_daily_memory"):
            result["feat_mem_rows"] = int(con.execute("SELECT COUNT(*) FROM feat_daily_memory").fetchone()[0] or 0)

        result["coverage"] = verify_v26_coverage()
        result["dim_security"] = verify_dim_security_consistency(sample_limit=sample_rows)

        if legacy_exists and view_exists:
            q = f"""
                WITH l AS (
                    SELECT * FROM daily_data ORDER BY ts_code, trade_date LIMIT {int(sample_rows)}
                ), v AS (
                    SELECT * FROM vw_daily_data_compat ORDER BY ts_code, trade_date LIMIT {int(sample_rows)}
                )
                SELECT COUNT(*)
                FROM (
                    SELECT ts_code, trade_date, close, pct_chg, ma20, capital_resonance_score, fund_memory_score
                    FROM l
                    EXCEPT
                    SELECT ts_code, trade_date, close, pct_chg, ma20, capital_resonance_score, fund_memory_score
                    FROM v
                ) t
            """
            result["sample_diff_count"] = int(con.execute(q).fetchone()[0] or 0)
        result["ok"] = bool(
            (not legacy_exists or result["legacy_rows"] == result["view_rows"])
            and result["coverage"].get("ok", False)
            and result["dim_security"].get("ok", False)
            and result["sample_diff_count"] == 0
        )
        return result
    except Exception as e:
        result["reason"] = str(e)
        return result


def load_p1_cache(trade_date_str):
    """读取指定交易日的 P1 缓存记录列表。"""
    if not table_exists('p1_cache'):
        return []
    con = get_read_conn_singleton()
    try:
        df = con.execute("SELECT ts_code, p1_score FROM p1_cache WHERE trade_date = ?", [str(trade_date_str)]).fetchdf()
        if df.empty:
            return []
        return df.to_dict('records')
    except Exception as e:
        logging.debug("load_p1_cache 失败: %s", e, exc_info=True)
        return []


def list_p1_cache_trade_dates_desc(limit: int = 15):
    """
    返回 p1_cache 中已有数据的交易日列表，新在前（字符串 YYYYMMDD 字典序即时间序）。
    """
    if not table_exists("p1_cache"):
        return []
    try:
        lim = max(1, min(int(limit), 365))
    except (TypeError, ValueError):
        lim = 15
    con = get_read_conn_singleton()
    try:
        df = con.execute(
            "SELECT DISTINCT trade_date FROM p1_cache ORDER BY trade_date DESC LIMIT ?",
            [lim],
        ).fetchdf()
        if df is None or df.empty:
            return []
        return [str(x).strip() for x in df["trade_date"].tolist() if str(x).strip()]
    except Exception as e:
        logging.debug("list_p1_cache_trade_dates_desc 失败: %s", e)
        return []


# ==================== 板块共振：股票基础信息 ====================
def sync_stock_basic():
    """
    同步全市场股票基础信息（含行业）到本地表 stock_basic，并同步写入 V26 维表 dim_security。
    - 合并 list_status=L/D/P，避免仅上市池漏掉退市/暂停等仍出现在日线中的代码。
    - 对 daily_data 中仍缺行的 ts_code 再逐只补拉，覆盖新股等边界。
    - 兼容旧表 stock_basic，不改变现有调用方。
    【关键修复】pandas 的 DataFrame 必须先 register 再 CREATE TABLE AS；同时写入 dim_security。
    """
    try:
        pro = init_tushare_pro()
        if not pro:
            logging.error("未找到有效 Tushare 配置，无法同步行业数据。")
            return False

        logging.info("正在拉取股票基础信息（L+D+P 合并）及日线缺失补全...")
        df_basic = _fetch_merged_stock_basic_tushare(pro)

        if df_basic.empty:
            logging.error("stock_basic 全市场拉取结果为空")
            return False

        try:
            df_gap = _backfill_stock_basic_from_daily_gaps(pro, df_basic)
            if df_gap is not None and not df_gap.empty:
                df_basic = pd.concat([df_basic, df_gap], ignore_index=True)
                df_basic["ts_code"] = df_basic["ts_code"].astype(str)
                df_basic = df_basic.drop_duplicates(subset=["ts_code"], keep="first")
                logging.info(
                    "stock_basic 日线兜底补行 %s 条，合并后共 %s 条",
                    len(df_gap),
                    len(df_basic),
                )
        except Exception as e:
            logging.warning("stock_basic 日线兜底补全失败（继续写入主合并表）: %s", e)

        # 保证代码列为字符串，与 JOIN 语义一致
        if "ts_code" in df_basic.columns:
            df_basic = df_basic.copy()
            df_basic["ts_code"] = df_basic["ts_code"].astype(str)

        now_ts = pd.Timestamp.now()
        dim_sec = pd.DataFrame({
            "ts_code": df_basic["ts_code"].astype(str),
            "symbol": df_basic["ts_code"].astype(str).str.split(".").str[0],
            "exchange": df_basic["ts_code"].astype(str).str.split(".").str[1].map({"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}).fillna(""),
            "market": "",
            "name": df_basic.get("name", pd.Series([""] * len(df_basic))).astype(str),
            "area": "",
            "industry": df_basic.get("industry", pd.Series([""] * len(df_basic))).astype(str),
            "fullname": "",
            "list_date": "",
            "delist_date": "",
            "is_active": True,
            "concept_tags_json": "",
            "updated_at": now_ts,
        })
        dim_sec = dim_sec.drop_duplicates(subset=["ts_code"], keep="first")

        def _write_stock_basic():
            con = get_conn()
            con.execute("DROP TABLE IF EXISTS stock_basic")
            con.register('df_basic', df_basic)
            try:
                con.execute("CREATE TABLE stock_basic AS SELECT * FROM df_basic")
            finally:
                try:
                    con.unregister('df_basic')
                except Exception:
                    pass
            con.register('dim_security_df', dim_sec)
            try:
                con.execute("DROP TABLE IF EXISTS dim_security")
                con.execute("CREATE TABLE dim_security AS SELECT * FROM dim_security_df")
            finally:
                try:
                    con.unregister('dim_security_df')
                except Exception:
                    pass
            con.commit()

        _duckdb_write_with_lock_retry("sync_stock_basic", _write_stock_basic, max_attempts=5)
        logging.info(f"✅ 成功同步 {len(df_basic)} 只股票基础信息入库，并写入 dim_security。")

        # 【V26.6 新增】同步落盘一份 JSON 文件（A股全量股票代码→中文名称映射），
        # 供 UI 等调用方优先从本地文件读取，跳过 fetch_realtime_batch API 调用耗时。
        # 后续 app.py 在同步缺失名称时优先加载此 JSON，完全零 API 调用。
        try:
            stock_names_json_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "stock_names.json"
            )
            os.makedirs(os.path.dirname(stock_names_json_path), exist_ok=True)
            name_dict: Dict[str, str] = {}
            for _, row in df_basic.iterrows():
                tc = str(row.get("ts_code", "")).strip()
                nm = str(row.get("name", "")).strip()
                if tc and nm and nm not in ("nan", "None", ""):
                    name_dict[tc] = nm

            def _upd(root):
                root.clear()
                root.update(name_dict)

            from core.file_utils import atomic_json_update
            atomic_json_update(stock_names_json_path, _upd, timeout=5)
            logging.info(f"✅ 股票名称映射已落盘 {stock_names_json_path}（{len(name_dict)} 只）")
        except Exception as e_json:
            logging.warning("stock_names.json 落盘失败（不影响主流程）: %s", e_json)

        return True
    except Exception as e:
        logging.error(f"同步股票基础信息失败: {e}")
        try:
            get_conn().rollback()
        except Exception:
            pass
        try:
            get_conn().unregister('df_basic')
        except Exception:
            pass
        try:
            get_conn().unregister('dim_security_df')
        except Exception:
            pass
        return False


def get_latest_sector_ranking():
    """按行业聚合最新交易日平均涨跌幅，返回 industry -> avg_pct_chg 字典。"""
    global _SECTOR_RANK_LAST_SB_SYNC_TRY_TS, _SECTOR_RANK_LAST_SB_WARN_TS
    if not (table_exists("dim_security") or table_exists("stock_basic")):
        now_ts = time.time()
        if now_ts - float(_SECTOR_RANK_LAST_SB_SYNC_TRY_TS) >= float(_SECTOR_RANK_SB_SYNC_COOLDOWN_SEC):
            _SECTOR_RANK_LAST_SB_SYNC_TRY_TS = now_ts
            try:
                ok_sync = bool(sync_stock_basic())
                if ok_sync and table_exists("stock_basic"):
                    logging.info("get_latest_sector_ranking: 已自动补齐 stock_basic，恢复板块排名计算。")
            except Exception as e:
                logging.debug("get_latest_sector_ranking: 自动补齐 stock_basic 失败: %s", e, exc_info=True)
        if not table_exists("stock_basic"):
            if now_ts - float(_SECTOR_RANK_LAST_SB_WARN_TS) >= float(_SECTOR_RANK_WARN_COOLDOWN_SEC):
                _SECTOR_RANK_LAST_SB_WARN_TS = now_ts
                logging.warning(
                    "get_latest_sector_ranking: 无 stock_basic 表，已尝试自动补齐但未成功；"
                    "请在侧栏「数据底座」同步股票基础信息。"
                )
            return {}

    if not (table_exists('vw_daily_data_compat') or table_exists('daily_data')) or not (table_exists('dim_security') or table_exists('stock_basic')):
        logging.debug("get_latest_sector_ranking: 日线/维表缺失，返回空结果")
        return {}

    with get_read_conn(max_wait_sec=30.0) as con:
        if con is None:
            logging.warning("get_latest_sector_ranking: 无法获取只读连接，返回空结果")
            return {}
        try:
            last_date_row = con.execute("SELECT MAX(trade_date) FROM daily_data").fetchone()
            if not last_date_row or not last_date_row[0]:
                logging.debug("get_latest_sector_ranking: 未取到 latest trade_date，返回空结果")
                return {}
            last_date = last_date_row[0]

            source_table = "vw_daily_data_compat" if table_exists("vw_daily_data_compat") else "daily_data"
            dim_table = "dim_security" if table_exists("dim_security") else "stock_basic"
            query = f"""
                SELECT
                    b.industry,
                    AVG(d.pct_chg) as avg_pct_chg,
                    COUNT(d.ts_code) as stock_count
                FROM {source_table} d
                JOIN {dim_table} b ON d.ts_code = b.ts_code
                WHERE d.trade_date = ?
                  AND b.industry IS NOT NULL
                  AND b.industry != ''
                GROUP BY b.industry
                HAVING COUNT(d.ts_code) >= 5
                ORDER BY avg_pct_chg DESC
            """
            df_sector = con.execute(query, [last_date]).fetchdf()

            if df_sector.empty:
                logging.debug("get_latest_sector_ranking: 行业聚合结果为空，返回空映射")
                return {}
            return dict(zip(df_sector['industry'], df_sector['avg_pct_chg']))
        except Exception as e:
            logging.error("计算板块排名异常: %s", e, exc_info=True)
            return {}


def get_stock_industry(ts_code):
    """查询单只股票行业；V26 优先 dim_security，无记录返回「未知」。"""
    source_table = "dim_security" if table_exists("dim_security") else "stock_basic"
    if not table_exists(source_table):
        logging.debug("get_stock_industry: %s 表不存在，返回未知 [%s]", source_table, ts_code)
        return "未知"
    con = get_read_conn_singleton()
    if con is None:
        logging.warning("get_stock_industry: 无法获取只读连接，返回未知 [%s]", ts_code)
        return "未知"
    try:
        res = con.execute(f"SELECT industry FROM {source_table} WHERE ts_code = ?", [str(ts_code)]).fetchone()
        return res[0] if res else "未知"
    except Exception as e:
        logging.debug("get_stock_industry 查询失败 [%s]: %s", ts_code, e, exc_info=True)
        return "未知"


def get_all_basic_industry():
    """返回 ts_code -> industry 全表映射；V26 优先 dim_security，回退 stock_basic。"""
    if not (table_exists("dim_security") or table_exists("stock_basic")):
        sync_stock_basic()
    source_table = "dim_security" if table_exists("dim_security") else "stock_basic"
    if not table_exists(source_table):
        return {}
    con = get_read_conn_singleton()
    try:
        df = con.execute(f"SELECT ts_code, industry FROM {source_table} WHERE industry IS NOT NULL AND industry != ''").fetchdf()
        return dict(zip(df['ts_code'], df['industry']))
    except Exception as e:
        logging.error("获取全市场行业映射异常: %s", e, exc_info=True)
        return {}


def _fill_names_from_tushare_map(want: frozenset, out: dict) -> None:
    """用 get_stock_names() 全市场映射补全 out 中仍缺的键（want 为大写 ts_code）。"""
    try:
        remote = get_stock_names()
        if not remote:
            return
        rem_up = {
            str(k).strip().upper(): str(v).strip()
            for k, v in remote.items()
            if str(k).strip()
        }
        for u in want:
            if out.get(u):
                continue
            alt = rem_up.get(u)
            if alt:
                out[u] = normalize_stock_display_name(alt)
    except Exception as e:
        logging.debug("map_ts_codes_to_names_local: Tushare 名称回退失败: %s", e)


def map_ts_codes_to_names_local(ts_codes):
    """
    从本地 stock_basic 批量解析 ts_code -> 中文简称。
    返回 dict，键为 **大写** ts_code（如 002709.SZ）。
    无表或空表时尝试 sync_stock_basic；仍缺则再用 Tushare stock_basic 接口补名。
    """
    if not ts_codes:
        return {}
    uniq_upper = []
    seen = set()
    for c in ts_codes:
        s = str(c).strip() if c is not None else ""
        if not s:
            continue
        u = s.upper()
        if u in seen:
            continue
        seen.add(u)
        uniq_upper.append(u)
    if not uniq_upper:
        return {}
    want = frozenset(uniq_upper)
    out: dict = {}

    if not table_exists("stock_basic"):
        try:
            sync_stock_basic()
        except Exception as e:
            logging.debug("map_ts_codes_to_names_local: sync_stock_basic(缺表): %s", e)

    if table_exists("stock_basic"):
        con = get_read_conn_singleton()
        try:
            df = con.execute("SELECT ts_code, name FROM stock_basic").fetchdf()
            if df is None or df.empty:
                try:
                    sync_stock_basic()
                    df = con.execute("SELECT ts_code, name FROM stock_basic").fetchdf()
                except Exception as e:
                    logging.debug("map_ts_codes_to_names_local: 空表重同步失败: %s", e)
            if df is not None and not df.empty:
                # 【性能优化 V3】向量化：用 Series 索引替代 iterrows
                tc_series = df['ts_code'].astype(str).str.strip().str.upper()
                nm_series = df['name'].astype(str).str.strip().apply(normalize_stock_display_name)
                ts_to_nm = dict(zip(tc_series, nm_series))
                for tc_u in want:
                    if tc_u in ts_to_nm:
                        out[tc_u] = ts_to_nm[tc_u]

        except Exception as e:
            logging.warning("map_ts_codes_to_names_local: 读 stock_basic 失败: %s", e)

    if len(out) < len(want):
        _fill_names_from_tushare_map(want, out)
    return out


# ---------------------------------------------------------------------------
# 指数日线兜底（指挥舱顶部卡片：腾讯行情失败时从 daily_data 取最新一根）
# 原 core/market_regime.RegimeEngine 逻辑收拢至此，避免多文件占位。
# ---------------------------------------------------------------------------
_INDEX_LATEST_CACHE: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}
_INDEX_LATEST_TTL_SEC = 45.0


def get_index_latest_from_daily_data(ts_code: str) -> Optional[Dict[str, Any]]:
    """
    返回 {close, pct_chg}；无数据或异常时 None。
    带 45s 进程内缓存，与旧 RegimeEngine 行为一致。
    """
    code = str(ts_code or "").strip()
    if not code:
        return None
    now = time.monotonic()
    hit = _INDEX_LATEST_CACHE.get(code)
    if hit is not None:
        ts, payload = hit
        if (now - ts) < _INDEX_LATEST_TTL_SEC and payload is not None:
            return dict(payload)
    try:
        con = get_read_conn_singleton()
        if con is None:
            logging.warning("get_index_latest_from_daily_data: 无法获取只读连接，返回 None [%s]", code)
            _INDEX_LATEST_CACHE[code] = (now, None)
            return None
        source_table = "vw_daily_data_compat" if table_exists("vw_daily_data_compat") else "daily_data"
        row = con.execute(
            f"""
            SELECT close, pct_chg FROM {source_table}
            WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 1
            """,
            [code],
        ).fetchone()
    except Exception as e:
        logging.debug("get_index_latest_from_daily_data 失败 [%s]: %s", code, e, exc_info=True)
        row = None
    if not row:
        _INDEX_LATEST_CACHE[code] = (now, None)
        return None
    try:
        close_v = float(row[0])
    except (TypeError, ValueError):
        _INDEX_LATEST_CACHE[code] = (now, None)
        return None
    try:
        pct = float(row[1]) if row[1] is not None else 0.0
    except (TypeError, ValueError):
        pct = 0.0
    out: Dict[str, Any] = {"close": close_v, "pct_chg": pct}
    _INDEX_LATEST_CACHE[code] = (now, out)
    return dict(out)


def close_db():
    """进程退出时关闭全局连接。"""
    global _write_con, _read_con
    with _conn_lock:
        if _read_con is not None:
            try:
                _read_con.close()
            except Exception:
                pass
            _read_con = None
        if _write_con is not None:
            try:
                _write_con.close()
            except Exception:
                pass
            _write_con = None


atexit.register(close_db)
