# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.6 — 独立运维脚本：维护模式 + 优雅终止守护进程 + DuckDB VACUUM + 解除维护锁

用法（在项目根或任意目录）:
    python tools/force_maintenance_vacuum.py

说明：依赖 psutil 做进程枚举；若缺失会尝试用当前解释器自动 pip 安装。
本脚本不再 subprocess 拉起 auto_sniper_daemon；7x24 部署请保持外层 start_daemon_24x7.bat
看门狗运行（进程退出后 60 秒自动重启），避免与维护脚本「双重唤醒」争抢 DuckDB。
"""
from __future__ import annotations

import os
import sys
import time

# ---------- 1) 项目根路径，保证可导入 core / data ----------
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.chdir(_PROJECT_ROOT)

from core.master_control import write_master_control  # noqa: E402
from data.db_core import duckdb_vacuum_silent  # noqa: E402

_TOOLS_DIR = os.path.join(_PROJECT_ROOT, "tools")
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)
from maintenance_process_control import (  # noqa: E402
    ensure_psutil,
    stop_project_db_holders,
)

ensure_psutil()


def _print_step(msg: str) -> None:
    print(f"\n>>> {msg}", flush=True)


def main() -> None:
    print("=" * 60, flush=True)
    print("小杰AI选股系统 Pro V26.6 — force_maintenance_vacuum 运维脚本", flush=True)
    print(f"项目根: {_PROJECT_ROOT}", flush=True)
    print("=" * 60, flush=True)

    # 步骤1：打开维护锁（后续任一步崩溃也必须释放，否则 UI/守护会永久跳过写库与同步）
    _print_step("步骤1：write_master_control(maintenance_mode=True)")
    write_master_control(maintenance_mode=True)
    try:
        # 步骤2：缓刑期
        _print_step("步骤2：等待 8 秒（守护进程完成当前微小事务）")
        time.sleep(8)

        # 步骤3：终止本项目守护进程与 Streamlit（释放 DuckDB 文件锁），并等待退出
        _print_step("步骤3：terminate 守护进程 + Streamlit，并等待进程退出")
        nd, ns = stop_project_db_holders(_PROJECT_ROOT, my_pid=os.getpid(), settle_sec=0.0, wait_dead_sec=90.0)
        print(f"    已对守护 {nd} 个、Streamlit {ns} 个进程发起 terminate，并已等待释放", flush=True)

        # 步骤4：再等待后执行 VACUUM
        _print_step("步骤4：等待 3 秒后执行 duckdb_vacuum_silent()")
        time.sleep(3)
        duckdb_vacuum_silent()
    except Exception as exc:
        print(f"    ❌ 维护流程中发生异常（仍将解除维护锁）: {exc}", flush=True)
        raise
    finally:
        _print_step("解除维护锁：write_master_control(maintenance_mode=False)")
        write_master_control(maintenance_mode=False)

    # 【V26.6 架构更新】已剥离主动唤醒职能，交由外层 start_daemon_24x7.bat 的 60 秒看门狗机制自动完成重启，彻底消灭双进程抢锁隐患。
    _print_step("步骤5（原步骤6已移除）：不再 subprocess 拉起守护进程")
    print(
        "    若使用 7x24 外壳：请保持 start_daemon_24x7.bat 看门狗运行；守护被维护杀死后约 60 秒将自动重启。",
        flush=True,
    )
    print("    若未使用看门狗，请在本机手动执行: python auto_sniper_daemon.py", flush=True)

    input("✅ 维护执行完毕！按 Enter 键退出...")


if __name__ == "__main__":
    main()
