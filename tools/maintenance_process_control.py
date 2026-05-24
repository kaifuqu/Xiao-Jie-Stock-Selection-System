# -*- coding: utf-8 -*-
"""
小杰AI选股系统 Pro V26.5 — 数据库维护用进程编排（方案二）
终止占用 quant_data.duckdb 的本项目 Python 进程（守护 + Streamlit），供
weekly_db_maintenance_orchestrated / force_maintenance_vacuum 复用。
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from typing import Optional, Set, Tuple

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None  # type: ignore


def ensure_psutil():
    """懒加载 psutil，缺失时尝试 pip 安装。"""
    global psutil
    if psutil is not None:
        return psutil
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "psutil>=5.9.0"],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        import psutil as _ps  # noqa: WPS433

        psutil = _ps
    except Exception as e:
        raise RuntimeError(
            "需要 psutil：pip install psutil>=5.9.0"
        ) from e
    return psutil


def _norm_root(project_root: str) -> str:
    return os.path.normcase(os.path.abspath(project_root))


def _cmdline_references_project(joined: str, project_root: str) -> bool:
    """命令行是否指向当前项目目录（避免误杀其它目录下的同名脚本）。"""
    nr = _norm_root(project_root)
    nj = os.path.normcase(joined)
    if nr in nj:
        return True
    base = os.path.basename(project_root.rstrip("\\/"))
    return bool(base) and base.lower() in nj.lower()


def _process_cwd_under_project(proc, project_root: str) -> bool:
    """进程工作目录是否在本项目根下（用于 streamlit run ui/app.py 等相对路径启动）。"""
    try:
        cwd = os.path.normcase(os.path.abspath(proc.cwd()))
        root = _norm_root(project_root)
        return cwd == root or cwd.startswith(root + os.sep)
    except Exception:
        return False


def terminate_auto_sniper_daemons(project_root: str, *, my_pid: Optional[int] = None) -> int:
    """
    终止 cmdline 含 auto_sniper_daemon 且属于本项目的 Python 进程（不含 my_pid）。
    返回已发送 terminate 的进程数。
    """
    ps = ensure_psutil()
    killed = 0
    my_pid = my_pid if my_pid is not None else os.getpid()
    needle = "auto_sniper_daemon"
    for p in ps.process_iter(["pid", "name", "cmdline"]):
        try:
            if p.pid == my_pid:
                continue
            cmdline = p.info.get("cmdline")
            if not cmdline:
                continue
            joined = " ".join(str(x) for x in cmdline if x)
            if needle.lower() not in joined.lower():
                continue
            if not (
                _cmdline_references_project(joined, project_root)
                or _process_cwd_under_project(p, project_root)
            ):
                continue
            name_l = (p.info.get("name") or "").lower()
            if "python" not in name_l and "python" not in joined.lower():
                continue
            print(f"    terminate daemon pid={p.pid} cmdline[:4]={cmdline[:4]!r}", flush=True)
            p.terminate()
            killed += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        except Exception as e:
            print(f"    跳过 pid={getattr(p, 'pid', '?')}: {e}", flush=True)
    return killed


def terminate_streamlit_for_project(project_root: str, *, my_pid: Optional[int] = None) -> int:
    """
    终止「streamlit run … ui/app.py」且工作目录/路径指向本项目的进程。
    Windows 下 Streamlit 长期只读打开 DuckDB，会导致独占 VACUUM 失败。
    """
    ps = ensure_psutil()
    killed = 0
    my_pid = my_pid if my_pid is not None else os.getpid()
    for p in ps.process_iter(["pid", "name", "cmdline"]):
        try:
            if p.pid == my_pid:
                continue
            cmdline = p.info.get("cmdline")
            if not cmdline:
                continue
            joined = " ".join(str(x) for x in cmdline if x)
            jl = joined.lower()
            if "streamlit" not in jl:
                continue
            if "app.py" not in jl:
                continue
            if not (
                _cmdline_references_project(joined, project_root)
                or _process_cwd_under_project(p, project_root)
            ):
                continue
            print(f"    terminate streamlit pid={p.pid} cmdline[:5]={cmdline[:5]!r}", flush=True)
            p.terminate()
            killed += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        except Exception as e:
            print(f"    跳过 pid={getattr(p, 'pid', '?')}: {e}", flush=True)
    return killed


def _collect_project_python_pids(project_root: str, *, my_pid: int) -> Set[int]:
    """用于终止后等待：与本项目守护/Streamlit 相关的 Python PID。"""
    ps = ensure_psutil()
    out: Set[int] = set()
    root = _norm_root(project_root)
    for p in ps.process_iter(["pid", "cmdline"]):
        try:
            if p.pid == my_pid:
                continue
            cmdline = p.info.get("cmdline")
            if not cmdline:
                continue
            joined = " ".join(str(x) for x in cmdline if x)
            if not _cmdline_references_project(joined, project_root):
                continue
            jl = joined.lower()
            if "auto_sniper_daemon" in jl and (
                _cmdline_references_project(joined, project_root)
                or _process_cwd_under_project(p, project_root)
            ):
                out.add(p.pid)
            elif (
                "streamlit" in jl
                and "app.py" in jl
                and (
                    _cmdline_references_project(joined, project_root)
                    or _process_cwd_under_project(p, project_root)
                )
            ):
                out.add(p.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return out


def wait_until_no_holders_or_timeout(
    project_root: str,
    *,
    my_pid: int,
    timeout_sec: float = 90.0,
    poll_sec: float = 0.5,
) -> bool:
    """轮询直至上述进程退出或超时。超时返回 False。"""
    ps = ensure_psutil()
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        alive = _collect_project_python_pids(project_root, my_pid=my_pid)
        if not alive:
            return True
        time.sleep(poll_sec)
    # 超时后再查一次
    return len(_collect_project_python_pids(project_root, my_pid=my_pid)) == 0


def force_kill_project_holders(project_root: str, *, my_pid: int) -> int:
    """对仍存活的目标进程发送 kill()（SIGKILL 等价），返回杀掉的数量。"""
    ps = ensure_psutil()
    nk = 0
    for pid in _collect_project_python_pids(project_root, my_pid=my_pid):
        try:
            p = ps.Process(pid)
            print(f"    kill -9 pid={pid}", flush=True)
            p.kill()
            nk += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return nk


def stop_project_db_holders(
    project_root: str,
    *,
    my_pid: Optional[int] = None,
    settle_sec: float = 8.0,
    wait_dead_sec: float = 90.0,
) -> Tuple[int, int]:
    """
    设置维护锁前由调用方完成；此处：可选先 sleep → terminate 守护与 Streamlit → 等待退出 → 必要时强杀。
    返回 (terminate 守护数, terminate UI 数) 的合计语义改为：(daemon_n, streamlit_n)
    """
    my = my_pid if my_pid is not None else os.getpid()
    if settle_sec > 0:
        time.sleep(float(settle_sec))
    d = terminate_auto_sniper_daemons(project_root, my_pid=my)
    s = terminate_streamlit_for_project(project_root, my_pid=my)
    ok = wait_until_no_holders_or_timeout(project_root, my_pid=my, timeout_sec=wait_dead_sec)
    if not ok:
        print("    ⚠ 等待进程退出超时，尝试 force kill …", flush=True)
        force_kill_project_holders(project_root, my_pid=my)
        time.sleep(2.0)
    return d, s
