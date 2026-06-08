# -*- coding: utf-8 -*-
"""
每周 DuckDB 维护（方案二）：maintenance_mode → 终止守护进程与 Streamlit → 独占 CHECKPOINT+VACUUM → 解除锁。
守护进程重启由外层 ``start_daemon_24x7.bat`` 看门狗（进程退出后 60s）完成，本脚本不再 subprocess 拉起，避免与看门狗双重唤醒抢锁。

由 auto_sniper_daemon 在 03:30 投递，或任务计划直接执行：
    python tools/weekly_db_maintenance_orchestrated.py --no-pause

说明：全量/缺失下载、data_fetcher 同步若与维护窗口重叠，均可能占库；本脚本通过先结束本项目的
守护与 UI 进程，避免与 VACUUM 争用。请勿在维护进行中手动运行其它直连 quant_data.duckdb 的工具脚本。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo

from core.file_utils import atomic_json_update

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)

_LOG_PATH = os.path.join(_PROJECT_ROOT, "data", "runtime", "weekly_maintenance.log")
_PIPELINE_STATE = os.path.join(_PROJECT_ROOT, "data", "runtime", "state", "sniper_pipeline_state.json")
BJ = ZoneInfo("Asia/Shanghai")


def _setup_logging() -> None:
    os.makedirs(os.path.dirname(_LOG_PATH) or ".", exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [WEEKLY_MAINT] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    # 开启日志轮转，防止 7x24 纯后台模式下日志文件无限膨胀撑爆磁盘（与 core/log_config.py 策略一致）
    fh = RotatingFileHandler(
        _LOG_PATH,
        maxBytes=50 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)


def _read_pipeline_state() -> dict:
    if not os.path.isfile(_PIPELINE_STATE):
        return {}
    try:
        with open(_PIPELINE_STATE, "r", encoding="utf-8") as f:
            o = json.load(f)
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}


def _write_pipeline_state_patch(**kwargs) -> None:
    now = datetime.now(BJ)

    def _upd(cur: dict) -> None:
        for k, v in kwargs.items():
            if v is not None:
                cur[k] = v
        cur["updated_at"] = now.isoformat()

    try:
        os.makedirs(os.path.dirname(_PIPELINE_STATE) or ".", exist_ok=True)
        atomic_json_update(_PIPELINE_STATE, _upd, timeout=5)
    except Exception as e:
        logging.warning("写入 pipeline 状态失败: %s", e)


def _notify(title: str, detail: str, *, category: str, dedup_key: str) -> None:
    try:
        from core.notification_gateway import notify_wechat_system_alert

        notify_wechat_system_alert(title=title, detail=detail, category=category, dedup_key=dedup_key)
    except Exception as e:
        logging.debug("企微通知失败（忽略）: %s", e)


def main() -> int:
    ap = argparse.ArgumentParser(description="每周 DuckDB 编排维护（方案二）")
    ap.add_argument("--no-pause", action="store_true", help="非交互（供计划任务/守护投递）")
    ap.add_argument("--force", action="store_true", help="忽略本周已执行标记，强制执行")
    args = ap.parse_args()

    _setup_logging()
    log = logging.getLogger("weekly_maint")

    from core.master_control import write_master_control
    from data.db_core import duckdb_disk_bytes_total, duckdb_vacuum_silent

    now = datetime.now(BJ)
    week_key = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"

    st = _read_pipeline_state()
    if not args.force and str(st.get("last_weekly_vacuum_week", "")).strip() == week_key:
        log.info("本周已记录过 weekly VACUUM (%s)，跳过（可用 --force）", week_key)
        return 0

    curr_min = now.hour * 60 + now.minute
    if (9 * 60) <= curr_min <= (15 * 60 + 30):
        log.info("当前为交易时段，跳过每周库维护")
        return 0

    _tools_dir = os.path.join(_PROJECT_ROOT, "tools")
    if _tools_dir not in sys.path:
        sys.path.insert(0, _tools_dir)
    from maintenance_process_control import stop_project_db_holders

    log.info("开始每周编排维护 | week=%s | 根目录=%s", week_key, _PROJECT_ROOT)

    write_master_control(maintenance_mode=True)
    try:
        log.info("步骤：等待 8s 后终止守护进程与 Streamlit …")
        stop_project_db_holders(_PROJECT_ROOT, my_pid=os.getpid(), settle_sec=8.0, wait_dead_sec=90.0)

        before = int(duckdb_disk_bytes_total() or 0)
        duckdb_vacuum_silent(log)
        after = int(duckdb_disk_bytes_total() or 0)
        done_at = datetime.now(BJ)

        _write_pipeline_state_patch(
            last_weekly_vacuum_week=week_key,
            last_weekly_vacuum_at=done_at.isoformat(),
            last_weekly_vacuum_before_bytes=before,
            last_weekly_vacuum_after_bytes=after,
        )
        log.info(
            "CHECKPOINT+VACUUM 完成 | %.2fMB -> %.2fMB",
            before / 1024.0 / 1024.0,
            after / 1024.0 / 1024.0,
        )
        _notify(
            title="【每周数据库维护】CHECKPOINT+VACUUM 完成",
            detail=(
                f"时间：{done_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"周标识：{week_key}\n"
                f"说明：方案二编排（已结束占用进程后独占维护）\n"
                f"库占用：{before / 1024.0 / 1024.0:.2f}MB → {after / 1024.0 / 1024.0:.2f}MB"
            ),
            category="daemon",
            dedup_key=f"daemon_weekly_vacuum_ok_{week_key}",
        )
    except Exception as e:
        log.exception("每周编排维护失败: %s", e)
        _notify(
            title="【每周数据库维护】执行失败",
            detail=(
                f"时间：{now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"周标识：{week_key}\n"
                f"说明：方案二编排失败\n"
                f"异常：{str(e)[:900]}"
            ),
            category="data_sync",
            dedup_key=f"daemon_weekly_vacuum_fail_{week_key}",
        )
        return 1
    finally:
        write_master_control(maintenance_mode=False)
        log.info("已解除 maintenance_mode")

    # 【V26.6 架构更新】已剥离主动唤醒职能，交由外层 start_daemon_24x7.bat 的 60 秒看门狗机制自动完成重启，彻底消灭双进程抢锁隐患。
    log.info(
        "本脚本不再 subprocess 拉起守护；若由 7x24 外壳启动，请保持 start_daemon_24x7.bat 看门狗运行以在约 60s 后自动拉起 auto_sniper_daemon。"
    )

    if not args.no_pause:
        input("✅ 每周维护执行完毕！按 Enter 键退出...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
