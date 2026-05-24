#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
离线压缩 DuckDB（强收敛体积）：
1) EXPORT DATABASE 到临时目录
2) IMPORT DATABASE 到新库文件
3) 校验关键行数（daily_data）
4) 原子替换旧库（保留 .bak 备份）

注意：必须在所有 Python 进程停止后执行，否则会因文件锁失败。
"""

from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime

import duckdb


def _mb(n: int) -> str:
    return f"{n / 1024 / 1024:.2f}MB"


def main() -> int:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = os.path.join(root, "data", "quant_data.duckdb")
    if not os.path.isfile(src):
        print(f"[FAIL] 数据库不存在: {src}")
        return 2

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp_dir = os.path.join(root, "data", "runtime", f"db_compact_export_{ts}")
    new_db = os.path.join(root, "data", f"quant_data_compact_{ts}.duckdb")
    bak_db = os.path.join(root, "data", f"quant_data_{ts}.bak.duckdb")

    os.makedirs(os.path.dirname(tmp_dir), exist_ok=True)
    old_size = os.path.getsize(src)
    print(f"[INFO] 原库: {src} size={_mb(old_size)}")

    try:
        con = duckdb.connect(src)
        con.execute(f"EXPORT DATABASE '{tmp_dir}' (FORMAT PARQUET)")
        old_rows = 0
        try:
            old_rows = int(con.execute("SELECT COUNT(*) FROM daily_data").fetchone()[0] or 0)
        except Exception:
            old_rows = 0
        con.close()

        con2 = duckdb.connect(new_db)
        con2.execute(f"IMPORT DATABASE '{tmp_dir}'")
        con2.execute("CHECKPOINT")
        con2.execute("VACUUM")
        con2.execute("CHECKPOINT")
        new_rows = 0
        try:
            new_rows = int(con2.execute("SELECT COUNT(*) FROM daily_data").fetchone()[0] or 0)
        except Exception:
            new_rows = 0
        con2.close()

        if old_rows > 0 and new_rows != old_rows:
            print(f"[FAIL] 行数校验失败 daily_data: old={old_rows}, new={new_rows}")
            return 3

        os.replace(src, bak_db)
        os.replace(new_db, src)
        new_size = os.path.getsize(src)
        print(f"[OK] 压缩完成: {_mb(old_size)} -> {_mb(new_size)}")
        print(f"[OK] 旧库备份: {bak_db}")
        return 0
    except Exception as e:
        print(f"[FAIL] 压缩失败: {e}")
        return 1
    finally:
        try:
            if os.path.isdir(tmp_dir):
                shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        try:
            if os.path.isfile(new_db):
                os.remove(new_db)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

