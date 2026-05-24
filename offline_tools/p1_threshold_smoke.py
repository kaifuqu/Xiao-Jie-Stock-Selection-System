# -*- coding: utf-8 -*-
"""小杰AI选股系统 Pro V26.6 - 一次性抽样：统计 P1 入池/拒绝对象分布（勿在业务中 import）。
P1 打分与 get_stock_data_qfq 一致：真实换手来自 turnover_rate_f 或 vol×close/circ_mv 反算，不读 total turnover_rate。
"""
import os
import sys
from collections import Counter

_PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(_PROJECT)
if _PROJECT not in sys.path:
    sys.path.insert(0, _PROJECT)

from data.db_core import get_all_stock_codes, get_p1_candidate_codes, get_stock_data_qfq
from core.pool_manager import build_p1_pool_and_cache


def main():
    limit = int(os.environ.get("P1_SMOKE_LIMIT", "450"))
    codes = get_p1_candidate_codes() or []
    if not codes:
        codes = get_all_stock_codes() or []
    codes = list(dict.fromkeys(codes))[:limit]

    mock = []
    for c in codes:
        df = get_stock_data_qfq(c, limit=120)
        if not df.empty:
            mock.append({"code": c, "df": df, "hist": df.iloc[-1].to_dict()})

    print(f"sample_codes={len(codes)} with_kline={len(mock)}")
    pool, rej = build_p1_pool_and_cache(mock, progress_callback=None)

    scores = [float(x.get("p1_score", 0)) for x in pool]
    rej_scores = []
    for r in rej:
        try:
            rej_scores.append(float(r.get("当前得分", 0)))
        except Exception:
            pass

    stages = Counter(str(r.get("被裁阶段", "")) for r in rej)
    reasons = Counter(str(r.get("淘汰死因", "")) for r in rej)
    rs = [x for x in rej_scores if x > 0]

    print(f"pool={len(pool)} rejected={len(rej)}")
    if scores:
        print(f"pool_score min={min(scores):.2f} max={max(scores):.2f}")
    else:
        print("pool_score empty")
    if rs:
        print(f"rejected_with_score>0 count={len(rs)} max={max(rs):.2f}")
    print("stage_counts", dict(stages.most_common(8)))
    print("top_reasons", reasons.most_common(15))


if __name__ == "__main__":
    main()
