# -*- coding: utf-8 -*-
"""证券简称展示清洗：去掉行情源偶发行首或中间用于标记的 + / ＋。"""
from __future__ import annotations

import re
from typing import Any


# 历史遗留 emoji 清洗（已停止向名称添加市值 emoji，保留此正则用于兼容旧数据）
_SIZE_EMOJI_RE = re.compile(r"[\U0001F98D\U0001F418\U0001F40E\U0001F425]\s*")


def normalize_stock_display_name(name: Any) -> str:
    s = str(name or "")
    # 兼容旧数据：去除可能残留的历史市值 emoji
    s = _SIZE_EMOJI_RE.sub("", s)
    s = re.sub(r"^[\s\+＋]+", "", s).strip()
    s = re.sub(r"\s*\+\s*", " ", s)
    s = re.sub(r"\s*＋\s*", " ", s)
    s = re.sub(r" {2,}", " ", s).strip()
    return s
