# -*- coding: utf-8 -*-
"""证券简称展示清洗：去掉行情源偶发行首或中间用于标记的 + / ＋。"""
from __future__ import annotations

import re
from typing import Any


# 市值体型体型标记 emoji（🦍🐘🐎🐥），不应出现在展示名称中
_SIZE_EMOJI_RE = re.compile(r"[\U0001F98D\U0001F418\U0001F40E\U0001F425]\s*")


def normalize_stock_display_name(name: Any) -> str:
    s = str(name or "")
    # 先去除市值体型标记 emoji（🦍🐘🐎🐥）
    s = _SIZE_EMOJI_RE.sub("", s)
    s = re.sub(r"^[\s\+＋]+", "", s).strip()
    s = re.sub(r"\s*\+\s*", " ", s)
    s = re.sub(r"\s*＋\s*", " ", s)
    s = re.sub(r" {2,}", " ", s).strip()
    return s
