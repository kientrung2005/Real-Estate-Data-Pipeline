"""Hàm chuẩn hóa dùng chung cho pipeline crawl."""

from typing import Optional

import pandas as pd


def normalize_id(value: Optional[object]) -> Optional[str]:
    """Chuẩn hóa ID để tránh giá trị rỗng hoặc giả null."""
    if value is None:
        return None
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "nan", "null"}:
        return None
    return text
