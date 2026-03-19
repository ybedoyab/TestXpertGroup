from __future__ import annotations

"""DWH date-key helper."""

from datetime import date
from typing import Any

import pandas as pd


def date_key_from_iso(iso: Any) -> int:
    if iso is None or (isinstance(iso, float) and pd.isna(iso)):
        return 0
    s = str(iso).strip()
    if not s:
        return 0
    try:
        dt = date.fromisoformat(s)
        return dt.year * 10000 + dt.month * 100 + dt.day
    except ValueError:
        return 0

