from __future__ import annotations

"""Small helpers for technical report generation."""

from typing import Any

import pandas as pd


def get_completeness(metrics_df: pd.DataFrame, table: str, column: str) -> float | None:
    mask = (
        (metrics_df["metric_name"] == "completeness_pct")
        & (metrics_df["table"] == table)
        & (metrics_df["column"] == column)
    )
    if not mask.any():
        return None
    return float(metrics_df.loc[mask, "metric_value"].iloc[0])


def truncate(value: Any, *, max_len: int = 140) -> str:
    s = "NULL" if value is None else str(value)
    s = s.replace("\n", " ").strip()
    return s if len(s) <= max_len else s[: max_len - 3] + "..."

