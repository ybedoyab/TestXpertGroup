from __future__ import annotations

"""Base quality metrics helpers."""

from typing import Any

import pandas as pd


def completeness(df: pd.DataFrame, columns: list[str]) -> dict[str, Any]:
    total = max(len(df), 1)
    out: dict[str, Any] = {}
    for c in columns:
        if c not in df.columns:
            continue
        non_null = int(df[c].notna().sum())
        out[c] = {"non_null_count": non_null, "completeness_pct": non_null / total}
    return out


def pk_uniqueness(df: pd.DataFrame, pk_col: str) -> dict[str, Any]:
    total = len(df)
    if total == 0 or pk_col not in df.columns:
        return {"unique_pk_count": 0, "pk_uniqueness_pct": None, "duplicate_pk_rows": 0}
    pk = df[pk_col]
    duplicate_rows = int(pk.duplicated().sum())
    unique_pk = int(pk.nunique(dropna=True))
    return {
        "unique_pk_count": unique_pk,
        "pk_uniqueness_pct": unique_pk / total if total else None,
        "duplicate_pk_rows": duplicate_rows,
    }


def validity_pct_non_null(values_valid_mask: pd.Series) -> float:
    non_null = int(values_valid_mask.notna().sum())
    if values_valid_mask.empty or non_null == 0:
        return float("nan")
    valid = int(values_valid_mask.sum())
    return valid / non_null


def flatten_metrics(*, dataset_metrics: dict[str, Any], global_metrics: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for metric_name, value in global_metrics.items():
        rows.append(
            {
                "scope": "global",
                "table": None,
                "column": None,
                "metric_name": metric_name,
                "metric_value": value,
            }
        )

    for table_name, tm in dataset_metrics.items():
        if "completeness" in tm:
            for col, comp in tm["completeness"].items():
                rows.append(
                    {
                        "scope": "table_column",
                        "table": table_name,
                        "column": col,
                        "metric_name": "completeness_pct",
                        "metric_value": comp["completeness_pct"],
                    }
                )
        if "validity" in tm:
            for vn, val in tm["validity"].items():
                rows.append(
                    {
                        "scope": "table",
                        "table": table_name,
                        "column": None,
                        "metric_name": vn,
                        "metric_value": val,
                    }
                )
        if "consistency" in tm:
            for cn, val in tm["consistency"].items():
                rows.append(
                    {
                        "scope": "table",
                        "table": table_name,
                        "column": None,
                        "metric_name": cn,
                        "metric_value": val,
                    }
                )
        if "pk_uniqueness" in tm:
            rows.append(
                {
                    "scope": "table",
                    "table": table_name,
                    "column": None,
                    "metric_name": "pk_uniqueness_pct",
                    "metric_value": tm["pk_uniqueness"]["pk_uniqueness_pct"],
                }
            )
        if "referential_integrity" in tm:
            for rn, val in tm["referential_integrity"].items():
                rows.append(
                    {
                        "scope": "table",
                        "table": table_name,
                        "column": None,
                        "metric_name": rn,
                        "metric_value": val,
                    }
                )

    return pd.DataFrame(rows)

