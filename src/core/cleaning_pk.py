from __future__ import annotations

"""Primary-key duplicate resolution helpers."""

from typing import Any

import pandas as pd

from src.core.schemas import IssueRecord


def build_row_signature(df: pd.DataFrame, *, drop_cols: list[str]) -> pd.Series:
    sig_df = df.drop(columns=drop_cols, errors="ignore").astype("string").fillna("")
    return sig_df.agg(lambda row: "|".join(row.tolist()), axis=1)


def resolve_pk_duplicates(
    df: pd.DataFrame,
    *,
    pk_col: str,
    table_name: str,
    issues: list[IssueRecord],
    allow_missing: bool,
    missing_rule_id: str,
    conflict_rule_id: str,
    exact_rule_id: str,
    source_row_id_col: str = "source_row_id",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if pk_col not in df.columns:
        raise ValueError(f"Missing pk_col='{pk_col}' in table '{table_name}'.")

    working = df.copy()
    rejected_rows = working.iloc[0:0].copy()
    rejected_rows["rejection_reason"] = []
    rejected_rows["rejection_rule_id"] = []

    if allow_missing:
        pk_series = working[pk_col]
        missing_mask = pk_series.isna() | (pk_series.astype(str).str.strip() == "")
        rejected_missing = working[missing_mask].copy()
        if not rejected_missing.empty:
            rejected_missing["rejection_reason"] = "PK primaria faltante"
            rejected_missing["rejection_rule_id"] = missing_rule_id
            for idx in rejected_missing.index.tolist():
                row_id = (
                    rejected_missing.at[idx, source_row_id_col]
                    if source_row_id_col in rejected_missing.columns
                    else idx
                )
                issues.append(
                    IssueRecord(
                        table=table_name,
                        row_id=row_id,
                        rule_id=missing_rule_id,
                        severity="error",
                        column=pk_col,
                        original_value=rejected_missing.at[idx, pk_col],
                        clean_value=None,
                        detail="Registro rechazado por PK primaria faltante.",
                    )
                )
            rejected_rows = pd.concat([rejected_rows, rejected_missing], ignore_index=True)
        working = working[~missing_mask].copy()

    if working.empty:
        return (
            working.drop(columns=[c for c in ["__clean_row_sig__"] if c in working.columns], errors="ignore"),
            rejected_rows,
        )

    working["__clean_row_sig__"] = build_row_signature(working, drop_cols=[source_row_id_col])

    dup_mask = working[pk_col].duplicated(keep=False)
    conflict_pks: list[Any] = []
    if dup_mask.any():
        for pk_val, group in working.loc[dup_mask].groupby(pk_col):
            if group["__clean_row_sig__"].nunique() > 1:
                conflict_pks.append(pk_val)

    if conflict_pks:
        conflict_mask = working[pk_col].isin(conflict_pks)
        rej_conflicts = working[conflict_mask].copy()
        rej_conflicts["rejection_reason"] = "PK duplicada con datos conflictivos"
        rej_conflicts["rejection_rule_id"] = conflict_rule_id
        for idx in rej_conflicts.index.tolist():
            row_id = (
                rej_conflicts.at[idx, source_row_id_col]
                if source_row_id_col in rej_conflicts.columns
                else idx
            )
            issues.append(
                IssueRecord(
                    table=table_name,
                    row_id=row_id,
                    rule_id=conflict_rule_id,
                    severity="error",
                    column=pk_col,
                    original_value=rej_conflicts.at[idx, pk_col],
                    clean_value=None,
                    detail="Registros con la misma PK presentan diferencias; se rechazan.",
                )
            )
        rejected_rows = pd.concat([rejected_rows, rej_conflicts], ignore_index=True)
        working = working[~conflict_mask].copy()

    if working.empty:
        if "__clean_row_sig__" in rejected_rows.columns:
            rejected_rows = rejected_rows.drop(columns=["__clean_row_sig__"], errors="ignore")
        if "__clean_row_sig__" in working.columns:
            working = working.drop(columns=["__clean_row_sig__"], errors="ignore")
        return working, rejected_rows

    exact_dup_mask = working.duplicated(subset=[pk_col], keep="first")
    exact_dups = working[exact_dup_mask]
    if not exact_dups.empty:
        for idx in exact_dups.index.tolist():
            row_id = (
                exact_dups.at[idx, source_row_id_col] if source_row_id_col in exact_dups.columns else idx
            )
            issues.append(
                IssueRecord(
                    table=table_name,
                    row_id=row_id,
                    rule_id=exact_rule_id,
                    severity="info",
                    column=pk_col,
                    original_value=exact_dups.at[idx, pk_col],
                    clean_value=None,
                    detail="Duplicado exacto por PK eliminado (keep first).",
                )
            )
        working = working[~exact_dup_mask].copy()

    if "__clean_row_sig__" in working.columns:
        working = working.drop(columns=["__clean_row_sig__"], errors="ignore")
    if "__clean_row_sig__" in rejected_rows.columns:
        rejected_rows = rejected_rows.drop(columns=["__clean_row_sig__"], errors="ignore")

    return working, rejected_rows

