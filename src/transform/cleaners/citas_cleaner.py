from __future__ import annotations

"""Reglas de limpieza para la tabla `citas_medicas`."""

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

import pandas as pd

from config.settings import Settings
from src.core.audit import add_issue
from src.core.catalog_normalizers import normalize_estado_cita
from src.core.cleaning_pk import resolve_pk_duplicates
from src.core.schemas import IssueRecord, pk_column_citas
from src.core.utils import parse_date_str, try_cast_float


def _norm_optional_text(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    return s or None


def clean_citas_medicas(
    df: pd.DataFrame,
    *,
    reference_date: date,
    settings: Settings,
) -> tuple[pd.DataFrame, pd.DataFrame, list[IssueRecord]]:
    table_name = "citas_medicas"
    pk_col = pk_column_citas()
    issues: list[IssueRecord] = []

    dfw = df.copy()
    if "source_row_id" not in dfw.columns:
        dfw = dfw.reset_index(drop=False).rename(columns={"index": "source_row_id"})

    if pk_col not in dfw.columns:
        raise ValueError("Missing id_cita column in citas_medicas.")

    missing_pk_mask = dfw[pk_col].isna() | (dfw[pk_col].astype(str).str.strip() == "")
    rejected_rows = dfw[missing_pk_mask].copy()
    if not rejected_rows.empty:
        rejected_rows["rejection_reason"] = "PK primaria faltante"
        rejected_rows["rejection_rule_id"] = "R_PK_MISSING"
        for idx in rejected_rows.index.tolist():
            add_issue(
                issues,
                table=table_name,
                row_id=dfw.at[idx, "source_row_id"],
                rule_id="R_PK_MISSING",
                severity="error",
                column=pk_col,
                original_value=dfw.at[idx, pk_col],
                clean_value=None,
                detail="Registro rechazado por PK primaria faltante.",
            )
    dfw_valid_pk = dfw[~missing_pk_mask].copy()

    dfw_valid_pk["fecha_cita_original"] = dfw_valid_pk.get("fecha_cita")
    parsed = dfw_valid_pk["fecha_cita_original"].apply(parse_date_str)
    mask_invalid = dfw_valid_pk["fecha_cita_original"].notna() & parsed.isna()
    for idx in dfw_valid_pk.index[mask_invalid].tolist():
        add_issue(
            issues,
            table=table_name,
            row_id=dfw_valid_pk.at[idx, "source_row_id"],
            rule_id="R_FECHA_CITA_INVALID",
            severity="warning",
            column="fecha_cita",
            original_value=dfw_valid_pk.at[idx, "fecha_cita_original"],
            clean_value=None,
            detail="Fecha de cita no parseable (esperado YYYY-MM-DD).",
        )
    dfw_valid_pk["fecha_cita"] = parsed.apply(lambda d: d.isoformat() if d is not None else None)

    if "estado_cita" in dfw_valid_pk.columns:
        dfw_valid_pk["estado_cita_original"] = dfw_valid_pk["estado_cita"]
        status_norm = dfw_valid_pk["estado_cita"].apply(normalize_estado_cita)
        mask_invalid_status = dfw_valid_pk["estado_cita_original"].notna() & status_norm.isna()
        for idx in dfw_valid_pk.index[mask_invalid_status].tolist():
            add_issue(
                issues,
                table=table_name,
                row_id=dfw_valid_pk.at[idx, "source_row_id"],
                rule_id="R_ESTADO_CITA_UNRECOGNIZED",
                severity="warning",
                column="estado_cita",
                original_value=dfw_valid_pk.at[idx, "estado_cita_original"],
                clean_value=None,
                detail="Estado de cita no mapeado al catálogo.",
            )
        dfw_valid_pk["estado_cita"] = status_norm
    else:
        dfw_valid_pk["estado_cita"] = None

    if "costo" in dfw_valid_pk.columns:
        dfw_valid_pk["costo_original"] = dfw_valid_pk["costo"]
        costo_cast = dfw_valid_pk["costo"].apply(try_cast_float)
        mask_non_numeric = dfw_valid_pk["costo_original"].notna() & costo_cast.isna()
        if mask_non_numeric.any():
            rej = dfw_valid_pk[mask_non_numeric].copy()
            rej["rejection_reason"] = "Costo no numérico"
            rej["rejection_rule_id"] = "R_COST_NON_NUMERIC"
            for idx in rej.index.tolist():
                add_issue(
                    issues,
                    table=table_name,
                    row_id=dfw_valid_pk.at[idx, "source_row_id"],
                    rule_id="R_COST_NON_NUMERIC",
                    severity="error",
                    column="costo",
                    original_value=dfw_valid_pk.at[idx, "costo_original"],
                    clean_value=None,
                    detail="Costo no convertible a número; registro rechazado.",
                )
            rejected_rows = pd.concat([rejected_rows, rej], ignore_index=True) if not rejected_rows.empty else rej
            dfw_valid_pk = dfw_valid_pk[~mask_non_numeric].copy()
            costo_cast = dfw_valid_pk["costo"].apply(try_cast_float)

        mask_negative = costo_cast.notna() & (costo_cast < 0)
        if mask_negative.any():
            rej = dfw_valid_pk[mask_negative].copy()
            rej["rejection_reason"] = "Costo negativo (valor inválido)"
            rej["rejection_rule_id"] = "R_COST_NEGATIVE"
            for idx in rej.index.tolist():
                add_issue(
                    issues,
                    table=table_name,
                    row_id=dfw_valid_pk.at[idx, "source_row_id"],
                    rule_id="R_COST_NEGATIVE",
                    severity="error",
                    column="costo",
                    original_value=dfw_valid_pk.at[idx, "costo_original"],
                    clean_value=None,
                    detail="Costo negativo; registro rechazado.",
                )
            rejected_rows = pd.concat([rejected_rows, rej], ignore_index=True) if not rejected_rows.empty else rej
            dfw_valid_pk = dfw_valid_pk[~mask_negative].copy()

        dfw_valid_pk["costo"] = dfw_valid_pk["costo"].apply(try_cast_float)
    else:
        dfw_valid_pk["costo"] = None

    for col in ["medico", "especialidad", "ciudad"]:
        if col in dfw_valid_pk.columns:
            dfw_valid_pk[col] = dfw_valid_pk[col].apply(_norm_optional_text)

    df_valid_pk, rejected_conflicts = resolve_pk_duplicates(
        dfw_valid_pk,
        pk_col=pk_col,
        table_name=table_name,
        issues=issues,
        allow_missing=False,
        missing_rule_id="R_PK_MISSING",
        conflict_rule_id="R_DUP_PK_CONFLICT",
        exact_rule_id="R_DUP_PK_EXACT",
    )

    if not rejected_conflicts.empty:
        rejected_rows = pd.concat([rejected_rows, rejected_conflicts], ignore_index=True)

    if rejected_rows.empty:
        rejected_rows = dfw.iloc[0:0].copy()
        rejected_rows["rejection_reason"] = []
        rejected_rows["rejection_rule_id"] = []

    logger.info(
        "Citas limpias: %d válidas, %d rechazadas, %d issues generados.",
        len(df_valid_pk),
        len(rejected_rows),
        len(issues),
    )
    return df_valid_pk, rejected_rows, issues

