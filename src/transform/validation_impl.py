from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from config.settings import Settings
from src.core.schemas import CATALOG_ESTADO_CITA, IssueRecord, pk_column_citas, pk_column_pacientes

logger = logging.getLogger(__name__)


def validate_cross_references(
    pacientes_clean: pd.DataFrame,
    citas_clean: pd.DataFrame,
    *,
    reference_date: date,
    settings: Settings,
) -> tuple[pd.DataFrame, pd.DataFrame, list[IssueRecord]]:
    issues: list[IssueRecord] = []

    pk_p = pk_column_pacientes()
    pk_c = pk_column_citas()

    pacientes_ids = set(pacientes_clean[pk_p].dropna().tolist()) if pk_p in pacientes_clean.columns else set()

    orphan_mask = citas_clean["id_paciente"].isna() | ~citas_clean["id_paciente"].isin(pacientes_ids)
    rejected_orphans = citas_clean[orphan_mask].copy()
    if not rejected_orphans.empty:
        rejected_orphans["rejection_reason"] = "Cita huérfana (FK id_paciente no encontrado en pacientes)"
        rejected_orphans["rejection_rule_id"] = "R_ORPHAN_CITA"
        for idx in rejected_orphans.index.tolist():
            issues.append(
                IssueRecord(
                    table="citas_medicas",
                    row_id=rejected_orphans.at[idx, "source_row_id"] if "source_row_id" in rejected_orphans.columns else idx,
                    rule_id="R_ORPHAN_CITA",
                    severity="error",
                    column="id_paciente",
                    original_value=rejected_orphans.at[idx, "id_paciente"],
                    clean_value=None,
                    detail="El id_paciente en la cita no existe en la tabla pacientes.",
                )
            )

    citas_valid = citas_clean[~orphan_mask].copy()

    if "estado_cita" in citas_valid.columns:
        valid_states = set(CATALOG_ESTADO_CITA.values())
        invalid_state_mask = citas_valid["estado_cita"].notna() & ~citas_valid["estado_cita"].isin(valid_states)
        if invalid_state_mask.any():
            for idx in citas_valid.index[invalid_state_mask].tolist():
                issues.append(
                    IssueRecord(
                        table="citas_medicas",
                        row_id=citas_valid.at[idx, "source_row_id"] if "source_row_id" in citas_valid.columns else idx,
                        rule_id="R_ESTADO_CITA_INVALID_REMAINING",
                        severity="warning",
                        column="estado_cita",
                        original_value=citas_valid.at[idx, "estado_cita"],
                        clean_value=None,
                        detail="Estado de cita inválido presente luego de limpieza.",
                    )
                )

    if pk_c in citas_valid.columns and citas_valid[pk_c].duplicated().any():
        issues.append(
            IssueRecord(
                table="citas_medicas",
                row_id="TABLE",
                rule_id="R_DUP_PK_AFTER_CROSS_VALIDATION",
                severity="error",
                column=pk_c,
                original_value=None,
                clean_value=None,
                detail="Se detectaron duplicados de PK luego de las validaciones cruzadas (debería ser imposible).",
            )
        )

    rejected_final = rejected_orphans
    return citas_valid, rejected_final, issues

