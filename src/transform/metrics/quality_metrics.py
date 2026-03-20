from __future__ import annotations

"""Before/after quality metrics computation."""

from typing import Any

import numpy as np
import pandas as pd

from config.settings import Settings
from src.core.schemas import EXPECTED_COLUMNS_CITAS_MEDICAS, EXPECTED_COLUMNS_PACIENTES
from src.transform.metrics.base import flatten_metrics
from src.transform.metrics.citas_metrics import compute_metrics_for_citas
from src.transform.metrics.pacientes_metrics import compute_metrics_for_pacientes


def _global_completeness(
    df_p: pd.DataFrame, df_c: pd.DataFrame, exp_p: list[str], exp_c: list[str]
) -> float:
    total_cells = 0
    non_null_cells = 0
    for col in exp_p:
        if col in df_p.columns:
            total_cells += len(df_p)
            non_null_cells += int(df_p[col].notna().sum())
    for col in exp_c:
        if col in df_c.columns:
            total_cells += len(df_c)
            non_null_cells += int(df_c[col].notna().sum())
    return non_null_cells / total_cells if total_cells else float("nan")


def compute_quality_metrics(
    *,
    before: dict[str, pd.DataFrame],
    after: dict[str, pd.DataFrame],
    reference_date: Any,
    settings: Settings,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    pacientes_before = before["pacientes"]
    citas_before = before["citas_medicas"]
    pacientes_ids_before = (
        set(pacientes_before["id_paciente"].dropna().tolist()) if "id_paciente" in pacientes_before.columns else set()
    )

    metrics_p_before = compute_metrics_for_pacientes(
        pacientes_before, reference_date=reference_date, settings=settings
    )
    metrics_c_before = compute_metrics_for_citas(
        citas_before, pacientes_ids=pacientes_ids_before, settings=settings
    )
    before_dataset_metrics = {"pacientes": metrics_p_before, "citas_medicas": metrics_c_before}

    exp_p = EXPECTED_COLUMNS_PACIENTES
    exp_c = EXPECTED_COLUMNS_CITAS_MEDICAS
    completeness_global_before = _global_completeness(pacientes_before, citas_before, exp_p, exp_c)
    global_before = {
        "row_count_total": int(len(pacientes_before) + len(citas_before)),
        "completeness_global_pct": completeness_global_before,
        "referential_integrity_fk_valid_pct_among_non_null": metrics_c_before["referential_integrity"][
            "fk_valid_pct_among_non_null"
        ],
    }
    before_metrics_df = flatten_metrics(dataset_metrics=before_dataset_metrics, global_metrics=global_before)

    pacientes_after = after["pacientes"]
    citas_after = after["citas_medicas"]
    pacientes_ids_after = (
        set(pacientes_after["id_paciente"].dropna().tolist()) if "id_paciente" in pacientes_after.columns else set()
    )
    metrics_p_after = compute_metrics_for_pacientes(
        pacientes_after, reference_date=reference_date, settings=settings
    )
    metrics_c_after = compute_metrics_for_citas(
        citas_after, pacientes_ids=pacientes_ids_after, settings=settings
    )
    after_dataset_metrics = {"pacientes": metrics_p_after, "citas_medicas": metrics_c_after}

    completeness_global_after = _global_completeness(pacientes_after, citas_after, exp_p, exp_c)
    global_after = {
        "row_count_total": int(len(pacientes_after) + len(citas_after)),
        "completeness_global_pct": completeness_global_after,
        "referential_integrity_fk_valid_pct_among_non_null": metrics_c_after["referential_integrity"][
            "fk_valid_pct_among_non_null"
        ],
    }
    after_metrics_df = flatten_metrics(dataset_metrics=after_dataset_metrics, global_metrics=global_after)

    summary = {
        "global_before": global_before,
        "global_after": global_after,
        "improvement": {
            "completeness_global_delta": completeness_global_after - completeness_global_before
            if np.isfinite(completeness_global_after) and np.isfinite(completeness_global_before)
            else None,
            "referential_integrity_fk_valid_delta": metrics_c_after["referential_integrity"][
                "fk_valid_pct_among_non_null"
            ]
            - metrics_c_before["referential_integrity"]["fk_valid_pct_among_non_null"]
            if np.isfinite(
                metrics_c_after["referential_integrity"]["fk_valid_pct_among_non_null"]
            )
            and np.isfinite(metrics_c_before["referential_integrity"]["fk_valid_pct_among_non_null"])
            else None,
        },
    }

    return before_metrics_df, after_metrics_df, summary

