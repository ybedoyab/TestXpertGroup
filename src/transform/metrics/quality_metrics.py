from __future__ import annotations

"""Before/after quality metrics computation."""

from typing import Any

import numpy as np
import pandas as pd

from config.settings import Settings
from src.core.schemas import expected_columns_citas_medicas, expected_columns_pacientes
from src.transform.metrics.base import flatten_metrics
from src.transform.metrics.citas_metrics import compute_metrics_for_citas
from src.transform.metrics.pacientes_metrics import compute_metrics_for_pacientes


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

    exp_p = expected_columns_pacientes()
    exp_c = expected_columns_citas_medicas()
    total_cells = 0
    non_null_cells = 0
    for col in exp_p:
        if col in pacientes_before.columns:
            total_cells += len(pacientes_before)
            non_null_cells += int(pacientes_before[col].notna().sum())
    for col in exp_c:
        if col in citas_before.columns:
            total_cells += len(citas_before)
            non_null_cells += int(citas_before[col].notna().sum())

    completeness_global_before = non_null_cells / total_cells if total_cells else float("nan")
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

    total_cells = 0
    non_null_cells = 0
    for col in exp_p:
        if col in pacientes_after.columns:
            total_cells += len(pacientes_after)
            non_null_cells += int(pacientes_after[col].notna().sum())
    for col in exp_c:
        if col in citas_after.columns:
            total_cells += len(citas_after)
            non_null_cells += int(citas_after[col].notna().sum())

    completeness_global_after = non_null_cells / total_cells if total_cells else float("nan")
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

