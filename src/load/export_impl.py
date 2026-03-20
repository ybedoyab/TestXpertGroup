from __future__ import annotations

"""Dataset exports (CSV/JSON) and metrics reports."""

import logging
from pathlib import Path

import pandas as pd

from src.core.schemas import (
    IssueRecord,
    export_columns_citas_medicas,
    export_columns_pacientes,
)
from src.core.utils import ensure_dir

logger = logging.getLogger(__name__)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False, encoding="utf-8")


def _select_export_columns(df: pd.DataFrame, canonical: list[str]) -> pd.DataFrame:
    """Retorna solo las columnas canónicas presentes en el DataFrame.

    Las columnas de auditoría interna (*_original, source_row_id, edad_limpia)
    no se incluyen en los CSVs limpios; quedan en data_quality_issues.csv.
    """
    cols = [c for c in canonical if c in df.columns]
    return df[cols]


def export_datasets(
    *,
    base_output_dir: Path,
    pacientes_clean: pd.DataFrame,
    citas_clean: pd.DataFrame,
    pacientes_rejected: pd.DataFrame,
    citas_rejected: pd.DataFrame,
    issues: list[IssueRecord],
    before_metrics_df: pd.DataFrame,
    after_metrics_df: pd.DataFrame,
) -> None:
    """Exporta datasets limpios, rechazados, issues y métricas (CSV).

    No escribe quality_summary.json; ese archivo se genera una sola vez
    desde el pipeline runner después de incorporar la información del DWH.
    """
    processed_dir = base_output_dir / "data" / "processed"
    reports_dir = base_output_dir / "data" / "reports"
    ensure_dir(processed_dir)
    ensure_dir(reports_dir)

    _write_csv(
        _select_export_columns(pacientes_clean, export_columns_pacientes()),
        processed_dir / "pacientes_clean.csv",
    )
    _write_csv(
        _select_export_columns(citas_clean, export_columns_citas_medicas()),
        processed_dir / "citas_medicas_clean.csv",
    )

    if pacientes_rejected is not None and not pacientes_rejected.empty:
        _write_csv(pacientes_rejected, processed_dir / "pacientes_rejected.csv")
    else:
        _write_csv(
            pacientes_rejected if pacientes_rejected is not None else pd.DataFrame(),
            processed_dir / "pacientes_rejected.csv",
        )

    if citas_rejected is not None and not citas_rejected.empty:
        _write_csv(citas_rejected, processed_dir / "citas_medicas_rejected.csv")
    else:
        _write_csv(
            citas_rejected if citas_rejected is not None else pd.DataFrame(),
            processed_dir / "citas_medicas_rejected.csv",
        )

    issues_df = pd.DataFrame([i.to_dict() for i in issues])
    _write_csv(issues_df, processed_dir / "data_quality_issues.csv")

    _write_csv(before_metrics_df, reports_dir / "before_quality_metrics.csv")
    _write_csv(after_metrics_df, reports_dir / "after_quality_metrics.csv")

    logger.info(
        "Exports completados: %d pacientes limpios, %d citas limpias, %d issues.",
        len(pacientes_clean),
        len(citas_clean),
        len(issues),
    )
