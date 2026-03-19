from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from typing import Any

from src.pipeline.pipeline_runner import run_pipeline


def test_end_to_end_smoke(tmp_path: Path, minimal_dataset: dict[str, Any]) -> None:
    input_path = tmp_path / "dataset_hospital 2.json"
    input_path.write_text(json.dumps(minimal_dataset), encoding="utf-8")

    run_pipeline(
        input_path=input_path,
        base_output_dir=tmp_path,
        reference_date=date(2026, 3, 18),
        limit=0,
    )

    # Verificar que todos los archivos esperados existen
    assert (tmp_path / "data" / "processed" / "pacientes_clean.csv").exists()
    assert (tmp_path / "data" / "processed" / "citas_medicas_clean.csv").exists()
    assert (tmp_path / "data" / "processed" / "pacientes_rejected.csv").exists()
    assert (tmp_path / "data" / "processed" / "citas_medicas_rejected.csv").exists()
    assert (tmp_path / "data" / "processed" / "data_quality_issues.csv").exists()
    assert (tmp_path / "data" / "reports" / "before_quality_metrics.csv").exists()
    assert (tmp_path / "data" / "reports" / "after_quality_metrics.csv").exists()
    assert (tmp_path / "data" / "reports" / "quality_summary.json").exists()
    assert (tmp_path / "data" / "reports" / "dwh.sqlite").exists()
    assert (tmp_path / "docs" / "technical_report.md").exists()


def test_end_to_end_csv_content(tmp_path: Path, minimal_dataset: dict[str, Any]) -> None:
    """Verifica que los CSV generados tienen contenido y headers correctos."""
    input_path = tmp_path / "dataset_hospital 2.json"
    input_path.write_text(json.dumps(minimal_dataset), encoding="utf-8")

    run_pipeline(
        input_path=input_path,
        base_output_dir=tmp_path,
        reference_date=date(2026, 3, 18),
        limit=0,
    )

    # Verificar contenido de pacientes_clean.csv
    pac_path = tmp_path / "data" / "processed" / "pacientes_clean.csv"
    with open(pac_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["id_paciente"] == "1"
    assert rows[0]["nombre"] == "A"

    # Verificar contenido de citas_medicas_clean.csv
    citas_path = tmp_path / "data" / "processed" / "citas_medicas_clean.csv"
    with open(citas_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["id_cita"] == "uuid-1"


def test_end_to_end_quality_summary_content(
    tmp_path: Path, minimal_dataset: dict[str, Any]
) -> None:
    """Verifica que quality_summary.json se escribe una sola vez y contiene DWH."""
    input_path = tmp_path / "dataset_hospital 2.json"
    input_path.write_text(json.dumps(minimal_dataset), encoding="utf-8")

    run_pipeline(
        input_path=input_path,
        base_output_dir=tmp_path,
        reference_date=date(2026, 3, 18),
        limit=0,
    )

    summary_path = tmp_path / "data" / "reports" / "quality_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert "global_before" in summary
    assert "global_after" in summary
    assert "dwh" in summary
    assert "counts" in summary["dwh"]
    assert "rejected_counts" in summary
    assert "issue_stats" in summary


def test_end_to_end_idempotent(tmp_path: Path, minimal_dataset: dict[str, Any]) -> None:
    """Verifica que ejecutar el pipeline dos veces es idempotente."""
    input_path = tmp_path / "dataset_hospital 2.json"
    input_path.write_text(json.dumps(minimal_dataset), encoding="utf-8")

    run_pipeline(
        input_path=input_path,
        base_output_dir=tmp_path,
        reference_date=date(2026, 3, 18),
        limit=0,
    )
    summary_1 = json.loads(
        (tmp_path / "data" / "reports" / "quality_summary.json").read_text(encoding="utf-8")
    )

    run_pipeline(
        input_path=input_path,
        base_output_dir=tmp_path,
        reference_date=date(2026, 3, 18),
        limit=0,
    )
    summary_2 = json.loads(
        (tmp_path / "data" / "reports" / "quality_summary.json").read_text(encoding="utf-8")
    )

    assert summary_1 == summary_2
