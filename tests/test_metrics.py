from __future__ import annotations

from datetime import date

import pandas as pd

from config.settings import Settings
from src.transform.metrics.quality_metrics import compute_quality_metrics


def _make_before_after(
    *, reference_date: date, settings: Settings
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    before = {
        "pacientes": pd.DataFrame(
            [
                {
                    "id_paciente": 1,
                    "nombre": "A",
                    "fecha_nacimiento": "2000-01-01",
                    "edad": 26,
                    "sexo": "Male",
                    "email": "a@a.com",
                    "telefono": "1234567890",
                    "ciudad": "Bogotá",
                }
            ]
        ),
        "citas_medicas": pd.DataFrame(
            [
                {
                    "id_cita": "uuid-1",
                    "id_paciente": 1,
                    "fecha_cita": "2022-02-14",
                    "especialidad": "Cardiología",
                    "medico": "Dr. X",
                    "costo": 100,
                    "estado_cita": "Completada",
                },
                {
                    "id_cita": "uuid-2",
                    "id_paciente": 999,
                    "fecha_cita": "2022-02-15",
                    "especialidad": "Cardiología",
                    "medico": "Dr. X",
                    "costo": 120,
                    "estado_cita": "Cancelada",
                },
            ]
        ),
    }

    after = {
        "pacientes": before["pacientes"],
        "citas_medicas": pd.DataFrame([before["citas_medicas"].iloc[0].to_dict()]),
    }
    return before, after


def test_referential_integrity_improves_after_cleaning(
    reference_date: date, settings: Settings
) -> None:
    before, after = _make_before_after(reference_date=reference_date, settings=settings)

    _before_metrics_df, _after_metrics_df, summary = compute_quality_metrics(
        before=before, after=after, reference_date=reference_date, settings=settings
    )

    assert (
        summary["global_after"]["referential_integrity_fk_valid_pct_among_non_null"] == 1.0
    )


def test_metrics_return_dataframes(reference_date: date, settings: Settings) -> None:
    before, after = _make_before_after(reference_date=reference_date, settings=settings)

    before_metrics_df, after_metrics_df, summary = compute_quality_metrics(
        before=before, after=after, reference_date=reference_date, settings=settings
    )

    assert isinstance(before_metrics_df, pd.DataFrame)
    assert isinstance(after_metrics_df, pd.DataFrame)
    assert not before_metrics_df.empty
    assert not after_metrics_df.empty
    assert "metric_name" in before_metrics_df.columns
    assert "metric_value" in before_metrics_df.columns


def test_completeness_delta(reference_date: date, settings: Settings) -> None:
    before, after = _make_before_after(reference_date=reference_date, settings=settings)

    _before_metrics_df, _after_metrics_df, summary = compute_quality_metrics(
        before=before, after=after, reference_date=reference_date, settings=settings
    )

    delta = summary["improvement"]["completeness_global_delta"]
    assert delta is not None
