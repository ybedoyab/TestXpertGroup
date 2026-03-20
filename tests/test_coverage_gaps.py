from __future__ import annotations

"""Tests targeting specific coverage gaps across the codebase."""

import json
import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

from config.settings import Settings

REFERENCE_DATE = date(2026, 3, 18)


# ── config.config ────────────────────────────────────────────────────────────


def test_config_defaults_importable() -> None:
    from config.config import DEFAULTS, PipelineDefaults

    assert DEFAULTS.limit == 0
    assert isinstance(DEFAULTS.reference_date, date)
    assert isinstance(DEFAULTS, PipelineDefaults)


# ── logging_config ───────────────────────────────────────────────────────────


def test_configure_logging_adds_handler() -> None:
    from src.core.logging_config import configure_logging

    logger_name = "test_configure_logging_gap_unique"
    test_logger = logging.getLogger(logger_name)
    test_logger.handlers.clear()
    configure_logging(logger_name=logger_name)
    assert any(isinstance(h, logging.StreamHandler) for h in test_logger.handlers)


def test_configure_logging_idempotent() -> None:
    from src.core.logging_config import configure_logging

    logger_name = "test_configure_logging_idem_unique"
    test_logger = logging.getLogger(logger_name)
    test_logger.handlers.clear()
    configure_logging(logger_name=logger_name)
    count = len(test_logger.handlers)
    configure_logging(logger_name=logger_name)
    assert len(test_logger.handlers) == count


# ── schemas ──────────────────────────────────────────────────────────────────


def test_profiling_report_to_dict() -> None:
    from src.core.schemas import ProfilingReport

    pr = ProfilingReport(
        table="pacientes",
        row_count=10,
        duplicate_counts={"id_paciente": 0},
        missing_counts={"email": 3},
        format_anomalies={"sexo_invalid": 1},
        categorical_cardinality={"sexo": {"M": 5, "F": 5}},
    )
    d = pr.to_dict()
    assert d["table"] == "pacientes"
    assert d["row_count"] == 10
    assert "missing_counts" in d


def test_quality_metrics_to_dict() -> None:
    from src.core.schemas import QualityMetrics

    qm = QualityMetrics(
        global_metrics={"completeness_global_pct": 0.85},
        table_metrics={"pacientes": {"row_count": 50}},
    )
    d = qm.to_dict()
    assert d["global_metrics"]["completeness_global_pct"] == 0.85
    assert "table_metrics" in d


# ── utils ────────────────────────────────────────────────────────────────────


def test_is_valid_email_none() -> None:
    from src.core.utils import is_valid_email

    assert is_valid_email(None) is False


def test_is_valid_email_empty_string() -> None:
    from src.core.utils import is_valid_email

    assert is_valid_email("") is False


def test_normalize_email_empty_string() -> None:
    from src.core.utils import normalize_email

    assert normalize_email("   ") is None


def test_normalize_phone_digits_empty_string() -> None:
    from src.core.utils import normalize_phone_digits

    assert normalize_phone_digits("   ") is None


def test_parse_date_str_swap_produces_invalid_date() -> None:
    from src.core.utils import parse_date_str

    assert parse_date_str("2023-31-02") is None


def test_compute_age_none_birth_date() -> None:
    from src.core.utils import compute_age

    assert compute_age(date(2026, 1, 1), None) is None


def test_coalesce_str_none() -> None:
    from src.core.utils import coalesce_str

    assert coalesce_str(None) is None


def test_coalesce_str_empty_string() -> None:
    from src.core.utils import coalesce_str

    assert coalesce_str("   ") is None


def test_coalesce_str_value() -> None:
    from src.core.utils import coalesce_str

    assert coalesce_str("hello") == "hello"


def test_try_cast_float_none() -> None:
    from src.core.utils import try_cast_float

    assert try_cast_float(None) is None


def test_try_cast_float_empty_string() -> None:
    from src.core.utils import try_cast_float

    assert try_cast_float("   ") is None


# ── cleaning_pk ──────────────────────────────────────────────────────────────


def test_resolve_pk_duplicates_missing_pk_col_raises() -> None:
    from src.core.cleaning_pk import resolve_pk_duplicates
    from src.core.schemas import IssueRecord

    df = pd.DataFrame([{"nombre": "A", "source_row_id": 0}])
    issues: list[IssueRecord] = []
    with pytest.raises(ValueError, match="Missing pk_col"):
        resolve_pk_duplicates(
            df,
            pk_col="id_paciente",
            table_name="pacientes",
            issues=issues,
            allow_missing=True,
            missing_rule_id="R_PK_MISSING",
            conflict_rule_id="R_DUP_PK_CONFLICT",
            exact_rule_id="R_DUP_PK_EXACT",
        )


def test_resolve_pk_duplicates_drops_sig_col_after_conflict() -> None:
    """All rows conflict → working empty → early return (line 111). __clean_row_sig__ cleaned there."""
    from src.core.cleaning_pk import resolve_pk_duplicates
    from src.core.schemas import IssueRecord

    df = pd.DataFrame(
        [
            {"id_p": 1, "nombre": "A", "source_row_id": 0},
            {"id_p": 1, "nombre": "B", "source_row_id": 1},
        ]
    )
    issues: list[IssueRecord] = []
    clean, rejected = resolve_pk_duplicates(
        df,
        pk_col="id_p",
        table_name="t",
        issues=issues,
        allow_missing=False,
        missing_rule_id="R_PK_MISSING",
        conflict_rule_id="R_DUP_PK_CONFLICT",
        exact_rule_id="R_DUP_PK_EXACT",
    )
    assert len(clean) == 0
    assert len(rejected) == 2
    assert "__clean_row_sig__" not in clean.columns
    assert "__clean_row_sig__" not in rejected.columns


def test_resolve_pk_duplicates_drops_sig_col_partial_conflict() -> None:
    """Partial conflict: some rows rejected (rejected_rows gets __clean_row_sig__),
    one row survives (working not empty) → lines 134-137 execute instead of early return."""
    from src.core.cleaning_pk import resolve_pk_duplicates
    from src.core.schemas import IssueRecord

    df = pd.DataFrame(
        [
            {"id_p": 1, "nombre": "A", "source_row_id": 0},
            {"id_p": 1, "nombre": "B", "source_row_id": 1},  # conflict with row above
            {"id_p": 2, "nombre": "C", "source_row_id": 2},  # unique, survives
        ]
    )
    issues: list[IssueRecord] = []
    clean, rejected = resolve_pk_duplicates(
        df,
        pk_col="id_p",
        table_name="t",
        issues=issues,
        allow_missing=False,
        missing_rule_id="R_PK_MISSING",
        conflict_rule_id="R_DUP_PK_CONFLICT",
        exact_rule_id="R_DUP_PK_EXACT",
    )
    assert len(clean) == 1
    assert clean.iloc[0]["nombre"] == "C"
    assert len(rejected) == 2
    assert "__clean_row_sig__" not in clean.columns
    assert "__clean_row_sig__" not in rejected.columns


# ── ingestion ────────────────────────────────────────────────────────────────


def test_to_dataframe_non_list_raises() -> None:
    from src.extract.ingestion_impl import _to_dataframe

    with pytest.raises(ValueError, match="must be a JSON array"):
        _to_dataframe("not-a-list", table_name="pacientes")  # type: ignore[arg-type]


def test_to_dataframe_empty_list_logs_warning() -> None:
    from src.extract.ingestion_impl import _to_dataframe

    df = _to_dataframe([], table_name="pacientes")
    assert df.empty


def test_validate_minimal_schema_missing_table() -> None:
    from src.extract.ingestion_impl import validate_minimal_schema

    pacientes = pd.DataFrame(
        {"id_paciente": [1], "nombre": ["A"], "fecha_nacimiento": ["2000-01-01"], "sexo": ["M"]}
    )
    with pytest.raises(ValueError, match="Missing table"):
        validate_minimal_schema({"pacientes": pacientes})


def test_validate_minimal_schema_missing_columns() -> None:
    from src.extract.ingestion_impl import validate_minimal_schema

    pacientes = pd.DataFrame({"id_paciente": [1]})
    citas = pd.DataFrame(
        {
            "id_cita": ["c1"],
            "id_paciente": [1],
            "fecha_cita": ["2022-01-01"],
            "estado_cita": ["Completada"],
            "costo": [100],
        }
    )
    with pytest.raises(ValueError, match="missing columns"):
        validate_minimal_schema({"pacientes": pacientes, "citas_medicas": citas})


def test_load_dataset_file_not_found() -> None:
    from src.extract.ingestion_impl import load_dataset

    with pytest.raises(FileNotFoundError):
        load_dataset(Path("/nonexistent/path/dataset.json"))


def test_load_dataset_non_dict_json(tmp_path: Path) -> None:
    from src.extract.ingestion_impl import load_dataset

    p = tmp_path / "bad.json"
    p.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with pytest.raises(ValueError, match="Top-level JSON"):
        load_dataset(p)


def test_load_dataset_missing_citas_key(tmp_path: Path) -> None:
    from src.extract.ingestion_impl import load_dataset

    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"pacientes": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="Missing 'citas_medicas'"):
        load_dataset(p)


def test_load_dataset_with_limit(tmp_path: Path, minimal_dataset: dict[str, Any]) -> None:
    from src.extract.ingestion_impl import load_dataset

    data = {
        "pacientes": minimal_dataset["pacientes"] * 5,
        "citas_medicas": minimal_dataset["citas_medicas"] * 5,
    }
    p = tmp_path / "data.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    df_dict = load_dataset(p, limit=2)
    assert len(df_dict["pacientes"]) == 2
    assert len(df_dict["citas_medicas"]) == 2


# ── pipeline_runner ──────────────────────────────────────────────────────────


def test_parse_reference_date_invalid() -> None:
    from src.pipeline.pipeline_runner import parse_reference_date

    with pytest.raises(ValueError, match="Invalid --reference-date"):
        parse_reference_date("not-a-date")


def test_build_issue_stats_severity_escalation() -> None:
    from src.core.schemas import IssueRecord
    from src.pipeline.pipeline_runner import build_issue_stats

    issues = [
        IssueRecord(
            table="t",
            row_id=0,
            rule_id="R_TEST",
            severity="info",
            column=None,
            original_value=None,
            clean_value=None,
            detail="",
        ),
        IssueRecord(
            table="t",
            row_id=1,
            rule_id="R_TEST",
            severity="warning",
            column=None,
            original_value=None,
            clean_value=None,
            detail="",
        ),
        IssueRecord(
            table="t",
            row_id=2,
            rule_id="R_TEST",
            severity="error",
            column=None,
            original_value=None,
            clean_value=None,
            detail="",
        ),
    ]
    stats = build_issue_stats(issues)
    assert stats["severity_by_rule"]["R_TEST"] == "error"
    assert stats["by_rule_id"]["R_TEST"] == 3


# ── dwh keys ─────────────────────────────────────────────────────────────────


def test_date_key_from_iso_empty_string() -> None:
    from src.load.dwh.keys import date_key_from_iso

    assert date_key_from_iso("") == 0


def test_date_key_from_iso_invalid_string() -> None:
    from src.load.dwh.keys import date_key_from_iso

    assert date_key_from_iso("not-a-date") == 0


# ── loader (ValueError fallback branch) ──────────────────────────────────────


def test_dwh_loader_absolute_path_fallback(tmp_path: Path) -> None:
    from src.load.dwh.loader import load_to_sqlite

    pacientes = pd.DataFrame(
        [{"id_paciente": 1, "nombre": "A", "sexo": "M", "ciudad": "Bogotá"}]
    )
    citas = pd.DataFrame(
        [
            {
                "id_cita": "c1",
                "id_paciente": 1,
                "fecha_cita": "2022-01-01",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100.0,
                "estado_cita": "Completada",
            }
        ]
    )

    original_relative_to = Path.relative_to

    def relative_to_raiser(self: Path, *args: Any) -> Path:
        raise ValueError("simulated non-relative path")

    with patch.object(Path, "relative_to", relative_to_raiser):
        result = load_to_sqlite(
            base_output_dir=tmp_path, pacientes_clean=pacientes, citas_clean=citas
        )

    assert result["db_path"].endswith("dwh.sqlite")


# ── export_impl ───────────────────────────────────────────────────────────────


def test_export_datasets_with_nonempty_rejected(tmp_path: Path) -> None:
    from src.core.schemas import IssueRecord
    from src.load.export_impl import export_datasets

    pac = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2000-01-01",
                "edad": 26,
                "edad_derivada": 26,
                "edad_inconsistente": False,
                "sexo": "M",
                "email": "a@a.com",
                "telefono": "1234567890",
                "ciudad": "Bogotá",
            }
        ]
    )
    citas = pd.DataFrame(
        [
            {
                "id_cita": "c1",
                "id_paciente": 1,
                "fecha_cita": "2022-01-01",
                "especialidad": "X",
                "medico": "D",
                "costo": 100.0,
                "estado_cita": "Completada",
            }
        ]
    )
    pac_rejected = pd.DataFrame([{"id_paciente": None, "nombre": "Bad", "rejection_reason": "no PK"}])
    citas_rejected = pd.DataFrame([{"id_cita": None, "id_paciente": 1, "rejection_reason": "no PK"}])
    before_m = pd.DataFrame([{"metric_name": "x", "metric_value": 1.0}])
    after_m = pd.DataFrame([{"metric_name": "x", "metric_value": 1.0}])

    export_datasets(
        base_output_dir=tmp_path,
        pacientes_clean=pac,
        citas_clean=citas,
        pacientes_rejected=pac_rejected,
        citas_rejected=citas_rejected,
        issues=[],
        before_metrics_df=before_m,
        after_metrics_df=after_m,
    )

    assert (tmp_path / "data" / "processed" / "pacientes_rejected.csv").exists()
    assert (tmp_path / "data" / "processed" / "citas_medicas_rejected.csv").exists()


# ── metrics base ─────────────────────────────────────────────────────────────


def test_pk_uniqueness_empty_df() -> None:
    from src.transform.metrics.base import pk_uniqueness

    df = pd.DataFrame({"id": pd.Series([], dtype=int)})
    result = pk_uniqueness(df, "id")
    assert result["pk_uniqueness_pct"] is None


def test_pk_uniqueness_missing_pk_col() -> None:
    from src.transform.metrics.base import pk_uniqueness

    df = pd.DataFrame({"x": [1, 2]})
    result = pk_uniqueness(df, "nonexistent")
    assert result["pk_uniqueness_pct"] is None


def test_validity_pct_non_null_empty_series() -> None:
    from src.transform.metrics.base import validity_pct_non_null

    result = validity_pct_non_null(pd.Series([], dtype=bool))
    assert result != result


# ── citas metrics: no id_paciente ────────────────────────────────────────────


def test_citas_metrics_no_id_paciente_column() -> None:
    from src.transform.metrics.citas_metrics import compute_metrics_for_citas

    df = pd.DataFrame(
        {
            "id_cita": ["c1"],
            "fecha_cita": ["2022-01-01"],
            "costo": [100],
            "estado_cita": ["Completada"],
        }
    )
    settings = Settings(reference_date=REFERENCE_DATE)
    result = compute_metrics_for_citas(df, pacientes_ids={1}, settings=settings)
    ri = result["referential_integrity"]["fk_valid_pct_among_non_null"]
    assert ri != ri


# ── citas_cleaner edge cases ──────────────────────────────────────────────────


def test_norm_optional_text_none() -> None:
    from src.transform.cleaners.citas_cleaner import _norm_optional_text

    assert _norm_optional_text(None) is None


def test_norm_optional_text_float_nan() -> None:
    from src.transform.cleaners.citas_cleaner import _norm_optional_text

    assert _norm_optional_text(float("nan")) is None


def test_norm_optional_text_pandas_na() -> None:
    from src.transform.cleaners.citas_cleaner import _norm_optional_text

    assert _norm_optional_text(pd.NA) is None


def test_norm_optional_text_exception_in_isna() -> None:
    from src.transform.cleaners.citas_cleaner import _norm_optional_text

    class WeirdObject:
        def __repr__(self) -> str:
            return "weird"

    with patch("pandas.isna", side_effect=Exception("isna failed")):
        result = _norm_optional_text(WeirdObject())

    assert result == "weird"


def test_clean_citas_no_source_row_id(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_cita": "c1",
                "id_paciente": 1,
                "fecha_cita": "2022-01-01",
                "especialidad": "X",
                "medico": "D",
                "costo": 100,
                "estado_cita": "Completada",
            }
        ]
    )
    from src.transform.cleaners.citas_cleaner import clean_citas_medicas

    clean, rejected, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean) == 1


def test_clean_citas_missing_id_cita_column_raises(
    reference_date: date, settings: Settings
) -> None:
    from src.transform.cleaners.citas_cleaner import clean_citas_medicas

    df = pd.DataFrame([{"id_paciente": 1, "fecha_cita": "2022-01-01", "source_row_id": 0}])
    with pytest.raises(ValueError, match="Missing id_cita"):
        clean_citas_medicas(df, reference_date=reference_date, settings=settings)


def test_clean_citas_invalid_fecha_cita_flagged(
    reference_date: date, settings: Settings
) -> None:
    from src.transform.cleaners.citas_cleaner import clean_citas_medicas

    df = pd.DataFrame(
        [
            {
                "id_cita": "c1",
                "id_paciente": 1,
                "fecha_cita": "not-a-date",
                "especialidad": "X",
                "medico": "D",
                "costo": 100,
                "estado_cita": "Completada",
            }
        ]
    ).reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean, rejected, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert any(i.rule_id == "R_FECHA_CITA_INVALID" for i in issues)


def test_clean_citas_no_estado_cita_column(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.citas_cleaner import clean_citas_medicas

    df = pd.DataFrame(
        [
            {
                "id_cita": "c1",
                "id_paciente": 1,
                "fecha_cita": "2022-01-01",
                "especialidad": "X",
                "medico": "D",
                "costo": 100,
            }
        ]
    ).reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean, rejected, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert clean.iloc[0]["estado_cita"] is None


def test_clean_citas_no_costo_column(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.citas_cleaner import clean_citas_medicas

    df = pd.DataFrame(
        [
            {
                "id_cita": "c1",
                "id_paciente": 1,
                "fecha_cita": "2022-01-01",
                "especialidad": "X",
                "medico": "D",
                "estado_cita": "Completada",
            }
        ]
    ).reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean, rejected, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert clean.iloc[0]["costo"] is None


def test_clean_citas_pk_conflict_rejected(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.citas_cleaner import clean_citas_medicas

    df = pd.DataFrame(
        [
            {
                "id_cita": "c1",
                "id_paciente": 1,
                "fecha_cita": "2022-01-01",
                "costo": 100,
                "estado_cita": "Completada",
            },
            {
                "id_cita": "c1",
                "id_paciente": 2,
                "fecha_cita": "2022-06-01",
                "costo": 200,
                "estado_cita": "Cancelada",
            },
        ]
    ).reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean, rejected, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean) == 0
    assert len(rejected) == 2
    assert any(i.rule_id == "R_DUP_PK_CONFLICT" for i in issues)


# ── pacientes_cleaner edge cases ─────────────────────────────────────────────


def test_clean_pacientes_no_source_row_id(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.pacientes_cleaner import clean_pacientes

    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2000-01-01",
                "edad": None,
                "sexo": "M",
                "email": "a@a.com",
                "telefono": "1234567890",
                "ciudad": "Bogotá",
            }
        ]
    )
    clean, rejected, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean) == 1


def test_clean_pacientes_no_sexo_column(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.pacientes_cleaner import clean_pacientes

    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2000-01-01",
                "edad": 26,
            }
        ]
    ).reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean, rejected, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert any(i.rule_id == "R_SEXO_MISSING" for i in issues)


def test_clean_pacientes_derived_age_unreasonable(
    reference_date: date, settings: Settings
) -> None:
    from src.transform.cleaners.pacientes_cleaner import clean_pacientes

    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "Future",
                "fecha_nacimiento": "2130-01-01",
                "edad": None,
                "sexo": "M",
                "email": None,
                "telefono": None,
                "ciudad": "Bogotá",
            }
        ]
    ).reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean, rejected, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert any(i.rule_id == "R_EDAD_DERIVADA_UNREASONABLE" for i in issues)


def test_clean_pacientes_edad_out_of_range(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.pacientes_cleaner import clean_pacientes

    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "Old",
                "fecha_nacimiento": "1900-01-01",
                "edad": 200,
                "sexo": "M",
                "email": None,
                "telefono": None,
                "ciudad": "Bogotá",
            }
        ]
    ).reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean, rejected, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert any(i.rule_id == "R_EDAD_PROVIDED_OUT_OF_RANGE" for i in issues)


# ── pacientes_contact_cleaner: columns ausentes ───────────────────────────────


def test_contact_cleaner_no_email_column(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.pacientes_contact_cleaner import clean_pacientes_contacto
    from src.core.schemas import IssueRecord

    df = pd.DataFrame(
        [{"id_paciente": 1, "telefono": "1234567890", "source_row_id": 0}]
    )
    issues: list[IssueRecord] = []
    result = clean_pacientes_contacto(df, table_name="pacientes", issues=issues, settings=settings)
    assert result.iloc[0]["email"] is None


def test_contact_cleaner_no_telefono_column(reference_date: date, settings: Settings) -> None:
    from src.transform.cleaners.pacientes_contact_cleaner import clean_pacientes_contacto
    from src.core.schemas import IssueRecord

    df = pd.DataFrame(
        [{"id_paciente": 1, "email": "a@a.com", "source_row_id": 0}]
    )
    issues: list[IssueRecord] = []
    result = clean_pacientes_contacto(df, table_name="pacientes", issues=issues, settings=settings)
    assert result.iloc[0]["telefono"] is None


# ── validation_impl edge cases ────────────────────────────────────────────────


def test_validate_cross_references_invalid_estado_remaining(
    reference_date: date, settings: Settings
) -> None:
    from src.transform.validation_impl import validate_cross_references

    pacientes = pd.DataFrame([{"id_paciente": 1, "source_row_id": 0}])
    citas = pd.DataFrame(
        [{"id_cita": "c1", "id_paciente": 1, "estado_cita": "EstadoInvalido", "source_row_id": 0}]
    )
    _, _, issues = validate_cross_references(
        pacientes, citas, reference_date=reference_date, settings=settings
    )
    assert any(i.rule_id == "R_ESTADO_CITA_INVALID_REMAINING" for i in issues)


def test_validate_cross_references_dup_pk_defensive(
    reference_date: date, settings: Settings
) -> None:
    from src.transform.validation_impl import validate_cross_references

    pacientes = pd.DataFrame([{"id_paciente": 1, "source_row_id": 0}])
    citas = pd.DataFrame(
        [
            {"id_cita": "c1", "id_paciente": 1, "estado_cita": "Completada", "source_row_id": 0},
            {"id_cita": "c1", "id_paciente": 1, "estado_cita": "Completada", "source_row_id": 1},
        ]
    )
    _, _, issues = validate_cross_references(
        pacientes, citas, reference_date=reference_date, settings=settings
    )
    assert any(i.rule_id == "R_DUP_PK_AFTER_CROSS_VALIDATION" for i in issues)


# ── helpers ───────────────────────────────────────────────────────────────────


def test_get_completeness_not_found() -> None:
    from src.report.technical_report.helpers import get_completeness

    df = pd.DataFrame([{"scope": "table_column", "table": "pacientes", "column": "edad", "metric_name": "completeness_pct", "metric_value": 0.9}])
    result = get_completeness(df, "pacientes", "nonexistent_column")
    assert result is None


def test_truncate_long_string() -> None:
    from src.report.technical_report.helpers import truncate

    long = "x" * 200
    result = truncate(long)
    assert result.endswith("...")
    assert len(result) <= 143


# ── audit report: all branches ────────────────────────────────────────────────


def test_build_quality_metrics_section_nan_value() -> None:
    from src.report.technical_report.audit import build_quality_metrics_section

    before_df = pd.DataFrame(
        [
            {
                "scope": "global",
                "table": None,
                "column": None,
                "metric_name": "completeness_global_pct",
                "metric_value": float("nan"),
            }
        ]
    )
    after_df = before_df.copy()
    quality_summary = {
        "global_before": {"completeness_global_pct": float("nan"), "referential_integrity_fk_valid_pct_among_non_null": 1.0},
        "global_after": {"completeness_global_pct": float("nan"), "referential_integrity_fk_valid_pct_among_non_null": 1.0},
    }
    lines = build_quality_metrics_section(
        before_metrics_df=before_df,
        after_metrics_df=after_df,
        quality_summary=quality_summary,
    )
    assert any("NULL" in l for l in lines)


def test_build_audit_sections_all_rule_branches() -> None:
    """Two calls needed: top_rules[:10] caps at 10, but _action() has 12 distinct paths."""
    from src.core.schemas import IssueRecord
    from src.report.technical_report.audit import build_audit_sections

    def _make_issues(rule_ids: list[str]) -> list[IssueRecord]:
        return [
            IssueRecord(
                table="pacientes",
                row_id=i,
                rule_id=rid,
                severity="warning",
                column="col",
                original_value=f"val_{i}",
                clean_value=f"clean_{i}",
                detail="test",
            )
            for i, rid in enumerate(rule_ids)
        ]

    # ── Call 1: covers _action branches for lines 89–107 (10 rules, all fit in top_rules[:10]) ──
    batch1 = [
        "R_ORPHAN_CITA",
        "R_DUP_PK_CONFLICT",
        "R_COST_NON_NUMERIC",
        "R_DUP_PK_EXACT",
        "R_EDAD_FILLED_FROM_DERIVADA",
        "R_EDAD_INCONSISTENT_WITH_DERIVED",
        "R_EDAD_PROVIDED_OUT_OF_RANGE",
        "R_FECHA_NAC_INVALID",
        "R_EMAIL_INVALID",
        "R_TELEFONO_INVALID_LENGTH",
    ]
    lines1 = build_audit_sections(
        rejected_counts={"pacientes_rejected_rows": 2, "citas_medicas_rejected_rows": 1},
        issue_stats={
            "by_rule_id": {rid: 1 for rid in batch1},
            "severity_by_rule": {rid: "warning" for rid in batch1},
        },
        issues=_make_issues(batch1),
    )
    assert any("Rechazo por FK" in l for l in lines1)
    assert any("PK conflictiva" in l for l in lines1)
    assert any("costo inválido" in l for l in lines1)

    # ── Call 2: covers _action lines 109 (R_FECHA_CITA_FUTURE), 110 (unknown rule),
    #            and line 138 (rule with no severity entry) ──
    batch2 = ["R_FECHA_CITA_FUTURE", "R_UNKNOWN_RULE"]
    lines2 = build_audit_sections(
        rejected_counts={"pacientes_rejected_rows": 0, "citas_medicas_rejected_rows": 0},
        issue_stats={
            "by_rule_id": {rid: 1 for rid in batch2},
            "severity_by_rule": {"R_FECHA_CITA_FUTURE": "warning"},  # R_UNKNOWN_RULE has no entry → line 138
        },
        issues=_make_issues(batch2),
    )
    assert any("R_FECHA_CITA_FUTURE" in l for l in lines2)
    assert any("R_UNKNOWN_RULE" in l for l in lines2)
