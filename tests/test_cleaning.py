from __future__ import annotations

from datetime import date

import pandas as pd

from config.settings import Settings
from src.core.catalog_normalizers import normalize_estado_cita, normalize_sexo
from src.core.utils import parse_date_str
from src.transform.cleaners.citas_cleaner import clean_citas_medicas
from src.transform.cleaners.pacientes_cleaner import clean_pacientes

# ── Normalizers ──────────────────────────────────────────────────────────


def test_normalize_sexo_standard_values() -> None:
    assert normalize_sexo("Female") == "F"
    assert normalize_sexo("male") == "M"
    assert normalize_sexo("M") == "M"
    assert normalize_sexo("F") == "F"
    assert normalize_sexo("hombre") == "M"
    assert normalize_sexo("mujer") == "F"


def test_normalize_sexo_invalid_values() -> None:
    assert normalize_sexo("X") is None
    assert normalize_sexo(None) is None
    assert normalize_sexo("") is None
    assert normalize_sexo("   ") is None


def test_normalize_estado_cita_standard_values() -> None:
    assert normalize_estado_cita("completada") == "Completada"
    assert normalize_estado_cita("Cancelada") == "Cancelada"
    assert normalize_estado_cita("Reprogramada") == "Reprogramada"


def test_normalize_estado_cita_invalid_values() -> None:
    assert normalize_estado_cita("X") is None
    assert normalize_estado_cita(None) is None
    assert normalize_estado_cita("") is None


# ── Date parsing ─────────────────────────────────────────────────────────


def test_parse_date_recovery_swapped_dd_mm() -> None:
    assert parse_date_str("2023-19-01").isoformat() == "2023-01-19"


def test_parse_date_spanish_textual() -> None:
    assert parse_date_str("02 de nov de 1977").isoformat() == "1977-11-02"
    assert parse_date_str("2 nov 1977").isoformat() == "1977-11-02"
    assert parse_date_str("02/nov/1977").isoformat() == "1977-11-02"
    assert parse_date_str("15 de enero de 2000").isoformat() == "2000-01-15"
    assert parse_date_str("32 de nov de 1977") is None


def test_parse_date_standard_format() -> None:
    assert parse_date_str("2000-01-15").isoformat() == "2000-01-15"


def test_parse_date_invalid_returns_none() -> None:
    assert parse_date_str("not-a-date") is None
    assert parse_date_str("") is None
    assert parse_date_str(None) is None


def test_parse_date_out_of_range_returns_none() -> None:
    assert parse_date_str("2023-32-01") is None


# ── Pacientes cleaner ───────────────────────────────────────────────────


def test_clean_pacientes_age_email_phone(
    sample_pacientes_df: pd.DataFrame, reference_date: date, settings: Settings
) -> None:
    clean_df, rejected_df, issues = clean_pacientes(
        sample_pacientes_df, reference_date=reference_date, settings=settings
    )

    assert len(rejected_df) == 0

    assert set(clean_df["sexo"].dropna().unique().tolist()) <= {"M", "F"}

    row1 = clean_df.loc[clean_df["id_paciente"] == 1].iloc[0]
    assert pd.isna(row1["email"])
    assert pd.isna(row1["telefono"])
    assert int(row1["edad"]) == 26

    row3 = clean_df.loc[clean_df["id_paciente"] == 3].iloc[0]
    assert int(row3["edad"]) == 26
    assert bool(row3["edad_inconsistente"]) is True


def test_clean_pacientes_empty_dataset(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        columns=[
            "id_paciente", "nombre", "fecha_nacimiento", "edad",
            "sexo", "email", "telefono", "ciudad", "source_row_id",
        ]
    )
    clean_df, rejected_df, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_df) == 0
    assert len(rejected_df) == 0
    assert len(issues) == 0


def test_clean_pacientes_all_null_fields(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": None,
                "fecha_nacimiento": None,
                "edad": None,
                "sexo": None,
                "email": None,
                "telefono": None,
                "ciudad": None,
            }
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_df, rejected_df, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_df) == 1
    assert len(rejected_df) == 0


def test_clean_pacientes_missing_pk_rejected(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_paciente": None,
                "nombre": "Sin ID",
                "fecha_nacimiento": "2000-01-01",
                "edad": 26,
                "sexo": "M",
                "email": "x@x.com",
                "telefono": "1234567890",
                "ciudad": "Bogotá",
            }
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_df, rejected_df, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_df) == 0
    assert len(rejected_df) == 1
    assert any(i.rule_id == "R_PK_MISSING" for i in issues)


def test_clean_pacientes_duplicate_pk_exact(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2000-01-01",
                "edad": 26,
                "sexo": "M",
                "email": "a@a.com",
                "telefono": "1234567890",
                "ciudad": "Bogotá",
            },
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2000-01-01",
                "edad": 26,
                "sexo": "M",
                "email": "a@a.com",
                "telefono": "1234567890",
                "ciudad": "Bogotá",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_df, rejected_df, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_df) == 1
    assert any(i.rule_id == "R_DUP_PK_EXACT" for i in issues)


def test_clean_pacientes_duplicate_pk_conflict(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2000-01-01",
                "edad": 26,
                "sexo": "M",
                "email": "a@a.com",
                "telefono": "1234567890",
                "ciudad": "Bogotá",
            },
            {
                "id_paciente": 1,
                "nombre": "B",
                "fecha_nacimiento": "1990-06-15",
                "edad": 35,
                "sexo": "F",
                "email": "b@b.com",
                "telefono": "9876543210",
                "ciudad": "Cali",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_df, rejected_df, issues = clean_pacientes(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_df) == 0
    assert len(rejected_df) == 2
    assert any(i.rule_id == "R_DUP_PK_CONFLICT" for i in issues)


# ── Citas cleaner ────────────────────────────────────────────────────────


def test_clean_citas_reject_negative_cost(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
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
                "id_paciente": 1,
                "fecha_cita": "2022-02-15",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": -5,
                "estado_cita": "Cancelada",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_citas, rejected_citas, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_citas) == 1
    assert len(rejected_citas) == 1
    assert rejected_citas.iloc[0]["rejection_rule_id"] == "R_COST_NEGATIVE"


def test_clean_citas_reject_non_numeric_cost(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": "abc",
                "estado_cita": "Completada",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_citas, rejected_citas, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_citas) == 0
    assert len(rejected_citas) == 1
    assert any(i.rule_id == "R_COST_NON_NUMERIC" for i in issues)


def test_clean_citas_empty_dataset(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        columns=[
            "id_cita", "id_paciente", "fecha_cita", "especialidad",
            "medico", "costo", "estado_cita", "source_row_id",
        ]
    )
    clean_citas, rejected_citas, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_citas) == 0
    assert len(rejected_citas) == 0
    assert len(issues) == 0


def test_clean_citas_missing_pk_rejected(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_cita": None,
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100,
                "estado_cita": "Completada",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_citas, rejected_citas, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_citas) == 0
    assert len(rejected_citas) == 1
    assert any(i.rule_id == "R_PK_MISSING" for i in issues)


def test_clean_citas_future_date_flagged_but_kept(reference_date: date, settings: Settings) -> None:
    """Una cita con fecha futura se conserva pero genera un issue R_FECHA_CITA_FUTURE."""
    future_date = date(reference_date.year + 1, 1, 1).isoformat()
    df = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": future_date,
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100,
                "estado_cita": "Completada",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_citas, rejected_citas, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_citas) == 1, "El registro no debe rechazarse por fecha futura"
    assert len(rejected_citas) == 0
    assert any(i.rule_id == "R_FECHA_CITA_FUTURE" for i in issues)
    future_issues = [i for i in issues if i.rule_id == "R_FECHA_CITA_FUTURE"]
    assert future_issues[0].severity == "warning"


def test_clean_citas_past_date_no_future_flag(reference_date: date, settings: Settings) -> None:
    """Una cita con fecha en el pasado no genera R_FECHA_CITA_FUTURE."""
    df = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2020-01-15",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100,
                "estado_cita": "Completada",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_citas, rejected_citas, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_citas) == 1
    assert not any(i.rule_id == "R_FECHA_CITA_FUTURE" for i in issues)


def test_clean_citas_invalid_estado_becomes_null(reference_date: date, settings: Settings) -> None:
    df = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100,
                "estado_cita": "Pendiente",
            },
        ]
    )
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    clean_citas, rejected_citas, issues = clean_citas_medicas(
        df, reference_date=reference_date, settings=settings
    )
    assert len(clean_citas) == 1
    assert pd.isna(clean_citas.iloc[0]["estado_cita"])
    assert any(i.rule_id == "R_ESTADO_CITA_UNRECOGNIZED" for i in issues)
