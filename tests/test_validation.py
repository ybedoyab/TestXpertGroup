from __future__ import annotations

from datetime import date

import pandas as pd

from config.settings import Settings
from src.transform.validation_impl import validate_cross_references


def test_validate_cross_references_orphans_rejected(
    reference_date: date, settings: Settings
) -> None:
    pacientes_clean = pd.DataFrame(
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
                "source_row_id": 0,
            }
        ]
    )

    citas_clean = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100,
                "estado_cita": "Completada",
                "source_row_id": 0,
            },
            {
                "id_cita": "uuid-2",
                "id_paciente": 999,
                "fecha_cita": "2022-02-15",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 120,
                "estado_cita": "Cancelada",
                "source_row_id": 1,
            },
        ]
    )

    citas_valid, rejected_orphans, issues = validate_cross_references(
        pacientes_clean, citas_clean, reference_date=reference_date, settings=settings
    )

    assert len(citas_valid) == 1
    assert len(rejected_orphans) == 1
    assert rejected_orphans.iloc[0]["id_paciente"] == 999
    assert any(i.rule_id == "R_ORPHAN_CITA" for i in issues)


def test_validate_no_orphans(reference_date: date, settings: Settings) -> None:
    pacientes_clean = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "sexo": "M",
                "source_row_id": 0,
            }
        ]
    )

    citas_clean = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "estado_cita": "Completada",
                "source_row_id": 0,
            },
        ]
    )

    citas_valid, rejected_orphans, issues = validate_cross_references(
        pacientes_clean, citas_clean, reference_date=reference_date, settings=settings
    )

    assert len(citas_valid) == 1
    assert len(rejected_orphans) == 0
    assert not any(i.rule_id == "R_ORPHAN_CITA" for i in issues)


def test_validate_null_fk_rejected(reference_date: date, settings: Settings) -> None:
    pacientes_clean = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "sexo": "M",
                "source_row_id": 0,
            }
        ]
    )

    citas_clean = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": None,
                "fecha_cita": "2022-02-14",
                "estado_cita": "Completada",
                "source_row_id": 0,
            },
        ]
    )

    citas_valid, rejected, issues = validate_cross_references(
        pacientes_clean, citas_clean, reference_date=reference_date, settings=settings
    )

    assert len(citas_valid) == 0
    assert len(rejected) == 1
    assert any(i.rule_id == "R_ORPHAN_CITA" for i in issues)
