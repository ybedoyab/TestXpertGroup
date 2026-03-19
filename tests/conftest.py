from __future__ import annotations

"""Fixtures compartidos para el suite de pruebas."""

from datetime import date
from typing import Any

import pandas as pd
import pytest

from config.settings import Settings

REFERENCE_DATE = date(2026, 3, 18)


@pytest.fixture
def reference_date() -> date:
    return REFERENCE_DATE


@pytest.fixture
def settings(reference_date: date) -> Settings:
    return Settings(reference_date=reference_date)


@pytest.fixture
def sample_pacientes_df() -> pd.DataFrame:
    """DataFrame de pacientes con escenarios variados para testing."""
    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "Ana García",
                "fecha_nacimiento": "2000-01-01",
                "edad": None,
                "sexo": "Female",
                "email": "bad-email",
                "telefono": "123",
                "ciudad": "Bogotá",
            },
            {
                "id_paciente": 2,
                "nombre": "Carlos López",
                "fecha_nacimiento": "invalid",
                "edad": 30,
                "sexo": "X",
                "email": "b@b.com",
                "telefono": "3171234567",
                "ciudad": "Cali",
            },
            {
                "id_paciente": 3,
                "nombre": "María Rodríguez",
                "fecha_nacimiento": "2000-01-01",
                "edad": 10,
                "sexo": "Male",
                "email": "c@c.com",
                "telefono": "317-123-4567",
                "ciudad": "Medellín",
            },
        ]
    )
    return df.reset_index(drop=False).rename(columns={"index": "source_row_id"})


@pytest.fixture
def sample_citas_df() -> pd.DataFrame:
    """DataFrame de citas médicas con escenarios variados para testing."""
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
    return df.reset_index(drop=False).rename(columns={"index": "source_row_id"})


@pytest.fixture
def minimal_dataset() -> dict[str, Any]:
    """Dataset mínimo (1 paciente, 1 cita) para smoke tests."""
    return {
        "pacientes": [
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2000-01-01",
                "edad": None,
                "sexo": "Male",
                "email": "a@a.com",
                "telefono": "1234567890",
                "ciudad": "Bogotá",
            }
        ],
        "citas_medicas": [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100,
                "estado_cita": "Completada",
            }
        ],
    }
