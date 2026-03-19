from __future__ import annotations

import pandas as pd

from src.transform.profiling_impl import profile_citas_medicas, profile_dataset, profile_pacientes


def test_profile_pacientes_basic() -> None:
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
                "source_row_id": 0,
            },
            {
                "id_paciente": 2,
                "nombre": "B",
                "fecha_nacimiento": None,
                "edad": None,
                "sexo": None,
                "email": None,
                "telefono": None,
                "ciudad": None,
                "source_row_id": 1,
            },
        ]
    )
    result = profile_pacientes(df)

    assert result["table"] == "pacientes"
    assert result["row_count"] == 2
    assert result["missing_counts"]["email"] == 1
    assert result["missing_counts"]["telefono"] == 1
    assert result["missing_counts"]["sexo"] == 1
    assert result["format_anomalies"]["sexo_invalid"] == 0
    assert "categorical_cardinality" in result


def test_profile_citas_medicas_basic() -> None:
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
                "source_row_id": 0,
            },
        ]
    )
    result = profile_citas_medicas(df)

    assert result["table"] == "citas_medicas"
    assert result["row_count"] == 1
    assert result["format_anomalies"]["costo_non_numeric"] == 0
    assert result["format_anomalies"]["costo_negative"] == 0


def test_profile_dataset_returns_both_tables() -> None:
    df_dict = {
        "pacientes": pd.DataFrame(
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
                    "source_row_id": 0,
                }
            ]
        ),
    }
    result = profile_dataset(df_dict)

    assert "pacientes" in result
    assert "citas_medicas" in result
    assert result["pacientes"]["row_count"] == 1
    assert result["citas_medicas"]["row_count"] == 1


def test_profile_pacientes_date_anomaly_detected() -> None:
    df = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "fecha_nacimiento": "2023-19-01",
                "sexo": "M",
                "source_row_id": 0,
            },
        ]
    )
    result = profile_pacientes(df)
    assert result["format_anomalies"]["fecha_nacimiento_invalid_raw"] == 1
