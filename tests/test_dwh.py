from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from src.load.dwh.loader import load_to_sqlite


def test_dwh_sqlite_creates_tables(tmp_path: Path) -> None:
    pacientes = pd.DataFrame(
        [
            {
                "id_paciente": 1,
                "nombre": "A",
                "sexo": "M",
                "ciudad": "Bogotá",
            },
        ]
    )
    citas = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100.0,
                "estado_cita": "Completada",
            },
        ]
    )

    result = load_to_sqlite(
        base_output_dir=tmp_path, pacientes_clean=pacientes, citas_clean=citas
    )

    db_path = tmp_path / "data" / "reports" / "dwh.sqlite"
    assert db_path.exists()

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM dim_paciente")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT COUNT(*) FROM dim_medico")
        assert cur.fetchone()[0] >= 1

        cur.execute("SELECT COUNT(*) FROM dim_especialidad")
        assert cur.fetchone()[0] >= 1

        cur.execute("SELECT COUNT(*) FROM dim_fecha")
        assert cur.fetchone()[0] >= 2  # 1 UNKNOWN + at least 1 data row

        cur.execute("SELECT COUNT(*) FROM fact_citas")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()

    assert result["counts"]["dim_paciente_rows"] == 1
    assert result["counts"]["fact_citas_rows"] == 1


def test_dwh_sqlite_fk_integrity(tmp_path: Path) -> None:
    pacientes = pd.DataFrame(
        [
            {"id_paciente": 1, "nombre": "A", "sexo": "M", "ciudad": "Bogotá"},
            {"id_paciente": 2, "nombre": "B", "sexo": "F", "ciudad": "Cali"},
        ]
    )
    citas = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": "2022-02-14",
                "especialidad": "Cardiología",
                "medico": "Dr. X",
                "costo": 100.0,
                "estado_cita": "Completada",
            },
            {
                "id_cita": "uuid-2",
                "id_paciente": 2,
                "fecha_cita": "2022-03-01",
                "especialidad": "Neurología",
                "medico": "Dr. Y",
                "costo": 200.0,
                "estado_cita": "Cancelada",
            },
        ]
    )

    load_to_sqlite(
        base_output_dir=tmp_path, pacientes_clean=pacientes, citas_clean=citas
    )

    db_path = tmp_path / "data" / "reports" / "dwh.sqlite"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()

        # Verify all fact_citas rows have valid FK to dim_paciente
        cur.execute("""
            SELECT COUNT(*)
            FROM fact_citas f
            LEFT JOIN dim_paciente p ON f.paciente_key = p.paciente_key
            WHERE p.paciente_key IS NULL
        """)
        orphan_facts = cur.fetchone()[0]
        assert orphan_facts == 0, "Hay fact_citas con FK huérfana hacia dim_paciente"

        # Verify all fact_citas rows have valid FK to dim_fecha
        cur.execute("""
            SELECT COUNT(*)
            FROM fact_citas f
            LEFT JOIN dim_fecha d ON f.fecha_key = d.fecha_key
            WHERE d.fecha_key IS NULL
        """)
        orphan_dates = cur.fetchone()[0]
        assert orphan_dates == 0, "Hay fact_citas con FK huérfana hacia dim_fecha"
    finally:
        conn.close()


def test_dwh_sqlite_null_fields_use_unknown(tmp_path: Path) -> None:
    pacientes = pd.DataFrame(
        [{"id_paciente": 1, "nombre": "A", "sexo": "M", "ciudad": "Bogotá"}]
    )
    citas = pd.DataFrame(
        [
            {
                "id_cita": "uuid-1",
                "id_paciente": 1,
                "fecha_cita": None,
                "especialidad": None,
                "medico": None,
                "costo": 50.0,
                "estado_cita": "Completada",
            },
        ]
    )

    load_to_sqlite(
        base_output_dir=tmp_path, pacientes_clean=pacientes, citas_clean=citas
    )

    db_path = tmp_path / "data" / "reports" / "dwh.sqlite"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT fecha_key, medico_key, especialidad_key FROM fact_citas")
        row = cur.fetchone()
        assert row[0] == 0, "fecha_key debería ser 0 (UNKNOWN) para NULL"
        assert row[1] == 0, "medico_key debería ser 0 (UNKNOWN) para NULL"
        assert row[2] == 0, "especialidad_key debería ser 0 (UNKNOWN) para NULL"
    finally:
        conn.close()
