from __future__ import annotations

"""SQLite DWH schema (star) builder."""

import sqlite3

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE dim_paciente (
    paciente_key INTEGER PRIMARY KEY,
    id_paciente INTEGER UNIQUE,
    nombre TEXT,
    sexo TEXT,
    ciudad TEXT
);

CREATE TABLE dim_medico (
    medico_key INTEGER PRIMARY KEY,
    medico_nombre TEXT UNIQUE
);

CREATE TABLE dim_especialidad (
    especialidad_key INTEGER PRIMARY KEY,
    especialidad_nombre TEXT UNIQUE
);

CREATE TABLE dim_fecha (
    fecha_key INTEGER PRIMARY KEY,
    fecha_date TEXT,
    ano INTEGER,
    mes INTEGER,
    dia INTEGER
);

CREATE TABLE fact_citas (
    id_cita TEXT PRIMARY KEY,
    fecha_key INTEGER,
    paciente_key INTEGER,
    medico_key INTEGER,
    especialidad_key INTEGER,
    costo REAL,
    estado_cita TEXT,
    FOREIGN KEY(fecha_key) REFERENCES dim_fecha(fecha_key),
    FOREIGN KEY(paciente_key) REFERENCES dim_paciente(paciente_key),
    FOREIGN KEY(medico_key) REFERENCES dim_medico(medico_key),
    FOREIGN KEY(especialidad_key) REFERENCES dim_especialidad(especialidad_key)
);
"""


def init_schema(cur: sqlite3.Cursor) -> None:
    cur.executescript(SCHEMA_SQL)
    cur.execute(
        "INSERT INTO dim_fecha(fecha_key, fecha_date, ano, mes, dia) VALUES (0, 'UNKNOWN', NULL, NULL, NULL)"
    )

