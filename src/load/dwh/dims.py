from __future__ import annotations

"""DWH dimension loaders."""


import sqlite3

import pandas as pd


def load_dim_paciente(cur: sqlite3.Cursor, pacientes_clean: pd.DataFrame) -> None:
    pac = pacientes_clean.copy()
    pac["paciente_key"] = pac["id_paciente"]
    cur.executemany(
        """
        INSERT INTO dim_paciente(paciente_key, id_paciente, nombre, sexo, ciudad)
        VALUES (?, ?, ?, ?, ?)
        """,
        pac[["paciente_key", "id_paciente", "nombre", "sexo", "ciudad"]].itertuples(
            index=False, name=None
        ),
    )


def build_catalog_map(values: list[str]) -> dict[str, int]:
    catalog = sorted([x for x in values if x and x != "UNKNOWN"])
    mapping: dict[str, int] = {"UNKNOWN": 0}
    key = 1
    for v in catalog:
        mapping[v] = key
        key += 1
    return mapping


def load_dim_medico_y_especialidad(
    cur: sqlite3.Cursor,
    *,
    citas_clean: pd.DataFrame,
) -> tuple[dict[str, int], dict[str, int]]:
    med_names = (
        citas_clean["medico"]
        .fillna("UNKNOWN")
        .astype(str)
        .replace("", "UNKNOWN")
        .unique()
        .tolist()
    )
    esp_names = (
        citas_clean["especialidad"]
        .fillna("UNKNOWN")
        .astype(str)
        .replace("", "UNKNOWN")
        .unique()
        .tolist()
    )

    med_map = build_catalog_map(med_names)
    esp_map = build_catalog_map(esp_names)

    cur.executemany(
        "INSERT INTO dim_medico(medico_key, medico_nombre) VALUES (?, ?)",
        [(k, v) for v, k in med_map.items()],
    )
    cur.executemany(
        "INSERT INTO dim_especialidad(especialidad_key, especialidad_nombre) VALUES (?, ?)",
        [(k, v) for v, k in esp_map.items()],
    )
    return med_map, esp_map


def load_dim_fecha(
    cur: sqlite3.Cursor,
    *,
    citas_clean: pd.DataFrame,
    date_key_series: pd.Series,
) -> int:
    fecha_keys = sorted([k for k in date_key_series.unique().tolist() if k != 0])
    fecha_rows: list[tuple[int, str, int, int, int]] = []
    for fk in fecha_keys:
        y = fk // 10000
        m = (fk // 100) % 100
        d = fk % 100
        fecha_rows.append((fk, f"{y:04d}-{m:02d}-{d:02d}", y, m, d))

    cur.executemany(
        "INSERT INTO dim_fecha(fecha_key, fecha_date, ano, mes, dia) VALUES (?, ?, ?, ?, ?)",
        fecha_rows,
    )
    return len(fecha_rows)

