from __future__ import annotations

"""DWH fact table loader."""

import sqlite3
from typing import Any

import pandas as pd


def load_fact_citas(
    cur: sqlite3.Cursor,
    *,
    citas_clean: pd.DataFrame,
    date_key_series: pd.Series,
    med_map: dict[str, int],
    esp_map: dict[str, int],
) -> int:
    def map_medico(v: Any) -> int:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return 0
        s = str(v).strip()
        return med_map.get(s, 0) if s else 0

    def map_especialidad(v: Any) -> int:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return 0
        s = str(v).strip()
        return esp_map.get(s, 0) if s else 0

    citas = citas_clean.copy()
    citas["fecha_key"] = date_key_series

    fact_rows = []
    for idx in citas.index.tolist():
        row = citas.loc[idx]
        fact_rows.append(
            (
                row["id_cita"],
                int(row["fecha_key"]),
                int(row["id_paciente"]),
                map_medico(row.get("medico")),
                map_especialidad(row.get("especialidad")),
                row.get("costo"),
                row.get("estado_cita"),
            )
        )

    cur.executemany(
        """
        INSERT INTO fact_citas(id_cita, fecha_key, paciente_key, medico_key, especialidad_key, costo, estado_cita)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        fact_rows,
    )
    return len(fact_rows)

