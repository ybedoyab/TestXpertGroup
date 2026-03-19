from __future__ import annotations

"""SQLite star-schema DWH loader."""

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.utils import ensure_dir
from src.load.dwh.dims import (
    load_dim_fecha,
    load_dim_medico_y_especialidad,
    load_dim_paciente,
)
from src.load.dwh.facts import load_fact_citas
from src.load.dwh.keys import date_key_from_iso
from src.load.dwh.schema import init_schema


def load_to_sqlite(
    *,
    base_output_dir: Path,
    pacientes_clean: pd.DataFrame,
    citas_clean: pd.DataFrame,
) -> dict[str, Any]:
    reports_dir = base_output_dir / "data" / "reports"
    ensure_dir(reports_dir)
    db_path = reports_dir / "dwh.sqlite"

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        init_schema(cur)

        load_dim_paciente(cur, pacientes_clean)

        med_map, esp_map = load_dim_medico_y_especialidad(cur, citas_clean=citas_clean)

        citas = citas_clean.copy()
        citas["fecha_key"] = citas["fecha_cita"].apply(date_key_from_iso)
        dim_fecha_rows = load_dim_fecha(
            cur, citas_clean=citas, date_key_series=citas["fecha_key"]
        )

        fact_rows = load_fact_citas(
            cur,
            citas_clean=citas_clean,
            date_key_series=citas["fecha_key"],
            med_map=med_map,
            esp_map=esp_map,
        )

        conn.commit()
        try:
            db_path_rel = db_path.relative_to(base_output_dir).as_posix()
        except ValueError:
            db_path_rel = db_path.as_posix()
        return {
            "db_path": db_path_rel,
            "counts": {
                "dim_paciente_rows": int(len(pacientes_clean)),
                "dim_medico_rows": int(len(med_map)),
                "dim_especialidad_rows": int(len(esp_map)),
                "dim_fecha_rows": int(1 + dim_fecha_rows),
                "fact_citas_rows": int(fact_rows),
            },
        }
    finally:
        conn.close()

