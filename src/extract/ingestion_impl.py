from __future__ import annotations

"""Extract tables from the input JSON into DataFrames."""

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.schemas import required_columns_for_minimal_schema
from src.core.utils import try_cast_int

logger = logging.getLogger(__name__)


def _to_dataframe(rows: list[dict[str, Any]], *, table_name: str) -> pd.DataFrame:
    if not isinstance(rows, list):
        raise ValueError(f"Table '{table_name}' must be a JSON array.")
    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("Table '%s' is empty after ingestion.", table_name)
    df = df.reset_index(drop=False).rename(columns={"index": "source_row_id"})
    return df


def validate_minimal_schema(df_dict: dict[str, pd.DataFrame]) -> None:
    required = required_columns_for_minimal_schema()
    for table_name, cols in required.items():
        if table_name not in df_dict:
            raise ValueError(f"Missing table '{table_name}' in input dataset.")
        missing = [c for c in cols if c not in df_dict[table_name].columns]
        if missing:
            raise ValueError(f"Table '{table_name}' missing columns: {missing}")


def load_dataset(json_path: Path, *, limit: int = 0) -> dict[str, pd.DataFrame]:
    if not json_path.exists():
        raise FileNotFoundError(str(json_path))

    raw = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Top-level JSON must be an object with table keys.")

    df_dict: dict[str, pd.DataFrame] = {}
    for table_name in ("pacientes", "citas_medicas"):
        if table_name not in raw:
            raise ValueError(f"Missing '{table_name}' in dataset JSON.")
        rows = raw[table_name]
        if limit and limit > 0:
            rows = rows[:limit]
        df = _to_dataframe(rows, table_name=table_name)

        if table_name == "pacientes" and "id_paciente" in df.columns:
            df["id_paciente"] = df["id_paciente"].apply(try_cast_int)
        if table_name == "citas_medicas" and "id_paciente" in df.columns:
            df["id_paciente"] = df["id_paciente"].apply(try_cast_int)
            if "id_cita" in df.columns:
                df["id_cita"] = df["id_cita"].apply(lambda x: str(x).strip() if x is not None else None)

        df_dict[table_name] = df

    validate_minimal_schema(df_dict)
    return df_dict

