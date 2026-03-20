from __future__ import annotations

from typing import Any

import pandas as pd

from config.settings import Settings
from src.core.catalog_normalizers import normalize_estado_cita
from src.core.schemas import EXPECTED_COLUMNS_CITAS_MEDICAS
from src.core.utils import parse_date_str, try_cast_float
from src.transform.metrics.base import completeness, pk_uniqueness, validity_pct_non_null


def compute_metrics_for_citas(
    df: pd.DataFrame,
    *,
    pacientes_ids: set[Any],
    settings: Settings,
) -> dict[str, Any]:
    expected_cols = EXPECTED_COLUMNS_CITAS_MEDICAS
    out: dict[str, Any] = {"row_count": int(len(df))}
    out["completeness"] = completeness(df, expected_cols)
    out["pk_uniqueness"] = pk_uniqueness(df, "id_cita")

    fecha_parsed = (
        df["fecha_cita"].apply(parse_date_str) if "fecha_cita" in df.columns else pd.Series([None] * len(df))
    )
    valid_fecha = df["fecha_cita"].notna() & fecha_parsed.notna()
    estado_valid = (
        df["estado_cita"].notna() & df["estado_cita"].apply(normalize_estado_cita).notna()
        if "estado_cita" in df.columns
        else pd.Series([False] * len(df))
    )
    costo_cast = df["costo"].apply(try_cast_float) if "costo" in df.columns else pd.Series([None] * len(df))
    valid_costo = df["costo"].notna() & costo_cast.notna() & (costo_cast >= 0)

    out["validity"] = {
        "fecha_cita_validity_pct": validity_pct_non_null(valid_fecha),
        "estado_cita_validity_pct": validity_pct_non_null(estado_valid),
        "costo_validity_pct": validity_pct_non_null(valid_costo),
    }

    if "id_paciente" in df.columns:
        fk_present = df["id_paciente"].notna()
        fk_valid = fk_present & df["id_paciente"].isin(pacientes_ids)
        out["referential_integrity"] = {
            "fk_valid_pct_among_non_null": float(fk_valid.sum() / fk_present.sum()) if fk_present.sum() else float("nan"),
            "fk_valid_pair_count": int(fk_valid.sum()),
            "fk_non_null_count": int(fk_present.sum()),
        }
    else:
        out["referential_integrity"] = {
            "fk_valid_pct_among_non_null": float("nan"),
            "fk_valid_pair_count": 0,
            "fk_non_null_count": 0,
        }
    return out

