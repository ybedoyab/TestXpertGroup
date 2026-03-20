from __future__ import annotations

from typing import Any

import pandas as pd

from config.settings import Settings
from src.core.catalog_normalizers import normalize_sexo
from src.core.schemas import EXPECTED_COLUMNS_PACIENTES
from src.core.utils import (
    compute_age,
    is_valid_email,
    normalize_phone_digits,
    parse_date_str,
)
from src.transform.metrics.base import completeness, pk_uniqueness, validity_pct_non_null


def compute_metrics_for_pacientes(
    df: pd.DataFrame,
    *,
    reference_date: Any,
    settings: Settings,
) -> dict[str, Any]:
    expected_cols = EXPECTED_COLUMNS_PACIENTES
    out: dict[str, Any] = {"row_count": int(len(df))}
    out["completeness"] = completeness(df, expected_cols)
    out["pk_uniqueness"] = pk_uniqueness(df, "id_paciente")

    fecha_parsed = (
        df["fecha_nacimiento"].apply(parse_date_str)
        if "fecha_nacimiento" in df.columns
        else pd.Series([None] * len(df))
    )
    valid_fecha = df["fecha_nacimiento"].notna() & fecha_parsed.notna()
    sexo_valid = (
        df["sexo"].notna() & df["sexo"].apply(normalize_sexo).notna() if "sexo" in df.columns else pd.Series([False] * len(df))
    )
    email_valid = (
        df["email"].notna() & df["email"].apply(is_valid_email) if "email" in df.columns else pd.Series([False] * len(df))
    )
    telefono_digits = df["telefono"].apply(normalize_phone_digits) if "telefono" in df.columns else pd.Series([None] * len(df))
    telefono_valid = (
        df["telefono"].notna()
        & telefono_digits.notna()
        & telefono_digits.str.len().between(settings.telefono_min_digits, settings.telefono_max_digits)
    ) if "telefono" in df.columns else pd.Series([False] * len(df))

    edad_valid = (
        df["edad"].notna() & pd.to_numeric(df["edad"], errors="coerce").between(0, 120)
    ) if "edad" in df.columns else pd.Series([False] * len(df))

    out["validity"] = {
        "fecha_nacimiento_validity_pct": validity_pct_non_null(valid_fecha),
        "sexo_validity_pct": validity_pct_non_null(sexo_valid),
        "email_validity_pct": validity_pct_non_null(email_valid),
        "telefono_validity_pct": validity_pct_non_null(telefono_valid),
        "edad_validity_pct": validity_pct_non_null(edad_valid),
    }

    derived_age = fecha_parsed.apply(lambda d: compute_age(reference_date, d) if d is not None else None)
    both_present = df["edad"].notna() & derived_age.notna()
    if both_present.any():
        diff = (
            pd.to_numeric(df.loc[both_present, "edad"], errors="coerce")
            - derived_age.loc[both_present]
        ).abs()
        consistent = diff <= settings.age_tolerance_years
        out["consistency"] = {
            "edad_consistency_pct": float(consistent.mean()),
            "edad_consistency_pair_count": int(both_present.sum()),
        }
    else:
        out["consistency"] = {"edad_consistency_pct": float("nan"), "edad_consistency_pair_count": 0}
    return out

