from __future__ import annotations

"""Profiling stage for extracted hospital tables."""

import datetime as _dt
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.schemas import (
    CATALOG_ESTADO_CITA,
    CATALOG_SEXO,
    expected_columns_citas_medicas,
    expected_columns_pacientes,
    pk_column_citas,
    pk_column_pacientes,
)
from src.core.utils import (
    ensure_dir,
    is_valid_email,
    normalize_phone_digits,
    parse_date_str,
    try_cast_float,
)

logger = logging.getLogger(__name__)


def _strict_iso_date(v: Any) -> bool:
    """Devuelve True si *v* parsea estrictamente como ``YYYY-MM-DD``."""
    try:
        _dt.datetime.strptime(str(v).strip(), "%Y-%m-%d")
        return True
    except Exception:
        return False


def _date_anomaly_stats(series: pd.Series) -> dict[str, int]:
    """Calcula estadísticas de anomalía de fechas para una serie dada."""
    values = series.dropna().tolist()
    non_null_count = len(values)

    strict_ok_count = sum(1 for v in values if _strict_iso_date(v))
    strict_fail_count = non_null_count - strict_ok_count

    recovered_count = 0
    for v in values:
        if _strict_iso_date(v):
            continue
        if parse_date_str(v) is not None:
            recovered_count += 1

    return {
        "invalid_raw": int(strict_fail_count),
        "recovered_by_swap": int(recovered_count),
        "final_null": int(strict_fail_count - recovered_count),
    }


def _top_categories(series: pd.Series, *, limit: int = 5) -> list[dict[str, Any]]:
    vc = series.dropna().astype(str).value_counts().head(limit)
    return [{"value": str(k), "count": int(v)} for k, v in vc.items()]


def profile_pacientes(df: pd.DataFrame) -> dict[str, Any]:
    expected_cols = expected_columns_pacientes()
    pk_col = pk_column_pacientes()
    out: dict[str, Any] = {"table": "pacientes"}

    out["row_count"] = int(len(df))

    missing_counts = {c: int(df[c].isna().sum()) for c in expected_cols if c in df.columns}
    out["missing_counts"] = missing_counts

    dedup_df = df.drop(columns=["source_row_id"], errors="ignore")
    out["duplicate_counts"] = {
        "exact_duplicate_rows": int(dedup_df.duplicated().sum()),
        "pk_duplicate_rows": int(df[pk_col].duplicated().sum()) if pk_col in df.columns else 0,
    }

    fecha_stats = (
        _date_anomaly_stats(df["fecha_nacimiento"])
        if "fecha_nacimiento" in df.columns
        else {"invalid_raw": 0, "recovered_by_swap": 0, "final_null": 0}
    )

    sexo_valid = 0
    if "sexo" in df.columns:
        sexo_lower = df["sexo"].dropna().astype(str).str.strip().str.lower()
        sexo_valid = int(sexo_lower.isin(CATALOG_SEXO.keys()).sum())
    sexo_invalid = int(df["sexo"].notna().sum() - sexo_valid) if "sexo" in df.columns else 0

    email_invalid = 0
    if "email" in df.columns:
        email_mask = df["email"].notna()
        email_invalid = int(email_mask.sum() - df.loc[email_mask, "email"].apply(is_valid_email).sum())

    telefono_invalid = 0
    if "telefono" in df.columns:
        tel_mask = df["telefono"].notna()
        digits_len = df.loc[tel_mask, "telefono"].apply(normalize_phone_digits).dropna().str.len()
        telefono_invalid = int(tel_mask.sum() - int((digits_len >= 10).sum()))

    out["format_anomalies"] = {
        "fecha_nacimiento_invalid_raw": fecha_stats["invalid_raw"],
        "fecha_nacimiento_recovered_by_swap": fecha_stats["recovered_by_swap"],
        "fecha_nacimiento_final_null": fecha_stats["final_null"],
        "sexo_invalid": sexo_invalid,
        "email_invalid": email_invalid,
        "telefono_invalid": telefono_invalid,
    }

    card = {}
    for col in ["sexo", "ciudad"]:
        if col in df.columns:
            card[col] = {
                "unique_count": int(df[col].nunique(dropna=True)),
                "top": _top_categories(df[col], limit=5),
            }
    out["categorical_cardinality"] = card

    return out


def profile_citas_medicas(df: pd.DataFrame) -> dict[str, Any]:
    expected_cols = expected_columns_citas_medicas()
    pk_col = pk_column_citas()
    out: dict[str, Any] = {"table": "citas_medicas"}
    out["row_count"] = int(len(df))

    missing_counts = {c: int(df[c].isna().sum()) for c in expected_cols if c in df.columns}
    out["missing_counts"] = missing_counts

    dedup_df = df.drop(columns=["source_row_id"], errors="ignore")
    out["duplicate_counts"] = {
        "exact_duplicate_rows": int(dedup_df.duplicated().sum()),
        "pk_duplicate_rows": int(df[pk_col].duplicated().sum()) if pk_col in df.columns else 0,
    }

    fecha_stats = (
        _date_anomaly_stats(df["fecha_cita"])
        if "fecha_cita" in df.columns
        else {"invalid_raw": 0, "recovered_by_swap": 0, "final_null": 0}
    )

    estado_valid = 0
    if "estado_cita" in df.columns:
        estado_lower = df["estado_cita"].dropna().astype(str).str.strip().str.lower()
        estado_valid = int(estado_lower.isin(CATALOG_ESTADO_CITA.keys()).sum())
    estado_invalid = int(df["estado_cita"].notna().sum() - estado_valid) if "estado_cita" in df.columns else 0

    costo_non_numeric = 0
    costo_negative = 0
    if "costo" in df.columns:
        costo_mask = df["costo"].notna()
        costo_cast = df.loc[costo_mask, "costo"].apply(try_cast_float)
        costo_non_numeric = int(costo_cast.isna().sum())
        costo_negative = int((costo_cast.dropna() < 0).sum())

    out["format_anomalies"] = {
        "fecha_cita_invalid_raw": fecha_stats["invalid_raw"],
        "fecha_cita_recovered_by_swap": fecha_stats["recovered_by_swap"],
        "fecha_cita_final_null": fecha_stats["final_null"],
        "estado_cita_invalid": estado_invalid,
        "costo_non_numeric": int(costo_non_numeric),
        "costo_negative": int(costo_negative),
    }

    card: dict[str, Any] = {}
    for col in ["estado_cita", "especialidad", "medico"]:
        if col in df.columns:
            card[col] = {
                "unique_count": int(df[col].nunique(dropna=True)),
                "top": _top_categories(df[col], limit=5),
            }
    out["categorical_cardinality"] = card
    return out


def profile_dataset(df_dict: dict[str, pd.DataFrame]) -> dict[str, Any]:
    return {
        "pacientes": profile_pacientes(df_dict["pacientes"]),
        "citas_medicas": profile_citas_medicas(df_dict["citas_medicas"]),
    }


def save_profiling_report(report: dict[str, Any], out_path: Path) -> None:
    ensure_dir(out_path.parent)
    out_path.write_text(
        __import__("json").dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
