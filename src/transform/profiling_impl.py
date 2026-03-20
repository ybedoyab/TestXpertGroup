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
    EXPECTED_COLUMNS_CITAS_MEDICAS,
    EXPECTED_COLUMNS_PACIENTES,
    PK_CITAS,
    PK_PACIENTES,
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


def _profile_table(
    df: pd.DataFrame,
    *,
    table: str,
    expected_cols: list[str],
    pk_col: str,
    date_col: str,
    date_key_prefix: str,
    categorical_cols: list[str],
    extra_anomalies: dict[str, Any],
) -> dict[str, Any]:
    out: dict[str, Any] = {"table": table, "row_count": int(len(df))}
    out["missing_counts"] = {c: int(df[c].isna().sum()) for c in expected_cols if c in df.columns}
    dedup_df = df.drop(columns=["source_row_id"], errors="ignore")
    out["duplicate_counts"] = {
        "exact_duplicate_rows": int(dedup_df.duplicated().sum()),
        "pk_duplicate_rows": int(df[pk_col].duplicated().sum()) if pk_col in df.columns else 0,
    }
    fecha_stats = (
        _date_anomaly_stats(df[date_col])
        if date_col in df.columns
        else {"invalid_raw": 0, "recovered_by_swap": 0, "final_null": 0}
    )
    out["format_anomalies"] = {
        f"{date_key_prefix}_invalid_raw": fecha_stats["invalid_raw"],
        f"{date_key_prefix}_recovered_by_swap": fecha_stats["recovered_by_swap"],
        f"{date_key_prefix}_final_null": fecha_stats["final_null"],
        **extra_anomalies,
    }
    out["categorical_cardinality"] = {
        col: {"unique_count": int(df[col].nunique(dropna=True)), "top": _top_categories(df[col], limit=5)}
        for col in categorical_cols
        if col in df.columns
    }
    return out


def profile_pacientes(df: pd.DataFrame) -> dict[str, Any]:
    sexo_valid = 0
    if "sexo" in df.columns:
        sexo_valid = int(df["sexo"].dropna().astype(str).str.strip().str.lower().isin(CATALOG_SEXO.keys()).sum())
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

    return _profile_table(
        df,
        table="pacientes",
        expected_cols=EXPECTED_COLUMNS_PACIENTES,
        pk_col=PK_PACIENTES,
        date_col="fecha_nacimiento",
        date_key_prefix="fecha_nacimiento",
        categorical_cols=["sexo", "ciudad"],
        extra_anomalies={
            "sexo_invalid": sexo_invalid,
            "email_invalid": email_invalid,
            "telefono_invalid": telefono_invalid,
        },
    )


def profile_citas_medicas(df: pd.DataFrame) -> dict[str, Any]:
    estado_valid = 0
    if "estado_cita" in df.columns:
        estado_valid = int(
            df["estado_cita"].dropna().astype(str).str.strip().str.lower().isin(CATALOG_ESTADO_CITA.keys()).sum()
        )
    estado_invalid = int(df["estado_cita"].notna().sum() - estado_valid) if "estado_cita" in df.columns else 0

    costo_non_numeric = 0
    costo_negative = 0
    if "costo" in df.columns:
        costo_mask = df["costo"].notna()
        costo_cast = df.loc[costo_mask, "costo"].apply(try_cast_float)
        costo_non_numeric = int(costo_cast.isna().sum())
        costo_negative = int((costo_cast.dropna() < 0).sum())

    return _profile_table(
        df,
        table="citas_medicas",
        expected_cols=EXPECTED_COLUMNS_CITAS_MEDICAS,
        pk_col=PK_CITAS,
        date_col="fecha_cita",
        date_key_prefix="fecha_cita",
        categorical_cols=["estado_cita", "especialidad", "medico"],
        extra_anomalies={
            "estado_cita_invalid": estado_invalid,
            "costo_non_numeric": int(costo_non_numeric),
            "costo_negative": int(costo_negative),
        },
    )


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
