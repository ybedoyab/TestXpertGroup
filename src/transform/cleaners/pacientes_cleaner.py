from __future__ import annotations

"""Reglas de limpieza para la tabla `pacientes`."""

import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

from config.settings import Settings
from src.core.audit import add_issue
from src.core.catalog_normalizers import normalize_sexo
from src.core.cleaning_pk import resolve_pk_duplicates
from src.core.schemas import IssueRecord, pk_column_pacientes
from src.core.utils import compute_age, parse_date_str, try_cast_int
from src.transform.cleaners.pacientes_contact_cleaner import clean_pacientes_contacto


def clean_pacientes(
    df: pd.DataFrame,
    *,
    reference_date: date,
    settings: Settings,
) -> tuple[pd.DataFrame, pd.DataFrame, list[IssueRecord]]:
    table_name = "pacientes"
    pk_col = pk_column_pacientes()
    issues: list[IssueRecord] = []

    dfw = df.copy()
    if "source_row_id" not in dfw.columns:
        dfw = dfw.reset_index(drop=False).rename(columns={"index": "source_row_id"})

    if "sexo" in dfw.columns:
        dfw["sexo_original"] = dfw["sexo"]
        sexo_norm = dfw["sexo"].apply(normalize_sexo)
        mask_changed = dfw["sexo"].notna() & sexo_norm.isna()
        for idx in dfw.index[mask_changed].tolist():
            add_issue(
                issues,
                table=table_name,
                row_id=dfw.at[idx, "source_row_id"],
                rule_id="R_SEXO_UNRECOGNIZED",
                severity="warning",
                column="sexo",
                original_value=dfw.at[idx, "sexo_original"],
                clean_value=None,
                detail="Valor de sexo no mapeado al catálogo (M/F).",
            )
        dfw["sexo"] = sexo_norm
    else:
        dfw["sexo"] = None
        add_issue(
            issues,
            table=table_name,
            row_id="TABLE",
            rule_id="R_SEXO_MISSING",
            severity="error",
            column="sexo",
            original_value=None,
            clean_value=None,
            detail="Columna sexo ausente en la tabla.",
        )

    dfw["fecha_nacimiento_original"] = dfw.get("fecha_nacimiento")
    birth_dates = dfw["fecha_nacimiento_original"].apply(parse_date_str)
    mask_invalid_date = dfw["fecha_nacimiento_original"].notna() & birth_dates.isna()
    for idx in dfw.index[mask_invalid_date].tolist():
        add_issue(
            issues,
            table=table_name,
            row_id=dfw.at[idx, "source_row_id"],
            rule_id="R_FECHA_NAC_INVALID",
            severity="warning",
            column="fecha_nacimiento",
            original_value=dfw.at[idx, "fecha_nacimiento_original"],
            clean_value=None,
            detail="Fecha de nacimiento no parseable (formato esperado YYYY-MM-DD).",
        )
    dfw["fecha_nacimiento"] = birth_dates.apply(lambda d: d.isoformat() if d is not None else None)

    dfw["edad_original"] = dfw.get("edad")
    dfw["edad"] = dfw["edad"].apply(try_cast_int) if "edad" in dfw.columns else None

    derived_age = birth_dates.apply(lambda d: compute_age(reference_date, d) if d is not None else None)
    derived_age_sane = derived_age.where(derived_age.isna() | derived_age.between(0, 120))
    mask_derived_unreasonable = derived_age.notna() & derived_age_sane.isna()
    for idx in dfw.index[mask_derived_unreasonable].tolist():
        add_issue(
            issues,
            table=table_name,
            row_id=dfw.at[idx, "source_row_id"],
            rule_id="R_EDAD_DERIVADA_UNREASONABLE",
            severity="warning",
            column="edad_derivada",
            original_value=derived_age.at[idx],
            clean_value=None,
            detail="Edad derivada fuera de rango razonable (0-120).",
        )
    dfw["edad_derivada"] = derived_age_sane.astype("Int64")

    age_tol = settings.age_tolerance_years
    dfw["edad_limpia"] = dfw["edad"]
    dfw["edad_inconsistente"] = False

    mask_age_outside = dfw["edad"].notna() & ~dfw["edad"].between(0, 120)
    for idx in dfw.index[mask_age_outside].tolist():
        add_issue(
            issues,
            table=table_name,
            row_id=dfw.at[idx, "source_row_id"],
            rule_id="R_EDAD_PROVIDED_OUT_OF_RANGE",
            severity="warning",
            column="edad",
            original_value=dfw.at[idx, "edad"],
            clean_value=None,
            detail="Edad provista fuera de rango razonable (0-120).",
        )
    dfw.loc[mask_age_outside, "edad"] = None

    mask_need_derive = dfw["edad"].isna() & dfw["edad_derivada"].notna()
    dfw.loc[mask_need_derive, "edad"] = dfw.loc[mask_need_derive, "edad_derivada"]
    for idx in dfw.index[mask_need_derive].tolist():
        orig_age = dfw.at[idx, "edad_original"]
        fnac = dfw.at[idx, "fecha_nacimiento"]
        add_issue(
            issues,
            table=table_name,
            row_id=dfw.at[idx, "source_row_id"],
            rule_id="R_EDAD_FILLED_FROM_DERIVADA",
            severity="info",
            column="edad",
            original_value=f"{orig_age} (nació: {fnac})",
            clean_value=dfw.at[idx, "edad"],
            detail=f"Se completó edad usando edad derivada (tolerancia=±{age_tol}).",
        )

    mask_diff = dfw["edad"].notna() & dfw["edad_derivada"].notna() & (
        (dfw["edad"] - dfw["edad_derivada"]).abs() > age_tol
    )
    dfw.loc[mask_diff, "edad"] = dfw.loc[mask_diff, "edad_derivada"]
    dfw.loc[mask_diff, "edad_inconsistente"] = True
    for idx in dfw.index[mask_diff].tolist():
        orig_age = dfw.at[idx, "edad_original"]
        fnac = dfw.at[idx, "fecha_nacimiento"]
        add_issue(
            issues,
            table=table_name,
            row_id=dfw.at[idx, "source_row_id"],
            rule_id="R_EDAD_INCONSISTENT_WITH_DERIVED",
            severity="warning",
            column="edad",
            original_value=f"{orig_age} (nació: {fnac})",
            clean_value=dfw.at[idx, "edad"],
            detail=f"Inconsistencia entre edad provista y derivada > {age_tol} años.",
        )

    dfw = clean_pacientes_contacto(
        dfw, table_name=table_name, issues=issues, settings=settings
    )

    df_valid_pk, rejected_rows = resolve_pk_duplicates(
        dfw,
        pk_col=pk_col,
        table_name=table_name,
        issues=issues,
        allow_missing=True,
        missing_rule_id="R_PK_MISSING",
        conflict_rule_id="R_DUP_PK_CONFLICT",
        exact_rule_id="R_DUP_PK_EXACT",
    )

    logger.info(
        "Pacientes limpiados: %d válidos, %d rechazados, %d issues generados.",
        len(df_valid_pk),
        len(rejected_rows),
        len(issues),
    )
    return df_valid_pk, rejected_rows, issues

