from __future__ import annotations

"""Cleaning rules for `pacientes` contact fields."""

from typing import Any

import pandas as pd

from config.settings import Settings
from src.core.audit import add_issue
from src.core.utils import normalize_email, normalize_phone_digits


def clean_pacientes_contacto(
    dfw: pd.DataFrame,
    *,
    table_name: str,
    issues: list[Any],
    settings: Settings,
) -> pd.DataFrame:
    if "email" in dfw.columns:
        dfw["email_original"] = dfw["email"]
        emails_norm = dfw["email"].apply(normalize_email)
        mask_invalid_email = dfw["email_original"].notna() & emails_norm.isna()
        for idx in dfw.index[mask_invalid_email].tolist():
            add_issue(
                issues,
                table=table_name,
                row_id=dfw.at[idx, "source_row_id"],
                rule_id="R_EMAIL_INVALID",
                severity="warning",
                column="email",
                original_value=dfw.at[idx, "email_original"],
                clean_value=None,
                detail="Email no cumple formato válido; se establece NULL.",
            )
        dfw["email"] = emails_norm
    else:
        dfw["email"] = None

    if "telefono" in dfw.columns:
        dfw["telefono_original"] = dfw["telefono"]
        tel_digits = dfw["telefono"].apply(normalize_phone_digits)
        mask_outside = tel_digits.notna() & (
            (tel_digits.str.len() < settings.telefono_min_digits)
            | (tel_digits.str.len() > settings.telefono_max_digits)
        )
        for idx in dfw.index[mask_outside].tolist():
            add_issue(
                issues,
                table=table_name,
                row_id=dfw.at[idx, "source_row_id"],
                rule_id="R_TELEFONO_INVALID_LENGTH",
                severity="warning",
                column="telefono",
                original_value=dfw.at[idx, "telefono_original"],
                clean_value=None,
                detail=(
                    f"Longitud de teléfono fuera de rango "
                    f"[{settings.telefono_min_digits},{settings.telefono_max_digits}]."
                ),
            )
        dfw["telefono"] = tel_digits.where(~mask_outside, None)
    else:
        dfw["telefono"] = None

    return dfw

