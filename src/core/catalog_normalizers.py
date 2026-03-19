from __future__ import annotations

"""Catalog normalizers for `sexo` and `estado_cita`."""

from typing import Any

from src.core.schemas import CATALOG_ESTADO_CITA, CATALOG_SEXO


def normalize_sexo(value: Any) -> str | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    if not v:
        return None
    return CATALOG_SEXO.get(v)


def normalize_estado_cita(value: Any) -> str | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    if not v:
        return None
    return CATALOG_ESTADO_CITA.get(v)

