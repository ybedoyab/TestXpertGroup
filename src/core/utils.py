from __future__ import annotations

"""Utilities shared across the ETL pipeline."""

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)

_MESES_ES_MAP = {
    "ene": 1, "enero": 1, "feb": 2, "febrero": 2, "mar": 3, "marzo": 3,
    "abr": 4, "abril": 4, "may": 5, "mayo": 5, "jun": 6, "junio": 6,
    "jul": 7, "julio": 7, "ago": 8, "agosto": 8, "sep": 9, "septiembre": 9,
    "set": 9, "setiembre": 9, "oct": 10, "octubre": 10, "nov": 11,
    "noviembre": 11, "dic": 12, "diciembre": 12
}

_SPANISH_DATE_RE = re.compile(
    r"^(\d{1,2})[\s\-\/]*(?:de\s*)?([a-z]{3,10})[\s\-\/]*(?:de\s*)?(\d{4})$",
    re.IGNORECASE
)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def is_valid_email(value: str | None) -> bool:
    if value is None:
        return False
    v = str(value).strip()
    if not v:
        return False
    return _EMAIL_RE.match(v) is not None


def normalize_email(value: str | None) -> str | None:
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    v = v.lower()
    return v if is_valid_email(v) else None


def normalize_phone_digits(value: str | None) -> str | None:
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    digits = re.sub(r"\D+", "", v)
    return digits or None


def parse_date_str(value: str | None) -> date | None:
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return datetime.strptime(v, "%Y-%m-%d").date()
    except ValueError:
        pass

    m_es = _SPANISH_DATE_RE.match(v)
    if m_es:
        day_str, month_str, year_str = m_es.groups()
        month_num = _MESES_ES_MAP.get(month_str.lower())
        if month_num:
            try:
                y = int(year_str)
                d = int(day_str)
                if 1900 <= y <= 2100:
                    return date(y, month_num, d)
            except ValueError:
                pass

    parts = v.split("-")
    if len(parts) == 3:
        y_s, a_s, b_s = parts
        try:
            y = int(y_s)
            a = int(a_s)
            b = int(b_s)
        except ValueError:
            return None

        if 1900 <= y <= 2100 and 1 <= a <= 31 and 1 <= b <= 12:
            try:
                return date(y, b, a)
            except ValueError:
                return None

    logger.debug("Unparseable date: %r", value)
    return None


def compute_age(reference_date: date, birth_date: date | None) -> int | None:
    if birth_date is None:
        return None
    years = reference_date.year - birth_date.year
    if (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day):
        years -= 1
    return years


def try_cast_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


def try_cast_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        return float(s)
    except (ValueError, TypeError):
        return None

