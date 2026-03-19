from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the data quality pipeline.

    Attributes:
        reference_date: Date used to compute derived age.
        age_tolerance_years: Maximum absolute difference allowed between
            provided `edad` and derived age before considering them inconsistent.
        telefono_min_digits: Minimum digits for a plausible phone number.
        telefono_max_digits: Maximum digits for a plausible phone number.
    """

    reference_date: date
    age_tolerance_years: int = 2
    telefono_min_digits: int = 10
    telefono_max_digits: int = 15

