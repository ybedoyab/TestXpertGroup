from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class PipelineDefaults:
    """Configuración por defecto para la ejecución CLI del pipeline."""

    input_path: str = "data/raw/dataset_hospital 2.json"
    output_dir: str = "."
    reference_date: date = date(2026, 3, 18)
    limit: int = 0
    age_tolerance_years: int = 2


DEFAULTS = PipelineDefaults()
