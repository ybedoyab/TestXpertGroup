from __future__ import annotations

import argparse
import logging
from pathlib import Path

from config.config import DEFAULTS
from src.core.logging_config import configure_logging
from src.pipeline.pipeline_runner import parse_reference_date, run_pipeline

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="Hospital data quality pipeline")
    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULTS.input_path,
        help="Path al JSON de entrada (`dataset_hospital 2.json`).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULTS.output_dir,
        help="Carpeta base donde se escribirán `data/` y `docs/` (usa rutas relativas al repo).",
    )
    parser.add_argument(
        "--reference-date",
        type=str,
        default=DEFAULTS.reference_date.isoformat(),
        help="Fecha de referencia (YYYY-MM-DD) para calcular edad derivada.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULTS.limit,
        help="Si >0, carga solo las primeras N filas por tabla para depuración.",
    )
    parser.add_argument(
        "--age-tolerance-years",
        type=int,
        default=DEFAULTS.age_tolerance_years,
        help="Tolerancia (en años) entre `edad` y `edad_derivada` para considerar consistencia.",
    )

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = repo_root / input_path

    base_output_dir = Path(args.output_dir)
    if not base_output_dir.is_absolute():
        base_output_dir = repo_root / base_output_dir

    reference_date = parse_reference_date(args.reference_date)
    run_pipeline(
        input_path=input_path,
        base_output_dir=base_output_dir,
        reference_date=reference_date,
        limit=args.limit,
        age_tolerance_years=args.age_tolerance_years,
    )


if __name__ == "__main__":
    main()

