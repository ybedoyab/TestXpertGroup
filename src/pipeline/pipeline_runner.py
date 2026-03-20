from __future__ import annotations

"""Pipeline orchestration from extraction to reports."""

from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import Settings
from src.core.utils import write_json
from src.extract.ingestion_impl import load_dataset
from src.load.dwh.loader import load_to_sqlite
from src.load.export_impl import export_datasets
from src.report.technical_report.generator import generate_technical_report_md
from src.transform.cleaners.citas_cleaner import clean_citas_medicas
from src.transform.cleaners.pacientes_cleaner import clean_pacientes
from src.transform.metrics.quality_metrics import compute_quality_metrics
from src.transform.profiling_impl import profile_dataset, save_profiling_report
from src.transform.validation_impl import validate_cross_references


def parse_reference_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError("Invalid --reference-date. Use format YYYY-MM-DD.") from e


def build_issue_stats(issues: list[Any]) -> dict[str, Any]:
    _order = {"info": 0, "warning": 1, "error": 2}
    by_severity: Counter[str] = Counter()
    by_rule_id: Counter[str] = Counter()
    severity_by_rule: dict[str, str] = {}
    for i in issues:
        sev = getattr(i, "severity", "info")
        rid = getattr(i, "rule_id", "UNKNOWN")
        by_severity[sev] += 1
        by_rule_id[rid] += 1
        if rid not in severity_by_rule or _order.get(sev, 0) > _order.get(severity_by_rule[rid], 0):
            severity_by_rule[rid] = sev
    return {
        "total_issues": len(issues),
        "by_severity": dict(by_severity),
        "by_rule_id": dict(by_rule_id),
        "severity_by_rule": severity_by_rule,
    }


def run_pipeline(
    *,
    input_path: Path,
    base_output_dir: Path,
    reference_date: date,
    limit: int = 0,
    age_tolerance_years: int = 2,
) -> None:
    import logging

    logger = logging.getLogger(__name__)
    settings = Settings(reference_date=reference_date, age_tolerance_years=age_tolerance_years)

    # ── Extract ──────────────────────────────────────────────────────────
    logger.info("Extrayendo dataset desde %s …", input_path)
    df_dict_before = load_dataset(input_path, limit=limit)

    # ── Profiling (antes de limpieza) ────────────────────────────────────
    logger.info("Generando profiling exploratorio …")
    profiling_before = profile_dataset(df_dict_before)
    save_profiling_report(
        profiling_before, base_output_dir / "data" / "reports" / "profiling_before.json"
    )

    # ── Cleaning ─────────────────────────────────────────────────────────
    logger.info("Limpiando tabla pacientes …")
    pacientes_clean, pacientes_rejected, issues_p = clean_pacientes(
        df_dict_before["pacientes"], reference_date=reference_date, settings=settings
    )
    logger.info("Limpiando tabla citas_medicas …")
    citas_clean_initial, citas_rejected_initial, issues_c = clean_citas_medicas(
        df_dict_before["citas_medicas"], reference_date=reference_date, settings=settings
    )
    issues_all: list[Any] = list(issues_p) + list(issues_c)

    # ── Cross-validation ─────────────────────────────────────────────────
    logger.info("Ejecutando validaciones cruzadas …")
    citas_clean_final, citas_rejected_orphans, issues_cross = validate_cross_references(
        pacientes_clean, citas_clean_initial, reference_date=reference_date, settings=settings
    )
    issues_all = issues_all + list(issues_cross)

    citas_rejected_final = (
        pd.concat([citas_rejected_initial, citas_rejected_orphans], ignore_index=True)
        if citas_rejected_initial is not None and not citas_rejected_initial.empty
        else citas_rejected_orphans
    )
    issue_stats = build_issue_stats(issues_all)

    rejected_counts = {
        "pacientes_rejected_rows": int(len(pacientes_rejected)) if pacientes_rejected is not None else 0,
        "citas_medicas_rejected_rows": int(len(citas_rejected_final)) if citas_rejected_final is not None else 0,
    }

    # ── Metrics ──────────────────────────────────────────────────────────
    logger.info("Calculando métricas de calidad antes/después …")
    df_dict_after = {"pacientes": pacientes_clean, "citas_medicas": citas_clean_final}
    before_metrics_df, after_metrics_df, metrics_summary = compute_quality_metrics(
        before=df_dict_before,
        after=df_dict_after,
        reference_date=reference_date,
        settings=settings,
    )

    quality_summary: dict[str, Any] = {}
    quality_summary.update(metrics_summary)
    quality_summary.update(
        {
            "rejected_counts": rejected_counts,
            "issue_stats": {
                "total_issues": issue_stats["total_issues"],
                "by_severity": issue_stats["by_severity"],
                "by_rule_id": issue_stats["by_rule_id"],
                "severity_by_rule": issue_stats["severity_by_rule"],
            },
            "counts": {
                "before_pacientes_rows": int(len(df_dict_before["pacientes"])),
                "after_pacientes_rows": int(len(pacientes_clean)),
                "before_citas_rows": int(len(df_dict_before["citas_medicas"])),
                "after_citas_rows": int(len(citas_clean_final)),
            },
        }
    )

    # ── Load: exports CSV/JSON ───────────────────────────────────────────
    logger.info("Exportando datasets limpios, rechazados y métricas …")
    export_datasets(
        base_output_dir=base_output_dir,
        pacientes_clean=pacientes_clean,
        citas_clean=citas_clean_final,
        pacientes_rejected=pacientes_rejected,
        citas_rejected=citas_rejected_final,
        issues=issues_all,
        before_metrics_df=before_metrics_df,
        after_metrics_df=after_metrics_df,
    )

    # ── Load: DWH SQLite ─────────────────────────────────────────────────
    logger.info("Cargando datos limpios al DWH (SQLite) …")
    dwh_result = load_to_sqlite(
        base_output_dir=base_output_dir,
        pacientes_clean=pacientes_clean,
        citas_clean=citas_clean_final,
    )
    quality_summary["dwh"] = dwh_result

    # ── Escribir quality_summary.json (una sola vez, con todos los datos) ─
    write_json(
        base_output_dir / "data" / "reports" / "quality_summary.json", quality_summary
    )

    # ── Report ───────────────────────────────────────────────────────────
    logger.info("Generando informe técnico …")
    generate_technical_report_md(
        report_path=base_output_dir / "docs" / "technical_report.md",
        input_path=input_path,
        reference_date=str(reference_date),
        profiling_before=profiling_before,
        rejected_counts=rejected_counts,
        issue_stats=quality_summary["issue_stats"],
        issues=issues_all,
        before_metrics_df=before_metrics_df,
        after_metrics_df=after_metrics_df,
        quality_summary=quality_summary,
    )

    logger.info("================ ENTREGABLES GENERADOS ================")
    logger.info("Informe Técnico PDF : %s", base_output_dir / "docs" / "technical_report.pdf")
    logger.info("Pacientes Limpios   : %s", base_output_dir / "data" / "processed" / "pacientes_clean.csv")
    logger.info("Citas Limpias       : %s", base_output_dir / "data" / "processed" / "citas_medicas_clean.csv")
    logger.info("Data Warehouse (DB) : %s", base_output_dir / "data" / "reports" / "dwh.sqlite")
    logger.info("=======================================================")
    logger.info("Pipeline completado exitosamente.")
