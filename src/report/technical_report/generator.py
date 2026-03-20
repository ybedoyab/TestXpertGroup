from __future__ import annotations

"""Technical report Markdown generator."""

from pathlib import Path
from typing import Any

import markdown
import pandas as pd
from xhtml2pdf import pisa

from src.report.technical_report.audit import (
    build_audit_sections,
    build_quality_metrics_section,
)
from src.report.technical_report.executive import build_executive_section
from src.report.technical_report.findings import build_findings_and_rules_sections
from src.report.technical_report.governance_dwh import build_governance_and_dwh_section


def generate_technical_report_md(
    *,
    report_path: Path,
    input_path: Path,
    reference_date: str,
    profiling_before: dict[str, Any],
    rejected_counts: dict[str, int],
    issue_stats: dict[str, Any],
    issues: list[Any] | None,
    before_metrics_df: pd.DataFrame,
    after_metrics_df: pd.DataFrame,
    quality_summary: dict[str, Any],
) -> None:
    lines: list[str] = []
    lines.append("# Informe Técnico - Calidad de Datos Hospitalarios")
    lines.append("")

    lines.extend(
        build_executive_section(quality_summary=quality_summary, rejected_counts=rejected_counts)
    )

    lines.append("## Portada")
    lines.append(f"- Dataset de entrada: `{input_path.name}`")
    lines.append(f"- Fecha de referencia (edad derivada): `{reference_date}`")
    lines.append("")

    lines.append("## Objetivo")
    lines.append(
        "Evaluar la calidad del dataset, aplicar limpieza y validaciones con trazabilidad, "
        "y generar métricas antes/después junto con exportables listos para auditoría y migración."
    )
    lines.append("")

    lines.append("## Descripción del dataset")
    lines.append(
        "El dataset contiene al menos dos tablas: `pacientes` y `citas_medicas`. "
        "Las llaves principales son `pacientes.id_paciente` (entero) y `citas_medicas.id_cita` (UUID). "
        "La integridad referencial se basa en `citas_medicas.id_paciente -> pacientes.id_paciente`."
    )
    lines.append("")

    lines.append("## Enfoque metodológico")
    lines.extend(
        [
            "1. Ingesta determinista en `pandas.DataFrame`.",
            "2. Profiling exploratorio antes de limpieza (nulos, duplicados, formatos, cardinalidad).",
            "3. Limpieza conservadora con reglas explícitas y auditoría por registro/campo.",
            "4. Validaciones cruzadas e identificación de huérfanos/rechazos.",
            "5. Métricas de calidad antes y después y resumen ejecutable.",
            "6. Bonus: suite de pruebas automáticas (`pytest`, 96 tests, cobertura 100%) que validan "
            "integridad de datos, reglas de limpieza críticas y el pipeline end-to-end.",
            "7. Bonus: simulación de carga a un modelo tipo Data Warehouse (SQLite).",
            "",
        ]
    )

    lines.extend(build_findings_and_rules_sections(profiling_before=profiling_before))
    lines.append("")

    lines.extend(
        build_quality_metrics_section(
            before_metrics_df=before_metrics_df,
            after_metrics_df=after_metrics_df,
            quality_summary=quality_summary,
        )
    )
    lines.append("")

    lines.extend(
        build_audit_sections(
            rejected_counts=rejected_counts,
            issue_stats=issue_stats,
            issues=issues,
        )
    )

    lines.append("")
    lines.extend(build_governance_and_dwh_section(quality_summary=quality_summary))

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    pdf_path = report_path.with_suffix(".pdf")
    _generate_pdf_from_md("\n".join(lines) + "\n", pdf_path)


def _generate_pdf_from_md(md_text: str, pdf_path: Path) -> None:
    html_body = markdown.markdown(md_text, extensions=["tables"])
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            @page {{
                size: letter;
                margin: 2cm;
            }}
            body {{ font-family: Helvetica, Arial, sans-serif; font-size: 11px; line-height: 1.4; color: #333; }}
            h1 {{ color: #2C3E50; font-size: 18px; border-bottom: 2px solid #2C3E50; padding-bottom: 5px; }}
            h2 {{ color: #2980B9; font-size: 15px; margin-top: 15px; border-bottom: 1px solid #BDC3C7; padding-bottom: 3px; }}
            h3 {{ color: #34495E; font-size: 13px; margin-top: 10px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 15px; table-layout: fixed; }}
            th, td {{ border: 1px solid #BDC3C7; padding: 6px; text-align: left; word-wrap: break-word; overflow-wrap: break-word; }}
            th {{ background-color: #ECF0F1; font-weight: bold; color: #2C3E50; }}
            code {{ font-family: "Courier New", Courier, monospace; background-color: #F8F9FA; padding: 2px 4px; border-radius: 3px; font-size: 10px; white-space: pre-wrap; word-wrap: break-word; word-break: break-all; }}
            li {{ margin-bottom: 4px; }}
        </style>
    </head>
    <body>
        {html_body}
    </body>
    </html>
    """
    with open(pdf_path, "wb") as pdf_file:
        pisa.CreatePDF(html_content, dest=pdf_file)

