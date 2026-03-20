from __future__ import annotations

"""Findings and rules sections for the technical report."""

from typing import Any


def build_findings_and_rules_sections(*, profiling_before: dict[str, Any]) -> list[str]:
    pacientes_before_prof = profiling_before.get("pacientes", {})
    citas_before_prof = profiling_before.get("citas_medicas", {})

    lines: list[str] = []
    lines.append('<pdf:nextpage />')
    lines.append('<div style="page-break-inside: avoid; margin-bottom: 10px;">')
    lines.append('<h2 style="color: #2980B9; font-size: 15px; margin-top: 15px; border-bottom: 1px solid #BDC3C7; padding-bottom: 3px;">Hallazgos principales de calidad y recuperabilidad detectada</h2>')
    def _build_html_list(title: str, prof_dict: dict[str, Any]) -> str:
        out = [f"<h3>Tabla <code>{title}</code></h3>", f"&bull; <b>Filas:</b> {prof_dict.get('row_count')}<br>"]
        if "missing_counts" in prof_dict:
            out.append("&bull; <b>Nulos (top):</b><br>")
            miss = sorted(prof_dict["missing_counts"].items(), key=lambda x: x[1], reverse=True)[:5]
            for c, n in miss:
                out.append(f"&nbsp;&nbsp;&bull; <code>{c}</code>: {n}<br>")
        if "format_anomalies" in prof_dict:
            out.append("&bull; <b>Formatos anómalos:</b><br>")
            for k, v in prof_dict["format_anomalies"].items():
                out.append(f"&nbsp;&nbsp;&bull; <code>{k}</code>: {v}<br>")
        return "".join(out)

    lines.append('<table style="width: 100%; border: none; table-layout: fixed;">')
    lines.append('<tr>')
    
    p_html = _build_html_list("pacientes", pacientes_before_prof)
    c_html = _build_html_list("citas_medicas", citas_before_prof)
    
    lines.append(f'<td style="width: 50%; vertical-align: top; border: none; padding-right: 10px; line-height: 1.2;">\n{p_html}\n</td>')
    lines.append(f'<td style="width: 50%; vertical-align: top; border: none; padding-left: 10px; line-height: 1.2;">\n{c_html}\n</td>')
    
    lines.append('</tr>')
    lines.append('</table>')
    lines.append('</div>')

    lines.append("## Reglas de validación implementadas")
    lines.append("- Sexo: normalización a catálogo `{M,F}`; valores no mapeables se dejan `NULL` y se registran.")
    lines.append(
        "- Estado de cita: normalización a `{Completada, Cancelada, Reprogramada}`; no mapeables -> `NULL`."
    )
    lines.append(
        "- Fechas: parsing conservador `YYYY-MM-DD` y recuperación controlada `YYYY-DD-MM` solo cuando falla el parse inicial "
        "(por mes/día inválidos); si ambas fallan -> `NULL`."
    )
    lines.append("- Emails: formato válido vía regex; inválidos -> `NULL`.")
    lines.append("- Teléfonos: normalización a dígitos y validación de longitud razonable; fuera de rango -> `NULL`.")
    lines.append(
        "- Edad: cálculo derivado desde `fecha_nacimiento` usando `REFERENCE_DATE`; se corrige `edad` solo si está `NULL` "
        "o difiere por más de la tolerancia configurada (±2 años)."
    )
    lines.append("- Costo: debe ser numérico y no negativo; no numérico/negativo -> registro rechazado.")
    lines.append(
        "- Fecha de cita futura: se emite `warning` si `fecha_cita > REFERENCE_DATE`. "
        "El registro se conserva (puede ser cita programada); la decisión final se delega al data owner."
    )
    lines.append("- Integridad referencial: citas sin `id_paciente` existente -> registro rechazado.")
    lines.append("")

    lines.append("## Estrategia de limpieza aplicada")
    lines.append(
        "La limpieza evita inferencias clínicas/dudosas: no se infiere sexo desde el nombre. "
        "Las correcciones ambiguas se documentan como `warning` y quedan en auditoría. "
        "Las reglas que no son corregibles automáticamente (por ejemplo costo inválido o huérfanos) "
        "se trasladan a datasets `*_rejected.csv`."
    )
    lines.append("")

    lines.append("## Supuestos adoptados")
    lines.append(
        "- Fechas se interpretan inicialmente como `YYYY-MM-DD`. Cuando ese parse falla por inconsistencia de mes/día, "
        "se intenta un swap conservador `YYYY-DD-MM` y si no aplica, se deja `NULL`."
    )
    lines.append("- Teléfonos se consideran plausibles si tienen entre 10 y 15 dígitos.")
    lines.append("- Edad razonable se acota a `[0,120]` años; valores fuera se consideran inconsistentes.")
    lines.append("- Para dimensiones en el DWH, `NULL` se mapea a un miembro `UNKNOWN` (key=0).")
    lines.append("")

    return lines

