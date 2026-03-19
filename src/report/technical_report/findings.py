from __future__ import annotations

"""Findings and rules sections for the technical report."""

from typing import Any


def build_findings_and_rules_sections(*, profiling_before: dict[str, Any]) -> list[str]:
    pacientes_before_prof = profiling_before.get("pacientes", {})
    citas_before_prof = profiling_before.get("citas_medicas", {})

    lines: list[str] = []
    lines.append("## Hallazgos principales de calidad y recuperabilidad detectada")
    lines.append("### Tabla `pacientes`")
    lines.append(f"- Filas: {pacientes_before_prof.get('row_count')}")
    if "missing_counts" in pacientes_before_prof:
        lines.append("- Nulos (top):")
        miss = sorted(pacientes_before_prof["missing_counts"].items(), key=lambda x: x[1], reverse=True)[:5]
        for c, n in miss:
            lines.append(f"  - `{c}`: {n}")
    if "format_anomalies" in pacientes_before_prof:
        lines.append("- Formatos anómalos:")
        for k, v in pacientes_before_prof["format_anomalies"].items():
            lines.append(f"  - `{k}`: {v}")
    lines.append("")

    lines.append("### Tabla `citas_medicas`")
    lines.append(f"- Filas: {citas_before_prof.get('row_count')}")
    if "missing_counts" in citas_before_prof:
        lines.append("- Nulos (top):")
        miss = sorted(citas_before_prof["missing_counts"].items(), key=lambda x: x[1], reverse=True)[:5]
        for c, n in miss:
            lines.append(f"  - `{c}`: {n}")
    if "format_anomalies" in citas_before_prof:
        lines.append("- Formatos anómalos:")
        for k, v in citas_before_prof["format_anomalies"].items():
            lines.append(f"  - `{k}`: {v}")
    lines.append("")

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

