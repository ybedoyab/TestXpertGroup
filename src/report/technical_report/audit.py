from __future__ import annotations

"""Technical report audit sections (metrics + evidences)."""

from typing import Any

import pandas as pd

from src.report.technical_report.helpers import get_completeness, truncate


def build_quality_metrics_section(
    *, before_metrics_df: pd.DataFrame, after_metrics_df: pd.DataFrame, quality_summary: dict[str, Any]
) -> list[str]:
    def _pct(v: Any) -> str:
        if v is None or v != v:
            return "NULL"
        return f"{float(v) * 100:.2f}%"

    lines: list[str] = []
    gb_before = quality_summary.get("global_before", {})
    gb_after = quality_summary.get("global_after", {})

    lines.append("## Métricas de calidad (antes y después)")
    lines.append(
        f"- Completitud global (pct de celdas no nulas): {_pct(gb_before.get('completeness_global_pct'))} -> "
        f"{_pct(gb_after.get('completeness_global_pct'))}"
    )
    lines.append(
        "- Integridad referencial FK valid (pct entre no-nulos): "
        f"{_pct(gb_before.get('referential_integrity_fk_valid_pct_among_non_null'))} -> "
        f"{_pct(gb_after.get('referential_integrity_fk_valid_pct_among_non_null'))}"
    )
    lines.append("")

    lines.append("### Cambios de completitud (campos clave)")
    key_completeness = [
        ("pacientes", "email"),
        ("pacientes", "telefono"),
        ("pacientes", "sexo"),
        ("pacientes", "fecha_nacimiento"),
        ("citas_medicas", "estado_cita"),
        ("citas_medicas", "fecha_cita"),
        ("citas_medicas", "costo"),
        ("citas_medicas", "medico"),
        ("citas_medicas", "especialidad"),
    ]
    for table, col in key_completeness:
        b = get_completeness(before_metrics_df, table, col)
        a = get_completeness(after_metrics_df, table, col)
        if b is None or a is None or b != b or a != a:
            continue
        delta_pp = (a - b) * 100
        lines.append(
            f"- `{table}.{col}`: {b * 100:.2f}% -> {a * 100:.2f}% (delta {delta_pp:+.2f} pp)"
        )

    return lines


def build_audit_sections(
    *,
    rejected_counts: dict[str, int],
    issue_stats: dict[str, Any],
    issues: list[Any] | None,
) -> list[str]:
    by_rule_id = issue_stats.get("by_rule_id", {})
    severity_by_rule = issue_stats.get("severity_by_rule", {})
    top_rules = sorted(by_rule_id.items(), key=lambda x: x[1], reverse=True)[:10]

    orphan = int(by_rule_id.get("R_ORPHAN_CITA", 0))
    dup_conflict = int(by_rule_id.get("R_DUP_PK_CONFLICT", 0))
    cost_non_numeric = int(by_rule_id.get("R_COST_NON_NUMERIC", 0))
    cost_negative = int(by_rule_id.get("R_COST_NEGATIVE", 0))
    dup_exact = int(by_rule_id.get("R_DUP_PK_EXACT", 0))

    top_rule_ids = [rid for rid, _ in top_rules[:8]]
    evidence_by_rule: dict[str, Any] = {}
    if issues:
        for issue in issues:
            rid = getattr(issue, "rule_id", None)
            if rid in top_rule_ids and rid not in evidence_by_rule:
                evidence_by_rule[rid] = issue
            if len(evidence_by_rule) >= min(5, len(top_rule_ids)):
                break

    def _action(rule_id: str) -> str:
        if rule_id == "R_ORPHAN_CITA":
            return "Rechazo por FK huérfana"
        if rule_id == "R_DUP_PK_CONFLICT":
            return "Rechazo por PK conflictiva"
        if rule_id in {"R_COST_NON_NUMERIC", "R_COST_NEGATIVE"}:
            return "Rechazo por costo inválido"
        if rule_id == "R_DUP_PK_EXACT":
            return "Deduplicación exacta (keep first)"
        if rule_id == "R_EDAD_FILLED_FROM_DERIVADA":
            return "Corrección automática: rellenar edad"
        if rule_id == "R_EDAD_INCONSISTENT_WITH_DERIVED":
            return "Corrección automática: ajustar edad a derivada"
        if rule_id == "R_EDAD_PROVIDED_OUT_OF_RANGE":
            return "Nulificación: edad fuera de rango"
        if rule_id == "R_FECHA_NAC_INVALID":
            return "Nulificación: fecha no parseable"
        if rule_id == "R_EMAIL_INVALID":
            return "Nulificación: email inválido"
        if rule_id == "R_TELEFONO_INVALID_LENGTH":
            return "Nulificación: teléfono fuera de rango"
        if rule_id == "R_FECHA_CITA_FUTURE":
            return "Advertencia: fecha cita futura (registro conservado)"
        return "Evento de auditoría"

    lines: list[str] = []
    lines.append("## Casos no corregidos automáticamente (y por qué)")

    if dup_conflict > 0:
        lines.append("- Duplicados de PK con datos conflictivos: se rechazan para no perder información contradictoria.")
    if cost_non_numeric + cost_negative > 0:
        lines.append("- Registros con costo no numérico o negativo: se rechazan por inviabilidad de corrección sin reglas adicionales.")
    elif cost_non_numeric + cost_negative == 0:
        lines.append("- Regla de costo no numérico/negativo: 0 rechazos en esta ejecución (sin activación en los registros procesados).")
    if orphan > 0:
        lines.append("- Citas huérfanas (FK `id_paciente` no existe): se rechazan porque la tabla puente no tiene entidad padre.")
    if dup_exact > 0:
        lines.append("- Duplicados exactos de PK: se deduplicaron conservando la primera ocurrencia y registrando el evento en auditoría (no se incluyen en `*_rejected.csv`).")
    lines.append("")

    lines.append("## Resultados de auditoría y rechazos")
    for k, v in rejected_counts.items():
        lines.append(f"- {k}: {v}")
    lines.append("")

    lines.append("### Top reglas por número de eventos de auditoría")
    for rule_id, count in top_rules:
        sev = severity_by_rule.get(rule_id)
        if sev:
            lines.append(f"- **{rule_id}**: {count} (severidad principal: {sev})")
        else:
            lines.append(f"- **{rule_id}**: {count}")

    lines.append("")
    lines.append("## Impacto por regla de calidad")
    lines.append("| Regla | Tabla | Campo | Acción | Severidad | Registros |")
    lines.append("|---|---|---|---|---|---|")
    for rule_id, count in top_rules:
        issue = evidence_by_rule.get(rule_id)
        table = getattr(issue, "table", "?")
        column = getattr(issue, "column", None)
        column_txt = str(column) if column else "-"
        sev = severity_by_rule.get(rule_id, "-")
        display_rule_id = rule_id.replace("_", " ")
        lines.append(f"| **{display_rule_id}** | {table} | {column_txt} | {_action(rule_id)} | {sev} | {count} |")

    lines.append("")
    lines.append("## Evidencias (ejemplos controlados)")
    
    _explanations = {
        "R_ORPHAN_CITA": "Registro rechazado íntegramente por romper integridad referencial (el paciente referenciado no existe en la dimensión de pacientes).",
        "R_DUP_PK_CONFLICT": "Rechazado porque existen múltiples registros con la misma llave primaria pero datos contradictorios.",
        "R_COST_NON_NUMERIC": "Rechazado por inviabilidad financiera (costo '{original_value}' no es analizable cuantitativamente).",
        "R_COST_NEGATIVE": "Rechazado por costo numérico negativo o inválido ({original_value}).",
        "R_DUP_PK_EXACT": "Fila descartada por ser 100% idéntica a otra ocurrencia. Se conservó la primera para no duplicar volumetría.",
        "R_EDAD_FILLED_FROM_DERIVADA": "El paciente no tenía registrada la edad, pero sí se conocía el dato original: {original_value}. Se calculó e imputó a {clean_value} comparando la fecha de nacimiento contra la fecha de referencia.",
        "R_EDAD_INCONSISTENT_WITH_DERIVED": "La dupla capturada de edad y nacimiento ({original_value}) se contradice matemáticamente. Se prefirió corregir la edad a {clean_value} respetando la fecha de nacimiento como fuente de la verdad.",
        "R_EDAD_PROVIDED_OUT_OF_RANGE": "La edad excedía límites lógicos de vida humana en este contexto (recibido {original_value}); se nulificó su valor.",
        "R_FECHA_NAC_INVALID": "La fecha provista ('{original_value}') tiene un formato de texto irreconocible o contiene un día/mes matemáticamente imposible en el calendario. Fue nulificada.",
        "R_EMAIL_INVALID": "El valor ingresado ('{original_value}') no cumplió con la estructura regex requerida por un correo electrónico válido; fue nulificado.",
        "R_TELEFONO_INVALID_LENGTH": "La longitud de los dígitos limpios extraídos de '{original_value}' no es válida; fue nulificado.",
        "R_FECHA_CITA_FUTURE": "La fecha de cita '{original_value}' es posterior a la fecha de referencia del pipeline. "
        "El registro se conserva (puede corresponder a una cita programada), pero queda marcado para revisión del equipo clínico.",
    }

    for rule_id in list(evidence_by_rule.keys()):
        issue = evidence_by_rule[rule_id]
        table = getattr(issue, "table", "?")
        row_id = getattr(issue, "row_id", "?")
        column = getattr(issue, "column", None)
        original_value = getattr(issue, "original_value", None)
        clean_value = getattr(issue, "clean_value", None)
        column_txt = f" (columna `{column}`)" if column else ""
        display_rule_id = rule_id.replace("_", " ")
        lines.append(
            f"- **{display_rule_id}** -> {table}{column_txt} (row_id={row_id}): "
            f"original={truncate(original_value)}, limpio={truncate(clean_value)}"
        )
        base_explain = _explanations.get(rule_id, "Aplicación estándar de regla de perfilamiento.")
        explain_txt = base_explain.format(
            original_value=truncate(original_value),
            clean_value=truncate(clean_value),
        )
        lines.append(f"  - _Justificación:_ {explain_txt}")

    return lines

