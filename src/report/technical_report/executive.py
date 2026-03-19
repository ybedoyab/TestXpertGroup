from __future__ import annotations

"""Executive summary builder for the technical report."""

from typing import Any


def build_executive_section(*, quality_summary: dict[str, Any], rejected_counts: dict[str, int]) -> list[str]:
    gb_before = quality_summary["global_before"]
    gb_after = quality_summary["global_after"]
    delta = quality_summary["improvement"]["completeness_global_delta"]
    completeness_before = gb_before["completeness_global_pct"] * 100
    completeness_after = gb_after["completeness_global_pct"] * 100
    delta_pp = delta * 100 if delta is not None else 0
    fk_before = gb_before["referential_integrity_fk_valid_pct_among_non_null"] * 100
    fk_after = gb_after["referential_integrity_fk_valid_pct_among_non_null"] * 100

    return [
        "## Resumen ejecutivo",
        f"- Completitud global (pct no nulos): {completeness_before:.2f}% -> {completeness_after:.2f}% "
        f"(delta {delta_pp:+.2f} pp).",
        "- Integridad referencial FK (pct válido entre no-nulos): "
        f"{fk_before:.2f}% -> {fk_after:.2f}%.",
        f"- Rechazos: pacientes={rejected_counts.get('pacientes_rejected_rows', 0)} / "
        f"citas={rejected_counts.get('citas_medicas_rejected_rows', 0)}.",
        "",
    ]

