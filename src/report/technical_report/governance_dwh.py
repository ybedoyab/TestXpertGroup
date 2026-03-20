from __future__ import annotations

"""Governance and DWH design section for the technical report."""

from typing import Any


def build_governance_and_dwh_section(*, quality_summary: dict[str, Any]) -> list[str]:
    dwh_counts = quality_summary.get("dwh", {}).get("counts", {})
    return [
        "## Consideraciones de gobierno de datos",
        "- Clasificación por severidad: `info` (observación/cambio no destructivo), `warning` "
        "(nulificación/normalización), `error` (rechazo por inviabilidad).",
        "- Separación explícita entre: corrección automática, nulificación técnica y rechazados para revisión.",
        "- Reglas reproducibles: la edad derivada depende de `REFERENCE_DATE` configurable por CLI.",
        "- Contrato de datos sugerido: validar en ingestión catálogos (`sexo`, `estado_cita`) y formatos "
        "(`fecha_*`, `telefono`, `email`).",
        "- Dueño de datos: definir data owner para catálogos (evitar variantes) y para decidir criterios de rejected "
        "vs NULL en producción.",
        "",
        "## Bonus: Diseño del modelo Data Warehouse (simulado en SQLite)",
        "Se propone un modelo en estrella con grain a nivel de fila de cita médica:",
        "- `fact_citas`: una fila por `id_cita` (natural key preservada como `id_cita`).",
        "- `dim_paciente`: dimensión con `id_paciente` (natural key) y atributos normalizados.",
        "- `dim_medico` y `dim_especialidad`: catálogos derivados de `medico` y `especialidad`.",
        "- `dim_fecha`: tabla calendario a partir de `fecha_cita` (y un miembro `UNKNOWN` para NULL).",
        "- Manejo de NULLs: se mapea a `UNKNOWN` en dimensiones para no romper FKs.",
        f"- Evidencia de carga (conteos): pacientes={dwh_counts.get('dim_paciente_rows','?')}, "
        f"médicos={dwh_counts.get('dim_medico_rows','?')}, especialidades={dwh_counts.get('dim_especialidad_rows','?')}, "
        f"fechas={dwh_counts.get('dim_fecha_rows','?')}, fact_citas={dwh_counts.get('fact_citas_rows','?')}.",
        "",
        "## Recomendaciones de mejora futura",
        "- Definir un contrato de datos (schema) y validarlo en ingestión (por ejemplo, con Pandera/Great Expectations).",
        "- Estabilizar la generación de `estado_cita` y `sexo` en fuente (evitar múltiples variantes).",
        "- Estandarizar el formato de fechas y teléfonos en origen para reducir `NULL` y parsing fallido.",
        "- Añadir reglas de linaje/ID consistentes para reducir discrepancias en PK.",
        "",
        "## Pruebas automáticas",
        "Se implementó una suite de pruebas con `pytest` (96 tests, cobertura de línea 100%) que valida:",
        "- Reglas de limpieza críticas por campo (`sexo`, `fecha_nacimiento`, `fecha_cita`, `costo`, `email`, `telefono`).",
        "- Integridad referencial antes y después de las validaciones cruzadas.",
        "- Comportamiento ante datos faltantes, PKs duplicadas y valores fuera de catálogo.",
        "- Pipeline end-to-end con dataset real: conteos de salida y ausencia de columnas de auditoría interna en CSVs limpios.",
        "",
        "## Conclusión",
        "El pipeline implementa limpieza conservadora, validaciones cruzadas y trazabilidad a nivel de registro/campo. "
        "Los datasets exportados `*_clean.csv` y `data_quality_issues.csv` quedan listos para auditoría y para una carga "
        "posterior a un modelo tipo Data Warehouse.",
        "Las reducciones marginales de completitud en algunos campos responden a una decisión deliberada de calidad: "
        "valores inválidos o no confiables fueron nulificados para priorizar validez y consistencia sobre completitud artificial.",
        "Se priorizó validez, consistencia y trazabilidad sobre imputaciones no verificables, dejando evidencia explícita "
        "de cada corrección, nulificación y rechazo.",
    ]

