from __future__ import annotations

"""Schemas and canonical catalogs shared across the ETL."""

from dataclasses import dataclass
from typing import Any, Literal

Severity = Literal["info", "warning", "error"]

SexoCanonical = Literal["M", "F"]

EstadoCitaCanonical = Literal["Completada", "Cancelada", "Reprogramada"]

CATALOG_SEXO: dict[str, SexoCanonical] = {
    "m": "M",
    "male": "M",
    "hombre": "M",
    "f": "F",
    "female": "F",
    "mujer": "F",
}

CATALOG_ESTADO_CITA: dict[str, EstadoCitaCanonical] = {
    "completada": "Completada",
    "cancelada": "Cancelada",
    "reprogramada": "Reprogramada",
}


@dataclass(frozen=True)
class IssueRecord:
    table: str
    row_id: Any
    rule_id: str
    severity: Severity
    column: str | None
    original_value: Any
    clean_value: Any
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "row_id": self.row_id,
            "rule_id": self.rule_id,
            "severity": self.severity,
            "column": self.column,
            "original_value": self.original_value,
            "clean_value": self.clean_value,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ProfilingReport:
    table: str
    row_count: int
    duplicate_counts: dict[str, Any]
    missing_counts: dict[str, Any]
    format_anomalies: dict[str, Any]
    categorical_cardinality: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "row_count": self.row_count,
            "duplicate_counts": self.duplicate_counts,
            "missing_counts": self.missing_counts,
            "format_anomalies": self.format_anomalies,
            "categorical_cardinality": self.categorical_cardinality,
        }


@dataclass(frozen=True)
class QualityMetrics:
    global_metrics: dict[str, Any]
    table_metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"global_metrics": self.global_metrics, "table_metrics": self.table_metrics}


def expected_columns_pacientes() -> list[str]:
    return [
        "id_paciente",
        "nombre",
        "fecha_nacimiento",
        "edad",
        "sexo",
        "email",
        "telefono",
        "ciudad",
    ]


def expected_columns_citas_medicas() -> list[str]:
    return [
        "id_cita",
        "id_paciente",
        "fecha_cita",
        "especialidad",
        "medico",
        "costo",
        "estado_cita",
    ]


def required_columns_for_minimal_schema() -> dict[str, list[str]]:
    return {
        "pacientes": ["id_paciente", "nombre", "fecha_nacimiento", "sexo"],
        "citas_medicas": ["id_cita", "id_paciente", "fecha_cita", "estado_cita", "costo"],
    }


def pk_column_pacientes() -> str:
    return "id_paciente"


def pk_column_citas() -> str:
    return "id_cita"

