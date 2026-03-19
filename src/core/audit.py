from __future__ import annotations

"""Audit helpers to append issue records."""

from typing import Any

from src.core.schemas import IssueRecord


def add_issue(
    issues: list[IssueRecord],
    *,
    table: str,
    row_id: Any,
    rule_id: str,
    severity: str,
    column: str | None,
    original_value: Any,
    clean_value: Any,
    detail: str,
) -> None:
    issues.append(
        IssueRecord(
            table=table,
            row_id=row_id,
            rule_id=rule_id,
            severity=severity,
            column=column,
            original_value=original_value,
            clean_value=clean_value,
            detail=detail,
        )
    )

