"""Pydantic models for data quality detection."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class QualityIssue(BaseModel):
    issue_type: str = ""
    severity: str = "warning"
    field_name: str = ""
    raw_value: Any = None
    normalized_value: Any = None
    source: str = ""
    location: str = ""
    reason: str = ""
    suggestion: str = ""
    affects_fill: bool = False
    confidence: float | None = None
    evidence: list[dict[str, Any]] = Field(default_factory=list)


class QualityReport(BaseModel):
    issues: list[QualityIssue] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
