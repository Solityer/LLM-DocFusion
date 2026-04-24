"""Pydantic models for natural-language document operations."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DocumentOperationRequest(BaseModel):
    file_path: str = ""
    instruction: str = ""
    operation: str = ""
    fields: list[str] = Field(default_factory=list)
    query: str = ""
    replacements: dict[str, str] = Field(default_factory=dict)
    output_format: str = ""
    save_as: str = ""
    use_llm: bool = False
    options: dict[str, Any] = Field(default_factory=dict)


class DocumentOperationResponse(BaseModel):
    status: str = "ok"
    operation: str = ""
    intent: str = ""
    result: Any = None
    output_file: str = ""
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    error: str = ""
