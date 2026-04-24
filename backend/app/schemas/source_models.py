"""Pydantic models for external and multi-source ingestion."""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    LOCAL_FILE = "local_file"
    HTTP_API = "http_api"
    WEB_PAGE = "web_page"
    DATABASE = "database"


class SourceSpec(BaseModel):
    """Generic source descriptor used by preview and multisource processing APIs."""

    source_type: SourceType = SourceType.LOCAL_FILE
    name: str = ""
    path: str = ""
    url: str = ""
    method: str = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    timeout: float = 20.0
    database_type: str = "sqlite"
    connection_string: str = ""
    database_path: str = ""
    query: str = ""
    priority: int = 0
    options: dict[str, Any] = Field(default_factory=dict)


class SourcePreviewRequest(BaseModel):
    source: Optional[SourceSpec] = None
    sources: list[SourceSpec] = Field(default_factory=list)
    max_rows: int = 20


class SourcePreviewItem(BaseModel):
    status: str = "ok"
    source_name: str = ""
    source_type: str = ""
    document_id: str = ""
    file_type: str = ""
    text_blocks: int = 0
    tables: int = 0
    table_previews: list[dict[str, Any]] = Field(default_factory=list)
    raw_text_preview: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    error: str = ""


class SourcePreviewResponse(BaseModel):
    status: str = "ok"
    source_types: list[dict[str, Any]] = Field(default_factory=list)
    previews: list[SourcePreviewItem] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class ProcessMultisourceRequest(BaseModel):
    sources: list[SourceSpec] = Field(default_factory=list)
    source_files: list[str] = Field(default_factory=list)
    template_files: list[str] = Field(default_factory=list)
    requirement: str = ""
    options: dict[str, Any] = Field(default_factory=dict)
