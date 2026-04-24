"""Pydantic models for the SQLite document data asset store."""
from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field


class StoreDocumentMeta(BaseModel):
    document_id: str = ""
    source_file: str = ""
    source_name: str = ""
    source_type: str = "file"
    file_type: str = ""
    title: str = ""
    raw_text_hash: str = ""
    created_at: str = ""
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    text_block_count: int = 0
    table_count: int = 0
    entity_count: int = 0
    field_count: int = 0
    quality_issue_count: int = 0


class StoreImportRequest(BaseModel):
    source_files: list[str] = Field(default_factory=list)
    use_llm: bool = True
    extract_entities: bool = True
    overwrite: bool = False


class StoreImportResponse(BaseModel):
    task_id: str = ""
    status: str = "queued"
    current_stage: str = "queued"
    progress: float = 0.0
    message: str = ""
    imported_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    errors: list[str] = Field(default_factory=list)


class StoreTaskStatus(BaseModel):
    task_id: str = ""
    status: str = "pending"
    current_stage: str = "queued"
    progress: float = 0.0
    message: str = ""
    imported_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    errors: list[str] = Field(default_factory=list)
    finished_at: Optional[float] = None


class StoreSearchResult(BaseModel):
    result_type: str = ""  # document | text_block | entity | field
    document_id: str = ""
    source_file: str = ""
    source_name: str = ""
    file_type: str = ""
    snippet: str = ""
    field_name: str = ""
    value: str = ""
    entity_type: str = ""
    confidence: Optional[float] = None
    location: str = ""


class StoreEntityItem(BaseModel):
    id: int = 0
    document_id: str = ""
    entity_text: str = ""
    entity_type: str = ""
    normalized_entity: str = ""
    source_location: str = ""
    confidence: Optional[float] = None
    evidence_snippet: str = ""


class StoreFieldItem(BaseModel):
    id: int = 0
    document_id: str = ""
    field_name: str = ""
    canonical_field: str = ""
    value: str = ""
    normalized_value: str = ""
    field_type: str = ""
    unit: str = ""
    source_location: str = ""
    confidence: Optional[float] = None
    evidence_snippet: str = ""


class StoreQualityItem(BaseModel):
    id: int = 0
    document_id: str = ""
    issue_type: str = ""
    severity: str = ""
    field_name: str = ""
    raw_value: str = ""
    normalized_value: str = ""
    source: str = ""
    location: str = ""
    reason: str = ""
    suggestion: str = ""
    affects_fill: bool = False


class StoreStats(BaseModel):
    document_count: int = 0
    text_block_count: int = 0
    table_count: int = 0
    entity_count: int = 0
    field_count: int = 0
    quality_issue_count: int = 0
    source_type_distribution: dict[str, int] = Field(default_factory=dict)
    file_type_distribution: dict[str, int] = Field(default_factory=dict)
    field_type_distribution: dict[str, int] = Field(default_factory=dict)
    quality_issue_type_distribution: dict[str, int] = Field(default_factory=dict)
    quality_severity_distribution: dict[str, int] = Field(default_factory=dict)
