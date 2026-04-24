"""Pydantic data models for the entire pipeline."""
from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, Field
from enum import Enum


class FileRole(str, Enum):
    SOURCE = "source"
    TEMPLATE = "template"
    UNKNOWN = "unknown"


class StructureType(str, Enum):
    TABULAR = "tabular"
    TEXT = "text"
    MIXED = "mixed"


# ── Normalized Document ──────────────────────────────────────────────
class TextBlock(BaseModel):
    content: str = ""
    heading_level: int = 0  # 0 = normal paragraph
    block_index: int = 0

class TableCell(BaseModel):
    row: int = 0
    col: int = 0
    value: str = ""

class NormalizedTable(BaseModel):
    table_index: int = 0
    sheet_name: str = ""
    headers: list[str] = Field(default_factory=list)
    rows: list[list[str]] = Field(default_factory=list)
    row_count: int = 0
    col_count: int = 0

class DocumentBundle(BaseModel):
    document_id: str = ""
    source_file: str = ""
    source_name: str = ""
    source_type: str = "file"
    canonical_name: str = ""
    file_type: str = ""
    role: FileRole = FileRole.UNKNOWN
    text_blocks: list[TextBlock] = Field(default_factory=list)
    tables: list[NormalizedTable] = Field(default_factory=list)
    raw_text: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Template Schema ──────────────────────────────────────────────────
class TemplateField(BaseModel):
    field_name: str = ""
    location: str = ""  # e.g. "Sheet1!B2", "table0.row1.col2", "paragraph3"
    field_type: str = "text"  # text, number, date, etc.
    required: bool = False
    default_value: str = ""
    placeholder: str = ""

class TemplateTable(BaseModel):
    table_index: int = 0
    sheet_name: str = ""
    headers: list[str] = Field(default_factory=list)
    writable_rows: list[int] = Field(default_factory=list)
    writable_cols: list[int] = Field(default_factory=list)
    row_count: int = 0
    col_count: int = 0
    description: str = ""

class TemplateSection(BaseModel):
    section_index: int = 0
    heading: str = ""
    placeholders: list[str] = Field(default_factory=list)
    content_template: str = ""

class TemplateSchema(BaseModel):
    template_id: str = ""
    source_file: str = ""
    file_type: str = ""
    structure_type: StructureType = StructureType.MIXED
    fields: list[TemplateField] = Field(default_factory=list)
    tables: list[TemplateTable] = Field(default_factory=list)
    sections: list[TemplateSection] = Field(default_factory=list)
    placeholders: list[str] = Field(default_factory=list)
    constraints: dict[str, Any] = Field(default_factory=dict)
    raw_text: str = ""


# ── Requirement Spec ─────────────────────────────────────────────────
class RequirementSpec(BaseModel):
    raw_text: str = ""
    time_range: Optional[list[str]] = None  # [start, end] as strings
    entity_keywords: list[str] = Field(default_factory=list)
    indicator_keywords: list[str] = Field(default_factory=list)
    filters: dict[str, list[str]] = Field(default_factory=dict)
    output_format: str = ""
    output_granularity: str = ""
    strict_matching: bool = False
    sort_limit: Optional[dict[str, Any]] = None  # e.g. {"top_n": 10, "order": "desc"}
    special_notes: list[str] = Field(default_factory=list)
    table_specs: list[dict[str, Any]] = Field(default_factory=list)  # per-table filters
    inferred_from: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# ── Candidate Evidence ───────────────────────────────────────────────
class CandidateEvidence(BaseModel):
    source_file: str = ""
    location: str = ""  # table index / block index / cell ref
    raw_snippet: str = ""
    match_reason: str = ""
    confidence: Optional[float] = None


class ModelUsageSummary(BaseModel):
    provider: str = ""
    model: str = ""
    called: bool = False
    model_not_used: bool = False
    total_calls: int = 0
    successful_calls: int = 0
    per_stage: dict[str, int] = Field(default_factory=dict)
    per_source: dict[str, int] = Field(default_factory=dict)
    per_source_stage: dict[str, dict[str, int]] = Field(default_factory=dict)
    per_template_source_stage: dict[str, dict[str, dict[str, int]]] = Field(default_factory=dict)
    per_template: dict[str, int] = Field(default_factory=dict)
    probe_sources: list[str] = Field(default_factory=list)
    probe_source_calls: dict[str, int] = Field(default_factory=dict)
    fallback_reasons: list[str] = Field(default_factory=list)
    trace_file: str = ""
    sample_trace: dict[str, Any] = Field(default_factory=dict)
    skip_events: list[dict[str, Any]] = Field(default_factory=list)
    required_calls: list[dict[str, Any]] = Field(default_factory=list)
    missing_required_calls: list[dict[str, Any]] = Field(default_factory=list)
    validation_errors: list[str] = Field(default_factory=list)
    degraded: bool = False
    availability_status: str = "unknown"


class SourceProcessingStat(BaseModel):
    source_file: str = ""
    file_type: str = ""
    text_blocks: int = 0
    tables: int = 0
    entity_blocks_detected: int = 0
    relevant_to_template: bool = False
    relevance_score: float = 0.0
    extracted_records: int = 0
    filtered_records: int = 0
    contributed_records: int = 0
    contributed_fields: int = 0
    evidence_contribution_fields: int = 0
    value_contribution_fields: int = 0
    row_contribution_records: int = 0
    effective_cell_delta: int = 0
    effective_row_delta: int = 0
    qwen_used: bool = False
    qwen_call_count: int = 0
    qwen_stages: list[str] = Field(default_factory=list)
    stage_audit: dict[str, int] = Field(default_factory=dict)
    narrative_audit: dict[str, Any] = Field(default_factory=dict)
    contribution_templates: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# ── Filled Result ────────────────────────────────────────────────────
class FilledFieldResult(BaseModel):
    field_name: str = ""
    canonical_field: str = ""
    target_location: str = ""
    value: Any = None
    normalized_value: Any = None
    unit: str = ""
    evidence: list[CandidateEvidence] = Field(default_factory=list)
    confidence: Optional[float] = None
    source_file: str = ""
    supporting_sources: list[str] = Field(default_factory=list)
    value_sources: list[str] = Field(default_factory=list)
    value_record_ids: list[str] = Field(default_factory=list)
    match_method: str = ""
    missing_reason: str = ""
    quality_flags: list[str] = Field(default_factory=list)

class ValidationItem(BaseModel):
    check: str = ""
    passed: bool = True
    message: str = ""

class FilledResult(BaseModel):
    template_id: str = ""
    template_file: str = ""
    output_file: str = ""
    status: str = "pending"
    meets_minimum: bool = False
    model_usage: Optional[ModelUsageSummary] = None
    filled_fields: list[FilledFieldResult] = Field(default_factory=list)
    unresolved_fields: list[str] = Field(default_factory=list)
    evidence_report: list[CandidateEvidence] = Field(default_factory=list)
    validation_report: list[ValidationItem] = Field(default_factory=list)
    source_stats: list[SourceProcessingStat] = Field(default_factory=list)
    effective_contribution_audit: dict[str, Any] = Field(default_factory=dict)
    entity_legality_report: dict[str, Any] = Field(default_factory=dict)
    quality_report: dict[str, Any] = Field(default_factory=dict)
    fusion_report: dict[str, Any] = Field(default_factory=dict)
    normalization_report: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    timing: dict[str, float] = Field(default_factory=dict)
    metric_definitions: dict[str, str] = Field(default_factory=dict)
    fill_rate: float = 0.0
    rows_filled: int = 0
    record_count: int = 0
    expected_rows: int = 0


class TemplateProcessingStatus(BaseModel):
    template_file: str = ""
    status: str = "pending"
    current_stage: str = "pending"
    records_extracted: int = 0
    output_file: str = ""
    warnings: list[str] = Field(default_factory=list)
    error: str = ""


# ── API Models ───────────────────────────────────────────────────────
class ProcessRequest(BaseModel):
    source_files: list[str] = Field(default_factory=list)
    template_files: list[str] = Field(default_factory=list)
    requirement: str = ""
    options: dict[str, Any] = Field(default_factory=dict)

class ProcessResponse(BaseModel):
    task_id: str = ""
    status: str = "pending"
    current_stage: str = "pending"
    stage_message: str = ""
    progress: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0
    updated_at: float = 0.0
    results: list[FilledResult] = Field(default_factory=list)
    template_statuses: list[TemplateProcessingStatus] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)
    error: str = ""
    auto_requirement: str = ""   # Auto-inferred requirement (shown to user when input was empty)
    requirement_spec: Optional[RequirementSpec] = None
    model_usage: Optional[ModelUsageSummary] = None
    latest_output_dir: str = ""
    warnings: list[str] = Field(default_factory=list)  # Pipeline-level warnings
    source_quality_report: dict[str, Any] = Field(default_factory=dict)
    fusion_report: dict[str, Any] = Field(default_factory=dict)
    normalization_report: dict[str, Any] = Field(default_factory=dict)
    report_summary: dict[str, Any] = Field(default_factory=dict)
    source_summary: dict[str, Any] = Field(default_factory=dict)

class HealthResponse(BaseModel):
    status: str = "ok"
    provider: str = "ollama"
    ollama_status: str = "unknown"
    model: str = ""
    version: str = "1.0.0"
