"""Pipeline service - orchestrates the full document processing flow."""
from __future__ import annotations

import json
import os
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Optional

from ..core.config import OUTPUT_DIR
from ..core.logging import logger
from ..schemas.models import (
    DocumentBundle,
    FileRole,
    FilledResult,
    FilledFieldResult,
    ModelUsageSummary,
    ProcessResponse,
    RequirementSpec,
    SourceProcessingStat,
    TemplateProcessingStatus,
)
from ..utils.entity_utils import describe_entity_reason
from ..utils.text_utils import clean_cell_value
from .cleanup_service import cleanup_runtime_artifacts
from .document_service import read_document
from .extraction_service import _is_narrative_source, extract_data
from .fill_service import fill_template
from .fusion_service import build_extraction_fusion_report, fuse_document_tables
from .ollama_service import get_ollama_service
from .quality_service import analyze_documents, analyze_filled_result, quality_report_to_dict
from .report_service import build_task_report
from .requirement_service import auto_infer_requirement, parse_requirement
from .retrieval_service import retrieve_evidence
from .template_service import parse_template
from .validation_service import apply_validation_outcome, validate_result


ProgressCallback = Callable[[dict[str, Any]], None]


def _model_usage_snapshot(
    task_id: str,
    *,
    reset: bool = False,
    finalize: bool = False,
    trace_file: str = "",
) -> ModelUsageSummary:
    """Read or reset the shared qwen usage ledger for the current task."""
    ollama = get_ollama_service()
    if reset:
        ollama.reset_usage(task_id=task_id, trace_file=trace_file)
    snapshot = ollama.finalize_usage(task_id=task_id) if finalize else ollama.snapshot_usage(task_id=task_id)
    return ModelUsageSummary(**snapshot)


def process_documents(
    source_files: list[str],
    template_files: list[str],
    user_requirement: str = "",
    options: Optional[dict] = None,
    task_id: str | None = None,
    progress_callback: Optional[ProgressCallback] = None,
    source_bundles: Optional[list[DocumentBundle]] = None,
) -> ProcessResponse:
    """Run the full read -> infer -> retrieve -> extract -> fill -> validate pipeline."""
    task_id = task_id or str(uuid.uuid4())[:8]
    options = options or {}
    use_llm = bool(options.get("use_llm", True))
    strict_mode = bool(options.get("strict_mode", False))
    enable_quality_detection = bool(options.get("enable_quality_detection", True))
    enable_data_fusion = bool(options.get("enable_data_fusion", True))
    keep_upload_dir = options.get("keep_upload_dir")
    keep_upload_dir = Path(keep_upload_dir) if keep_upload_dir else None
    keep_upload_dirs = [
        Path(path)
        for path in options.get("keep_upload_dirs", [])
        if path
    ]
    trace_file = str(OUTPUT_DIR / f"model_trace_{task_id}.json")

    response = ProcessResponse(
        task_id=task_id,
        status="processing",
        current_stage="queued",
        stage_message="任务已启动",
        progress=0.0,
        started_at=time.time(),
        updated_at=time.time(),
        latest_output_dir=str(OUTPUT_DIR),
        template_statuses=[
            TemplateProcessingStatus(template_file=template_file, status="pending", current_stage="pending")
            for template_file in template_files
        ],
        model_usage=_model_usage_snapshot(task_id, reset=True, trace_file=trace_file),
    )
    logs: list[str] = []
    warnings: list[str] = []
    for connector_error in options.get("source_connector_errors", []) or []:
        warnings.append(str(connector_error))
        logs.append(str(connector_error))

    start_message = f"[{task_id}] 启动处理流程: {len(source_files)} 个 source, {len(template_files)} 个 template"
    _log_and_emit(logs, start_message, progress_callback, stage="queued", progress=0.0)

    try:
        _log_and_emit(
            logs,
            "清理历史输出、旧上传目录和缓存",
            progress_callback,
            stage="cleanup",
            message="清理历史输出与缓存",
            progress=0.03,
            status="processing",
            latest_output_dir=str(OUTPUT_DIR),
            model_usage=_model_usage_snapshot(task_id),
        )
        cleanup_runtime_artifacts(keep_upload_dir=keep_upload_dir, keep_upload_dirs=keep_upload_dirs)

        documents = _read_source_documents(source_files, logs, warnings, progress_callback, source_bundles=source_bundles)
        if not documents:
            raise RuntimeError("没有可用的数据源文件，任务终止")
        source_quality_report = {}
        source_fusion_report = {}
        if enable_quality_detection:
            _emit_progress(
                progress_callback,
                stage="parse",
                message="识别数据质量问题",
                progress=0.17,
                log="开始执行数据质量识别",
            )
            source_quality_report = quality_report_to_dict(analyze_documents(documents))
            response.source_quality_report = source_quality_report
            logs.append(
                f"数据质量识别完成: {source_quality_report.get('summary', {}).get('issue_count', 0)} 个问题"
            )
        if enable_data_fusion:
            _emit_progress(
                progress_callback,
                stage="parse",
                message="生成多源融合概览",
                progress=0.18,
                log="开始执行多源融合概览",
            )
            source_fusion_report = fuse_document_tables(documents)
            response.fusion_report = source_fusion_report
            logs.append(
                f"多源融合概览: raw={source_fusion_report.get('summary', {}).get('raw_records', 0)}, "
                f"fused={source_fusion_report.get('summary', {}).get('fused_records', 0)}, "
                f"conflicts={source_fusion_report.get('summary', {}).get('conflict_count', 0)}"
            )

        if user_requirement and user_requirement.strip():
            requirement, auto_requirement = _resolve_requirement(
                user_requirement,
                source_files,
                template_files,
                strict_mode,
                use_llm,
                task_id,
                logs,
                warnings,
                progress_callback,
            )
            response.requirement_spec = requirement
            response.auto_requirement = auto_requirement
            response.model_usage = _model_usage_snapshot(task_id)
        else:
            requirement = None
            auto_requirement = ""
            logs.append("requirement 为空，将按模板逐个自动推断，避免多模板任务出现 requirement 串扰")
            _emit_progress(
                progress_callback,
                stage="requirement",
                message="requirement 为空，后续将按模板逐个自动推断",
                progress=0.21,
                auto_requirement="",
                requirement_spec=None,
                model_usage=_model_usage_snapshot(task_id),
                log="已切换为模板级 requirement 推断模式",
            )

        results: list[FilledResult] = []
        template_span = 0.72 / max(len(template_files), 1)
        for template_index, template_path in enumerate(template_files):
            template_status = response.template_statuses[template_index]
            template_name = Path(template_path).name
            template_status.status = "processing"
            template_status.current_stage = "template"
            _emit_progress(
                progress_callback,
                stage="template",
                status="processing",
                message=f"开始处理模板 {template_name}",
                progress=0.22 + template_index * template_span,
                template_file=template_path,
                template_status="processing",
                template_stage="template",
                log=f"开始处理模板: {template_name}",
            )

            try:
                if requirement is None:
                    template_requirement, template_auto_requirement = _resolve_requirement(
                        "",
                        source_files,
                        [template_path],
                        strict_mode,
                        use_llm,
                        task_id,
                        logs,
                        warnings,
                        progress_callback,
                    )
                    response.requirement_spec = template_requirement
                    response.auto_requirement = template_auto_requirement
                    response.model_usage = _model_usage_snapshot(task_id)
                else:
                    template_requirement = requirement.model_copy(deep=True)
                    template_auto_requirement = auto_requirement

                result = _process_single_template(
                    documents=documents,
                    template_path=template_path,
                    requirement=template_requirement,
                    auto_requirement=template_auto_requirement,
                    use_llm=use_llm,
                    task_id=task_id,
                    logs=logs,
                    progress_callback=progress_callback,
                    template_index=template_index,
                    template_count=len(template_files),
                    source_quality_report=source_quality_report,
                    enable_quality_detection=enable_quality_detection,
                    enable_data_fusion=enable_data_fusion,
                )
                results.append(result)
                template_status.status = "completed" if result.status == "completed" else "error"
                template_status.current_stage = "completed" if result.status == "completed" else "failed"
                template_status.records_extracted = result.record_count
                template_status.output_file = result.output_file
                for warning in result.warnings:
                    if warning not in template_status.warnings:
                        template_status.warnings.append(warning)
                if result.status != "completed" and result.warnings:
                    template_status.error = result.warnings[-1]
                _emit_progress(
                    progress_callback,
                    stage="output",
                    message=f"模板 {template_name} 已完成",
                    progress=min(0.93, 0.22 + (template_index + 1) * template_span),
                    template_file=template_path,
                    template_status=template_status.status,
                    template_stage="output" if result.status == "completed" else "failed",
                    template_output_file=result.output_file,
                    records_extracted=result.record_count,
                    result=result,
                    log=f"模板完成: {template_name}，输出 {Path(result.output_file).name if result.output_file else '无'}",
                )
            except Exception as exc:
                error_text = f"模板 {template_name} 处理失败: {exc}"
                logger.error(error_text, exc_info=True)
                warnings.append(error_text)
                template_status.status = "error"
                template_status.current_stage = "failed"
                template_status.error = str(exc)
                template_status.warnings.append(str(exc))
                results.append(FilledResult(template_file=template_path, status="error", warnings=[str(exc)]))
                _emit_progress(
                    progress_callback,
                    stage="validate",
                    message=error_text,
                    progress=min(0.93, 0.22 + (template_index + 1) * template_span),
                    template_file=template_path,
                    template_status="error",
                    template_stage="failed",
                    template_error=str(exc),
                    warning=error_text,
                    log=error_text,
                )

        _aggregate_pipeline_warnings(results, warnings, logs)

        successful_results = [item for item in results if item.status == "completed" and item.output_file]
        response.status = "completed" if successful_results else "error"
        response.current_stage = "completed" if response.status == "completed" else "failed"
        if response.status == "completed" and len(successful_results) != len(results):
            response.stage_message = "任务结束，部分模板成功，部分模板失败"
        else:
            response.stage_message = "全部模板处理完成" if response.status == "completed" else "全部模板处理失败"
        response.progress = 1.0 if response.status == "completed" else 0.99
        response.finished_at = time.time()
        response.updated_at = response.finished_at
        response.results = results
        response.logs = logs
        response.warnings = _deduplicate_text(warnings)
        response.latest_output_dir = str(OUTPUT_DIR)
        response.model_usage = _model_usage_snapshot(task_id, finalize=True)
        response.source_quality_report = source_quality_report if enable_quality_detection else {}
        response.fusion_report = source_fusion_report if enable_data_fusion else {}
        response.normalization_report = _build_normalization_report(results)
        response.report_summary = build_task_report(response)

        _emit_progress(
            progress_callback,
            stage=response.current_stage,
            status=response.status,
            message=response.stage_message,
            progress=response.progress,
            latest_output_dir=response.latest_output_dir,
            requirement_spec=response.requirement_spec,
            auto_requirement=response.auto_requirement,
            model_usage=response.model_usage,
            log=f"任务完成，总耗时 {response.finished_at - response.started_at:.1f}s",
        )
        return response
    except Exception as exc:
        logger.error("Pipeline error: %s", exc, exc_info=True)
        response.status = "error"
        response.current_stage = "failed"
        response.stage_message = str(exc)
        response.error = str(exc)
        response.progress = min(response.progress or 0.0, 0.99)
        response.finished_at = time.time()
        response.updated_at = response.finished_at
        response.logs = logs
        response.warnings = _deduplicate_text(warnings)
        response.model_usage = _model_usage_snapshot(task_id, finalize=True)
        _emit_progress(
            progress_callback,
            stage="failed",
            status="error",
            message=str(exc),
            progress=response.progress,
            model_usage=response.model_usage,
            warning=str(exc),
            log=f"任务失败: {exc}",
        )
        return response


def _read_source_documents(
    source_files: list[str],
    logs: list[str],
    warnings: list[str],
    progress_callback: Optional[ProgressCallback],
    source_bundles: Optional[list[DocumentBundle]] = None,
) -> list[DocumentBundle]:
    """Read all source files and keep the readable ones."""
    documents: list[DocumentBundle] = list(source_bundles or [])
    for bundle in documents:
        logs.append(
            f"已接入外部 source {Path(bundle.source_file).name}: {bundle.file_type}, "
            f"{len(bundle.text_blocks)} 个文本块, {len(bundle.tables)} 张表"
        )
    for index, source_file in enumerate(source_files):
        source_name = Path(source_file).name
        _emit_progress(
            progress_callback,
            stage="parse",
            message=f"解析数据源 {source_name}",
            progress=0.05 + (index / max(len(source_files), 1)) * 0.12,
            log=f"解析数据源: {source_name}",
        )
        try:
            document = read_document(source_file, FileRole.SOURCE)
            documents.append(document)
            logs.append(
                f"已读取 source {source_name}: {document.file_type}, {len(document.text_blocks)} 个文本块, "
                f"{len(document.tables)} 张表, 标题 {document.metadata.get('title', source_name)[:60]}"
            )
        except Exception as exc:
            warning = f"读取 source 失败 {source_name}: {exc}"
            warnings.append(warning)
            logs.append(warning)
            logger.warning(warning)
    return documents


def _resolve_requirement(
    user_requirement: str,
    source_files: list[str],
    template_files: list[str],
    strict_mode: bool,
    use_llm: bool,
    task_id: str,
    logs: list[str],
    warnings: list[str],
    progress_callback: Optional[ProgressCallback],
) -> tuple[RequirementSpec, str]:
    """Parse or auto-infer the requirement."""
    _emit_progress(
        progress_callback,
        stage="requirement",
        message="解析 requirement",
        progress=0.19,
        log="开始解析 requirement",
    )

    auto_requirement = ""
    if user_requirement and user_requirement.strip():
        requirement = parse_requirement(user_requirement, strict_mode=strict_mode)
        logs.append(f"使用显式 requirement，实体关键词 {len(requirement.entity_keywords)} 个")
        get_ollama_service().note_skip(
            "已提供显式 requirement，未触发 qwen 自动推断",
            {
                "stage": "requirement",
                "source_files": source_files,
                "template_files": template_files,
                "task_id": task_id,
            },
        )
    else:
        requirement, auto_requirement, infer_warnings = auto_infer_requirement(
            template_files,
            source_files,
            use_llm=use_llm,
            usage_context={
                "stage": "requirement",
                "source_files": source_files,
                "template_files": template_files,
                "task_id": task_id,
            },
        )
        if strict_mode:
            requirement.strict_matching = True
        warnings.extend(infer_warnings)
        if infer_warnings:
            logs.extend(infer_warnings)
        logs.append(f"自动识别 requirement: {(auto_requirement or '无').replace(chr(10), ' | ')[:220]}")
        if not auto_requirement:
            warnings.append("requirement 自动识别失败，已回退到最小可用规则")
        _emit_progress(
            progress_callback,
            stage="requirement",
            message="已完成 requirement 自动识别",
            progress=0.21,
            auto_requirement=auto_requirement,
            requirement_spec=requirement,
            model_usage=_model_usage_snapshot(task_id),
            log="已生成自动 requirement",
        )

    requirement.strict_matching = bool(requirement.strict_matching or strict_mode)
    logs.append(f"时间范围: {requirement.time_range}")
    logs.append(f"实体关键词: {requirement.entity_keywords[:8]}")
    logs.append(f"指标关键词: {requirement.indicator_keywords[:12]}")
    logs.append(f"表级过滤规则: {len(requirement.table_specs)}")
    return requirement, auto_requirement


def _process_single_template(
    documents: list[DocumentBundle],
    template_path: str,
    requirement: RequirementSpec,
    auto_requirement: str,
    use_llm: bool,
    task_id: str,
    logs: list[str],
    progress_callback: Optional[ProgressCallback],
    template_index: int,
    template_count: int,
    source_quality_report: dict[str, Any] | None = None,
    enable_quality_detection: bool = True,
    enable_data_fusion: bool = True,
) -> FilledResult:
    """Process a single template through the pipeline."""
    started_at = time.time()
    template_name = Path(template_path).name
    progress_base = 0.22 + template_index * (0.72 / max(template_count, 1))
    progress_span = 0.72 / max(template_count, 1)

    logs.append(f"处理模板 {template_name}")
    logs.append(
        f"模板 requirement: {(auto_requirement or requirement.raw_text or '无').replace(chr(10), ' | ')[:220]}"
    )
    _emit_progress(
        progress_callback,
        stage="template",
        message=f"解析模板 {template_name}",
        progress=progress_base + progress_span * 0.05,
        template_file=template_path,
        template_status="processing",
        template_stage="template",
    )
    template = parse_template(template_path)
    logs.append(
        f"模板结构: {len(template.tables)} 张表, {len(template.fields)} 个字段, 类型 {template.structure_type.value}"
    )

    retrieve_start = time.time()
    _emit_progress(
        progress_callback,
        stage="retrieve",
        message=f"检索证据 {template_name}",
        progress=progress_base + progress_span * 0.25,
        template_file=template_path,
        template_status="processing",
        template_stage="retrieve",
        log=f"模板 {template_name}: 开始检索证据",
    )
    retrieval = retrieve_evidence(
        documents,
        template,
        requirement,
        use_llm=use_llm,
        usage_context={
            "stage": "retrieve",
            "task_id": task_id,
            "template_file": template_path,
            "source_files": [document.source_file for document in documents],
        },
    )
    retrieve_duration = time.time() - retrieve_start
    logs.append(
        f"证据检索完成: {len(retrieval.table_candidates)} 个表候选, {len(retrieval.text_candidates)} 个文本候选"
    )

    extract_start = time.time()
    _emit_progress(
        progress_callback,
        stage="extract",
        message=f"抽取数据 {template_name}",
        progress=progress_base + progress_span * 0.48,
        template_file=template_path,
        template_status="processing",
        template_stage="extract",
        log=f"模板 {template_name}: 开始抽取数据",
    )
    extracted = extract_data(
        retrieval,
        template,
        requirement,
        use_llm=use_llm,
        usage_context={
            "stage": "extract",
            "task_id": task_id,
            "template_file": template_path,
        },
    )
    extract_duration = time.time() - extract_start
    record_count = sum(len(item.get("records", [])) for item in extracted)
    template_target_rows = sum(len(table.writable_rows) for table in template.tables if table.writable_rows)
    expected_rows = template_target_rows or max((item.get("entity_estimate", 0) for item in extracted), default=0)
    logs.append(f"抽取完成: 共 {record_count} 条记录, 预估实体数 {expected_rows}")
    for table_data in extracted:
        logs.append(
            f"  表 {table_data.get('table_index', 0)}: {len(table_data.get('records', []))} 条, "
            f"来源 {len([k for k, v in table_data.get('source_counts', {}).items() if v > 0])} 个 source, "
            f"方式 {table_data.get('extraction_method', 'unknown')}"
        )
        logs.extend(table_data.get("warnings", []))

    fill_start = time.time()
    output_name = f"{Path(template_path).stem}_filled_{task_id}_{template_index + 1}{Path(template_path).suffix}"
    output_path = str(OUTPUT_DIR / output_name)
    _emit_progress(
        progress_callback,
        stage="fill",
        message=f"回写模板 {template_name}",
        progress=progress_base + progress_span * 0.72,
        template_file=template_path,
        template_status="processing",
        template_stage="fill",
        log=f"模板 {template_name}: 写入输出文件",
    )
    filled = fill_template(template, extracted, output_path)
    fill_duration = time.time() - fill_start
    filled.template_id = template.template_id or Path(template_path).stem
    filled = _finalize_result_metrics(filled, record_count=record_count, expected_rows=expected_rows)
    filled.timing = {
        "retrieve": retrieve_duration,
        "extract": extract_duration,
        "fill": fill_duration,
        "validate": 0.0,
        "total": time.time() - started_at,
    }
    filled.model_usage = _template_model_usage_snapshot(task_id, template_path)
    filled.effective_contribution_audit = _build_effective_contribution_audit(
        documents=documents,
        template=template,
        requirement=requirement,
        use_llm=use_llm,
        extracted=extracted,
        filled=filled,
    )
    filled.source_stats = _build_source_stats(
        documents,
        retrieval,
        extracted,
        filled,
        template_name,
        model_usage=filled.model_usage,
    )
    if enable_data_fusion:
        filled.fusion_report = build_extraction_fusion_report(extracted)
    filled.normalization_report = _build_result_normalization_report(extracted)
    _finalize_effective_contribution_audit_flags(filled)
    logs.append(
        f"回写完成: 填充率 {filled.fill_rate:.1f}%, 写入 {filled.rows_filled} 行, 输出 {Path(output_path).name}"
    )

    _emit_progress(
        progress_callback,
        stage="validate",
        message=f"验证输出 {template_name}",
        progress=progress_base + progress_span * 0.88,
        template_file=template_path,
        template_status="processing",
        template_stage="validate",
        records_extracted=filled.record_count,
        template_output_file=filled.output_file,
        log=f"模板 {template_name}: 开始验证输出",
    )
    validate_start = time.time()
    filled = validate_result(filled)
    if enable_quality_detection:
        filled.quality_report = quality_report_to_dict(
            analyze_filled_result(
                filled,
                source_quality_report if source_quality_report else None,
            )
        )
    filled.timing["validate"] = time.time() - validate_start
    filled.timing["total"] = time.time() - started_at
    _refresh_response_time_validation(filled)
    _emit_progress(
        progress_callback,
        stage="output",
        message=f"生成输出 {template_name}",
        progress=progress_base + progress_span * 0.95,
        template_file=template_path,
        template_status="completed" if filled.status == "completed" else "error",
        template_stage="output" if filled.status == "completed" else "failed",
        template_output_file=filled.output_file,
        records_extracted=filled.record_count,
        result=filled,
        model_usage=filled.model_usage,
        log=(
            f"模板 {template_name}: 已输出 {Path(filled.output_file).name}"
            if filled.output_file else
            f"模板 {template_name}: 未保留输出文件"
        ),
    )
    passed_checks = sum(1 for item in filled.validation_report if item.passed)
    logs.append(f"验证完成: {passed_checks}/{len(filled.validation_report)} 项通过")

    return filled


def _ordered_unique(values: list[str]) -> list[str]:
    """Deduplicate short string lists while preserving their order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = clean_cell_value(str(value or ""))
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _field_visible_value(field: FilledFieldResult | None) -> str:
    """Normalize one filled field into the visible workbook/text value used for delta comparisons."""
    if field is None:
        return ""
    value = field.normalized_value if field.normalized_value not in (None, "") else field.value
    return clean_cell_value(str(value or ""))


def _field_has_visible_value(field: FilledFieldResult | None) -> bool:
    """Return True when the filled field has a visible non-empty value."""
    return _field_visible_value(field) not in {"", "N/A"}


def _filled_field_key(field: FilledFieldResult) -> tuple[str, str]:
    """Use target cell/location + field name as the cross-run comparison key."""
    return field.target_location, field.field_name


def _filled_field_map(filled: FilledResult) -> dict[tuple[str, str], FilledFieldResult]:
    """Index one filled result by target location and field name."""
    return {
        _filled_field_key(field): field
        for field in filled.filled_fields
    }


def _field_value_sources(field) -> set[str]:
    """Collect the source(s) that actually supplied the chosen final value."""
    explicit = {
        source_file
        for source_file in getattr(field, "value_sources", [])
        if source_file
    }
    if explicit:
        return explicit
    if getattr(field, "source_file", ""):
        return {field.source_file}
    return _field_supporting_sources(field)


def _field_value_record_ids(field) -> set[str]:
    """Collect raw narrative record ids that supplied the chosen final value."""
    return {
        record_id
        for record_id in getattr(field, "value_record_ids", [])
        if record_id
    }


def _source_contribution_counters(
    filled: FilledResult,
) -> tuple[Counter[str], Counter[str], dict[str, set[str]]]:
    """Split evidence support from actual chosen-value support on final written fields."""
    evidence_counts: Counter[str] = Counter()
    value_counts: Counter[str] = Counter()
    row_counts: dict[str, set[str]] = {}
    for field in filled.filled_fields:
        if not _field_has_visible_value(field):
            continue
        row_identifier = _field_row_identifier(field.target_location, field.field_name)
        for source_file in _field_supporting_sources(field):
            evidence_counts[source_file] += 1
        for source_file in _field_value_sources(field):
            value_counts[source_file] += 1
            if row_identifier:
                row_counts.setdefault(source_file, set()).add(row_identifier)
    return evidence_counts, value_counts, row_counts


def _simulate_filled_result_for_audit(
    documents: list[DocumentBundle],
    template,
    requirement: RequirementSpec,
    use_llm: bool,
) -> tuple[FilledResult, list[dict]]:
    """Re-run a smaller baseline fill so we can measure true multi-source cell deltas."""
    retrieval = retrieve_evidence(
        documents,
        template,
        requirement,
        use_llm=use_llm,
        usage_context={"stage": "audit_baseline", "template_file": template.source_file},
    )
    extracted = extract_data(
        retrieval,
        template,
        requirement,
        use_llm=use_llm,
        usage_context={"stage": "audit_baseline", "template_file": template.source_file},
    )
    record_count = sum(len(item.get("records", [])) for item in extracted)
    expected_rows = (
        sum(len(table.writable_rows) for table in template.tables if table.writable_rows)
        or max((item.get("entity_estimate", 0) for item in extracted), default=0)
    )
    temp_output = OUTPUT_DIR / f".effective_audit_{uuid.uuid4().hex}{Path(template.source_file).suffix}"
    baseline = fill_template(template, extracted, str(temp_output))
    baseline.template_id = template.template_id or Path(template.source_file).stem
    baseline = _finalize_result_metrics(baseline, record_count=record_count, expected_rows=expected_rows)
    try:
        temp_output.unlink(missing_ok=True)
    except Exception:
        logger.warning("Failed to remove temporary audit output %s", temp_output, exc_info=True)
    return baseline, extracted


def _effective_delta_details(
    baseline: FilledResult,
    filled: FilledResult,
) -> dict[str, Any]:
    """Compare baseline and multi-source outputs and attribute visible cell deltas back to value sources."""
    baseline_map = _filled_field_map(baseline)
    multi_map = _filled_field_map(filled)
    all_keys = set(baseline_map) | set(multi_map)
    changed_rows: set[str] = set()
    changed_field_names: set[str] = set()
    changes_preview: list[dict[str, Any]] = []
    changed_cell_count = 0
    per_source: dict[str, dict[str, Any]] = {}
    changed_record_rows: dict[str, set[str]] = {}

    for key in sorted(all_keys):
        baseline_field = baseline_map.get(key)
        multi_field = multi_map.get(key)
        baseline_value = _field_visible_value(baseline_field)
        multi_value = _field_visible_value(multi_field)
        if baseline_value == multi_value:
            continue
        changed_cell_count += 1

        target_location = multi_field.target_location if multi_field else baseline_field.target_location
        field_name = multi_field.field_name if multi_field else baseline_field.field_name
        row_identifier = _field_row_identifier(target_location, field_name)
        if row_identifier:
            changed_rows.add(row_identifier)
        changed_field_names.add(field_name)
        if len(changes_preview) < 24:
            changes_preview.append({
                "target_location": target_location,
                "field_name": field_name,
                "baseline_value": baseline_value,
                "multisource_value": multi_value,
                "value_sources": sorted(_field_value_sources(multi_field)) if multi_field else [],
                "supporting_sources": sorted(_field_supporting_sources(multi_field)) if multi_field else [],
            })

        if multi_field is None:
            continue
        for record_id in _field_value_record_ids(multi_field):
            if row_identifier:
                changed_record_rows.setdefault(record_id, set()).add(row_identifier)
        for source_file in _field_value_sources(multi_field):
            bucket = per_source.setdefault(source_file, {
                "changed_cells": 0,
                "changed_rows": set(),
                "changed_field_names": set(),
                "target_locations_preview": [],
            })
            bucket["changed_cells"] += 1
            if row_identifier:
                bucket["changed_rows"].add(row_identifier)
            bucket["changed_field_names"].add(field_name)
            if target_location and len(bucket["target_locations_preview"]) < 12:
                bucket["target_locations_preview"].append(target_location)

    serialized_per_source: dict[str, dict[str, Any]] = {}
    for source_file, payload in per_source.items():
        serialized_per_source[source_file] = {
            "changed_cells": int(payload["changed_cells"]),
            "changed_rows": len(payload["changed_rows"]),
            "changed_field_names": sorted(payload["changed_field_names"]),
            "target_locations_preview": payload["target_locations_preview"],
        }

    baseline_row_ids = {
        _field_row_identifier(field.target_location, field.field_name)
        for field in baseline.filled_fields
        if _field_has_visible_value(field) and _field_row_identifier(field.target_location, field.field_name)
    }
    audit = {
        "changed_cells": changed_cell_count,
        "changed_rows": len(changed_rows),
        "changed_field_names": sorted(changed_field_names),
        "sources": sorted(serialized_per_source.keys()),
        "changes_preview": changes_preview,
        "per_source": serialized_per_source,
    }
    return {
        "audit": audit,
        "baseline_row_ids": baseline_row_ids,
        "changed_record_rows": changed_record_rows,
    }


def _compute_effective_cell_delta(baseline: FilledResult, filled: FilledResult) -> dict[str, Any]:
    """Public helper used by tests to compare baseline and multi-source visible deltas."""
    return _effective_delta_details(baseline, filled)["audit"]


def _collect_narrative_record_registry(extracted: list[dict]) -> dict[str, dict[str, dict[str, Any]]]:
    """Merge per-table narrative raw-record registries into one per-source lookup."""
    registry: dict[str, dict[str, dict[str, Any]]] = {}
    for table_data in extracted:
        for source_file, entries in (table_data.get("narrative_record_registry", {}) or {}).items():
            bucket = registry.setdefault(source_file, {})
            for entry in entries:
                record_id = clean_cell_value(str(entry.get("record_id", "")))
                if not record_id:
                    continue
                bucket[record_id] = dict(entry)
    return registry


def _classify_narrative_merge_outcomes(
    extracted: list[dict],
    delta_details: dict[str, Any],
) -> dict[str, dict[str, int]]:
    """Classify each raw narrative record into row merge/evidence/discard outcomes."""
    registry = _collect_narrative_record_registry(extracted)
    final_lineage_ids: set[str] = set()
    for table_data in extracted:
        for record in table_data.get("records", []):
            final_lineage_ids.update(record.get("origin_record_ids", []) or [])
    baseline_row_ids = delta_details.get("baseline_row_ids", set())
    changed_record_rows = delta_details.get("changed_record_rows", {})
    outcomes_by_source: dict[str, dict[str, int]] = {}
    for source_file, entries in registry.items():
        counts = Counter({
            "merged_into_existing_row": 0,
            "emitted_as_standalone_row": 0,
            "evidence_only": 0,
            "discarded": 0,
        })
        for record_id in entries:
            if record_id not in final_lineage_ids:
                counts["discarded"] += 1
                continue
            row_ids = changed_record_rows.get(record_id, set())
            if not row_ids:
                counts["evidence_only"] += 1
                continue
            if any(row_id in baseline_row_ids for row_id in row_ids):
                counts["merged_into_existing_row"] += 1
            else:
                counts["emitted_as_standalone_row"] += 1
        outcomes_by_source[source_file] = dict(counts)
    return outcomes_by_source


def _build_narrative_stage_loss_ledger(
    raw_counts: Counter[str],
    stage_payload: Counter[str],
    stage_breakdown: dict[str, dict[str, Any]],
    filter_reasons: Counter[str],
    merge_outcome: dict[str, int],
) -> dict[str, Any]:
    """Summarize narrative loss from suspicious -> qwen -> filter -> final materialization."""
    suspicious_records = int(raw_counts.get("suspicious_records", 0))
    llm_records = int(stage_payload.get("llm_records", 0))
    post_entity_records = int(stage_payload.get("post_entity_records", 0))
    final_records = int(stage_payload.get("final_records", 0))
    raw_total = int(raw_counts.get("total", 0))

    dropped_by_stage: dict[str, int] = {}
    remaining_by_stage: dict[str, int] = {}
    for stage_name, payload in stage_breakdown.items():
        dropped = int(payload.get("dropped", 0))
        remaining = int(payload.get("remaining", 0))
        if dropped > 0:
            dropped_by_stage[stage_name] = dropped
        if remaining > 0:
            remaining_by_stage[stage_name] = remaining

    qwen_refinement_drop = max(suspicious_records - llm_records, 0)
    if suspicious_records > 0 or llm_records > 0:
        remaining_by_stage["qwen_refinement"] = llm_records
        if qwen_refinement_drop > 0:
            dropped_by_stage["qwen_refinement"] = qwen_refinement_drop

    if post_entity_records > 0:
        remaining_by_stage["post_entity"] = post_entity_records
    merge_materialization_drop = max(post_entity_records - final_records, 0)
    remaining_by_stage["final"] = final_records
    if merge_materialization_drop > 0:
        dropped_by_stage["merge_materialization"] = merge_materialization_drop

    merge_outcome_total = sum(int(count) for count in merge_outcome.values())
    unexplained_counts: dict[str, int] = {}
    # qwen may expand records (llm_records > suspicious_records): that is a legitimate outcome,
    # not a loss.  Only flag as unexplained when there is a true positive loss (balance > 0).
    qwen_balance = suspicious_records - (llm_records + qwen_refinement_drop)
    if qwen_balance > 0:
        unexplained_counts["qwen_refinement"] = qwen_balance
    if post_entity_records != final_records + merge_materialization_drop:
        unexplained_counts["merge_materialization"] = post_entity_records - (final_records + merge_materialization_drop)
    if raw_total != merge_outcome_total:
        unexplained_counts["merge_outcome"] = raw_total - merge_outcome_total

    return {
        "remaining_records": post_entity_records,
        "final_records": final_records,
        "stage_flow": {
            "raw_total": raw_total,
            "rule_records": int(raw_counts.get("rule_records", 0)),
            "qwen_records": int(raw_counts.get("qwen_records", 0)),
            "stable_records": int(raw_counts.get("stable_records", 0)),
            "suspicious_records": suspicious_records,
            "llm_records": llm_records,
            "post_entity_records": post_entity_records,
            "final_records": final_records,
        },
        "remaining_by_stage": remaining_by_stage,
        "dropped_by_stage": dropped_by_stage,
        "drop_reason_counts": dict(filter_reasons),
        "qwen_refinement": {
            "suspicious_records": suspicious_records,
            "llm_records": llm_records,
            "unrecovered_records": qwen_refinement_drop,
        },
        "accounting": {
            "raw_total": raw_total,
            "merge_outcome_total": merge_outcome_total,
            "loss_accounting_complete": not unexplained_counts,
            "unexplained_counts": unexplained_counts,
        },
    }


def _build_effective_contribution_audit(
    *,
    documents: list[DocumentBundle],
    template,
    requirement: RequirementSpec,
    use_llm: bool,
    extracted: list[dict],
    filled: FilledResult,
) -> dict[str, Any]:
    """Build one generic audit that explains narrative loss, merge outcome, and real workbook delta."""
    narrative_docs = [document for document in documents if _is_narrative_source(document)]
    if not narrative_docs:
        return {}

    baseline_docs = [document for document in documents if not _is_narrative_source(document)]
    baseline, _baseline_extracted = _simulate_filled_result_for_audit(
        baseline_docs,
        template,
        requirement,
        use_llm=False,
    )
    delta_details = _effective_delta_details(baseline, filled)
    delta_audit = delta_details["audit"]
    narrative_registry = _collect_narrative_record_registry(extracted)
    merge_outcomes = _classify_narrative_merge_outcomes(extracted, delta_details)
    evidence_field_counts, value_field_counts, row_counts = _source_contribution_counters(filled)

    stage_audit_by_source: dict[str, Counter[str]] = {}
    filter_reasons_by_source: dict[str, Counter[str]] = {}
    per_source_stage: dict[str, dict[str, dict[str, Any]]] = {}
    for table_data in extracted:
        for source_file, counts in (table_data.get("narrative_stage_audit", {}) or {}).items():
            stage_audit_by_source.setdefault(source_file, Counter())
            stage_audit_by_source[source_file].update({name: int(count) for name, count in counts.items()})
        diagnostics = table_data.get("filter_diagnostics", {}) or {}
        for source_file, counts in (diagnostics.get("per_source", {}) or {}).items():
            filter_reasons_by_source.setdefault(source_file, Counter())
            filter_reasons_by_source[source_file].update({name: int(count) for name, count in counts.items()})
        for source_file, stage_info in (diagnostics.get("per_source_stage", {}) or {}).items():
            source_bucket = per_source_stage.setdefault(source_file, {})
            for stage_name, payload in stage_info.items():
                current = source_bucket.setdefault(stage_name, {
                    "dropped": 0,
                    "remaining": 0,
                    "reason_counts": {},
                })
                current["dropped"] = int(current.get("dropped", 0)) + int(payload.get("dropped", 0))
                current["remaining"] = max(int(current.get("remaining", 0)), int(payload.get("remaining", 0)))
                reason_counter = Counter(current.get("reason_counts", {}))
                reason_counter.update(payload.get("reason_counts", {}))
                current["reason_counts"] = dict(reason_counter)

    per_source: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    errors: list[str] = []
    for document in narrative_docs:
        source_file = document.source_file
        registry_entries = list(narrative_registry.get(source_file, {}).values())
        raw_counts = Counter()
        for entry in registry_entries:
            raw_counts["total"] += 1
            origin = clean_cell_value(str(entry.get("record_origin", "")))
            quality = clean_cell_value(str(entry.get("quality_bucket", "")))
            if origin == "rule":
                raw_counts["rule_records"] += 1
            if origin == "qwen":
                raw_counts["qwen_records"] += 1
            if quality == "stable":
                raw_counts["stable_records"] += 1
            if quality == "suspicious":
                raw_counts["suspicious_records"] += 1

        stage_payload = stage_audit_by_source.get(source_file, Counter())
        stage_breakdown = per_source_stage.get(source_file, {})
        merge_outcome = merge_outcomes.get(source_file, {
            "merged_into_existing_row": 0,
            "emitted_as_standalone_row": 0,
            "evidence_only": 0,
            "discarded": 0,
        })
        post_filter_audit = _build_narrative_stage_loss_ledger(
            raw_counts,
            stage_payload,
            stage_breakdown,
            filter_reasons_by_source.get(source_file, Counter()),
            merge_outcome,
        )
        source_delta = dict((delta_audit.get("per_source", {}) or {}).get(source_file, {}))
        source_delta.setdefault("changed_cells", 0)
        source_delta.setdefault("changed_rows", 0)
        source_delta.setdefault("changed_field_names", [])
        source_delta.setdefault("target_locations_preview", [])
        evidence_field_total = int(evidence_field_counts.get(source_file, 0))
        value_field_total = int(value_field_counts.get(source_file, 0))
        row_contribution_total = len(row_counts.get(source_file, set()))
        source_delta["evidence_contribution_fields"] = evidence_field_total
        source_delta["value_contribution_fields"] = value_field_total
        source_delta["row_contribution_records"] = row_contribution_total
        source_delta["supporting_only_fields"] = max(evidence_field_total - value_field_total, 0)
        source_delta["logical_only_value_fields"] = max(value_field_total - int(source_delta.get("changed_cells", 0)), 0)
        per_source[source_file] = {
            "raw_narrative_records": {
                "total": int(raw_counts.get("total", 0)),
                "rule_records": int(raw_counts.get("rule_records", 0)),
                "qwen_records": int(raw_counts.get("qwen_records", 0)),
                "stable_records": int(raw_counts.get("stable_records", 0)),
                "suspicious_records": int(raw_counts.get("suspicious_records", 0)),
            },
            "post_filter_narrative_records": post_filter_audit,
            "merge_outcome": merge_outcome,
            "effective_cell_delta": source_delta,
        }
        if raw_counts.get("suspicious_records", 0) > stage_payload.get("llm_records", 0):
            warnings.append(
                f"{Path(source_file).name}: suspicious {raw_counts.get('suspicious_records', 0)} -> qwen仅回补 {stage_payload.get('llm_records', 0)}"
            )
        if int(source_delta.get("logical_only_value_fields", 0)) > 0:
            warnings.append(
                f"{Path(source_file).name}: value_contribution={value_field_total} 但 visible_delta={int(source_delta.get('changed_cells', 0))}"
            )
        if not (post_filter_audit.get("accounting", {}) or {}).get("loss_accounting_complete", True):
            errors.append(
                f"{Path(source_file).name}: narrative loss accounting incomplete {post_filter_audit.get('accounting', {}).get('unexplained_counts', {})}"
            )

    return {
        "mode": "baseline_vs_multisource",
        "baseline_source_files": [document.source_file for document in baseline_docs],
        "multisource_source_files": [document.source_file for document in documents],
        "effective_cell_delta": delta_audit,
        "per_source": per_source,
        "warnings": _ordered_unique(warnings),
        "errors": _ordered_unique(errors),
    }


def _finalize_effective_contribution_audit_flags(filled: FilledResult):
    """Add false-positive/evidence-only audit flags after source stats are built."""
    audit = dict(filled.effective_contribution_audit or {})
    warnings = list(audit.get("warnings", []))
    errors = list(audit.get("errors", []))
    for source_file, payload in (audit.get("per_source", {}) or {}).items():
        post_filter = (payload.get("post_filter_narrative_records", {}) or {}) if isinstance(payload, dict) else {}
        accounting = (post_filter.get("accounting", {}) or {}) if isinstance(post_filter, dict) else {}
        if accounting and not accounting.get("loss_accounting_complete", True):
            errors.append(
                f"{Path(source_file).name}: narrative loss accounting incomplete {accounting.get('unexplained_counts', {})}"
            )
        delta_payload = (payload.get("effective_cell_delta", {}) or {}) if isinstance(payload, dict) else {}
        logical_only = int(delta_payload.get("logical_only_value_fields", 0))
        if logical_only > 0:
            warnings.append(
                f"{Path(source_file).name}: logical value fields {logical_only} 未形成可见 workbook delta"
            )
    for stat in filled.source_stats:
        if stat.file_type not in {"word", "markdown", "text"}:
            continue
        if stat.evidence_contribution_fields > 0 and stat.contributed_fields == 0:
            warnings.append(
                f"{Path(stat.source_file).name}: evidence_contribution={stat.evidence_contribution_fields} 但 value_contribution=0"
            )
        if stat.contributed_fields > 0 and stat.effective_cell_delta == 0:
            errors.append(
                f"{Path(stat.source_file).name}: contributed_fields={stat.contributed_fields} 但 effective_cell_delta=0"
            )
    audit["warnings"] = _ordered_unique(warnings)
    audit["errors"] = _ordered_unique(errors)
    filled.effective_contribution_audit = audit


def _build_source_stats(
    documents: list[DocumentBundle],
    retrieval,
    extracted: list[dict],
    filled: FilledResult,
    template_name: str,
    model_usage: dict[str, Any] | None = None,
) -> list[SourceProcessingStat]:
    """Build per-source contribution stats for a template result."""
    if hasattr(model_usage, "model_dump"):
        model_usage = model_usage.model_dump()
    source_counts: dict[str, int] = {document.source_file: 0 for document in documents}
    filtered_source_counts: dict[str, int] = {document.source_file: 0 for document in documents}
    entity_block_counts: dict[str, int] = {document.source_file: 0 for document in documents}
    filter_reasons_by_source: dict[str, Counter[str]] = {document.source_file: Counter() for document in documents}
    narrative_stage_audit_by_source: dict[str, Counter[str]] = {document.source_file: Counter() for document in documents}
    for table_data in extracted:
        for source_file, count in table_data.get("source_counts", {}).items():
            source_counts[source_file] = source_counts.get(source_file, 0) + int(count)
        for source_file, count in table_data.get("filtered_source_counts", {}).items():
            filtered_source_counts[source_file] = filtered_source_counts.get(source_file, 0) + int(count)
        for source_file, count in table_data.get("entity_block_counts", {}).items():
            entity_block_counts[source_file] = max(entity_block_counts.get(source_file, 0), int(count))
        for source_file, stage_counts in (table_data.get("narrative_stage_audit", {}) or {}).items():
            narrative_stage_audit_by_source.setdefault(source_file, Counter())
            narrative_stage_audit_by_source[source_file].update(
                {name: int(count) for name, count in stage_counts.items()}
            )
        for source_file, reason_counts in (table_data.get("filter_diagnostics", {}) or {}).get("per_source", {}).items():
            filter_reasons_by_source.setdefault(source_file, Counter())
            filter_reasons_by_source[source_file].update({reason: int(count) for reason, count in reason_counts.items()})
    evidence_field_counts, value_field_counts, row_counts = _source_contribution_counters(filled)
    effective_audit = filled.effective_contribution_audit or {}
    per_source_audit = (effective_audit.get("per_source", {}) or {}) if isinstance(effective_audit, dict) else {}
    per_source_usage = (model_usage or {}).get("per_source", {}) if isinstance(model_usage, dict) else {}
    per_source_stage_usage = (model_usage or {}).get("per_source_stage", {}) if isinstance(model_usage, dict) else {}
    per_template_source_stage_usage = (
        (model_usage or {}).get("per_template_source_stage", {})
        if isinstance(model_usage, dict) else {}
    )
    probe_sources = set((model_usage or {}).get("probe_sources", [])) if isinstance(model_usage, dict) else set()
    probe_source_calls = (model_usage or {}).get("probe_source_calls", {}) if isinstance(model_usage, dict) else {}
    template_source_stage_usage = (
        per_template_source_stage_usage.get(filled.template_file, {})
        if isinstance(per_template_source_stage_usage, dict) else {}
    )
    relevant_source_files = {
        source_file
        for table_data in extracted
        for source_file in table_data.get("relevant_source_files", [])
        if source_file
    }
    invalidated_source_files = {
        source_file
        for table_data in extracted
        for source_file in table_data.get("invalidated_source_files", [])
        if source_file
    }

    stats: list[SourceProcessingStat] = []
    for document in documents:
        extracted_records = source_counts.get(document.source_file, 0)
        filtered_records = filtered_source_counts.get(document.source_file, 0)
        entity_blocks_detected = entity_block_counts.get(document.source_file, 0)
        evidence_contribution_fields = int(evidence_field_counts.get(document.source_file, 0))
        value_contribution_fields = int(value_field_counts.get(document.source_file, 0))
        row_contribution_records = len(row_counts.get(document.source_file, set()))
        contributed_records = row_contribution_records
        contributed_fields = value_contribution_fields
        stage_audit = dict(narrative_stage_audit_by_source.get(document.source_file, Counter()))
        narrative_audit = dict(per_source_audit.get(document.source_file, {}))
        effective_cell_delta = int((narrative_audit.get("effective_cell_delta", {}) or {}).get("changed_cells", 0))
        effective_row_delta = int((narrative_audit.get("effective_cell_delta", {}) or {}).get("changed_rows", 0))
        warnings: list[str] = []
        filter_reason_summary = filter_reasons_by_source.get(document.source_file, Counter())
        if contributed_records > filled.rows_filled:
            warnings.append(
                f"数据源 {Path(document.source_file).name} 的贡献行数 {contributed_records} 超过模板写回行数 {filled.rows_filled}，已按有效写回行重算"
            )
            contributed_records = filled.rows_filled
        if extracted_records and contributed_records > extracted_records:
            warnings.append(
                f"数据源 {Path(document.source_file).name} 的贡献行数 {contributed_records} 超过抽取记录数 {extracted_records}，请检查合并或写回映射"
            )
        source_stage_counts = template_source_stage_usage.get(document.source_file, {}) or {}
        qwen_call_count = sum(int(count) for count in source_stage_counts.values())
        probe_call_count = int(source_stage_counts.get("source_probe", 0))
        if qwen_call_count == 0:
            qwen_call_count = int(per_source_usage.get(document.source_file, 0))
            probe_call_count = int(probe_source_calls.get(document.source_file, 0))
            source_stage_counts = per_source_stage_usage.get(document.source_file, {}) or {}
        qwen_stages = [
            stage_name
            for stage_name, count in source_stage_counts.items()
            if int(count) > 0
        ]
        if not qwen_stages:
            if probe_call_count > 0 or document.source_file in probe_sources:
                qwen_stages.append("source_probe")
            if qwen_call_count - probe_call_count > 0:
                qwen_stages.append("extract")
        source_context_matched = document.source_file in relevant_source_files
        if document.source_file in invalidated_source_files and filtered_records == 0 and contributed_fields == 0:
            source_context_matched = False
            warnings.append(
                f"数据源 {Path(document.source_file).name} 仅产出实体类型不合法或粒度不符的候选记录，已视为未命中当前模板"
            )
        relevant_to_template = bool(
            source_context_matched
            or filtered_records > 0
            or contributed_fields > 0
            or evidence_contribution_fields > 0
        )
        relevance_score = 0.0
        if source_context_matched:
            relevance_score += 1.0
        if filtered_records > 0 or contributed_fields > 0 or evidence_contribution_fields > 0:
            relevance_score += 0.5
        if not relevant_to_template:
            warnings.append(f"数据源 {Path(document.source_file).name} 未命中当前模板主题或字段，未参与本次填充")
        elif filtered_records == 0 and contributed_fields == 0 and evidence_contribution_fields == 0:
            warnings.append(f"数据源 {Path(document.source_file).name} 未对模板 {template_name} 贡献可写入记录")
        if filter_reason_summary:
            reason_text = "；".join(
                f"{describe_entity_reason(reason)} x{count}"
                for reason, count in filter_reason_summary.most_common(3)
            )
            warnings.append(f"过滤原因摘要: {reason_text}")
        if stage_audit and (extracted_records > 0 or contributed_fields > 0):
            warnings.append(
                "叙事阶段审计: "
                f"relevant_segments={stage_audit.get('relevant_segments', 0)}, "
                f"rule={stage_audit.get('rule_records', 0)}, "
                f"stable={stage_audit.get('stable_records', 0)}, "
                f"suspicious={stage_audit.get('suspicious_records', 0)}, "
                f"qwen={stage_audit.get('llm_records', 0)}, "
                f"post_entity={stage_audit.get('post_entity_records', filtered_records)}, "
                f"final={stage_audit.get('final_records', contributed_records)}"
            )
        if evidence_contribution_fields > 0 or contributed_fields > 0:
            warnings.append(
                f"贡献拆分: evidence={evidence_contribution_fields}, "
                f"value={value_contribution_fields}, row={row_contribution_records}, "
                f"effective_delta={effective_cell_delta}"
            )
        if narrative_audit.get("post_filter_narrative_records"):
            post_filter = narrative_audit["post_filter_narrative_records"]
            dropped_by_stage = post_filter.get("dropped_by_stage", {})
            drop_reason_counts = post_filter.get("drop_reason_counts", {})
            if dropped_by_stage or drop_reason_counts:
                stage_text = ", ".join(f"{name}={count}" for name, count in dropped_by_stage.items())
                reason_text = "；".join(
                    f"{describe_entity_reason(reason)} x{count}"
                    for reason, count in Counter(drop_reason_counts).most_common(3)
                )
                warnings.append(
                    "post-filter loss: "
                    + ", ".join(part for part in [stage_text, reason_text] if part)
                )
        if narrative_audit.get("merge_outcome"):
            merge_text = ", ".join(
                f"{name}={count}"
                for name, count in narrative_audit["merge_outcome"].items()
                if int(count) > 0
            )
            if merge_text:
                warnings.append(f"merge outcome: {merge_text}")
        stats.append(SourceProcessingStat(
            source_file=document.source_file,
            file_type=document.file_type,
            text_blocks=len(document.text_blocks),
            tables=len(document.tables),
            entity_blocks_detected=entity_blocks_detected,
            relevant_to_template=relevant_to_template,
            relevance_score=round(relevance_score, 2),
            extracted_records=extracted_records,
            filtered_records=filtered_records,
            contributed_records=contributed_records,
            contributed_fields=contributed_fields,
            evidence_contribution_fields=evidence_contribution_fields,
            value_contribution_fields=value_contribution_fields,
            row_contribution_records=row_contribution_records,
            effective_cell_delta=effective_cell_delta,
            effective_row_delta=effective_row_delta,
            qwen_used=bool(qwen_call_count),
            qwen_call_count=qwen_call_count,
            qwen_stages=qwen_stages,
            stage_audit=stage_audit,
            narrative_audit=narrative_audit,
            contribution_templates=[template_name] if filtered_records > 0 or evidence_contribution_fields > 0 or contributed_fields > 0 else [],
            warnings=warnings,
        ))
    return stats


def _template_model_usage_snapshot(task_id: str, template_file: str) -> ModelUsageSummary:
    """Build a truthful per-template qwen usage summary from the task trace."""
    snapshot = _model_usage_snapshot(task_id)
    snapshot_dict = snapshot.model_dump() if hasattr(snapshot, "model_dump") else dict(snapshot)
    trace_entries = _load_trace_entries(snapshot_dict.get("trace_file", ""))
    filtered_entries = [
        entry
        for entry in trace_entries
        if template_file in (entry.get("template_files") or [])
        or entry.get("template_file") == template_file
    ]
    per_stage: Counter[str] = Counter()
    per_source: Counter[str] = Counter()
    per_source_stage: dict[str, Counter[str]] = {}
    probe_sources: list[str] = []
    probe_source_calls: Counter[str] = Counter()

    for entry in filtered_entries:
        stage_name = str(entry.get("stage_name", "unknown") or "unknown")
        per_stage[stage_name] += 1
        sources = entry.get("source_files") or ([entry.get("source_file")] if entry.get("source_file") else [])
        for source_file in sources:
            per_source[source_file] += 1
            stage_counter = per_source_stage.setdefault(source_file, Counter())
            stage_counter[stage_name] += 1
            if entry.get("probe_only"):
                probe_source_calls[source_file] += 1
                if source_file not in probe_sources:
                    probe_sources.append(source_file)

    filtered_skip_events = [
        event
        for event in snapshot_dict.get("skip_events", [])
        if template_file in (event.get("template_files") or [])
        or event.get("template_file") == template_file
    ]
    filtered_required_calls = [
        item
        for item in snapshot_dict.get("required_calls", [])
        if template_file in (item.get("template_files") or [])
        or item.get("template_file") == template_file
    ]
    filtered_missing_required_calls = [
        item
        for item in snapshot_dict.get("missing_required_calls", [])
        if template_file in (item.get("template_files") or [])
        or item.get("template_file") == template_file
    ]
    filtered_fallback_reasons = [
        reason
        for reason in snapshot_dict.get("fallback_reasons", [])
        if not template_file or Path(template_file).name in reason or template_file in reason
    ]
    sample_trace = {}
    for entry in reversed(filtered_entries):
        if entry.get("finish_status") == "success":
            sample_trace = entry
            break
    if not sample_trace and filtered_entries:
        sample_trace = filtered_entries[-1]

    return ModelUsageSummary(
        provider=snapshot.provider,
        model=snapshot.model,
        called=bool(filtered_entries),
        model_not_used=not bool(filtered_entries),
        total_calls=len(filtered_entries),
        successful_calls=sum(1 for entry in filtered_entries if entry.get("finish_status") == "success"),
        per_stage=dict(per_stage),
        per_source=dict(per_source),
        per_source_stage={source: dict(counter) for source, counter in per_source_stage.items()},
        per_template_source_stage={
            template_file: {
                source: dict(counter)
                for source, counter in per_source_stage.items()
            }
        } if filtered_entries else {},
        per_template={template_file: len(filtered_entries)} if filtered_entries else {},
        probe_sources=probe_sources,
        probe_source_calls=dict(probe_source_calls),
        fallback_reasons=filtered_fallback_reasons,
        trace_file=snapshot.trace_file,
        sample_trace=sample_trace,
        skip_events=filtered_skip_events,
        required_calls=filtered_required_calls,
        missing_required_calls=filtered_missing_required_calls,
        validation_errors=list(snapshot_dict.get("validation_errors", [])),
        degraded=bool(filtered_missing_required_calls or snapshot_dict.get("degraded", False)),
        availability_status=snapshot.availability_status,
    )


def _load_trace_entries(trace_file: str) -> list[dict[str, Any]]:
    """Load trace entries from the task-scoped model trace file."""
    if not trace_file:
        return []
    trace_path = Path(trace_file)
    if not trace_path.exists():
        return []
    try:
        payload = json.loads(trace_path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to read model trace file %s", trace_file, exc_info=True)
        return []
    entries = payload.get("entries", [])
    return entries if isinstance(entries, list) else []


def _field_supporting_sources(field) -> set[str]:
    """Collect all supporting sources for one written field."""
    explicit = {
        source_file
        for source_file in getattr(field, "supporting_sources", [])
        if source_file
    }
    if explicit:
        return explicit
    sources = {
        evidence.source_file
        for evidence in getattr(field, "evidence", [])
        if getattr(evidence, "source_file", "")
    }
    if not sources and getattr(field, "source_file", ""):
        sources.add(field.source_file)
    return sources


def _refresh_response_time_validation(filled: FilledResult):
    """Refresh response-time validation after the final timing is known."""
    response_time_ok = filled.timing.get("total", 0.0) <= 90.0 if filled.timing else True
    for item in filled.validation_report:
        if item.check == "response_time":
            item.passed = response_time_ok
            item.message = f"Response time: {filled.timing.get('total', 0.0):.1f}s"
            break
    apply_validation_outcome(filled)


def _aggregate_pipeline_warnings(results: list[FilledResult], warnings: list[str], logs: list[str]):
    """Promote important template-level warnings to the pipeline level."""
    for result in results:
        for warning in result.warnings:
            if warning not in warnings:
                warnings.append(warning)
        if result.expected_rows > 1 and result.record_count < result.expected_rows:
            warning = (
                f"模板 {Path(result.template_file).name} 预估应抽取约 {result.expected_rows} 条记录，"
                f"实际仅写入 {result.record_count} 条"
            )
            if warning not in warnings:
                warnings.append(warning)
        for source_stat in result.source_stats:
            for warning in source_stat.warnings:
                if warning not in warnings:
                    warnings.append(warning)

    if len(results) > 1:
        successful = [result for result in results if result.output_file]
        if len(successful) != len(results):
            warnings.append("多模板任务中存在部分模板失败，请分别检查结果卡片与日志")
        _warn_about_similar_outputs(results, warnings)

    for warning in _deduplicate_text(warnings):
        if warning not in logs:
            logs.append(warning)


def _log_and_emit(
    logs: list[str],
    log_text: str,
    progress_callback: Optional[ProgressCallback],
    **event: Any,
):
    """Append a log line and optionally emit a progress event."""
    logs.append(log_text)
    logger.info(log_text)
    payload = {"log": log_text}
    payload.update(event)
    _emit_progress(progress_callback, **payload)


def _emit_progress(progress_callback: Optional[ProgressCallback], **event: Any):
    """Safely emit a pipeline progress event."""
    if progress_callback is None:
        return
    try:
        progress_callback(event)
    except Exception:
        logger.debug("progress callback failed", exc_info=True)


def _deduplicate_text(values: list[str]) -> list[str]:
    """Deduplicate strings while preserving order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _field_row_identifier(target_location: str, field_name: str) -> str:
    """Build a stable contributed-record identifier from a filled field location."""
    if not target_location:
        return ""
    lowered = target_location.lower()
    if "placeholder" in lowered or lowered.startswith("paragraph") or lowered.startswith("txt_pos") or lowered.startswith("md_pos"):
        return ""
    excel_match = Path(target_location).name if "!" not in target_location else target_location
    row_match = None
    import re

    excel_row = re.search(r'!([A-Z]+)(\d+)$', excel_match)
    if excel_row:
        return f"{target_location.split('!')[0]}!{excel_row.group(2)}"
    word_row = re.search(r'(table\d+\.row\d+)', target_location)
    if word_row:
        return word_row.group(1)
    row_match = re.search(r'([a-z_]+\d*\.row\d+)', target_location)
    if row_match:
        return row_match.group(1)
    return ""


def _finalize_result_metrics(filled: FilledResult, record_count: int, expected_rows: int) -> FilledResult:
    """Normalize record_count / rows_filled / expected_rows / fill_rate definitions."""
    filled.record_count = max(int(filled.record_count or 0), int(record_count or 0))
    filled.rows_filled = max(0, int(filled.rows_filled or 0))
    filled.expected_rows = max(int(expected_rows or 0), filled.rows_filled)
    if filled.expected_rows > 0:
        filled.fill_rate = round((filled.rows_filled / filled.expected_rows) * 100, 4)
    elif filled.filled_fields:
        populated = sum(1 for field in filled.filled_fields if field.value not in (None, "", "N/A"))
        filled.fill_rate = round((populated / max(len(filled.filled_fields), 1)) * 100, 4)
    else:
        filled.fill_rate = 0.0
    return filled


def _build_result_normalization_report(extracted: list[dict]) -> dict[str, Any]:
    """Summarize standardization outcomes from extracted table records."""
    from .normalization_service import normalize_value
    from .schema_registry_service import canonical_field_name, infer_field_type

    field_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    unit_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for table_data in extracted:
        for record in table_data.get("records", []):
            for field_name, value in (record.get("values", {}) or {}).items():
                if value in (None, ""):
                    continue
                normalized = normalize_value(value, field_name)
                canonical = canonical_field_name(field_name)
                field_counts[canonical or field_name] += 1
                type_counts[normalized.get("field_type") or infer_field_type(field_name)] += 1
                status_counts[normalized.get("status", "unknown")] += 1
                if normalized.get("unit"):
                    unit_counts[normalized["unit"]] += 1
                if len(examples) < 24:
                    examples.append({
                        "field_name": field_name,
                        "canonical_field": canonical,
                        "raw_value": value,
                        "standard_value": normalized.get("standard_value"),
                        "field_type": normalized.get("field_type"),
                        "unit": normalized.get("unit"),
                        "status": normalized.get("status"),
                    })
    return {
        "field_distribution": dict(field_counts),
        "type_distribution": dict(type_counts),
        "status_distribution": dict(status_counts),
        "unit_distribution": dict(unit_counts),
        "examples": examples,
    }


def _build_normalization_report(results: list[FilledResult]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    unit_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for result in results:
        report = result.normalization_report or {}
        field_counts.update(report.get("field_distribution", {}))
        type_counts.update(report.get("type_distribution", {}))
        status_counts.update(report.get("status_distribution", {}))
        unit_counts.update(report.get("unit_distribution", {}))
        for example in report.get("examples", []):
            if len(examples) >= 40:
                break
            examples.append({"template_file": result.template_file, **example})
    return {
        "field_distribution": dict(field_counts),
        "type_distribution": dict(type_counts),
        "status_distribution": dict(status_counts),
        "unit_distribution": dict(unit_counts),
        "examples": examples,
    }


def _discard_invalid_output(filled: FilledResult):
    """Keep the output directory limited to latest valid outputs only."""
    if not filled.output_file:
        return
    try:
        Path(filled.output_file).unlink(missing_ok=True)
    except Exception:
        logger.warning("Failed to remove invalid output %s", filled.output_file, exc_info=True)
    filled.output_file = ""


def _warn_about_similar_outputs(results: list[FilledResult], warnings: list[str]):
    """Warn when multiple templates produce suspiciously similar outputs or evidence."""
    signatures: dict[tuple[tuple[str, ...], tuple[str, ...]], list[str]] = {}
    for result in results:
        values = tuple(
            str(field.value)
            for field in result.filled_fields[:12]
            if field.value not in (None, "", "N/A")
        )
        evidence_sources = tuple(sorted({
            Path(evidence.source_file).name
            for evidence in result.evidence_report[:12]
            if evidence.source_file
        }))
        key = (values, evidence_sources)
        signatures.setdefault(key, []).append(Path(result.template_file).name or "模板")

    for key, names in signatures.items():
        values, evidence_sources = key
        if len(names) > 1 and (values or evidence_sources):
            warnings.append(
                f"模板 {', '.join(names)} 的输出字段和证据来源异常相似，请检查是否存在模板串扰或缓存污染"
            )
