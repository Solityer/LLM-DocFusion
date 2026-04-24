"""Validation service - check fill results for correctness."""
from __future__ import annotations

import math
import os
import re
from pathlib import Path

from ..core.logging import logger
from ..schemas.models import FilledResult, ValidationItem
from ..utils.entity_utils import describe_entity_reason, is_entity_header, validate_entity_value


CRITICAL_VALIDATION_CHECKS = frozenset({
    "output_file_exists",
    "fill_rate",
    "rows_filled",
    "record_coverage",
    "list_like_record_count",
    "metric_consistency",
    "source_stat_consistency",
    "entity_legality",
    "source_topic_match",
    "unstructured_source_contribution",
    "effective_contribution_alignment",
    "narrative_loss_accounting",
    "narrative_pipeline_effectiveness",
    "repeated_entity_narrative",
    "model_usage_truthfulness",
    "model_usage_validation",
    "model_trace_file",
    "response_time",
})

HARD_VALIDATION_CHECKS = frozenset({
    "output_file_exists",
    "fill_rate",
    "rows_filled",
    "record_coverage",
    "list_like_record_count",
    "metric_consistency",
    "source_stat_consistency",
    "entity_legality",
    "source_topic_match",
    "unstructured_source_contribution",
    "effective_contribution_alignment",
    "narrative_loss_accounting",
    "narrative_pipeline_effectiveness",
    "repeated_entity_narrative",
    "model_usage_truthfulness",
    "model_usage_validation",
    "model_trace_file",
})


def apply_validation_outcome(result: FilledResult) -> FilledResult:
    """Apply the canonical validation pass/fail policy to the result status."""
    failed_checks = {
        item.check
        for item in result.validation_report
        if not item.passed
    }
    critical_failures = failed_checks & CRITICAL_VALIDATION_CHECKS
    hard_failures = failed_checks & HARD_VALIDATION_CHECKS
    result.meets_minimum = not critical_failures
    result.status = "completed" if result.output_file and not hard_failures else "error"
    return result


def validate_result(result: FilledResult) -> FilledResult:
    """Run validation checks on a filled result."""
    logger.info("Validating result for: %s", result.template_file)

    checks: list[ValidationItem] = []
    warnings: list[str] = list(result.warnings)
    model_usage = result.model_usage.model_dump() if hasattr(result.model_usage, "model_dump") else (result.model_usage or {})
    metric_definitions = result.metric_definitions or {
        "record_count": "本次模板各目标表/区域生成的抽取记录总数；跨表按目标位置分别计数。",
        "rows_filled": "成功写入模板且至少包含一个非空值的目标行数。",
        "expected_rows": "本次模板目标应填行数；优先使用模板可写行位，其次使用实体估计。",
        "fill_rate": "rows_filled / expected_rows * 100；若无行级目标，则回退为字段填充率。",
    }
    result.metric_definitions = metric_definitions

    if result.output_file:
        exists = os.path.exists(result.output_file)
        checks.append(ValidationItem(
            check="output_file_exists",
            passed=exists,
            message=f"Output file {'exists' if exists else 'missing'}: {result.output_file}",
        ))
    else:
        checks.append(ValidationItem(
            check="output_file_exists",
            passed=False,
            message="No valid output file retained after validation",
        ))

    fill_rate_ok = result.fill_rate >= 80.0
    checks.append(ValidationItem(
        check="fill_rate",
        passed=fill_rate_ok,
        message=f"Fill rate: {result.fill_rate:.1f}% (定义: {metric_definitions['fill_rate']})",
    ))

    checks.append(ValidationItem(
        check="rows_filled",
        passed=result.rows_filled > 0,
        message=f"Rows filled: {result.rows_filled}",
    ))

    suspicious = []
    for field in result.filled_fields:
        if field.value and field.value not in ("", "N/A") and (not field.evidence or not field.source_file):
            suspicious.append(field.field_name)
            field.confidence = None
            if field.missing_reason != "No evidence":
                field.missing_reason = "No evidence"

    if suspicious:
        warning = f"字段有值但缺少完整证据链: {suspicious[:8]}"
        warnings.append(warning)
        checks.append(ValidationItem(
            check="value_without_evidence",
            passed=False,
            message=warning,
        ))
    else:
        checks.append(ValidationItem(
            check="value_without_evidence",
            passed=True,
            message="All populated fields have evidence and source trace",
        ))

    missing_required = [field.field_name for field in result.filled_fields if not field.value or field.value in ("", "N/A")]
    missing_ratio = (len(missing_required) / max(len(result.filled_fields), 1)) if result.filled_fields else 0.0
    checks.append(ValidationItem(
        check="missing_fields",
        passed=missing_ratio <= 0.5,
        message=f"Missing fields ({len(missing_required)}): {missing_required[:10]}",
    ))

    checks.append(ValidationItem(
        check="unresolved_fields",
        passed=not result.unresolved_fields,
        message="No unresolved fields recorded" if not result.unresolved_fields else f"Unresolved: {result.unresolved_fields[:10]}",
    ))

    if result.expected_rows > 0:
        coverage_ok = result.rows_filled >= max(1, int(result.expected_rows * 0.8))
        checks.append(ValidationItem(
            check="record_coverage",
            passed=coverage_ok,
            message=f"Rows filled: {result.rows_filled}, expected about {result.expected_rows}",
        ))

    if result.expected_rows > 1 and result.record_count <= 1:
        warning = f"检测到明显列表型任务，但仅抽取出 {result.record_count} 条记录"
        warnings.append(warning)
        checks.append(ValidationItem(
            check="list_like_record_count",
            passed=False,
            message=warning,
        ))
    elif result.expected_rows > 1:
        checks.append(ValidationItem(
            check="list_like_record_count",
            passed=True,
            message=f"List-like extraction returned {result.record_count} records",
        ))

    if result.source_stats:
        relevant_stats = [stat for stat in result.source_stats if stat.relevant_to_template]
        contributing_relevant_stats = [
            stat for stat in relevant_stats
            if stat.contributed_records > 0 or stat.contributed_fields > 0 or stat.evidence_contribution_fields > 0
        ]
        zero_sources = [
            Path(stat.source_file).name
            for stat in relevant_stats
            if stat.contributed_records == 0 and stat.contributed_fields == 0 and stat.evidence_contribution_fields == 0
        ]
        ignored_sources = [
            Path(stat.source_file).name
            for stat in result.source_stats
            if not stat.relevant_to_template
        ]
        if zero_sources:
            warnings.append(f"多源任务中以下数据源未贡献结果: {zero_sources[:6]}")
        if ignored_sources:
            warnings.append(f"以下数据源未命中当前模板主题，已跳过: {ignored_sources[:6]}")
        checks.append(ValidationItem(
            check="source_contribution",
            passed=bool(contributing_relevant_stats) or not relevant_stats,
            message=(
                f"Template-relevant contributing sources: {[Path(stat.source_file).name for stat in contributing_relevant_stats[:6]]}"
                + (f"；zero-contribution warnings: {zero_sources[:6]}" if zero_sources else "")
                + (f"；ignored: {ignored_sources[:6]}" if ignored_sources else "")
            ),
        ))

        inconsistent_sources = []
        for stat in result.source_stats:
            if result.rows_filled > 0 and stat.contributed_records > result.rows_filled:
                inconsistent_sources.append(
                    f"{Path(stat.source_file).name}: contributed_records={stat.contributed_records} > rows_filled={result.rows_filled}"
                )
            if stat.extracted_records > 0 and stat.contributed_records > stat.extracted_records:
                inconsistent_sources.append(
                    f"{Path(stat.source_file).name}: contributed_records={stat.contributed_records} > extracted_records={stat.extracted_records}"
                )
            if stat.contributed_records > 0 and stat.contributed_fields == 0:
                inconsistent_sources.append(
                    f"{Path(stat.source_file).name}: contributed_records>0 but contributed_fields==0"
                )
            if stat.value_contribution_fields and stat.value_contribution_fields != stat.contributed_fields:
                inconsistent_sources.append(
                    f"{Path(stat.source_file).name}: value_contribution_fields={stat.value_contribution_fields} != contributed_fields={stat.contributed_fields}"
                )
        if inconsistent_sources:
            warning = "source 贡献统计不一致: " + "；".join(inconsistent_sources[:4])
            warnings.append(warning)
        checks.append(ValidationItem(
            check="source_stat_consistency",
            passed=not inconsistent_sources,
            message=(
                "Per-source extracted/contributed metrics are consistent"
                if not inconsistent_sources else
                "；".join(inconsistent_sources[:4])
            ),
        ))

        narrative_stats = [
            stat for stat in result.source_stats
            if stat.file_type in {"word", "markdown", "text"} and stat.relevant_to_template
        ]
        false_positive_sources = [
            (
                f"{Path(stat.source_file).name}: evidence={stat.evidence_contribution_fields}, "
                f"value={stat.contributed_fields}, effective_cell_delta={stat.effective_cell_delta}"
            )
            for stat in narrative_stats
            if stat.contributed_fields > 0 and stat.effective_cell_delta == 0
        ]
        audit_errors = list((result.effective_contribution_audit or {}).get("errors", []))
        if false_positive_sources or audit_errors:
            warning = "；".join((false_positive_sources + audit_errors)[:4])
            warnings.append(warning)
            checks.append(ValidationItem(
                check="effective_contribution_alignment",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="effective_contribution_alignment",
                passed=True,
                message="effective contribution audit 与 source stats 一致",
            ))

        incomplete_loss_audits = []
        for stat in narrative_stats:
            if not (stat.extracted_records > 0 or stat.qwen_used or stat.stage_audit):
                continue
            audit_payload = stat.narrative_audit or {}
            post_filter = dict(audit_payload.get("post_filter_narrative_records", {}) or {})
            remaining_by_stage = dict(post_filter.get("remaining_by_stage", {}) or {})
            dropped_by_stage = dict(post_filter.get("dropped_by_stage", {}) or {})
            accounting = dict(post_filter.get("accounting", {}) or {})
            suspicious_records = int((stat.stage_audit or {}).get("suspicious_records", 0))
            if not post_filter:
                incomplete_loss_audits.append(
                    f"{Path(stat.source_file).name}: missing post_filter_narrative_records"
                )
                continue
            if suspicious_records > 0 and "qwen_refinement" not in remaining_by_stage and "qwen_refinement" not in dropped_by_stage:
                incomplete_loss_audits.append(
                    f"{Path(stat.source_file).name}: missing qwen_refinement loss accounting"
                )
                continue
            if not accounting:
                incomplete_loss_audits.append(
                    f"{Path(stat.source_file).name}: missing narrative loss accounting summary"
                )
                continue
            if not accounting.get("loss_accounting_complete", True):
                incomplete_loss_audits.append(
                    f"{Path(stat.source_file).name}: unexplained narrative losses {accounting.get('unexplained_counts', {})}"
                )
        if incomplete_loss_audits:
            warning = "；".join(incomplete_loss_audits[:4])
            warnings.append(warning)
            checks.append(ValidationItem(
                check="narrative_loss_accounting",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="narrative_loss_accounting",
                passed=True,
                message="narrative stage loss accounting is complete",
            ))

        zero_record_narratives = [
            Path(stat.source_file).name
            for stat in narrative_stats
            if stat.text_blocks > 0 and stat.extracted_records == 0
        ]
        if zero_record_narratives:
            warning = f"已识别 narrative text_blocks，但未成功生成 records: {zero_record_narratives[:6]}"
            warnings.append(warning)
            checks.append(ValidationItem(
                check="narrative_records_extracted",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="narrative_records_extracted",
                passed=True,
                message=(
                    "Relevant narrative sources generated structured records"
                    if narrative_stats else
                    "当前模板未触发 narrative records 抽取校验"
                ),
            ))

        missing_stage_audit = [
            Path(stat.source_file).name
            for stat in narrative_stats
            if (stat.extracted_records > 0 or stat.qwen_used) and not stat.stage_audit
        ]
        if missing_stage_audit:
            warning = f"已触发 narrative 抽取但缺少阶段审计计数: {missing_stage_audit[:6]}"
            warnings.append(warning)
            checks.append(ValidationItem(
                check="narrative_stage_audit",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="narrative_stage_audit",
                passed=True,
                message=(
                    "Relevant narrative sources expose stage-level audit counts"
                    if narrative_stats else
                    "当前模板未触发 narrative stage audit 校验"
                ),
            ))

        filtered_out_narratives = [
            Path(stat.source_file).name
            for stat in narrative_stats
            if stat.entity_blocks_detected > 0 and stat.filtered_records == 0
        ]
        if filtered_out_narratives:
            warning = f"entity blocks 已识别，但 records 在过滤后被清空: {filtered_out_narratives[:6]}"
            warnings.append(warning)
            checks.append(ValidationItem(
                check="entity_block_record_flow",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="entity_block_record_flow",
                passed=True,
                message=(
                    "entity_blocks -> filtered_records 链路正常"
                    if narrative_stats else
                    "当前模板未触发 entity block 过滤校验"
                ),
            ))

        attempted_narrative = any(
            stat.extracted_records > 0
            or ("extract" in stat.qwen_stages)
            for stat in narrative_stats
        )
        narrative_contributed = any(
            stat.contributed_records > 0 or stat.contributed_fields > 0 or stat.effective_cell_delta > 0
            for stat in narrative_stats
        )
        if attempted_narrative and not narrative_contributed:
            warning = "已触发叙事型 source 抽取，但最终结果未保留任何非结构化来源贡献"
            warnings.append(warning)
            checks.append(ValidationItem(
                check="unstructured_source_contribution",
                passed=False,
                message=f"[严重] {warning}（存在 relevant narrative source 但最终贡献为 0，fill_rate={result.fill_rate:.1f}%）",
            ))
        else:
            checks.append(ValidationItem(
                check="unstructured_source_contribution",
                passed=True,
                message=(
                    "非结构化来源已贡献最终结果"
                    if narrative_contributed else
                    "当前模板未触发非结构化来源贡献校验"
                ),
            ))

        zero_contribution_narratives = [
            Path(stat.source_file).name
            for stat in narrative_stats
            if stat.extracted_records > 0 and stat.contributed_records == 0 and stat.contributed_fields == 0 and stat.effective_cell_delta == 0
        ]
        if zero_contribution_narratives:
            warning = f"存在已抽取 records 但最终贡献仍为 0 的 narrative source: {zero_contribution_narratives[:6]}"
            warnings.append(warning)
            checks.append(ValidationItem(
                check="narrative_source_lower_bound",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="narrative_source_lower_bound",
                passed=True,
                message="relevant narrative source 均满足最小贡献下限",
            ))

        ineffective_narratives = []
        for stat in narrative_stats:
            stage_audit = stat.stage_audit or {}
            relevant_segments = int(stage_audit.get("relevant_segments", stat.entity_blocks_detected))
            suspicious_records = int(stage_audit.get("suspicious_records", 0))
            llm_records = int(stage_audit.get("llm_records", 0))
            post_entity_records = int(stage_audit.get("post_entity_records", stat.filtered_records))
            final_records = int(stage_audit.get("final_records", stat.contributed_records))
            qwen_extract_ran = "extract" in stat.qwen_stages or llm_records > 0
            if not qwen_extract_ran:
                continue
            if relevant_segments < 3:
                continue
            if stat.extracted_records < 8 and suspicious_records < 4:
                continue
            if stat.effective_cell_delta > 0 or stat.contributed_fields > 0:
                continue
            ineffective_narratives.append(
                f"{Path(stat.source_file).name}: relevant_segments={relevant_segments}, "
                f"extracted={stat.extracted_records}, suspicious={suspicious_records}, "
                f"qwen={llm_records}, post_entity={post_entity_records}, "
                f"contributed_records={stat.contributed_records}, contributed_fields={stat.contributed_fields}, "
                f"effective_cell_delta={stat.effective_cell_delta}"
            )
        if ineffective_narratives:
            warning = "qwen 已执行，但叙事型 source 对最终结果几乎零效果: " + "；".join(ineffective_narratives[:4])
            warnings.append(warning)
            checks.append(ValidationItem(
                check="narrative_pipeline_effectiveness",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="narrative_pipeline_effectiveness",
                passed=True,
                message="未发现 qwen 已执行但 narrative pipeline 几乎零效果的问题",
            ))

        repeated_narrative_failures = [
            Path(stat.source_file).name
            for stat in narrative_stats
            if (
                stat.entity_blocks_detected >= 3
                and stat.filtered_records <= 1
                and stat.contributed_records == 0
                and stat.contributed_fields == 0
            )
        ]
        if repeated_narrative_failures:
            warning = f"重复实体叙事文档抽取记录过少: {repeated_narrative_failures[:6]}"
            warnings.append(warning)
            checks.append(ValidationItem(
                check="repeated_entity_narrative",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="repeated_entity_narrative",
                passed=True,
                message="未发现重复实体叙事抽取过少的问题",
            ))

        per_source_filter_reasons = (result.entity_legality_report or {}).get("per_source_filter_reasons", {})
        uniform_filter_failures = []
        for stat in narrative_stats:
            reason_counts = {
                reason: int(count)
                for reason, count in (per_source_filter_reasons.get(stat.source_file, {}) or {}).items()
                if int(count) > 0
            }
            total_filtered = sum(reason_counts.values())
            if stat.extracted_records > 0 and stat.filtered_records == 0 and total_filtered >= 2 and len(reason_counts) == 1:
                only_reason = next(iter(reason_counts))
                uniform_filter_failures.append(
                    f"{Path(stat.source_file).name}:{describe_entity_reason(only_reason)}"
                )
        if uniform_filter_failures:
            warning = f"某些 narrative source 被同一过滤原因批量清空: {uniform_filter_failures[:6]}"
            warnings.append(warning)
            checks.append(ValidationItem(
                check="uniform_filter_reason",
                passed=False,
                message=warning,
            ))
        else:
            checks.append(ValidationItem(
                check="uniform_filter_reason",
                passed=True,
                message="未发现同一过滤原因批量清空 narrative records",
            ))

    metric_issue = None
    if result.rows_filled > result.expected_rows > 0:
        metric_issue = "rows_filled 大于 expected_rows，指标定义不一致"
    elif result.record_count < result.rows_filled:
        metric_issue = "record_count 小于 rows_filled，说明写回行数超过抽取记录数"
    if metric_issue:
        warnings.append(metric_issue)
    checks.append(ValidationItem(
        check="metric_consistency",
        passed=metric_issue is None,
        message=metric_issue or (
            "Metrics are consistent: "
            f"record_count={result.record_count}, rows_filled={result.rows_filled}, "
            f"expected_rows={result.expected_rows}, fill_rate={result.fill_rate:.1f}%"
        ),
    ))

    row_ids = {
        _row_identifier(field.target_location)
        for field in result.filled_fields
        if field.value not in (None, "", "N/A") and _row_identifier(field.target_location)
    }
    row_count_message = f"写回行标识 {len(row_ids)} 个，rows_filled={result.rows_filled}"
    checks.append(ValidationItem(
        check="writeback_row_count",
        passed=(not row_ids) or len(row_ids) == result.rows_filled,
        message=row_count_message,
    ))

    legality_report = result.entity_legality_report or {}
    surviving_illegal = []
    for field in result.filled_fields:
        if not is_entity_header(field.field_name):
            continue
        if field.match_method == "placeholder_replace" or field.target_location == "word_placeholder":
            continue
        if field.value in (None, "", "N/A"):
            continue
        legal, reason = validate_entity_value(str(field.value), field.field_name)
        if not legal:
            surviving_illegal.append(
                f"{field.field_name}={field.value}({describe_entity_reason(reason)})"
            )
    blocked_examples = legality_report.get("blocked_examples", [])[:3]
    accepted_examples = legality_report.get("accepted_examples", [])[:3]
    if surviving_illegal:
        warning = f"存在未被阻断的非法实体: {surviving_illegal[:4]}"
        warnings.append(warning)
        checks.append(ValidationItem(
            check="entity_legality",
            passed=False,
            message=warning,
        ))
    else:
        blocked_text = "；".join(
            f"{item.get('field_name')}={item.get('value')}({item.get('reason')})"
            for item in blocked_examples
        )
        accepted_text = "；".join(
            f"{item.get('field_name')}={item.get('value')}"
            for item in accepted_examples
        )
        message_parts = []
        if legality_report.get("blocked_count"):
            message_parts.append(
                f"已阻断 {legality_report.get('blocked_count', 0)} 个疑似伪实体"
            )
            if blocked_text:
                message_parts.append(f"阻断示例: {blocked_text}")
        else:
            message_parts.append("未发现需阻断的非法实体")
        if accepted_text:
            message_parts.append(f"保留示例: {accepted_text}")
        checks.append(ValidationItem(
            check="entity_legality",
            passed=True,
            message="；".join(message_parts),
        ))

    conf_vals = [field.confidence for field in result.filled_fields if field.value and field.confidence is not None]
    homogeneous_warning = _confidence_homogeneity_warning(conf_vals)
    if homogeneous_warning:
        warnings.append(homogeneous_warning)
        checks.append(ValidationItem(
            check="confidence_uniformity",
            passed=False,
            message=homogeneous_warning,
        ))
    else:
        checks.append(ValidationItem(
            check="confidence_uniformity",
            passed=True,
            message=f"Confidence spread looks normal ({len({round(c, 2) for c in conf_vals}) if conf_vals else 0} distinct levels)",
        ))

    topic_warning = _topic_mismatch_warning(result)
    if topic_warning:
        warnings.append(topic_warning)
    checks.append(ValidationItem(
        check="source_topic_match",
        passed=topic_warning is None,
        message=topic_warning or "Output topic and evidence sources look consistent",
    ))

    response_time_ok = result.timing.get("total", 0.0) <= 90.0 if result.timing else True
    checks.append(ValidationItem(
        check="response_time",
        passed=response_time_ok,
        message=f"Response time: {result.timing.get('total', 0.0):.1f}s",
    ))

    checks.append(ValidationItem(
        check="evidence_report",
        passed=bool(result.evidence_report) or result.record_count == 0,
        message=f"Evidence items: {len(result.evidence_report)}",
    ))

    total_calls = int(model_usage.get("total_calls", 0)) if isinstance(model_usage, dict) else 0
    model_called = bool(model_usage.get("called", False)) if isinstance(model_usage, dict) else False
    model_validation_errors = list(model_usage.get("validation_errors", [])) if isinstance(model_usage, dict) else []
    if model_called and total_calls == 0:
        warning = "后端标记模型已使用，但 total_model_calls == 0"
        warnings.append(warning)
        checks.append(ValidationItem(
            check="model_usage_truthfulness",
            passed=False,
            message=warning,
        ))
    else:
        checks.append(ValidationItem(
            check="model_usage_truthfulness",
            passed=True,
            message=(
                f"Model usage consistent: called={model_called}, total_calls={total_calls}"
                if model_called or total_calls else
                "当前结果没有真实模型调用，UI 必须显示 model not used"
            ),
        ))

    if model_validation_errors:
        warning = "；".join(model_validation_errors[:4])
        warnings.append(warning)
        checks.append(ValidationItem(
            check="model_usage_validation",
            passed=False,
            message=warning,
        ))
    else:
        checks.append(ValidationItem(
            check="model_usage_validation",
            passed=True,
            message="No backend model-usage validation errors",
        ))

    trace_file = model_usage.get("trace_file", "") if isinstance(model_usage, dict) else ""
    trace_exists = bool(trace_file) and os.path.exists(trace_file)
    checks.append(ValidationItem(
        check="model_trace_file",
        passed=(not model_called and not total_calls) or trace_exists,
        message=(
            f"Model trace file exists: {trace_file}"
            if trace_exists else
            "模型调用后未找到可读 trace 文件"
        ),
    ))

    result.validation_report = checks
    result.warnings = _deduplicate_text(warnings)

    apply_validation_outcome(result)

    passed = sum(1 for item in checks if item.passed)
    total = len(checks)
    logger.info("  -> Validation: %s/%s checks passed", passed, total)
    return result


def _row_identifier(target_location: str) -> str:
    """Extract a stable row identifier from target locations."""
    if not target_location:
        return ""
    excel_match = re.match(r'([^!]+)!([A-Z]+)(\d+)$', target_location)
    if excel_match:
        return f"{excel_match.group(1)}!{excel_match.group(3)}"
    word_match = re.search(r'(table\d+\.row\d+)', target_location)
    if word_match:
        return word_match.group(1)
    generic_match = re.search(r'([a-z_]+\d*\.row\d+)', target_location)
    return generic_match.group(1) if generic_match else ""


def _confidence_homogeneity_warning(conf_vals: list[float | None]) -> str | None:
    """Return a warning string when confidence values are suspiciously uniform."""
    values = [float(value) for value in conf_vals if value is not None]
    if len(values) < 5:
        return None
    rounded = {round(value, 2) for value in values}
    spread = max(values) - min(values)
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    std_dev = math.sqrt(variance)
    # Only flag when spread AND std_dev are both tiny (requiring both conditions prevents
    # false positives when entity vs. metric columns have legitimately different confidence levels)
    if spread < 0.04 and std_dev < 0.015:
        return (
            f"Confidence 过于集中，范围 {min(values):.2f}-{max(values):.2f}，"
            f"离散度 {std_dev:.3f}"
        )
    return None


def _topic_mismatch_warning(result: FilledResult) -> str | None:
    """Warn when source topics and output values look obviously inconsistent."""
    template_stem = Path(result.template_file).stem
    template_core = re.sub(r'[-_ ]?模板$', '', template_stem)
    normalized_template_core = _normalize_topic_text(template_core)
    template_tokens = _meaningful_tokens(template_core)
    contributing_sources = [
        Path(stat.source_file).stem
        for stat in result.source_stats
        if stat.extracted_records > 0 or stat.contributed_fields > 0
    ]
    if any(
        normalized_template_core and (
            normalized_template_core in _normalize_topic_text(source_stem)
            or _normalize_topic_text(source_stem) in normalized_template_core
        )
        for source_stem in contributing_sources
    ):
        return None

    source_tokens = {
        token
        for stat in result.source_stats
        for token in _meaningful_tokens(Path(stat.source_file).stem)
        if stat.extracted_records > 0 or stat.contributed_fields > 0
    }
    mismatched_sources = [
        Path(stat.source_file).name
        for stat in result.source_stats
        if (stat.extracted_records > 0 or stat.contributed_fields > 0)
        and template_tokens
        and not (_meaningful_tokens(Path(stat.source_file).stem) & template_tokens)
    ]
    matched_sources = [
        Path(stat.source_file).name
        for stat in result.source_stats
        if (stat.extracted_records > 0 or stat.contributed_fields > 0)
        and template_tokens
        and (_meaningful_tokens(Path(stat.source_file).stem) & template_tokens)
    ]
    if mismatched_sources and matched_sources:
        return (
            f"模板 {Path(result.template_file).name} 存在主题不一致的数据源贡献: "
            f"{mismatched_sources[:4]}"
        )
    if not template_tokens or not source_tokens:
        return None
    if template_tokens & source_tokens:
        return None
    template_name = Path(result.template_file).name
    return f"模板 {template_name} 的主题词与实际贡献数据源差异较大，请检查是否发生来源错配"


def _meaningful_tokens(text: str) -> set[str]:
    """Extract lightweight topic tokens."""
    tokens = set(re.findall(r'[\u4e00-\u9fa5A-Za-z0-9]{2,12}', text or ""))
    return {token.lower() for token in tokens if not token.isdigit()}


def _normalize_topic_text(text: str) -> str:
    """Normalize topic text for loose source-template matching."""
    return re.sub(r'[\d_\- ]+', '', (text or "").lower())


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
