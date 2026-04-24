"""Generic data quality detection for source data and filled outputs."""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from ..schemas.models import DocumentBundle, FilledResult
from ..schemas.quality_models import QualityIssue, QualityReport
from ..utils.text_utils import clean_cell_value
from .normalization_service import is_null_value, normalize_value
from .schema_registry_service import infer_field_type, normalize_field_name, registry_quality_rule


def analyze_documents(documents: list[DocumentBundle]) -> QualityReport:
    issues: list[QualityIssue] = []
    source_type_distribution = Counter(document.file_type for document in documents)
    cross_values: dict[tuple[str, str], list[tuple[str, str, str]]] = defaultdict(list)

    for document in documents:
        for table in document.tables:
            headers = table.headers
            duplicate_counter: Counter[str] = Counter()
            numeric_values: dict[str, list[float]] = defaultdict(list)
            normalized_rows: list[dict[str, Any]] = []
            for row_index, row in enumerate(table.rows):
                normalized_row: dict[str, Any] = {}
                row_key_parts = []
                for col_index, header in enumerate(headers):
                    raw_value = clean_cell_value(row[col_index] if col_index < len(row) else "")
                    location = f"table{table.table_index}.row{row_index + 1}.col{col_index + 1}"
                    normalized = normalize_value(raw_value, header)
                    normalized_row[header] = normalized
                    if col_index < 2 and raw_value:
                        row_key_parts.append(normalize_field_name(raw_value))
                    if is_null_value(raw_value):
                        issues.append(_issue(
                            "missing_value", "warning", header, raw_value, normalized.get("standard_value"),
                            document.source_file, location, "字段为空或属于空值标记", "核对来源或允许模板留空", True,
                        ))
                    elif normalized.get("status") == "type_error":
                        issue_type = "date_format_error" if normalized.get("field_type") == "date" else "type_error"
                        issues.append(_issue(
                            issue_type, "warning", header, raw_value, normalized.get("standard_value"),
                            document.source_file, location, f"{normalized.get('field_type')} 类型标准化失败", "修正格式或调整字段类型规则", True,
                        ))
                    if normalized.get("field_type") in {"number", "currency", "percent"} and isinstance(normalized.get("standard_value"), (int, float)):
                        value = float(normalized["standard_value"])
                        numeric_values[header].append(value)
                        if value < 0 and "率" not in header:
                            issues.append(_issue(
                                "range_anomaly", "warning", header, raw_value, normalized.get("standard_value"),
                                document.source_file, location, "数值为负，可能超出常规业务范围", "确认该字段是否允许负值", True,
                            ))
                    normalized_key = normalize_field_name(row_key_parts[0]) if row_key_parts else ""
                    if normalized_key:
                        field_key = normalize_field_name(header)
                        cross_values[(normalized_key, field_key)].append((str(normalized.get("standard_value", raw_value)), document.source_file, location))
                normalized_rows.append(normalized_row)
                row_fingerprint = "|".join(
                    normalize_field_name(clean_cell_value(cell))
                    for cell in row
                    if clean_cell_value(cell)
                )
                if row_fingerprint:
                    duplicate_counter[row_fingerprint] += 1

            for fingerprint, count in duplicate_counter.items():
                if count > 1:
                    issues.append(_issue(
                        "duplicate_record", "warning", "", fingerprint, fingerprint,
                        document.source_file, f"table{table.table_index}", f"检测到 {count} 条重复记录", "保留高优先级或高置信来源记录", True,
                    ))
            issues.extend(_numeric_outliers(document, table.table_index, headers, table.rows, numeric_values))

    for (entity_key, field_key), values in cross_values.items():
        unique_values = {normalize_field_name(value) for value, _source, _location in values if value}
        sources = {source for _value, source, _location in values}
        if len(unique_values) > 1 and len(sources) > 1:
            sample = values[:4]
            issues.append(_issue(
                "cross_source_conflict", "error", field_key, [item[0] for item in sample], "",
                "；".join(sorted(sources)[:4]), sample[0][2] if sample else "",
                "同一实体字段在多个来源中存在不同标准值", "按来源优先级或人工核验后选择可信值", True,
            ))

    return _report(issues, {
        "source_count": len(documents),
        "source_type_distribution": dict(source_type_distribution),
        "table_count": sum(len(document.tables) for document in documents),
        "text_block_count": sum(len(document.text_blocks) for document in documents),
    })


def analyze_filled_result(result: FilledResult, base_report: QualityReport | dict[str, Any] | None = None) -> QualityReport:
    if isinstance(base_report, QualityReport):
        issues = list(base_report.issues)
    elif isinstance(base_report, dict):
        issues = [QualityIssue(**item) for item in base_report.get("issues", []) if isinstance(item, dict)]
    else:
        issues = []
    low_threshold = float(registry_quality_rule("low_confidence_threshold", 0.45))
    for field in result.filled_fields:
        raw_value = field.value
        normalized = normalize_value(raw_value, field.field_name)
        if raw_value not in (None, "", "N/A") and (not field.evidence or not field.source_file):
            issues.append(_issue(
                "value_without_evidence", "error", field.field_name, raw_value, normalized.get("standard_value"),
                field.source_file, field.target_location, "字段有值但无完整证据链", "回溯原始数据或将该字段标为待核验", True,
                confidence=field.confidence,
            ))
        if field.confidence is not None and field.confidence < low_threshold and raw_value not in (None, "", "N/A"):
            issues.append(_issue(
                "low_confidence", "warning", field.field_name, raw_value, normalized.get("standard_value"),
                field.source_file, field.target_location, "字段置信度低于阈值", "优先人工复核该字段", True,
                confidence=field.confidence,
            ))
        if raw_value in (None, "", "N/A"):
            issues.append(_issue(
                "missing_filled_value", "warning", field.field_name, raw_value, normalized.get("standard_value"),
                field.source_file, field.target_location, "模板目标字段未填充", "补充数据源或调整字段别名映射", True,
                confidence=field.confidence,
            ))
    return _report(issues, {
        "filled_field_count": len(result.filled_fields),
        "rows_filled": result.rows_filled,
        "fill_rate": result.fill_rate,
    })


def quality_report_to_dict(report: QualityReport) -> dict[str, Any]:
    return report.model_dump()


def _numeric_outliers(document: DocumentBundle, table_index: int, headers: list[str], rows: list[list[str]], values_by_header: dict[str, list[float]]) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    z_threshold = float(registry_quality_rule("numeric_outlier_zscore", 3.5))
    for header, values in values_by_header.items():
        if len(values) < 5:
            continue
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        std = math.sqrt(variance)
        if std <= 0:
            continue
        col_index = headers.index(header)
        for row_index, row in enumerate(rows):
            if col_index >= len(row):
                continue
            normalized = normalize_value(row[col_index], header)
            value = normalized.get("standard_value")
            if not isinstance(value, (int, float)):
                continue
            z_score = abs((float(value) - mean) / std)
            if z_score >= z_threshold:
                issues.append(_issue(
                    "numeric_anomaly", "warning", header, row[col_index], value,
                    document.source_file, f"table{table_index}.row{row_index + 1}.col{col_index + 1}",
                    f"数值偏离均值 {z_score:.2f} 个标准差", "核验是否为极端真实值或录入错误", True,
                ))
    return issues


def _issue(
    issue_type: str,
    severity: str,
    field_name: str,
    raw_value: Any,
    normalized_value: Any,
    source: str,
    location: str,
    reason: str,
    suggestion: str,
    affects_fill: bool,
    confidence: float | None = None,
) -> QualityIssue:
    return QualityIssue(
        issue_type=issue_type,
        severity=severity,
        field_name=field_name,
        raw_value=raw_value,
        normalized_value=normalized_value,
        source=source,
        location=location,
        reason=reason,
        suggestion=suggestion,
        affects_fill=affects_fill,
        confidence=confidence,
    )


def _report(issues: list[QualityIssue], extra_summary: dict[str, Any]) -> QualityReport:
    actual_count = len(issues)
    type_counts = Counter(issue.issue_type for issue in issues)
    severity_counts = Counter(issue.severity for issue in issues)
    affects_fill = sum(1 for issue in issues if issue.affects_fill)
    max_issues = 1000
    summary = {
        **extra_summary,
        "issue_count": actual_count,
        "returned_issue_count": min(actual_count, max_issues),
        "truncated": actual_count > max_issues,
        "affects_fill_count": affects_fill,
        "issue_type_distribution": dict(type_counts),
        "severity_distribution": dict(severity_counts),
    }
    return QualityReport(issues=issues[:max_issues], summary=summary)
