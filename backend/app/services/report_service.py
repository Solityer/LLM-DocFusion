"""Task report generation for API and frontend display."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from ..core.config import OUTPUT_DIR
from ..schemas.models import ProcessResponse


def build_task_report(response: ProcessResponse) -> dict[str, Any]:
    results = response.results or []
    quality_issues = [
        issue
        for result in results
        for issue in ((result.quality_report or {}).get("issues", []) if isinstance(result.quality_report, dict) else [])
    ]
    source_stats = [stat for result in results for stat in (result.source_stats or [])]
    source_types = Counter(stat.file_type for stat in source_stats)
    validation_failures = [
        item
        for result in results
        for item in result.validation_report
        if not item.passed
    ]
    report = {
        "task_id": response.task_id,
        "status": response.status,
        "data_source_count": len({stat.source_file for stat in source_stats}),
        "source_type_distribution": dict(source_types),
        "template_count": len(response.template_statuses or results),
        "field_match_rate": _average([_field_match_rate(result) for result in results]),
        "fill_rate": _average([result.fill_rate for result in results]),
        "quality_issue_count": len(quality_issues),
        "quality_issue_distribution": dict(Counter(issue.get("issue_type", "") for issue in quality_issues if isinstance(issue, dict))),
        "low_confidence_fields": [
            {
                "template_file": result.template_file,
                "field_name": field.field_name,
                "target_location": field.target_location,
                "confidence": field.confidence,
            }
            for result in results
            for field in result.filled_fields
            if field.confidence is not None and field.confidence < 0.45
        ][:100],
        "conflict_fields": _collect_conflicts(results),
        "response_time": response.finished_at - response.started_at if response.finished_at and response.started_at else None,
        "llm_call_count": response.model_usage.total_calls if response.model_usage else 0,
        "validation_failures": [item.model_dump() for item in validation_failures[:100]],
        "before_after": [
            {
                "template_file": result.template_file,
                "output_file": result.output_file,
                "rows_filled": result.rows_filled,
                "field_count": len(result.filled_fields),
                "fill_rate": result.fill_rate,
            }
            for result in results
        ],
        "outputs": [result.output_file for result in results if result.output_file],
        "warnings": response.warnings[:100],
    }
    report_path = OUTPUT_DIR / f"report_{response.task_id}.json"
    try:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        report["report_file"] = str(report_path)
    except Exception:
        report["report_file"] = ""
    return report


def _field_match_rate(result) -> float:
    total = len(result.filled_fields)
    if not total:
        return 0.0
    matched = sum(1 for field in result.filled_fields if field.match_method and field.value not in (None, "", "N/A"))
    return round(matched / total * 100, 4)


def _average(values: list[float]) -> float:
    valid = [float(value) for value in values if value is not None]
    return round(sum(valid) / len(valid), 4) if valid else 0.0


def _collect_conflicts(results) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for result in results:
        fusion_report = result.fusion_report or {}
        for conflict in fusion_report.get("conflicts", [])[:50]:
            conflicts.append({"template_file": result.template_file, **conflict})
    return conflicts[:100]
