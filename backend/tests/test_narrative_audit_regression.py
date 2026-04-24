"""Regression tests for narrative stage-audit visibility and zero-effect validation."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.schemas.models import (
    CandidateEvidence,
    DocumentBundle,
    FileRole,
    FilledFieldResult,
    FilledResult,
    ModelUsageSummary,
    SourceProcessingStat,
)
from app.services.pipeline_service import _build_source_stats, _refresh_response_time_validation
from app.services.retrieval_service import RetrievalResult
from app.services.validation_service import validate_result


def _make_evidence(source_file: str, location: str, snippet: str, reason: str, confidence: float = 0.8) -> CandidateEvidence:
    return CandidateEvidence(
        source_file=source_file,
        location=location,
        raw_snippet=snippet,
        match_reason=reason,
        confidence=confidence,
    )


def _make_zero_effect_stat(source_file: str = "/tmp/source-alpha.docx") -> SourceProcessingStat:
    return SourceProcessingStat(
        source_file=source_file,
        file_type="word",
        text_blocks=31,
        entity_blocks_detected=30,
        relevant_to_template=True,
        relevance_score=1.5,
        extracted_records=37,
        filtered_records=1,
        contributed_records=0,
        contributed_fields=0,
        evidence_contribution_fields=3,
        value_contribution_fields=0,
        row_contribution_records=0,
        effective_cell_delta=0,
        qwen_used=True,
        qwen_call_count=2,
        qwen_stages=["extract"],
        stage_audit={
            "relevant_segments": 30,
            "rule_records": 29,
            "stable_records": 1,
            "suspicious_records": 28,
            "llm_records": 8,
            "post_entity_records": 1,
            "final_records": 1,
        },
        narrative_audit={
            "raw_narrative_records": {
                "total": 37,
                "rule_records": 29,
                "qwen_records": 8,
                "stable_records": 1,
                "suspicious_records": 28,
            },
            "merge_outcome": {
                "merged_into_existing_row": 0,
                "emitted_as_standalone_row": 0,
                "evidence_only": 1,
                "discarded": 36,
            },
            "effective_cell_delta": {
                "changed_cells": 0,
                "changed_rows": 0,
                "changed_field_names": [],
            },
        },
    )


def _make_meaningful_stat(source_file: str = "/tmp/source-alpha.docx") -> SourceProcessingStat:
    return SourceProcessingStat(
        source_file=source_file,
        file_type="word",
        text_blocks=8,
        entity_blocks_detected=4,
        relevant_to_template=True,
        relevance_score=1.5,
        extracted_records=6,
        filtered_records=2,
        contributed_records=1,
        contributed_fields=4,
        evidence_contribution_fields=4,
        value_contribution_fields=4,
        row_contribution_records=1,
        effective_cell_delta=4,
        qwen_used=True,
        qwen_call_count=1,
        qwen_stages=["extract"],
        stage_audit={
            "relevant_segments": 4,
            "rule_records": 3,
            "stable_records": 1,
            "suspicious_records": 2,
            "llm_records": 1,
            "post_entity_records": 2,
            "final_records": 1,
        },
        narrative_audit={
            "effective_cell_delta": {
                "changed_cells": 4,
                "changed_rows": 1,
                "changed_field_names": ["病例数", "人口"],
            },
            "merge_outcome": {
                "merged_into_existing_row": 1,
                "emitted_as_standalone_row": 0,
                "evidence_only": 0,
                "discarded": 2,
            },
        },
    )


def test_narrative_stage_audit_visibility():
    """Per-source stats should surface narrative stage counts from extraction output."""
    narrative_doc = DocumentBundle(
        document_id="narrative",
        source_file="/tmp/source-alpha.docx",
        file_type="word",
        role=FileRole.SOURCE,
    )
    structured_doc = DocumentBundle(
        document_id="structured",
        source_file="/tmp/source-beta.xlsx",
        file_type="excel",
        role=FileRole.SOURCE,
    )
    retrieval = RetrievalResult()
    retrieval.source_docs = [narrative_doc, structured_doc]
    extracted = [{
        "source_counts": {narrative_doc.source_file: 37, structured_doc.source_file: 2542},
        "filtered_source_counts": {narrative_doc.source_file: 1, structured_doc.source_file: 2542},
        "entity_block_counts": {narrative_doc.source_file: 30},
        "relevant_source_files": [narrative_doc.source_file, structured_doc.source_file],
        "narrative_stage_audit": {
            narrative_doc.source_file: {
                "relevant_segments": 30,
                "rule_records": 29,
                "stable_records": 1,
                "suspicious_records": 28,
                "llm_records": 8,
                "post_entity_records": 1,
                "final_records": 1,
            }
        },
    }]
    filled = FilledResult(
        template_file="/tmp/template.xlsx",
        rows_filled=1,
        record_count=1,
        expected_rows=1,
        fill_rate=100.0,
        filled_fields=[
            FilledFieldResult(
                field_name="病例数",
                target_location="Sheet1!F2",
                value="68",
                normalized_value="68",
                source_file=narrative_doc.source_file,
                evidence=[
                    _make_evidence(narrative_doc.source_file, "text_block1", "中国报告 68 例", "summary", 0.82),
                ],
            ),
        ],
        model_usage=ModelUsageSummary(),
    )

    stats = _build_source_stats(
        [narrative_doc, structured_doc],
        retrieval,
        extracted,
        filled,
        "template.xlsx",
        model_usage=filled.model_usage,
    )
    per_source = {item.source_file: item for item in stats}

    assert per_source[narrative_doc.source_file].stage_audit["suspicious_records"] == 28
    assert per_source[narrative_doc.source_file].stage_audit["llm_records"] == 8
    assert per_source[narrative_doc.source_file].stage_audit["final_records"] == 1

    print("✓ test_narrative_stage_audit_visibility passed")


def test_narrative_supplement_contribution_preserved():
    """Meaningful narrative supplements should not be mislabeled as a zero-effect qwen pipeline."""
    result = FilledResult(
        template_file=__file__,
        output_file=__file__,
        rows_filled=1,
        record_count=1,
        expected_rows=1,
        fill_rate=100.0,
        source_stats=[_make_meaningful_stat()],
    )

    validated = validate_result(result)
    assert any(
        item.check == "narrative_pipeline_effectiveness" and item.passed
        for item in validated.validation_report
    )

    print("✓ test_narrative_supplement_contribution_preserved passed")


def test_narrative_not_silently_zeroed_after_qwen():
    """A qwen-driven narrative pipeline that only leaves a weak summary row must fail validation."""
    result = FilledResult(
        template_file=__file__,
        output_file=__file__,
        rows_filled=10,
        record_count=10,
        expected_rows=10,
        fill_rate=100.0,
        source_stats=[
            _make_zero_effect_stat(),
            SourceProcessingStat(
                source_file="/tmp/source-beta.xlsx",
                file_type="excel",
                relevant_to_template=True,
                extracted_records=10,
                filtered_records=10,
                contributed_records=10,
                contributed_fields=60,
            ),
        ],
    )

    validated = validate_result(result)
    assert any(
        item.check == "narrative_pipeline_effectiveness" and not item.passed
        for item in validated.validation_report
    )

    print("✓ test_narrative_not_silently_zeroed_after_qwen passed")


def test_validation_flags_zero_effect_narrative_pipeline():
    """Zero-effect narrative pipelines should flip the final result out of completed status."""
    result = FilledResult(
        template_file=__file__,
        output_file=__file__,
        rows_filled=10,
        record_count=10,
        expected_rows=10,
        fill_rate=100.0,
        source_stats=[
            _make_zero_effect_stat(),
            SourceProcessingStat(
                source_file="/tmp/source-beta.xlsx",
                file_type="excel",
                relevant_to_template=True,
                extracted_records=10,
                filtered_records=10,
                contributed_records=10,
                contributed_fields=60,
            ),
        ],
    )

    validated = validate_result(result)
    assert validated.status == "error"
    assert validated.meets_minimum is False

    print("✓ test_validation_flags_zero_effect_narrative_pipeline passed")


def test_no_dataset_specific_hardcoding():
    """The zero-effect validation must trigger on generic filenames, not only COVID-specific paths."""
    result = FilledResult(
        template_file=__file__,
        output_file=__file__,
        rows_filled=6,
        record_count=6,
        expected_rows=6,
        fill_rate=100.0,
        source_stats=[
            _make_zero_effect_stat("/tmp/generic-alpha.docx"),
            SourceProcessingStat(
                source_file="/tmp/generic-beta.xlsx",
                file_type="excel",
                relevant_to_template=True,
                extracted_records=6,
                filtered_records=6,
                contributed_records=6,
                contributed_fields=36,
            ),
        ],
    )

    validated = validate_result(result)
    matched = [
        item.message
        for item in validated.validation_report
        if item.check == "narrative_pipeline_effectiveness" and not item.passed
    ]
    assert matched
    assert "generic-alpha.docx" in matched[0]

    print("✓ test_no_dataset_specific_hardcoding passed")


def test_response_time_refresh_keeps_validation_hard_failure():
    """Late response-time refresh must not overwrite prior hard validation failures."""
    result = FilledResult(
        template_file=__file__,
        output_file=__file__,
        rows_filled=10,
        record_count=10,
        expected_rows=10,
        fill_rate=100.0,
        timing={"total": 12.0},
        source_stats=[
            _make_zero_effect_stat(),
            SourceProcessingStat(
                source_file="/tmp/source-beta.xlsx",
                file_type="excel",
                relevant_to_template=True,
                extracted_records=10,
                filtered_records=10,
                contributed_records=10,
                contributed_fields=60,
            ),
        ],
    )

    validated = validate_result(result)
    assert validated.status == "error"

    _refresh_response_time_validation(validated)

    assert validated.status == "error"
    assert validated.meets_minimum is False
    assert any(
        item.check == "narrative_pipeline_effectiveness" and not item.passed
        for item in validated.validation_report
    )

    print("✓ test_response_time_refresh_keeps_validation_hard_failure passed")


if __name__ == "__main__":
    test_narrative_stage_audit_visibility()
    test_narrative_supplement_contribution_preserved()
    test_narrative_not_silently_zeroed_after_qwen()
    test_validation_flags_zero_effect_narrative_pipeline()
    test_no_dataset_specific_hardcoding()
    test_response_time_refresh_keeps_validation_hard_failure()
    print("\n✅ narrative audit regression tests passed!")
