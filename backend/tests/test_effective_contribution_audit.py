"""Regression tests for effective contribution audit and narrative loss accounting."""
from collections import Counter
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.schemas.models import CandidateEvidence, DocumentBundle, FilledFieldResult, FilledResult, SourceProcessingStat
from app.services.extraction_service import (
    _annotate_records_with_entity_semantics,
    _filter_records_by_entity_legality,
    _filter_records_by_template_context,
    _merge_filter_diagnostics,
)
from app.services.pipeline_service import (
    _build_narrative_stage_loss_ledger,
    _build_source_stats,
    _classify_narrative_merge_outcomes,
    _compute_effective_cell_delta,
)
from app.services.retrieval_service import RetrievalResult
from app.services.validation_service import validate_result


def _make_evidence(source_file: str, snippet: str, reason: str, location: str = "text_block1", confidence: float = 0.8):
    return CandidateEvidence(
        source_file=source_file,
        location=location,
        raw_snippet=snippet,
        match_reason=reason,
        confidence=confidence,
    )


def test_effective_contribution_distinguishes_evidence_vs_value():
    """Narrative evidence should not be counted as a value contribution when the chosen value stays structured."""
    narrative_doc = DocumentBundle(source_file="/tmp/source-alpha.docx", file_type="word")
    structured_doc = DocumentBundle(source_file="/tmp/source-beta.xlsx", file_type="excel")
    retrieval = RetrievalResult()
    retrieval.source_docs = [narrative_doc, structured_doc]
    extracted = [{
        "source_counts": {narrative_doc.source_file: 3, structured_doc.source_file: 1},
        "filtered_source_counts": {narrative_doc.source_file: 1, structured_doc.source_file: 1},
        "entity_block_counts": {narrative_doc.source_file: 3},
        "relevant_source_files": [narrative_doc.source_file, structured_doc.source_file],
        "narrative_stage_audit": {
            narrative_doc.source_file: {
                "relevant_segments": 3,
                "rule_records": 2,
                "stable_records": 1,
                "suspicious_records": 1,
                "llm_records": 1,
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
                source_file=structured_doc.source_file,
                evidence=[
                    _make_evidence(structured_doc.source_file, "China 68", "table"),
                    _make_evidence(narrative_doc.source_file, "中国 68 例", "narrative"),
                ],
                supporting_sources=[structured_doc.source_file, narrative_doc.source_file],
                value_sources=[structured_doc.source_file],
                value_record_ids=["structured-1"],
            ),
        ],
        effective_contribution_audit={
            "per_source": {
                narrative_doc.source_file: {
                    "effective_cell_delta": {"changed_cells": 0, "changed_rows": 0, "changed_field_names": []},
                }
            }
        },
    )

    stats = _build_source_stats(
        [narrative_doc, structured_doc],
        retrieval,
        extracted,
        filled,
        "template.xlsx",
        model_usage={},
    )
    per_source = {item.source_file: item for item in stats}

    assert per_source[narrative_doc.source_file].evidence_contribution_fields == 1
    assert per_source[narrative_doc.source_file].value_contribution_fields == 0
    assert per_source[narrative_doc.source_file].contributed_fields == 0


def test_false_positive_contribution_is_flagged():
    """A source that claims value contribution without any effective cell delta must fail validation."""
    result = FilledResult(
        template_file=__file__,
        output_file=__file__,
        rows_filled=1,
        record_count=1,
        expected_rows=1,
        fill_rate=100.0,
        source_stats=[
            SourceProcessingStat(
                source_file="/tmp/source-alpha.docx",
                file_type="word",
                relevant_to_template=True,
                extracted_records=6,
                filtered_records=1,
                contributed_records=1,
                contributed_fields=3,
                evidence_contribution_fields=3,
                value_contribution_fields=3,
                row_contribution_records=1,
                effective_cell_delta=0,
                qwen_used=True,
                qwen_stages=["extract"],
            ),
        ],
        effective_contribution_audit={
            "errors": ["source-alpha.docx: contributed_fields=3 但 effective_cell_delta=0"],
        },
    )

    validated = validate_result(result)
    assert any(
        item.check == "effective_contribution_alignment" and not item.passed
        for item in validated.validation_report
    )
    assert validated.status == "error"


def test_post_filter_loss_accounting_for_narrative_records():
    """Template-context and entity-legality drops should both remain auditable by stage."""
    headers = ["国家", "大洲", "病例数"]
    template_context = {
        "template_file": "/tmp/COVID-19 模板.xlsx",
        "topic_tokens": {"疫情", "病例", "covid"},
        "anchor_tokens": {"covid"},
        "anchor_domains": {"epidemic"},
        "filter_hints": {},
    }
    records = [
        {
            "record_id": "topic-drop",
            "values": {"国家": "南京市", "大洲": "亚洲", "病例数": "10"},
            "source_file": "/tmp/财政报告.docx",
            "source_location": "text_block1",
            "field_evidence": {
                "国家": [_make_evidence("/tmp/财政报告.docx", "财政收入 10", "economy")],
                "大洲": [_make_evidence("/tmp/财政报告.docx", "财政收入 10", "economy")],
                "病例数": [_make_evidence("/tmp/财政报告.docx", "财政收入 10", "economy")],
            },
            "match_methods": {"国家": "text_rule", "大洲": "text_rule", "病例数": "text_rule"},
        },
        {
            "record_id": "entity-drop",
            "values": {"国家": "河南省", "大洲": "亚洲", "病例数": "10"},
            "source_file": "/tmp/疫情纪实.docx",
            "source_location": "text_block2",
            "field_evidence": {
                "国家": [_make_evidence("/tmp/疫情纪实.docx", "河南省 10 例", "narrative")],
                "大洲": [_make_evidence("/tmp/疫情纪实.docx", "河南省 10 例", "narrative")],
                "病例数": [_make_evidence("/tmp/疫情纪实.docx", "河南省 10 例", "narrative")],
            },
            "match_methods": {"国家": "text_rule", "大洲": "text_rule", "病例数": "text_rule"},
        },
        {
            "record_id": "kept",
            "values": {"国家": "中国", "大洲": "亚洲", "病例数": "68"},
            "source_file": "/tmp/疫情纪实.docx",
            "source_location": "text_block3",
            "field_evidence": {
                "国家": [_make_evidence("/tmp/疫情纪实.docx", "中国 68 例", "narrative")],
                "大洲": [_make_evidence("/tmp/疫情纪实.docx", "中国 68 例", "narrative")],
                "病例数": [_make_evidence("/tmp/疫情纪实.docx", "中国 68 例", "narrative")],
            },
            "match_methods": {"国家": "text_rule", "大洲": "text_rule", "病例数": "text_rule"},
        },
    ]

    warnings: list[str] = []
    topic_filtered, topic_diag = _filter_records_by_template_context(records, template_context, warnings)
    annotated = _annotate_records_with_entity_semantics(headers, topic_filtered, template_context)
    entity_filtered, _invalidated_sources, entity_diag = _filter_records_by_entity_legality(
        headers,
        annotated,
        warnings,
        template_context=template_context,
    )
    merged = _merge_filter_diagnostics(topic_diag, entity_diag)

    assert len(entity_filtered) == 1
    assert merged["stage_loss_counts"]["template_context"] == 1
    assert merged["stage_loss_counts"]["entity_legality"] == 1
    assert merged["stage_reason_counts"]["template_context"]["template_topic_mismatch"] == 1
    assert merged["stage_reason_counts"]["entity_legality"]["region_field_country_only"] == 1
    assert topic_diag["per_source_stage"]["/tmp/疫情纪实.docx"]["template_context"]["remaining"] == 2
    assert entity_diag["per_source_stage"]["/tmp/疫情纪实.docx"]["entity_legality"]["remaining"] == 1


def test_qwen_narrative_records_not_silently_disappear():
    """qwen-origin narrative rows must end up classified, not silently vanish from audit."""
    source_file = "/tmp/source-alpha.docx"
    extracted = [{
        "narrative_record_registry": {
            source_file: [
                {"record_id": "rule-1", "record_origin": "rule", "quality_bucket": "stable"},
                {"record_id": "qwen-1", "record_origin": "qwen", "quality_bucket": "qwen"},
            ]
        },
        "records": [
            {"origin_record_ids": ["rule-1"]},
        ],
    }]

    outcomes = _classify_narrative_merge_outcomes(
        extracted,
        {
            "baseline_row_ids": set(),
            "changed_record_rows": {"rule-1": {"Sheet1!2"}},
        },
    )

    assert outcomes[source_file]["emitted_as_standalone_row"] == 1
    assert outcomes[source_file]["discarded"] == 1
    assert outcomes[source_file]["evidence_only"] == 0


def test_multisource_delta_audit():
    """Baseline vs multi-source comparison should report only real visible cell deltas."""
    baseline = FilledResult(
        filled_fields=[
            FilledFieldResult(
                field_name="国家/地区",
                target_location="Sheet1!A2",
                value="Albania",
                normalized_value="Albania",
                value_sources=["/tmp/source-beta.xlsx"],
            ),
        ],
    )
    multisource = FilledResult(
        filled_fields=[
            FilledFieldResult(
                field_name="国家/地区",
                target_location="Sheet1!A2",
                value="Albania",
                normalized_value="Albania",
                value_sources=["/tmp/source-beta.xlsx"],
            ),
            FilledFieldResult(
                field_name="国家/地区",
                target_location="Sheet1!A3",
                value="中国",
                normalized_value="中国",
                value_sources=["/tmp/source-alpha.docx"],
                value_record_ids=["narr-1"],
            ),
            FilledFieldResult(
                field_name="大洲",
                target_location="Sheet1!B3",
                value="亚洲",
                normalized_value="亚洲",
                value_sources=["/tmp/source-alpha.docx"],
                value_record_ids=["narr-1"],
            ),
            FilledFieldResult(
                field_name="病例数",
                target_location="Sheet1!F3",
                value="68",
                normalized_value="68",
                value_sources=["/tmp/source-alpha.docx"],
                value_record_ids=["narr-1"],
            ),
        ],
    )

    audit = _compute_effective_cell_delta(baseline, multisource)

    assert audit["changed_cells"] == 3
    assert audit["changed_rows"] == 1
    assert audit["per_source"]["/tmp/source-alpha.docx"]["changed_cells"] == 3
    assert audit["per_source"]["/tmp/source-alpha.docx"]["changed_rows"] == 1


def test_narrative_stage_loss_ledger_accounts_qwen_and_materialization_losses():
    """Narrative loss ledger should explicitly account for qwen attrition and final materialization loss."""
    ledger = _build_narrative_stage_loss_ledger(
        Counter({
            "total": 10,
            "rule_records": 7,
            "qwen_records": 3,
            "stable_records": 2,
            "suspicious_records": 7,
        }),
        Counter({
            "llm_records": 3,
            "post_entity_records": 3,
            "final_records": 2,
        }),
        {
            "template_context": {"dropped": 1, "remaining": 5, "reason_counts": {"template_topic_mismatch": 1}},
            "entity_legality": {"dropped": 2, "remaining": 3, "reason_counts": {"region_field_country_only": 2}},
        },
        Counter({
            "template_topic_mismatch": 1,
            "region_field_country_only": 2,
        }),
        {
            "merged_into_existing_row": 1,
            "emitted_as_standalone_row": 1,
            "evidence_only": 0,
            "discarded": 8,
        },
    )

    assert ledger["qwen_refinement"]["unrecovered_records"] == 4
    assert ledger["remaining_by_stage"]["qwen_refinement"] == 3
    assert ledger["dropped_by_stage"]["merge_materialization"] == 1
    assert ledger["remaining_by_stage"]["entity_legality"] == 3
    assert ledger["accounting"]["loss_accounting_complete"] is True


def test_narrative_loss_accounting_flags_incomplete_audit():
    """Incomplete narrative loss accounting must fail validation instead of silently passing."""
    result = FilledResult(
        template_file=__file__,
        output_file=__file__,
        rows_filled=1,
        record_count=1,
        expected_rows=1,
        fill_rate=100.0,
        source_stats=[
            SourceProcessingStat(
                source_file="/tmp/source-alpha.docx",
                file_type="word",
                relevant_to_template=True,
                extracted_records=7,
                filtered_records=1,
                contributed_records=1,
                contributed_fields=1,
                evidence_contribution_fields=1,
                value_contribution_fields=1,
                row_contribution_records=1,
                effective_cell_delta=1,
                qwen_used=True,
                qwen_stages=["extract"],
                stage_audit={
                    "suspicious_records": 5,
                    "llm_records": 2,
                    "post_entity_records": 1,
                    "final_records": 1,
                },
                narrative_audit={
                    "post_filter_narrative_records": {
                        "remaining_records": 1,
                        "final_records": 1,
                        "remaining_by_stage": {"entity_legality": 1},
                        "dropped_by_stage": {"entity_legality": 1},
                        "accounting": {
                            "loss_accounting_complete": False,
                            "unexplained_counts": {"qwen_refinement": 3},
                        },
                    },
                },
            ),
        ],
    )

    validated = validate_result(result)

    assert any(
        item.check == "narrative_loss_accounting" and not item.passed
        for item in validated.validation_report
    )
    assert validated.status == "error"


if __name__ == "__main__":
    test_effective_contribution_distinguishes_evidence_vs_value()
    test_false_positive_contribution_is_flagged()
    test_post_filter_loss_accounting_for_narrative_records()
    test_qwen_narrative_records_not_silently_disappear()
    test_multisource_delta_audit()
    test_narrative_stage_loss_ledger_accounts_qwen_and_materialization_losses()
    test_narrative_loss_accounting_flags_incomplete_audit()
    print("\n✅ effective contribution audit regression tests passed!")
