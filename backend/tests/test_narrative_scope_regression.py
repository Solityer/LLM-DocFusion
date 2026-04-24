"""Regression tests for generic narrative-source contribution handling."""
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
    RequirementSpec,
    TextBlock,
)
from app.services.extraction_service import (
    _annotate_records_with_entity_semantics,
    _extract_text_records,
    _filter_records_by_entity_legality,
    _merge_records_by_semantic_key,
    _normalize_records,
)
from app.services.pipeline_service import _build_source_stats
from app.services.retrieval_service import RetrievalResult


def _generic_template_context() -> dict:
    return {
        "template_file": "/tmp/template-beta.xlsx",
        "anchor_text": "global public health bulletin",
        "topic_text": "global public health bulletin 国家 大洲 人均GDP 人口 每日检测数 病例数",
        "anchor_tokens": {"global", "public", "health", "bulletin"},
        "anchor_domains": {"epidemic"},
        "topic_tokens": {"global", "public", "health", "bulletin"},
        "topic_domains": {"epidemic"},
    }


def _generic_headers() -> list[str]:
    return ["国家", "大洲", "人均GDP", "人口", "每日检测数", "病例数"]


def _make_evidence(source_file: str, location: str, snippet: str, reason: str, confidence: float = 0.8) -> CandidateEvidence:
    return CandidateEvidence(
        source_file=source_file,
        location=location,
        raw_snippet=snippet,
        match_reason=reason,
        confidence=confidence,
    )


def _make_generic_narrative_doc() -> DocumentBundle:
    return DocumentBundle(
        document_id="scope-doc",
        source_file="/tmp/source-alpha.docx",
        file_type="word",
        role=FileRole.SOURCE,
        text_blocks=[
            TextBlock(content="2024 年 7 月 27 日阿兰德国公共卫生纪要", block_index=0),
            TextBlock(content="2024 年 7 月 27 日，地处Europe（欧洲）的阿兰德国报告 68 例确诊病例，整体防控平稳。", block_index=1),
            TextBlock(content="北境省常住人口约 570 万人，人均 GDP 约 7300 元，当日核酸检测量约 12.6 万份。", block_index=2),
        ],
    )


def test_narrative_scope_supplement_survives_country_only_filter_and_merges():
    """Generic narrative supplements should survive filtering and enrich the scoped final record."""
    headers = _generic_headers()
    document = _make_generic_narrative_doc()
    retrieval = RetrievalResult()
    retrieval.source_docs = [document]
    requirement = RequirementSpec(raw_text="根据 2024/7/1 到 2024/7/31 的公共卫生数据填表")
    template_context = _generic_template_context()

    records, warnings, source_counts, evidence = _extract_text_records(
        headers=headers,
        retrieval=retrieval,
        requirement=requirement,
        candidate_documents=[document],
        template_context=template_context,
    )
    assert not warnings
    assert source_counts[document.source_file] >= 2
    assert len(evidence) >= 2

    normalized = _normalize_records(headers, records)
    annotated = _annotate_records_with_entity_semantics(headers, normalized, template_context)
    filtered, invalidated_sources, diagnostics = _filter_records_by_entity_legality(
        headers,
        annotated,
        [],
        template_context=template_context,
    )

    assert invalidated_sources == set()
    assert len(filtered) >= 2
    assert diagnostics["remap_examples"]

    merged = _merge_records_by_semantic_key(
        filtered,
        headers=headers,
        requirement=requirement,
        use_llm=False,
    )
    assert len(merged) == 1

    values = merged[0]["values"]
    assert values["国家"] == "阿兰德国"
    assert values["大洲"] == "欧洲"
    assert values["人均GDP"] == "7300"
    assert values["人口"] == "5700000"
    assert values["每日检测数"] == "126000"
    assert values["病例数"] == "68"

    merged_sources = {
        item.source_file
        for item in merged[0]["field_evidence"]["人口"]
    }
    assert merged_sources == {document.source_file}

    print("✓ test_narrative_scope_supplement_survives_country_only_filter_and_merges passed")


def test_same_source_narrative_fragments_do_not_merge_across_different_temporal_scope():
    """Same-source narrative fragments must not collapse into a fake time series row across dates."""
    headers = ["国家/地区", "病例数", "每日检测数"]
    base_record = {
        "values": {"国家/地区": "阿兰德国", "病例数": "68", "每日检测数": ""},
        "field_evidence": {
            "国家/地区": [_make_evidence("/tmp/source-alpha.docx", "text_block1", "阿兰德国报告 68 例", "summary")],
            "病例数": [_make_evidence("/tmp/source-alpha.docx", "text_block1", "阿兰德国报告 68 例", "summary")],
            "每日检测数": [],
        },
        "field_confidence": {"国家/地区": 0.8, "病例数": 0.82, "每日检测数": None},
        "match_methods": {"国家/地区": "text_context", "病例数": "text_exact", "每日检测数": "none"},
        "source_file": "/tmp/source-alpha.docx",
        "source_location": "text_block1",
        "row_index": 0,
        "temporal_scope": "2024-07-27",
        "record_role": "narrative_row",
    }
    later_record = {
        "values": {"国家/地区": "阿兰德国", "病例数": "", "每日检测数": "126000"},
        "field_evidence": {
            "国家/地区": [_make_evidence("/tmp/source-alpha.docx", "text_block9", "阿兰德国检测量 12.6 万份", "followup")],
            "病例数": [],
            "每日检测数": [_make_evidence("/tmp/source-alpha.docx", "text_block9", "阿兰德国检测量 12.6 万份", "followup")],
        },
        "field_confidence": {"国家/地区": 0.78, "病例数": None, "每日检测数": 0.8},
        "match_methods": {"国家/地区": "text_context", "病例数": "none", "每日检测数": "text_exact"},
        "source_file": "/tmp/source-alpha.docx",
        "source_location": "text_block9",
        "row_index": 0,
        "temporal_scope": "2024-07-28",
        "record_role": "narrative_row",
    }

    merged = _merge_records_by_semantic_key(
        [base_record, later_record],
        headers=headers,
        requirement=RequirementSpec(raw_text="按日期范围整理公共卫生摘要"),
        use_llm=False,
    )
    assert len(merged) == 2

    print("✓ test_same_source_narrative_fragments_do_not_merge_across_different_temporal_scope passed")


def test_semantic_merge_preserves_multisource_value_provenance_for_equal_values():
    """Equal values from different sources should keep both value owners after semantic merge."""
    headers = ["国家/地区", "病例数", "每日检测数"]
    structured_record = {
        "values": {"国家/地区": "阿兰德国", "病例数": "68", "每日检测数": "126000"},
        "field_evidence": {
            "国家/地区": [_make_evidence("/tmp/source-beta.xlsx", "table0(Sheet1).row2.col1", "阿兰德国", "table", 0.9)],
            "病例数": [_make_evidence("/tmp/source-beta.xlsx", "table0(Sheet1).row2.col6", "68", "table", 0.88)],
            "每日检测数": [_make_evidence("/tmp/source-beta.xlsx", "table0(Sheet1).row2.col5", "126000", "table", 0.88)],
        },
        "field_confidence": {"国家/地区": 0.9, "病例数": 0.88, "每日检测数": 0.88},
        "match_methods": {"国家/地区": "table", "病例数": "table", "每日检测数": "table"},
        "field_value_sources": {
            "国家/地区": ["/tmp/source-beta.xlsx"],
            "病例数": ["/tmp/source-beta.xlsx"],
            "每日检测数": ["/tmp/source-beta.xlsx"],
        },
        "field_value_record_ids": {
            "国家/地区": ["structured-1"],
            "病例数": ["structured-1"],
            "每日检测数": ["structured-1"],
        },
        "record_id": "structured-1",
        "origin_record_ids": ["structured-1"],
        "source_file": "/tmp/source-beta.xlsx",
        "source_location": "table0(Sheet1)",
        "row_index": 0,
    }
    narrative_record = {
        "values": {"国家/地区": "阿兰德国", "病例数": "68", "每日检测数": "126000"},
        "field_evidence": {
            "国家/地区": [_make_evidence("/tmp/source-alpha.docx", "text_block1", "阿兰德国报告 68 例", "summary", 0.82)],
            "病例数": [_make_evidence("/tmp/source-alpha.docx", "text_block1", "阿兰德国报告 68 例", "summary", 0.82)],
            "每日检测数": [_make_evidence("/tmp/source-alpha.docx", "text_block2", "检测量约 12.6 万份", "doc_scope", 0.8)],
        },
        "field_confidence": {"国家/地区": 0.82, "病例数": 0.82, "每日检测数": 0.8},
        "match_methods": {"国家/地区": "text_context", "病例数": "text_exact", "每日检测数": "doc_scope"},
        "field_value_sources": {
            "国家/地区": ["/tmp/source-alpha.docx"],
            "病例数": ["/tmp/source-alpha.docx"],
            "每日检测数": ["/tmp/source-alpha.docx"],
        },
        "field_value_record_ids": {
            "国家/地区": ["narrative-1"],
            "病例数": ["narrative-1"],
            "每日检测数": ["narrative-1"],
        },
        "record_id": "narrative-1",
        "origin_record_ids": ["narrative-1"],
        "source_file": "/tmp/source-alpha.docx",
        "source_location": "text_block1",
        "row_index": 0,
    }

    merged = _merge_records_by_semantic_key(
        [structured_record, narrative_record],
        headers=headers,
        requirement=RequirementSpec(raw_text="根据公共卫生纪要补表"),
        use_llm=False,
    )

    assert len(merged) == 1
    assert merged[0]["field_value_sources"]["国家/地区"] == ["/tmp/source-beta.xlsx", "/tmp/source-alpha.docx"]
    assert merged[0]["field_value_sources"]["病例数"] == ["/tmp/source-beta.xlsx", "/tmp/source-alpha.docx"]
    assert merged[0]["field_value_sources"]["每日检测数"] == ["/tmp/source-beta.xlsx", "/tmp/source-alpha.docx"]
    assert merged[0]["field_value_record_ids"]["病例数"] == ["structured-1", "narrative-1"]

    print("✓ test_semantic_merge_preserves_multisource_value_provenance_for_equal_values passed")


def test_source_stats_surface_generic_narrative_contribution_after_merge():
    """Narrative support should remain visible in final source stats after cross-source merge."""
    narrative_doc = DocumentBundle(document_id="narrative", source_file="/tmp/source-alpha.docx", file_type="word", role=FileRole.SOURCE)
    structured_doc = DocumentBundle(document_id="structured", source_file="/tmp/source-beta.xlsx", file_type="excel", role=FileRole.SOURCE)
    documents = [narrative_doc, structured_doc]
    retrieval = RetrievalResult()
    retrieval.source_docs = documents
    extracted = [{
        "source_counts": {narrative_doc.source_file: 2, structured_doc.source_file: 4},
        "filtered_source_counts": {narrative_doc.source_file: 2, structured_doc.source_file: 4},
        "entity_block_counts": {narrative_doc.source_file: 3},
        "relevant_source_files": [narrative_doc.source_file, structured_doc.source_file],
    }]
    filled = FilledResult(
        template_file="/tmp/template-beta.xlsx",
        rows_filled=1,
        record_count=1,
        expected_rows=1,
        fill_rate=100.0,
        filled_fields=[
            FilledFieldResult(
                field_name="国家/地区",
                target_location="Sheet1!A2",
                value="阿兰德国",
                normalized_value="阿兰德国",
                source_file=structured_doc.source_file,
                value_sources=[structured_doc.source_file, narrative_doc.source_file],
                evidence=[
                    _make_evidence(structured_doc.source_file, "table0(Sheet1).row2.col1", "阿兰德国", "table", 0.9),
                    _make_evidence(narrative_doc.source_file, "text_block1", "阿兰德国报告 68 例", "doc_scope", 0.76),
                ],
            ),
            FilledFieldResult(
                field_name="每日检测数",
                target_location="Sheet1!E2",
                value="126000",
                normalized_value="126000",
                source_file=structured_doc.source_file,
                value_sources=[structured_doc.source_file, narrative_doc.source_file],
                evidence=[
                    _make_evidence(structured_doc.source_file, "table0(Sheet1).row2.col5", "126000", "table", 0.88),
                    _make_evidence(narrative_doc.source_file, "text_block2", "北境省...检测量约 12.6 万份", "doc_scope", 0.8),
                ],
            ),
            FilledFieldResult(
                field_name="病例数",
                target_location="Sheet1!F2",
                value="68",
                normalized_value="68",
                source_file=narrative_doc.source_file,
                value_sources=[narrative_doc.source_file],
                evidence=[
                    _make_evidence(narrative_doc.source_file, "text_block1", "阿兰德国报告 68 例", "summary", 0.82),
                ],
            ),
        ],
        model_usage=ModelUsageSummary(),
    )

    stats = _build_source_stats(
        documents,
        retrieval,
        extracted,
        filled,
        "template-beta.xlsx",
        model_usage=filled.model_usage,
    )
    per_source = {item.source_file: item for item in stats}

    assert per_source[narrative_doc.source_file].relevant_to_template is True
    assert per_source[narrative_doc.source_file].contributed_fields >= 2
    assert per_source[narrative_doc.source_file].contributed_records == 1
    assert per_source[narrative_doc.source_file].entity_blocks_detected == 3
    assert per_source[structured_doc.source_file].contributed_records == 1

    print("✓ test_source_stats_surface_generic_narrative_contribution_after_merge passed")


if __name__ == "__main__":
    test_narrative_scope_supplement_survives_country_only_filter_and_merges()
    test_same_source_narrative_fragments_do_not_merge_across_different_temporal_scope()
    test_semantic_merge_preserves_multisource_value_provenance_for_equal_values()
    test_source_stats_surface_generic_narrative_contribution_after_merge()
    print("\n✅ narrative scope regression tests passed!")
