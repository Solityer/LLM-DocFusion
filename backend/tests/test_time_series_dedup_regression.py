"""Regression tests for structured time-series dedup behavior."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.schemas.models import CandidateEvidence
from app.services.extraction_service import _deduplicate_records


def _build_table_record(headers: list[str], values: dict[str, str], row_index: int) -> dict:
    field_confidence: dict[str, float | None] = {}
    field_evidence: dict[str, list[CandidateEvidence]] = {}
    field_value_sources: dict[str, list[str]] = {}
    field_value_record_ids: dict[str, list[str]] = {}
    match_methods: dict[str, str] = {}
    record_id = f"table-row-{row_index}"
    for col_index, header in enumerate(headers, start=1):
        value = values.get(header, "")
        field_confidence[header] = 0.92 if value else None
        field_evidence[header] = []
        field_value_sources[header] = ["/tmp/covid.xlsx"] if value else []
        field_value_record_ids[header] = [record_id] if value else []
        match_methods[header] = "exact" if value else "none"
        if value:
            field_evidence[header].append(CandidateEvidence(
                source_file="/tmp/covid.xlsx",
                location=f"table0(Sheet1).row{row_index + 1}.col{col_index}",
                raw_snippet="Bosnia and Herzegovina | Europe | 14895.6 | 3280815 | 158",
                match_reason=f"{header} <- source col {col_index}",
                confidence=0.92,
            ))
    return {
        "values": {header: values.get(header, "") for header in headers},
        "field_confidence": field_confidence,
        "field_evidence": field_evidence,
        "field_value_sources": field_value_sources,
        "field_value_record_ids": field_value_record_ids,
        "record_id": record_id,
        "origin_record_ids": [record_id],
        "source_file": "/tmp/covid.xlsx",
        "source_location": "table0(Sheet1)",
        "row_index": row_index,
        "match_methods": match_methods,
    }


def _build_narrative_record(headers: list[str], values: dict[str, str], location: str, source_file: str = "/tmp/covid-report.docx") -> dict:
    field_confidence: dict[str, float | None] = {}
    field_evidence: dict[str, list[CandidateEvidence]] = {}
    field_value_sources: dict[str, list[str]] = {}
    field_value_record_ids: dict[str, list[str]] = {}
    match_methods: dict[str, str] = {}
    record_id = f"{source_file}:{location}"
    for header in headers:
        value = values.get(header, "")
        field_confidence[header] = 0.77 if value else None
        field_evidence[header] = []
        field_value_sources[header] = [source_file] if value else []
        field_value_record_ids[header] = [record_id] if value else []
        match_methods[header] = "text_context" if value else "none"
        if value:
            field_evidence[header].append(CandidateEvidence(
                source_file=source_file,
                location=location,
                raw_snippet="Bosnia and Herzegovina reported 158 cases.",
                match_reason=f"narrative extract: {header}",
                confidence=0.77,
            ))
    return {
        "values": {header: values.get(header, "") for header in headers},
        "field_confidence": field_confidence,
        "field_evidence": field_evidence,
        "field_value_sources": field_value_sources,
        "field_value_record_ids": field_value_record_ids,
        "record_id": record_id,
        "origin_record_ids": [record_id],
        "source_file": source_file,
        "source_location": location,
        "row_index": 0,
        "match_methods": match_methods,
    }


def test_deduplicate_records_preserves_distinct_structured_rows_with_same_visible_values():
    """Structured rows must survive early dedup even when date is hidden by the template."""
    headers = ["国家/地区", "大洲", "人均GDP", "人口", "每日检测数", "病例数"]
    values = {
        "国家/地区": "Bosnia and Herzegovina",
        "大洲": "Europe",
        "人均GDP": "14895.6",
        "人口": "3280815",
        "每日检测数": "158",
        "病例数": "68",
    }

    deduped = _deduplicate_records([
        _build_table_record(headers, values, row_index=14),
        _build_table_record(headers, values, row_index=15),
    ], headers)

    assert len(deduped) == 2
    assert [record["row_index"] for record in deduped] == [14, 15]

    print("✓ test_deduplicate_records_preserves_distinct_structured_rows_with_same_visible_values passed")


def test_deduplicate_records_merges_same_value_narrative_rows_and_keeps_evidence():
    """Narrative duplicates should still collapse by visible values and keep both evidences."""
    headers = ["国家/地区", "病例数"]
    values = {
        "国家/地区": "Bosnia and Herzegovina",
        "病例数": "68",
    }

    deduped = _deduplicate_records([
        _build_narrative_record(headers, values, location="text_block1"),
        _build_narrative_record(headers, values, location="text_block7"),
    ], headers)

    assert len(deduped) == 1
    merged = deduped[0]
    evidence_locations = {
        evidence.location
        for evidence in merged["field_evidence"]["病例数"]
    }
    assert evidence_locations == {"text_block1", "text_block7"}

    print("✓ test_deduplicate_records_merges_same_value_narrative_rows_and_keeps_evidence passed")


def test_deduplicate_records_keeps_cross_source_value_provenance_for_same_narrative_value():
    """Cross-source same-value narrative rows should retain both value owners after dedup."""
    headers = ["国家/地区", "病例数"]
    values = {
        "国家/地区": "Bosnia and Herzegovina",
        "病例数": "68",
    }

    deduped = _deduplicate_records([
        _build_narrative_record(headers, values, location="text_block1", source_file="/tmp/source-alpha.docx"),
        _build_narrative_record(headers, values, location="text_block7", source_file="/tmp/source-beta.docx"),
    ], headers)

    assert len(deduped) == 1
    merged = deduped[0]
    assert merged["field_value_sources"]["病例数"] == ["/tmp/source-alpha.docx", "/tmp/source-beta.docx"]
    assert len(merged["field_value_record_ids"]["病例数"]) == 2

    print("✓ test_deduplicate_records_keeps_cross_source_value_provenance_for_same_narrative_value passed")


if __name__ == "__main__":
    test_deduplicate_records_preserves_distinct_structured_rows_with_same_visible_values()
    test_deduplicate_records_merges_same_value_narrative_rows_and_keeps_evidence()
    test_deduplicate_records_keeps_cross_source_value_provenance_for_same_narrative_value()
    print("\n✅ time-series dedup regression tests passed!")
