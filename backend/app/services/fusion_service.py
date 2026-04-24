"""Generic multi-source fusion utilities."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from ..schemas.models import CandidateEvidence, DocumentBundle
from ..utils.text_utils import clean_cell_value
from .normalization_service import normalize_value
from .schema_registry_service import canonical_field_name, field_match_score, infer_field_type, normalize_field_name


def fuse_document_tables(documents: list[DocumentBundle]) -> dict[str, Any]:
    """Fuse all structured source tables into a generic record set for reporting and reuse."""
    raw_records: list[dict[str, Any]] = []
    for document in documents:
        priority = int((document.metadata or {}).get("priority", 0) or 0)
        for table in document.tables:
            canonical_headers = [canonical_field_name(header) or normalize_field_name(header) for header in table.headers]
            key_columns = _key_columns(table.headers)
            for row_index, row in enumerate(table.rows):
                values: dict[str, dict[str, Any]] = {}
                for col_index, header in enumerate(table.headers):
                    raw_value = clean_cell_value(row[col_index] if col_index < len(row) else "")
                    if not raw_value:
                        continue
                    canonical = canonical_headers[col_index] or f"col{col_index + 1}"
                    values[canonical] = {
                        "field_name": header,
                        "raw_value": raw_value,
                        "normalized": normalize_value(raw_value, header),
                        "source": document.source_file,
                        "location": f"table{table.table_index}.row{row_index + 1}.col{col_index + 1}",
                        "confidence": 0.86 if document.file_type in {"excel", "csv", "json", "database"} else 0.72,
                        "priority": priority,
                    }
                if not values:
                    continue
                entity_key = _record_key(table.headers, row, key_columns)
                raw_records.append({
                    "entity_key": entity_key,
                    "values": values,
                    "source": document.source_file,
                    "table_index": table.table_index,
                    "row_index": row_index,
                })

    fused: dict[str, dict[str, Any]] = {}
    conflicts: list[dict[str, Any]] = []
    duplicates = 0
    for record in raw_records:
        key = record["entity_key"] or f"{record['source']}#{record['table_index']}#{record['row_index']}"
        bucket = fused.setdefault(key, {"entity_key": key, "fields": {}, "sources": set(), "origin_count": 0})
        bucket["sources"].add(record["source"])
        bucket["origin_count"] += 1
        if bucket["origin_count"] > 1:
            duplicates += 1
        for field_key, payload in record["values"].items():
            existing = bucket["fields"].get(field_key)
            if existing is None:
                bucket["fields"][field_key] = payload
                continue
            existing_value = str(existing.get("normalized", {}).get("standard_value", existing.get("raw_value", "")))
            new_value = str(payload.get("normalized", {}).get("standard_value", payload.get("raw_value", "")))
            if normalize_field_name(existing_value) != normalize_field_name(new_value):
                conflicts.append({
                    "entity_key": key,
                    "field": field_key,
                    "left": existing,
                    "right": payload,
                })
            if _payload_score(payload) > _payload_score(existing):
                bucket["fields"][field_key] = payload

    records = [
        {
            "entity_key": key,
            "values": {field: payload.get("normalized", {}).get("standard_value", payload.get("raw_value", "")) for field, payload in item["fields"].items()},
            "field_sources": {field: payload.get("source", "") for field, payload in item["fields"].items()},
            "sources": sorted(item["sources"]),
            "origin_count": item["origin_count"],
        }
        for key, item in fused.items()
    ]
    return {
        "records": records,
        "summary": {
            "input_sources": len(documents),
            "input_tables": sum(len(document.tables) for document in documents),
            "raw_records": len(raw_records),
            "fused_records": len(records),
            "duplicate_records_merged": duplicates,
            "conflict_count": len(conflicts),
            "source_type_distribution": dict(Counter(document.file_type for document in documents)),
        },
        "conflicts": conflicts[:100],
    }


def build_extraction_fusion_report(extracted: list[dict]) -> dict[str, Any]:
    conflicts: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    record_keys: Counter[str] = Counter()
    field_sources: dict[tuple[str, str], set[str]] = defaultdict(set)
    for table_data in extracted:
        headers = table_data.get("headers", [])
        entity_headers = [header for header in headers if infer_field_type(header) == "entity"]
        key_header = entity_headers[0] if entity_headers else (headers[0] if headers else "")
        seen_values: dict[tuple[str, str], str] = {}
        for record in table_data.get("records", []):
            values = record.get("values", {})
            key = normalize_field_name(values.get(key_header, "")) or record.get("record_id", "")
            if key:
                record_keys[key] += 1
            source = record.get("source_file", "")
            if source:
                source_counts[source] += 1
            for field, value in values.items():
                if not value:
                    continue
                field_sources[(key, field)].add(source)
                normalized = str(normalize_value(value, field).get("standard_value", value))
                conflict_key = (key, field)
                previous = seen_values.get(conflict_key)
                if previous and normalize_field_name(previous) != normalize_field_name(normalized):
                    conflicts.append({
                        "table_index": table_data.get("table_index", 0),
                        "entity_key": key,
                        "field": field,
                        "left": previous,
                        "right": normalized,
                        "source": source,
                    })
                seen_values[conflict_key] = normalized
    return {
        "summary": {
            "record_count": sum(record_keys.values()),
            "unique_record_keys": len(record_keys),
            "merged_duplicate_keys": sum(1 for count in record_keys.values() if count > 1),
            "conflict_count": len(conflicts),
            "source_count": len(source_counts),
            "source_record_distribution": dict(source_counts),
            "multi_source_fields": sum(1 for sources in field_sources.values() if len(sources) > 1),
        },
        "conflicts": conflicts[:100],
    }


def _payload_score(payload: dict[str, Any]) -> tuple[int, float]:
    return (int(payload.get("priority", 0)), float(payload.get("confidence", 0.0)))


def _key_columns(headers: list[str]) -> list[int]:
    candidates = [
        index for index, header in enumerate(headers)
        if infer_field_type(header) == "entity"
        or any(token in normalize_field_name(header) for token in ["id", "code", "编号", "编码", "名称", "name"])
    ]
    return candidates[:2] if candidates else [0]


def _record_key(headers: list[str], row: list[str], key_columns: list[int]) -> str:
    parts = []
    for index in key_columns:
        if index < len(row):
            value = clean_cell_value(row[index])
            if value:
                parts.append(normalize_field_name(value))
    return "|".join(parts)
