"""HTTP/API source connector."""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any

import httpx

from ..core.exceptions import SourceConnectError
from ..schemas.models import DocumentBundle, FileRole, NormalizedTable, TextBlock
from ..schemas.source_models import SourceSpec
from ..utils.text_utils import clean_cell_value
from .document_service import _populate_bundle_from_json


def fetch_api_source(spec: SourceSpec) -> DocumentBundle:
    """Fetch a JSON/CSV/text HTTP endpoint and normalize it to DocumentBundle."""
    if not spec.url:
        raise SourceConnectError("HTTP/API source requires url")
    if spec.method.upper() != "GET":
        raise SourceConnectError("Only GET is supported for HTTP/API sources")

    try:
        with httpx.Client(timeout=max(float(spec.timeout or 20), 1.0), follow_redirects=True) as client:
            response = client.get(spec.url, headers=spec.headers or {})
            response.raise_for_status()
    except Exception as exc:
        raise SourceConnectError(f"HTTP/API source failed: {exc}") from exc

    content_type = response.headers.get("content-type", "").lower()
    text = response.text
    name = spec.name or Path(spec.url.split("?")[0]).name or spec.url
    bundle = DocumentBundle(
        document_id=_safe_document_id(name),
        source_file=spec.url,
        file_type=_api_file_type(content_type, spec.url, text),
        role=FileRole.SOURCE,
        metadata={
            "source_type": "http_api",
            "url": spec.url,
            "status_code": response.status_code,
            "content_type": content_type,
            "priority": spec.priority,
        },
    )

    if bundle.file_type == "json":
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise SourceConnectError(f"JSON API parse failed: {exc}") from exc
        _populate_bundle_from_json(bundle, payload, title=name)
    elif bundle.file_type == "csv":
        _populate_bundle_from_csv_text(bundle, text, name=name)
    else:
        _populate_bundle_from_text(bundle, text, name=name, source_type="http_api")
    return bundle


def _populate_bundle_from_csv_text(bundle: DocumentBundle, text: str, name: str = "api"):
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel
    rows = list(csv.reader(io.StringIO(text), dialect))
    if rows:
        headers = [clean_cell_value(cell) for cell in rows[0]]
        data_rows = [[clean_cell_value(cell) for cell in row] for row in rows[1:] if any(clean_cell_value(cell) for cell in row)]
        bundle.tables.append(NormalizedTable(
            table_index=0,
            sheet_name=name,
            headers=headers,
            rows=data_rows,
            row_count=len(data_rows),
            col_count=len(headers),
        ))
        lines = [" | ".join(headers)]
        lines.extend(" | ".join(row) for row in data_rows[:200])
        bundle.raw_text = "\n".join(lines)
    else:
        bundle.raw_text = text
    if bundle.raw_text:
        bundle.text_blocks.append(TextBlock(content=bundle.raw_text[:20000], block_index=0))
    bundle.metadata["title"] = name


def _populate_bundle_from_text(bundle: DocumentBundle, text: str, name: str = "text", source_type: str = ""):
    bundle.raw_text = text or ""
    paragraphs = [part.strip() for part in bundle.raw_text.splitlines() if part.strip()]
    if len(paragraphs) <= 1:
        paragraphs = [part.strip() for part in bundle.raw_text.split("\n\n") if part.strip()]
    bundle.text_blocks = [
        TextBlock(content=paragraph[:12000], heading_level=0, block_index=index)
        for index, paragraph in enumerate(paragraphs[:1000])
    ]
    if not bundle.text_blocks and bundle.raw_text:
        bundle.text_blocks = [TextBlock(content=bundle.raw_text[:12000], block_index=0)]
    bundle.metadata["title"] = name
    if source_type:
        bundle.metadata["source_type"] = source_type


def _api_file_type(content_type: str, url: str, text: str) -> str:
    lowered_url = url.lower().split("?")[0]
    stripped = (text or "").lstrip()
    if "json" in content_type or lowered_url.endswith(".json") or stripped.startswith("{") or stripped.startswith("["):
        return "json"
    if "csv" in content_type or lowered_url.endswith(".csv"):
        return "csv"
    return "text"


def _safe_document_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)[:80] or "api_source"
