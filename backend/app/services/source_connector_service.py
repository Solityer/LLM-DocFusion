"""Unified source connector layer for local files, HTTP APIs, web pages, and databases."""
from __future__ import annotations

from pathlib import Path

from ..core.exceptions import SourceConnectError
from ..schemas.models import DocumentBundle, FileRole
from ..schemas.source_models import SourcePreviewItem, SourceSpec, SourceType
from .api_source_service import fetch_api_source
from .database_source_service import fetch_database_source
from .document_service import read_document
from .schema_registry_service import source_type_catalog
from .web_source_service import fetch_web_source


def get_source_types() -> list[dict]:
    return source_type_catalog()


def load_source(spec: SourceSpec) -> DocumentBundle:
    source_type = spec.source_type
    try:
        if source_type == SourceType.LOCAL_FILE:
            path = spec.path or spec.url
            if not path:
                raise SourceConnectError("Local file source requires path")
            bundle = read_document(path, FileRole.SOURCE)
            bundle.metadata["source_type"] = "local_file"
            bundle.metadata["priority"] = spec.priority
            if spec.name:
                bundle.metadata["title"] = spec.name
            return bundle
        if source_type == SourceType.HTTP_API:
            return fetch_api_source(spec)
        if source_type == SourceType.WEB_PAGE:
            return fetch_web_source(spec)
        if source_type == SourceType.DATABASE:
            return fetch_database_source(spec)
    except SourceConnectError:
        raise
    except Exception as exc:
        raise SourceConnectError(f"Source '{spec.name or spec.path or spec.url}' failed: {exc}") from exc
    raise SourceConnectError(f"Unsupported source type: {source_type}")


def load_sources(specs: list[SourceSpec], *, fail_soft: bool = True) -> tuple[list[DocumentBundle], list[str]]:
    documents: list[DocumentBundle] = []
    errors: list[str] = []
    for spec in specs:
        try:
            documents.append(load_source(spec))
        except Exception as exc:
            message = f"{spec.name or spec.path or spec.url or spec.source_type}: {exc}"
            if not fail_soft:
                raise SourceConnectError(message) from exc
            errors.append(message)
    return documents, errors


def preview_sources(specs: list[SourceSpec], max_rows: int = 20) -> list[SourcePreviewItem]:
    previews: list[SourcePreviewItem] = []
    for spec in specs:
        try:
            bundle = load_source(spec)
            previews.append(preview_bundle(bundle, max_rows=max_rows))
        except Exception as exc:
            previews.append(SourcePreviewItem(
                status="error",
                source_name=spec.name or spec.path or spec.url,
                source_type=spec.source_type.value if hasattr(spec.source_type, "value") else str(spec.source_type),
                error=str(exc),
            ))
    return previews


def preview_bundle(bundle: DocumentBundle, max_rows: int = 20) -> SourcePreviewItem:
    return SourcePreviewItem(
        status="ok",
        source_name=bundle.metadata.get("title") or Path(bundle.source_file).name,
        source_type=bundle.metadata.get("source_type", bundle.file_type),
        document_id=bundle.document_id,
        file_type=bundle.file_type,
        text_blocks=len(bundle.text_blocks),
        tables=len(bundle.tables),
        table_previews=[
            {
                "table_index": table.table_index,
                "sheet_name": table.sheet_name,
                "headers": table.headers,
                "rows": table.rows[:max_rows],
                "row_count": table.row_count,
                "col_count": table.col_count,
            }
            for table in bundle.tables[:8]
        ],
        raw_text_preview=(bundle.raw_text or "")[:1200],
        metadata=bundle.metadata,
    )
