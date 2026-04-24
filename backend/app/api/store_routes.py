"""API routes for the SQLite document data asset store."""
from __future__ import annotations

import threading
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel

from ..core.config import UPLOAD_DIR, PROJECT_ROOT
from ..core.logging import logger
from ..schemas.store_models import StoreImportRequest, StoreTaskStatus
from ..services.document_store_service import (
    checkout_document,
    create_store_task,
    delete_document,
    export_document_package,
    get_documents,
    get_document_detail,
    get_entities,
    get_quality_issues,
    get_stats,
    get_store_task,
    init_db,
    run_import_task,
    search_store,
)

router = APIRouter(prefix="/api/store")

OUTPUT_DIR = PROJECT_ROOT / "outputs"

# Ensure DB is initialized when this module loads
try:
    init_db()
except Exception as _exc:
    logger.warning(f"Store DB init warning: {_exc}")


@router.post("/import/local")
async def import_local(request: StoreImportRequest):
    """Import local files into the document data asset store (async)."""
    for p in request.source_files:
        if not Path(p).exists():
            raise HTTPException(404, f"Source file not found: {p}")

    task = create_store_task()

    def runner():
        run_import_task(
            task_id=task.task_id,
            source_files=request.source_files,
            use_llm=request.use_llm,
            extract_entities=request.extract_entities,
            overwrite=request.overwrite,
        )

    thread = threading.Thread(target=runner, name=f"store-{task.task_id}", daemon=True)
    thread.start()
    logger.info(f"Store import task {task.task_id} started with {len(request.source_files)} files")
    return task.model_dump()


@router.post("/import/upload")
async def import_upload(
    files: list[UploadFile] = File(...),
    extract_entities: bool = True,
    overwrite: bool = False,
):
    """Upload and import files into the document data asset store (async)."""
    task_dir = UPLOAD_DIR / f"store_{str(uuid.uuid4())[:8]}"
    task_dir.mkdir(parents=True, exist_ok=True)

    saved_paths = []
    for f in files:
        safe_name = Path(f.filename).name if f.filename else f"upload_{uuid.uuid4().hex[:6]}"
        dest = task_dir / safe_name
        content = await f.read()
        with open(dest, "wb") as out:
            out.write(content)
        saved_paths.append(str(dest))
        logger.info(f"Store upload: {safe_name} ({len(content)} bytes)")

    task = create_store_task()

    def runner():
        run_import_task(
            task_id=task.task_id,
            source_files=saved_paths,
            use_llm=True,
            extract_entities=extract_entities,
            overwrite=overwrite,
        )

    thread = threading.Thread(target=runner, name=f"store-{task.task_id}", daemon=True)
    thread.start()
    return {**task.model_dump(), "uploaded_files": [Path(p).name for p in saved_paths]}


@router.get("/status/{task_id}")
async def store_task_status(task_id: str):
    """Poll the status of a store import task."""
    task = get_store_task(task_id)
    if task is None:
        raise HTTPException(404, f"Store task not found: {task_id}")
    return task.model_dump()


@router.get("/documents")
async def list_documents(limit: int = 100, offset: int = 0):
    """List all documents in the store."""
    docs = get_documents(limit=min(limit, 500), offset=offset)
    return {"status": "ok", "count": len(docs), "documents": [d.model_dump() for d in docs]}


@router.get("/documents/{document_id}")
async def get_document(document_id: str):
    """Get detailed metadata and preview data for a specific document."""
    detail = get_document_detail(document_id)
    if detail is None:
        raise HTTPException(404, f"Document not found: {document_id}")
    return {"status": "ok", **detail}


class CheckoutRequest(BaseModel):
    remove_after_export: bool = True


@router.post("/documents/{document_id}/export")
async def export_document(document_id: str):
    """Export a document to a JSON package without removing it from the store."""
    try:
        result = export_document_package(document_id, OUTPUT_DIR)
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    except Exception as exc:
        logger.error(f"Export failed for {document_id}: {exc}", exc_info=True)
        raise HTTPException(500, f"Export failed: {exc}")
    return result


@router.post("/documents/{document_id}/checkout")
async def checkout_document_route(document_id: str, request: CheckoutRequest):
    """Export a document and optionally remove it from the store."""
    try:
        result = checkout_document(
            document_id, OUTPUT_DIR, remove_after_export=request.remove_after_export
        )
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    except Exception as exc:
        logger.error(f"Checkout failed for {document_id}: {exc}", exc_info=True)
        raise HTTPException(500, f"Checkout failed: {exc}")
    return result


@router.delete("/documents/{document_id}")
async def delete_document_route(document_id: str):
    """Delete a document and all its associated data from the store.

    Does NOT delete the original upload file from disk.
    """
    try:
        result = delete_document(document_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    except Exception as exc:
        logger.error(f"Delete failed for {document_id}: {exc}", exc_info=True)
        raise HTTPException(500, f"Delete failed: {exc}")
    return result


@router.get("/search")
async def search(q: str = "", limit: int = 50):
    """Search across documents, text blocks, entities, and fields."""
    if not q.strip():
        raise HTTPException(400, "Query parameter 'q' is required")
    results = search_store(q, limit=min(limit, 200))
    return {
        "status": "ok",
        "query": q,
        "count": len(results),
        "results": [r.model_dump() for r in results],
    }


@router.get("/entities")
async def list_entities(
    entity_type: str = "",
    document_id: str = "",
    limit: int = 200,
):
    """List extracted entities, optionally filtered by type or document."""
    entities = get_entities(
        entity_type=entity_type or None,
        document_id=document_id or None,
        limit=min(limit, 1000),
    )
    return {"status": "ok", "count": len(entities), "entities": [e.model_dump() for e in entities]}


@router.get("/quality")
async def list_quality(document_id: str = "", limit: int = 500):
    """List quality issues across all documents."""
    result = get_quality_issues(
        document_id=document_id or None,
        limit=min(limit, 2000),
    )
    return {"status": "ok", **result}


@router.get("/stats")
async def store_stats():
    """Global statistics for the document data asset store."""
    stats = get_stats()
    return {"status": "ok", **stats.model_dump()}
