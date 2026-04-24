"""Template inspection API routes."""
from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel

from ..core.config import UPLOAD_DIR
from ..core.logging import logger
from ..services.template_service import parse_template

router = APIRouter(prefix="/api/templates")


class TemplateInspectLocalRequest(BaseModel):
    file_path: str = ""


@router.post("/inspect/upload")
async def inspect_template_upload(file: UploadFile = File(...)):
    """Upload a template file and return its schema (headers, fields, placeholders)."""
    task_dir = UPLOAD_DIR / f"tpl_{str(uuid.uuid4())[:8]}"
    task_dir.mkdir(parents=True, exist_ok=True)

    safe_name = Path(file.filename).name if file.filename else f"template_{uuid.uuid4().hex[:6]}"
    dest = task_dir / safe_name

    with open(dest, "wb") as out:
        content = await file.read()
        out.write(content)

    logger.info(f"Template inspect upload: {safe_name} ({len(content)} bytes)")

    try:
        schema = parse_template(str(dest))
        return {
            "status": "ok",
            "file_path": str(dest),
            "filename": safe_name,
            "schema": schema.model_dump(),
        }
    except Exception as exc:
        raise HTTPException(400, f"Template parsing failed: {exc}") from exc


@router.post("/inspect/local")
async def inspect_template_local(request: TemplateInspectLocalRequest):
    """Inspect a template already on the server filesystem."""
    if not request.file_path:
        raise HTTPException(400, "file_path is required")
    if not Path(request.file_path).exists():
        raise HTTPException(404, f"Template file not found: {request.file_path}")

    try:
        schema = parse_template(request.file_path)
        return {
            "status": "ok",
            "file_path": request.file_path,
            "filename": Path(request.file_path).name,
            "schema": schema.model_dump(),
        }
    except Exception as exc:
        raise HTTPException(400, f"Template parsing failed: {exc}") from exc
