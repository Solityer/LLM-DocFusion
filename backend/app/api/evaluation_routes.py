"""Evaluation API routes for gold-standard comparison and competition mode."""
from __future__ import annotations

import os
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel

from ..core.config import UPLOAD_DIR
from ..core.logging import logger
from ..services.evaluation_service import compare_outputs

router = APIRouter(prefix="/api")


class CompareRequest(BaseModel):
    output_file: str = ""
    gold_file: str = ""
    key_columns: list[str] = []
    ignore_empty: bool = True


@router.post("/evaluate/compare")
async def evaluate_compare(request: CompareRequest):
    """Compare a filled output file against a gold-standard file."""
    if not request.output_file:
        raise HTTPException(400, "output_file is required")
    if not request.gold_file:
        raise HTTPException(400, "gold_file is required")

    output_path = Path(request.output_file)
    gold_path = Path(request.gold_file)

    if not output_path.exists():
        raise HTTPException(404, f"Output file not found: {request.output_file}")
    if not gold_path.exists():
        raise HTTPException(404, f"Gold file not found: {request.gold_file}")

    try:
        result = compare_outputs(
            output_file=str(output_path),
            gold_file=str(gold_path),
            key_columns=request.key_columns or None,
            ignore_empty=request.ignore_empty,
        )
        return {"status": "ok", **result}
    except Exception as exc:
        raise HTTPException(400, f"Comparison failed: {exc}") from exc


@router.post("/evaluate/compare/upload")
async def evaluate_compare_upload(
    output_file: UploadFile = File(...),
    gold_file: UploadFile = File(...),
    ignore_empty: bool = True,
):
    """Upload both output and gold files for comparison."""
    task_dir = UPLOAD_DIR / f"eval_{str(uuid.uuid4())[:8]}"
    task_dir.mkdir(parents=True, exist_ok=True)

    # Save output file
    output_name = Path(output_file.filename or "output").name
    output_path = task_dir / f"output_{output_name}"
    with open(output_path, "wb") as f:
        f.write(await output_file.read())

    # Save gold file
    gold_name = Path(gold_file.filename or "gold").name
    gold_path = task_dir / f"gold_{gold_name}"
    with open(gold_path, "wb") as f:
        f.write(await gold_file.read())

    try:
        result = compare_outputs(
            output_file=str(output_path),
            gold_file=str(gold_path),
            ignore_empty=ignore_empty,
        )
        return {"status": "ok", **result}
    except Exception as exc:
        raise HTTPException(400, f"Comparison failed: {exc}") from exc
