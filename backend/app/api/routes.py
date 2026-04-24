"""API routes for the document fusion system."""
import json
import os
import uuid
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse

from ..core.config import UPLOAD_DIR, OUTPUT_DIR
from ..core.logging import logger
from ..schemas.models import ProcessRequest, ProcessResponse, HealthResponse
from ..schemas.operation_models import DocumentOperationRequest
from ..schemas.source_models import ProcessMultisourceRequest, SourcePreviewRequest
from ..services.document_operation_service import operate_document
from ..services.ollama_service import get_ollama_service
from ..services.report_service import build_task_report
from ..services.source_connector_service import get_source_types, load_sources, preview_sources
from ..services.task_service import create_task, get_task, start_pipeline_task

router = APIRouter(prefix="/api")


def _collect_keep_upload_dirs(paths: list[str]) -> list[str]:
    """Keep upload directories that are referenced by process/local requests."""
    keepers: set[str] = set()
    upload_root = UPLOAD_DIR.resolve()
    for path_str in paths:
        path = Path(path_str).resolve()
        try:
            path.relative_to(upload_root)
        except ValueError:
            continue
        keepers.add(str(path.parent))
    return sorted(keepers)


@router.get("/health", response_model=HealthResponse)
@router.post("/health", response_model=HealthResponse)
async def health_check():
    """Check system health including Ollama status."""
    ollama = get_ollama_service()
    health = ollama.check_health()

    return HealthResponse(
        status="ok",
        provider="ollama",
        ollama_status=health.get("status", "unknown"),
        model=ollama.model,
        version="1.0.0",
    )


@router.post("/files/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    """Upload source files."""
    uploaded = []
    task_dir = UPLOAD_DIR / str(uuid.uuid4())[:8]
    task_dir.mkdir(parents=True, exist_ok=True)

    for f in files:
        safe_name = Path(f.filename).name if f.filename else f"file_{uuid.uuid4()[:6]}"
        dest = task_dir / safe_name
        with open(dest, "wb") as out:
            content = await f.read()
            out.write(content)
        uploaded.append({
            "filename": safe_name,
            "path": str(dest),
            "size": len(content),
        })
        logger.info(f"Uploaded source file: {safe_name} ({len(content)} bytes)")

    return {"status": "ok", "files": uploaded}


@router.post("/templates/upload")
async def upload_templates(files: list[UploadFile] = File(...)):
    """Upload template files."""
    uploaded = []
    task_dir = UPLOAD_DIR / str(uuid.uuid4())[:8]
    task_dir.mkdir(parents=True, exist_ok=True)

    for f in files:
        safe_name = Path(f.filename).name if f.filename else f"template_{uuid.uuid4()[:6]}"
        dest = task_dir / safe_name
        with open(dest, "wb") as out:
            content = await f.read()
            out.write(content)
        uploaded.append({
            "filename": safe_name,
            "path": str(dest),
            "size": len(content),
        })
        logger.info(f"Uploaded template file: {safe_name} ({len(content)} bytes)")

    return {"status": "ok", "files": uploaded}


@router.post("/process", response_model=ProcessResponse)
async def process_task(
    source_files: list[UploadFile] = File(...),
    template_files: list[UploadFile] = File(...),
    requirement: str = Form(default=""),
    use_llm: bool = Form(default=True),
    strict_mode: bool = Form(default=False),
    options: str = Form(default=""),
):
    """Process documents: upload sources + templates + requirement in one call."""
    task_dir = UPLOAD_DIR / str(uuid.uuid4())[:8]
    src_dir = task_dir / "sources"
    tpl_dir = task_dir / "templates"
    src_dir.mkdir(parents=True, exist_ok=True)
    tpl_dir.mkdir(parents=True, exist_ok=True)

    # Save source files
    src_paths = []
    for f in source_files:
        safe_name = Path(f.filename).name if f.filename else f"src_{uuid.uuid4()[:6]}"
        dest = src_dir / safe_name
        with open(dest, "wb") as out:
            out.write(await f.read())
        src_paths.append(str(dest))

    # Save template files
    tpl_paths = []
    for f in template_files:
        safe_name = Path(f.filename).name if f.filename else f"tpl_{uuid.uuid4()[:6]}"
        dest = tpl_dir / safe_name
        with open(dest, "wb") as out:
            out.write(await f.read())
        tpl_paths.append(str(dest))

    logger.info(f"Processing: {len(src_paths)} sources, {len(tpl_paths)} templates")
    logger.info(f"Requirement: {requirement[:200]}")

    parsed_options = _parse_options_json(options)
    parsed_options.setdefault("use_llm", use_llm)
    parsed_options.setdefault("strict_mode", strict_mode)
    parsed_options.update({
        "keep_upload_dir": str(task_dir),
        "keep_upload_dirs": [str(src_dir), str(tpl_dir)],
    })
    task = create_task(tpl_paths)
    start_pipeline_task(
        task_id=task.task_id,
        source_files=src_paths,
        template_files=tpl_paths,
        requirement=requirement,
        options=parsed_options,
    )
    return get_task(task.task_id) or task


@router.post("/process/local")
async def process_local(request: ProcessRequest):
    """Process documents already on the server filesystem."""
    # Validate paths exist
    for p in request.source_files:
        if not os.path.exists(p):
            raise HTTPException(404, f"Source file not found: {p}")
    for p in request.template_files:
        if not os.path.exists(p):
            raise HTTPException(404, f"Template file not found: {p}")

    task = create_task(request.template_files)
    options = dict(request.options or {})
    keep_upload_dirs = _collect_keep_upload_dirs([*request.source_files, *request.template_files])
    if keep_upload_dirs:
        options["keep_upload_dirs"] = sorted({
            *options.get("keep_upload_dirs", []),
            *keep_upload_dirs,
        })
    start_pipeline_task(
        task_id=task.task_id,
        source_files=request.source_files,
        template_files=request.template_files,
        requirement=request.requirement,
        options=options,
    )
    return get_task(task.task_id) or task


@router.get("/sources/types")
async def source_types():
    """List supported source connector types."""
    return {"source_types": get_source_types()}


@router.post("/sources/preview")
async def preview_source(request: SourcePreviewRequest):
    """Preview one or more data sources after normalization."""
    specs = list(request.sources or [])
    if request.source is not None:
        specs.insert(0, request.source)
    if not specs:
        raise HTTPException(400, "No source specified")
    previews = preview_sources(specs, max_rows=max(1, min(int(request.max_rows or 20), 100)))
    errors = [item.error for item in previews if item.status == "error" and item.error]
    return {
        "status": "ok" if not errors else "partial",
        "source_types": get_source_types(),
        "previews": previews,
        "errors": errors,
    }


@router.post("/process/multisource", response_model=ProcessResponse)
async def process_multisource(request: ProcessMultisourceRequest):
    """Process templates with local files plus HTTP/web/database sources."""
    for p in request.source_files:
        if not os.path.exists(p):
            raise HTTPException(404, f"Source file not found: {p}")
    for p in request.template_files:
        if not os.path.exists(p):
            raise HTTPException(404, f"Template file not found: {p}")
    if not request.source_files and not request.sources:
        raise HTTPException(400, "At least one source is required")
    if not request.template_files:
        raise HTTPException(400, "At least one template is required")

    options = dict(request.options or {})
    fail_soft = not bool(options.get("strict_validation", False))
    source_bundles, errors = load_sources(request.sources, fail_soft=fail_soft)
    if errors:
        options.setdefault("source_connector_errors", errors)
    task = create_task(request.template_files)
    keep_upload_dirs = _collect_keep_upload_dirs([*request.source_files, *request.template_files])
    if keep_upload_dirs:
        options["keep_upload_dirs"] = sorted({
            *options.get("keep_upload_dirs", []),
            *keep_upload_dirs,
        })
    start_pipeline_task(
        task_id=task.task_id,
        source_files=request.source_files,
        template_files=request.template_files,
        requirement=request.requirement,
        options=options,
        source_bundles=source_bundles,
    )
    current = get_task(task.task_id) or task
    if errors:
        current.warnings.extend(errors)
    return current


@router.post("/document/operate")
async def document_operate(request: DocumentOperationRequest):
    """Run a natural-language document operation."""
    try:
        return operate_document(request)
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/document/extract")
async def document_extract(request: DocumentOperationRequest):
    """Extract fields/content from a document."""
    request.operation = request.operation or "extract"
    try:
        return operate_document(request)
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/document/summarize")
async def document_summarize(request: DocumentOperationRequest):
    """Summarize a document."""
    request.operation = request.operation or "summarize"
    try:
        return operate_document(request)
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/result/{task_id}", response_model=ProcessResponse)
async def get_result(task_id: str):
    """Get task status or final result."""
    task = get_task(task_id)
    if task is None:
        raise HTTPException(404, f"Task not found: {task_id}")
    return task


@router.get("/status/{task_id}", response_model=ProcessResponse)
async def get_status(task_id: str):
    """Alias for polling task status."""
    task = get_task(task_id)
    if task is None:
        raise HTTPException(404, f"Task not found: {task_id}")
    return task


@router.get("/report/{task_id}")
async def get_report(task_id: str):
    """Get a task-level processing and quality report."""
    task = get_task(task_id)
    if task is None:
        raise HTTPException(404, f"Task not found: {task_id}")
    return build_task_report(task)


@router.get("/report/{task_id}/json")
async def get_report_json(task_id: str):
    """Download or view a task report as JSON."""
    task = get_task(task_id)
    if task is None:
        raise HTTPException(404, f"Task not found: {task_id}")
    return build_task_report(task)


@router.get("/download/{filename:path}")
async def download_file(filename: str):
    """Download an output file."""
    # Check in outputs directory
    file_path = OUTPUT_DIR / filename
    if not file_path.exists():
        # Try absolute path
        file_path = Path(filename)
        if not file_path.exists():
            raise HTTPException(404, f"File not found: {filename}")

    # Security: ensure file is within allowed directories
    try:
        file_path.resolve().relative_to(OUTPUT_DIR.resolve())
    except ValueError:
        try:
            file_path.resolve().relative_to(UPLOAD_DIR.resolve())
        except ValueError:
            raise HTTPException(403, "Access denied")

    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type="application/octet-stream",
    )


@router.get("/outputs")
async def list_outputs():
    """List all output files."""
    files = []
    if OUTPUT_DIR.exists():
        for f in OUTPUT_DIR.iterdir():
            if f.is_file():
                files.append({
                    "filename": f.name,
                    "size": f.stat().st_size,
                    "path": str(f),
                })
    return {"files": files}


def _parse_options_json(options: str) -> dict:
    if not options or not options.strip():
        return {}
    try:
        payload = json.loads(options)
    except json.JSONDecodeError as exc:
        raise HTTPException(400, f"Invalid options JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(400, "options must be a JSON object")
    return payload
