"""Background task management for long-running document processing jobs."""
from __future__ import annotations

import threading
import time
import traceback
import uuid
from pathlib import Path

from ..core.logging import logger
from ..schemas.models import ProcessResponse, TemplateProcessingStatus
from .cleanup_service import cleanup_after_run
from .pipeline_service import process_documents


_TASKS: dict[str, ProcessResponse] = {}
_TASK_LOCK = threading.Lock()
_TASK_RETENTION_SECONDS = 60 * 60
_MAX_TASKS = 64


def _prune_tasks_locked(now: float | None = None):
    """Drop stale finished tasks and cap in-memory task history."""
    now = now or time.time()
    stale_task_ids = [
        task_id
        for task_id, task in _TASKS.items()
        if task.finished_at and (now - (task.updated_at or task.finished_at)) > _TASK_RETENTION_SECONDS
    ]
    for task_id in stale_task_ids:
        _TASKS.pop(task_id, None)

    overflow = len(_TASKS) - _MAX_TASKS
    if overflow <= 0:
        return

    ordered = sorted(
        _TASKS.items(),
        key=lambda item: (item[1].updated_at or item[1].finished_at or item[1].started_at or 0.0),
    )
    for task_id, _task in ordered[:overflow]:
        _TASKS.pop(task_id, None)


def _new_template_statuses(template_files: list[str]) -> list[TemplateProcessingStatus]:
    """Create initial template status entries for a task."""
    return [
        TemplateProcessingStatus(template_file=path, status="pending", current_stage="pending")
        for path in template_files
    ]


def create_task(template_files: list[str]) -> ProcessResponse:
    """Create a task entry and return its initial response payload."""
    task_id = str(uuid.uuid4())[:8]
    now = time.time()
    response = ProcessResponse(
        task_id=task_id,
        status="queued",
        current_stage="queued",
        stage_message="任务已创建，等待执行",
        progress=0.0,
        started_at=now,
        updated_at=now,
        template_statuses=_new_template_statuses(template_files),
    )
    with _TASK_LOCK:
        _prune_tasks_locked(now)
        _TASKS[task_id] = response
    return response.model_copy(deep=True)


def get_task(task_id: str) -> ProcessResponse | None:
    """Return a deep copy of a task response."""
    with _TASK_LOCK:
        task = _TASKS.get(task_id)
        return task.model_copy(deep=True) if task else None


def _append_unique(items: list[str], value: str):
    """Append a string only if it is not already present."""
    if value and value not in items:
        items.append(value)


def apply_task_event(task_id: str, event: dict):
    """Merge a pipeline progress event into the stored task state."""
    with _TASK_LOCK:
        task = _TASKS.get(task_id)
        if task is None:
            return
        task.updated_at = time.time()

        log_text = event.get("log")
        if log_text:
            task.logs.append(log_text)

        stage = event.get("stage")
        if stage:
            task.current_stage = stage
        if "status" in event and event["status"]:
            task.status = event["status"]
        if "message" in event and event["message"]:
            task.stage_message = event["message"]
        if "progress" in event and event["progress"] is not None:
            task.progress = float(event["progress"])

        auto_requirement = event.get("auto_requirement")
        if auto_requirement:
            task.auto_requirement = auto_requirement

        requirement_spec = event.get("requirement_spec")
        if requirement_spec is not None:
            task.requirement_spec = requirement_spec

        latest_output_dir = event.get("latest_output_dir")
        if latest_output_dir:
            task.latest_output_dir = latest_output_dir

        model_usage = event.get("model_usage")
        if model_usage is not None:
            task.model_usage = model_usage

        warning = event.get("warning")
        if warning:
            _append_unique(task.warnings, warning)

        template_file = event.get("template_file")
        if template_file:
            template_status = next(
                (item for item in task.template_statuses if item.template_file == template_file),
                None,
            )
            if template_status is None:
                template_status = TemplateProcessingStatus(template_file=template_file)
                task.template_statuses.append(template_status)
            if event.get("template_status"):
                template_status.status = event["template_status"]
            if event.get("template_stage"):
                template_status.current_stage = event["template_stage"]
            if event.get("template_warning"):
                _append_unique(template_status.warnings, event["template_warning"])
            if event.get("template_error"):
                template_status.error = event["template_error"]
                template_status.status = "error"
            if event.get("template_output_file"):
                template_status.output_file = event["template_output_file"]
            if event.get("records_extracted") is not None:
                template_status.records_extracted = int(event["records_extracted"])

        partial_result = event.get("result")
        if partial_result is not None:
            task.results = [
                item for item in task.results if item.template_file != partial_result.template_file
            ] + [partial_result]


def finish_task(task_id: str, response: ProcessResponse):
    """Persist the final task response."""
    now = time.time()
    response.finished_at = now
    response.updated_at = now
    with _TASK_LOCK:
        _prune_tasks_locked(now)
        _TASKS[task_id] = response


def fail_task(task_id: str, error: str):
    """Mark a task as failed with a visible error state."""
    with _TASK_LOCK:
        task = _TASKS.get(task_id)
        if task is None:
            return
        now = time.time()
        task.status = "error"
        task.current_stage = "failed"
        task.stage_message = error
        task.error = error
        task.finished_at = now
        task.updated_at = now
        task.progress = min(task.progress or 0.0, 0.99)
        _prune_tasks_locked(now)


def start_pipeline_task(
    task_id: str,
    source_files: list[str],
    template_files: list[str],
    requirement: str,
    options: dict,
    source_bundles: list | None = None,
):
    """Run the document pipeline in a background thread."""

    def runner():
        keep_upload_dir = options.get("keep_upload_dir")
        keep_upload_dir = Path(keep_upload_dir) if keep_upload_dir else None
        keep_upload_dirs = [
            Path(path)
            for path in options.get("keep_upload_dirs", [])
            if path
        ]
        try:
            apply_task_event(task_id, {
                "status": "processing",
                "stage": "cleanup",
                "message": "准备清理旧输出并启动任务",
                "progress": 0.01,
                "log": f"[{task_id}] 任务已启动，准备处理 {len(source_files)} 个 source 和 {len(template_files)} 个 template",
            })
            result = process_documents(
                source_files=source_files,
                template_files=template_files,
                user_requirement=requirement,
                options=options,
                task_id=task_id,
                progress_callback=lambda event: apply_task_event(task_id, event),
                source_bundles=source_bundles,
            )
            finish_task(task_id, result)
        except Exception as exc:
            logger.error("Background task failed: %s", exc, exc_info=True)
            trace = traceback.format_exc(limit=8)
            fail_task(task_id, f"{exc}\n{trace}")
        finally:
            try:
                cleanup_after_run(keep_upload_dir=keep_upload_dir, keep_upload_dirs=keep_upload_dirs)
            except Exception:
                logger.warning("Post-run cleanup failed", exc_info=True)

    thread = threading.Thread(target=runner, name=f"docfusion-{task_id}", daemon=True)
    thread.start()
