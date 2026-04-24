"""Ollama LLM service with task-scoped tracing for qwen2.5:14b."""
from __future__ import annotations

import copy
import hashlib
import httpx
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from ..core.config import OLLAMA_BASE, OLLAMA_MODEL, OLLAMA_NUM_PREDICT, OLLAMA_TEMPERATURE, MAX_RETRIES
from ..core.exceptions import LLMError
from ..core.logging import logger
from ..utils.json_repair import safe_parse_json


DEFAULT_TASK_ID = "__default__"


def _make_client(timeout: float = 30) -> httpx.Client:
    """Create httpx client that bypasses proxy for localhost."""
    transport = httpx.HTTPTransport(proxy=None)
    return httpx.Client(transport=transport, timeout=timeout)


class OllamaService:
    def __init__(self):
        self.base_url = OLLAMA_BASE
        self.model = OLLAMA_MODEL
        self.temperature = OLLAMA_TEMPERATURE
        self.num_predict = OLLAMA_NUM_PREDICT
        self.max_retries = MAX_RETRIES
        self._available: Optional[bool] = None
        self._health_status = "unknown"
        self._usage_lock = threading.Lock()
        self._usage_by_task: dict[str, dict[str, Any]] = {}
        self._trace_entries_by_task: dict[str, list[dict[str, Any]]] = {}

    def _empty_usage(self, task_id: str = DEFAULT_TASK_ID, trace_file: str = "") -> dict[str, Any]:
        return {
            "task_id": task_id,
            "provider": "ollama",
            "model": self.model,
            "called": False,
            "model_not_used": True,
            "total_calls": 0,
            "successful_calls": 0,
            "per_stage": {},
            "per_source": {},
            "per_source_stage": {},
            "per_template_source_stage": {},
            "per_template": {},
            "probe_sources": [],
            "probe_source_calls": {},
            "fallback_reasons": [],
            "trace_file": trace_file,
            "sample_trace": {},
            "skip_events": [],
            "required_calls": [],
            "missing_required_calls": [],
            "validation_errors": [],
            "degraded": False,
            "availability_status": self._health_status,
        }

    def _resolve_task_id(
        self,
        usage_context: Optional[dict[str, Any]] = None,
        task_id: Optional[str] = None,
    ) -> str:
        if task_id:
            return str(task_id).strip()
        if usage_context and usage_context.get("task_id"):
            return str(usage_context["task_id"]).strip()
        return DEFAULT_TASK_ID

    def _ensure_task_state(self, task_id: str, trace_file: str = "") -> dict[str, Any]:
        usage = self._usage_by_task.get(task_id)
        if usage is None:
            usage = self._empty_usage(task_id=task_id, trace_file=trace_file)
            self._usage_by_task[task_id] = usage
            self._trace_entries_by_task[task_id] = []
        elif trace_file:
            usage["trace_file"] = trace_file
        usage["model"] = self.model
        usage["provider"] = "ollama"
        usage["availability_status"] = self._health_status
        return usage

    def reset_usage(self, task_id: str = DEFAULT_TASK_ID, trace_file: str = ""):
        """Reset per-run usage counters and trace state."""
        with self._usage_lock:
            self._usage_by_task[task_id] = self._empty_usage(task_id=task_id, trace_file=trace_file)
            self._trace_entries_by_task[task_id] = []
            self._write_trace_file_locked(task_id)

    def snapshot_usage(self, task_id: str = DEFAULT_TASK_ID) -> dict[str, Any]:
        """Return a deep copy of the current usage summary."""
        with self._usage_lock:
            usage = copy.deepcopy(self._ensure_task_state(task_id))
            usage["model_not_used"] = not bool(usage.get("total_calls"))
            usage["sample_trace"] = copy.deepcopy(self._sample_trace_locked(task_id))
            return usage

    def finalize_usage(self, task_id: str = DEFAULT_TASK_ID) -> dict[str, Any]:
        """Compute derived validation fields and persist the task trace file."""
        with self._usage_lock:
            usage = self._ensure_task_state(task_id)
            usage["model"] = self.model
            usage["availability_status"] = self._health_status
            usage["model_not_used"] = not bool(usage.get("total_calls"))
            usage["sample_trace"] = self._sample_trace_locked(task_id)
            usage["missing_required_calls"] = self._missing_required_calls_locked(task_id)
            if usage["called"] and usage["total_calls"] == 0:
                self._append_unique_locked(
                    usage["validation_errors"],
                    "Backend inconsistency: model usage claims qwen was called but total_calls == 0",
                )
            if usage["missing_required_calls"]:
                usage["degraded"] = True
                self._append_unique_locked(
                    usage["validation_errors"],
                    "Required qwen semantic stage completed without a matching successful local Ollama call",
                )
            self._write_trace_file_locked(task_id)
            return copy.deepcopy(usage)

    def record_validation_error(self, task_id: str, message: str):
        if not message:
            return
        with self._usage_lock:
            usage = self._ensure_task_state(task_id)
            usage["degraded"] = True
            self._append_unique_locked(usage["validation_errors"], message)
            self._write_trace_file_locked(task_id)

    def mark_required_call(self, reason: str, usage_context: Optional[dict[str, Any]] = None):
        """Register a semantic stage that must produce a successful qwen call."""
        task_id = self._resolve_task_id(usage_context)
        stage = (usage_context or {}).get("stage", "unknown")
        sources = self._context_values(usage_context, "source_file", "source_files")
        templates = self._context_values(usage_context, "template_file", "template_files")
        requirement = {
            "stage": stage,
            "reason": reason,
            "source_file": sources[0] if sources else "",
            "source_files": sources,
            "template_file": templates[0] if templates else "",
            "template_files": templates,
        }
        with self._usage_lock:
            usage = self._ensure_task_state(task_id)
            if requirement not in usage["required_calls"]:
                usage["required_calls"].append(requirement)
            self._write_trace_file_locked(task_id)

    def note_skip(self, reason: str, usage_context: Optional[dict[str, Any]] = None):
        """Record why qwen was not used on a stage/source."""
        if not reason:
            return
        task_id = self._resolve_task_id(usage_context)
        stage = (usage_context or {}).get("stage", "unknown")
        sources = self._context_values(usage_context, "source_file", "source_files")
        templates = self._context_values(usage_context, "template_file", "template_files")
        if sources:
            reason_text = f"{stage}: {reason} [{', '.join(sources[:4])}]"
        else:
            reason_text = f"{stage}: {reason}"
        skip_event = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "stage": stage,
            "reason": reason,
            "source_file": sources[0] if sources else "",
            "source_files": sources,
            "template_file": templates[0] if templates else "",
            "template_files": templates,
        }
        with self._usage_lock:
            usage = self._ensure_task_state(task_id)
            if "回退" in reason or "不可用" in reason or "未调用" in reason:
                usage["degraded"] = True
            self._append_unique_locked(usage["fallback_reasons"], reason_text)
            if skip_event not in usage["skip_events"]:
                usage["skip_events"].append(skip_event)
            self._write_trace_file_locked(task_id)

    def _append_unique_locked(self, values: list[str], value: str):
        if value and value not in values:
            values.append(value)

    def _sample_trace_locked(self, task_id: str) -> dict[str, Any]:
        entries = self._trace_entries_by_task.get(task_id, [])
        if not entries:
            return {}
        for entry in reversed(entries):
            if entry.get("finish_status") == "success":
                return copy.deepcopy(entry)
        return copy.deepcopy(entries[-1])

    def _missing_required_calls_locked(self, task_id: str) -> list[dict[str, Any]]:
        usage = self._usage_by_task.get(task_id) or self._empty_usage(task_id=task_id)
        required_calls = usage.get("required_calls", [])
        entries = self._trace_entries_by_task.get(task_id, [])
        successful = [entry for entry in entries if entry.get("finish_status") == "success"]
        missing: list[dict[str, Any]] = []
        for requirement in required_calls:
            if not self._has_matching_success(requirement, successful):
                missing.append(copy.deepcopy(requirement))
        return missing

    def _has_matching_success(self, requirement: dict[str, Any], successful_entries: list[dict[str, Any]]) -> bool:
        for entry in successful_entries:
            if requirement.get("stage") and entry.get("stage_name") != requirement.get("stage"):
                continue
            required_sources = requirement.get("source_files") or []
            if required_sources:
                entry_sources = entry.get("source_files") or ([entry.get("source_file")] if entry.get("source_file") else [])
                if not any(source in entry_sources for source in required_sources):
                    continue
            required_templates = requirement.get("template_files") or []
            if required_templates:
                entry_templates = entry.get("template_files") or ([entry.get("template_file")] if entry.get("template_file") else [])
                if not any(template in entry_templates for template in required_templates):
                    continue
            return True
        return False

    def _write_trace_file_locked(self, task_id: str):
        usage = self._usage_by_task.get(task_id)
        if not usage:
            return
        trace_file = usage.get("trace_file")
        if not trace_file:
            return
        payload = {
            "task_id": task_id,
            "provider": usage.get("provider", "ollama"),
            "model": usage.get("model", self.model),
            "availability_status": usage.get("availability_status", self._health_status),
            "summary": {
                **copy.deepcopy(usage),
                "model_not_used": not bool(usage.get("total_calls")),
                "sample_trace": self._sample_trace_locked(task_id),
                "missing_required_calls": self._missing_required_calls_locked(task_id),
            },
            "entries": copy.deepcopy(self._trace_entries_by_task.get(task_id, [])),
        }
        trace_path = Path(trace_file)
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        trace_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _context_values(self, usage_context: Optional[dict[str, Any]], singular: str, plural: str) -> list[str]:
        if not usage_context:
            return []
        values: list[str] = []
        for key in (singular, plural):
            item = usage_context.get(key)
            if isinstance(item, str) and item.strip():
                values.append(item.strip())
            elif isinstance(item, (list, tuple, set)):
                for value in item:
                    if isinstance(value, str) and value.strip():
                        values.append(value.strip())
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped

    def _record_call(
        self,
        usage_context: Optional[dict[str, Any]],
        success: bool,
        *,
        request_id: str,
        prompt_hash: str,
        prompt_length: int,
        response_length: int,
        latency_ms: int,
        finish_status: str,
        error: str = "",
        cache_used: bool = False,
    ):
        task_id = self._resolve_task_id(usage_context)
        stage = (usage_context or {}).get("stage", "unknown")
        sources = self._context_values(usage_context, "source_file", "source_files")
        templates = self._context_values(usage_context, "template_file", "template_files")
        probe_only = bool((usage_context or {}).get("probe_only"))
        entry = {
            "request_id": request_id,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "provider": "ollama",
            "model": self.model,
            "source_file": sources[0] if sources else "",
            "source_files": sources,
            "template_file": templates[0] if templates else "",
            "template_files": templates,
            "stage_name": stage,
            "prompt_hash": prompt_hash,
            "prompt_length": prompt_length,
            "response_length": response_length,
            "latency_ms": latency_ms,
            "finish_status": finish_status,
            "fallback_used": bool((usage_context or {}).get("fallback_used", False)),
            "cache_used": bool(cache_used),
            "probe_only": probe_only,
            "error": error,
        }
        with self._usage_lock:
            usage = self._ensure_task_state(task_id)
            usage["called"] = True
            usage["model_not_used"] = False
            usage["total_calls"] += 1
            if success:
                usage["successful_calls"] += 1
            usage["per_stage"][stage] = usage["per_stage"].get(stage, 0) + 1
            for source in sources:
                usage["per_source"][source] = usage["per_source"].get(source, 0) + 1
                source_stage_usage = usage["per_source_stage"].setdefault(source, {})
                source_stage_usage[stage] = source_stage_usage.get(stage, 0) + 1
                if probe_only and source not in usage["probe_sources"]:
                    usage["probe_sources"].append(source)
                if probe_only:
                    usage["probe_source_calls"][source] = usage["probe_source_calls"].get(source, 0) + 1
            for template in templates:
                usage["per_template"][template] = usage["per_template"].get(template, 0) + 1
                template_usage = usage["per_template_source_stage"].setdefault(template, {})
                for source in sources:
                    template_source_usage = template_usage.setdefault(source, {})
                    template_source_usage[stage] = template_source_usage.get(stage, 0) + 1
            self._trace_entries_by_task.setdefault(task_id, []).append(entry)
            usage["sample_trace"] = self._sample_trace_locked(task_id)
            self._write_trace_file_locked(task_id)

    def check_health(self) -> dict:
        """Check if Ollama is available."""
        try:
            with _make_client(timeout=10) as client:
                response = client.get(f"{self.base_url}/api/tags")
            if response.status_code == 200:
                models = [model.get("name", "") for model in response.json().get("models", [])]
                model_found = any(self.model in model for model in models)
                self._available = model_found
                self._health_status = "ok" if model_found else "model_not_found"
                return {
                    "status": self._health_status,
                    "models": models,
                    "target_model": self.model,
                    "model_available": model_found,
                }
            self._available = False
            self._health_status = "error"
            return {"status": "error", "message": f"HTTP {response.status_code}"}
        except Exception as exc:
            self._available = False
            self._health_status = "error"
            return {"status": "error", "message": str(exc)}

    @property
    def is_available(self) -> bool:
        if self._available is None:
            self.check_health()
        return self._available or False

    def generate(
        self,
        prompt: str,
        system: str = "",
        temperature: Optional[float] = None,
        num_predict: Optional[int] = None,
        usage_context: Optional[dict[str, Any]] = None,
    ) -> str:
        """Call Ollama generate API and record a machine-readable trace entry."""
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature if temperature is not None else self.temperature,
                "num_predict": num_predict if num_predict is not None else self.num_predict,
            },
        }
        if system:
            payload["system"] = system

        prompt_hash = hashlib.sha256((system + "\n" + prompt).encode("utf-8")).hexdigest()[:16]
        prompt_length = len(prompt) + len(system or "")

        for attempt in range(self.max_retries):
            request_id = f"{self._resolve_task_id(usage_context)}-{uuid.uuid4().hex[:12]}"
            try:
                logger.info("LLM call attempt %s/%s, prompt length=%s", attempt + 1, self.max_retries, len(prompt))
                started_at = time.time()
                with _make_client(timeout=180) as client:
                    response = client.post(f"{self.base_url}/api/generate", json=payload)
                latency_ms = int((time.time() - started_at) * 1000)
                logger.info("LLM responded in %.1fs, status=%s", latency_ms / 1000.0, response.status_code)

                if response.status_code != 200:
                    self._record_call(
                        usage_context,
                        success=False,
                        request_id=request_id,
                        prompt_hash=prompt_hash,
                        prompt_length=prompt_length,
                        response_length=0,
                        latency_ms=latency_ms,
                        finish_status=f"http_{response.status_code}",
                        error=response.text[:300],
                    )
                    logger.warning("LLM HTTP error: %s %s", response.status_code, response.text[:200])
                    continue

                data = response.json()
                result = data.get("response", "").strip()
                if not result:
                    self._record_call(
                        usage_context,
                        success=False,
                        request_id=request_id,
                        prompt_hash=prompt_hash,
                        prompt_length=prompt_length,
                        response_length=0,
                        latency_ms=latency_ms,
                        finish_status="empty_response",
                    )
                    logger.warning("LLM returned empty response")
                    continue

                self._record_call(
                    usage_context,
                    success=True,
                    request_id=request_id,
                    prompt_hash=prompt_hash,
                    prompt_length=prompt_length,
                    response_length=len(result),
                    latency_ms=latency_ms,
                    finish_status="success",
                )
                return result

            except httpx.TimeoutException:
                self._record_call(
                    usage_context,
                    success=False,
                    request_id=request_id,
                    prompt_hash=prompt_hash,
                    prompt_length=prompt_length,
                    response_length=0,
                    latency_ms=180000,
                    finish_status="timeout",
                    error="timeout",
                )
                logger.warning("LLM timeout on attempt %s", attempt + 1)
            except Exception as exc:
                self._record_call(
                    usage_context,
                    success=False,
                    request_id=request_id,
                    prompt_hash=prompt_hash,
                    prompt_length=prompt_length,
                    response_length=0,
                    latency_ms=0,
                    finish_status="exception",
                    error=str(exc),
                )
                logger.warning("LLM error on attempt %s: %s", attempt + 1, exc)

        raise LLMError(f"LLM failed after {self.max_retries} attempts")

    def generate_json(
        self,
        prompt: str,
        system: str = "",
        num_predict: Optional[int] = None,
        usage_context: Optional[dict[str, Any]] = None,
    ) -> tuple[Optional[Any], str]:
        """Call LLM and parse JSON from response. Returns (parsed_object, error_string)."""
        try:
            raw = self.generate(prompt, system, num_predict=num_predict, usage_context=usage_context)
            logger.debug("LLM raw output: %s", raw[:500])
            parsed, err = safe_parse_json(raw)
            if err:
                logger.warning("JSON parse issue: %s", err)
            return parsed, err
        except LLMError as exc:
            return None, str(exc)


_ollama_service: Optional[OllamaService] = None


def get_ollama_service() -> OllamaService:
    global _ollama_service
    if _ollama_service is None:
        _ollama_service = OllamaService()
    return _ollama_service
