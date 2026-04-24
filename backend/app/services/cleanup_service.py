"""Workspace cleanup helpers used before and after each processing run."""
from __future__ import annotations

import shutil
import time
from pathlib import Path

from ..core.config import OUTPUT_DIR, PROJECT_ROOT, UPLOAD_DIR
from ..core.logging import logger

# Keep this many most-recent output files; excess files are removed after each run.
MAX_OUTPUTS = 20
UPLOAD_RETENTION_SECONDS = 6 * 60 * 60


RUNTIME_CACHE_DIRS = [
    PROJECT_ROOT / ".pytest_cache",
    PROJECT_ROOT / "backend" / "app" / "__pycache__",
    PROJECT_ROOT / "backend" / "app" / "api" / "__pycache__",
    PROJECT_ROOT / "backend" / "app" / "core" / "__pycache__",
    PROJECT_ROOT / "backend" / "app" / "schemas" / "__pycache__",
    PROJECT_ROOT / "backend" / "app" / "services" / "__pycache__",
    PROJECT_ROOT / "backend" / "app" / "utils" / "__pycache__",
    PROJECT_ROOT / "backend" / "tests" / "__pycache__",
]

REDUNDANT_FILES = [
    PROJECT_ROOT / "backend" / "app" / "services" / "extraction_service.py.bak",
]

REDUNDANT_OUTPUT_DIRS = [
    PROJECT_ROOT / "results",
]


def _safe_remove(path: Path):
    """Remove a file or directory if it exists."""
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def _normalize_keep_upload_dirs(keep_upload_dir: Path | None = None, keep_upload_dirs: list[Path] | None = None) -> set[Path]:
    """Normalize keep-upload inputs into a resolved path set."""
    keepers: set[Path] = set()
    if keep_upload_dir:
        keepers.add(keep_upload_dir.resolve())
    for item in keep_upload_dirs or []:
        if item:
            keepers.add(item.resolve())
    return keepers


def _prune_stale_upload_dirs(keepers: set[Path], *, now: float | None = None):
    """Remove only stale upload directories so concurrent tasks do not delete each other."""
    now = now or time.time()
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for child in UPLOAD_DIR.iterdir():
        try:
            resolved = child.resolve()
        except FileNotFoundError:
            continue
        if resolved in keepers:
            continue
        age_seconds = now - child.stat().st_mtime
        if age_seconds < UPLOAD_RETENTION_SECONDS:
            continue
        logger.info("Removing stale upload artifact: %s", child.name)
        _safe_remove(child)


def cleanup_runtime_artifacts(keep_upload_dir: Path | None = None, keep_upload_dirs: list[Path] | None = None):
    """Clean stale uploads, caches, and redundant generated files before a run.
    
    Output files are NOT deleted here to allow the user to download previous results.
    Output trimming happens in cleanup_after_run to keep only the latest MAX_OUTPUTS files.
    """
    logger.info("Cleaning runtime artifacts before processing")
    keepers = _normalize_keep_upload_dirs(keep_upload_dir=keep_upload_dir, keep_upload_dirs=keep_upload_dirs)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Do NOT delete output files at the start of each run - users need to download them.
    # Old outputs are trimmed by cleanup_after_run after each task.

    for extra_dir in REDUNDANT_OUTPUT_DIRS:
        if extra_dir.exists():
            for child in extra_dir.iterdir():
                _safe_remove(child)

    _prune_stale_upload_dirs(keepers)

    for cache_dir in RUNTIME_CACHE_DIRS:
        _safe_remove(cache_dir)

    for redundant in REDUNDANT_FILES:
        _safe_remove(redundant)


def cleanup_after_run(keep_upload_dir: Path | None = None, keep_upload_dirs: list[Path] | None = None):
    """Post-run cleanup: trim outputs to the MAX_OUTPUTS most recent, and remove stale uploads."""
    logger.info("Cleaning runtime artifacts after processing")
    keepers = _normalize_keep_upload_dirs(keep_upload_dir=keep_upload_dir, keep_upload_dirs=keep_upload_dirs)
    _prune_stale_upload_dirs(keepers)

    # Trim output directory to keep only the newest MAX_OUTPUTS files
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_files = sorted(
        [child for child in OUTPUT_DIR.iterdir() if child.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old_file in output_files[MAX_OUTPUTS:]:
        logger.info("Trimming old output file: %s", old_file.name)
        _safe_remove(old_file)

    for cache_dir in RUNTIME_CACHE_DIRS:
        _safe_remove(cache_dir)

    for redundant in REDUNDANT_FILES:
        _safe_remove(redundant)
