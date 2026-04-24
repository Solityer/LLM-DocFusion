"""Configurable schema registry for field aliases, types, and quality defaults."""
from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..core.config import PROJECT_ROOT
from ..utils.text_utils import similarity


REGISTRY_PATH = PROJECT_ROOT / "config" / "schema_registry.json"


DEFAULT_REGISTRY: dict[str, Any] = {
    "null_tokens": ["", "nan", "none", "null", "n/a", "-", "--", "无", "空", "缺失", "未知"],
    "field_types": {
        "entity": {"aliases": ["名称", "单位", "城市", "地区", "国家", "name", "entity"]},
        "date": {"aliases": ["日期", "时间", "年份", "date", "time", "year"]},
        "number": {"aliases": ["数量", "数值", "金额", "价格", "count", "amount", "value", "price"]},
        "percent": {"aliases": ["比例", "占比", "率", "percent", "rate", "ratio"]},
        "currency": {"aliases": ["金额", "价格", "收入", "支出", "currency", "price", "revenue"]},
    },
    "canonical_fields": {},
    "quality_rules": {
        "missing_required_ratio": 0.5,
        "low_confidence_threshold": 0.45,
        "numeric_outlier_zscore": 3.5,
        "duplicate_key_min_fields": 2,
    },
    "units": ["元", "万元", "亿元", "%"],
}


@lru_cache(maxsize=1)
def load_schema_registry() -> dict[str, Any]:
    registry = dict(DEFAULT_REGISTRY)
    if REGISTRY_PATH.exists():
        try:
            payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
            registry = _deep_merge(registry, payload)
        except Exception:
            registry = dict(DEFAULT_REGISTRY)
    return registry


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def normalize_field_name(name: str) -> str:
    """Normalize field names for matching across languages and punctuation variants."""
    text = unicodedata.normalize("NFKC", str(name or "")).strip().lower()
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"[\s_\-:：,，、/\\|;；.。·'\"`~!！?？\[\]【】{}<>《》]+", "", text)
    return text


def canonical_field_name(name: str) -> str:
    normalized = normalize_field_name(name)
    if not normalized:
        return ""
    registry = load_schema_registry()
    for canonical, aliases in (registry.get("canonical_fields") or {}).items():
        candidates = [canonical, *(aliases or [])]
        if normalized in {normalize_field_name(item) for item in candidates if item}:
            return canonical
    return normalized


def infer_field_type(name: str, sample_values: list[Any] | None = None) -> str:
    normalized = normalize_field_name(name)
    registry = load_schema_registry()
    for field_type, payload in (registry.get("field_types") or {}).items():
        aliases = payload.get("aliases", []) if isinstance(payload, dict) else []
        patterns = payload.get("patterns", []) if isinstance(payload, dict) else []
        if any(normalize_field_name(alias) and normalize_field_name(alias) in normalized for alias in aliases):
            return field_type
        if any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in patterns):
            return field_type
    if sample_values:
        values = [str(value) for value in sample_values if value not in (None, "")]
        if values and sum(_looks_like_date(value) for value in values[:20]) >= max(1, len(values[:20]) // 2):
            return "date"
        if values and sum(_looks_like_number(value) for value in values[:20]) >= max(1, len(values[:20]) // 2):
            return "number"
    return "text"


def field_match_score(left: str, right: str) -> float:
    left_norm = normalize_field_name(left)
    right_norm = normalize_field_name(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    if canonical_field_name(left) == canonical_field_name(right):
        return 0.94
    if left_norm in right_norm or right_norm in left_norm:
        return 0.84
    return max(similarity(left_norm, right_norm), _token_overlap(left_norm, right_norm))


def best_field_match(target: str, candidates: list[str], threshold: float = 0.45) -> tuple[str, float]:
    best = ""
    best_score = 0.0
    for candidate in candidates:
        score = field_match_score(target, candidate)
        if score > best_score:
            best = candidate
            best_score = score
    return (best, best_score) if best_score >= threshold else ("", best_score)


def registry_quality_rule(name: str, default: Any = None) -> Any:
    return (load_schema_registry().get("quality_rules") or {}).get(name, default)


def registry_null_tokens() -> set[str]:
    return {normalize_field_name(item) for item in load_schema_registry().get("null_tokens", [])}


def registry_units() -> list[str]:
    return list(load_schema_registry().get("units", []))


def source_type_catalog() -> list[dict[str, Any]]:
    return [
        {"type": "local_file", "name": "本地文件源", "supports": ["xlsx", "xls", "docx", "md", "txt", "csv", "json"]},
        {"type": "http_api", "name": "HTTP/API 数据源", "supports": ["json", "csv", "text"], "methods": ["GET"]},
        {"type": "web_page", "name": "网页数据源", "supports": ["html_text", "html_table"]},
        {"type": "database", "name": "数据库数据源", "supports": ["sqlite"], "planned": ["mysql", "postgresql"]},
    ]


def _token_overlap(left: str, right: str) -> float:
    left_tokens = set(re.findall(r"[a-z0-9]+|[\u4e00-\u9fa5]{1,4}", left))
    right_tokens = set(re.findall(r"[a-z0-9]+|[\u4e00-\u9fa5]{1,4}", right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / max(min(len(left_tokens), len(right_tokens)), 1)


def _looks_like_number(value: str) -> bool:
    return bool(re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", str(value or "")))


def _looks_like_date(value: str) -> bool:
    return bool(re.search(r"(?:19|20)\d{2}[-/.年](?:0?[1-9]|1[0-2])", str(value or "")))
