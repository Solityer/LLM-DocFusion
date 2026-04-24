"""Field and value normalization utilities shared across ingestion, fusion, and quality checks."""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from typing import Any

from ..schemas.models import NormalizedTable
from ..utils.text_utils import clean_cell_value
from .schema_registry_service import (
    canonical_field_name,
    infer_field_type,
    normalize_field_name,
    registry_null_tokens,
    registry_units,
)


CHINESE_DIGITS = {
    "零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
    "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
}
CHINESE_UNITS = {"十": 10, "百": 100, "千": 1000, "万": 10000, "亿": 100000000}


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", clean_cell_value(value))
    text = text.replace("，", ",").replace("。", ".").replace("：", ":").replace("；", ";")
    return re.sub(r"\s+", " ", text).strip()


def is_null_value(value: Any) -> bool:
    normalized = normalize_field_name(normalize_text(value))
    return normalized in registry_null_tokens()


def normalize_value(value: Any, field_name: str = "", field_type: str = "") -> dict[str, Any]:
    original = clean_cell_value(value)
    text = normalize_text(value)
    if is_null_value(text):
        return {
            "original": original,
            "standard_value": "",
            "field_type": field_type or infer_field_type(field_name),
            "unit": "",
            "status": "null",
        }
    resolved_type = field_type or infer_field_type(field_name, [text])
    unit = detect_unit(text)
    if resolved_type in {"number", "currency"}:
        number = normalize_number(text)
        return {
            "original": original,
            "standard_value": number if number is not None else text,
            "field_type": resolved_type,
            "unit": unit,
            "status": "ok" if number is not None else "type_error",
        }
    if resolved_type == "percent":
        percent = normalize_percent(text)
        return {
            "original": original,
            "standard_value": percent if percent is not None else text,
            "field_type": resolved_type,
            "unit": "%" if "%" in text or "％" in original else unit,
            "status": "ok" if percent is not None else "type_error",
        }
    if resolved_type == "date":
        date_value = normalize_date(text)
        return {
            "original": original,
            "standard_value": date_value or text,
            "field_type": resolved_type,
            "unit": "",
            "status": "ok" if date_value else "type_error",
        }
    return {
        "original": original,
        "standard_value": text,
        "field_type": resolved_type,
        "unit": unit,
        "status": "ok",
    }


def normalize_number(value: Any) -> float | int | None:
    text = normalize_text(value)
    if not text:
        return None
    multiplier = 1
    if "万亿" in text:
        multiplier = 1000000000000
    elif "亿" in text:
        multiplier = 100000000
    elif "万" in text:
        multiplier = 10000
    number_match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", text)
    if number_match:
        raw = number_match.group(0).replace(",", "")
        try:
            numeric = float(raw) * multiplier
            return int(numeric) if numeric.is_integer() else round(numeric, 8)
        except ValueError:
            return None
    chinese = chinese_number_to_int(text)
    return chinese * multiplier if chinese is not None else None


def normalize_percent(value: Any) -> float | None:
    text = normalize_text(value)
    number = normalize_number(text)
    if number is None:
        return None
    if "%" in text or "百分" in text:
        return round(float(number) / 100.0, 8)
    return float(number)


def normalize_date(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    cleaned = text.replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-").replace(".", "-")
    cleaned = re.sub(r"-+", "-", cleaned).strip("- ")
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m",
        "%Y",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(cleaned[:len(datetime.now().strftime(fmt))], fmt)
            if fmt == "%Y":
                return parsed.strftime("%Y-01-01")
            if fmt == "%Y-%m":
                return parsed.strftime("%Y-%m-01")
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    match = re.search(r"((?:19|20)\d{2})-(\d{1,2})(?:-(\d{1,2}))?", cleaned)
    if match:
        year, month, day = match.group(1), match.group(2), match.group(3) or "1"
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def detect_unit(value: Any) -> str:
    text = normalize_text(value)
    for unit in sorted(registry_units(), key=len, reverse=True):
        if unit and unit in text:
            return unit
    match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?\s*([A-Za-z%]+)", text)
    return match.group(1) if match else ""


def chinese_number_to_int(text: str) -> int | None:
    chars = [ch for ch in str(text) if ch in CHINESE_DIGITS or ch in CHINESE_UNITS]
    if not chars:
        return None
    total = 0
    section = 0
    number = 0
    for ch in chars:
        if ch in CHINESE_DIGITS:
            number = CHINESE_DIGITS[ch]
        elif ch in {"十", "百", "千"}:
            unit = CHINESE_UNITS[ch]
            section += (number or 1) * unit
            number = 0
        elif ch in {"万", "亿"}:
            section += number
            total += (section or 1) * CHINESE_UNITS[ch]
            section = 0
            number = 0
    return total + section + number


def normalize_table(table: NormalizedTable) -> dict[str, Any]:
    headers = list(table.headers)
    canonical_headers = [canonical_field_name(header) for header in headers]
    normalized_rows = []
    for row_index, row in enumerate(table.rows):
        normalized_row = {}
        for col_index, header in enumerate(headers):
            value = row[col_index] if col_index < len(row) else ""
            normalized_row[canonical_headers[col_index] or header] = normalize_value(value, header)
        normalized_rows.append({"row_index": row_index, "values": normalized_row})
    return {
        "table_index": table.table_index,
        "sheet_name": table.sheet_name,
        "headers": headers,
        "canonical_headers": canonical_headers,
        "rows": normalized_rows,
    }
