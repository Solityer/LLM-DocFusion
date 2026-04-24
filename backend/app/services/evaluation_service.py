"""Evaluation service for comparing filled outputs against gold standards."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from ..core.logging import logger


def compare_outputs(
    output_file: str,
    gold_file: str,
    key_columns: list[str] | None = None,
    ignore_empty: bool = True,
) -> dict[str, Any]:
    """Compare a filled output file against a gold-standard file.

    Supports Excel (.xlsx/.xls) and CSV.
    Returns: cell_accuracy, row_accuracy, matched_cells, total_cells,
             mismatch_examples, missing_cells, extra_cells.
    """
    output_rows = _read_tabular(output_file)
    gold_rows = _read_tabular(gold_file)

    if not output_rows or not gold_rows:
        return {
            "error": "Could not read one or both files",
            "output_rows": len(output_rows),
            "gold_rows": len(gold_rows),
        }

    output_headers = output_rows[0] if output_rows else {}
    gold_headers = gold_rows[0] if gold_rows else {}

    # Align headers (both sides use the union)
    all_headers = list(dict.fromkeys(list(gold_headers.keys()) + list(output_headers.keys())))

    matched_cells = 0
    total_cells = 0
    mismatch_examples: list[dict[str, Any]] = []
    missing_cells = 0  # cells in gold but not in output
    extra_cells = 0    # cells in output but not in gold

    min_rows = min(len(output_rows), len(gold_rows))
    max_rows = max(len(output_rows), len(gold_rows))
    matched_rows = 0

    for row_idx in range(min_rows):
        out_row = output_rows[row_idx]
        gold_row = gold_rows[row_idx]
        row_match = True

        for header in all_headers:
            out_val = _normalize_cell(out_row.get(header, ""))
            gold_val = _normalize_cell(gold_row.get(header, ""))

            if ignore_empty and not gold_val:
                continue

            total_cells += 1
            if out_val == gold_val:
                matched_cells += 1
            else:
                row_match = False
                if len(mismatch_examples) < 20:
                    mismatch_examples.append({
                        "row": row_idx + 1,
                        "column": header,
                        "output": out_val,
                        "gold": gold_val,
                    })

        if row_match:
            matched_rows += 1

    # Extra/missing row cells
    missing_cells = sum(
        sum(1 for h in all_headers if _normalize_cell(gold_rows[i].get(h, "")) and not _normalize_cell(output_rows[i].get(h, "") if i < len(output_rows) else ""))
        for i in range(min_rows, len(gold_rows))
    ) if len(gold_rows) > min_rows else 0

    extra_cells = sum(
        sum(1 for h in all_headers if _normalize_cell(output_rows[i].get(h, "")) and not _normalize_cell(gold_rows[i].get(h, "") if i < len(gold_rows) else ""))
        for i in range(min_rows, len(output_rows))
    ) if len(output_rows) > min_rows else 0

    cell_accuracy = round(matched_cells / max(total_cells, 1) * 100, 2)
    row_accuracy = round(matched_rows / max(min_rows, 1) * 100, 2)

    return {
        "cell_accuracy": cell_accuracy,
        "row_accuracy": row_accuracy,
        "matched_cells": matched_cells,
        "total_cells": total_cells,
        "matched_rows": matched_rows,
        "compared_rows": min_rows,
        "output_row_count": len(output_rows),
        "gold_row_count": len(gold_rows),
        "mismatch_examples": mismatch_examples,
        "missing_cells": missing_cells,
        "extra_cells": extra_cells,
        "meets_accuracy_threshold": cell_accuracy >= 80.0,
        "accuracy_threshold": 80.0,
    }


def _read_tabular(file_path: str) -> list[dict[str, str]]:
    """Read Excel or CSV file into list of row dicts."""
    path = Path(file_path)
    if not path.exists():
        logger.warning(f"evaluate: file not found: {file_path}")
        return []

    suffix = path.suffix.lower()
    try:
        if suffix in {".xlsx", ".xls"}:
            return _read_excel(str(path))
        elif suffix == ".csv":
            return _read_csv(str(path))
        else:
            # Try word table extraction
            return _read_word_tables(str(path))
    except Exception as exc:
        logger.warning(f"evaluate: failed to read {file_path}: {exc}")
        return []


def _read_excel(file_path: str) -> list[dict[str, str]]:
    import openpyxl
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [str(cell or "").strip() for cell in rows[0]]
    result = []
    for row in rows[1:]:
        row_dict = {headers[i]: str(cell or "").strip() for i, cell in enumerate(row) if i < len(headers)}
        if any(row_dict.values()):
            result.append(row_dict)
    return result


def _read_csv(file_path: str) -> list[dict[str, str]]:
    result = []
    with open(file_path, encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            result.append({k: str(v or "").strip() for k, v in row.items()})
    return result


def _read_word_tables(file_path: str) -> list[dict[str, str]]:
    """Extract first table from a Word document."""
    try:
        from docx import Document
        doc = Document(file_path)
        for table in doc.tables:
            if not table.rows:
                continue
            headers = [cell.text.strip() for cell in table.rows[0].cells]
            result = []
            for row in table.rows[1:]:
                cells = [cell.text.strip() for cell in row.cells]
                row_dict = {headers[i]: cells[i] if i < len(cells) else "" for i in range(len(headers))}
                result.append(row_dict)
            return result
    except Exception:
        pass
    return []


def _normalize_cell(value: str) -> str:
    """Normalize a cell value for comparison."""
    v = str(value or "").strip()
    # Remove trailing zeros after decimal
    try:
        f = float(v.replace(",", ""))
        if f == int(f):
            return str(int(f))
        return str(round(f, 4))
    except (ValueError, TypeError):
        pass
    # Normalize whitespace and full-width chars
    v = v.replace("　", " ").replace("\u00a0", " ")
    return " ".join(v.split())
