"""Tests for the evaluation service (Module F) - gold standard comparison."""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _write_csv(path: Path, rows: list[list[str]]) -> str:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    return str(path)


def _write_xlsx(path: Path, rows: list[list]) -> str:
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    wb.save(path)
    return str(path)


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_perfect_csv_match():
    """Two identical CSVs should yield 100% cell accuracy."""
    from app.services.evaluation_service import compare_outputs

    data = [
        ["城市", "GDP"],
        ["南京市", "18500"],
        ["苏州市", "22718"],
    ]

    with tempfile.TemporaryDirectory() as tmp:
        out = _write_csv(Path(tmp) / "output.csv", data)
        gold = _write_csv(Path(tmp) / "gold.csv", data)

        result = compare_outputs(out, gold)

    assert result["cell_accuracy"] == 100.0
    assert result["matched_cells"] == result["total_cells"]
    assert result["meets_accuracy_threshold"] is True
    print("✓ test_perfect_csv_match passed")


def test_partial_mismatch_csv():
    """One wrong cell should reduce accuracy below 100%."""
    from app.services.evaluation_service import compare_outputs

    with tempfile.TemporaryDirectory() as tmp:
        out = _write_csv(
            Path(tmp) / "output.csv",
            [["城市", "GDP"], ["南京市", "18000"], ["苏州市", "22718"]],  # 18000 is wrong
        )
        gold = _write_csv(
            Path(tmp) / "gold.csv",
            [["城市", "GDP"], ["南京市", "18500"], ["苏州市", "22718"]],
        )

        result = compare_outputs(out, gold)

    assert result["cell_accuracy"] < 100.0
    assert result["matched_cells"] < result["total_cells"]
    assert len(result["mismatch_examples"]) >= 1
    mis = result["mismatch_examples"][0]
    assert mis["column"] == "GDP"
    print("✓ test_partial_mismatch_csv passed")


def test_empty_cells_ignored_with_ignore_empty():
    """Empty gold cells should not count against accuracy when ignore_empty=True."""
    from app.services.evaluation_service import compare_outputs

    with tempfile.TemporaryDirectory() as tmp:
        out = _write_csv(
            Path(tmp) / "output.csv",
            [["Name", "Score"], ["Alice", "90"], ["Bob", ""]],
        )
        gold = _write_csv(
            Path(tmp) / "gold.csv",
            [["Name", "Score"], ["Alice", "90"], ["Bob", ""]],  # Bob score is empty in gold
        )

        result = compare_outputs(out, gold, ignore_empty=True)

    # Should only count Alice's non-empty row
    assert result["cell_accuracy"] == 100.0
    print("✓ test_empty_cells_ignored_with_ignore_empty passed")


def test_numeric_normalization_in_comparison():
    """'18500.0' and '18500' should be treated as equal."""
    from app.services.evaluation_service import compare_outputs

    with tempfile.TemporaryDirectory() as tmp:
        out = _write_csv(
            Path(tmp) / "output.csv",
            [["城市", "GDP"], ["南京市", "18500.0"]],
        )
        gold = _write_csv(
            Path(tmp) / "gold.csv",
            [["城市", "GDP"], ["南京市", "18500"]],
        )

        result = compare_outputs(out, gold)

    assert result["cell_accuracy"] == 100.0, f"Expected 100%, got {result['cell_accuracy']}"
    print("✓ test_numeric_normalization_in_comparison passed")


def test_excel_vs_csv_comparison():
    """Comparing an Excel output against a CSV gold should work."""
    from app.services.evaluation_service import compare_outputs

    with tempfile.TemporaryDirectory() as tmp:
        out = _write_xlsx(
            Path(tmp) / "output.xlsx",
            [["城市", "人口（万）"], ["杭州市", 1200], ["宁波市", 940]],
        )
        gold = _write_csv(
            Path(tmp) / "gold.csv",
            [["城市", "人口（万）"], ["杭州市", "1200"], ["宁波市", "940"]],
        )

        result = compare_outputs(out, gold)

    assert result["cell_accuracy"] == 100.0, f"Got {result['cell_accuracy']}"
    print("✓ test_excel_vs_csv_comparison passed")


def test_missing_output_file():
    """Non-existent output file should return an error dict."""
    from app.services.evaluation_service import compare_outputs

    with tempfile.TemporaryDirectory() as tmp:
        gold = _write_csv(Path(tmp) / "gold.csv", [["A", "B"], ["1", "2"]])
        result = compare_outputs("/nonexistent/path/output.csv", gold)

    assert "error" in result
    print("✓ test_missing_output_file passed")


def test_completely_wrong_output():
    """All wrong cells should yield 0% accuracy."""
    from app.services.evaluation_service import compare_outputs

    with tempfile.TemporaryDirectory() as tmp:
        out = _write_csv(
            Path(tmp) / "output.csv",
            [["城市", "GDP"], ["北京市", "99999"], ["上海市", "88888"]],
        )
        gold = _write_csv(
            Path(tmp) / "gold.csv",
            [["城市", "GDP"], ["南京市", "18500"], ["苏州市", "22718"]],
        )

        result = compare_outputs(out, gold)

    assert result["cell_accuracy"] < 50.0
    assert result["meets_accuracy_threshold"] is False
    print("✓ test_completely_wrong_output passed")


def test_row_accuracy_metric():
    """row_accuracy should count fully-matching rows."""
    from app.services.evaluation_service import compare_outputs

    with tempfile.TemporaryDirectory() as tmp:
        # Row 1 is correct, row 2 has a wrong GDP
        out = _write_csv(
            Path(tmp) / "output.csv",
            [["城市", "GDP"], ["南京市", "18500"], ["苏州市", "99999"]],
        )
        gold = _write_csv(
            Path(tmp) / "gold.csv",
            [["城市", "GDP"], ["南京市", "18500"], ["苏州市", "22718"]],
        )

        result = compare_outputs(out, gold)

    assert result["matched_rows"] == 1
    assert result["compared_rows"] == 2
    assert result["row_accuracy"] == 50.0
    print("✓ test_row_accuracy_metric passed")


def test_normalize_cell_strips_whitespace():
    """_normalize_cell should strip leading/trailing whitespace."""
    from app.services.evaluation_service import _normalize_cell

    assert _normalize_cell("  hello  ") == "hello"
    assert _normalize_cell("18500.0") == "18500"
    assert _normalize_cell("0.5") == "0.5"
    assert _normalize_cell("") == ""
    assert _normalize_cell("南京市") == "南京市"
    print("✓ test_normalize_cell_strips_whitespace passed")


def test_excel_perfect_match():
    """Two identical Excel files should yield 100% accuracy."""
    from app.services.evaluation_service import compare_outputs

    data = [["Name", "Score", "Rank"], ["Alice", 95, 1], ["Bob", 88, 2]]
    with tempfile.TemporaryDirectory() as tmp:
        out = _write_xlsx(Path(tmp) / "output.xlsx", data)
        gold = _write_xlsx(Path(tmp) / "gold.xlsx", data)

        result = compare_outputs(out, gold)

    assert result["cell_accuracy"] == 100.0
    print("✓ test_excel_perfect_match passed")


if __name__ == "__main__":
    test_perfect_csv_match()
    test_partial_mismatch_csv()
    test_empty_cells_ignored_with_ignore_empty()
    test_numeric_normalization_in_comparison()
    test_excel_vs_csv_comparison()
    test_missing_output_file()
    test_completely_wrong_output()
    test_row_accuracy_metric()
    test_normalize_cell_strips_whitespace()
    test_excel_perfect_match()
    print("\n✅ All evaluation service tests passed!")
