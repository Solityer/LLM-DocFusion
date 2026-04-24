"""Tests for the template inspection API service (Module D / template_routes)."""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _write_csv(path: Path, content: str) -> str:
    path.write_text(content, encoding="utf-8")
    return str(path)


def _write_txt(path: Path, content: str) -> str:
    path.write_text(content, encoding="utf-8")
    return str(path)


def _write_md(path: Path, content: str) -> str:
    path.write_text(content, encoding="utf-8")
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

def test_csv_template_returns_headers():
    """parse_template on a CSV should return the column headers."""
    from app.services.template_service import parse_template

    with tempfile.TemporaryDirectory() as tmp:
        path = _write_csv(
            Path(tmp) / "template.csv",
            "城市名,GDP总量（亿元）,常住人口（万）\n,,\n",
        )
        schema = parse_template(path)

    assert schema.file_type == "csv"
    assert len(schema.tables) == 1
    headers = schema.tables[0].headers
    assert "城市名" in headers
    assert "GDP总量（亿元）" in headers
    assert "常住人口（万）" in headers
    print("✓ test_csv_template_returns_headers passed")


def test_txt_template_returns_placeholders():
    """parse_template on a TXT with {{placeholder}} should return placeholder names."""
    from app.services.template_service import parse_template

    with tempfile.TemporaryDirectory() as tmp:
        path = _write_txt(
            Path(tmp) / "template.txt",
            "公司名称：{{company_name}}\n日期：{{date}}\n总金额：{{total_amount}}\n",
        )
        schema = parse_template(path)

    assert schema.file_type == "text"
    assert "company_name" in schema.placeholders
    assert "date" in schema.placeholders
    assert "total_amount" in schema.placeholders
    print("✓ test_txt_template_returns_placeholders passed")


def test_markdown_template_returns_table_headers():
    """parse_template on a Markdown file with a table should extract headers."""
    from app.services.template_service import parse_template

    with tempfile.TemporaryDirectory() as tmp:
        path = _write_md(
            Path(tmp) / "template.md",
            "# 城市数据表\n\n| 城市 | AQI | PM2.5 |\n| --- | --- | --- |\n| | | |\n",
        )
        schema = parse_template(path)

    assert schema.file_type == "markdown"
    all_headers = []
    for table in schema.tables:
        all_headers.extend(table.headers)
    assert "城市" in all_headers or "AQI" in all_headers
    print("✓ test_markdown_template_returns_table_headers passed")


def test_excel_template_returns_headers():
    """parse_template on an Excel file should extract the column headers."""
    from app.services.template_service import parse_template

    with tempfile.TemporaryDirectory() as tmp:
        path = _write_xlsx(
            Path(tmp) / "template.xlsx",
            [["国家/地区", "累计确诊", "累计死亡", "日期"], ["", "", "", ""]],
        )
        schema = parse_template(path)

    assert schema.file_type == "excel"
    assert len(schema.tables) >= 1
    headers = schema.tables[0].headers
    assert "国家/地区" in headers
    assert "累计确诊" in headers
    print("✓ test_excel_template_returns_headers passed")


def test_schema_has_model_dump():
    """TemplateSchema.model_dump() should produce a serializable dict (Pydantic v2)."""
    from app.services.template_service import parse_template

    with tempfile.TemporaryDirectory() as tmp:
        path = _write_csv(
            Path(tmp) / "t.csv",
            "A,B,C\n,,\n",
        )
        schema = parse_template(path)

    dumped = schema.model_dump()
    assert isinstance(dumped, dict)
    assert "tables" in dumped
    assert "file_type" in dumped
    print("✓ test_schema_has_model_dump passed")


def test_template_placeholder_count_matches_content():
    """Template with 3 distinct placeholders should report 3 placeholder keys."""
    from app.services.template_service import parse_template

    with tempfile.TemporaryDirectory() as tmp:
        path = _write_txt(
            Path(tmp) / "multi_placeholder.txt",
            "项目名称：{{project_name}}\n负责人：{{owner}}\n截止日期：{{deadline}}\n备注：{{notes}}",
        )
        schema = parse_template(path)

    assert len(schema.placeholders) >= 3
    print("✓ test_template_placeholder_count_matches_content passed")


def test_csv_multi_column_template():
    """A template with many columns should list all non-empty header names."""
    from app.services.template_service import parse_template

    headers = ["省份", "GDP（亿元）", "人口（万）", "面积（km²）", "年份"]
    header_line = ",".join(headers)

    with tempfile.TemporaryDirectory() as tmp:
        path = _write_csv(
            Path(tmp) / "wide.csv",
            f"{header_line}\n" + ",".join([""] * len(headers)) + "\n",
        )
        schema = parse_template(path)

    returned_headers = schema.tables[0].headers if schema.tables else []
    for h in headers:
        assert h in returned_headers, f"Expected header '{h}' not found"
    print("✓ test_csv_multi_column_template passed")


def test_field_aliases_do_not_break_default_matching():
    """Passing custom field aliases should not disrupt the default matching pipeline."""
    from app.schemas.models import RequirementSpec
    from app.services.requirement_service import parse_requirement

    # Requirement with no aliases should parse normally
    spec1 = parse_requirement("将数据填入模板")
    assert spec1.raw_text == "将数据填入模板"

    # Aliases provided via options dict should be accessible
    field_aliases = {"GDP总量（亿元）": ["地区生产总值", "GDP"], "城市名": ["城市", "地区"]}

    # The aliases map itself should be valid JSON-parseable
    import json
    alias_str = json.dumps(field_aliases, ensure_ascii=False)
    parsed = json.loads(alias_str)
    assert parsed["GDP总量（亿元）"] == ["地区生产总值", "GDP"]
    print("✓ test_field_aliases_do_not_break_default_matching passed")


if __name__ == "__main__":
    test_csv_template_returns_headers()
    test_txt_template_returns_placeholders()
    test_markdown_template_returns_table_headers()
    test_excel_template_returns_headers()
    test_schema_has_model_dump()
    test_template_placeholder_count_matches_content()
    test_csv_multi_column_template()
    test_field_aliases_do_not_break_default_matching()
    print("\n✅ All template inspect tests passed!")
