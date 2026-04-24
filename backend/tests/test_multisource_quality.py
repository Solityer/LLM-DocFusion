"""Coverage for generic source connectors, normalization, quality, and operations."""
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_json_source_normalization_and_quality():
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services.fusion_service import fuse_document_tables
    from app.services.quality_service import analyze_documents

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "data.json"
        path.write_text(
            json.dumps([
                {"name": "Alpha", "price": "12.5元", "date": "2026年4月24日"},
                {"name": "Alpha", "price": "", "date": "bad-date"},
            ], ensure_ascii=False),
            encoding="utf-8",
        )
        document = read_document(str(path), FileRole.SOURCE)
        assert document.file_type == "json"
        assert document.tables[0].row_count == 2

        quality = analyze_documents([document])
        assert quality.summary["issue_count"] >= 1
        assert any(issue.issue_type in {"missing_value", "date_format_error"} for issue in quality.issues)

        fusion = fuse_document_tables([document])
        assert fusion["summary"]["raw_records"] == 2
        assert fusion["summary"]["fused_records"] >= 1


def test_sqlite_source_connector_preview():
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import load_source, preview_sources

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "demo.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("create table items (name text, amount real)")
        conn.execute("insert into items values (?, ?)", ("Alpha", 10.5))
        conn.commit()
        conn.close()

        spec = SourceSpec(
            source_type=SourceType.DATABASE,
            database_type="sqlite",
            database_path=str(db_path),
            query="select * from items",
        )
        document = load_source(spec)
        assert document.file_type == "database"
        assert document.tables[0].headers == ["name", "amount"]
        preview = preview_sources([spec])[0]
        assert preview.status == "ok"
        assert preview.tables == 1


def test_document_operation_extract_from_json():
    from app.schemas.operation_models import DocumentOperationRequest
    from app.services.document_operation_service import operate_document

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "data.json"
        path.write_text(json.dumps([{"price": "19.9", "date": "2026-04-24"}]), encoding="utf-8")
        response = operate_document(DocumentOperationRequest(
            file_path=str(path),
            operation="extract",
            fields=["价格", "日期"],
        ))
        assert response.status == "ok"
        assert response.result["field_count"] == 2
        assert response.result["fields"]["价格"]


if __name__ == "__main__":
    test_json_source_normalization_and_quality()
    test_sqlite_source_connector_preview()
    test_document_operation_extract_from_json()
    print("new multisource quality tests passed")
