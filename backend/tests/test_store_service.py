"""Tests for the SQLite document store service (Module A)."""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_temp_db_path(tmp_dir: Path) -> Path:
    """Patch DATA_DIR and DB_PATH to use a throw-away temp directory."""
    import app.services.document_store_service as svc
    data_dir = tmp_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    svc.DATA_DIR = data_dir
    svc.DB_PATH = data_dir / "test_store.sqlite"
    # Reset _DB_LOCK so it is fresh for each test
    import threading
    svc._DB_LOCK = threading.Lock()
    svc._STORE_TASKS = {}
    svc._STORE_TASK_LOCK = threading.Lock()
    return data_dir


def _write_csv(path: Path, content: str) -> str:
    path.write_text(content, encoding="utf-8")
    return str(path)


def _write_txt(path: Path, content: str) -> str:
    path.write_text(content, encoding="utf-8")
    return str(path)


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_init_db_creates_schema():
    """init_db() should create all 8 tables and indices without error."""
    from app.services.document_store_service import init_db, DB_PATH, DATA_DIR

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        from app.services import document_store_service as svc
        svc.init_db()
        assert svc.DB_PATH.exists(), "DB file should be created"

        import sqlite3
        conn = sqlite3.connect(str(svc.DB_PATH))
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        required = {
            "documents", "text_blocks", "tables", "table_rows",
            "extracted_entities", "extracted_fields", "quality_issues", "task_metrics",
        }
        assert required.issubset(tables), f"Missing tables: {required - tables}"
        conn.close()

    print("✓ test_init_db_creates_schema passed")


def test_csv_document_import():
    """A CSV document bundle should be imported with table rows and fields."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_path = _write_csv(
            Path(tmp) / "city_gdp.csv",
            "城市名,GDP总量（亿元）,常住人口（万）\n"
            "南京市,18500,955\n"
            "苏州市,22718,1275\n",
        )

        bundle = read_document(csv_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle, overwrite=False, extract_entities=True)

        assert result["status"] == "imported"
        assert result["tables"] >= 1
        assert result["fields"] >= 1

        # Verify document appears in get_documents()
        docs = svc.get_documents()
        assert any(d.document_id == result["document_id"] for d in docs)

    print("✓ test_csv_document_import passed")


def test_txt_document_import():
    """A TXT document bundle should be imported with text blocks."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        txt_path = _write_txt(
            Path(tmp) / "report.txt",
            "南京市GDP总量达到18500亿元，常住人口955万人，人均GDP约19.4万元。\n"
            "苏州市是制造业大市，GDP总量为22718亿元，常住人口1275万人。\n",
        )

        bundle = read_document(txt_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle, overwrite=False, extract_entities=True)

        assert result["status"] == "imported"
        assert result["text_blocks"] >= 1
        assert result["entities"] >= 1  # city names should be extracted

    print("✓ test_txt_document_import passed")


def test_deduplication_skips_same_content():
    """Importing the same document twice should skip the second import."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_path = _write_csv(
            Path(tmp) / "dup.csv",
            "Name,Value\nAlice,100\nBob,200\n",
        )

        bundle = read_document(csv_path, FileRole.SOURCE)
        r1 = svc.save_document_bundle(bundle, overwrite=False)
        r2 = svc.save_document_bundle(bundle, overwrite=False)

        assert r1["status"] == "imported"
        assert r2["status"] == "skipped"
        assert r2["reason"] == "duplicate_hash"

    print("✓ test_deduplication_skips_same_content passed")


def test_overwrite_replaces_existing_document():
    """overwrite=True should replace the existing record."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_path = _write_csv(
            Path(tmp) / "overwrite_test.csv",
            "Name,Value\nAlice,100\n",
        )

        bundle = read_document(csv_path, FileRole.SOURCE)
        r1 = svc.save_document_bundle(bundle, overwrite=False)
        r2 = svc.save_document_bundle(bundle, overwrite=True)

        assert r1["status"] == "imported"
        assert r2["status"] == "imported"

        docs = svc.get_documents()
        matching = [d for d in docs if d.document_id == r1["document_id"]]
        assert len(matching) == 1  # only one record, not two

    print("✓ test_overwrite_replaces_existing_document passed")


def test_search_finds_imported_field():
    """search_store() should return results matching a field name or value."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_path = _write_csv(
            Path(tmp) / "city_data.csv",
            "城市,GDP\n深圳市,32387\n广州市,30355\n",
        )

        bundle = read_document(csv_path, FileRole.SOURCE)
        svc.save_document_bundle(bundle, overwrite=False)

        results = svc.search_store("GDP")
        assert len(results) > 0, "Search should find 'GDP' in field names"
        types = {r.result_type for r in results}
        assert "field" in types or "text_block" in types or "document" in types

    print("✓ test_search_finds_imported_field passed")


def test_search_finds_imported_text_block():
    """search_store() should return text_block results matching query content."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        txt_path = _write_txt(
            Path(tmp) / "air_quality.txt",
            "北京市空气质量指数AQI为85，PM2.5浓度为42微克每立方米。\n",
        )

        bundle = read_document(txt_path, FileRole.SOURCE)
        svc.save_document_bundle(bundle, overwrite=False)

        results = svc.search_store("AQI")
        assert any(r.result_type == "text_block" for r in results)

    print("✓ test_search_finds_imported_text_block passed")


def test_quality_issues_write_and_query():
    """Quality issues passed to save_document_bundle() should be queryable."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_path = _write_csv(
            Path(tmp) / "quality_test.csv",
            "Name,Score\nAlice,N/A\nBob,200\n",
        )

        bundle = read_document(csv_path, FileRole.SOURCE)
        quality_issues = [
            {
                "issue_type": "invalid_value",
                "severity": "error",
                "field_name": "Score",
                "raw_value": "N/A",
                "normalized_value": "",
                "source": csv_path,
                "location": "row1",
                "reason": "Non-numeric value in numeric field",
                "suggestion": "Replace with 0 or leave blank",
                "affects_fill": True,
            }
        ]
        result = svc.save_document_bundle(bundle, overwrite=False, quality_issues=quality_issues)
        assert result["quality_issues"] == 1

        # Query back
        q_data = svc.get_quality_issues()
        assert q_data["total"] >= 1
        issues = q_data["issues"]
        assert any(i["issue_type"] == "invalid_value" for i in issues)
        assert any(i["field_name"] == "Score" for i in issues)

    print("✓ test_quality_issues_write_and_query passed")


def test_quality_issues_per_document():
    """get_quality_issues(document_id=...) should filter to one document."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_a = _write_csv(Path(tmp) / "a.csv", "Name,Val\nA,1\n")
        csv_b = _write_csv(Path(tmp) / "b.csv", "Name,Val\nB,2\n")

        bundle_a = read_document(csv_a, FileRole.SOURCE)
        bundle_b = read_document(csv_b, FileRole.SOURCE)

        r_a = svc.save_document_bundle(bundle_a, quality_issues=[{
            "issue_type": "missing_value", "severity": "warning",
            "field_name": "Val", "raw_value": "", "normalized_value": "",
            "source": csv_a, "location": "row0", "reason": "empty", "suggestion": "",
            "affects_fill": False,
        }])
        svc.save_document_bundle(bundle_b)

        q_a = svc.get_quality_issues(document_id=r_a["document_id"])
        assert q_a["total"] == 1
        assert q_a["issues"][0]["issue_type"] == "missing_value"

    print("✓ test_quality_issues_per_document passed")


def test_get_stats_reflects_imported_docs():
    """get_stats() should count documents, tables, text blocks correctly."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        svc.save_document_bundle(
            read_document(_write_csv(Path(tmp) / "s1.csv", "A,B\n1,2\n"), FileRole.SOURCE)
        )
        svc.save_document_bundle(
            read_document(_write_txt(Path(tmp) / "s2.txt", "Some text here."), FileRole.SOURCE)
        )

        stats = svc.get_stats()
        assert stats.document_count >= 2
        assert stats.table_count >= 1
        assert stats.text_block_count >= 1

    print("✓ test_get_stats_reflects_imported_docs passed")


def test_save_task_metrics():
    """save_task_metrics() should persist and be queryable via raw SQLite."""
    from app.services import document_store_service as svc
    import sqlite3

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        svc.save_task_metrics(
            task_id="test_task_001",
            template_count=2,
            source_count=3,
            fill_rate=95.5,
            field_match_rate=88.0,
            quality_issue_count=4,
            response_time=12.3,
            report={"summary": "ok"},
        )

        conn = sqlite3.connect(str(svc.DB_PATH))
        row = conn.execute(
            "SELECT * FROM task_metrics WHERE task_id='test_task_001'"
        ).fetchone()
        conn.close()

        assert row is not None
        assert row[2] == 3  # source_count

    print("✓ test_save_task_metrics passed")


def test_store_task_lifecycle():
    """create_store_task / get_store_task should track status correctly."""
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))

        task = svc.create_store_task()
        assert task.status == "queued"

        retrieved = svc.get_store_task(task.task_id)
        assert retrieved is not None
        assert retrieved.task_id == task.task_id

        missing = svc.get_store_task("nonexistent")
        assert missing is None

    print("✓ test_store_task_lifecycle passed")


def test_entity_extraction_finds_city_names():
    """Entity extraction should detect Chinese city names in text blocks."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        txt_path = _write_txt(
            Path(tmp) / "cities.txt",
            "杭州市经济总量突破两万亿元，宁波市进出口总额创历史新高，嘉兴市制造业转型升级成效显著。",
        )

        bundle = read_document(txt_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle, extract_entities=True)
        assert result["entities"] >= 2

        entities = svc.get_entities(entity_type="location")
        entity_texts = [e.entity_text for e in entities]
        assert any("市" in t for t in entity_texts)

    print("✓ test_entity_extraction_finds_city_names passed")


def test_store_document_detail():
    """get_document_detail() should return full preview data including text_blocks, entities, fields."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        txt_path = _write_txt(
            Path(tmp) / "detail_test.txt",
            "北京市GDP总量达到43760.7亿元，同比增长5.2%。\n上海市2024年地区生产总值为47218.6亿元。\n",
        )

        bundle = read_document(txt_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle, extract_entities=True)
        doc_id = result["document_id"]

        detail = svc.get_document_detail(doc_id)
        assert detail is not None, "Detail should not be None for existing document"
        assert detail["document_id"] == doc_id
        assert "text_block_count" in detail
        assert "entity_count" in detail
        assert "field_count" in detail
        assert "quality_issue_count" in detail
        # Should include preview arrays
        assert "text_blocks" in detail
        assert "entities" in detail
        assert "fields" in detail
        assert "quality_issues" in detail
        # At most 5 text blocks in preview
        assert len(detail["text_blocks"]) <= 5
        # At most 20 entities
        assert len(detail["entities"]) <= 20
        # text_blocks should have expected structure
        if detail["text_blocks"]:
            blk = detail["text_blocks"][0]
            assert "content" in blk
            assert "block_index" in blk

    print("✓ test_store_document_detail passed")


def test_store_document_export():
    """export_document_package() should create a valid JSON file with version field."""
    import json
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_path = _write_csv(
            Path(tmp) / "export_test.csv",
            "城市,GDP\n南京,18500\n苏州,22718\n",
        )

        bundle = read_document(csv_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle, extract_entities=True)
        doc_id = result["document_id"]

        output_dir = Path(tmp) / "exports"
        export_result = svc.export_document_package(doc_id, output_dir)

        assert export_result["status"] == "ok"
        assert export_result["document_id"] == doc_id
        assert export_result["removed"] is False
        assert export_result["download_url"].startswith("/api/download/")

        output_file = Path(export_result["output_file"])
        assert output_file.exists(), "Export file should exist"
        assert output_file.suffix == ".json"

        with open(output_file, encoding="utf-8") as f:
            pkg = json.load(f)

        assert pkg["export_version"] == "1.0"
        assert "exported_at" in pkg
        assert "document" in pkg
        assert "text_blocks" in pkg
        assert "tables" in pkg
        assert "table_rows" in pkg
        assert "entities" in pkg
        assert "fields" in pkg
        assert "quality_issues" in pkg
        assert pkg["document"]["document_id"] == doc_id

        # Document should still be in the store (not removed)
        detail = svc.get_document_detail(doc_id)
        assert detail is not None, "Document should still be in store after export"

    print("✓ test_store_document_export passed")


def test_store_document_checkout():
    """checkout_document(remove_after_export=True) should export and remove from store."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        txt_path = _write_txt(
            Path(tmp) / "checkout_test.txt",
            "武汉市GDP总量达到20011亿元，成都市地区生产总值为22074亿元。",
        )

        bundle = read_document(txt_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle, extract_entities=True)
        doc_id = result["document_id"]

        output_dir = Path(tmp) / "exports"
        checkout_result = svc.checkout_document(doc_id, output_dir, remove_after_export=True)

        assert checkout_result["status"] == "ok"
        assert checkout_result["document_id"] == doc_id
        assert checkout_result["removed"] is True
        assert Path(checkout_result["output_file"]).exists(), "Export file should exist"

        # Document should no longer be in the store
        detail = svc.get_document_detail(doc_id)
        assert detail is None, "Document should be removed from store after checkout"

    print("✓ test_store_document_checkout passed")


def test_store_document_checkout_no_remove():
    """checkout_document(remove_after_export=False) should export but keep in store."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        txt_path = _write_txt(
            Path(tmp) / "keep_test.txt",
            "重庆市GDP总量达到30145亿元，天津市实现GDP15680亿元。",
        )

        bundle = read_document(txt_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle)
        doc_id = result["document_id"]

        output_dir = Path(tmp) / "exports"
        checkout_result = svc.checkout_document(doc_id, output_dir, remove_after_export=False)

        assert checkout_result["removed"] is False
        detail = svc.get_document_detail(doc_id)
        assert detail is not None, "Document should remain in store when remove_after_export=False"

    print("✓ test_store_document_checkout_no_remove passed")


def test_store_document_delete():
    """delete_document() should remove the document and all its data."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        csv_path = _write_csv(
            Path(tmp) / "delete_test.csv",
            "A,B,C\n1,2,3\n4,5,6\n",
        )

        bundle = read_document(csv_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle)
        doc_id = result["document_id"]

        # Verify it's there
        assert svc.get_document_detail(doc_id) is not None

        # Delete it
        del_result = svc.delete_document(doc_id)
        assert del_result["status"] == "ok"
        assert del_result["deleted"] is True
        assert del_result["document_id"] == doc_id

        # Should no longer exist
        assert svc.get_document_detail(doc_id) is None

        # Stats should decrease
        docs = svc.get_documents()
        assert not any(d.document_id == doc_id for d in docs)

    print("✓ test_store_document_delete passed")


def test_export_then_delete_does_not_delete_on_export_failure():
    """checkout_document with nonexistent doc_id should raise ValueError before any deletion."""
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        output_dir = Path(tmp) / "exports"
        try:
            svc.checkout_document("nonexistent_id", output_dir, remove_after_export=True)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "nonexistent_id" in str(e)

    print("✓ test_export_then_delete_does_not_delete_on_export_failure passed")


def test_deleted_document_returns_404():
    """get_document_detail() returns None (API: 404) after deletion."""
    from app.schemas.models import FileRole
    from app.services.document_service import read_document
    from app.services import document_store_service as svc

    with tempfile.TemporaryDirectory() as tmp:
        _make_temp_db_path(Path(tmp))
        svc.init_db()

        txt_path = _write_txt(Path(tmp) / "gone.txt", "A document that will be deleted.")
        bundle = read_document(txt_path, FileRole.SOURCE)
        result = svc.save_document_bundle(bundle)
        doc_id = result["document_id"]

        svc.delete_document(doc_id)
        assert svc.get_document_detail(doc_id) is None, "Deleted document detail should be None"
        assert svc.get_document_detail("totally_missing") is None

    print("✓ test_deleted_document_returns_404 passed")


def test_product_copy_has_no_competition_words():
    """UI files and README must not contain competition-specific Chinese terms."""
    competition_words = ["竞赛", "赛题", "参赛", "评委", "作品", "比赛"]
    # Path relative to tests/ directory: ../../frontend and ../../README.md
    tests_dir = Path(__file__).parent
    project_root = tests_dir.parent.parent

    files_to_check = [
        project_root / "frontend" / "index.html",
        project_root / "frontend" / "app.js",
        project_root / "README.md",
        project_root / "docs" / "验收清单.md",
    ]

    violations: list[str] = []
    for fpath in files_to_check:
        if not fpath.exists():
            continue
        content = fpath.read_text(encoding="utf-8", errors="ignore")
        for word in competition_words:
            if word in content:
                violations.append(f"{fpath.name}: contains '{word}'")

    assert violations == [], "Product copy violations found:\n" + "\n".join(violations)
    print("✓ test_product_copy_has_no_competition_words passed")


if __name__ == "__main__":
    test_init_db_creates_schema()
    test_csv_document_import()
    test_txt_document_import()
    test_deduplication_skips_same_content()
    test_overwrite_replaces_existing_document()
    test_search_finds_imported_field()
    test_search_finds_imported_text_block()
    test_quality_issues_write_and_query()
    test_quality_issues_per_document()
    test_get_stats_reflects_imported_docs()
    test_save_task_metrics()
    test_store_task_lifecycle()
    test_entity_extraction_finds_city_names()
    test_store_document_detail()
    test_store_document_export()
    test_store_document_checkout()
    test_store_document_checkout_no_remove()
    test_store_document_delete()
    test_export_then_delete_does_not_delete_on_export_failure()
    test_deleted_document_returns_404()
    test_product_copy_has_no_competition_words()
    print("\n✅ All store service tests passed!")
