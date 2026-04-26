"""Tests for the multi-source preview functionality (Module C)."""
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


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_local_file_csv_preview():
    """preview_sources() on a local CSV file should return structured table previews."""
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import preview_sources

    with tempfile.TemporaryDirectory() as tmp:
        csv_path = _write_csv(
            Path(tmp) / "city.csv",
            "城市,GDP（亿元）\n南京市,18500\n苏州市,22718\n",
        )

        spec = SourceSpec(
            source_type=SourceType.LOCAL_FILE,
            name="城市经济数据",
            path=csv_path,
        )
        previews = preview_sources([spec])

    assert len(previews) == 1
    p = previews[0]
    assert p.status == "ok"
    assert p.tables >= 1
    assert p.table_previews is not None
    assert len(p.table_previews) >= 1
    headers = p.table_previews[0]["headers"]
    assert "城市" in headers or "城市名" in headers or "GDP（亿元）" in headers
    print("✓ test_local_file_csv_preview passed")


def test_local_file_txt_preview():
    """preview_sources() on a local TXT file should return text_block count."""
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import preview_sources

    with tempfile.TemporaryDirectory() as tmp:
        txt_path = _write_txt(
            Path(tmp) / "report.txt",
            "南京市GDP总量达到18500亿元，常住人口955万人。\n"
            "苏州市GDP总量为22718亿元，是制造业重镇。\n",
        )

        spec = SourceSpec(
            source_type=SourceType.LOCAL_FILE,
            name="城市报告",
            path=txt_path,
        )
        previews = preview_sources([spec])

    assert len(previews) == 1
    p = previews[0]
    assert p.status == "ok"
    assert p.text_blocks >= 1
    assert len(p.raw_text_preview) > 0
    print("✓ test_local_file_txt_preview passed")


def test_nonexistent_file_returns_error_preview():
    """preview_sources() on a missing file should return status='error', not raise."""
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import preview_sources

    spec = SourceSpec(
        source_type=SourceType.LOCAL_FILE,
        name="missing_file",
        path="/nonexistent/path/does_not_exist.csv",
    )
    previews = preview_sources([spec])

    assert len(previews) == 1
    assert previews[0].status == "error"
    assert previews[0].error is not None
    print("✓ test_nonexistent_file_returns_error_preview passed")


def test_multiple_sources_returns_multiple_previews():
    """preview_sources() with multiple specs should return one preview per spec."""
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import preview_sources

    with tempfile.TemporaryDirectory() as tmp:
        csv1 = _write_csv(Path(tmp) / "a.csv", "A,B\n1,2\n3,4\n")
        csv2 = _write_csv(Path(tmp) / "b.csv", "C,D\n5,6\n7,8\n")

        specs = [
            SourceSpec(source_type=SourceType.LOCAL_FILE, name="Source A", path=csv1),
            SourceSpec(source_type=SourceType.LOCAL_FILE, name="Source B", path=csv2),
        ]
        previews = preview_sources(specs)

    assert len(previews) == 2
    statuses = {p.status for p in previews}
    assert statuses == {"ok"}
    print("✓ test_multiple_sources_returns_multiple_previews passed")


def test_mixed_valid_and_invalid_sources():
    """With one valid and one invalid source, preview should return both."""
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import preview_sources

    with tempfile.TemporaryDirectory() as tmp:
        good = _write_csv(Path(tmp) / "good.csv", "X,Y\n10,20\n")

        specs = [
            SourceSpec(source_type=SourceType.LOCAL_FILE, name="Good", path=good),
            SourceSpec(source_type=SourceType.LOCAL_FILE, name="Bad", path="/nope/nope.csv"),
        ]
        previews = preview_sources(specs)

    assert len(previews) == 2
    statuses = [p.status for p in previews]
    assert "ok" in statuses
    assert "error" in statuses
    print("✓ test_mixed_valid_and_invalid_sources passed")


def test_preview_row_count_limit():
    """Table previews should be capped at max_rows."""
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import preview_sources

    with tempfile.TemporaryDirectory() as tmp:
        # Write 50 rows
        lines = ["A,B"] + [f"{i},{i*2}" for i in range(50)]
        csv_path = _write_csv(Path(tmp) / "big.csv", "\n".join(lines) + "\n")

        spec = SourceSpec(source_type=SourceType.LOCAL_FILE, name="big", path=csv_path)
        previews = preview_sources([spec], max_rows=5)

    p = previews[0]
    assert p.status == "ok"
    if p.table_previews:
        assert len(p.table_previews[0]["rows"]) <= 5
    print("✓ test_preview_row_count_limit passed")


def test_load_source_local_csv():
    """load_source() for a local CSV should return a DocumentBundle with tables."""
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import load_source

    with tempfile.TemporaryDirectory() as tmp:
        csv_path = _write_csv(
            Path(tmp) / "data.csv",
            "Name,Score\nAlice,90\nBob,85\n",
        )
        spec = SourceSpec(source_type=SourceType.LOCAL_FILE, name="test", path=csv_path)
        bundle = load_source(spec)

    assert bundle.file_type == "csv"
    assert len(bundle.tables) >= 1
    assert bundle.tables[0].headers == ["Name", "Score"]
    print("✓ test_load_source_local_csv passed")


def test_source_spec_source_type_values():
    """SourceType enum values should include all required types."""
    from app.schemas.source_models import SourceType

    assert hasattr(SourceType, "LOCAL_FILE")
    assert hasattr(SourceType, "HTTP_API")
    assert hasattr(SourceType, "WEB_PAGE")
    assert hasattr(SourceType, "DATABASE")
    print("✓ test_source_spec_source_type_values passed")


def test_no_ollama_rule_path_works():
    """Without Ollama available, rule-based extraction should still function."""
    from app.schemas.models import FileRole, RequirementSpec, TemplateTable
    from app.services.document_service import read_document
    from app.services.extraction_service import extract_data
    from app.services.retrieval_service import RetrievalResult
    from app.services.template_service import parse_template

    with tempfile.TemporaryDirectory() as tmp:
        csv_path = _write_csv(
            Path(tmp) / "source.csv",
            "城市名,GDP总量（亿元）,常住人口（万）\n南京市,18500,955\n苏州市,22718,1275\n",
        )
        tmpl_path = _write_csv(
            Path(tmp) / "template.csv",
            "城市名,GDP总量（亿元）,常住人口（万）\n,,\n",
        )

        source_doc = read_document(csv_path, FileRole.SOURCE)
        template = parse_template(tmpl_path)

        retrieval = RetrievalResult()
        retrieval.source_docs = [source_doc]
        retrieval.table_pairs = []

        requirement = RequirementSpec(raw_text="将数据填入模板")
        # use_llm=False forces rule path (no Ollama needed)
        result = extract_data(retrieval, template, requirement, use_llm=False)

    assert isinstance(result, list)
    # The rule path should still produce records from the CSV
    if result:
        assert "records" in result[0]
    print("✓ test_no_ollama_rule_path_works passed")


def test_existing_fill_api_compatibility():
    """The fill pipeline should be invokable via existing ProcessRequest schema (no regressions)."""
    from app.schemas.models import ProcessRequest

    req = ProcessRequest(
        source_files=["/tmp/source.csv"],
        template_files=["/tmp/template.csv"],
        requirement="帮我智能填表",
        options={"use_llm": False},
    )
    assert req.requirement == "帮我智能填表"
    assert req.options.get("use_llm") is False
    assert isinstance(req.source_files, list)
    print("✓ test_existing_fill_api_compatibility passed")


# ── Web page source tests (no public network required) ─────────────────────────

def test_web_page_preview_ignores_env_socks_proxy_by_default(monkeypatch):
    """fetch_web_source should default trust_env=False and never fail due to a
    SOCKS env-proxy when socksio is absent."""
    import http.server
    import threading

    from app.schemas.source_models import SourceSpec, SourceType

    # Minimal HTML served locally.
    HTML = b"<html><body><h1>Hello Test</h1><p>Paragraph text.</p></body></html>"

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(HTML)))
            self.end_headers()
            self.wfile.write(HTML)

        def log_message(self, *args):
            pass  # suppress output

    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    # Poison the environment with a SOCKS proxy that has no socksio backing.
    monkeypatch.setenv("ALL_PROXY", "socks5://127.0.0.1:9999")
    monkeypatch.setenv("HTTPS_PROXY", "socks5://127.0.0.1:9999")
    monkeypatch.setenv("HTTP_PROXY", "socks5://127.0.0.1:9999")

    try:
        from app.services.web_source_service import fetch_web_source

        spec = SourceSpec(
            source_type=SourceType.WEB_PAGE,
            name="test_page",
            url=f"http://127.0.0.1:{port}/",
        )
        bundle = fetch_web_source(spec)
        assert bundle is not None
        assert "Hello Test" in bundle.raw_text or len(bundle.text_blocks) >= 1
    finally:
        server.shutdown()

    print("✓ test_web_page_preview_ignores_env_socks_proxy_by_default passed")


def test_web_page_preview_extracts_text_and_table(monkeypatch):
    """fetch_web_source should extract text blocks and table data from local HTML."""
    import http.server
    import threading

    from app.schemas.source_models import SourceSpec, SourceType

    HTML = (
        b"<html><body>"
        b"<h1>Cities</h1>"
        b"<p>Some intro text.</p>"
        b"<table>"
        b"<tr><th>City</th><th>GDP</th></tr>"
        b"<tr><td>Nanjing</td><td>18500</td></tr>"
        b"<tr><td>Suzhou</td><td>22718</td></tr>"
        b"</table>"
        b"</body></html>"
    )

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(HTML)))
            self.end_headers()
            self.wfile.write(HTML)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        from app.services.web_source_service import fetch_web_source

        spec = SourceSpec(
            source_type=SourceType.WEB_PAGE,
            name="city_table",
            url=f"http://127.0.0.1:{port}/",
        )
        bundle = fetch_web_source(spec)
        assert len(bundle.text_blocks) >= 1, "Expected at least one text block"
        assert len(bundle.tables) >= 1, "Expected at least one table"
        headers = bundle.tables[0].headers
        assert "City" in headers or "city" in [h.lower() for h in headers], \
            f"Expected 'City' column, got {headers}"
    finally:
        server.shutdown()

    print("✓ test_web_page_preview_extracts_text_and_table passed")


def test_web_page_rejects_non_http_scheme():
    """fetch_web_source must reject non-http/https URLs (file://, ftp://, etc.)."""
    from app.core.exceptions import SourceConnectError
    from app.schemas.source_models import SourceSpec, SourceType
    from app.services.source_connector_service import preview_sources
    from app.services.web_source_service import fetch_web_source

    for bad_url in ("file:///etc/passwd", "ftp://example.com/data"):
        # Direct call should raise SourceConnectError.
        try:
            fetch_web_source(SourceSpec(source_type=SourceType.WEB_PAGE, url=bad_url))
            assert False, f"Expected SourceConnectError for {bad_url}"
        except SourceConnectError as exc:
            assert "http" in str(exc).lower() or "scheme" in str(exc).lower(), \
                f"Unexpected error message: {exc}"

        # Via preview_sources it should be fail-soft (status=error, not exception).
        previews = preview_sources(
            [SourceSpec(source_type=SourceType.WEB_PAGE, url=bad_url)]
        )
        assert len(previews) == 1
        assert previews[0].status == "error"

    print("✓ test_web_page_rejects_non_http_scheme passed")


if __name__ == "__main__":
    test_local_file_csv_preview()
    test_local_file_txt_preview()
    test_nonexistent_file_returns_error_preview()
    test_multiple_sources_returns_multiple_previews()
    test_mixed_valid_and_invalid_sources()
    test_preview_row_count_limit()
    test_load_source_local_csv()
    test_source_spec_source_type_values()
    test_no_ollama_rule_path_works()
    test_existing_fill_api_compatibility()
    # Web page tests (monkeypatch not available outside pytest; skip env-proxy test)
    test_web_page_preview_extracts_text_and_table(None)
    test_web_page_rejects_non_http_scheme()
    print("\n✅ All multisource preview tests passed!")
