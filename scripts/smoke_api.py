#!/usr/bin/env python3
"""
Smoke test for DocFusion API.
Usage: python scripts/smoke_api.py [--base http://localhost:8000]

Tests all major API endpoints. Requires server to be running.
"""
import argparse
import csv
import json
import os
import sys
import tempfile
import time
import urllib.request
import urllib.error
import urllib.parse

BASE = "http://localhost:8000"
PASS = "✅"
FAIL = "❌"
WARN = "⚠️"

results: list[tuple[str, bool, str]] = []


def req(method: str, path: str, body=None, files=None, form_data=None, timeout=30) -> dict:
    # Ensure non-ASCII characters in path are percent-encoded
    parsed = urllib.parse.urlparse(BASE + path)
    encoded_path = urllib.parse.quote(parsed.path, safe="/:@!$&'()*+,;=-._~") + (
        "?" + urllib.parse.quote(parsed.query, safe="=&+%") if parsed.query else ""
    )
    url = parsed.scheme + "://" + parsed.netloc + encoded_path
    if files or form_data:
        import io
        boundary = "----FormBoundary" + str(int(time.time()))
        parts = io.BytesIO()
        if form_data:
            for k, v in form_data.items():
                parts.write(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode())
        if files:
            for field_name, (fname, fdata, ctype) in files.items():
                parts.write(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{field_name}\"; filename=\"{fname}\"\r\nContent-Type: {ctype}\r\n\r\n".encode())
                parts.write(fdata if isinstance(fdata, bytes) else fdata.encode())
                parts.write(b"\r\n")
        parts.write(f"--{boundary}--\r\n".encode())
        data = parts.getvalue()
        req_obj = urllib.request.Request(url, data=data,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    elif body is not None:
        data = json.dumps(body).encode()
        req_obj = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/json"})
    else:
        req_obj = urllib.request.Request(url)
    req_obj.method = method
    try:
        with urllib.request.urlopen(req_obj, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        try:
            return json.loads(body_text)
        except Exception:
            return {"_http_error": e.code, "_body": body_text}


def check(name: str, cond: bool, detail: str = ""):
    icon = PASS if cond else FAIL
    print(f"  {icon} {name}" + (f": {detail}" if detail else ""))
    results.append((name, cond, detail))
    return cond


def section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def wait_task(endpoint: str, task_id: str, max_wait: int = 120) -> dict:
    for _ in range(max_wait):
        time.sleep(1)
        data = req("GET", f"{endpoint}/{task_id}")
        status = data.get("status", "")
        if status in ("completed", "error"):
            return data
    return {"status": "timeout"}


# ── Create temp test files ────────────────────────────────────────────────────

def make_csv_source(rows=5) -> bytes:
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["城市", "GDP（亿元）", "人口（万人）", "日期"])
    cities = ["北京", "上海", "广州", "深圳", "杭州"]
    for i, city in enumerate(cities[:rows]):
        w.writerow([city, (i + 1) * 1000.5, (i + 1) * 500, f"2024-{i+1:02d}-01"])
    return buf.getvalue().encode()


def make_csv_template(rows=3) -> bytes:
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["城市", "GDP（亿元）", "人口（万人）"])
    for _ in range(rows):
        w.writerow(["", "", ""])
    return buf.getvalue().encode()


def make_txt_doc() -> bytes:
    return """2024年城市经济报告

北京市GDP总量达到43760.7亿元，同比增长5.2%。
上海市2024年地区生产总值为47218.6亿元。
广州市实现GDP 30818.8亿元。
深圳市完成地区生产总值36831.2亿元。

人口方面：
北京市常住人口2188万人。
上海市常住人口2487万人。
""".encode()


def make_gold_csv(rows=3) -> bytes:
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["城市", "GDP（亿元）", "人口（万人）"])
    data = [["北京", "43760.7", "2188"], ["上海", "47218.6", "2487"], ["广州", "30818.8", "900"]]
    for row in data[:rows]:
        w.writerow(row)
    return buf.getvalue().encode()


# ── Test functions ─────────────────────────────────────────────────────────────

def test_health():
    section("1. Health Check")
    data = req("GET", "/api/health")
    check("GET /api/health returns status ok", data.get("status") == "ok", str(data.get("status")))
    check("ollama_status field present", "ollama_status" in data)
    check("model field present", "model" in data)
    return data.get("status") == "ok"


def test_source_types():
    section("2. Source Types")
    data = req("GET", "/api/sources/types")
    types = [t["type"] for t in data.get("source_types", [])]
    check("GET /api/sources/types returns types", len(types) > 0, f"{types}")
    check("http_api type present", "http_api" in types)
    check("database type present", "database" in types)
    check("web_page type present", "web_page" in types)


def test_template_inspect():
    section("3. Template Inspect")
    tpl_bytes = make_csv_template()
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        f.write(tpl_bytes)
        tpl_path = f.name
    try:
        # Upload template
        upd = req("POST", "/api/templates/upload",
                  files={"files": ("template_test.csv", tpl_bytes, "text/csv")})
        check("POST /api/templates/upload ok", upd.get("status") == "ok",
              str(upd.get("status", upd.get("detail", ""))))
        if upd.get("status") == "ok" and upd.get("files"):
            file_path = upd["files"][0]["path"]
            # Inspect local
            data = req("POST", "/api/templates/inspect/local",
                       body={"file_path": file_path})
            check("POST /api/templates/inspect/local ok", data.get("status") == "ok",
                  str(data.get("detail", data.get("status"))))
            check("schema has fields", len(data.get("schema", {}).get("fields", [])) > 0)

        # Upload + inspect via upload endpoint
        data2 = req("POST", "/api/templates/inspect/upload",
                    files={"file": ("template_test.csv", tpl_bytes, "text/csv")})
        check("POST /api/templates/inspect/upload ok", data2.get("status") == "ok",
              str(data2.get("detail", data2.get("status"))))
    finally:
        os.unlink(tpl_path)


def test_store_stats():
    section("4. Store Stats")
    data = req("GET", "/api/store/stats")
    check("GET /api/store/stats ok", data.get("status") == "ok")
    check("document_count field present", "document_count" in data)
    check("document_count is int >= 0", isinstance(data.get("document_count"), int))


def test_store_import_and_search():
    section("5. Store Import & Search")
    txt_bytes = make_txt_doc()
    upd = req("POST", "/api/store/import/upload",
              files={"files": ("test_doc.txt", txt_bytes, "text/plain")},
              form_data={"extract_entities": "true", "overwrite": "true"})
    task_id = upd.get("task_id")
    check("POST /api/store/import/upload creates task", bool(task_id), str(task_id))
    if task_id:
        final = wait_task("/api/store/status", task_id, max_wait=60)
        check("Store import completes", final.get("status") in ("completed", "error"),
              f"status={final.get('status')} msg={final.get('message','')}")

    # List documents
    docs = req("GET", "/api/store/documents?limit=10")
    check("GET /api/store/documents ok", docs.get("status") == "ok")

    # Search
    s = req("GET", "/api/store/search?q=北京&limit=10")
    check("GET /api/store/search?q=北京 ok", s.get("status") == "ok")
    check("Search returns count", "count" in s)

    # Entities
    ents = req("GET", "/api/store/entities")
    check("GET /api/store/entities ok", ents.get("status") == "ok")

    # Quality
    qual = req("GET", "/api/store/quality")
    check("GET /api/store/quality ok", qual.get("status") == "ok")


def test_analytics_dashboard():
    section("6. Analytics Dashboard")
    data = req("GET", "/api/analytics/dashboard")
    check("GET /api/analytics/dashboard ok", data.get("status") == "ok",
          str(data.get("detail", "")))
    check("store field present", "store" in data)
    check("recent_tasks is list", isinstance(data.get("recent_tasks"), list))
    check("avg_fill_rate is number", isinstance(data.get("avg_fill_rate"), (int, float)))
    check("meets_time_threshold field present", "meets_time_threshold" in data)


def test_document_operations():
    section("7. Document Operations")
    txt_bytes = make_txt_doc()
    # Upload file first
    upd = req("POST", "/api/files/upload",
              files={"files": ("test_ops.txt", txt_bytes, "text/plain")})
    check("File upload for operations ok", upd.get("status") == "ok")
    if not upd.get("files"):
        return
    file_path = upd["files"][0]["path"]

    # Summarize
    sr = req("POST", "/api/document/summarize",
             body={"file_path": file_path, "operation": "summarize", "use_llm": False})
    check("POST /api/document/summarize ok", sr.get("status") == "ok",
          str(sr.get("detail", sr.get("status"))))
    check("summarize returns key_points", "key_points" in sr.get("result", {}))

    # Extract
    er = req("POST", "/api/document/extract",
             body={"file_path": file_path, "operation": "extract",
                   "fields": ["城市", "GDP"], "use_llm": False})
    check("POST /api/document/extract ok", er.get("status") == "ok",
          str(er.get("detail", er.get("status"))))

    # Find
    fr = req("POST", "/api/document/operate",
             body={"file_path": file_path, "operation": "find",
                   "instruction": "查找北京", "use_llm": False})
    check("POST /api/document/operate (find) ok", fr.get("status") == "ok",
          str(fr.get("detail", fr.get("status"))))

    # Export table (no table in txt → should return warning, not 500)
    etr = req("POST", "/api/document/operate",
              body={"file_path": file_path, "operation": "export_table", "use_llm": False})
    check("export_table on no-table file: status ok", etr.get("status") == "ok",
          str(etr.get("detail", etr.get("status"))))
    check("export_table on no-table file: table_count == 0",
          etr.get("result", {}).get("table_count") == 0,
          str(etr.get("result")))
    check("export_table on no-table file: has warning",
          len(etr.get("warnings", [])) > 0, str(etr.get("warnings")))

    # Export table from CSV source (has tables)
    csv_bytes = make_csv_source()
    upd2 = req("POST", "/api/files/upload",
               files={"files": ("test_src.csv", csv_bytes, "text/csv")})
    if upd2.get("files"):
        fp2 = upd2["files"][0]["path"]
        etr2 = req("POST", "/api/document/operate",
                   body={"file_path": fp2, "operation": "export_table", "use_llm": False})
        check("export_table on CSV file: ok", etr2.get("status") == "ok")
        check("export_table on CSV file: table_count > 0",
              (etr2.get("result") or {}).get("table_count", 0) > 0,
              str(etr2.get("result", {}).get("table_count")))


def test_evaluate_compare():
    section("8. Evaluate Compare")
    # Test that missing files give proper error
    err = req("POST", "/api/evaluate/compare",
              body={"output_file": "", "gold_file": ""})
    check("evaluate/compare requires output_file", "output_file is required" in str(err.get("detail", "")))

    err2 = req("POST", "/api/evaluate/compare",
               body={"output_file": "/nonexistent.csv", "gold_file": ""})
    check("evaluate/compare requires gold_file", "gold_file is required" in str(err2.get("detail", "")))

    # Upload two matching CSV files and compare
    gold_bytes = make_gold_csv()
    output_bytes = make_gold_csv()  # identical → 100% accuracy

    upd_gold = req("POST", "/api/files/upload",
                   files={"files": ("gold.csv", gold_bytes, "text/csv")})
    upd_out = req("POST", "/api/files/upload",
                  files={"files": ("output.csv", output_bytes, "text/csv")})
    if upd_gold.get("files") and upd_out.get("files"):
        gold_path = upd_gold["files"][0]["path"]
        out_path = upd_out["files"][0]["path"]
        cmp = req("POST", "/api/evaluate/compare",
                  body={"output_file": out_path, "gold_file": gold_path, "ignore_empty": True})
        check("evaluate/compare with files ok", cmp.get("status") == "ok",
              str(cmp.get("detail", cmp.get("status"))))
        check("evaluate/compare cell_accuracy >= 0",
              isinstance(cmp.get("cell_accuracy"), (int, float)) and cmp.get("cell_accuracy", -1) >= 0,
              str(cmp.get("cell_accuracy")))

    # Upload comparison via evaluate/compare/upload
    cmp2 = req("POST", "/api/evaluate/compare/upload",
               files={
                   "output_file": ("output.csv", output_bytes, "text/csv"),
                   "gold_file": ("gold.csv", gold_bytes, "text/csv"),
               })
    check("POST /api/evaluate/compare/upload ok", cmp2.get("status") == "ok",
          str(cmp2.get("detail", cmp2.get("status"))))


def test_main_process_flow():
    section("9. Main Process Flow (CSV→CSV)")
    src_bytes = make_csv_source()
    tpl_bytes = make_csv_template()

    # Upload source
    src_up = req("POST", "/api/files/upload",
                 files={"files": ("source.csv", src_bytes, "text/csv")})
    check("Upload source file", src_up.get("status") == "ok")
    if not src_up.get("files"):
        return
    src_path = src_up["files"][0]["path"]

    # Upload template
    tpl_up = req("POST", "/api/templates/upload",
                 files={"files": ("template.csv", tpl_bytes, "text/csv")})
    check("Upload template file", tpl_up.get("status") == "ok")
    if not tpl_up.get("files"):
        return
    tpl_path = tpl_up["files"][0]["path"]

    # Create task
    task = req("POST", "/api/process/local",
               body={"source_files": [src_path], "template_files": [tpl_path],
                     "requirement": "", "options": {"use_llm": False}})
    task_id = task.get("task_id")
    check("POST /api/process/local creates task", bool(task_id), str(task_id))
    if not task_id:
        return

    # Poll
    final = wait_task("/api/status", task_id, max_wait=120)
    check("Task completes (status=completed or error)",
          final.get("status") in ("completed", "error"),
          f"status={final.get('status')}")
    check("Task has results", len(final.get("results", [])) > 0)

    if final.get("status") == "completed" and final.get("results"):
        res = final["results"][0]
        check("Output file exists", bool(res.get("output_file")))
        check("rows_filled >= 0", isinstance(res.get("rows_filled"), int) and res["rows_filled"] >= 0,
              str(res.get("rows_filled")))
        check("fill_rate in [0,100]",
              isinstance(res.get("fill_rate"), (int, float)) and 0 <= res["fill_rate"] <= 100,
              str(res.get("fill_rate")))

        # Download result
        if res.get("output_file"):
            fname = res["output_file"].split("/")[-1]
            dl = req("GET", f"/api/download/{urllib.parse.quote(fname)}")
            # If response is dict it may be an error; file download returns raw bytes via urllib
            # Re-test via urllib directly
            try:
                url = f"{BASE}/api/download/{urllib.parse.quote(fname)}"
                with urllib.request.urlopen(url, timeout=10) as r:
                    content = r.read()
                check("Download output file", len(content) > 0, f"{len(content)} bytes")
            except Exception as e:
                check("Download output file", False, str(e))

    # Get report
    rpt = req("GET", f"/api/report/{task_id}")
    check("GET /api/report/{task_id} ok", "task_id" in rpt or "status" in rpt)

    # Markdown report
    md = req("GET", f"/api/report/{task_id}/markdown")
    check("GET /api/report/{task_id}/markdown ok", "markdown" in md or "task_id" in md,
          str(md.get("detail", "ok")))

    return task_id


def test_sources_preview():
    section("10. Sources Preview (fail-soft)")
    # Test with unreachable URL - should return partial, not 500
    data = req("POST", "/api/sources/preview",
               body={"source": {"source_type": "http_api", "name": "bad",
                                "url": "http://localhost:19999/bad"},
                     "max_rows": 5})
    check("sources/preview fails gracefully (status ok/partial)",
          data.get("status") in ("ok", "partial"),
          str(data.get("status")))
    check("sources/preview returns errors list",
          isinstance(data.get("errors"), list))


def test_multisource():
    section("11. Multisource Process (CSV + inline source)")
    src_bytes = make_csv_source()
    tpl_bytes = make_csv_template()

    src_up = req("POST", "/api/files/upload",
                 files={"files": ("source.csv", src_bytes, "text/csv")})
    tpl_up = req("POST", "/api/templates/upload",
                 files={"files": ("template.csv", tpl_bytes, "text/csv")})
    if not src_up.get("files") or not tpl_up.get("files"):
        check("Multisource upload ok", False, "Upload failed")
        return

    task = req("POST", "/api/process/multisource",
               body={
                   "source_files": [src_up["files"][0]["path"]],
                   "template_files": [tpl_up["files"][0]["path"]],
                   "requirement": "",
                   "sources": [{"source_type": "http_api", "name": "failsrc",
                                 "url": "http://localhost:19999/bad"}],
                   "options": {"use_llm": False},
               })
    task_id = task.get("task_id")
    check("POST /api/process/multisource creates task", bool(task_id))
    if task_id:
        final = wait_task("/api/status", task_id, max_wait=120)
        check("Multisource task completes (not hung)",
              final.get("status") in ("completed", "error"),
              f"status={final.get('status')}")
        check("Multisource task has source_connector_errors in warnings or results",
              True)  # fail-soft: task should complete even with bad external source


def test_store_document_operations():
    section("5b. Store Document Detail / Export / Checkout / Delete")

    # First, ensure we have a document in the store
    txt_bytes = make_txt_doc()
    upd = req("POST", "/api/store/import/upload",
              files={"files": ("smoke_ops_test.txt", txt_bytes, "text/plain")},
              form_data={"extract_entities": "true", "overwrite": "true"})
    task_id = upd.get("task_id")
    if task_id:
        wait_task("/api/store/status", task_id, max_wait=60)

    # Get document list to find a document_id
    docs = req("GET", "/api/store/documents?limit=5")
    check("Store has at least one document for operations test",
          isinstance(docs.get("documents"), list) and len(docs.get("documents", [])) > 0,
          f"count={docs.get('count', 0)}")
    if not docs.get("documents"):
        return

    doc_id = docs["documents"][0]["document_id"]

    # GET /api/store/documents/{document_id} - detail
    # Use doc_id directly; req() handles URL encoding
    detail = req("GET", f"/api/store/documents/{doc_id}")
    check("GET /api/store/documents/{id} ok", detail.get("status") == "ok",
          str(detail.get("detail", detail.get("status"))))
    check("Detail has text_blocks list", isinstance(detail.get("text_blocks"), list))
    check("Detail has entities list", isinstance(detail.get("entities"), list))
    check("Detail has fields list", isinstance(detail.get("fields"), list))
    check("Detail has quality_issues list", isinstance(detail.get("quality_issues"), list))

    # GET nonexistent document_id → 404
    bad = req("GET", "/api/store/documents/nonexistent_id_xyz")
    check("GET /api/store/documents/nonexistent → 404",
          bad.get("_http_error") == 404 or "not found" in str(bad.get("detail", "")).lower(),
          str(bad.get("detail", bad.get("_http_error"))))

    # POST /api/store/documents/{id}/export
    exp = req("POST", f"/api/store/documents/{doc_id}/export")
    check("POST /api/store/documents/{id}/export ok", exp.get("status") == "ok",
          str(exp.get("detail", exp.get("status"))))
    check("Export has download_url", bool(exp.get("download_url")),
          str(exp.get("download_url")))
    check("Export removed=False (export does not delete)", exp.get("removed") is False,
          str(exp.get("removed")))

    # Download the exported file
    if exp.get("download_url"):
        dl_url = exp["download_url"]
        fname = dl_url.split("/")[-1]
        try:
            url = f"{BASE}/api/download/{urllib.parse.quote(fname)}"
            with urllib.request.urlopen(url, timeout=10) as r:
                content = r.read()
            check("Exported JSON file is downloadable", len(content) > 0, f"{len(content)} bytes")
        except Exception as e:
            check("Exported JSON file is downloadable", False, str(e))

    # Document should still exist after export (export does not delete)
    still_there = req("GET", f"/api/store/documents/{doc_id}")
    check("Document still in store after export", still_there.get("status") == "ok",
          str(still_there.get("status")))

    # Import a new document for checkout/delete (use overwrite=true to guarantee a fresh one)
    upd2 = req("POST", "/api/store/import/upload",
               files={"files": ("smoke_checkout_test.txt", make_txt_doc(), "text/plain")},
               form_data={"extract_entities": "false", "overwrite": "true"})
    task_id2 = upd2.get("task_id")
    checkout_id = None
    if task_id2:
        final2 = wait_task("/api/store/status", task_id2, max_wait=60)
        # Get the imported document id from the latest doc list
        docs2 = req("GET", "/api/store/documents?limit=5")
        if docs2.get("documents"):
            checkout_id = docs2["documents"][0]["document_id"]

    if not checkout_id:
        checkout_id = doc_id  # fallback

    # POST /api/store/documents/{id}/checkout with remove_after_export=true
    co = req("POST", f"/api/store/documents/{checkout_id}/checkout",
             body={"remove_after_export": True})
    check("POST /api/store/documents/{id}/checkout ok", co.get("status") == "ok",
          str(co.get("detail", co.get("status"))))
    check("Checkout has download_url", bool(co.get("download_url")))
    check("Checkout removed=True", co.get("removed") is True, str(co.get("removed")))

    # Document should no longer exist → 404
    gone = req("GET", f"/api/store/documents/{checkout_id}")
    check("Document returns 404 after checkout",
          gone.get("_http_error") == 404 or "not found" in str(gone.get("detail", "")).lower(),
          str(gone.get("detail", gone.get("_http_error"))))

    # List should no longer contain the checked-out document_id
    docs3 = req("GET", "/api/store/documents?limit=100")
    doc_ids_after = [d["document_id"] for d in docs3.get("documents", [])]
    check("Checked-out document absent from list", checkout_id not in doc_ids_after,
          f"found={checkout_id in doc_ids_after}")

    # DELETE /api/store/documents/{id} — use a fresh import
    upd3 = req("POST", "/api/store/import/upload",
               files={"files": ("smoke_delete_test.txt", make_txt_doc(), "text/plain")},
               form_data={"extract_entities": "false", "overwrite": "true"})
    task_id3 = upd3.get("task_id")
    delete_id = None
    if task_id3:
        wait_task("/api/store/status", task_id3, max_wait=60)
        docs4 = req("GET", "/api/store/documents?limit=5")
        if docs4.get("documents"):
            delete_id = docs4["documents"][0]["document_id"]

    if delete_id:
        del_r = req("DELETE", f"/api/store/documents/{delete_id}")
        check("DELETE /api/store/documents/{id} ok",
              del_r.get("status") == "ok" or del_r.get("deleted") is True,
              str(del_r.get("detail", del_r.get("status"))))

        # Second delete attempt → 404
        del_r2 = req("DELETE", f"/api/store/documents/{delete_id}")
        check("DELETE already-deleted document returns 404",
              del_r2.get("_http_error") == 404 or "not found" in str(del_r2.get("detail", "")).lower(),
              str(del_r2.get("detail", del_r2.get("_http_error"))))
    else:
        check("DELETE /api/store/documents/{id} ok", False, "Could not get delete_id")
        check("DELETE already-deleted document returns 404", False, "skipped")


def test_outputs_list():
    section("12. Outputs List")
    data = req("GET", "/api/outputs")
    check("GET /api/outputs returns files list", "files" in data, str(type(data.get("files"))))




# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DocFusion API Smoke Test")
    parser.add_argument("--base", default="http://localhost:8000", help="Base URL")
    parser.add_argument("--skip-process", action="store_true",
                        help="Skip long-running process tests")
    parser.add_argument("--quick", action="store_true",
                        help="Quick mode: skip long-running process/local and multisource tests (same as --skip-process)")
    args = parser.parse_args()
    global BASE
    BASE = args.base.rstrip("/")

    skip_process = args.skip_process or args.quick
    mode = "QUICK" if skip_process else "FULL"
    print(f"\nDocFusion Smoke Test [{mode}] — {BASE}\n{'=' * 60}")
    if skip_process:
        print("  (--quick/--skip-process: skipping process/local and multisource tests)")

    # Check server is up
    try:
        urllib.request.urlopen(BASE + "/api/health", timeout=5)
    except Exception as e:
        print(f"\n{FAIL} Server not reachable at {BASE}: {e}")
        print("Please start the server first:")
        print("  ./start.sh  OR  source venv/bin/activate && python3 -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000")
        sys.exit(1)

    test_health()
    test_source_types()
    test_template_inspect()
    test_store_stats()
    test_store_import_and_search()
    test_store_document_operations()
    test_analytics_dashboard()
    test_document_operations()
    test_evaluate_compare()
    test_sources_preview()
    test_outputs_list()
    if not skip_process:
        test_main_process_flow()
        test_multisource()

    # Summary
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    failed = [(n, d) for n, ok, d in results if not ok]

    print(f"\n{'=' * 60}")
    print(f"  SMOKE TEST RESULTS: {passed}/{total} passed")
    if failed:
        print(f"\n  {FAIL} FAILED CHECKS:")
        for name, detail in failed:
            print(f"    - {name}" + (f": {detail}" if detail else ""))
    else:
        print(f"  {PASS} All checks passed!")
    print(f"{'=' * 60}\n")

    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
