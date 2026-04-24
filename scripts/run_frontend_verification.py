from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the real DocFusion frontend upload/processing flow.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Frontend base URL")
    parser.add_argument("--source", dest="sources", action="append", required=True, help="Absolute source file path")
    parser.add_argument("--template", dest="templates", action="append", required=True, help="Absolute template file path")
    parser.add_argument("--requirement", default="", help="Requirement textarea content")
    parser.add_argument("--strict-mode", action="store_true", help="Enable strict mode")
    parser.add_argument("--disable-llm", action="store_true", help="Disable LLM checkbox")
    parser.add_argument("--headful", action="store_true", help="Show browser UI")
    parser.add_argument("--timeout", type=int, default=900, help="Overall timeout in seconds")
    parser.add_argument("--download-dir", default="", help="Directory to store downloaded output files")
    parser.add_argument("--output-json", default="", help="Path to write verification summary JSON")
    return parser.parse_args()


def _require_existing(paths: list[str], label: str):
    missing = [path for path in paths if not Path(path).exists()]
    if missing:
        raise FileNotFoundError(f"Missing {label} files: {missing}")


def _wait_for(predicate, *, timeout_s: int, interval_s: float, description: str):
    deadline = time.time() + timeout_s
    last_error = ""
    while time.time() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
        time.sleep(interval_s)
    if last_error:
        raise TimeoutError(f"Timed out waiting for {description}: {last_error}")
    raise TimeoutError(f"Timed out waiting for {description}")


def _text(locator) -> str:
    try:
        value = locator.text_content(timeout=3000)
    except PlaywrightTimeoutError:
        return ""
    return (value or "").strip()


def _checked(page, selector: str) -> bool:
    return bool(page.locator(selector).evaluate("el => !!el.checked"))


def _set_checkbox(page, selector: str, desired: bool):
    current = _checked(page, selector)
    if current != desired:
        page.locator(selector).click()


def _poll_task(base_url: str, task_id: str, timeout_s: int) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        response = requests.get(f"{base_url.rstrip('/')}/api/status/{task_id}", timeout=30)
        response.raise_for_status()
        last_payload = response.json()
        if last_payload.get("status") in {"completed", "error"}:
            return last_payload
        time.sleep(1)
    raise TimeoutError(f"Task {task_id} did not finish within {timeout_s}s")


def _scrape_result_cards(page) -> list[dict[str, Any]]:
    cards = page.locator(".result-card")
    results: list[dict[str, Any]] = []
    for index in range(cards.count()):
        card = cards.nth(index)
        results.append(
            {
                "title": _text(card.locator("h3")),
                "summary": _text(card.locator(".fill-rate")),
                "meta": _text(card.locator("p").first),
                "download_href": card.locator(".download-btn").get_attribute("href") if card.locator(".download-btn").count() else "",
                "card_text": _text(card),
            }
        )
    return results


def _download_outputs(base_url: str, result_payload: dict[str, Any], download_dir: Path) -> list[str]:
    download_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[str] = []
    for item in result_payload.get("results", []):
        output_file = str(item.get("output_file") or "").strip()
        if not output_file:
            continue
        file_name = Path(output_file).name
        response = requests.get(
            f"{base_url.rstrip('/')}/api/download/{quote(file_name)}",
            timeout=120,
        )
        response.raise_for_status()
        destination = download_dir / file_name
        destination.write_bytes(response.content)
        downloaded.append(str(destination))
    return downloaded


def run() -> int:
    args = parse_args()
    _require_existing(args.sources, "source")
    _require_existing(args.templates, "template")

    download_dir = Path(args.download_dir) if args.download_dir else Path("/tmp") / f"docfusion_frontend_{int(time.time())}"
    summary: dict[str, Any] = {
        "base_url": args.base_url,
        "sources": args.sources,
        "templates": args.templates,
        "requirement": args.requirement,
        "strict_mode": args.strict_mode,
        "llm_enabled": not args.disable_llm,
        "started_at": time.time(),
    }

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not args.headful)
        page = browser.new_page(viewport={"width": 1440, "height": 1800})
        try:
            page.goto(args.base_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector("#source-input", timeout=30000)
            _wait_for(
                lambda: _text(page.locator("#model-info")) and "检查模型状态" not in _text(page.locator("#model-info")),
                timeout_s=60,
                interval_s=1.0,
                description="frontend health banner",
            )

            page.locator("#source-input").set_input_files(args.sources)
            page.locator("#template-input").set_input_files(args.templates)

            if args.requirement:
                page.locator("#requirement").fill(args.requirement)
            _set_checkbox(page, "#strict-mode", args.strict_mode)
            _set_checkbox(page, "#use-llm", not args.disable_llm)

            page.locator("#process-btn").click()

            task_id = _wait_for(
                lambda: _text(page.locator("#task-id")) if _text(page.locator("#task-id")) not in {"", "-"} else "",
                timeout_s=120,
                interval_s=1.0,
                description="task id",
            )
            task_payload = _poll_task(args.base_url, task_id, args.timeout)

            _wait_for(
                lambda: not page.locator("#process-btn").is_disabled(),
                timeout_s=120,
                interval_s=1.0,
                description="frontend button reset",
            )
            page.wait_for_timeout(1500)

            summary.update(
                {
                    "task_id": task_id,
                    "task_payload": task_payload,
                    "frontend": {
                        "model_banner": _text(page.locator("#model-info")),
                        "task_stage": _text(page.locator("#task-stage-text")),
                        "task_progress": _text(page.locator("#task-progress-text")),
                        "task_model_usage": _text(page.locator("#task-model-usage")),
                        "result_cards": _scrape_result_cards(page),
                        "logs": [_text(page.locator("#log-container .log-line").nth(i)) for i in range(page.locator("#log-container .log-line").count())],
                    },
                }
            )
            summary["downloaded_files"] = _download_outputs(args.base_url, task_payload, download_dir)
            summary["finished_at"] = time.time()
            summary["status"] = task_payload.get("status")
        finally:
            browser.close()

    payload_text = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.output_json:
        Path(args.output_json).write_text(payload_text, encoding="utf-8")
    print(payload_text)
    return 0 if summary.get("status") == "completed" else 1


if __name__ == "__main__":
    sys.exit(run())