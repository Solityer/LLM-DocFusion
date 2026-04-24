from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the real DocFusion frontend flow with Selenium + Firefox.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Frontend base URL")
    parser.add_argument("--source", dest="sources", action="append", required=True, help="Absolute source file path")
    parser.add_argument("--template", dest="templates", action="append", required=True, help="Absolute template file path")
    parser.add_argument("--requirement", default="", help="Requirement textarea content")
    parser.add_argument("--strict-mode", action="store_true", help="Enable strict mode")
    parser.add_argument("--disable-llm", action="store_true", help="Disable LLM checkbox")
    parser.add_argument("--timeout", type=int, default=900, help="Overall task timeout in seconds")
    parser.add_argument("--download-dir", default="", help="Directory to store downloaded output files")
    parser.add_argument("--output-json", default="", help="Path to write summary JSON")
    parser.add_argument("--geckodriver", default="/snap/bin/geckodriver", help="Path to geckodriver")
    parser.add_argument("--firefox-binary", default="", help="Optional path to Firefox binary")
    parser.add_argument("--headful", action="store_true", help="Show the browser window")
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


def _text(driver, css_selector: str) -> str:
    try:
        return driver.find_element(By.CSS_SELECTOR, css_selector).text.strip()
    except Exception:  # noqa: BLE001
        return ""


def _non_empty_task_id(driver) -> str:
    value = _text(driver, "#task-id")
    return value if value not in {"", "-"} else ""


def _set_checkbox(driver, css_selector: str, desired: bool):
    driver.execute_script(
        """
        const el = document.querySelector(arguments[0]);
        if (!el) {
            return;
        }
        if (el.checked !== arguments[1]) {
            el.checked = arguments[1];
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        }
        """,
        css_selector,
        desired,
    )


def _set_files(driver, css_selector: str, paths: list[str]):
    file_input = driver.find_element(By.CSS_SELECTOR, css_selector)
    driver.execute_script(
        "arguments[0].hidden = false; arguments[0].style.display = 'block'; arguments[0].style.visibility = 'visible';",
        file_input,
    )
    file_input.send_keys("\n".join(paths))


def _file_list_names(driver, list_selector: str) -> list[str]:
    return [
        element.text.strip()
        for element in driver.find_elements(By.CSS_SELECTOR, f"{list_selector} .file-name")
        if element.text.strip()
    ]


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


def _collect_result_cards(driver) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for card in driver.find_elements(By.CSS_SELECTOR, ".result-card"):
        title = ""
        summary = ""
        download_href = ""
        try:
            title = card.find_element(By.CSS_SELECTOR, "h3").text.strip()
        except Exception:  # noqa: BLE001
            pass
        try:
            summary = card.find_element(By.CSS_SELECTOR, ".fill-rate").text.strip()
        except Exception:  # noqa: BLE001
            pass
        try:
            download_href = card.find_element(By.CSS_SELECTOR, ".download-btn").get_attribute("href") or ""
        except Exception:  # noqa: BLE001
            pass
        results.append(
            {
                "title": title,
                "summary": summary,
                "download_href": download_href,
                "card_text": card.text.strip(),
            }
        )
    return results


def _wait_for_download(download_dir: Path, before_names: set[str], timeout_s: int) -> list[str]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        files = [
            path for path in download_dir.iterdir()
            if path.is_file() and not path.name.endswith(".part") and not path.name.endswith(".tmp")
        ]
        new_files = [str(path) for path in files if path.name not in before_names]
        if new_files:
            return sorted(new_files)
        time.sleep(1)
    raise TimeoutError("Timed out waiting for browser download")


def _download_via_href(href: str, download_dir: Path) -> str:
    response = requests.get(href, timeout=120)
    response.raise_for_status()
    file_name = href.rstrip("/").split("/")[-1] or f"download_{int(time.time())}"
    destination = download_dir / file_name
    destination.write_bytes(response.content)
    return str(destination)


def _click_downloads(driver, download_dir: Path) -> list[str]:
    downloaded: list[str] = []
    links = driver.find_elements(By.CSS_SELECTOR, ".download-btn")
    for link in links:
        before_names = {path.name for path in download_dir.iterdir() if path.is_file()}
        href = link.get_attribute("href") or ""
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link)
        link.click()
        try:
            downloaded.extend(_wait_for_download(download_dir, before_names, timeout_s=30))
        except TimeoutError:
            if not href:
                raise
            downloaded.append(_download_via_href(href, download_dir))
    return downloaded


def _build_driver(args: argparse.Namespace, download_dir: Path) -> webdriver.Firefox:
    options = Options()
    if not args.headful:
        options.add_argument("-headless")
    if args.firefox_binary:
        options.binary_location = args.firefox_binary
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", str(download_dir))
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.helperApps.alwaysAsk.force", False)
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        ",".join([
            "application/octet-stream",
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "text/csv",
        ]),
    )
    options.set_preference("pdfjs.disabled", True)
    service = Service(args.geckodriver)
    return webdriver.Firefox(service=service, options=options)


def run() -> int:
    args = parse_args()
    _require_existing(args.sources, "source")
    _require_existing(args.templates, "template")

    download_dir = Path(args.download_dir) if args.download_dir else Path("/tmp") / f"docfusion_frontend_firefox_{int(time.time())}"
    download_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "base_url": args.base_url,
        "sources": args.sources,
        "templates": args.templates,
        "requirement": args.requirement,
        "strict_mode": args.strict_mode,
        "llm_enabled": not args.disable_llm,
        "download_dir": str(download_dir),
        "started_at": time.time(),
    }

    driver = _build_driver(args, download_dir)
    driver.set_page_load_timeout(60)
    wait = WebDriverWait(driver, 60)

    try:
        driver.get(args.base_url)
        wait.until(lambda current: current.find_element(By.CSS_SELECTOR, "#source-input"))
        _wait_for(
            lambda: (banner := _text(driver, "#model-info")) and "检查模型状态" not in banner and banner,
            timeout_s=60,
            interval_s=1.0,
            description="frontend health banner",
        )

        _set_files(driver, "#source-input", args.sources)
        _wait_for(
            lambda: len(_file_list_names(driver, "#source-list")) >= len(args.sources),
            timeout_s=30,
            interval_s=0.5,
            description="source file list",
        )

        _set_files(driver, "#template-input", args.templates)
        _wait_for(
            lambda: len(_file_list_names(driver, "#template-list")) >= len(args.templates),
            timeout_s=30,
            interval_s=0.5,
            description="template file list",
        )

        if args.requirement:
            requirement_box = driver.find_element(By.CSS_SELECTOR, "#requirement")
            requirement_box.clear()
            requirement_box.send_keys(args.requirement)

        _set_checkbox(driver, "#strict-mode", args.strict_mode)
        _set_checkbox(driver, "#use-llm", not args.disable_llm)

        process_button = driver.find_element(By.CSS_SELECTOR, "#process-btn")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", process_button)
        process_button.click()

        task_id = _wait_for(
            lambda: _non_empty_task_id(driver),
            timeout_s=120,
            interval_s=1.0,
            description="task id",
        )

        task_payload = _poll_task(args.base_url, task_id, args.timeout)

        _wait_for(
            lambda: not driver.find_element(By.CSS_SELECTOR, "#process-btn").get_property("disabled"),
            timeout_s=120,
            interval_s=1.0,
            description="process button reset",
        )
        browser_downloads: list[str] = []
        if task_payload.get("status") == "completed":
            _wait_for(
                lambda: len(driver.find_elements(By.CSS_SELECTOR, ".result-card")) >= len(args.templates),
                timeout_s=120,
                interval_s=1.0,
                description="result cards",
            )
            browser_downloads = _click_downloads(driver, download_dir)

        summary.update(
            {
                "status": task_payload.get("status"),
                "task_id": task_id,
                "task_payload": task_payload,
                "frontend": {
                    "model_banner": _text(driver, "#model-info"),
                    "task_stage": _text(driver, "#task-stage-text"),
                    "task_progress": _text(driver, "#task-progress-text"),
                    "task_model_usage": _text(driver, "#task-model-usage"),
                    "source_list": _file_list_names(driver, "#source-list"),
                    "template_list": _file_list_names(driver, "#template-list"),
                    "result_cards": _collect_result_cards(driver),
                    "logs": [element.text.strip() for element in driver.find_elements(By.CSS_SELECTOR, "#log-container .log-line") if element.text.strip()],
                },
                "browser_downloads": browser_downloads,
                "finished_at": time.time(),
            }
        )
    finally:
        driver.quit()

    payload_text = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.output_json:
        Path(args.output_json).write_text(payload_text, encoding="utf-8")
    print(payload_text)
    return 0 if summary.get("status") == "completed" else 1


if __name__ == "__main__":
    sys.exit(run())