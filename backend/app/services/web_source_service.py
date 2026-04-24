"""Web page source connector with lightweight HTML text/table extraction."""
from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path

import httpx

from ..core.exceptions import SourceConnectError
from ..schemas.models import DocumentBundle, FileRole, NormalizedTable, TextBlock
from ..schemas.source_models import SourceSpec
from ..utils.text_utils import clean_cell_value


class _HTMLContentParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text_parts: list[str] = []
        self.tables: list[list[list[str]]] = []
        self._tag_stack: list[str] = []
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        self._tag_stack.append(tag)
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
        if tag == "table":
            self._current_table = []
        elif tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self._skip_depth:
            self._skip_depth -= 1
        if tag in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            self._current_row.append(clean_cell_value(" ".join(self._current_cell)))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None and self._current_table is not None:
            if any(cell for cell in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._current_table is not None:
            if self._current_table:
                self.tables.append(self._current_table)
            self._current_table = None
        if tag in {"p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "div", "br"}:
            self.text_parts.append("\n")
        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data):
        if self._skip_depth:
            return
        text = clean_cell_value(data)
        if not text:
            return
        if self._current_cell is not None:
            self._current_cell.append(text)
        elif self._current_table is None:
            self.text_parts.append(text)


def fetch_web_source(spec: SourceSpec) -> DocumentBundle:
    if not spec.url:
        raise SourceConnectError("Web source requires url")
    try:
        with httpx.Client(timeout=max(float(spec.timeout or 20), 1.0), follow_redirects=True) as client:
            response = client.get(spec.url, headers=spec.headers or {})
            response.raise_for_status()
    except Exception as exc:
        raise SourceConnectError(f"Web source failed: {exc}") from exc

    parser = _HTMLContentParser()
    parser.feed(response.text or "")
    title = spec.name or Path(spec.url.split("?")[0]).name or spec.url
    bundle = DocumentBundle(
        document_id=_safe_document_id(title),
        source_file=spec.url,
        file_type="web",
        role=FileRole.SOURCE,
        metadata={
            "source_type": "web_page",
            "url": spec.url,
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type", ""),
            "priority": spec.priority,
            "title": title,
        },
    )

    text = "\n".join(part.strip() for part in "".join(parser.text_parts).splitlines() if part.strip())
    bundle.raw_text = text
    blocks = [part.strip() for part in text.splitlines() if part.strip()]
    bundle.text_blocks = [
        TextBlock(content=block[:12000], heading_level=0, block_index=index)
        for index, block in enumerate(blocks[:1000])
    ]
    for table_index, rows in enumerate(parser.tables):
        headers = [clean_cell_value(cell) for cell in rows[0]] if rows else []
        data_rows = [[clean_cell_value(cell) for cell in row] for row in rows[1:]]
        if not headers and data_rows:
            headers = [f"col{index + 1}" for index in range(max(len(row) for row in data_rows))]
        bundle.tables.append(NormalizedTable(
            table_index=table_index,
            sheet_name=title,
            headers=headers,
            rows=data_rows,
            row_count=len(data_rows),
            col_count=len(headers),
        ))
    return bundle


def _safe_document_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)[:80] or "web_source"
