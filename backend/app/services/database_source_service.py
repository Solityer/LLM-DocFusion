"""Database source connector. SQLite is implemented; SQL engines are pluggable."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from ..core.exceptions import SourceConnectError
from ..schemas.models import DocumentBundle, FileRole, NormalizedTable, TextBlock
from ..schemas.source_models import SourceSpec
from ..utils.text_utils import clean_cell_value


def fetch_database_source(spec: SourceSpec) -> DocumentBundle:
    db_type = (spec.database_type or "sqlite").lower()
    if db_type != "sqlite":
        raise SourceConnectError(f"Database type '{db_type}' is reserved but not implemented; use sqlite")
    db_path = spec.database_path or spec.connection_string
    if not db_path:
        raise SourceConnectError("SQLite source requires database_path or connection_string")
    if not spec.query:
        raise SourceConnectError("Database source requires query")
    if not Path(db_path).exists():
        raise SourceConnectError(f"SQLite database not found: {db_path}")

    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        cursor = connection.execute(spec.query)
        rows = cursor.fetchall()
        headers = [description[0] for description in cursor.description or []]
    except Exception as exc:
        raise SourceConnectError(f"SQLite query failed: {exc}") from exc
    finally:
        try:
            connection.close()
        except Exception:
            pass

    table_rows = [[clean_cell_value(row[header]) for header in headers] for row in rows]
    name = spec.name or Path(db_path).stem
    bundle = DocumentBundle(
        document_id=_safe_document_id(name),
        source_file=db_path,
        file_type="database",
        role=FileRole.SOURCE,
        metadata={
            "source_type": "database",
            "database_type": "sqlite",
            "query": spec.query,
            "priority": spec.priority,
            "title": name,
        },
    )
    bundle.tables.append(NormalizedTable(
        table_index=0,
        sheet_name=name,
        headers=headers,
        rows=table_rows,
        row_count=len(table_rows),
        col_count=len(headers),
    ))
    lines = [" | ".join(headers)]
    lines.extend(" | ".join(row) for row in table_rows[:200])
    bundle.raw_text = "\n".join(lines)
    bundle.text_blocks.append(TextBlock(content=bundle.raw_text[:20000], block_index=0))
    return bundle


def _safe_document_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)[:80] or "database_source"
