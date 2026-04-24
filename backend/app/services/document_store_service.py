"""SQLite document data asset store service.

Supports document import, search, export, checkout, and deletion.
Uses only Python standard library sqlite3 - no heavy dependencies.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from ..core.config import PROJECT_ROOT
from ..core.logging import logger
from ..schemas.models import DocumentBundle, FileRole
from ..schemas.store_models import (
    StoreDocumentMeta,
    StoreEntityItem,
    StoreFieldItem,
    StoreQualityItem,
    StoreSearchResult,
    StoreStats,
    StoreTaskStatus,
)

# ── Database location ──────────────────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "docfusion_store.sqlite"

_DB_LOCK = threading.Lock()

# ── Store task registry (lightweight, in-memory) ──────────────────────────────
_STORE_TASKS: dict[str, StoreTaskStatus] = {}
_STORE_TASK_LOCK = threading.Lock()


# ── Schema DDL ─────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS documents (
    document_id   TEXT PRIMARY KEY,
    source_file   TEXT NOT NULL,
    source_name   TEXT,
    source_type   TEXT DEFAULT 'file',
    file_type     TEXT,
    title         TEXT,
    raw_text_hash TEXT,
    created_at    TEXT,
    metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS text_blocks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id  TEXT NOT NULL REFERENCES documents(document_id),
    block_index  INTEGER,
    heading_level INTEGER DEFAULT 0,
    content      TEXT,
    content_hash TEXT
);

CREATE TABLE IF NOT EXISTS tables (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id  TEXT NOT NULL REFERENCES documents(document_id),
    table_index  INTEGER,
    sheet_name   TEXT,
    headers_json TEXT,
    row_count    INTEGER DEFAULT 0,
    col_count    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS table_rows (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id         TEXT NOT NULL REFERENCES documents(document_id),
    table_id            INTEGER REFERENCES tables(id),
    row_index           INTEGER,
    row_json            TEXT,
    canonical_row_json  TEXT
);

CREATE TABLE IF NOT EXISTS extracted_entities (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id       TEXT NOT NULL REFERENCES documents(document_id),
    entity_text       TEXT,
    entity_type       TEXT,
    normalized_entity TEXT,
    source_location   TEXT,
    confidence        REAL,
    evidence_snippet  TEXT
);

CREATE TABLE IF NOT EXISTS extracted_fields (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id      TEXT NOT NULL REFERENCES documents(document_id),
    field_name       TEXT,
    canonical_field  TEXT,
    value            TEXT,
    normalized_value TEXT,
    field_type       TEXT,
    unit             TEXT,
    source_location  TEXT,
    confidence       REAL,
    evidence_snippet TEXT
);

CREATE TABLE IF NOT EXISTS quality_issues (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id      TEXT NOT NULL REFERENCES documents(document_id),
    issue_type       TEXT,
    severity         TEXT,
    field_name       TEXT,
    raw_value        TEXT,
    normalized_value TEXT,
    source           TEXT,
    location         TEXT,
    reason           TEXT,
    suggestion       TEXT,
    affects_fill     INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS task_metrics (
    task_id             TEXT PRIMARY KEY,
    template_count      INTEGER DEFAULT 0,
    source_count        INTEGER DEFAULT 0,
    fill_rate           REAL DEFAULT 0,
    field_match_rate    REAL DEFAULT 0,
    quality_issue_count INTEGER DEFAULT 0,
    response_time       REAL DEFAULT 0,
    created_at          TEXT,
    report_json         TEXT
);

CREATE INDEX IF NOT EXISTS idx_text_blocks_doc   ON text_blocks(document_id);
CREATE INDEX IF NOT EXISTS idx_tables_doc        ON tables(document_id);
CREATE INDEX IF NOT EXISTS idx_table_rows_doc    ON table_rows(document_id);
CREATE INDEX IF NOT EXISTS idx_entities_doc      ON extracted_entities(document_id);
CREATE INDEX IF NOT EXISTS idx_entities_type     ON extracted_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_fields_doc        ON extracted_fields(document_id);
CREATE INDEX IF NOT EXISTS idx_quality_doc       ON quality_issues(document_id);
"""


def _get_connection() -> sqlite3.Connection:
    """Return a thread-local SQLite connection."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Initialize (or upgrade) the SQLite database schema."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with _DB_LOCK:
        conn = _get_connection()
        try:
            conn.executescript(_DDL)
            conn.commit()
            logger.info(f"DocFusion store initialized at {DB_PATH}")
        finally:
            conn.close()


# ── Low-level helpers ──────────────────────────────────────────────────────────

def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:32]


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


# ── Document import ────────────────────────────────────────────────────────────

def document_exists(conn: sqlite3.Connection, document_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM documents WHERE document_id=?", (document_id,)).fetchone()
    return row is not None


def hash_exists(conn: sqlite3.Connection, raw_text_hash: str) -> str | None:
    """Return existing document_id if same hash already in store, else None."""
    row = conn.execute(
        "SELECT document_id FROM documents WHERE raw_text_hash=?", (raw_text_hash,)
    ).fetchone()
    return row["document_id"] if row else None


def save_document_bundle(
    bundle: DocumentBundle,
    overwrite: bool = False,
    extract_entities: bool = True,
    quality_issues: list[dict] | None = None,
) -> dict[str, Any]:
    """Persist a DocumentBundle to the SQLite store.

    Returns a summary dict with counts and the document_id used.
    """
    from .normalization_service import normalize_value
    from .schema_registry_service import canonical_field_name

    raw_hash = _hash_text(bundle.raw_text or "\n".join(b.content for b in bundle.text_blocks))
    doc_id = bundle.document_id or str(uuid.uuid4())[:12]
    title = Path(bundle.source_file).stem if bundle.source_file else bundle.source_name

    with _DB_LOCK:
        conn = _get_connection()
        try:
            existing_id = hash_exists(conn, raw_hash)
            if existing_id and not overwrite:
                return {
                    "document_id": existing_id,
                    "status": "skipped",
                    "reason": "duplicate_hash",
                    "text_blocks": 0,
                    "tables": 0,
                    "entities": 0,
                    "fields": 0,
                }

            if overwrite and existing_id:
                _delete_document(conn, existing_id)
                doc_id = existing_id

            # Insert document record
            conn.execute(
                """INSERT OR REPLACE INTO documents
                   (document_id, source_file, source_name, source_type, file_type,
                    title, raw_text_hash, created_at, metadata_json)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    doc_id,
                    bundle.source_file,
                    bundle.source_name or title,
                    bundle.source_type,
                    bundle.file_type,
                    title,
                    raw_hash,
                    _ts(),
                    json.dumps(bundle.metadata, ensure_ascii=False),
                ),
            )

            # Text blocks
            tb_count = 0
            for block in bundle.text_blocks:
                if not block.content or not block.content.strip():
                    continue
                conn.execute(
                    """INSERT INTO text_blocks
                       (document_id, block_index, heading_level, content, content_hash)
                       VALUES (?,?,?,?,?)""",
                    (
                        doc_id,
                        block.block_index,
                        block.heading_level,
                        block.content,
                        _hash_text(block.content),
                    ),
                )
                tb_count += 1

            # Tables + rows
            table_count = 0
            field_count = 0
            for table in bundle.tables:
                cur = conn.execute(
                    """INSERT INTO tables
                       (document_id, table_index, sheet_name, headers_json, row_count, col_count)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        doc_id,
                        table.table_index,
                        table.sheet_name,
                        json.dumps(table.headers, ensure_ascii=False),
                        table.row_count,
                        table.col_count,
                    ),
                )
                table_id = cur.lastrowid
                table_count += 1

                # Rows
                for row_idx, row in enumerate(table.rows[:2000]):  # cap at 2000 rows
                    row_dict = {
                        header: row[col_idx] if col_idx < len(row) else ""
                        for col_idx, header in enumerate(table.headers)
                    }
                    canonical_row = {
                        canonical_field_name(k) or k: normalize_value(v, k).get("standard_value", v)
                        for k, v in row_dict.items()
                    }
                    conn.execute(
                        """INSERT INTO table_rows
                           (document_id, table_id, row_index, row_json, canonical_row_json)
                           VALUES (?,?,?,?,?)""",
                        (
                            doc_id,
                            table_id,
                            row_idx,
                            json.dumps(row_dict, ensure_ascii=False),
                            json.dumps(canonical_row, ensure_ascii=False, default=str),
                        ),
                    )

                # Extracted fields from table headers+rows
                for col_idx, header in enumerate(table.headers):
                    if not header:
                        continue
                    canonical = canonical_field_name(header)
                    nv = normalize_value(
                        table.rows[0][col_idx] if table.rows and col_idx < len(table.rows[0]) else "",
                        header,
                    )
                    conn.execute(
                        """INSERT INTO extracted_fields
                           (document_id, field_name, canonical_field, value, normalized_value,
                            field_type, unit, source_location, confidence, evidence_snippet)
                           VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (
                            doc_id,
                            header,
                            canonical,
                            table.rows[0][col_idx] if table.rows and col_idx < len(table.rows[0]) else "",
                            str(nv.get("standard_value", "")),
                            nv.get("field_type", ""),
                            nv.get("unit", ""),
                            f"table{table.table_index}.col{col_idx}",
                            0.9,
                            "",
                        ),
                    )
                    field_count += 1

            # Entities from text (simple keyword extraction)
            entity_count = 0
            if extract_entities:
                entity_count = _extract_and_store_entities(conn, doc_id, bundle)

            # Quality issues
            q_count = 0
            if quality_issues:
                for issue in quality_issues:
                    conn.execute(
                        """INSERT INTO quality_issues
                           (document_id, issue_type, severity, field_name, raw_value,
                            normalized_value, source, location, reason, suggestion, affects_fill)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            doc_id,
                            issue.get("issue_type", ""),
                            issue.get("severity", "warning"),
                            issue.get("field_name", ""),
                            str(issue.get("raw_value", "")),
                            str(issue.get("normalized_value", "")),
                            issue.get("source", ""),
                            issue.get("location", ""),
                            issue.get("reason", ""),
                            issue.get("suggestion", ""),
                            1 if issue.get("affects_fill") else 0,
                        ),
                    )
                    q_count += 1

            conn.commit()
            return {
                "document_id": doc_id,
                "status": "imported",
                "text_blocks": tb_count,
                "tables": table_count,
                "entities": entity_count,
                "fields": field_count,
                "quality_issues": q_count,
            }
        except Exception as exc:
            conn.rollback()
            logger.error(f"Store import failed for {bundle.source_file}: {exc}", exc_info=True)
            raise
        finally:
            conn.close()


def _delete_document(conn: sqlite3.Connection, doc_id: str) -> None:
    for tbl in ["quality_issues", "extracted_fields", "extracted_entities",
                "table_rows", "tables", "text_blocks", "documents"]:
        conn.execute(f"DELETE FROM {tbl} WHERE document_id=?", (doc_id,))


def _extract_and_store_entities(
    conn: sqlite3.Connection,
    doc_id: str,
    bundle: DocumentBundle,
) -> int:
    """Simple rule-based entity extraction from text blocks."""
    import re

    CITY_SUFFIXES = ("市", "县", "区", "省", "州", "镇")
    ORG_SUFFIXES = ("公司", "集团", "企业", "机构", "局", "院", "所")
    NUMERIC_PATTERN = re.compile(r"([\u4e00-\u9fa5]{1,6})\s*([\d,，.]+)\s*(亿|万亿|万|千|百|%|个|人)")

    count = 0
    text = bundle.raw_text or "\n".join(b.content for b in bundle.text_blocks)

    # City/region entities
    city_pattern = re.compile(r"([\u4e00-\u9fa5]{2,6}(?:" + "|".join(CITY_SUFFIXES) + r"))")
    for match in city_pattern.finditer(text[:20000]):
        entity = match.group(1)
        if 2 <= len(entity) <= 8:
            conn.execute(
                """INSERT INTO extracted_entities
                   (document_id, entity_text, entity_type, normalized_entity,
                    source_location, confidence, evidence_snippet)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    doc_id,
                    entity,
                    "location",
                    entity,
                    f"text_pos{match.start()}",
                    0.75,
                    text[max(0, match.start()-20):match.end()+20],
                ),
            )
            count += 1
            if count > 500:
                break

    # Organization entities
    org_pattern = re.compile(r"([\u4e00-\u9fa5]{2,10}(?:" + "|".join(ORG_SUFFIXES) + r"))")
    for match in org_pattern.finditer(text[:20000]):
        entity = match.group(1)
        if 3 <= len(entity) <= 15:
            conn.execute(
                """INSERT INTO extracted_entities
                   (document_id, entity_text, entity_type, normalized_entity,
                    source_location, confidence, evidence_snippet)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    doc_id,
                    entity,
                    "organization",
                    entity,
                    f"text_pos{match.start()}",
                    0.7,
                    text[max(0, match.start()-20):match.end()+20],
                ),
            )
            count += 1
            if count > 800:
                break

    return count


# ── Query functions ────────────────────────────────────────────────────────────

def get_documents(limit: int = 100, offset: int = 0) -> list[StoreDocumentMeta]:
    with _DB_LOCK:
        conn = _get_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM documents ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            result = []
            for row in rows:
                doc_id = row["document_id"]
                tb_count = conn.execute(
                    "SELECT COUNT(*) FROM text_blocks WHERE document_id=?", (doc_id,)
                ).fetchone()[0]
                t_count = conn.execute(
                    "SELECT COUNT(*) FROM tables WHERE document_id=?", (doc_id,)
                ).fetchone()[0]
                e_count = conn.execute(
                    "SELECT COUNT(*) FROM extracted_entities WHERE document_id=?", (doc_id,)
                ).fetchone()[0]
                f_count = conn.execute(
                    "SELECT COUNT(*) FROM extracted_fields WHERE document_id=?", (doc_id,)
                ).fetchone()[0]
                q_count = conn.execute(
                    "SELECT COUNT(*) FROM quality_issues WHERE document_id=?", (doc_id,)
                ).fetchone()[0]
                meta = json.loads(row["metadata_json"] or "{}")
                result.append(StoreDocumentMeta(
                    document_id=doc_id,
                    source_file=row["source_file"] or "",
                    source_name=row["source_name"] or "",
                    source_type=row["source_type"] or "file",
                    file_type=row["file_type"] or "",
                    title=row["title"] or "",
                    raw_text_hash=row["raw_text_hash"] or "",
                    created_at=row["created_at"] or "",
                    metadata_json=meta,
                    text_block_count=tb_count,
                    table_count=t_count,
                    entity_count=e_count,
                    field_count=f_count,
                    quality_issue_count=q_count,
                ))
            return result
        finally:
            conn.close()


def get_document_detail(document_id: str) -> dict[str, Any] | None:
    with _DB_LOCK:
        conn = _get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM documents WHERE document_id=?", (document_id,)
            ).fetchone()
            if not row:
                return None
            tb_count = conn.execute(
                "SELECT COUNT(*) FROM text_blocks WHERE document_id=?", (document_id,)
            ).fetchone()[0]
            t_count = conn.execute(
                "SELECT COUNT(*) FROM tables WHERE document_id=?", (document_id,)
            ).fetchone()[0]
            e_count = conn.execute(
                "SELECT COUNT(*) FROM extracted_entities WHERE document_id=?", (document_id,)
            ).fetchone()[0]
            f_count = conn.execute(
                "SELECT COUNT(*) FROM extracted_fields WHERE document_id=?", (document_id,)
            ).fetchone()[0]
            q_count = conn.execute(
                "SELECT COUNT(*) FROM quality_issues WHERE document_id=?", (document_id,)
            ).fetchone()[0]

            tables = conn.execute(
                "SELECT * FROM tables WHERE document_id=? LIMIT 20", (document_id,)
            ).fetchall()

            text_blocks = conn.execute(
                "SELECT block_index, heading_level, content FROM text_blocks WHERE document_id=? "
                "ORDER BY block_index LIMIT 5",
                (document_id,),
            ).fetchall()

            entities = conn.execute(
                "SELECT entity_text, entity_type, normalized_entity, confidence, evidence_snippet "
                "FROM extracted_entities WHERE document_id=? LIMIT 20",
                (document_id,),
            ).fetchall()

            fields = conn.execute(
                "SELECT field_name, canonical_field, value, normalized_value, field_type, confidence "
                "FROM extracted_fields WHERE document_id=? LIMIT 20",
                (document_id,),
            ).fetchall()

            quality_issues = conn.execute(
                "SELECT issue_type, severity, field_name, raw_value, reason, suggestion "
                "FROM quality_issues WHERE document_id=? ORDER BY severity DESC LIMIT 20",
                (document_id,),
            ).fetchall()

            return {
                "document_id": document_id,
                "source_file": row["source_file"],
                "source_name": row["source_name"],
                "source_type": row["source_type"],
                "file_type": row["file_type"],
                "title": row["title"],
                "raw_text_hash": row["raw_text_hash"],
                "created_at": row["created_at"],
                "metadata": json.loads(row["metadata_json"] or "{}"),
                "text_block_count": tb_count,
                "table_count": t_count,
                "entity_count": e_count,
                "field_count": f_count,
                "quality_issue_count": q_count,
                "tables": [
                    {
                        "table_index": t["table_index"],
                        "sheet_name": t["sheet_name"],
                        "headers": json.loads(t["headers_json"] or "[]"),
                        "row_count": t["row_count"],
                    }
                    for t in tables
                ],
                "text_blocks": [
                    {
                        "block_index": b["block_index"],
                        "heading_level": b["heading_level"],
                        "content": b["content"] or "",
                    }
                    for b in text_blocks
                ],
                "entities": [
                    {
                        "entity_text": e["entity_text"] or "",
                        "entity_type": e["entity_type"] or "",
                        "normalized_entity": e["normalized_entity"] or "",
                        "confidence": e["confidence"],
                        "evidence_snippet": e["evidence_snippet"] or "",
                    }
                    for e in entities
                ],
                "fields": [
                    {
                        "field_name": f["field_name"] or "",
                        "canonical_field": f["canonical_field"] or "",
                        "value": f["value"] or "",
                        "normalized_value": f["normalized_value"] or "",
                        "field_type": f["field_type"] or "",
                        "confidence": f["confidence"],
                    }
                    for f in fields
                ],
                "quality_issues": [
                    {
                        "issue_type": q["issue_type"] or "",
                        "severity": q["severity"] or "",
                        "field_name": q["field_name"] or "",
                        "raw_value": q["raw_value"] or "",
                        "reason": q["reason"] or "",
                        "suggestion": q["suggestion"] or "",
                    }
                    for q in quality_issues
                ],
            }
        finally:
            conn.close()


def search_store(query: str, limit: int = 50) -> list[StoreSearchResult]:
    """Full-text search across documents, text_blocks, entities, fields."""
    if not query or not query.strip():
        return []
    q = f"%{query.strip()}%"
    results: list[StoreSearchResult] = []

    with _DB_LOCK:
        conn = _get_connection()
        try:
            # Documents (title/source_name)
            for row in conn.execute(
                "SELECT document_id, source_file, source_name, file_type, title "
                "FROM documents WHERE title LIKE ? OR source_name LIKE ? LIMIT ?",
                (q, q, limit // 4),
            ).fetchall():
                results.append(StoreSearchResult(
                    result_type="document",
                    document_id=row["document_id"],
                    source_file=row["source_file"] or "",
                    source_name=row["source_name"] or "",
                    file_type=row["file_type"] or "",
                    snippet=row["title"] or "",
                ))

            # Text blocks
            for row in conn.execute(
                "SELECT tb.document_id, tb.content, d.source_file, d.source_name, d.file_type "
                "FROM text_blocks tb JOIN documents d ON tb.document_id=d.document_id "
                "WHERE tb.content LIKE ? LIMIT ?",
                (q, limit // 4),
            ).fetchall():
                content = row["content"] or ""
                idx = content.lower().find(query.lower())
                snippet = content[max(0, idx - 30):idx + 80] if idx >= 0 else content[:100]
                results.append(StoreSearchResult(
                    result_type="text_block",
                    document_id=row["document_id"],
                    source_file=row["source_file"] or "",
                    source_name=row["source_name"] or "",
                    file_type=row["file_type"] or "",
                    snippet=snippet,
                ))

            # Entities
            for row in conn.execute(
                "SELECT e.id, e.document_id, e.entity_text, e.entity_type, e.confidence, "
                "e.evidence_snippet, d.source_file, d.source_name, d.file_type "
                "FROM extracted_entities e JOIN documents d ON e.document_id=d.document_id "
                "WHERE e.entity_text LIKE ? LIMIT ?",
                (q, limit // 4),
            ).fetchall():
                results.append(StoreSearchResult(
                    result_type="entity",
                    document_id=row["document_id"],
                    source_file=row["source_file"] or "",
                    source_name=row["source_name"] or "",
                    file_type=row["file_type"] or "",
                    snippet=row["evidence_snippet"] or "",
                    entity_type=row["entity_type"] or "",
                    value=row["entity_text"] or "",
                    confidence=row["confidence"],
                ))

            # Fields
            for row in conn.execute(
                "SELECT f.id, f.document_id, f.field_name, f.value, f.normalized_value, "
                "f.confidence, f.evidence_snippet, d.source_file, d.source_name, d.file_type "
                "FROM extracted_fields f JOIN documents d ON f.document_id=d.document_id "
                "WHERE f.field_name LIKE ? OR f.value LIKE ? LIMIT ?",
                (q, q, limit // 4),
            ).fetchall():
                results.append(StoreSearchResult(
                    result_type="field",
                    document_id=row["document_id"],
                    source_file=row["source_file"] or "",
                    source_name=row["source_name"] or "",
                    file_type=row["file_type"] or "",
                    snippet=row["evidence_snippet"] or f"{row['field_name']}={row['value']}",
                    field_name=row["field_name"] or "",
                    value=row["value"] or "",
                    confidence=row["confidence"],
                ))
        finally:
            conn.close()

    return results[:limit]


def get_entities(
    entity_type: str | None = None,
    document_id: str | None = None,
    limit: int = 200,
) -> list[StoreEntityItem]:
    with _DB_LOCK:
        conn = _get_connection()
        try:
            where = []
            params: list[Any] = []
            if entity_type:
                where.append("entity_type=?")
                params.append(entity_type)
            if document_id:
                where.append("document_id=?")
                params.append(document_id)
            clause = ("WHERE " + " AND ".join(where)) if where else ""
            rows = conn.execute(
                f"SELECT * FROM extracted_entities {clause} LIMIT ?",
                params + [limit],
            ).fetchall()
            return [
                StoreEntityItem(
                    id=row["id"],
                    document_id=row["document_id"],
                    entity_text=row["entity_text"] or "",
                    entity_type=row["entity_type"] or "",
                    normalized_entity=row["normalized_entity"] or "",
                    source_location=row["source_location"] or "",
                    confidence=row["confidence"],
                    evidence_snippet=row["evidence_snippet"] or "",
                )
                for row in rows
            ]
        finally:
            conn.close()


def get_quality_issues(
    document_id: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    with _DB_LOCK:
        conn = _get_connection()
        try:
            where = "WHERE document_id=?" if document_id else ""
            params: list[Any] = [document_id] if document_id else []
            rows = conn.execute(
                f"SELECT * FROM quality_issues {where} ORDER BY severity DESC LIMIT ?",
                params + [limit],
            ).fetchall()
            issues = [
                StoreQualityItem(
                    id=row["id"],
                    document_id=row["document_id"],
                    issue_type=row["issue_type"] or "",
                    severity=row["severity"] or "",
                    field_name=row["field_name"] or "",
                    raw_value=row["raw_value"] or "",
                    normalized_value=row["normalized_value"] or "",
                    source=row["source"] or "",
                    location=row["location"] or "",
                    reason=row["reason"] or "",
                    suggestion=row["suggestion"] or "",
                    affects_fill=bool(row["affects_fill"]),
                )
                for row in rows
            ]
            # Distribution
            type_rows = conn.execute(
                f"SELECT issue_type, COUNT(*) AS cnt FROM quality_issues {where} GROUP BY issue_type",
                params,
            ).fetchall()
            sev_rows = conn.execute(
                f"SELECT severity, COUNT(*) AS cnt FROM quality_issues {where} GROUP BY severity",
                params,
            ).fetchall()
            return {
                "issues": [i.model_dump() for i in issues],
                "type_distribution": {r["issue_type"]: r["cnt"] for r in type_rows},
                "severity_distribution": {r["severity"]: r["cnt"] for r in sev_rows},
                "total": len(rows),
            }
        finally:
            conn.close()


# ── Document export / checkout / delete ──────────────────────────────────────

def export_document_package(document_id: str, output_dir: Path) -> dict[str, Any]:
    """Export all data for a document to a JSON file.

    Creates ``outputs/document_export_{document_id}_{timestamp}.json``.
    Does NOT remove the document from the database.
    """
    with _DB_LOCK:
        conn = _get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM documents WHERE document_id=?", (document_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Document not found: {document_id}")

            doc_data: dict[str, Any] = dict(row)
            doc_data["metadata"] = json.loads(doc_data.pop("metadata_json") or "{}")

            text_blocks = [dict(r) for r in conn.execute(
                "SELECT * FROM text_blocks WHERE document_id=?", (document_id,)
            ).fetchall()]

            tables = [dict(r) for r in conn.execute(
                "SELECT * FROM tables WHERE document_id=?", (document_id,)
            ).fetchall()]
            for t in tables:
                t["headers"] = json.loads(t.pop("headers_json") or "[]")

            table_rows = [dict(r) for r in conn.execute(
                "SELECT * FROM table_rows WHERE document_id=?", (document_id,)
            ).fetchall()]

            entities = [dict(r) for r in conn.execute(
                "SELECT * FROM extracted_entities WHERE document_id=?", (document_id,)
            ).fetchall()]

            fields = [dict(r) for r in conn.execute(
                "SELECT * FROM extracted_fields WHERE document_id=?", (document_id,)
            ).fetchall()]

            quality_issues = [dict(r) for r in conn.execute(
                "SELECT * FROM quality_issues WHERE document_id=?", (document_id,)
            ).fetchall()]
        finally:
            conn.close()

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    filename = f"document_export_{document_id}_{ts}.json"
    output_file = output_dir / filename

    package: dict[str, Any] = {
        "export_version": "1.0",
        "exported_at": _ts(),
        "document": doc_data,
        "text_blocks": text_blocks,
        "tables": tables,
        "table_rows": table_rows,
        "entities": entities,
        "fields": fields,
        "quality_issues": quality_issues,
    }

    with open(output_file, "w", encoding="utf-8") as fh:
        json.dump(package, fh, ensure_ascii=False, indent=2, default=str)

    logger.info(f"Exported document {document_id} to {output_file}")
    return {
        "status": "ok",
        "document_id": document_id,
        "output_file": str(output_file),
        "download_url": f"/api/download/{filename}",
        "removed": False,
    }


def delete_document(document_id: str) -> dict[str, Any]:
    """Delete a document and all its associated data from the SQLite store.

    Does NOT delete the original upload file from disk.
    Raises ValueError if the document does not exist.
    """
    with _DB_LOCK:
        conn = _get_connection()
        try:
            row = conn.execute(
                "SELECT 1 FROM documents WHERE document_id=?", (document_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Document not found: {document_id}")
            _delete_document(conn, document_id)
            conn.commit()
            logger.info(f"Deleted document {document_id} from store")
            return {"status": "ok", "document_id": document_id, "deleted": True}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def checkout_document(
    document_id: str,
    output_dir: Path,
    remove_after_export: bool = True,
) -> dict[str, Any]:
    """Export document to a JSON package, then optionally remove it from the store.

    Export always happens first; deletion only proceeds if export succeeds.
    """
    result = export_document_package(document_id, output_dir)
    if remove_after_export:
        delete_document(document_id)
        result["removed"] = True
    return result


def get_stats() -> StoreStats:
    with _DB_LOCK:
        conn = _get_connection()
        try:
            doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            tb_count = conn.execute("SELECT COUNT(*) FROM text_blocks").fetchone()[0]
            t_count = conn.execute("SELECT COUNT(*) FROM tables").fetchone()[0]
            e_count = conn.execute("SELECT COUNT(*) FROM extracted_entities").fetchone()[0]
            f_count = conn.execute("SELECT COUNT(*) FROM extracted_fields").fetchone()[0]
            q_count = conn.execute("SELECT COUNT(*) FROM quality_issues").fetchone()[0]

            src_type_rows = conn.execute(
                "SELECT source_type, COUNT(*) AS cnt FROM documents GROUP BY source_type"
            ).fetchall()
            file_type_rows = conn.execute(
                "SELECT file_type, COUNT(*) AS cnt FROM documents GROUP BY file_type"
            ).fetchall()
            field_type_rows = conn.execute(
                "SELECT field_type, COUNT(*) AS cnt FROM extracted_fields GROUP BY field_type"
            ).fetchall()
            q_type_rows = conn.execute(
                "SELECT issue_type, COUNT(*) AS cnt FROM quality_issues GROUP BY issue_type"
            ).fetchall()
            q_sev_rows = conn.execute(
                "SELECT severity, COUNT(*) AS cnt FROM quality_issues GROUP BY severity"
            ).fetchall()

            return StoreStats(
                document_count=doc_count,
                text_block_count=tb_count,
                table_count=t_count,
                entity_count=e_count,
                field_count=f_count,
                quality_issue_count=q_count,
                source_type_distribution={r["source_type"]: r["cnt"] for r in src_type_rows},
                file_type_distribution={r["file_type"]: r["cnt"] for r in file_type_rows},
                field_type_distribution={r["field_type"]: r["cnt"] for r in field_type_rows},
                quality_issue_type_distribution={r["issue_type"]: r["cnt"] for r in q_type_rows},
                quality_severity_distribution={r["severity"]: r["cnt"] for r in q_sev_rows},
            )
        finally:
            conn.close()


def save_task_metrics(
    task_id: str,
    template_count: int,
    source_count: int,
    fill_rate: float,
    field_match_rate: float,
    quality_issue_count: int,
    response_time: float,
    report: dict[str, Any],
) -> None:
    with _DB_LOCK:
        conn = _get_connection()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO task_metrics
                   (task_id, template_count, source_count, fill_rate, field_match_rate,
                    quality_issue_count, response_time, created_at, report_json)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    task_id,
                    template_count,
                    source_count,
                    fill_rate,
                    field_match_rate,
                    quality_issue_count,
                    response_time,
                    _ts(),
                    json.dumps(report, ensure_ascii=False),
                ),
            )
            conn.commit()
        finally:
            conn.close()


# ── Store task management ──────────────────────────────────────────────────────

def create_store_task() -> StoreTaskStatus:
    task_id = str(uuid.uuid4())[:8]
    status = StoreTaskStatus(
        task_id=task_id,
        status="queued",
        current_stage="queued",
        progress=0.0,
        message="入库任务已创建，等待执行",
    )
    with _STORE_TASK_LOCK:
        _STORE_TASKS[task_id] = status
    return status


def get_store_task(task_id: str) -> StoreTaskStatus | None:
    with _STORE_TASK_LOCK:
        return _STORE_TASKS.get(task_id)


def _update_store_task(task_id: str, **kwargs) -> None:
    with _STORE_TASK_LOCK:
        task = _STORE_TASKS.get(task_id)
        if task:
            for key, value in kwargs.items():
                setattr(task, key, value)


def run_import_task(
    task_id: str,
    source_files: list[str],
    use_llm: bool = True,
    extract_entities: bool = True,
    overwrite: bool = False,
) -> None:
    """Background worker for importing documents into the store."""
    from .document_service import read_document
    from .quality_service import analyze_documents, quality_report_to_dict

    total = len(source_files)
    imported = 0
    skipped = 0
    errors = []

    _update_store_task(task_id, status="processing", current_stage="reading", progress=0.05,
                       message=f"开始入库 {total} 个文档...")

    bundles = []
    for idx, file_path in enumerate(source_files):
        try:
            _update_store_task(
                task_id,
                progress=0.1 + 0.4 * idx / total,
                message=f"读取文档 {idx+1}/{total}: {Path(file_path).name}",
            )
            bundle = read_document(file_path, FileRole.SOURCE)
            bundles.append(bundle)
        except Exception as exc:
            logger.warning(f"Store import - failed to read {file_path}: {exc}")
            errors.append(f"读取失败: {Path(file_path).name}: {str(exc)[:100]}")

    _update_store_task(task_id, current_stage="quality", progress=0.55,
                       message="执行质量分析...")

    quality_issues_per_doc: dict[str, list[dict]] = {}
    if bundles:
        try:
            quality_report = analyze_documents(bundles)
            for issue in quality_report.issues:
                src = issue.source or ""
                if src not in quality_issues_per_doc:
                    quality_issues_per_doc[src] = []
                quality_issues_per_doc[src].append(issue.model_dump())
        except Exception as exc:
            logger.warning(f"Store quality analysis failed: {exc}")

    _update_store_task(task_id, current_stage="storing", progress=0.65,
                       message="写入 SQLite 数据库...")

    for idx, bundle in enumerate(bundles):
        try:
            _update_store_task(
                task_id,
                progress=0.65 + 0.3 * idx / max(len(bundles), 1),
                message=f"入库文档 {idx+1}/{len(bundles)}: {Path(bundle.source_file).name}",
            )
            q_issues = quality_issues_per_doc.get(bundle.source_file, [])
            result = save_document_bundle(
                bundle,
                overwrite=overwrite,
                extract_entities=extract_entities,
                quality_issues=q_issues,
            )
            if result["status"] == "skipped":
                skipped += 1
                logger.info(f"Store: skipped duplicate {bundle.source_file}")
            else:
                imported += 1
                logger.info(
                    f"Store: imported {bundle.source_file} -> "
                    f"{result['text_blocks']} blocks, {result['tables']} tables, "
                    f"{result['entities']} entities"
                )
        except Exception as exc:
            logger.error(f"Store failed for {bundle.source_file}: {exc}", exc_info=True)
            errors.append(f"入库失败: {Path(bundle.source_file).name}: {str(exc)[:100]}")

    _update_store_task(
        task_id,
        status="completed",
        current_stage="done",
        progress=1.0,
        message=f"入库完成: 成功 {imported}，跳过 {skipped}，失败 {len(errors)}",
        imported_count=imported,
        skipped_count=skipped,
        error_count=len(errors),
        errors=errors,
        finished_at=time.time(),
    )
