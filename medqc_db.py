#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Общий модуль БД/хранилища для medqc-* CLI.
- SQLite-схема
- Утилиты ingest
- Запись/чтение raw_text, sections, entities
- Лента событий (events) с колонкой ts (вместо зарезервированного when)
"""
from __future__ import annotations
import os, sqlite3, hashlib, json, shutil, mimetypes, secrets
from pathlib import Path
from datetime import datetime
from typing import Optional, Iterable, Dict, Any, List

DB_PATH = Path(os.getenv("MEDQC_DB", "./medqc.db")).resolve()
CASES_ROOT = Path(os.getenv("MEDQC_CASES", "./cases")).resolve()
CASES_ROOT.mkdir(parents=True, exist_ok=True)

# ------------------ БД ------------------
def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

SCHEMA = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS docs (
  doc_id      TEXT PRIMARY KEY,
  sha256      TEXT NOT NULL,
  src_path    TEXT NOT NULL,
  mime        TEXT,
  size        INTEGER,
  facility    TEXT,
  dept        TEXT,
  author      TEXT,
  admit_dt    TEXT,
  created_at  TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_docs_sha256 ON docs(sha256);

CREATE TABLE IF NOT EXISTS artifacts (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id     TEXT NOT NULL,
  kind       TEXT NOT NULL,    -- 'source','raw','sections','entities','events', ...
  path       TEXT NOT NULL,
  sha256     TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS raw_text (
  doc_id     TEXT PRIMARY KEY,
  is_scanned INTEGER NOT NULL,
  pages      INTEGER,
  producer   TEXT,
  lang_hint  TEXT,
  full_text  TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS pages (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id  TEXT NOT NULL,
  pageno  INTEGER NOT NULL,
  start   INTEGER NOT NULL,
  end     INTEGER NOT NULL,
  text    TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS sections (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id      TEXT NOT NULL,
  section_id  TEXT NOT NULL,
  name        TEXT NOT NULL,
  kind        TEXT,
  start       INTEGER NOT NULL,
  end         INTEGER NOT NULL,
  pageno      INTEGER,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS entities (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id      TEXT NOT NULL,
  section_id  TEXT,
  etype       TEXT NOT NULL,   -- 'datetime','diagnosis','medication','vital','signature', ...
  start       INTEGER NOT NULL,
  end         INTEGER NOT NULL,
  value_json  TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

-- Лента событий (timeline). ВРЕМЯ — колонка ts (а не 'when', т.к. WHEN зарезервировано в SQLite-триггерах)
CREATE TABLE IF NOT EXISTS events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id      TEXT NOT NULL,
  kind        TEXT NOT NULL,   -- 'admit','triage','initial_exam','daily_note','ecg','diagnosis','medication','vital','epicrisis', ...
  ts          TEXT,            -- ISO8601 без TZ (или NULL)
  section_id  TEXT,
  start       INTEGER,
  end         INTEGER,
  value_json  TEXT NOT NULL,   -- payload события (dict)
  created_at  TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);
CREATE INDEX IF NOT EXISTS idx_events_doc_ts ON events(doc_id, ts);
"""

def init_schema() -> None:
    with connect() as c:
        c.executescript(SCHEMA)
        c.commit()

# ------------------ Утилиты ------------------
def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()

def guess_mime(path: Path) -> str:
    m, _ = mimetypes.guess_type(str(path))
    return m or 'application/octet-stream'

def ensure_doc_id() -> str:
    return f"KZ-{datetime.utcnow().strftime('%Y%m%d')}-{secrets.token_hex(4).upper()}"

# ------------------ docs ------------------
def find_doc_by_sha256(sha256: str) -> Optional[str]:
    with connect() as c:
        row = c.execute("SELECT doc_id FROM docs WHERE sha256=?", (sha256,)).fetchone()
        return row[0] if row else None

def insert_doc(doc_id: str, sha256: str, src_path: Path, mime: str, size: int,
               facility: Optional[str], dept: Optional[str], author: Optional[str],
               admit_dt: Optional[str]) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO docs(doc_id, sha256, src_path, mime, size, facility, dept, author, admit_dt, created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (doc_id, sha256, str(src_path), mime, size, facility, dept, author, admit_dt, now_iso()),
        )
        c.commit()

def get_doc(doc_id: str) -> Optional[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()

# ------------------ raw_text/pages ------------------
def upsert_raw_text(doc_id: str, is_scanned: bool, pages_count: int, producer: Optional[str],
                    lang_hint: Optional[str], full_text: str) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO raw_text(doc_id, is_scanned, pages, producer, lang_hint, full_text, created_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(doc_id) DO UPDATE SET
              is_scanned=excluded.is_scanned,
              pages=excluded.pages,
              producer=excluded.producer,
              lang_hint=excluded.lang_hint,
              full_text=excluded.full_text,
              created_at=excluded.created_at
            """,
            (doc_id, int(is_scanned), pages_count, producer, lang_hint, full_text, now_iso()),
        )
        c.commit()

def get_full_text(doc_id: str) -> Optional[str]:
    with connect() as c:
        row = c.execute("SELECT full_text FROM raw_text WHERE doc_id=?", (doc_id,)).fetchone()
        return row[0] if row else None

def replace_pages(doc_id: str, pages: Iterable[Dict[str, Any]]) -> None:
    with connect() as c:
        c.execute("DELETE FROM pages WHERE doc_id=?", (doc_id,))
        c.executemany(
            "INSERT INTO pages(doc_id, pageno, start, end, text) VALUES(?,?,?,?,?)",
            ((doc_id, p["pageno"], p["start"], p["end"], p["text"]) for p in pages),
        )
        c.commit()

# ------------------ sections ------------------
def replace_sections(doc_id: str, sections: Iterable[Dict[str, Any]]) -> None:
    with connect() as c:
        c.execute("DELETE FROM sections WHERE doc_id=?", (doc_id,))
        c.executemany(
            """
            INSERT INTO sections(doc_id, section_id, name, kind, start, end, pageno)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                (doc_id, s["section_id"], s["name"], s.get("kind"), s["start"], s["end"], s.get("pageno"))
                for s in sections
            ),
        )
        c.commit()

def get_sections(doc_id: str) -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM sections WHERE doc_id=? ORDER BY start", (doc_id,)).fetchall()

# ------------------ entities ------------------
def replace_entities(doc_id: str, entities: Iterable[Dict[str, Any]]) -> None:
    with connect() as c:
        c.execute("DELETE FROM entities WHERE doc_id=?", (doc_id,))
        c.executemany(
            """
            INSERT INTO entities(doc_id, section_id, etype, start, end, value_json)
            VALUES(?,?,?,?,?,?)
            """,
            (
                (
                    doc_id,
                    e.get("section_id"),
                    e["etype"],
                    e["start"],
                    e["end"],
                    json.dumps(e["value"], ensure_ascii=False),
                )
                for e in entities
            ),
        )
        c.commit()

def get_entities(doc_id: str) -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM entities WHERE doc_id=? ORDER BY start", (doc_id,)).fetchall()

# ------------------ events (timeline) ------------------
def replace_events(doc_id: str, events: Iterable[Dict[str, Any]]) -> None:
    """
    Принимает события, где время может лежать в ключе 'when' или 'ts'.
    В БД всегда записываем в колонку 'ts'.
    """
    with connect() as c:
        c.execute("DELETE FROM events WHERE doc_id=?", (doc_id,))
        c.executemany(
            """
            INSERT INTO events(doc_id, kind, ts, section_id, start, end, value_json, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                (
                    doc_id,
                    e["kind"],
                    (e.get("when") or e.get("ts")),
                    e.get("section_id"),
                    e.get("start"),
                    e.get("end"),
                    json.dumps(e.get("value", {}), ensure_ascii=False),
                    now_iso(),
                )
                for e in events
            ),
        )
        c.commit()

def get_events(doc_id: str) -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM events WHERE doc_id=? ORDER BY COALESCE(ts,'9999-12-31')", (doc_id,)).fetchall()

# ------------------ storage ------------------
def ensure_case_dir(doc_id: str) -> Path:
    d = CASES_ROOT / doc_id
    d.mkdir(parents=True, exist_ok=True)
    return d

def store_source(file_path: Path, doc_id: str) -> Path:
    dst_dir = ensure_case_dir(doc_id)
    dst = dst_dir / ("source" + file_path.suffix.lower())
    shutil.copy2(file_path, dst)
    return dst

# ------------------ high-level ingest ------------------
def ingest_local_file(path: Path, facility: Optional[str], dept: Optional[str],
                      author: Optional[str], admit_dt: Optional[str]) -> dict:
    init_schema()
    path = path.resolve()
    if not path.exists():
        return {"error": {"code": "NOT_FOUND", "message": f"no such file: {path}"}}
    # hash & duplicate check
    sha = file_sha256(path)
    existing = find_doc_by_sha256(sha)
    if existing:
        return {"doc_id": existing, "status": "duplicate", "sha256": sha}
    # new doc
    doc_id = ensure_doc_id()
    dst = store_source(path, doc_id)
    mime = guess_mime(dst)
    insert_doc(doc_id, sha, dst, mime, dst.stat().st_size, facility, dept, author, admit_dt)
    return {
        "doc_id": doc_id,
        "status": "ingested",
        "sha256": sha,
        "src_path": str(dst)
    }
