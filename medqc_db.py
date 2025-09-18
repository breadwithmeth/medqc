#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import json
import sqlite3
from typing import Any, Dict, List, Optional, Iterable, Mapping

# Конфигурация путей
DB_PATH = os.getenv("MEDQC_DB", "./medqc.db")
UPLOADS_DIR = os.getenv("MEDQC_UPLOADS", "/app/uploads")

# ----------------------------------------------------------------------
# Базовые утилиты
# ----------------------------------------------------------------------

def get_conn():
    db = os.getenv("MEDQC_DB", "/app/medqc.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn

def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r[1] for r in rows}
    except Exception:
        return set()

def _ensure_column_simple(conn: sqlite3.Connection, table: str, col: str, decl: str):
    """Добавляет колонку без неконстантных DEFAULT (совместимо с SQLite ALTER TABLE)."""
    cols = _table_columns(conn, table)
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

def dicts(cur, rows):
    cols = [c[0] for c in cur.description]
    for r in rows:
        yield dict(zip(cols, r))

# ----------------------------------------------------------------------
# docs: schema + helpers
# ----------------------------------------------------------------------

def ensure_docs_schema(conn: sqlite3.Connection):
    """
    Создаёт (если нет) таблицу docs и мягко добавляет недостающие столбцы.
    Целевая схема:
      doc_id TEXT PRIMARY KEY,
      sha256 TEXT,
      src_path TEXT,
      mime TEXT,
      size INTEGER,
      facility TEXT,
      dept TEXT,
      author TEXT,
      admit_dt TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      filename TEXT,
      path TEXT,
      department TEXT
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS docs(
            doc_id     TEXT PRIMARY KEY,
            sha256     TEXT,
            src_path   TEXT,
            mime       TEXT,
            size       INTEGER,
            facility   TEXT,
            dept       TEXT,
            author     TEXT,
            admit_dt   TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            filename   TEXT,
            path       TEXT,
            department TEXT
        );
    """)
    # мягкая миграция столбцов (если таблица существовала с иной схемой)
    _ensure_column_simple(conn, "docs", "sha256",     "TEXT")
    _ensure_column_simple(conn, "docs", "src_path",   "TEXT")
    _ensure_column_simple(conn, "docs", "mime",       "TEXT")
    _ensure_column_simple(conn, "docs", "size",       "INTEGER")
    _ensure_column_simple(conn, "docs", "facility",   "TEXT")
    _ensure_column_simple(conn, "docs", "dept",       "TEXT")
    _ensure_column_simple(conn, "docs", "author",     "TEXT")
    _ensure_column_simple(conn, "docs", "admit_dt",   "TEXT")
    cols = _table_columns(conn, "docs")
    if "created_at" not in cols:
        conn.execute("ALTER TABLE docs ADD COLUMN created_at TEXT")
        conn.execute("UPDATE docs SET created_at = datetime('now') WHERE created_at IS NULL")
    _ensure_column_simple(conn, "docs", "filename",   "TEXT")
    _ensure_column_simple(conn, "docs", "path",       "TEXT")
    _ensure_column_simple(conn, "docs", "department", "TEXT")

# ----------------------------------------------------------------------
# Поиск исходного файла документа (docs.* + /app/uploads)
# ----------------------------------------------------------------------

def get_doc(conn: sqlite3.Connection, doc_id: str) -> Dict[str, Any]:
    cur = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,))
    row = cur.fetchone()
    if not row:
        return {}
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))

def get_doc_file_path(conn: sqlite3.Connection, doc_id: str) -> Optional[str]:
    """
    Возвращает абсолютный путь к исходному файлу документа.
    Приоритет:
      1) docs.src_path
      2) docs.path
      3) /app/uploads/<doc_id>/<filename>
      4) любой файл в /app/uploads/<doc_id>/*
      5) ЛЕГАСИ: /app/uploads/<doc_id>__*.*
      6) ЛЕГАСИ: /app/uploads/<doc_id>*.*
    """
    cur = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,))
    row = cur.fetchone()
    cols = [d[0] for d in cur.description] if cur.description else []
    data = dict(zip(cols, row)) if row else {}

    # 1) src_path
    p = (data.get("src_path") or "").strip()
    if p:
        if os.path.isabs(p) and os.path.exists(p):
            return p
        cand = os.path.join("/app", p.lstrip("/"))
        if os.path.exists(cand):
            return cand

    # 2) path
    p = (data.get("path") or "").strip()
    if p:
        if os.path.isabs(p) and os.path.exists(p):
            return p
        cand = os.path.join("/app", p.lstrip("/"))
        if os.path.exists(cand):
            return cand

    # 3) uploads/<doc_id>/<filename>
    fname = (data.get("filename") or "").strip()
    if fname:
        cand = os.path.join(UPLOADS_DIR, doc_id, fname)
        if os.path.exists(cand):
            return cand

    # 4) любой файл в uploads/<doc_id>/*
    folder = os.path.join(UPLOADS_DIR, doc_id)
    if os.path.isdir(folder):
        files = sorted(glob.glob(os.path.join(folder, "*")))
        if files:
            return files[0]

    # 5) ЛЕГАСИ: /app/uploads/<doc_id>__*.*
    legacy = sorted(glob.glob(os.path.join(UPLOADS_DIR, f"{doc_id}__*")))
    if legacy:
        return legacy[0]

    # 6) ЛЕГАСИ: /app/uploads/<doc_id>*.*
    legacy2 = sorted(glob.glob(os.path.join(UPLOADS_DIR, f"{doc_id}*")))
    if legacy2:
        return legacy2[0]

    return None

# ----------------------------------------------------------------------
# Тексты: pages/raw + мягкие миграции
# ----------------------------------------------------------------------

def ensure_extract_tables(conn: sqlite3.Connection):
    """
    Создаём/мигрируем pages/raw. Для уже существующих таблиц мягко добавляем недостающие столбцы.
    Важно: при ALTER TABLE не используем неконстантные DEFAULT (sqlite не поддерживает).
    """
    # pages
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pages(
            id         INTEGER PRIMARY KEY,
            doc_id     TEXT NOT NULL,
            idx        INTEGER,
            text       TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    _ensure_column_simple(conn, "pages", "idx",  "INTEGER")
    _ensure_column_simple(conn, "pages", "text", "TEXT")
    cols = _table_columns(conn, "pages")
    if "created_at" not in cols:
        conn.execute("ALTER TABLE pages ADD COLUMN created_at TEXT")
        conn.execute("UPDATE pages SET created_at = datetime('now') WHERE created_at IS NULL")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_doc ON pages(doc_id, idx)")
    except Exception:
        pass

    # raw
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw(
            doc_id     TEXT PRIMARY KEY,
            content    TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)

def get_full_text(doc_id: str) -> str:
    """
    Возвращает «сырой» текст документа.
    Сначала пробует raw.content, если нет — собирает из artifacts(kind='text_pages').
    """
    conn = get_conn()
    try:
        # 1) пробуем raw
        cur = conn.execute("SELECT content FROM raw WHERE doc_id=?", (doc_id,))
        r = cur.fetchone()
        if r and r["content"]:
            return r["content"]
        # 2) собираем из artifacts
        cur = conn.execute("SELECT content FROM artifacts WHERE doc_id=? AND kind='text_pages'", (doc_id,))
        a = cur.fetchone()
        if a and a["content"]:
            try:
                pages = json.loads(a["content"])
                if isinstance(pages, list):
                    return "\n\n".join(pages)
                return str(a["content"])
            except Exception:
                return str(a["content"])
        return ""
    finally:
        conn.close()

# ----------------------------------------------------------------------
# sections: schema + replace/get
# ----------------------------------------------------------------------

def _ensure_sections_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sections(
            id         INTEGER PRIMARY KEY,
            doc_id     TEXT NOT NULL,
            idx        INTEGER,
            start      INTEGER,
            "end"      INTEGER,
            title      TEXT,
            name       TEXT,
            text       TEXT,
            kind       TEXT,
            created_at TEXT
        );
    """)
    _ensure_column_simple(conn, "sections", "idx",   "INTEGER")
    _ensure_column_simple(conn, "sections", "start", "INTEGER")
    _ensure_column_simple(conn, "sections", "end",   "INTEGER")
    _ensure_column_simple(conn, "sections", "title", "TEXT")
    _ensure_column_simple(conn, "sections", "name",  "TEXT")
    _ensure_column_simple(conn, "sections", "text",  "TEXT")
    _ensure_column_simple(conn, "sections", "kind",  "TEXT")
    if "created_at" not in _table_columns(conn, "sections"):
        conn.execute("ALTER TABLE sections ADD COLUMN created_at TEXT")
        conn.execute("UPDATE sections SET created_at = datetime('now') WHERE created_at IS NULL")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sections_doc ON sections(doc_id, idx)")
    except Exception:
        pass

def _val(d: Mapping, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def replace_sections(doc_id: str, rows: Iterable[Mapping]):
    with get_conn() as conn:
        _ensure_sections_schema(conn)
        conn.execute("DELETE FROM sections WHERE doc_id=?", (doc_id,))
        payload = []
        for r in rows:
            payload.append((
                doc_id,
                _val(r, "idx", "index", default=None),
                _val(r, "start", default=None),
                _val(r, "end", default=None),
                _val(r, "title", default=None),
                _val(r, "name", default=None),
                _val(r, "text", "content", default=""),
                _val(r, "kind", default=None),
            ))
        if payload:
            conn.executemany("""
                INSERT INTO sections(doc_id, idx, start, "end", title, name, text, kind, created_at)
                VALUES(?,?,?,?,?,?,?, ?, datetime('now'))
            """, payload)
        conn.commit()

def get_sections(*args, **kwargs) -> List[Dict[str, Any]]:
    if len(args) == 1:
        conn = get_conn()
        doc_id = args[0]
        should_close = True
    elif len(args) == 2:
        conn, doc_id = args
        should_close = False
    else:
        raise TypeError("get_sections expects (doc_id) or (conn, doc_id)")

    try:
        cols_info = conn.execute("PRAGMA table_info(sections)").fetchall()
        colnames_existing = {r[1] for r in cols_info}
        order_by = "idx" if "idx" in colnames_existing else ("start" if "start" in colnames_existing else "id")

        cur = conn.execute(f"SELECT * FROM sections WHERE doc_id=? ORDER BY {order_by}", (doc_id,))
        rows = []
        if cur.description:
            colnames = [c[0] for c in cur.description]
            for rec in cur.fetchall():
                row = dict(zip(colnames, rec))
                row.setdefault("section_id", row.get("id"))
                row.setdefault("kind", None)
                row.setdefault("idx", None)
                row.setdefault("start", None)
                row.setdefault("end", None)
                row.setdefault("title", row.get("title") or row.get("name"))
                row.setdefault("name", row.get("name") or row.get("title"))
                row.setdefault("text", row.get("content") or row.get("text") or "")
                rows.append(row)
        return rows
    finally:
        if should_close:
            conn.close()

# ----------------------------------------------------------------------
# entities: schema + replace/get
# ----------------------------------------------------------------------

def _ensure_entities_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entities(
            id         INTEGER PRIMARY KEY,
            doc_id     TEXT NOT NULL,
            etype      TEXT,
            value_json TEXT,
            span_start INTEGER,
            span_end   INTEGER,
            section_id INTEGER,
            created_at TEXT
        );
    """)
    _ensure_column_simple(conn, "entities", "etype", "TEXT")
    _ensure_column_simple(conn, "entities", "value_json", "TEXT")
    _ensure_column_simple(conn, "entities", "span_start", "INTEGER")
    _ensure_column_simple(conn, "entities", "span_end", "INTEGER")
    _ensure_column_simple(conn, "entities", "section_id", "INTEGER")
    if "created_at" not in _table_columns(conn, "entities"):
        conn.execute("ALTER TABLE entities ADD COLUMN created_at TEXT")
        conn.execute("UPDATE entities SET created_at = datetime('now') WHERE created_at IS NULL")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_doc ON entities(doc_id)")
    except Exception:
        pass

def replace_entities(doc_id: str, rows: Iterable[Mapping]):
    with get_conn() as conn:
        _ensure_entities_schema(conn)
        conn.execute("DELETE FROM entities WHERE doc_id=?", (doc_id,))
        payload = []
        for r in rows:
            val = _val(r, "value_json", default=None)
            if val is None:
                vobj = _val(r, "value", default={})
                try:
                    val = json.dumps(vobj, ensure_ascii=False)
                except Exception:
                    val = "{}"
            payload.append((
                doc_id,
                _val(r, "etype", default=None),
                val,
                _val(r, "span_start", "start", default=None),
                _val(r, "span_end", "end", default=None),
                _val(r, "section_id", default=None),
            ))
        if payload:
            conn.executemany("""
                INSERT INTO entities(doc_id, etype, value_json, span_start, span_end, section_id, created_at)
                VALUES(?,?,?,?,?, ?, datetime('now'))
            """, payload)
        conn.commit()

def get_entities(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM entities WHERE doc_id=?", (doc_id,))
    return list(dicts(cur, cur.fetchall()))

# ----------------------------------------------------------------------
# events (для timeline/rules)
# ----------------------------------------------------------------------

def init_events_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY,
      doc_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      ts TEXT,
      value_json TEXT
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_doc ON events(doc_id, ts)")

def clear_events(conn: sqlite3.Connection, doc_id: str):
    conn.execute("DELETE FROM events WHERE doc_id=?", (doc_id,))

def add_event(conn: sqlite3.Connection, doc_id: str, kind: str, ts: Optional[str], value: Optional[Dict[str, Any]] = None):
    init_events_schema(conn)
    conn.execute(
        "INSERT INTO events(doc_id, kind, ts, value_json) VALUES(?,?,?,?)",
        (doc_id, kind, ts, json.dumps(value or {}, ensure_ascii=False))
    )

def get_events(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()]
    ts_col = "ts" if "ts" in cols else ("when" if "when" in cols else None)
    order = f" ORDER BY {ts_col}" if ts_col else ""
    cur = conn.execute(f"SELECT * FROM events WHERE doc_id=?{order}", (doc_id,))
    return list(dicts(cur, cur.fetchall()))

# --- violations schema + мягкая миграция ---
def ensure_violations_schema(conn: sqlite3.Connection):
    """
    Унифицированная таблица нарушений.
    Базовые поля совместимы со старой схемой, новые — добавляются мягко.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS violations(
            id           INTEGER PRIMARY KEY,
            doc_id       TEXT NOT NULL,
            rule_id      TEXT NOT NULL,
            severity     TEXT NOT NULL,
            message      TEXT NOT NULL,
            evidence_json TEXT,
            sources_json  TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now')),
            profile       TEXT,
            extra_json    TEXT
        );
    """)
    # мягкие добавления на случай старых БД
    _ensure_column_simple(conn, "violations", "profile",   "TEXT")
    _ensure_column_simple(conn, "violations", "extra_json","TEXT")
    cols = _table_columns(conn, "violations")
    if "created_at" not in cols:
        conn.execute("ALTER TABLE violations ADD COLUMN created_at TEXT")
        conn.execute("UPDATE violations SET created_at = datetime('now') WHERE created_at IS NULL")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_violations_doc ON violations(doc_id)")
    except Exception:
        pass

# --- универсальная инициализация схемы для CLI, которые её просят ---
def init_schema():
    """
    Создаёт/мигрирует все необходимые таблицы. Безопасно вызывать много раз.
    """
    with get_conn() as conn:
        # порядок важен только для удобочитаемости
        ensure_docs_schema(conn)
        ensure_extract_tables(conn)
        _ensure_sections_schema(conn)
        _ensure_entities_schema(conn)
        init_events_schema(conn)
        ensure_violations_schema(conn)
        conn.commit()
