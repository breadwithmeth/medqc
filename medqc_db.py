#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
import glob

DB_PATH = os.getenv("MEDQC_DB", "./medqc.db")
UPLOADS_DIR = os.getenv("MEDQC_UPLOADS", "/app/uploads")


# ---------- low-level ----------

def get_conn(path: Optional[str] = None) -> sqlite3.Connection:
    return sqlite3.connect(path or DB_PATH)


def dicts(cur, rows):
    cols = [c[0] for c in cur.description]
    for r in rows:
        yield dict(zip(cols, r))


# ---------- docs / file path helpers ----------

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


# ---------- generic readers used by rules/timeline ----------

def get_sections(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    # пробуем распространённые варианты сортировки
    for order_col in ("start", "idx", "pos", "rowid"):
        try:
            cur = conn.execute(f"SELECT * FROM sections WHERE doc_id=? ORDER BY {order_col}", (doc_id,))
            return list(dicts(cur, cur.fetchall()))
        except Exception:
            continue
    cur = conn.execute("SELECT * FROM sections WHERE doc_id=?", (doc_id,))
    return list(dicts(cur, cur.fetchall()))


def get_entities(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    cur = conn.execute("SELECT * FROM entities WHERE doc_id=?", (doc_id,))
    return list(dicts(cur, cur.fetchall()))


def get_events(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    # определим, как у нас называется колонка времени
    cols = [r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()]
    ts_col = "ts" if "ts" in cols else ("when" if "when" in cols else None)
    order = f" ORDER BY {ts_col}" if ts_col else ""
    cur = conn.execute(f"SELECT * FROM events WHERE doc_id=?{order}", (doc_id,))
    return list(dicts(cur, cur.fetchall()))



def get_full_text(doc_id: str) -> str:
    """
    Возвращает полный текст документа:
      1) из raw.content, если есть;
      2) иначе склеивает pages.text по idx;
      3) иначе пустая строка.
    """
    with get_conn() as conn:
        # raw
        cur = conn.execute("SELECT content FROM raw WHERE doc_id=?", (doc_id,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        # pages
        try:
            cur = conn.execute("SELECT text FROM pages WHERE doc_id=? ORDER BY idx", (doc_id,))
            texts = [r[0] or "" for r in cur.fetchall()]
            if texts:
                return "\n\n".join(texts)
        except Exception:
            pass
    return ""



# ---------- simple init helpers (не агрессивно) ----------

SCHEMA_PAGES = """
CREATE TABLE IF NOT EXISTS pages(
  id       INTEGER PRIMARY KEY,
  doc_id   TEXT NOT NULL,
  idx      INTEGER NOT NULL,
  text     TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_pages_doc ON pages(doc_id, idx);
"""

SCHEMA_RAW = """
CREATE TABLE IF NOT EXISTS raw(
  doc_id    TEXT PRIMARY KEY,
  content   TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""
def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r[1] for r in rows}
    except Exception:
        return set()

def _ensure_column_simple(conn: sqlite3.Connection, table: str, col: str, decl: str):
    """Простое добавление колонки без неконстантных DEFAULT."""
    cols = _table_columns(conn, table)
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

def ensure_extract_tables(conn: sqlite3.Connection):
    """
    Создаём/мигрируем pages/raw. Для уже существующих таблиц мягко добавляем недостающие столбцы.
    Важно: при ALTER TABLE не используем неконстантные DEFAULT (sqlite не поддерживает).
    """
    # pages: создать если нет (с дефолтами допустимыми при CREATE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pages(
            id         INTEGER PRIMARY KEY,
            doc_id     TEXT NOT NULL,
            idx        INTEGER,
            text       TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)

    # мягкая миграция существующей pages
    _ensure_column_simple(conn, "pages", "idx",  "INTEGER")
    _ensure_column_simple(conn, "pages", "text", "TEXT")
    # created_at добавляем как TEXT без DEFAULT, затем проставляем значения там, где NULL
    cols = _table_columns(conn, "pages")
    if "created_at" not in cols:
        conn.execute(f"ALTER TABLE pages ADD COLUMN created_at TEXT")
        conn.execute("UPDATE pages SET created_at = datetime('now') WHERE created_at IS NULL")

    # индекс (идемпотентно)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_doc ON pages(doc_id, idx)")
    except Exception:
        pass

    # raw: создать если нет
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw(
            doc_id     TEXT PRIMARY KEY,
            content    TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
