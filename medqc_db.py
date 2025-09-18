#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

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
    """
    cur = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,))
    row = cur.fetchone()
    if not row:
        return None

    cols = [d[0] for d in cur.description]
    data = dict(zip(cols, row))

    # 1) src_path — главный
    p = (data.get("src_path") or "").strip()
    if p:
        if os.path.isabs(p) and os.path.exists(p):
            return p
        cand = os.path.join("/app", p.lstrip("/"))
        if os.path.exists(cand):
            return cand

    # 2) path (если задан)
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

    # 4) любой файл внутри uploads/<doc_id>
    folder = os.path.join(UPLOADS_DIR, doc_id)
    if os.path.isdir(folder):
        files = sorted(glob.glob(os.path.join(folder, "*")))
        if files:
            return files[0]

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

def ensure_extract_tables(conn: sqlite3.Connection):
    conn.executescript(SCHEMA_PAGES)
    conn.executescript(SCHEMA_RAW)
