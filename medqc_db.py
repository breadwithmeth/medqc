#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
import glob
from typing import Iterable, Mapping

DB_PATH = os.getenv("MEDQC_DB", "./medqc.db")
UPLOADS_DIR = os.getenv("MEDQC_UPLOADS", "/app/uploads")




def _val(d: Mapping, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def replace_sections(doc_id: str, rows: Iterable[Mapping]):
    """
    Полностью заменяет секции документа.
    rows — итерируемая коллекция dict-подобных объектов.
    Поддерживает ключи: idx, start, end, title, name, text  (избыточные — игнорируются).
    """
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
                _val(r, "text", "content", default="")
            ))
        if payload:
            conn.executemany("""
                INSERT INTO sections(doc_id, idx, start, "end", title, name, text, created_at)
                VALUES(?,?,?,?,?,?,?, datetime('now'))
            """, payload)
        conn.commit()




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
def _ensure_table(conn: sqlite3.Connection, create_sql: str):
    conn.execute(create_sql)

def _ensure_sections_schema(conn: sqlite3.Connection):
    # создать если нет
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
            created_at TEXT
        );
    """)
    # мягко добавить недостающие колонки
    _ensure_column_simple(conn, "sections", "idx",  "INTEGER")
    _ensure_column_simple(conn, "sections", "start","INTEGER")
    _ensure_column_simple(conn, "sections", "end",  "INTEGER")
    _ensure_column_simple(conn, "sections", "title","TEXT")
    _ensure_column_simple(conn, "sections", "name", "TEXT")
    _ensure_column_simple(conn, "sections", "text", "TEXT")
    if "created_at" not in _table_columns(conn, "sections"):
        conn.execute("ALTER TABLE sections ADD COLUMN created_at TEXT")
        conn.execute("UPDATE sections SET created_at = datetime('now') WHERE created_at IS NULL")
    # индексы
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sections_doc ON sections(doc_id, idx)")
    except Exception:
        pass


def _ensure_entities_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entities(
            id         INTEGER PRIMARY KEY,
            doc_id     TEXT NOT NULL,
            etype      TEXT,
            value_json TEXT,
            span_start INTEGER,
            span_end   INTEGER,
            created_at TEXT
        );
    """)
    _ensure_column_simple(conn, "entities", "etype", "TEXT")
    _ensure_column_simple(conn, "entities", "value_json", "TEXT")
    _ensure_column_simple(conn, "entities", "span_start", "INTEGER")
    _ensure_column_simple(conn, "entities", "span_end", "INTEGER")
    if "created_at" not in _table_columns(conn, "entities"):
        conn.execute("ALTER TABLE entities ADD COLUMN created_at TEXT")
        conn.execute("UPDATE entities SET created_at = datetime('now') WHERE created_at IS NULL")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_doc ON entities(doc_id)")
    except Exception:
        pass

def replace_entities(doc_id: str, rows: Iterable[Mapping]):
    """
    Полностью заменяет сущности документа.
    rows — dict-подобные объекты с ключами: etype, value_json|value, span_start|start, span_end|end.
    """
    with get_conn() as conn:
        _ensure_entities_schema(conn)
        conn.execute("DELETE FROM entities WHERE doc_id=?", (doc_id,))
        payload = []
        for r in rows:
            val = _val(r, "value_json", default=None)
            if val is None:
                # если пришёл python-объект в "value" — сериализуем
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
            ))
        if payload:
            conn.executemany("""
                INSERT INTO entities(doc_id, etype, value_json, span_start, span_end, created_at)
                VALUES(?,?,?,?,?, datetime('now'))
            """, payload)
        conn.commit()


def get_sections(doc_id: str) -> list[dict]:
    """
    Возвращает список секций документа.
    Каждая секция: dict с ключами id, doc_id, idx, start, end, title, name, text, created_at.
    """
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "SELECT id, doc_id, idx, start, \"end\", title, name, text, created_at "
                "FROM sections WHERE doc_id=? ORDER BY idx",
                (doc_id,)
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception:
            return []
