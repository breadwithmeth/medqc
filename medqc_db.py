# medqc_db.py
# ВАРИАНТ А: слой БД всегда возвращает dict (Row→dict), плюс ensure_schema()

import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple

# =========================
# Подключение и row_factory
# =========================

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

@contextmanager
def get_cursor(conn: sqlite3.Connection):
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()

# =========================
# Универсальная конвертация
# =========================

def row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    try:
        return dict(row)
    except Exception:
        return row  # type: ignore[return-value]

def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [row_to_dict(r) for r in rows if r is not None]  # type: ignore[list-item]

# =========================
# Схема / миграции
# =========================

SCHEMA_SQL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS docs (
  doc_id        TEXT PRIMARY KEY,
  profile       TEXT,
  dept          TEXT,
  title         TEXT,
  content       TEXT,
  content_head  TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT DEFAULT (datetime('now'))
);

CREATE TRIGGER IF NOT EXISTS trg_docs_updated_at
AFTER UPDATE ON docs
FOR EACH ROW
BEGIN
  UPDATE docs SET updated_at = datetime('now') WHERE doc_id = OLD.doc_id;
END;

CREATE TABLE IF NOT EXISTS entities (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id    TEXT NOT NULL,
  kind      TEXT,
  value     TEXT,
  start     INTEGER,
  "end"     INTEGER,
  payload   TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (doc_id) REFERENCES docs(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_entities_doc ON entities(doc_id);

CREATE TABLE IF NOT EXISTS events (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id    TEXT NOT NULL,
  event_type TEXT,
  ts        TEXT,
  payload   TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (doc_id) REFERENCES docs(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_events_doc_ts ON events(doc_id, ts);

CREATE TABLE IF NOT EXISTS rules_meta (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  package    TEXT NOT NULL,
  version    TEXT NOT NULL,
  title      TEXT,
  description TEXT,
  active     INTEGER DEFAULT 0,
  imported_at TEXT DEFAULT (datetime('now')),
  UNIQUE(package, version)
);

CREATE INDEX IF NOT EXISTS idx_rules_meta_active ON rules_meta(active);

CREATE TABLE IF NOT EXISTS rules (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id       TEXT NOT NULL,        -- например "STA-020"
  package       TEXT NOT NULL,
  version       TEXT NOT NULL,
  title         TEXT,
  profile       TEXT,
  severity      TEXT,
  enabled       INTEGER,
  params_json   TEXT,
  sources_json  TEXT,
  effective_from TEXT,
  effective_to   TEXT,
  notes          TEXT,
  created_at     TEXT DEFAULT (datetime('now')),
  UNIQUE(rule_id, package, version)
);

CREATE INDEX IF NOT EXISTS idx_rules_profile ON rules(profile);
CREATE INDEX IF NOT EXISTS idx_rules_enabled ON rules(enabled);

CREATE TABLE IF NOT EXISTS rule_applications (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id       TEXT NOT NULL,
  rule_id      TEXT NOT NULL,
  status       TEXT NOT NULL,      -- PASS | VIOLATION | SKIPPED | INCONCLUSIVE
  reason       TEXT,
  evidence_ref TEXT,
  payload      TEXT,
  created_at   TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (doc_id) REFERENCES docs(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rule_apps_doc ON rule_applications(doc_id);
CREATE INDEX IF NOT EXISTS idx_rule_apps_rule ON rule_applications(rule_id);

CREATE TABLE IF NOT EXISTS violations (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id       TEXT NOT NULL,
  rule_id      TEXT NOT NULL,
  severity     TEXT,
  reason       TEXT,
  evidence_ref TEXT,
  created_at   TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (doc_id) REFERENCES docs(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_violations_doc ON violations(doc_id);

CREATE TABLE IF NOT EXISTS doc_stats (
  doc_id      TEXT PRIMARY KEY,
  payload     TEXT,
  updated_at  TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (doc_id) REFERENCES docs(doc_id) ON DELETE CASCADE
);
"""

def ensure_schema(conn: sqlite3.Connection) -> None:
    with get_cursor(conn) as cur:
        cur.executescript(SCHEMA_SQL)
        conn.commit()

# =========================
# CRUD / Select helpers
# =========================

def get_doc(conn: sqlite3.Connection, doc_id: str) -> Optional[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM docs WHERE doc_id = ?", (doc_id,))
        row = cur.fetchone()
    return row_to_dict(row)

def upsert_doc(conn: sqlite3.Connection, doc: Dict[str, Any]) -> None:
    cols = list(doc.keys())
    placeholders = ",".join(["?"] * len(cols))
    columns_csv = ",".join(cols)
    update_csv = ",".join([f"{c}=excluded.{c}" for c in cols if c != "doc_id"])
    sql = f"""
    INSERT INTO docs ({columns_csv}) VALUES ({placeholders})
    ON CONFLICT(doc_id) DO UPDATE SET {update_csv}
    """
    with get_cursor(conn) as cur:
        cur.execute(sql, tuple(doc[c] for c in cols))
        conn.commit()

def get_doc_entities(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM entities WHERE doc_id = ? ORDER BY id ASC", (doc_id,))
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def get_doc_events(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM events WHERE doc_id = ? ORDER BY ts ASC, id ASC", (doc_id,))
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def get_active_rules_package(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM rules_meta WHERE active = 1 ORDER BY imported_at DESC LIMIT 1")
        row = cur.fetchone()
    return row_to_dict(row)

def set_active_rules_package(conn: sqlite3.Connection, package: str, version: str) -> None:
    with get_cursor(conn) as cur:
        cur.execute("UPDATE rules_meta SET active = 0 WHERE active = 1")
        cur.execute(
            "UPDATE rules_meta SET active = 1 WHERE package = ? AND version = ?",
            (package, version),
        )
        conn.commit()

def list_rules_for_profile(conn: sqlite3.Connection, profile: str) -> List[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute(
            "SELECT * FROM rules WHERE enabled = 1 AND profile = ? ORDER BY rule_id ASC",
            (profile,),
        )
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def list_all_rules(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM rules WHERE enabled = 1 ORDER BY profile, rule_id")
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def save_rule_application(
    conn: sqlite3.Connection,
    doc_id: str,
    rule_id: str,
    status: str,
    reason: Optional[str] = None,
    evidence_ref: Optional[str] = None,
    payload: Optional[str] = None,
) -> None:
    with get_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO rule_applications (doc_id, rule_id, status, reason, evidence_ref, payload)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (doc_id, rule_id, status, reason, evidence_ref, payload),
        )
        conn.commit()

def list_rule_applications(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute(
            "SELECT * FROM rule_applications WHERE doc_id = ? ORDER BY created_at ASC, id ASC",
            (doc_id,),
        )
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def get_doc_stats(conn: sqlite3.Connection, doc_id: str) -> Optional[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM doc_stats WHERE doc_id = ?", (doc_id,))
        row = cur.fetchone()
    return row_to_dict(row)

def list_violations(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    with get_cursor(conn) as cur:
        cur.execute(
            "SELECT * FROM violations WHERE doc_id = ? ORDER BY id ASC",
            (doc_id,),
        )
        rows = cur.fetchall()
    return rows_to_dicts(rows)
