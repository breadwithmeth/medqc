# medqc_db.py
# ВАРИАНТ А: все публичные методы, возвращающие строки из БД,
# конвертируют sqlite3.Row → dict, чтобы наверху можно было безопасно вызывать .get(...)

import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

# =========================
# Подключение и row_factory
# =========================

def connect(db_path: str) -> sqlite3.Connection:
    """
    Открывает соединение к SQLite и включает sqlite3.Row как row_factory,
    чтобы сохранялись имена колонок.
    """
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
    """
    Безопасно конвертирует sqlite3.Row → dict.
    Возвращает None, если row == None.
    """
    if row is None:
        return None
    try:
        return dict(row)
    except Exception:
        # На всякий случай, если row уже dict или что-то необычное
        return row  # type: ignore[return-value]

def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> List[Dict[str, Any]]:
    """
    Конвертирует Iterable[sqlite3.Row] → List[dict].
    """
    return [row_to_dict(r) for r in rows if r is not None]  # type: ignore[list-item]

# =========================
# CRUD / Select helpers
# =========================

def get_doc(conn: sqlite3.Connection, doc_id: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает документ по doc_id как dict (не sqlite3.Row).
    Предполагаем таблицу docs с колонками как минимум: doc_id (PK).
    """
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM docs WHERE doc_id = ?", (doc_id,))
        row = cur.fetchone()
    return row_to_dict(row)

def upsert_doc(conn: sqlite3.Connection, doc: Dict[str, Any]) -> None:
    """
    Пример upsert: если у вас уже реализован — оставьте ваш код.
    Здесь просто набросок; адаптируйте под свою схему.
    """
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
    """
    Возвращает список сущностей, извлечённых из документа, как list[dict].
    Предполагаем таблицу entities: (doc_id, kind, value, start, end, ...).
    """
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM entities WHERE doc_id = ? ORDER BY rowid ASC", (doc_id,))
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def get_doc_events(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    """
    Возвращает список «событий»/таймлайна документа как list[dict].
    Предполагаем таблицу events: (doc_id, event_type, ts, payload, ...).
    """
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM events WHERE doc_id = ? ORDER BY ts ASC, rowid ASC", (doc_id,))
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def get_active_rules_package(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """
    Возвращает активный пакет правил (package/version/…).
    Предполагаем таблицу rules_meta с флагом active.
    """
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM rules_meta WHERE active = 1 ORDER BY updated_at DESC LIMIT 1")
        row = cur.fetchone()
    return row_to_dict(row)

def list_rules_for_profile(conn: sqlite3.Connection, profile: str) -> List[Dict[str, Any]]:
    """
    Возвращает список правил для профиля (enabled=1).
    Предполагаем таблицу rules с колонками (id, profile, enabled, severity, params_json, ...).
    """
    with get_cursor(conn) as cur:
        cur.execute(
            "SELECT * FROM rules WHERE enabled = 1 AND profile = ? ORDER BY id ASC",
            (profile,),
        )
        rows = cur.fetchall()
    return rows_to_dicts(rows)

def list_all_rules(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    Возвращает все активные правила (enabled=1).
    """
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM rules WHERE enabled = 1 ORDER BY profile, id")
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
    """
    Логирует применение правила к документу (для аудита/отчётов).
    Предполагаем таблицу rule_applications.
    """
    with get_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO rule_applications (doc_id, rule_id, status, reason, evidence_ref, payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (doc_id, rule_id, status, reason, evidence_ref, payload),
        )
        conn.commit()

def list_rule_applications(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    """
    Возвращает список применений правил к документу как list[dict].
    """
    with get_cursor(conn) as cur:
        cur.execute(
            "SELECT * FROM rule_applications WHERE doc_id = ? ORDER BY created_at ASC, rowid ASC",
            (doc_id,),
        )
        rows = cur.fetchall()
    return rows_to_dicts(rows)

# =========================
# Прочее (поддержка API)
# =========================

def get_doc_stats(conn: sqlite3.Connection, doc_id: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает агрегированные/предрасчитанные статистики по документу (если есть).
    """
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM doc_stats WHERE doc_id = ?", (doc_id,))
        row = cur.fetchone()
    return row_to_dict(row)

def list_violations(conn: sqlite3.Connection, doc_id: str) -> List[Dict[str, Any]]:
    """
    Возвращает только нарушения по документу (view или таблица).
    """
    with get_cursor(conn) as cur:
        cur.execute("SELECT * FROM violations WHERE doc_id = ? ORDER BY severity DESC, rowid ASC", (doc_id,))
        rows = cur.fetchall()
    return rows_to_dicts(rows)
