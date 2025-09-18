#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import argparse
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from medqc_db import DB_PATH, get_conn, get_sections, get_entities

# Нормализация дат/времени
def parse_iso_any(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # попытка ISO-подобного
    try:
        t = s.replace("Z", "")
        if "+" in t:
            t = t.split("+", 1)[0]
        return datetime.fromisoformat(t)
    except Exception:
        return None


# Эвристики поиска дат в тексте (простой вариант)
DATE_RE = re.compile(r"(?:(?:20|19)\d{2})[-./](?:0?[1-9]|1[0-2])[-./](?:0?[1-9]|[12]\d|3[01])(?:[ T](?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d)?)?")


def init_events_schema(conn: sqlite3.Connection):
    """
    Создаём таблицу events, если её нет. Колонку времени называем ts.
    НЕ используем ключевое слово 'when'.
    """
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


def add_event(conn: sqlite3.Connection, doc_id: str, kind: str, ts: Optional[datetime], value: Optional[Dict[str, Any]] = None):
    init_events_schema(conn)
    conn.execute(
        "INSERT INTO events(doc_id, kind, ts, value_json) VALUES(?,?,?,?)",
        (doc_id, kind, (ts.strftime("%Y-%m-%dT%H:%M:%S") if ts else None), json.dumps(value or {}, ensure_ascii=False))
    )


def guess_admit_discharge_from_sections(sections: List[Dict[str, Any]]) -> Dict[str, Optional[datetime]]:
    """
    Очень простая эвристика: пытаемся найти по заголовкам.
    """
    admit = None
    discharge = None
    for s in sections:
        title = (s.get("title") or s.get("name") or "").lower()
        body = (s.get("text") or s.get("content") or "")
        # поступление
        if any(w in title for w in ("поступлен", "госпитал")):
            m = DATE_RE.search(body)
            if m:
                admit = parse_iso_any(m.group(0))
        # выписка
        if any(w in title for w in ("выпис", "заключ", "эпикриз")):
            m = DATE_RE.search(body)
            if m:
                discharge = parse_iso_any(m.group(0))
    return {"admit": admit, "discharge": discharge}


def build_timeline(doc_id: str) -> Dict[str, Any]:
    with get_conn() as conn:
        init_events_schema(conn)
        clear_events(conn, doc_id)

        sections = get_sections(conn, doc_id)
        entities = get_entities(conn, doc_id)

        # 1) события из сущностей (если они уже выделены экстракторами/NER)
        # ожидаемые поля: entity.etype, value_json (с ts/when)
        for e in entities:
            et = (e.get("etype") or "").lower()
            try:
                data = json.loads(e.get("value_json") or "{}")
            except Exception:
                data = {}
            # поддержим оба варианта: ts / when
            tsv = data.get("ts") or data.get("when")
            ts = parse_iso_any(tsv) if isinstance(tsv, str) else None

            # простое сопоставление: первичный осмотр, ежедневные записи, лаборатория и т.д.
            if et in ("exam_initial", "первичный осмотр"):
                add_event(conn, doc_id, "initial_exam", ts, data)
            elif et in ("discharge_summary", "эпикриз"):
                # сам по себе эпикриз — это не discharge, но зафиксируем момент
                add_event(conn, doc_id, "epicrisis", ts, data)
            elif et in ("med_order", "назначен", "лист назначений"):
                add_event(conn, doc_id, "med_order", ts, data)
            elif et in ("complaint", "symptom", "жалоб", "симптом"):
                add_event(conn, doc_id, "complaint", ts, data)
            elif et in ("isolation", "infection_control", "изоляция"):
                add_event(conn, doc_id, "infection_control", ts, data)

        # 2) попытка угадать admit/discharge из секций, если их нет
        ad = guess_admit_discharge_from_sections(sections)
        if ad.get("admit"):
            add_event(conn, doc_id, "admit", ad["admit"], {})
        if ad.get("discharge"):
            add_event(conn, doc_id, "discharge", ad["discharge"], {})

        conn.commit()
        # вернём краткую сводку
        cur = conn.execute("SELECT kind, ts FROM events WHERE doc_id=? ORDER BY ts", (doc_id,))
        events = [{"kind": k, "ts": t} for (k, t) in cur.fetchall()]
        return {"doc_id": doc_id, "events": events, "status": "timeline_built"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()
    print(json.dumps(build_timeline(args.doc_id), ensure_ascii=False))


if __name__ == "__main__":
    main()
