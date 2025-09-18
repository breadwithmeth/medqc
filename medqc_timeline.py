#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import sqlite3
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from medqc_db import (
    get_conn,
    get_sections,
    get_entities,
    init_events_schema,
    clear_events,
    add_event,
)

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
    try:
        t = s.replace("Z", "")
        if "+" in t:
            t = t.split("+", 1)[0]
        return datetime.fromisoformat(t)
    except Exception:
        return None

DATE_RE = re.compile(r"(?:(?:20|19)\d{2})[-./](?:0?[1-9]|1[0-2])[-./](?:0?[1-9]|[12]\d|3[01])(?:[ T](?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d)?)?")

def guess_admit_discharge_from_sections(sections: List[Dict[str, Any]]) -> Dict[str, Optional[datetime]]:
    admit = None
    discharge = None
    for s in sections:
        title = (s.get("title") or s.get("name") or "").lower()
        body = (s.get("text") or s.get("content") or "")
        if any(w in title for w in ("поступлен", "госпитал")):
            m = DATE_RE.search(body)
            if m:
                admit = parse_iso_any(m.group(0))
        if any(w in title for w in ("выпис", "заключ", "эпикриз")):
            m = DATE_RE.search(body)
            if m:
                discharge = parse_iso_any(m.group(0))
    return {"admit": admit, "discharge": discharge}

def build_timeline(doc_id: str) -> Dict[str, Any]:
    with get_conn() as conn:
        init_events_schema(conn)
        clear_events(conn, doc_id)

        sections = get_sections(conn, doc_id)  # универсальная сигнатура
        entities = get_entities(conn, doc_id)

        # 1) события из сущностей (если уже размечены)
        for e in entities:
            et = (e.get("etype") or "").lower()
            try:
                data = json.loads(e.get("value_json") or "{}")
            except Exception:
                data = {}
            tsv = data.get("ts") or data.get("when")
            ts = parse_iso_any(tsv) if isinstance(tsv, str) else None
            ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S") if ts else None

            if et in ("exam_initial", "первичный осмотр"):
                add_event(conn, doc_id, "initial_exam", ts_str, data)
            elif et in ("discharge_summary", "эпикриз"):
                add_event(conn, doc_id, "epicrisis", ts_str, data)
            elif et in ("med_order", "назначен", "лист назначений"):
                add_event(conn, doc_id, "med_order", ts_str, data)
            elif et in ("complaint", "symptom", "жалоб", "симптом"):
                add_event(conn, doc_id, "complaint", ts_str, data)
            elif et in ("isolation", "infection_control", "изоляция"):
                add_event(conn, doc_id, "infection_control", ts_str, data)

        # 2) эвристика по секциям
        ad = guess_admit_discharge_from_sections(sections)
        if ad.get("admit"):
            add_event(conn, doc_id, "admit", ad["admit"].strftime("%Y-%m-%dT%H:%M:%S"), {})
        if ad.get("discharge"):
            add_event(conn, doc_id, "discharge", ad["discharge"].strftime("%Y-%m-%dT%H:%M:%S"), {})

        conn.commit()
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
