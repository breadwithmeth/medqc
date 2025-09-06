#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
medqc-timeline — строит ленту событий по sections+entities и пишет в БД (таблица events.ts).
"""
from __future__ import annotations
import argparse, re, json
from datetime import datetime
from typing import Optional, List, Dict, Any
import medqc_db as db

# Привязка типов секций к типам событий
SECTION_EVENT_MAP = {
    "admit": "admit",
    "triage": "triage",
    "initial_exam": "initial_exam",
    "daily_note": "daily_note",
    "ecg": "ecg",
    "epicrisis": "epicrisis",
}

# Порядок попыток парсинга даты/времени
DT_PARSE_ORDER = [
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%H:%M:%S",
    "%H:%M",
]

def parse_dt_any(s: str) -> Optional[datetime]:
    s = s.strip()
    for fmt in DT_PARSE_ORDER:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

# Дата/время в тексте: 21.08.2025[ HH:MM], 2025-08-21[ HH:MM], HH:MM[:SS]
DT_RE = re.compile(
    r"\b((?:\d{2}[.]){2}\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?|"
    r"\d{4}-\d{2}-\d{2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?|"
    r"\d{1,2}:\d{2}(?::\d{2})?)\b"
)

def pick_section_timestamp(full_text: str, start: int, end: int) -> Optional[str]:
    """Берём первый распознанный datetime внутри секции; возвращаем ISO без TZ (точность — до минут)."""
    chunk = full_text[start:end]
    for m in DT_RE.finditer(chunk):
        dt = parse_dt_any(m.group(1))
        if dt:
            return dt.isoformat(timespec="minutes")
    return None

def build_timeline(doc_id: str) -> Dict[str, Any]:
    db.init_schema()

    doc = db.get_doc(doc_id)
    if not doc:
        return {"error": {"code": "NO_DOC", "message": f"unknown doc_id {doc_id}"}}

    full = db.get_full_text(doc_id)
    if not full:
        return {"error": {"code": "NO_TEXT", "message": "run medqc-extract first"}}

    sections = db.get_sections(doc_id)
    ents = db.get_entities(doc_id)

    # Индексация всех дат/времени внутри каждой секции (для привязки лекарств/виталов)
    section_dt_list: Dict[str, List[tuple[int, int, str]]] = {}
    for s in sections:
        chunk = full[s["start"]:s["end"]]
        dts: List[tuple[int, int, str]] = []
        for m in DT_RE.finditer(chunk):
            dt = parse_dt_any(m.group(1))
            if dt:
                iso = dt.isoformat(timespec="minutes")
                dts.append((s["start"] + m.start(1), s["start"] + m.end(1), iso))
        section_dt_list[s["section_id"]] = dts

    events: List[Dict[str, Any]] = []

    # 1) События из самих секций (admit/triage/initial_exam/...):
    for s in sections:
        kind = SECTION_EVENT_MAP.get(s["kind"])
        if not kind:
            continue
        iso = pick_section_timestamp(full, s["start"], s["end"]) or (doc["admit_dt"] if kind == "admit" else None)
        events.append({
            "kind": kind,
            "when": iso,                 # db.replace_events сам положит в колонку ts
            "section_id": s["section_id"],
            "start": s["start"],
            "end": s["end"],
            "value": {"name": s["name"]}
        })

    # 2) Диагнозы → события diagnosis (время: первая дата секции или admit_dt)
    for e in ents:
        if e["etype"] != "diagnosis":
            continue
        val = json.loads(e["value_json"]) if isinstance(e["value_json"], str) else e["value_json"]
        sec_id = e["section_id"]
        sec_time = section_dt_list.get(sec_id, [None])
        ev_ts = sec_time[0][2] if (sec_time and sec_time[0]) else doc["admit_dt"]
        events.append({
            "kind": "diagnosis",
            "when": ev_ts,
            "section_id": sec_id,
            "start": e["start"],
            "end": e["end"],
            "value": val
        })

    # Вспомогательная: ближайшее время в секции
    def nearest_dt_iso(sec_id: Optional[str], pos: int) -> Optional[str]:
        if not sec_id:
            return None
        cands = section_dt_list.get(sec_id) or []
        if not cands:
            return None
        i = min(range(len(cands)), key=lambda k: abs(cands[k][0] - pos))
        return cands[i][2]

    # 3) Медикаменты → events.medication (время: ближайший datetime внутри секции, если есть)
    for e in ents:
        if e["etype"] != "medication":
            continue
        val = json.loads(e["value_json"]) if isinstance(e["value_json"], str) else e["value_json"]
        ts = nearest_dt_iso(e["section_id"], e["start"])
        events.append({
            "kind": "medication",
            "when": ts,
            "section_id": e["section_id"],
            "start": e["start"],
            "end": e["end"],
            "value": val
        })

    # 4) Витальные → events.vital (аналогично)
    for e in ents:
        if e["etype"] != "vital":
            continue
        val = json.loads(e["value_json"]) if isinstance(e["value_json"], str) else e["value_json"]
        ts = nearest_dt_iso(e["section_id"], e["start"])
        events.append({
            "kind": "vital",
            "when": ts,
            "section_id": e["section_id"],
            "start": e["start"],
            "end": e["end"],
            "value": val
        })

    # Сортировка и запись
    def sort_key(ev: Dict[str, Any]):
        return (ev.get("when") is None, ev.get("when") or "9999-12-31T00:00")

    events.sort(key=sort_key)
    db.replace_events(doc_id, events)

    return {"doc_id": doc_id, "status": "timeline", "events": len(events)}

def main():
    ap = argparse.ArgumentParser(description="medqc-timeline — построение ленты событий")
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()

    res = build_timeline(args.doc_id)
    print(json.dumps(res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
