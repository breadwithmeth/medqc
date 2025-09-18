# -*- coding: utf-8 -*-
import os
import re
import json
import sqlite3
from datetime import datetime
from typing import List, Dict, Tuple, Optional

DB_PATH = os.getenv("MEDQC_DB", "/app/medqc.db")

# ====== базовые утилиты ======
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS artifacts(
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id     TEXT NOT NULL,
      kind       TEXT NOT NULL,
      content    TEXT,
      meta_json  TEXT,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS raw(
      doc_id     TEXT PRIMARY KEY,
      content    TEXT,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS entities(
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id      TEXT NOT NULL,
      etype       TEXT,
      ts          TEXT,
      span_start  INTEGER,
      span_end    INTEGER,
      value_json  TEXT,
      source      TEXT,
      confidence  REAL,
      created_at  TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS events(
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id     TEXT NOT NULL,
      kind       TEXT,
      ts         TEXT,
      payload    TEXT,
      created_at TEXT NOT NULL
    );
    """)
    conn.commit()

def read_full_text(conn: sqlite3.Connection, doc_id: str) -> str:
    r = conn.execute("SELECT content FROM raw WHERE doc_id=?", (doc_id,)).fetchone()
    if r and r["content"]:
        return r["content"]
    a = conn.execute("SELECT content FROM artifacts WHERE doc_id=? AND kind='text_pages'", (doc_id,)).fetchone()
    if a and a["content"]:
        try:
            pages = json.loads(a["content"])
            if isinstance(pages, list):
                return "\n\n".join(pages)
            return str(a["content"])
        except Exception:
            return str(a["content"])
    return ""

def to_iso(date_s: str, time_s: Optional[str]) -> Optional[str]:
    """
    Превращает строки вида '25.04.2025' и '14:05' в ISO.
    Поддерживает также '25-04-2025', '25/04/25', а время может отсутствовать.
    """
    if not date_s:
        return None
    date_s = date_s.strip()
    # нормализуем разделители
    ds = re.sub(r"[/-]", ".", date_s)
    parts = ds.split(".")
    if len(parts) < 3:
        return None
    d, m, y = parts[0], parts[1], parts[2]
    if len(y) == 2:
        y = "20" + y
    if len(d) == 1: d = "0" + d
    if len(m) == 1: m = "0" + m
    hh, mm = "00", "00"
    if time_s:
        tm = time_s.strip()
        mobj = re.match(r"^(\d{1,2}):(\d{2})", tm)
        if mobj:
            hh = mobj.group(1).zfill(2)
            mm = mobj.group(2).zfill(2)
    try:
        dt = datetime(int(y), int(m), int(d), int(hh), int(mm))
        return dt.isoformat()
    except Exception:
        return None

def insert_event(conn: sqlite3.Connection, doc_id: str, kind: str, ts: Optional[str], payload: dict):
    conn.execute(
        "INSERT INTO events(doc_id, kind, ts, payload, created_at) VALUES(?,?,?,?,datetime('now'))",
        (doc_id, kind, ts, json.dumps(payload, ensure_ascii=False))
    )

def insert_entity(conn: sqlite3.Connection, doc_id: str, etype: str, ts: Optional[str],
                  span: Tuple[int,int], value: dict, source="regex", confidence: float = 0.9):
    s0, s1 = (span or (None, None))
    conn.execute(
        """INSERT INTO entities(doc_id, etype, ts, span_start, span_end, value_json, source, confidence, created_at)
           VALUES(?,?,?,?,?,?,?, ?, datetime('now'))""",
        (doc_id, etype, ts, s0, s1, json.dumps(value, ensure_ascii=False), source, float(confidence))
    )

# ====== регэкспы и извлечение ======
# Общая дата/время:  dd.mm.yyyy (г.)? hh:mm
DT_RE = re.compile(
    r"(?P<date>\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4})(?:\s*[гГ]\.?)?(?:\s+|[,;]\s*)(?P<time>\d{1,2}:\d{2})?",
    flags=re.U | re.I
)

def find_first_dt(s: str) -> Optional[Tuple[str,str]]:
    m = DT_RE.search(s)
    if not m: return None
    return (m.group("date"), m.group("time"))

def extract_admit(text: str) -> List[Tuple[int,int,str,dict]]:
    """
    Поступление: ключевые фразы + ближайшая дата/время
    """
    out = []
    for m in re.finditer(r"(поступл\w+|госпитал\w+|дата\s+поступл\w+)", text, flags=re.I|re.U):
        a, b = m.span()
        ctx = text[max(0,a-120):min(len(text), b+120)]
        dt = find_first_dt(ctx)
        ts = to_iso(dt[0], dt[1]) if dt else None
        out.append((a,b, ts, {"context": ctx.strip()[:200]}))
    return out

def extract_discharge(text: str) -> List[Tuple[int,int,str,dict]]:
    out = []
    for m in re.finditer(r"(выписан\w+|выбы\w+|дата\s+выписк\w+)", text, flags=re.I|re.U):
        a,b = m.span()
        ctx = text[max(0,a-120):min(len(text), b+120)]
        dt = find_first_dt(ctx)
        ts = to_iso(dt[0], dt[1]) if dt else None
        out.append((a,b, ts, {"context": ctx.strip()[:200]}))
    return out

def extract_initial_exam(text: str) -> List[Tuple[int,int,str,dict]]:
    out = []
    for m in re.finditer(r"(первичн\w+\s+осмотр|осмотр\s+при\s+поступл\w+|осмотр\s+в\s+(при[ёе]мном|ПДО))", text, flags=re.I|re.U):
        a,b = m.span()
        ctx = text[max(0,a-120):min(len(text), b+160)]
        dt = find_first_dt(ctx)
        ts = to_iso(dt[0], dt[1]) if dt else None
        out.append((a,b, ts, {"context": ctx.strip()[:200]}))
    return out

def extract_triage(text: str) -> List[Tuple[int,int,str,dict]]:
    out = []
    for m in re.finditer(r"(триаж|сортиров\w+|ПДО|при[ёе]мн\w+\s+отделени\w+)", text, flags=re.I|re.U):
        a,b = m.span()
        ctx = text[max(0,a-120):min(len(text), b+140)]
        dt = find_first_dt(ctx)
        ts = to_iso(dt[0], dt[1]) if dt else None
        out.append((a,b, ts, {"context": ctx.strip()[:200]}))
    return out

def extract_daily_notes(text: str) -> List[Tuple[int,int,str,dict]]:
    """
    Ежедневные записи: строки, начинающиеся с ДД.ММ.ГГГГ( г.)? HH:MM и ключевых маркеров
    """
    out = []
    # грубый поиск строк с датой/временем + слова состояния
    for m in re.finditer(r"(^|\n)\s*(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})(?:\s*[гГ]\.?)?\s+(\d{1,2}:\d{2}).{0,40}?(жалоб|осмотр|состо[яи]н|температур|артериальн|сатурац)", text, flags=re.I|re.U):
        a,b = m.span()
        date_s = m.group(2); time_s = m.group(3)
        ts = to_iso(date_s, time_s)
        ctx = text[max(0,a):min(len(text), a+300)]
        out.append((a,b, ts, {"context": ctx.strip()}))
    return out

def extract_ecg(text: str) -> List[Tuple[int,int,str,dict]]:
    out = []
    for m in re.finditer(r"\bЭКГ\b", text, flags=re.I|re.U):
        a,b = m.span()
        ctx = text[max(0,a-80):min(len(text), b+120)]
        dt = find_first_dt(ctx)
        ts = to_iso(dt[0], dt[1]) if dt else None
        payload = {"test":"ECG","context": ctx.strip()[:200]}
        out.append((a,b, ts, payload))
    return out

def extract_labs(text: str) -> List[Tuple[int,int,str,dict]]:
    out = []
    lab_keys = [
        (r"\bОАК\b|\bобщ(ий|его)\s+анализ\s+крови\b", "CBC"),
        (r"\bбиохими\w+\b", "Biochem"),
        (r"\bСРБ\b|\bCRP\b", "CRP"),
        (r"\bкоагул\w+\b", "Coag")
    ]
    for rx, name in lab_keys:
        for m in re.finditer(rx, text, flags=re.I|re.U):
            a,b = m.span()
            ctx = text[max(0,a-80):min(len(text), b+140)]
            dt = find_first_dt(ctx)
            ts = to_iso(dt[0], dt[1]) if dt else None
            out.append((a,b, ts, {"test": name, "context": ctx.strip()[:200]}))
    return out

def extract_discharge_summary(text: str) -> List[Tuple[int,int,str,dict]]:
    out = []
    for m in re.finditer(r"(выписн\w+\s+эпикриз|эпикриз\s+выписн\w+)", text, flags=re.I|re.U):
        a,b = m.span()
        ctx = text[max(0,a-120):min(len(text), b+240)]
        dt = find_first_dt(ctx)
        ts = to_iso(dt[0], dt[1]) if dt else None
        out.append((a,b, ts, {"context": ctx.strip()[:300]}))
    return out

def extract_med_order(text: str) -> List[Tuple[int,int,str,dict]]:
    out = []
    for m in re.finditer(r"(лист\s+назначени\w+|назначено:|назначен[оы]\s+)", text, flags=re.I|re.U):
        a,b = m.span()
        ctx = text[max(0,a-60):min(len(text), b+300)]
        dt = find_first_dt(ctx)
        ts = to_iso(dt[0], dt[1]) if dt else None
        out.append((a,b, ts, {"context": ctx.strip()[:300]}))
    return out

# ====== основной процесс ======
def run_entities(doc_id: str) -> Dict[str, int]:
    conn = get_conn()
    ensure_schema(conn)

    full = read_full_text(conn, doc_id)
    if not full:
        conn.close()
        return {"doc_id": doc_id, "entities": 0, "events": 0}

    inserted_e = inserted_ev = 0

    # события
    for a,b,ts,payload in extract_admit(full):
        insert_event(conn, doc_id, "admit", ts, payload); inserted_ev += 1
    for a,b,ts,payload in extract_discharge(full):
        insert_event(conn, doc_id, "discharge", ts, payload); inserted_ev += 1
    for a,b,ts,payload in extract_initial_exam(full):
        insert_event(conn, doc_id, "initial_exam", ts, payload); inserted_ev += 1
    for a,b,ts,payload in extract_triage(full):
        insert_event(conn, doc_id, "triage", ts, payload); inserted_ev += 1
    # daily_note — дедуп по дате
    seen_dates = set()
    for a,b,ts,payload in extract_daily_notes(full):
        if ts:
            d = ts.split("T",1)[0]
            if d in seen_dates:  # один daily_note на дату достаточно
                continue
            seen_dates.add(d)
        insert_event(conn, doc_id, "daily_note", ts, payload); inserted_ev += 1
    for a,b,ts,payload in extract_ecg(full):
        insert_event(conn, doc_id, "ecg", ts, payload); inserted_ev += 1
    for a,b,ts,payload in extract_labs(full):
        insert_event(conn, doc_id, "lab", ts, payload); inserted_ev += 1

    # сущности
    for a,b,ts,payload in extract_discharge_summary(full):
        insert_entity(conn, doc_id, "discharge_summary", ts, (a,b), payload, source="regex", confidence=0.9); inserted_e += 1
    for a,b,ts,payload in extract_med_order(full):
        insert_entity(conn, doc_id, "med_order", ts, (a,b), payload, source="regex", confidence=0.8); inserted_e += 1

    conn.commit()
    conn.close()
    return {"doc_id": doc_id, "entities": inserted_e, "events": inserted_ev}

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Extract entities/events via regex from full text")
    parser.add_argument("--doc-id", required=True)
    args = parser.parse_args()
    print(json.dumps(run_entities(args.doc_id), ensure_ascii=False))

if __name__ == "__main__":
    main()
