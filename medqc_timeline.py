# -*- coding: utf-8 -*-
import os
import json
import sqlite3
from datetime import datetime
from typing import Dict

DB_PATH = os.getenv("MEDQC_DB", "/app/medqc.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
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

# простая нормализация kind (на случай старых пайпов)
def normalize_kind(k: str) -> str:
    t = (k or "").lower()
    syn = {
        "admit": ["admit","поступ","госпитал"],
        "discharge": ["discharge","выписк","выбыт"],
        "daily_note": ["daily_note","ежеднев","осмотр","жалоб","состояни"],
        "triage": ["triage","триаж","сортиров","ПДО","приёмн","приемн"],
        "ecg": ["ecg","экг"],
        "lab": ["lab","анализ","лаборат","оак","общий анализ крови","биохим","crp","срб"],
        "initial_exam": ["initial_exam","первичн","осмотр при поступ"]
    }
    for canon, keys in syn.items():
        if any(x.lower() in t for x in keys):
            return canon
    return t

def run_timeline(doc_id: str) -> Dict[str,int]:
    conn = get_conn()
    ensure_schema(conn)

    rows = conn.execute("SELECT id, kind, ts, payload FROM events WHERE doc_id=?", (doc_id,)).fetchall()
    if not rows:
        conn.close()
        return {"doc_id": doc_id, "normalized": 0}

    changed = 0
    for r in rows:
        kid = r["id"]
        k = normalize_kind(r["kind"])
        ts = r["ts"]
        # если ts пусто и в payload есть подсказки — можно добавить, но оставим консервативно
        if k != r["kind"]:
            conn.execute("UPDATE events SET kind=? WHERE id=?", (k, kid))
            changed += 1
        # очистим payload от мусора типа слишком длинного контекста (необязательно)
        # p = json.loads(r["payload"] or "{}")
        # if "context" in p and len(p["context"]) > 1000: p["context"] = p["context"][:1000]
        # conn.execute("UPDATE events SET payload=? WHERE id=?", (json.dumps(p, ensure_ascii=False), kid))

    conn.commit()
    conn.close()
    return {"doc_id": doc_id, "normalized": changed}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc-id", required=True)
    args = parser.parse_args()
    print(json.dumps(run_timeline(args.doc_id), ensure_ascii=False))

if __name__ == "__main__":
    main()
