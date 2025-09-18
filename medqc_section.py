#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, re, sys
import medqc_db as db
import json
from medqc_db import get_full_text

SECTION_PATTERNS = [
    ("Поступление", r"\b(Поступление|Госпитализац(ия|ии)|Время поступления)\b", "admit", 90),
    ("Триаж", r"\b(Триаж|Triage|Категория приоритета)\b", "triage", 80),
    ("Осмотр при поступлении", r"\b(Осмотр при поступлении|Первичный осмотр)\b", "initial_exam", 80),
    ("Ежедневная запись", r"\b(Ежедневн(ая|ые) запись|Дневниковая запись)\b", "daily_note", 50),
    ("План лечения", r"\b(План лечения|План обследования|План ведения)\b", "plan", 60),
    ("Лист назначений", r"\b(Лист назначений|Назначения|Ордер(-| )сет)\b", "orders", 70),
    ("Показатели здоровья", r"\b(Показатели здоровья|Температурный лист|Витальные|T°|ЧСС|АД|SpO₂)\b", "vitals", 40),
    ("ЭКГ", r"\b(ЭКГ|ECG)\b", "ecg", 60),
    ("Эпикриз", r"\b(Эпикриз|Выписной эпикриз|Переводной эпикриз)\b", "epicrisis", 70),
]


def main():
    
    ap = argparse.ArgumentParser(description="medqc-section — секционирование")
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc-id", required=True)
    args = parser.parse_args()

    full = get_full_text(args.doc_id)
    if not full:
        print(json.dumps({"error":{"code":"NO_TEXT","message":"no text extracted"}}))
        return

    

    # Собрать кандидаты: (name, kind, start, priority)
    candidates = []
    for name, rx, kind, prio in SECTION_PATTERNS:
        for m in re.finditer(rx, full, flags=re.I):
            candidates.append((name, kind, m.start(), prio))

    candidates.sort(key=lambda x: (x[2], -x[3]))
    # Строим непересекающиеся секции по первому в позиции (max priority уже учли)
    final = []
    taken_positions = []
    for name, kind, start, prio in candidates:
        if any(abs(start - s) < 2 for s in taken_positions):
            continue
        taken_positions.append(start)
        final.append((name, kind, start))
    final.sort(key=lambda x: x[2])

    # Завершаем границы end по следующему старту
    sections_rows = []
    for i, (name, kind, start) in enumerate(final):
        end = final[i+1][2] if i+1 < len(final) else len(full)
        sections_rows.append({
            "section_id": f"S{i+1}",
            "name": name,
            "kind": kind,
            "start": start,
            "end": end,
            "pageno": None
        })

    db.replace_sections(args.doc_id, sections_rows)

    import json
    print(json.dumps({
        "doc_id": args.doc_id,
        "status": "sectioned",
        "sections": len(sections_rows)
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()