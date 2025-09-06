#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, re, json, sys
import medqc_db as db

# Дата/время: 21.08.2025, 21.08.2025 11:51, 11:51, 2025-08-21, 2025-08-21 11:51
DATE_RE = r"(?:(?:\d{2}[.]){2}\d{4}|\d{4}-\d{2}-\d{2})"
TIME_RE = r"\d{1,2}:\d{2}(?::\d{2})?"
DT_RE   = re.compile(rf"\b({DATE_RE}(?:\s+{TIME_RE})?|{TIME_RE})\b")

ICD_RE  = re.compile(r"\b([A-Z][0-9]{2}(?:\.[0-9A-Z]{1,4})?)\b")

DOSE_RE = re.compile(r"\b(\d+[\.,]?\d*)\s*(мг|г|мл|ЕД|IU|%)\b", re.I)
ROUTE_RE= re.compile(r"\b(в/в|в/м|п/о|перорально|сублингв|sublingual|п/к|ингаляционно)\b", re.I)
FREQ_RE = re.compile(r"\b((?:\d+\s*раз/сут)|(?:\d+\s*р/д)|(?:каждые\s*\d+\s*ч)|(?:q\d+h))\b", re.I)

TEMP_RE = re.compile(r"(?:T|Т|Температура)[^\n]{0,20}?(\d{1,2}[\.,]\d)")
BP_RE   = re.compile(r"(\d{2,3})\s*/\s*(\d{2,3})\s*(?:мм\s*рт\.?\s*ст\.?|mmHg)?", re.I)
SPO2_RE = re.compile(r"\b(SpO2|SpO₂)\s*[:=]?\s*(\d{2,3})\s*%\b", re.I)

SECTION_KIND_MAP = {
  "orders": "Лист назначений",
  "vitals": "Показатели здоровья",
  "initial_exam": "Осмотр при поступлении",
}


def main():
    ap = argparse.ArgumentParser(description="medqc-entities — извлечение сущностей")
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()

    full = db.get_full_text(args.doc_id)
    if full is None:
        print(json.dumps({"error":{"code":"NO_TEXT","message":"run medqc-extract first"}}))
        sys.exit(1)

    sections = db.get_sections(args.doc_id)
    entities = []

    # 1) Дата/время — по всем секциям
    for s in sections:
        chunk = full[s["start"]:s["end"]]
        for m in DT_RE.finditer(chunk):
            start = s["start"] + m.start(1)
            end   = s["start"] + m.end(1)
            entities.append({
                "section_id": s["section_id"],
                "etype": "datetime",
                "start": start,
                "end": end,
                "value": {"raw": m.group(1)}
            })

    # 2) Диагнозы и коды МКБ — ищем по ключам и ICD-формату
    diag_keys = re.compile(r"\b(диагноз|заключительный диагноз|клинический диагноз)\b", re.I)
    for s in sections:
        chunk = full[s["start"]:s["end"]]
        if diag_keys.search(chunk) or ICD_RE.search(chunk):
            for m in ICD_RE.finditer(chunk):
                start = s["start"] + m.start(1)
                end   = s["start"] + m.end(1)
                entities.append({
                    "section_id": s["section_id"],
                    "etype": "diagnosis",
                    "start": start,
                    "end": end,
                    "value": {"icd": m.group(1)}
                })

    # 3) Назначения — разбираем построчно в секциях orders
    for s in sections:
        if s["kind"] != "orders":
            continue
        chunk = full[s["start"]:s["end"]]
        for line_m in re.finditer(r"[^\n]+", chunk):
            line = line_m.group(0).strip()
            if len(line) < 5:
                continue
            dose = DOSE_RE.search(line)
            route= ROUTE_RE.search(line)
            freq = FREQ_RE.search(line)
            if dose or route or freq:
                entities.append({
                    "section_id": s["section_id"],
                    "etype": "medication",
                    "start": s["start"] + line_m.start(0),
                    "end":   s["start"] + line_m.end(0),
                    "value": {
                        "line": line,
                        "dose": (dose.group(1).replace(',', '.') + " " + dose.group(2)) if dose else None,
                        "route": route.group(1) if route else None,
                        "freq": freq.group(1) if freq else None
                    }
                })

    # 4) Виталы (T, АД, SpO2) — секция vitals + весь документ на всякий
    def scan_vitals(text: str, offset: int, section_id: str):
        for m in TEMP_RE.finditer(text):
            entities.append({
                "section_id": section_id,
                "etype": "vital",
                "start": offset + m.start(1),
                "end":   offset + m.end(1),
                "value": {"kind": "temperature", "value": float(m.group(1).replace(',', '.')), "unit": "C"}
            })
        for m in BP_RE.finditer(text):
            entities.append({
                "section_id": section_id,
                "etype": "vital",
                "start": offset + m.start(1),
                "end":   offset + m.end(2),
                "value": {"kind": "blood_pressure", "syst": int(m.group(1)), "diast": int(m.group(2)), "unit": "mmHg"}
            })
        for m in SPO2_RE.finditer(text):
            entities.append({
                "section_id": section_id,
                "etype": "vital",
                "start": offset + m.start(2),
                "end":   offset + m.end(2),
                "value": {"kind": "spo2", "value": int(m.group(2)), "unit": "%"}
            })

    for s in sections:
        chunk = full[s["start"]:s["end"]]
        scan_vitals(chunk, s["start"], s["section_id"])

    # Запись в БД и вывод
    db.replace_entities(args.doc_id, entities)
    print(json.dumps({
        "doc_id": args.doc_id,
        "status": "entities",
        "entities": len(entities)
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()