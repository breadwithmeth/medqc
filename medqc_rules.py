#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
medqc-rules — проверка истории по событиям/сущностям.
Теперь поддерживает ДВА источника правил:
  1) --rules rules.json         (замороженный файл)
  2) --pkg <name> --version <v> (runtime из БД norm_packages/norm_rules)

Примеры:
  # из JSON
  python medqc_rules.py --doc-id KZ-... --rules compiled_rules.json

  # runtime из БД (взято из medqc-norms-admin)
  python medqc_rules.py --doc-id KZ-... --pkg rules-pack-stationary-er --version 2025-09-07
  # с фильтром по профилям и включением отключённых правил
  python medqc_rules.py --doc-id KZ-... --pkg rules-pack-stationary-er --version 2025-09-07 \
                        --profiles STA,ER --include-disabled
"""
from __future__ import annotations
import argparse, json, math, sqlite3
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import medqc_db as db  # используем db.connect(), get_doc/sections/entities/events

# ---------- схема для таблицы violations (создаём здесь, чтобы не трогать medqc_db) ----------
VIOLATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS violations (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id        TEXT NOT NULL,
  rule_id       TEXT NOT NULL,
  severity      TEXT NOT NULL,
  message       TEXT NOT NULL,
  evidence_json TEXT,
  sources_json  TEXT,
  created_at    TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);
CREATE INDEX IF NOT EXISTS idx_violations_doc ON violations(doc_id);
"""

# ---------- дефолтные правила (если --rules и --pkg не заданы) ----------
DEFAULT_RULES = {
  "schema_version": "1.0",
  "rules": [
    {"id": "STA-001", "profile": "STA", "title": "Ежедневные записи лечащего врача",
     "severity": "major", "sources": [{"order_no": "ҚР-ДСМ-27", "date": "2022-03-24"}]},
    {"id": "STA-002", "profile": "STA", "title": "Первичный осмотр при поступлении",
     "severity": "critical", "params": {"ПЕРВИЧНЫЙ_ОСМОТР_ЧАСОВ": 6},
     "sources": [{"order_no": "ҚР-ДСМ-27", "date": "2022-03-24"}]},
    {"id": "STA-006", "profile": "STA", "title": "Полнота атрибутов в листе назначений",
     "severity": "major", "sources": [{"order_no": "ҚР-ДСМ-27", "date": "2022-03-24"}]},
    {"id": "STA-010", "profile": "STA", "title": "Выписной эпикриз в день выписки",
     "severity": "critical", "sources": [{"order_no": "ҚР-ДСМ-27", "date": "2022-03-24"}]},
    {"id": "ER-001", "profile": "ER", "title": "Триаж при поступлении",
     "severity": "critical", "params": {"TRIAGE_MAX_MIN": 15},
     "sources": [{"order_no": "ҚР ДСМ-27", "date": "2021-04-02"}]},
    {"id": "ER-004", "profile": "ER", "title": "Боль в груди: ЭКГ в допустимые сроки",
     "severity": "critical", "params": {"ECG_MAX_MIN": 10},
     "sources": [{"order_no": "ҚР ДСМ-139", "date": "2021-12-31"}]},
  ]
}

# ---------- утилиты времени/дат ----------
def parse_iso_any(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s2 = s.replace("Z", "").replace(" ", "T")
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        return None

def day_span(start_dt: datetime, end_dt: datetime) -> List[date]:
    ds: List[date] = []
    d = start_dt.date()
    while d <= end_dt.date():
        ds.append(d)
        d = date.fromordinal(d.toordinal() + 1)
    return ds

# ---------- маленькие хелперы для sqlite3.Row ----------
def row_has(row, key: str) -> bool:
    try:
        return key in row.keys()
    except Exception:
        return False

def row_get(row, key: str):
    return row[key] if row_has(row, key) else None

def ev_ts(ev) -> Optional[datetime]:
    """Вернёт datetime из events-строки: сначала 'ts' (из БД), fallback 'when' (на будущее)."""
    return parse_iso_any(row_get(ev, "ts") or row_get(ev, "when"))

# ---------- помощник сборки нарушения ----------
def mk_violation(rule: Dict[str, Any], message: str, evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "rule_id": rule.get("id"),
        "severity": rule.get("severity", "minor"),
        "message": message,
        "evidence": evidence,
        "sources": rule.get("sources", []),
    }

# ---------- загрузка правил ИЗ БД (runtime) ----------
def load_rules_from_db(pkg_name: str, version: str,
                       profiles: Optional[List[str]] = None,
                       include_disabled: bool = False) -> Dict[str, Any] | Dict[str, Dict]:
    """
    Читает norm_packages/norm_rules и возвращает rules-словарь той же формы,
    что и JSON-файл: {'schema_version','package','version','generated_at','rules':[...]}.
    Требует, чтобы таблицы были созданы (через medqc-norms-admin init/import-*).
    """
    try:
        with db.connect() as c:
            pkg = c.execute("SELECT * FROM norm_packages WHERE name=? AND version=?",
                            (pkg_name, version)).fetchone()
    except sqlite3.OperationalError as e:
        return {"error": {"code": "NO_NORMS_SCHEMA",
                          "message": "Таблицы norm_packages/norm_rules не найдены. Выполните: medqc_norms_admin.py init",
                          "details": str(e)}}
    if not pkg:
        return {"error": {"code": "PKG_NOT_FOUND", "message": f"Пакет не найден: {pkg_name}@{version}"}}

    with db.connect() as c:
        if profiles:
            qmarks = ",".join(["?"] * len(profiles))
            rows = c.execute(
                f"""SELECT * FROM norm_rules WHERE pkg_id=? AND profile IN ({qmarks})
                    ORDER BY rule_id""",
                [pkg["pkg_id"], *profiles]
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM norm_rules WHERE pkg_id=? ORDER BY rule_id",
                             (pkg["pkg_id"],)).fetchall()

    out_rules: List[Dict[str, Any]] = []
    now = datetime.utcnow().date()
    for r in rows:
        if (not include_disabled) and int(r["enabled"]) == 0:
            continue
        eff_from = r["effective_from"]
        eff_to = r["effective_to"]
        # фильтрация по датам действия, если заданы
        if eff_from:
            try:
                if datetime.fromisoformat(eff_from.replace("Z", "").replace(" ", "T")).date() > now:
                    continue
            except Exception:
                pass
        if eff_to:
            try:
                if datetime.fromisoformat(eff_to.replace("Z", "").replace(" ", "T")).date() < now:
                    continue
            except Exception:
                pass
        item = {
            "id": r["rule_id"],
            "title": r["title"],
            "profile": r["profile"],
            "severity": r["severity"],
            "params": json.loads(r["params_json"]) if r["params_json"] else {},
            "sources": json.loads(r["sources_json"]) if r["sources_json"] else [],
        }
        if eff_from:
            item["effective_from"] = eff_from
        if eff_to:
            item["effective_to"] = eff_to
        out_rules.append(item)

    return {
        "schema_version": "1.0",
        "package": pkg_name,
        "version": version,
        "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "rules": out_rules
    }

# ---------- реализации правил ----------
def rule_STA_001(rule, doc, sections, entities, events) -> List[Dict[str, Any]]:
    v: List[Dict[str, Any]] = []
    admit = parse_iso_any(doc["admit_dt"]) if row_has(doc, "admit_dt") else None
    if not admit:
        return v

    last_ts = None
    for ev in events:
        ts = ev_ts(ev)
        if ts and (not last_ts or ts > last_ts):
            last_ts = ts
    if not last_ts:
        return v

    days = day_span(admit, last_ts)
    notes_by_day = {d: False for d in days}
    for ev in events:
        if row_get(ev, "kind") != "daily_note":
            continue
        ts = ev_ts(ev)
        if ts and ts.date() in notes_by_day:
            notes_by_day[ts.date()] = True

    missing = [d for d, ok in notes_by_day.items() if not ok]
    if missing:
        msg = "Нет ежедневных записей за даты: " + ", ".join(d.strftime("%d.%m.%Y") for d in missing)
        v.append(mk_violation(rule, msg, evidence=[{"kind": "daily_note_missing", "dates": [d.isoformat() for d in missing]}]))
    return v

def rule_STA_002(rule, doc, sections, entities, events) -> List[Dict[str, Any]]:
    v: List[Dict[str, Any]] = []
    admit = parse_iso_any(doc["admit_dt"]) if row_has(doc, "admit_dt") else None
    if not admit:
        return v
    init_exam_ts = None
    for ev in events:
        if row_get(ev, "kind") == "initial_exam":
            init_exam_ts = ev_ts(ev)
            if init_exam_ts:
                break
    if not init_exam_ts:
        v.append(mk_violation(rule, "Нет зафиксированного первичного осмотра при поступлении", evidence=[]))
        return v
    max_hours = rule.get("params", {}).get("ПЕРВИЧНЫЙ_ОСМОТР_ЧАСОВ", 6)
    delta = init_exam_ts - admit
    if delta.total_seconds() > max_hours * 3600:
        v.append(mk_violation(rule, f"Первичный осмотр оформлен поздно: +{delta}", evidence=[{"initial_exam": init_exam_ts.isoformat()}]))
    return v

def rule_STA_006(rule, doc, sections, entities, events) -> List[Dict[str, Any]]:
    v: List[Dict[str, Any]] = []
    bad: List[Dict[str, Any]] = []
    for e in entities:
        if row_get(e, "etype") != "medication":
            continue
        val = json.loads(row_get(e, "value_json")) if isinstance(row_get(e, "value_json"), str) else row_get(e, "value_json")
        dose = (val or {}).get("dose")
        route = (val or {}).get("route")
        freq = (val or {}).get("freq")
        have = sum(1 for x in (dose, route, freq) if x)
        if have < 2:
            bad.append({"section_id": row_get(e, "section_id"), "start": row_get(e, "start"), "end": row_get(e, "end"), "line": (val or {}).get("line")})
    if bad:
        v.append(mk_violation(rule, "В некоторых назначениях отсутствуют ≥2 обязательных атрибутов (доза/путь/кратность)", bad[:20]))
    return v

def rule_STA_010(rule, doc, sections, entities, events) -> List[Dict[str, Any]]:
    v: List[Dict[str, Any]] = []
    epi_ts = None
    for ev in events:
        if row_get(ev, "kind") == "epicrisis":
            epi_ts = ev_ts(ev)
            if epi_ts:
                break
    if not epi_ts:
        v.append(mk_violation(rule, "Отсутствует выписной эпикриз", evidence=[]))
        return v
    last_ts = None
    for ev in events:
        ts = ev_ts(ev)
        if ts and (not last_ts or ts > last_ts):
            last_ts = ts
    if last_ts and epi_ts.date() != last_ts.date():
        v.append(mk_violation(rule, "Эпикриз не датирован днём выписки (по последнему событию)", evidence=[{"epicrisis": epi_ts.date().isoformat(), "last_event": last_ts.date().isoformat()}]))
    return v

def rule_ER_001(rule, doc, sections, entities, events) -> List[Dict[str, Any]]:
    v: List[Dict[str, Any]] = []
    admit = parse_iso_any(doc["admit_dt"]) if row_has(doc, "admit_dt") else None
    triage_ts = None
    for ev in events:
        if row_get(ev, "kind") == "triage":
            triage_ts = ev_ts(ev)
            if triage_ts:
                break
    if not (admit and triage_ts):
        return v
    max_min = rule.get("params", {}).get("TRIAGE_MAX_MIN", 15)
    delta = triage_ts - admit
    if delta.total_seconds() > max_min * 60:
        v.append(mk_violation(rule, f"Триаж выполнен поздно: +{delta}", evidence=[{"admit": row_get(doc, "admit_dt")}, {"triage": triage_ts.isoformat()}]))
    return v

def rule_ER_004(rule, doc, sections, entities, events) -> List[Dict[str, Any]]:
    v: List[Dict[str, Any]] = []
    admit = parse_iso_any(row_get(doc, "admit_dt"))
    if not admit:
        return v
    ecg_ts = None
    for ev in events:
        if row_get(ev, "kind") == "ecg":
            t = ev_ts(ev)
            if t and (not ecg_ts or t < ecg_ts):
                ecg_ts = t
    if not ecg_ts:
        return v
    max_min = rule.get("params", {}).get("ECG_MAX_MIN", 10)
    delta = ecg_ts - admit
    if delta.total_seconds() > max_min * 60:
        v.append(mk_violation(rule, f"ЭКГ выполнено поздно: +{delta}", evidence=[{"admit": row_get(doc, "admit_dt")}, {"ecg": ecg_ts.isoformat()}]))
    return v

RULE_IMPLS = {
    "STA-001": rule_STA_001,
    "STA-002": rule_STA_002,
    "STA-006": rule_STA_006,
    "STA-010": rule_STA_010,
    "ER-001":  rule_ER_001,
    "ER-004":  rule_ER_004,
}

# ---------- основной процесс ----------
def ensure_violations_schema():
    with db.connect() as c:
        c.executescript(VIOLATIONS_SCHEMA)
        c.commit()

def write_violations(doc_id: str, violations: List[Dict[str, Any]]):
    ensure_violations_schema()
    with db.connect() as c:
        c.execute("DELETE FROM violations WHERE doc_id=?", (doc_id,))
        c.executemany(
            """
            INSERT INTO violations(doc_id, rule_id, severity, message, evidence_json, sources_json, created_at)
            VALUES(?,?,?,?,?,?,datetime('now'))
            """,
            (
                (
                    doc_id,
                    v.get("rule_id"),
                    v.get("severity", "minor"),
                    v.get("message", ""),
                    json.dumps(v.get("evidence", []), ensure_ascii=False),
                    json.dumps(v.get("sources", []), ensure_ascii=False),
                )
                for v in violations
            ),
        )
        c.commit()

def run_rules(doc_id: str, rules: Dict[str, Any]) -> Dict[str, Any]:
    db.init_schema()

    doc = db.get_doc(doc_id)
    if not doc:
        return {"error": {"code": "NO_DOC", "message": f"unknown doc_id {doc_id}"}}

    sections = db.get_sections(doc_id)
    if not sections:
        return {"error": {"code": "NO_SECTIONS", "message": "run medqc-section first"}}

    events = db.get_events(doc_id)
    if not events:
        return {"error": {"code": "NO_EVENTS", "message": "run medqc-timeline first"}}

    entities = db.get_entities(doc_id)

    violations: List[Dict[str, Any]] = []
    for rule in rules.get("rules", []):
        impl = RULE_IMPLS.get(rule.get("id"))
        if not impl:
            continue
        vlist = impl(rule, doc, sections, entities, events)
        for v in vlist:
            if (not v.get("sources")) and rule.get("sources"):
                v["sources"] = rule["sources"]
        violations.extend(vlist)

    write_violations(doc_id, violations)
    return {
        "doc_id": doc_id,
        "status": "rules",
        "violations": [{"rule_id": v["rule_id"], "severity": v["severity"], "message": v["message"]} for v in violations],
        "count": len(violations)
    }

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="medqc-rules — применение правил качества (JSON или runtime из БД)")
    ap.add_argument("--doc-id", required=True)
    # режим 1: файл JSON
    ap.add_argument("--rules", help="Путь к rules.json (замороженный файл)")
    # режим 2: чтение из БД
    ap.add_argument("--pkg", help="Имя пакета правил в БД (norm_packages.name)")
    ap.add_argument("--version", help="Версия пакета правил (norm_packages.version)")
    ap.add_argument("--profiles", help="Фильтр профилей, через запятую (например: STA,ER)")
    ap.add_argument("--include-disabled", action="store_true", help="Включать отключённые правила")
    args = ap.parse_args()

    # приоритет: файл > БД > дефолт
    if args.rules:
        with open(args.rules, "r", encoding="utf-8") as f:
            rules = json.load(f)
    elif args.pkg and args.version:
        profiles = [p.strip() for p in args.profiles.split(",")] if args.profiles else None
        rules = load_rules_from_db(args.pkg, args.version, profiles, args.include_disabled)
        if "error" in rules:
            print(json.dumps(rules, ensure_ascii=False, indent=2))
            return
    else:
        rules = DEFAULT_RULES

    res = run_rules(args.doc_id, rules)
    print(json.dumps(res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
