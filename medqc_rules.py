#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

DB_PATH = os.getenv("MEDQC_DB", "./medqc.db")

# ---------- utils ----------

def _row_to_dict(cur, row) -> Dict[str, Any]:
    cols = [c[0] for c in cur.description]
    return {cols[i]: row[i] for i in range(len(cols))}

def _fetch_all(conn: sqlite3.Connection, sql: str, params=()) -> List[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return [_row_to_dict(cur, r) for r in rows]

def _fetch_one(conn: sqlite3.Connection, sql: str, params=()) -> Dict[str, Any]:
    cur = conn.execute(sql, params)
    r = cur.fetchone()
    return _row_to_dict(cur, r) if r else {}

def parse_iso_any(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M","%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%Y-%m-%d"):
        try: return datetime.strptime(s, fmt)
        except: pass
    try:
        t = s.replace("Z","")
        if "+" in t: t = t.split("+",1)[0]
        return datetime.fromisoformat(t)
    except: return None

def minutes_between(a: Optional[datetime], b: Optional[datetime]) -> Optional[int]:
    if not a or not b: return None
    return int(abs((b-a).total_seconds())//60)

def day_span(a: Optional[datetime], b: Optional[datetime]) -> Optional[int]:
    if not a or not b: return None
    return (b.date()-a.date()).days + 1

def norm_json(obj: Any) -> str:
    try: return json.dumps(obj, ensure_ascii=False, separators=(",",":"))
    except: return "{}"

# ---------- robust readers ----------

def get_doc(conn, doc_id: str) -> Dict[str, Any]:
    candidates = [
        ("SELECT * FROM docs WHERE doc_id=? LIMIT 1", (doc_id,)),
        ("SELECT * FROM docs WHERE id=? LIMIT 1", (doc_id,)),
    ]
    for sql, p in candidates:
        try:
            d = _fetch_one(conn, sql, p)
            if d: return d
        except: pass
    return {}

def get_sections(conn, doc_id: str) -> List[Dict[str, Any]]:
    for ordercol in ("start","idx","pos","rowid"):
        try:
            return _fetch_all(conn, f"SELECT * FROM sections WHERE doc_id=? ORDER BY {ordercol}", (doc_id,))
        except: continue
    return _fetch_all(conn, "SELECT * FROM sections WHERE doc_id=?", (doc_id,))

def get_entities(conn, doc_id: str) -> List[Dict[str, Any]]:
    return _fetch_all(conn, "SELECT * FROM entities WHERE doc_id=?", (doc_id,))

def get_events(conn, doc_id: str) -> List[Dict[str, Any]]:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()]
    ts_col = "ts" if "ts" in cols else ("when" if "when" in cols else None)
    order = f" ORDER BY {ts_col}" if ts_col else ""
    return _fetch_all(conn, f"SELECT * FROM events WHERE doc_id=?{order}", (doc_id,))

# ---------- profiles ----------

def infer_profiles(doc: Dict[str, Any], entities: List[Dict[str, Any]], events: List[Dict[str, Any]]) -> List[str]:
    profiles = set()
    admit = discharge = None

    def _ev_ts(ev):
        ts = ev.get("ts") or ev.get("when")
        return parse_iso_any(ts)

    for ev in events:
        k = (ev.get("kind") or "").lower()
        if k == "admit":
            admit = _ev_ts(ev)
        elif k == "discharge":
            discharge = _ev_ts(ev)

    if admit and discharge:
        profiles.add("DAY" if admit.date()==discharge.date() else "STA")
    elif admit:
        profiles.add("STA")
    if any((ev.get("kind") or "").lower()=="triage" for ev in events):
        profiles.add("ER")

    # pediatric / neonatal by age
    age_days = None
    for e in entities:
        if (e.get("etype") or "").lower()=="patient":
            try:
                info = json.loads(e.get("value_json") or "{}")
            except: info={}
            if isinstance(info.get("age_days"), int):
                age_days = info["age_days"]
            elif info.get("dob") and admit:
                dob = parse_iso_any(info["dob"])
                if dob: age_days = (admit.date() - dob.date()).days
            break
    if age_days is not None:
        if age_days <= 28: profiles.add("NEO")
        elif age_days < 18*365: profiles.add("PED")

    dept = (doc.get("dept") or doc.get("department") or "").lower()
    if "гинек" in dept or "род" in dept or "акуш" in dept: profiles.add("OBG")
    if "инфек" in dept: profiles.add("INF")
    if "карди" in dept: profiles.add("CAR")
    if "нефр" in dept: profiles.add("NEPH")
    if "ревмат" in dept: profiles.add("RHEUM")
    if "пульмон" in dept: profiles.add("PUL")
    if "урол" in dept or "андролог" in dept: profiles.add("URO")
    if "гастро" in dept or "гепат" in dept: profiles.add("GIH")
    if "нейрохи" in dept: profiles.add("NEURO")
    if "травмат" in dept or "ортопед" in dept: profiles.add("TRAUMA")
    if "онко" in dept and "дет" in dept: profiles.add("PONC")
    elif "онко" in dept: profiles.add("ONC")
    if not profiles: profiles.add("STA")
    return sorted(profiles)

# ---------- load rules ----------

def load_active_rules(conn: sqlite3.Connection, profiles: List[str],
                      pkg_name: Optional[str]=None, pkg_version: Optional[str]=None) -> List[Dict[str, Any]]:
    placeholders = ",".join(["?"]*len(profiles)) if profiles else "?"
    if pkg_name and pkg_version:
        pkg_filter = """
          COALESCE(r.package_name, p1.name, p2.name)=? AND
          COALESCE(r.package_version, p1.version, p2.version)=?
        """
        params = profiles + [pkg_name, pkg_version]
    else:
        pkg_filter = "COALESCE(p1.active, p2.active, 0)=1"
        params = profiles

    sql = f"""
    SELECT
      r.rule_id,
      r.profile, r.title, r.severity, r.enabled,
      COALESCE(r.params_json,'{{}}')         AS params_json,
      COALESCE(r.sources_json,'[]')          AS sources_json,
      COALESCE(r.package_name, p1.name, p2.name)     AS package_name,
      COALESCE(r.package_version, p1.version, p2.version) AS package_version
    FROM norm_rules r
    LEFT JOIN norm_packages p1 ON p1.pkg_id = r.pkg_id
    LEFT JOIN norm_packages p2 ON (p2.name = r.package_name AND p2.version = r.package_version)
    WHERE r.enabled=1
      AND r.profile IN ({placeholders})
      AND ({pkg_filter})
    ORDER BY r.profile, r.rule_id
    """
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]

# ---------- violations (адаптация к схеме) ----------

def _violation_columns(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("PRAGMA table_info(violations)")
    return [r[1] for r in cur.fetchall()]

def ensure_violations_table(conn: sqlite3.Connection):
    try:
        conn.execute("SELECT 1 FROM violations LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS violations(
          id INTEGER PRIMARY KEY,
          doc_id TEXT NOT NULL,
          rule_id TEXT NOT NULL,
          severity TEXT NOT NULL,
          message TEXT NOT NULL,
          evidence_json TEXT,
          sources_json TEXT,
          created_at TEXT NOT NULL
        );
        """)

def clear_violations(conn: sqlite3.Connection, doc_id: str):
    ensure_violations_table(conn)
    conn.execute("DELETE FROM violations WHERE doc_id=?", (doc_id,))

def insert_violation(conn: sqlite3.Connection, doc_id: str, rule_id: str, severity: Any, message: str,
                     profile: Optional[str], sources: Optional[List[Dict[str, Any]]],
                     extra: Optional[Dict[str, Any]] = None):
    """
    Гибкая вставка:
      - если есть колонка profile — пишем туда;
      - если есть extra_json — пишем туда;
      - иначе всё это уходит в evidence_json.
    """
    cols = _violation_columns(conn)
    ensure_violations_table(conn)

    sev = str(severity) if severity is not None else "minor"
    src = norm_json(sources or [])
    evidence_obj: Dict[str, Any] = {}

    # если в схеме нет profile/extra_json — положим в evidence_json
    use_profile_col = "profile" in cols
    use_extra_col = "extra_json" in cols
    use_evidence = "evidence_json" in cols

    if not use_profile_col:
        if profile:
            evidence_obj["profile"] = profile
    if not use_extra_col and extra:
        evidence_obj["extra"] = extra

    # Строим INSERT динамически
    insert_cols = ["doc_id", "rule_id", "severity", "message"]
    params = [doc_id, rule_id, sev, message]

    if use_profile_col:
        insert_cols.append("profile")
        params.append(profile or "")

    if use_evidence:
        # объединяем с существующим evidence
        insert_cols.append("evidence_json")
        params.append(norm_json(evidence_obj) if evidence_obj else None)

    if "sources_json" in cols:
        insert_cols.append("sources_json")
        params.append(src)

    if "created_at" in cols:
        # будем ставить через SQL-выражение
        created_at_expr = "datetime('now')"
        columns_sql = ", ".join(insert_cols + ["created_at"])
        placeholders = ", ".join(["?"] * len(params) + [created_at_expr])
        sql = f"INSERT INTO violations({columns_sql}) VALUES({placeholders})"
        conn.execute(sql, params)
    else:
        columns_sql = ", ".join(insert_cols)
        placeholders = ", ".join(["?"] * len(params))
        sql = f"INSERT INTO violations({columns_sql}) VALUES({placeholders})"
        conn.execute(sql, params)

# ---------- rule impls (сокращённый набор) ----------

def rule_require_daily_notes(rule, doc, sections, entities, events):
    v=[]
    def _ts(ev): return parse_iso_any(ev.get("ts") or ev.get("when"))
    admit=discharge=None
    for ev in events:
        k=(ev.get("kind") or "").lower()
        if k=="admit": admit=_ts(ev)
        elif k=="discharge": discharge=_ts(ev)
    if not admit or not discharge: return v
    dn=set()
    for ev in events:
        if (ev.get("kind") or "").lower()=="daily_note":
            ts=_ts(ev)
            if ts: dn.add(ts.date())
    miss=[]
    d = day_span(admit,discharge) or 0
    cur = admit.date()
    for _ in range(d):
        if cur not in dn: miss.append(str(cur))
        cur = cur + timedelta(days=1)
    if miss:
        v.append({"message":"Отсутствуют ежедневные записи: "+", ".join(miss),"severity":rule.get("severity","major")})
    return v

def rule_initial_exam_within_hours(rule, doc, sections, entities, events):
    v=[]
    try: params=json.loads(rule.get("params_json") or "{}")
    except: params={}
    hours=int(params.get("ПЕРВИЧНЫЙ_ОСМОТР_ЧАСОВ",6))
    def _ts(ev): return parse_iso_any(ev.get("ts") or ev.get("when"))
    admit_ts=init_ts=None
    for ev in events:
        k=(ev.get("kind") or "").lower()
        if k=="admit": admit_ts=_ts(ev)
        elif k in ("initial_exam","primary_exam"): init_ts=_ts(ev)
    if not init_ts:
        for e in entities:
            if (e.get("etype") or "").lower()=="exam_initial":
                try: data=json.loads(e.get("value_json") or "{}")
                except: data={}
                init_ts=parse_iso_any(data.get("ts") or data.get("when"))
                if init_ts: break
    if not admit_ts or not init_ts:
        v.append({"message":"Нет данных о первичном осмотре/поступлении","severity":rule.get("severity","major")})
        return v
    diff=minutes_between(admit_ts,init_ts)
    if diff is None or diff>hours*60:
        v.append({"message":f"Первичный осмотр с опозданием ({diff} мин; норма ≤ {hours*60})","severity":"critical"})
    return v

def rule_discharge_summary_on_discharge_date(rule, doc, sections, entities, events):
    v=[]; discharge=None
    for ev in events:
        if (ev.get("kind") or "").lower()=="discharge":
            discharge=parse_iso_any(ev.get("ts") or ev.get("when")); break
    if not discharge: return v
    epi_dates=set()
    for e in entities:
        if (e.get("etype") or "").lower() in ("discharge_summary","epicrisis"):
            try: data=json.loads(e.get("value_json") or "{}")
            except: data={}
            ts=parse_iso_any(data.get("ts") or data.get("when"))
            if ts: epi_dates.add(ts.date())
    if discharge.date() not in epi_dates:
        v.append({"message":"Выписной эпикриз не датирован днём выписки","severity":rule.get("severity","major")})
    return v

def rule_triage_within_minutes(rule, doc, sections, entities, events):
    v=[]
    try: params=json.loads(rule.get("params_json") or "{}")
    except: params={}
    limit=int(params.get("TRIAGE_MAX_MIN",15))
    def _ts(ev): return parse_iso_any(ev.get("ts") or ev.get("when"))
    admit_ts=triage_ts=None
    for ev in events:
        k=(ev.get("kind") or "").lower()
        if k=="admit": admit_ts=_ts(ev)
        elif k=="triage": triage_ts=_ts(ev)
    if not admit_ts or not triage_ts:
        v.append({"message":"Нет данных о времени триажа/поступления","severity":rule.get("severity","major")})
        return v
    d=minutes_between(admit_ts,triage_ts)
    if d is None or d>limit:
        v.append({"message":f"Триаж с опозданием ({d} мин; норма ≤ {limit})","severity":"critical"})
    return v

def _has_chest_pain(entities):
    for e in entities:
        if (e.get("etype") or "").lower() in ("complaint","symptom"):
            val=(e.get("value_json") or "").lower()
            if "боль в груд" in val or "загрудин" in val: return True
    return False

def rule_ecg_on_chest_pain(rule, doc, sections, entities, events):
    v=[]
    try: params=json.loads(rule.get("params_json") or "{}")
    except: params={}
    limit=int(params.get("ECG_MAX_MIN",10))
    if not _has_chest_pain(entities): return v
    def _ts(ev): return parse_iso_any(ev.get("ts") or ev.get("when"))
    admit_ts=ecg_ts=None
    for ev in events:
        k=(ev.get("kind") or "").lower()
        if k=="admit": admit_ts=_ts(ev)
        elif k=="ecg": ecg_ts=_ts(ev)
    if not admit_ts or not ecg_ts:
        v.append({"message":"Нет данных о времени ЭКГ/поступления","severity":rule.get("severity","major")})
        return v
    d=minutes_between(admit_ts,ecg_ts)
    if d is None or d>limit:
        v.append({"message":f"ЭКГ выполнена поздно ({d} мин; норма ≤ {limit})","severity":"critical"})
    return v

def rule_med_orders_attributes(rule, doc, sections, entities, events):
    v=[]; bad=0; total=0
    for e in entities:
        if (e.get("etype") or "").lower()=="med_order":
            total+=1
            try: x=json.loads(e.get("value_json") or "{}")
            except: x={}
            attrs = int(bool(x.get("dose"))) + int(bool(x.get("route"))) + int(bool(x.get("freq") or x.get("frequency")))
            if attrs<2: bad+=1
    if total>0 and bad>0:
        v.append({"message":f"Неполные назначения: {bad} из {total} (доза/путь/кратность)","severity":rule.get("severity","major")})
    return v

def rule_infection_isolation_present(rule, doc, sections, entities, events):
    v=[]; found=False
    for e in entities:
        et=(e.get("etype") or "").lower()
        if et in ("isolation","infection_control"): found=True; break
        if et in ("text_hint","note"):
            val=(e.get("value_json") or "").lower()
            if any(w in val for w in ["изоляц","бокс","контактная изоляция"]): found=True; break
    if not found:
        v.append({"message":"Нет отметки об изоляции/режиме инфекционной безопасности","severity":rule.get("severity","major")})
    return v

def rule_cbc_within_24h(rule, doc, sections, entities, events):
    v=[]; admit_ts=None
    for ev in events:
        if (ev.get("kind") or "").lower()=="admit":
            admit_ts=parse_iso_any(ev.get("ts") or ev.get("when")); break
    if not admit_ts: return v
    lab_ts=None
    for ev in events:
        if (ev.get("kind") or "").lower()=="lab":
            try: data=json.loads(ev.get("value_json") or "{}")
            except: data={}
            name=(data.get("name") or "").lower()
            if any(w in name for w in ["оак","общий анализ крови","cbc","hemogram","hemogramme"]):
                lab_ts=parse_iso_any(ev.get("ts") or ev.get("when")); break
    if not lab_ts:
        v.append({"message":"Нет ОАК в первые 24 часа","severity":"minor"}); return v
    d=minutes_between(admit_ts,lab_ts)
    if d is None or d>24*60:
        v.append({"message":"ОАК позже 24 часов","severity":"minor"})
    return v

RULE_IMPLS = {
    "STA-001": rule_require_daily_notes,
    "OBG-001": rule_require_daily_notes,
    "PED-001": rule_require_daily_notes,
    "RHEUM-001": rule_require_daily_notes,
    "PUL-001": rule_require_daily_notes,
    "GIH-001": rule_require_daily_notes,
    "NEPH-001": rule_require_daily_notes,
    "URO-001": rule_require_daily_notes,
    "TRAUMA-001": rule_require_daily_notes,
    "NEURO-001": rule_require_daily_notes,
    "HEM-001": rule_require_daily_notes,
    "ONC-001": rule_require_daily_notes,
    "PONC-001": rule_require_daily_notes,

    "STA-002": rule_initial_exam_within_hours,
    "OBG-002": rule_initial_exam_within_hours,
    "PED-002": rule_initial_exam_within_hours,
    "NEO-002": rule_initial_exam_within_hours,

    "STA-006": rule_med_orders_attributes,
    "INF-001": rule_infection_isolation_present,
    "STA-010": rule_discharge_summary_on_discharge_date,

    "ER-001": rule_triage_within_minutes,
    "ER-004": rule_ecg_on_chest_pain,
    "CAR-001": rule_ecg_on_chest_pain,

    "INF-010": rule_cbc_within_24h,
    "STA-020": rule_cbc_within_24h,
}

# ---------- run ----------

def run_rules(doc_id: str, pkg_name: Optional[str]=None, pkg_version: Optional[str]=None) -> Dict[str, Any]:
    with sqlite3.connect(DB_PATH) as conn:
        doc = get_doc(conn, doc_id)
        sections = get_sections(conn, doc_id)
        entities  = get_entities(conn, doc_id)
        events = get_events(conn, doc_id)

        profiles = infer_profiles(doc, entities, events)
        rules = load_active_rules(conn, profiles, pkg_name, pkg_version)

        clear_violations(conn, doc_id)
        stored=0
        for r in rules:
            rid = r["rule_id"]
            impl = RULE_IMPLS.get(rid)
            if not impl:
                continue
            viols = impl(r, doc, sections, entities, events) or []
            for v in viols:
                insert_violation(
                    conn, doc_id, rid,
                    v.get("severity") or r.get("severity") or "minor",
                    v.get("message") or r.get("title") or rid,
                    r.get("profile"),
                    json.loads(r.get("sources_json") or "[]"),
                    extra={"package":{"name": r.get("package_name"), "version": r.get("package_version")}}
                )
                stored += 1
        conn.commit()
        return {"doc_id": doc_id, "profiles": profiles, "rules": len(rules), "violations": stored}

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-id", required=True)
    ap.add_argument("--package-name", default=None)
    ap.add_argument("--package-version", default=None)
    a = ap.parse_args()
    print(json.dumps(run_rules(a.doc_id, a.package_name, a.package_version), ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
