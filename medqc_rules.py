import os
import re
import json
import sqlite3
from datetime import datetime, timedelta
from typing import List

# ====== НОРМАЛИЗАЦИЯ РУССКИХ СИНОНИМОВ ======
RU_EVENT_SYNONYMS = {
    "admit":       ["admit","поступ","госпитал"],
    "discharge":   ["discharge","выписк"],
    "daily_note":  ["daily_note","ежеднев"],
    "triage":      ["triage","сортиров"],
    "ecg":         ["ecg","экг"],
    "lab":         ["lab","анализ","лаборат"],
    "initial_exam":["initial_exam","первичн","осмотр при поступ"]
}

RU_ENTITY_SYNONYMS = {
    "exam_initial":      ["exam_initial","первичн","осмотр при поступ"],
    "discharge_summary": ["discharge_summary","выписной","эпикриз"],
    "med_order":         ["med_order","назначен","лист назначений"],
    "complaint":         ["complaint","жалоб"],
    "symptom":           ["symptom","симптом"]
}

def _normalize_kind(kind: str) -> str:
    k = (kind or "").lower()
    for canon, syns in RU_EVENT_SYNONYMS.items():
        if any(s in k for s in syns):
            return canon
    return k

def _normalize_etype(etype: str) -> str:
    t = (etype or "").lower()
    for canon, syns in RU_ENTITY_SYNONYMS.items():
        if any(s in t for s in syns):
            return canon
    return t

# ====== УТИЛИТЫ ======
def parse_iso_any(s: str):
    if not s: return None
    try:
        # поддержим короткие ISO
        return datetime.fromisoformat(s.replace("Z","").replace("z",""))
    except Exception:
        return None

def get_doc(conn: sqlite3.Connection, doc_id: str):
    conn.row_factory = sqlite3.Row
    return conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()

def get_sections(conn: sqlite3.Connection, doc_id: str):
    conn.row_factory = sqlite3.Row
    return conn.execute("SELECT * FROM sections WHERE doc_id=? ORDER BY start", (doc_id,)).fetchall()

def get_entities(conn: sqlite3.Connection, doc_id: str):
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM entities WHERE doc_id=?", (doc_id,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["value"] = json.loads(d.get("value_json") or "{}")
        except Exception:
            d["value"] = {}
        d["etype"] = _normalize_etype(d.get("etype",""))
        out.append(d)
    return out

def get_events(conn: sqlite3.Connection, doc_id: str):
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM events WHERE doc_id=?", (doc_id,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["kind"] = _normalize_kind(d.get("kind",""))
        out.append(d)
    return out

def infer_profiles(doc_row, entities, events):
    """
    Простая эвристика профилей:
    - ER если есть triage
    - DAY если admit/discharge в один день
    - Иначе STA
    - Дополнительно по dept/department подмешиваем спец-профили (INF, PED, NEURO, NEPH, PUL, GIH и т.д.)
    """
    profiles = set()
    kinds = {e.get("kind") for e in events}
    if "triage" in kinds:
        profiles.add("ER")

    # admit/discharge в один день?
    admit_ts = None
    discharge_ts = None
    for e in events:
        if e.get("kind") == "admit":
            admit_ts = parse_iso_any(e.get("ts") or e.get("when"))
        elif e.get("kind") == "discharge":
            discharge_ts = parse_iso_any(e.get("ts") or e.get("when"))
    if admit_ts and discharge_ts and admit_ts.date() == discharge_ts.date():
        profiles.add("DAY")

    # базовый — стационар
    profiles.add("STA")

    dept = (doc_row.get("dept") or doc_row.get("department") or "").lower()
    mapping = {
        "инфек": "INF",
        "педиатр": "PED",
        "кардиол": "CAR",
        "нейрохир": "NEUROSURG",
        "нейро": "NEURO",
        "нефро": "NEPH",
        "пульмон": "PUL",
        "ревмат": "RHEUM",
        "уролог": "URO",
        "гастро": "GIH",
        "гепат": "GIH",
        "онко": "ONC",
        "акуш": "OBG",
        "гинек": "OBG",
        "травм": "TRAUMA",
        "хирург": "SURG",
        "неонат": "NEO",
        "дневн": "DAY"
    }
    for key, prof in mapping.items():
        if key in dept:
            profiles.add(prof)

    return sorted(profiles)

def load_active_rules(conn: sqlite3.Connection, profiles: List[str],
                      package_name: str = None, package_version: str = None):
    """
    Загружает активные правила для заданных профилей.
    Приоритет: явные package_name/version → иначе активный пакет из norm_packages.active=1.
    """
    conn.row_factory = sqlite3.Row
    prof_in = ",".join(["?"] * len(profiles)) if profiles else "?"
    params = list(profiles or ["STA"])

    if package_name and package_version:
        sql = f"""
        SELECT r.*
        FROM norm_rules r
        WHERE r.enabled=1
          AND r.profile IN ({prof_in})
          AND (
                (r.package_name=? AND r.package_version=?)
                OR r.pkg_id IN (
                    SELECT pkg_id FROM norm_packages
                    WHERE name=? AND version=?
                )
              )
        ORDER BY r.rule_id
        """
        params += [package_name, package_version, package_name, package_version]
        rows = conn.execute(sql, params).fetchall()
    else:
        sql = f"""
        SELECT r.*
        FROM norm_rules r
        JOIN norm_packages p
          ON (p.pkg_id=r.pkg_id)
        WHERE r.enabled=1
          AND p.active=1
          AND r.profile IN ({prof_in})
        ORDER BY r.rule_id
        """
        rows = conn.execute(sql, params).fetchall()

    return [dict(r) for r in rows]

def insert_violation(conn: sqlite3.Connection, doc_id: str, rule_id: str,
                     sev: str, message: str, sources: list = None, evidence: dict = None):
    conn.execute(
        """INSERT INTO violations(doc_id, rule_id, severity, message, evidence_json, sources_json, created_at)
           VALUES(?,?,?,?,?, ?, datetime('now'))""",
        (doc_id, rule_id, sev, message,
         json.dumps(evidence or {}, ensure_ascii=False),
         json.dumps(sources or [], ensure_ascii=False))
    )

# ====== ПРИМЕРЫ ПРАВИЛ (логика-«ядро» под основные кейсы) ======
def rule_STA_001(doc, sections, entities, events, params):
    """Ежедневные записи лечащего врача (стационар) — проверяем наличие daily_note за каждый день между admit и discharge."""
    doc_id = doc["doc_id"]
    admit = discharge = None
    for ev in events:
        if ev["kind"] == "admit":
            admit = parse_iso_any(ev.get("ts") or ev.get("when"))
        if ev["kind"] == "discharge":
            discharge = parse_iso_any(ev.get("ts") or ev.get("when"))
    if not admit or not discharge:
        return []
    days = (discharge.date() - admit.date()).days + 1
    if days <= 1:
        return []
    # соберём дни, по которым есть daily_note
    notes = set()
    for ev in events:
        if ev["kind"] == "daily_note":
            ts = parse_iso_any(ev.get("ts") or ev.get("when"))
            if ts:
                notes.add(ts.date())
    missing = []
    for i in range(days):
        d = admit.date() + timedelta(days=i)
        if d not in notes:
            missing.append(str(d))
    violations = []
    if missing:
        violations.append(("STA-001","major", f"Нет ежедневных записей за дни: {', '.join(missing[:5])}" + (" ..." if len(missing)>5 else "")))
    return violations

def rule_STA_002(doc, sections, entities, events, params):
    """Первичный осмотр ≤ N часов от поступления (default 6h)."""
    limit_h = int(params.get("within_hours", 6)) if isinstance(params, dict) else 6
    admit_ts = init_ts = None
    for ev in events:
        k = ev["kind"]
        if k == "admit": admit_ts = parse_iso_any(ev.get("ts") or ev.get("when"))
        if k in ("initial_exam","exam_initial"): init_ts = parse_iso_any(ev.get("ts") or ev.get("when"))
    if admit_ts and init_ts:
        delta = (init_ts - admit_ts).total_seconds() / 3600.0
        if delta > limit_h:
            return [("STA-002","critical", f"Первичный осмотр через {delta:.1f} ч (> {limit_h} ч).")]
    else:
        return [("STA-002","critical", "Нет данных о первичном осмотре или моменте поступления.")]
    return []

def rule_STA_010(doc, sections, entities, events, params):
    """Выписной эпикриз в день выписки."""
    doc_id = doc["doc_id"]
    dch = None
    for ev in events:
        if ev["kind"] == "discharge":
            dch = parse_iso_any(ev.get("ts") or ev.get("when"))
    if not dch:
        return []
    has = False
    for ent in entities:
        if ent["etype"] == "discharge_summary":
            ts = parse_iso_any(ent.get("ts") or ent.get("when"))
            if ts and ts.date() == dch.date():
                has = True
                break
    if not has:
        return [("STA-010","major","Выписной эпикриз не оформлен в день выписки.")]
    return []

def rule_DAY_001(doc, sections, entities, events, params):
    """ДС: запись в день визита."""
    admit_ts = discharge_ts = None
    for ev in events:
        if ev["kind"] == "admit":
            admit_ts = parse_iso_any(ev.get("ts") or ev.get("when"))
        if ev["kind"] == "discharge":
            discharge_ts = parse_iso_any(ev.get("ts") or ev.get("when"))
    if not admit_ts or not discharge_ts:
        return []
    if admit_ts.date() != discharge_ts.date():
        return []
    has = any(ev["kind"] == "daily_note" and parse_iso_any(ev.get("ts") or ev.get("when")).date() == admit_ts.date() for ev in events if ev.get("ts") or ev.get("when"))
    if not has:
        return [("DAY-001","major","Нет записи врача в день посещения (ДС).")]
    return []

def rule_ER_001(doc, sections, entities, events, params):
    """ER: триаж ≤ 15 минут от поступления."""
    limit_min = int(params.get("within_minutes", 15)) if isinstance(params, dict) else 15
    admit = triage = None
    for ev in events:
        if ev["kind"] == "admit": admit = parse_iso_any(ev.get("ts") or ev.get("when"))
        if ev["kind"] == "triage": triage = parse_iso_any(ev.get("ts") or ev.get("when"))
    if not admit or not triage:
        return [("ER-001","critical","Нет данных triage или момента поступления.")]
    delta = (triage - admit).total_seconds() / 60.0
    if delta > limit_min:
        return [("ER-001","critical", f"Триаж через {delta:.0f} мин (> {limit_min} мин).")]
    return []

# маппинг rule_id → функция
RULE_IMPL = {
    "STA-001": rule_STA_001,
    "STA-002": rule_STA_002,
    "STA-010": rule_STA_010,
    "DAY-001": rule_DAY_001,
    "ER-001":  rule_ER_001,
    # при желании дополняйте остальные (URO-001, PUL-001 и т.д.) однотипной логикой
}

def run_rules(doc_id: str, package_name: str = None, package_version: str = None):
    db = os.getenv("MEDQC_DB", "/app/medqc.db")
    conn = sqlite3.connect(db); conn.row_factory = sqlite3.Row

    doc = get_doc(conn, doc_id)
    if not doc:
        conn.close()
        return {"error": {"code": "DOC_NOT_FOUND", "message": f"doc_id={doc_id}"}}

    sections = get_sections(conn, doc_id)
    entities = get_entities(conn, doc_id)
    events   = get_events(conn, doc_id)

    profiles = infer_profiles(dict(doc), entities, events)
    rules = load_active_rules(conn, profiles, package_name, package_version)

    total = 0
    for r in rules:
        rid = r.get("rule_id")
        impl = RULE_IMPL.get(rid)
        params = {}
        try:
            params = json.loads(r.get("params_json") or "{}")
        except Exception:
            params = {}
        if not impl:
            # нет явной реализации — пропускаем тихо (можно логировать)
            continue
        try:
            vlist = impl(dict(doc), sections, entities, events, params)
        except Exception as ex:
            vlist = [ (rid, str(r.get("severity","minor")), f"Ошибка исполнения правила: {ex}") ]
        for (rule_id, severity, message) in vlist or []:
            insert_violation(conn, doc_id, rule_id, severity, message, sources=[{"rule_id": rid}])
            total += 1

    conn.commit(); conn.close()
    return {"doc_id": doc_id, "profiles": profiles, "rules_checked": len(rules), "violations": total}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc-id", required=True)
    parser.add_argument("--package-name", default=os.getenv("DEFAULT_RULES_PACKAGE"))
    parser.add_argument("--package-version", default=os.getenv("DEFAULT_RULES_VERSION"))
    a = parser.parse_args()
    res = run_rules(a.doc_id, a.package_name, a.package_version)
    print(json.dumps(res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
