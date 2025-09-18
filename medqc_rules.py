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


# ===================== BEGIN: AUTO-APPENDED DETAILED RULES =====================
# Помощники для новых правил

def _has_text_keywords(sections, keywords):
    """Ищем любые из keywords в тексте секций (title/text)."""
    if not sections: return False
    keys = [k.lower() for k in keywords]
    for s in sections:
        t = f"{(s.get('title') or '')} {(s.get('text') or '')}".lower()
        if any(k in t for k in keys):
            return True
    return False

def _find_section(sections, kw):
    """Возвращает секцию, содержащую ключевое слово kw (в title/text)."""
    if not sections: return None
    kw = kw.lower()
    for s in sections:
        t = f"{(s.get('title') or '')} {(s.get('text') or '')}".lower()
        if kw in t:
            return s
    return None

def _find_event(events, kind_name):
    """Найти первый event по нормализованному виду kind."""
    for e in events or []:
        if e.get("kind") == kind_name:
            return e
    return None

def _get_ts(ev):
    return parse_iso_any(ev.get("ts") or ev.get("when")) if ev else None

def _list_events(events, kind_name):
    return [e for e in events or [] if e.get("kind")==kind_name]

def _has_entity(entities, etype_name, any_text=None):
    for ent in entities or []:
        if ent.get("etype")==etype_name:
            if not any_text:
                return True
            val = ent.get("value")
            if isinstance(val, str):
                txt = val.lower()
            elif isinstance(val, dict):
                txt = json.dumps(val, ensure_ascii=False).lower()
            else:
                txt = str(val).lower()
            if any(t.lower() in txt for t in (any_text or [])):
                return True
    return False

def _iter_med_orders(entities):
    for ent in entities or []:
        if ent.get("etype")=="med_order":
            yield ent

def _value_text(ent):
    val = ent.get("value")
    if isinstance(val, str):
        return val
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        return str(val)

# --- Универсальные проверки ----
def _rule_primary_exam_within(doc, sections, entities, events, params, rule_id, default_hours):
    limit_h = int(params.get("ПЕРВИЧНЫЙ_ОСМОТР_ЧАСОВ", default_hours)) if isinstance(params, dict) else default_hours
    admit_ts = _get_ts(_find_event(events, "admit"))
    init = _find_event(events, "initial_exam") or _find_event(events, "exam_initial")
    init_ts = _get_ts(init)
    if admit_ts and init_ts:
        delta = (init_ts - admit_ts).total_seconds()/3600.0
        if delta > limit_h:
            return [(rule_id, "critical", f"Первичный осмотр через {delta:.1f} ч (> {limit_h} ч).")]
        return []
    return [(rule_id, "critical", "Нет данных о первичном осмотре или моменте поступления.")]

def _rule_daily_notes_every_day(rule_id, doc, events):
    admit_ts = _get_ts(_find_event(events, "admit"))
    dch_ts   = _get_ts(_find_event(events, "discharge"))
    if not admit_ts or not dch_ts: return []
    days = (dch_ts.date() - admit_ts.date()).days + 1
    if days <= 1: return []
    have = { (parse_iso_any(e.get("ts") or e.get("when")).date()) for e in _list_events(events, "daily_note") if parse_iso_any(e.get("ts") or e.get("when")) }
    missing = [ (admit_ts.date() + datetime.timedelta(days=i)) for i in range(days) if (admit_ts.date() + datetime.timedelta(days=i)) not in have ]
    if missing:
        return [(rule_id, "major", f"Нет ежедневной записи за дни: {', '.join(map(str, missing[:10]))}{'…' if len(missing)>10 else ''}.")]
    return []

def _rule_discharge_same_day(rule_id, doc, entities, events):
    dch_ts = _get_ts(_find_event(events, "discharge"))
    if not dch_ts: return []
    for ent in entities or []:
        if ent.get("etype") == "discharge_summary":
            ts = parse_iso_any(ent.get("ts") or ent.get("when"))
            if ts and ts.date()==dch_ts.date():
                return []
    return [(rule_id,"major","Выписной эпикриз не оформлен в день выписки.")]

def _rule_lab_within(rule_id, lab_hints, max_hours, events, ref_event_kind="admit"):
    base_ts = _get_ts(_find_event(events, ref_event_kind))
    if not base_ts: return []
    for ev in _list_events(events, "lab"):
        ts = _get_ts(ev)
        if not ts: continue
        name = (ev.get("title") or ev.get("name") or "").lower()
        if any(h in name for h in [h.lower() for h in lab_hints]):
            delta = (ts - base_ts).total_seconds()/3600.0
            if delta <= max_hours:
                return []
    return [(rule_id,"minor",f"Нет лабораторного исследования ({'/'.join(lab_hints)}) в пределах {max_hours} ч от события {ref_event_kind}.")]

# --- Реализации правил ---

def rule_STA_006(doc, sections, entities, events, params):
    """Полнота листа назначений: минимум 2 из 3 (dose/route/freq) для каждого назначения."""
    req = [x.lower() for x in params.get("REQUIRED_ATTRS", ["dose","route","freq"])] if isinstance(params, dict) else ["dose","route","freq"]
    bad = []
    for ent in _iter_med_orders(entities):
        v = ent.get("value") or {}
        present = [k for k in req if (isinstance(v, dict) and v.get(k)) or (isinstance(v, str) and k in v.lower())]
        if len(set(present)) < 2:
            bad.append(_value_text(ent))
    if bad:
        return [("STA-006","major", f"Неполные назначения: {len(bad)} шт. Требуются ≥2 из {req}.")]
    return []

def rule_STA_020(doc, sections, entities, events, params):
    """ОАК в первые N часов от госпитализации."""
    hints = params.get("LAB_NAME_HINTS", ["оак","общий анализ крови","cbc","hemogram"]) if isinstance(params, dict) else ["оак","общий анализ крови"]
    max_h = int(params.get("MAX_HOURS", 24)) if isinstance(params, dict) else 24
    return _rule_lab_within("STA-020", hints, max_h, events, "admit")

def rule_DAY_002(doc, sections, entities, events, params):
    """ДС: наличие плана лечения и информированного согласия."""
    need = [x.lower() for x in params.get("REQUIRE_SECTIONS", ["план лечения","информированное согласие"])] if isinstance(params, dict) else ["план лечения","информированное согласие"]
    ok = all(_has_text_keywords(sections, [n]) for n in need)
    if not ok:
        return [("DAY-002","major", f"Отсутствуют обязательные разделы: {', '.join([n for n in need if not _has_text_keywords(sections,[n])])}.")]
    return []

def rule_ER_002(doc, sections, entities, events, params):
    """Наблюдение в приёмном отделении ≤ MAX_OBSERVATION_HOURS (по admit→discharge)."""
    max_h = int(params.get("MAX_OBSERVATION_HOURS", 24)) if isinstance(params, dict) else 24
    admit_ts = _get_ts(_find_event(events, "admit"))
    dch_ts   = _get_ts(_find_event(events, "discharge"))
    if not admit_ts or not dch_ts: return []
    delta = (dch_ts - admit_ts).total_seconds()/3600.0
    if delta > max_h:
        return [("ER-002","major", f"Наблюдение длилось {delta:.1f} ч (> {max_h} ч).")]
    return []

def rule_ER_004(doc, sections, entities, events, params):
    """Боль в груди: ЭКГ ≤ ECG_MAX_MIN от admit, если есть жалоба 'боль в груди'."""
    max_min = int(params.get("ECG_MAX_MIN", 10)) if isinstance(params, dict) else 10
    # проверим жалобу/симптом
    chest_kw = ["боль в груди","загрудин","давящая боль", "chest pain"]
    has_chest = _has_entity(entities, "complaint", chest_kw) or _has_entity(entities, "symptom", chest_kw) or _has_text_keywords(sections, chest_kw)
    if not has_chest:
        return []
    admit_ts = _get_ts(_find_event(events, "admit"))
    ecg = _find_event(events, "ecg")
    if not admit_ts or not ecg:
        return [("ER-004","critical","Нет данных об ЭКГ или моменте поступления при жалобе на боль в груди.")]
    delta = (_get_ts(ecg) - admit_ts).total_seconds()/60.0
    if delta > max_min:
        return [("ER-004","critical", f"ЭКГ выполнена через {delta:.0f} мин (> {max_min} мин).")]
    return []

def rule_CAR_001(doc, sections, entities, events, params):
    """Кардиология: ЭКГ при боли в груди ≤ ECG_MAX_MIN (аналог ER-004)."""
    params = {"ECG_MAX_MIN": int(params.get("ECG_MAX_MIN", 10))} if isinstance(params, dict) else {"ECG_MAX_MIN": 10}
    return rule_ER_004(doc, sections, entities, events, params)

def rule_INF_001(doc, sections, entities, events, params):
    """Инфекционный контроль: в документации есть отметка изоляции/бокса/контактной изоляции."""
    keys = [x.lower() for x in params.get("KEYWORDS", ["изоляц","бокс","контактная изоляция"])] if isinstance(params, dict) else ["изоляц","бокс"]
    if not _has_text_keywords(sections, keys):
        return [("INF-001","major","Нет отметки об изоляции/боксе в истории (при показаниях).")]
    return []

def rule_INF_010(doc, sections, entities, events, params):
    """ОАК при инфекционных заболеваниях ≤ N часов."""
    hints = params.get("LAB_NAME_HINTS", ["оак","общий анализ крови","cbc","hemogram"]) if isinstance(params, dict) else ["оак"]
    max_h = int(params.get("MAX_HOURS", 24)) if isinstance(params, dict) else 24
    return _rule_lab_within("INF-010", hints, max_h, events, "admit")

def rule_OBG_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("OBG-001", doc, events)

def rule_OBG_002(doc, sections, entities, events, params):
    return _rule_primary_exam_within(doc, sections, entities, events, params, "OBG-002", 6)

def rule_RHEUM_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("RHEUM-001", doc, events)

def rule_NEO_002(doc, sections, entities, events, params):
    return _rule_primary_exam_within(doc, sections, entities, events, params, "NEO-002", 2)

def rule_NEPH_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("NEPH-001", doc, events)

def rule_PUL_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("PUL-001", doc, events)

def rule_URO_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("URO-001", doc, events)

def rule_GIH_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("GIH-001", doc, events)

def rule_NEURO_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("NEURO-001", doc, events)

def rule_TRAUMA_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("TRAUMA-001", doc, events)

def rule_HEM_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("HEM-001", doc, events)

def rule_ONC_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("ONC-001", doc, events)

def rule_PONC_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("PONC-001", doc, events)

def rule_PED_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("PED-001", doc, events)

def rule_PED_002(doc, sections, entities, events, params):
    return _rule_primary_exam_within(doc, sections, entities, events, params, "PED-002", 6)

def rule_PED_003(doc, sections, entities, events, params):
    # как STA-006
    p = params if isinstance(params, dict) else {"REQUIRED_ATTRS":["dose","route","freq"]}
    return rule_STA_006(doc, sections, entities, events, p)

def rule_PED_004(doc, sections, entities, events, params):
    return _rule_discharge_same_day("PED-004", doc, entities, events)

def rule_SUR_001(doc, sections, entities, events, params):
    need = [x.lower() for x in params.get("REQUIRE_SECTIONS", ["предоперационный осмотр","план операции","информированное согласие"])] if isinstance(params, dict) else ["предоперационный осмотр","план операции","информированное согласие"]
    miss = [n for n in need if not _has_text_keywords(sections,[n])]
    if miss:
        return [("SUR-001","major", f"Нет обязательных разделов перед операцией: {', '.join(miss)}.")]
    return []

def rule_SUR_010(doc, sections, entities, events, params):
    """Протокол операции оформлен в день вмешательства (ищем секцию 'протокол операции')."""
    # найдём событие operation, иначе fallback — anesthesia_start/ incision
    op_ev = _find_event(events, "operation") or _find_event(events, "surgery") or _find_event(events, "procedure")
    op_ts = _get_ts(op_ev)
    if not op_ts:
        return [("SUR-010","major","Нет временной отметки операции в событиях.")]
    sec = _find_section(sections, "протокол операции")
    if not sec or not sec.get("start"):
        return [("SUR-010","major","Нет протокола операции в документации.")]
    ts = parse_iso_any(sec.get("start")) or parse_iso_any(sec.get("ts") or sec.get("when"))
    if ts and ts.date()==op_ts.date():
        return []
    return [("SUR-010","major","Протокол операции не оформлен в день операции.")]

def rule_SUR_011(doc, sections, entities, events, params):
    """Полнота операционного протокола: ключевые поля в одной секции 'протокол операции'."""
    sec = _find_section(sections, "протокол операции")
    if not sec:
        return [("SUR-011","major","Протокол операции отсутствует.")]
    text = f"{(sec.get('title') or '')} {(sec.get('text') or '')}".lower()
    need = [x.lower() for x in params.get("REQUIRE_FIELDS", ["диагноз","операция","хирург","ассист","анестез","объем","кровопотер","осложн","дренаж","материал","описан"])]
    miss = [n for n in need if n not in text]
    if miss:
        return [("SUR-011","major", f"В протоколе операции отсутствуют поля: {', '.join(miss)}.")]
    return []

def rule_ANR_001(doc, sections, entities, events, params):
    """Преданестезиологический осмотр до операции; ищем ASA/оценку рисков в истории/карте."""
    op_ts = _get_ts(_find_event(events, "operation") or _find_event(events, "surgery"))
    if not op_ts:
        return [("ANR-001","critical","Нет временной отметки операции.")]
    # есть ли преданестезиологический осмотр до операции
    if not _has_text_keywords(sections, ["преданестезиолог", "оценка анестезиологического риска", "asa", "маллампати", "mallampati", "airway"]):
        return [("ANR-001","critical","Нет преданестезиологического осмотра/оценки рисков (ASA/Mallampati).")]
    return []

def rule_ANR_010(doc, sections, entities, events, params):
    """Наличие анестезиологической карты/наркозного листа."""
    if not _has_text_keywords(sections, ["анестезиологическая карта","наркозный лист","индукция","поддержание","выведение из анестезии"]):
        return [("ANR-010","major","Нет анестезиологической карты/наркозного листа.")]
    return []

def rule_NEUR_001(doc, sections, entities, events, params):
    """Неврология (взрослые): ежедневные записи."""
    return _rule_daily_notes_every_day("NEUR-001", doc, events)

def rule_NEUR_002(doc, sections, entities, events, params):
    """Неврология (взрослые): первичный осмотр ≤ 6 ч."""
    return _rule_primary_exam_within(doc, sections, entities, events, params, "NEUR-002", 6)

def rule_PSURG_001(doc, sections, entities, events, params):
    return _rule_daily_notes_every_day("PSURG-001", doc, events)

def rule_PSURG_002(doc, sections, entities, events, params):
    return _rule_primary_exam_within(doc, sections, entities, events, params, "PSURG-002", 6)

def rule_CHR_001(doc, sections, entities, events, params):
    """Хронические заболевания: у пациентов с хроническими диагнозами должен быть план диспансерного наблюдения."""
    # грубо детектируем наличие хронического диагноза по ключу 'хронич'
    has_chronic_dx = _has_entity(entities, "diagnosis", ["хронич"])
    if not has_chronic_dx:
        return []
    if not _has_text_keywords(sections, ["диспансерное наблюдение","план наблюдения","диспансеризац"]):
        return [("CHR-001","major","Нет плана диспансерного наблюдения при хроническом заболевании.")]
    return []

def rule_SOC_001(doc, sections, entities, events, params):
    """Соцзначимые заболевания: при наличии диагноза из перечня — отметка о маршрутизации/учёте."""
    # упрощённый список ключевых слов
    kw = [x.lower() for x in params.get("KEYWORDS", ["туберкул","вич","гепатит b","гепатит c","сахарный диабет 1 типа","онкологическ","шизофрен","ожирение 3","гемофил"])]
    has_social = _has_entity(entities, "diagnosis", kw) or _has_text_keywords(sections, kw)
    if not has_social:
        return []
    if not _has_text_keywords(sections, ["маршрут","учет","диспансер","профильный центр","направлен"]):
        return [("SOC-001","minor","Нет отметки о маршрутизации/учёте для соцзначимого заболевания.")]
    return []

# Расширяем карту реализаций
RULE_IMPL.update({
    "STA-006": rule_STA_006,
    "STA-020": rule_STA_020,
    "DAY-002": rule_DAY_002,
    "ER-002":  rule_ER_002,
    "ER-004":  rule_ER_004,
    "CAR-001": rule_CAR_001,
    "INF-001": rule_INF_001,
    "INF-010": rule_INF_010,
    "OBG-001": rule_OBG_001,
    "OBG-002": rule_OBG_002,
    "RHEUM-001": rule_RHEUM_001,
    "NEO-002": rule_NEO_002,
    "NEPH-001": rule_NEPH_001,
    "PUL-001": rule_PUL_001,
    "URO-001":  rule_URO_001,
    "GIH-001":  rule_GIH_001,
    "NEURO-001": rule_NEURO_001,
    "TRAUMA-001": rule_TRAUMA_001,
    "HEM-001":  rule_HEM_001,
    "ONC-001":  rule_ONC_001,
    "PONC-001": rule_PONC_001,
    "PED-001":  rule_PED_001,
    "PED-002":  rule_PED_002,
    "PED-003":  rule_PED_003,
    "PED-004":  rule_PED_004,
    "SUR-001":  rule_SUR_001,
    "SUR-010":  rule_SUR_010,
    "SUR-011":  rule_SUR_011,
    "ANR-001":  rule_ANR_001,
    "ANR-010":  rule_ANR_010,
    "NEUR-001": rule_NEUR_001,
    "NEUR-002": rule_NEUR_002,
    "PSURG-001": rule_PSURG_001,
    "PSURG-002": rule_PSURG_002,
    "CHR-001":  rule_CHR_001,
    "SOC-001":  rule_SOC_001,
})
# =====================  END: AUTO-APPENDED DETAILED RULES  =====================


if __name__ == "__main__":
    main()
