# medqc_api.py
# Стартап: ensure_schema(); /v1/admin/migrate вызывает реальный импорт правил

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Any, Dict, Optional
import os

import medqc_db as db
import medqc_rules as rules
import medqc_norms_admin as norms_admin

APP_DB_PATH = os.environ.get("MEDQC_DB", "/app/medqc.db")
RULES_JSON_PATH = os.environ.get("MEDQC_RULES_JSON", "/app/rules.json")

app = FastAPI(title="medqc API")

# Инициализация коннекта и схемы
CONN = db.connect(APP_DB_PATH)
db.ensure_schema(CONN)

# ---------- Модели (минимум) ----------

class IngestPayload(BaseModel):
    doc_id: str
    profile: Optional[str] = None
    dept: Optional[str] = None
    title: Optional[str] = None
    content: str

# ---------- Health ----------

@app.get("/v1/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/v1/healthz/db")
def healthz_db():
    try:
        pkg = db.get_active_rules_package(CONN)
        return {"status": "ok", "active_rules": pkg}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Docs ----------

@app.get("/v1/docs/{doc_id}")
def get_doc(doc_id: str):
    doc = db.get_doc(CONN, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="doc not found")
    return doc

@app.get("/v1/docs/{doc_id}/stats")
def get_doc_stats(doc_id: str):
    stats = db.get_doc_stats(CONN, doc_id)
    if not stats:
        raise HTTPException(status_code=404, detail="stats not found")
    return stats

@app.post("/v1/ingest")
def ingest(payload: IngestPayload):
    doc = {
        "doc_id": payload.doc_id,
        "profile": payload.profile,
        "dept": payload.dept,
        "title": payload.title,
        "content": payload.content,
    }
    db.upsert_doc(CONN, doc)
    return {"status": "ok", "doc_id": payload.doc_id}

# ---------- Rules / Debug ----------

@app.get("/v1/debug/rules")
def debug_rules(doc_id: str = Query(..., description="Document ID")):
    doc = db.get_doc(CONN, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="doc not found")

    ents = db.get_doc_entities(CONN, doc_id)
    evs  = db.get_doc_events(CONN, doc_id)

    try:
        prof = rules.infer_profiles(doc, ents, evs)
        result = rules.debug_apply_rules(CONN, doc, ents, evs, prof)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"rules debug failed: {e}")

    return {"doc_id": doc_id, "profile_inferred": prof, "debug": result}

@app.post("/v1/run-rules")
def run_rules(body: Dict[str, Any]):
    doc_id = body.get("doc_id")
    if not doc_id:
        raise HTTPException(status_code=422, detail="doc_id is required")

    doc = db.get_doc(CONN, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="doc not found")

    ents = db.get_doc_entities(CONN, doc_id)
    evs  = db.get_doc_events(CONN, doc_id)

    prof = rules.infer_profiles(doc, ents, evs)
    summary = rules.apply_rules_and_store(CONN, doc, ents, evs, prof)
    return {"doc_id": doc_id, "profile": prof, "summary": summary}

@app.get("/v1/rules/{doc_id}")
def list_rules_for_doc(doc_id: str):
    apps = db.list_rule_applications(CONN, doc_id)
    return {"doc_id": doc_id, "rule_applications": apps}

@app.get("/v1/violations/{doc_id}")
def list_violations(doc_id: str):
    vio = db.list_violations(CONN, doc_id)
    return {"doc_id": doc_id, "violations": vio}

@app.get("/v1/report/{doc_id}")
def get_report(doc_id: str):
    vio = db.list_violations(CONN, doc_id)
    return {"doc_id": doc_id, "violations": vio, "note": "Render your HTML/PDF here."}

# ---------- Admin ----------

@app.post("/v1/admin/migrate")
def migrate():
    """
    Создаёт схему (идемпотентно) и импортирует / активирует rules.json.
    """
    try:
        db.ensure_schema(CONN)
        result = norms_admin.migrate(CONN, RULES_JSON_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
