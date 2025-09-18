# medqc_api.py
# Минимальные правки: перед применением правил превращаем sqlite3.Row → dict,
# чтобы .get(...) в правилах был безопасен.

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Any, Dict, Optional

import medqc_db as db
import medqc_rules as rules

import os

APP_DB_PATH = os.environ.get("MEDQC_DB", "/app/medqc.db")

app = FastAPI(title="medqc API")

# Инициализация коннекта (упрощённо, без пулов)
CONN = db.connect(APP_DB_PATH)

# ---------- Модели (по минимуму для примеров) ----------

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
    # Сюда обычно входит парсинг, нормализация и upsert документа
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
    """
    Диагностика применимости правил: извлекаем doc/entities/events из БД,
    гарантируем dict, и передаём в движок правил.
    """
    doc = db.get_doc(CONN, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="doc not found")

    # ВАЖНО: doc уже dict (вариант А). Если по каким-то причинам upstream вернул row,
    # можно продублировать защиту:
    if not isinstance(doc, dict):
        try:
            doc = dict(doc)  # fallback, если кто-то вернул sqlite3.Row
        except Exception:
            pass

    ents = db.get_doc_entities(CONN, doc_id)  # уже list[dict]
    evs = db.get_doc_events(CONN, doc_id)     # уже list[dict]

    try:
        prof = rules.infer_profiles(doc, ents, evs)  # внутри можно использовать .get
        result = rules.debug_apply_rules(CONN, doc, ents, evs, prof)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"rules debug failed: {e}")

    return {
        "doc_id": doc_id,
        "profile_inferred": prof,
        "debug": result,
    }

# ---------- Выполнение правил и отчёты (заглушки под интерфейс) ----------

@app.post("/v1/run-rules")
def run_rules(body: Dict[str, Any]):
    doc_id = body.get("doc_id")
    if not doc_id:
        raise HTTPException(status_code=422, detail="doc_id is required")

    doc = db.get_doc(CONN, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="doc not found")

    ents = db.get_doc_entities(CONN, doc_id)
    evs = db.get_doc_events(CONN, doc_id)

    prof = rules.infer_profiles(doc, ents, evs)
    summary = rules.apply_rules_and_store(CONN, doc, ents, evs, prof)
    return {"doc_id": doc_id, "profile": prof, "summary": summary}

@app.get("/v1/rules/{doc_id}")
def list_rules_for_doc(doc_id: str):
    # возвращает статусы правил по документу (как вы реализовали)
    # здесь условно:
    apps = db.list_rule_applications(CONN, doc_id)
    return {"doc_id": doc_id, "rule_applications": apps}

@app.get("/v1/violations/{doc_id}")
def list_violations(doc_id: str):
    vio = db.list_violations(CONN, doc_id)
    return {"doc_id": doc_id, "violations": vio}

@app.get("/v1/report/{doc_id}")
def get_report(doc_id: str):
    # сгенерированный отчёт (если есть отдельная таблица/хранилище)
    # можно вернуть шаблон, ниже просто заглушка
    vio = db.list_violations(CONN, doc_id)
    return {
        "doc_id": doc_id,
        "violations": vio,
        "note": "Здесь может быть HTML/PDF отчёт, если реализовано."
    }

# ---------- Admin ----------

@app.post("/v1/admin/migrate")
def migrate():
    """
    Миграция/импорт правил из rules.json (если у вас реализован в другом модуле — вызовите его здесь).
    Здесь просто заглушка.
    """
    # например: medqc_norms_admin.migrate(CONN, "/app/rules.json")
    return {"status": "migrated"}
