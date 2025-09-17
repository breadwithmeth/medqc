import os
import io
import uuid
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ======== ENV / CONFIG ========
DB_PATH = os.getenv("MEDQC_DB", "/app/medqc.db")
DEFAULT_RULES_PACKAGE = os.getenv("DEFAULT_RULES_PACKAGE", "kz-standards")
DEFAULT_RULES_VERSION = os.getenv("DEFAULT_RULES_VERSION", "2025-09-17")
API_KEY = os.getenv("API_KEY", "devkey")

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/app/uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ======== APP ========
app = FastAPI(title="MedQC API", version="1.0.0")

# CORS (локальный фронт / любой домен, по желанию сузьте)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # при желании заменить на ["http://localhost:8080", "http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======== HELPERS ========
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def require_api_key(x_api_key: Optional[str] = Header(default=None)):
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True
def ensure_docs_table():
    """
    Гарантирует наличие таблицы docs и всех необходимых колонок.
    Безопасно для повторного вызова (idempotent).
    """
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS docs(
      doc_id     TEXT PRIMARY KEY,
      created_at TEXT NOT NULL
    );
    """)
    # актуальные колонки, которые должны быть
    required_cols = {
        "filename":   "TEXT",
        "path":       "TEXT",
        "dept":       "TEXT",
        "department": "TEXT"
    }

    # какие колонки уже есть
    cur = conn.execute("PRAGMA table_info(docs)")
    existing = {row[1] for row in cur.fetchall()}  # row[1] = name

    # добавим недостающие
    for col, coltype in required_cols.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE docs ADD COLUMN {col} {coltype}")

    conn.commit()
    conn.close()


# импортируем функции правил (для /debug/rules)
from medqc_rules import (
    infer_profiles,
    get_doc as rules_get_doc,
    get_entities,
    get_events,
    load_active_rules,
)

# импорт оркестратора (полный прогон пайплайна)
from medqc_orchestrator import run_all, run_rules_only

# ======== ENDPOINTS ========

@app.get("/v1/healthz")
def healthz():
    return {"ok": True, "version": app.version}

@app.get("/v1/healthz/db")
def healthz_db():
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0
    return {"db_path": DB_PATH, "exists": exists, "size": size}

@app.get("/v1/docs/{doc_id}")
def get_doc(doc_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="doc_id not found")
    return dict(row)

@app.get("/v1/docs/{doc_id}/stats")
def doc_stats(doc_id: str):
    conn = get_conn()
    s = conn.execute("SELECT COUNT(*) FROM sections WHERE doc_id=?", (doc_id,)).fetchone()[0]
    e = conn.execute("SELECT COUNT(*) FROM entities WHERE doc_id=?", (doc_id,)).fetchone()[0]
    v = conn.execute("SELECT COUNT(*) FROM events   WHERE doc_id=?", (doc_id,)).fetchone()[0]
    conn.close()
    return {"doc_id": doc_id, "sections": s, "entities": e, "events": v}

@app.get("/v1/violations/{doc_id}")
def get_violations(doc_id: str) -> List[Dict[str, Any]]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT rule_id, severity, message, created_at
        FROM violations
        WHERE doc_id=?
        ORDER BY created_at DESC
    """, (doc_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/v1/debug/rules")
def debug_rules(doc_id: str):
    """
    Диагностика: какие профили рассчитаны и какие правила видны API
    """
    conn = get_conn()
    doc = rules_get_doc(conn, doc_id)
    if not doc:
        conn.close()
        raise HTTPException(status_code=404, detail="doc_id not found")

    ents = get_entities(conn, doc_id)
    evs = get_events(conn, doc_id)
    profiles = infer_profiles(doc, ents, evs)

    rules = load_active_rules(
        conn,
        profiles,
        DEFAULT_RULES_PACKAGE,
        DEFAULT_RULES_VERSION
    )
    conn.close()
    return {
        "doc_id": doc_id,
        "profiles": profiles,
        "rules_count": len(rules),
        "rule_ids": [r.get("rule_id") for r in rules]
    }

@app.post("/v1/run-rules")
def api_run_rules(
    doc_id: str = Form(...),
    _: bool = Depends(require_api_key)
):
    """
    Принудительный запуск правил по doc_id с активным пакетом из ENV.
    """
    try:
        result = run_rules_only(
            doc_id,
            package=DEFAULT_RULES_PACKAGE,
            version=DEFAULT_RULES_VERSION
        )
        return JSONResponse(content=result)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"run-rules failed: {ex}")

@app.post("/v1/ingest")
async def ingest_file(
    file: UploadFile = File(...),
    dept: Optional[str] = Form(default=None),
    department: Optional[str] = Form(default=None),
    _: bool = Depends(require_api_key)
):
    """
    Принимает файл (PDF/DOCX), сохраняет запись в docs, запускает полный пайплайн и возвращает doc_id.
    """
    ensure_docs_table()

    # Генерим doc_id (UTC+0; на фронте храним как есть)
    doc_id = f"KZ-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    # Сохраняем файл
    safe_name = file.filename or f"{doc_id}.bin"
    dst_path = os.path.join(UPLOAD_DIR, f"{doc_id}__{safe_name}")
    blob = await file.read()
    with open(dst_path, "wb") as f:
        f.write(blob)

    # Запись в docs
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO docs(doc_id, filename, path, dept, department, created_at) VALUES(?,?,?,?,?,?)",
        (doc_id, safe_name, dst_path, dept, department, datetime.utcnow().isoformat() + "Z")
    )
    conn.commit()
    conn.close()

    # Запускаем полный пайплайн (extract → section → entities → timeline → rules)
    try:
        run_all(
            doc_id=doc_id,
            package=DEFAULT_RULES_PACKAGE,
            version=DEFAULT_RULES_VERSION
        )
    except Exception as ex:
        # Даже если пайплайн упал, вернём doc_id — фронт сможет запросить /stats и /violations
        return JSONResponse(
            status_code=202,
            content={"doc_id": doc_id, "status": "INGESTED_WITH_ERRORS", "error": str(ex)}
        )

    return {"doc_id": doc_id, "status": "OK"}
