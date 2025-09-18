import os
import io
import uuid
import json
import hashlib
import sqlite3
import subprocess
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Depends, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse

# ======== ENV / CONFIG ========
DB_PATH = os.getenv("MEDQC_DB", "/app/medqc.db")
DEFAULT_RULES_PACKAGE = os.getenv("DEFAULT_RULES_PACKAGE", "kz-standards")
DEFAULT_RULES_VERSION = os.getenv("DEFAULT_RULES_VERSION", "2025-09-17")
API_KEY = os.getenv("API_KEY", "devkey")

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/app/uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ======== APP ========
app = FastAPI(title="MedQC API", version="1.1.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # при желании ограничьте фронтом
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======== DB HELPERS ========
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def require_api_key(x_api_key: Optional[str] = Header(default=None)):
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

def get_table_info(conn: sqlite3.Connection, table: str):
    info = {}
    for cid, name, ctype, notnull, dflt, pk in conn.execute(f"PRAGMA table_info({table})"):
        info[name] = {"type": (ctype or "").upper(), "notnull": bool(notnull), "default": dflt, "pk": bool(pk)}
    return info

def safe_defaults(coltype: str):
    t = (coltype or "").upper()
    if "INT" in t:
        return 0
    if "DATE" in t or "TIME" in t:
        return datetime.utcnow().isoformat() + "Z"
    return ""

def insert_row_dynamic(conn: sqlite3.Connection, table: str, data: dict):
    cols_info = get_table_info(conn, table)
    row = {}
    for name, meta in cols_info.items():
        if name in data and data[name] is not None:
            row[name] = data[name]
        elif meta["notnull"] and not meta["pk"]:
            row[name] = safe_defaults(meta["type"])
    if not row:
        raise RuntimeError(f"Table {table} has no columns to insert")
    fields = ",".join(row.keys())
    placeholders = ",".join(["?"] * len(row))
    values = list(row.values())
    conn.execute(f"INSERT OR REPLACE INTO {table} ({fields}) VALUES ({placeholders})", values)

def ensure_docs_table():
    """
    Мягкая миграция: создаёт docs при отсутствии и добавляет недостающие часто используемые колонки.
    Не ломает существующую схему.
    """
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS docs(
      doc_id     TEXT PRIMARY KEY,
      created_at TEXT NOT NULL
    );
    """)
    existing = {r[1] for r in conn.execute("PRAGMA table_info(docs)").fetchall()}
    for col, ctype in {
        "filename":   "TEXT",
        "path":       "TEXT",
        "dept":       "TEXT",
        "department": "TEXT",
        "sha256":     "TEXT",
        "size":       "INTEGER",
        "mime":       "TEXT",
        "status":     "TEXT"
    }.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE docs ADD COLUMN {col} {ctype}")
    conn.commit()
    conn.close()

def ensure_core_schema():
    """
    Создаёт минимально необходимые таблицы, если их нет.
    Безопасно для повторного вызова.
    """
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS artifacts(
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id     TEXT NOT NULL,
      kind       TEXT NOT NULL,
      content    TEXT,
      meta_json  TEXT,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sections(
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id     TEXT NOT NULL,
      title      TEXT,
      start      INTEGER,
      end        INTEGER,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS entities(
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id      TEXT NOT NULL,
      etype       TEXT,
      ts          TEXT,
      span_start  INTEGER,
      span_end    INTEGER,
      value_json  TEXT,
      source      TEXT,
      confidence  REAL,
      created_at  TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS events(
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id     TEXT NOT NULL,
      kind       TEXT,
      ts         TEXT,
      payload    TEXT,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS violations(
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id        TEXT NOT NULL,
      rule_id       TEXT NOT NULL,
      severity      TEXT NOT NULL,
      message       TEXT NOT NULL,
      evidence_json TEXT,
      sources_json  TEXT,
      created_at    TEXT NOT NULL
    );
    """)
    conn.commit()
    conn.close()

# ======== RULES/ORCHESTRATOR ========
from medqc_rules import (
    infer_profiles,
    get_doc as rules_get_doc,
    get_entities,
    get_events,
    load_active_rules,
)
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

@app.post("/v1/admin/migrate")
def admin_migrate(_: bool = Depends(require_api_key)):
    ensure_docs_table()
    ensure_core_schema()
    return {"status": "migrated", "db": DB_PATH}

@app.get("/v1/docs/{doc_id}")
def get_doc(doc_id: str = Path(...)):
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
def debug_rules(doc_id: str = Query(...)):
    conn = get_conn()
    doc = rules_get_doc(conn, doc_id)
    if not doc:
        conn.close()
        raise HTTPException(status_code=404, detail="doc_id not found")
    ents = get_entities(conn, doc_id)
    evs  = get_events(conn, doc_id)
    profiles = infer_profiles(doc, ents, evs)
    rules = load_active_rules(conn, profiles, DEFAULT_RULES_PACKAGE, DEFAULT_RULES_VERSION)
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
    ensure_core_schema()
    try:
        result = run_rules_only(
            doc_id,
            package=DEFAULT_RULES_PACKAGE,
            version=DEFAULT_RULES_VERSION
        )
        return JSONResponse(content=result)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"run-rules failed: {ex}")

@app.post("/v1/pipeline/{doc_id}")
def api_run_pipeline(doc_id: str, _: bool = Depends(require_api_key)):
    """
    Ручной запуск полного пайплайна по doc_id.
    """
    ensure_docs_table()
    ensure_core_schema()
    try:
        run_all(
            doc_id=doc_id,
            package=DEFAULT_RULES_PACKAGE,
            version=DEFAULT_RULES_VERSION
        )
        return {"doc_id": doc_id, "status": "OK"}
    except subprocess.CalledProcessError as cpe:
        # соберем хвост stderr/stdout, если есть
        return JSONResponse(
            status_code=202,
            content={
                "doc_id": doc_id,
                "status": "PIPELINE_ERROR",
                "failed_cmd": " ".join(cpe.cmd) if getattr(cpe, "cmd", None) else None,
                "stderr_tail": getattr(cpe, "stderr", None),
            }
        )
    except Exception as ex:
        return JSONResponse(
            status_code=202,
            content={"doc_id": doc_id, "status": "PIPELINE_ERROR", "error": str(ex)}
        )

@app.get("/v1/report/{doc_id}")
def api_report(doc_id: str, format: str = Query("html"), mask: int = Query(0)):
    """
    Возвращает отчёт по документу.
    Если у тебя уже есть medqc_report.py, можно дергать его.
    Иначе — соберём простой HTML из БД.
    """
    if format not in ("html", "json"):
        raise HTTPException(400, "format must be html or json")

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    doc = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    if not doc:
        conn.close()
        raise HTTPException(404, "doc_id not found")

    stats = {
        "sections": conn.execute("SELECT COUNT(*) FROM sections WHERE doc_id=?", (doc_id,)).fetchone()[0],
        "entities": conn.execute("SELECT COUNT(*) FROM entities WHERE doc_id=?", (doc_id,)).fetchone()[0],
        "events":   conn.execute("SELECT COUNT(*) FROM events   WHERE doc_id=?", (doc_id,)).fetchone()[0],
    }
    viol = conn.execute("""
        SELECT rule_id, severity, message, created_at
        FROM violations
        WHERE doc_id=?
        ORDER BY created_at DESC
    """, (doc_id,)).fetchall()
    violations = [dict(r) for r in viol]
    conn.close()

    if format == "json":
        return JSONResponse({"doc": dict(doc), "stats": stats, "violations": violations})

    # простой HTML-репорт
    html = [
        "<html><head><meta charset='utf-8'><title>MedQC Report</title>",
        "<style>body{font-family:system-ui,Arial,sans-serif;padding:16px} .crit{color:#b30000} .maj{color:#b36b00} .min{color:#666} table{border-collapse:collapse;width:100%} td,th{border:1px solid #ddd;padding:8px} th{background:#f8f8f8;text-align:left}</style>",
        "</head><body>",
        f"<h2>Отчёт по документу {doc_id}</h2>",
        "<h3>Статистика</h3>",
        f"<ul><li>Разделов: {stats['sections']}</li><li>Сущностей: {stats['entities']}</li><li>Событий: {stats['events']}</li></ul>",
        "<h3>Нарушения</h3>",
        "<table><thead><tr><th>Правило</th><th>Критичность</th><th>Сообщение</th><th>Время</th></tr></thead><tbody>"
    ]
    sev_cls = {"critical":"crit","major":"maj","minor":"min"}
    if violations:
        for v in violations:
            cls = sev_cls.get(v["severity"], "")
            html.append(f"<tr><td>{v['rule_id']}</td><td class='{cls}'>{v['severity']}</td><td>{v['message']}</td><td>{v['created_at']}</td></tr>")
    else:
        html.append("<tr><td colspan='4'>Нарушений не выявлено</td></tr>")
    html += ["</tbody></table>", "</body></html>"]
    return HTMLResponse("\n".join(html))

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
    ensure_core_schema()

    doc_id = f"KZ-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    safe_name = file.filename or f"{doc_id}.bin"
    dst_path = os.path.join(UPLOAD_DIR, f"{doc_id}__{safe_name}")
    blob = await file.read()
    with open(dst_path, "wb") as f:
        f.write(blob)

    sha256 = hashlib.sha256(blob).hexdigest()
    size = len(blob)
    mime = file.content_type or ""

    conn = get_conn()
    try:
        insert_row_dynamic(conn, "docs", {
            "doc_id": doc_id,
            "filename": safe_name,
            "path": dst_path,
            "dept": dept,
            "department": department,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "sha256": sha256,
            "size": size,
            "mime": mime,
            "status": "new"
        })
        conn.commit()
    finally:
        conn.close()

    # полный пайплайн
    try:
        run_all(
            doc_id=doc_id,
            package=DEFAULT_RULES_PACKAGE,
            version=DEFAULT_RULES_VERSION
        )
        return {"doc_id": doc_id, "status": "OK"}
    except subprocess.CalledProcessError as cpe:
        return JSONResponse(
            status_code=202,
            content={
                "doc_id": doc_id,
                "status": "PIPELINE_ERROR",
                "failed_cmd": " ".join(cpe.cmd) if getattr(cpe, "cmd", None) else None,
                "stderr_tail": getattr(cpe, "stderr", None)
            }
        )
    except Exception as ex:
        return JSONResponse(
            status_code=202,
            content={"doc_id": doc_id, "status": "PIPELINE_ERROR", "error": str(ex)}
        )
