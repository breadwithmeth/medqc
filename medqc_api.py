#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from typing import Optional
import uuid, datetime as dt
# ENV
DEFAULT_RULES_PACKAGE = os.getenv("DEFAULT_RULES_PACKAGE", "kz-standards")
DEFAULT_RULES_VERSION = os.getenv("DEFAULT_RULES_VERSION", "2025-09-17")
MEDQC_DB = os.getenv("MEDQC_DB", "/app/medqc.db")
UPLOADS_DIR = os.getenv("MEDQC_UPLOADS", "/app/uploads")

app = FastAPI(title="medqc api", version="1.0")

# CORS (при необходимости поправь происхождения)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def run_cmd(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    return p.returncode, out, err

@app.get("/v1/healthz")
def healthz():
    return {"status": "ok"}

# --- upload/ingest (если у тебя уже был — можешь оставить свой, этот примерный) ---

def make_doc_id(prefix: str = "KZ") -> str:
    # Пример: KZ-20250918-94A30A4B
    ts = dt.datetime.utcnow().strftime("%Y%m%d")
    suf = uuid.uuid4().hex[:8].upper()
    return f"{prefix}-{ts}-{suf}"

@app.post("/v1/ingest")
async def ingest(
    file: UploadFile = File(...),
    # form (optional)
    doc_id: Optional[str] = Form(None),
    facility: str = Form(""),
    dept: str = Form(""),
    author: str = Form(""),
    # также позволим query-параметр
    doc_id_q: Optional[str] = None,
):
    # выберем doc_id: приоритет form > query > автогенерация
    the_doc_id = (doc_id or doc_id_q or make_doc_id())

    os.makedirs(UPLOADS_DIR, exist_ok=True)
    tmp_path = os.path.join(UPLOADS_DIR, f"{the_doc_id}__{file.filename}")
    with open(tmp_path, "wb") as f:
        f.write(await file.read())

    code, out, err = run_cmd([
        "python", "medqc_ingest.py",
        "--file", tmp_path,
        "--doc-id", the_doc_id,
        "--facility", facility,
        "--dept", dept,
        "--author", author,
    ])
    if code != 0:
        return JSONResponse(status_code=500, content={
            "doc_id": the_doc_id,
            "status": "INGEST_ERROR",
            "stderr_tail": "\n".join((err or "").splitlines()[-30:])
        })
    try:
        return JSONResponse(status_code=200, content=eval(out) if out.strip().startswith("{") else {
            "doc_id": the_doc_id, "status": "INGESTED"
        })
    except Exception:
        return JSONResponse(status_code=200, content={"doc_id": the_doc_id, "status": "INGESTED"})
# --- полный пайплайн: extract -> section -> entities -> timeline -> rules ---
@app.post("/v1/pipeline/{doc_id}")
def run_pipeline(doc_id: str):
    steps = [
        ["python", "medqc_extract.py",  "--doc-id", doc_id],
        ["python", "medqc_section.py",  "--doc-id", doc_id],
        ["python", "medqc_entities.py", "--doc-id", doc_id],
        ["python", "medqc_timeline.py", "--doc-id", doc_id],
        ["python", "medqc_rules.py",    "--doc-id", doc_id, "--package-name", DEFAULT_RULES_PACKAGE, "--package-version", DEFAULT_RULES_VERSION],
    ]
    results = []
    for cmd in steps:
        code, out, err = run_cmd(cmd)
        results.append({
            "cmd": " ".join(cmd),
            "code": code,
            "out": (out or "").strip(),
            "err_tail": "\n".join((err or "").splitlines()[-20:])
        })
        if code != 0:
            return JSONResponse(
                status_code=500,
                content={
                    "doc_id": doc_id,
                    "status": "PIPELINE_ERROR",
                    "failed_cmd": " ".join(cmd),
                    "stderr_tail": results[-1]["err_tail"],
                    "steps": results
                }
            )
    return JSONResponse(status_code=200, content={"doc_id": doc_id, "status": "PIPELINE_OK", "steps": results})

# --- отчёт (json/html) ---
@app.get("/v1/report/{doc_id}")
def get_report(doc_id: str, format: str = "json", mask: int = 0):
    fmt = format.lower()
    cmd = ["python", "medqc_report.py", "--doc-id", doc_id,
           "--package-name", DEFAULT_RULES_PACKAGE, "--package-version", DEFAULT_RULES_VERSION,
           "--format", ("html" if fmt == "html" else "json")]
    if mask:
        cmd += ["--mask"]
    code, out, err = run_cmd(cmd)
    if code != 0:
        raise HTTPException(status_code=500, detail={
            "doc_id": doc_id, "stage": "report",
            "stderr_tail": "\n".join((err or "").splitlines()[-30:])
        })
    if fmt == "html":
        return HTMLResponse(content=out, status_code=200)
    return PlainTextResponse(out, status_code=200, media_type="application/json")

# --- быстрый список нарушений (удобно для дебага) ---
@app.get("/v1/violations/{doc_id}")
def list_violations(doc_id: str):
    code, out, err = run_cmd([
        "python", "medqc_report.py", "--doc-id", doc_id,
        "--package-name", DEFAULT_RULES_PACKAGE, "--package-version", DEFAULT_RULES_VERSION,
        "--format", "json"
    ])
    if code != 0:
        raise HTTPException(status_code=500, detail={
            "doc_id": doc_id, "stage": "report",
            "stderr_tail": "\n".join((err or "").splitlines()[-30:])
        })
    return PlainTextResponse(out, status_code=200, media_type="application/json")
