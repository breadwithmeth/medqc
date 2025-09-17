#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
medqc-api — REST сервис для конвейера и админки норм/справочников.

Эндпоинты (v1):
- [health]
  GET  /v1/healthz

- [ingest + конвейер]
  POST /v1/ingest                               (загрузка файла и регистрация doc_id)
  POST /v1/pipeline/{doc_id}                    (прогон шагов конвейера, можно rules=JSON или pkg/version)
  POST /v1/rules/{doc_id}                       (только правила; JSON или pkg/version)
  GET  /v1/report/{doc_id}                      (вернёт HTML/JSON/MD, query ?format=html|json|md)

- [просмотр данных]
  GET  /v1/docs/{doc_id}                        (метаданные + счётчики)
  GET  /v1/docs/{doc_id}/violations             (список нарушений)
  GET  /v1/docs/{doc_id}/events                 (события; ?limit/&offset)
  GET  /v1/docs/{doc_id}/entities               (сущности; ?limit/&offset)
  GET  /v1/docs/{doc_id}/sections               (секции)

- [админка норм (rules в БД)]
  GET  /v1/norms/packages                       (список пакетов)
  GET  /v1/norms/packages/{name}/{version}/rules
  POST /v1/norms/packages                       (импорт rules.json из тела)
  PATCH/PUT /v1/norms/packages/{name}/{version}/rules/{rule_id}  (severity/params/enabled)
  POST /v1/norms/export                         (собрать bundle правил из БД и вернуть JSON)

- [админка справочников (dicts в БД)]
  GET  /v1/dicts/sets                           (список наборов)
  GET  /v1/dicts/sets/{name}/{version}/items    (?dtype)
  POST /v1/dicts/sets                           (импорт dicts JSON из тела)
  PATCH /v1/dicts/sets/{name}/{version}/items/{id} (norm/enabled/attrs.*)

Авторизация: заголовок X-API-Key (опц.), ключ в MEDQC_API_KEY; если пусто — без защиты.
"""

from __future__ import annotations
import os, json, tempfile, subprocess, sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import medqc_db as db






# ----------- конфигурация -----------
API_PREFIX = "/v1"
HERE = Path(__file__).resolve().parent
PY = os.getenv("PYTHON", "") or "python"
API_KEY = os.getenv("MEDQC_API_KEY", "")
DB_PATH = os.getenv("MEDQC_DB", "/app/medqc.db")
DEFAULT_RULES_PACKAGE = os.getenv("DEFAULT_RULES_PACKAGE", "kz-standards")
DEFAULT_RULES_VERSION = os.getenv("DEFAULT_RULES_VERSION", "2025-09-17")
# ----------- FastAPI -----------
app = FastAPI(title="medqc-api", version="0.1", openapi_url=f"{API_PREFIX}/openapi.json")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)
# пути до CLI-скриптов (используем их как «воркеры» шагов)
SCRIPTS = {
    'extract': HERE / 'medqc_extract.py',
    'section': HERE / 'medqc_section.py',
    'entities': HERE / 'medqc_entities.py',
    'timeline': HERE / 'medqc_timeline.py',
    'rules':   HERE / 'medqc_rules.py',
    'report':  HERE / 'medqc_report.py',
    'orch':    HERE / 'medqc_orchestrator.py',
}

# ----------- утилиты -----------
def auth_dep(x_api_key: Optional[str] = None):
    must = bool(API_KEY)
    if must and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

def run_cmd(argv: List[str]) -> Dict[str, Any]:
    """Запустить подпроцесс и вернуть JSON stdout (или {'stdout': ...})."""
    p = subprocess.run(argv, capture_output=True, text=True)
    if p.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Command failed: {' '.join(argv)}\n{p.stderr}")
    try:
        out = json.loads(p.stdout)
    except Exception:
        out = {"stdout": p.stdout.strip()}
    if p.stderr.strip():
        out["stderr"] = p.stderr.strip()
    return out

def bool_q(v: Optional[str]) -> bool:
    return str(v).lower() in ("1","true","yes","y","on")


# ----------- схемы запросов/ответов -----------
class PipelineBody(BaseModel):
    steps: Optional[List[str]] = Field(default=None, description="extract,section,entities,timeline,rules,report")
    force: Optional[str] = Field(default=None, description="какой 1 шаг форсировать")
    # режим правил из файла:
    rules_json: Optional[dict] = Field(default=None, description="rules.json прямо в теле")
    # режим правил из БД (runtime):
    pkg: Optional[str] = None
    version: Optional[str] = None
    profiles: Optional[List[str]] = None
    include_disabled: Optional[bool] = False
    # отчёт
    report_format: Optional[str] = Field(default="html", pattern="^(html|json|md)$")
    out: Optional[str] = None

class RulesBody(BaseModel):
    rules_json: Optional[dict] = None
    pkg: Optional[str] = None
    version: Optional[str] = None
    profiles: Optional[List[str]] = None
    include_disabled: Optional[bool] = False

class NormsImportBody(BaseModel):
    package: str
    version: str
    rules: List[dict]
    meta: Optional[dict] = None

class NormsPatchBody(BaseModel):
    severity: Optional[str] = Field(default=None, pattern="^(critical|major|minor)$")
    params: Optional[dict] = None
    enabled: Optional[bool] = None
    title: Optional[str] = None
    profile: Optional[str] = None
    sources: Optional[List[dict]] = None

class DictsImportBody(BaseModel):
    package: str
    version: str
    items: List[dict]
    meta: Optional[dict] = None

class DictItemPatchBody(BaseModel):
    norm: Optional[str] = None
    enabled: Optional[bool] = None
    attrs: Optional[dict] = None

# ----------- стартовая инициализация -----------
@app.on_event("startup")
def _init():
    db.init_schema()

# ----------- health -----------
@app.get(f"{API_PREFIX}/healthz")
def health():
    try:
        with db.connect() as c:
            c.execute("SELECT 1")
        return {"ok": True, "db": str(DB_PATH)}
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

# ----------- ingest -----------
@app.post(f"{API_PREFIX}/ingest", dependencies=[Depends(auth_dep)])
def ingest(
    file: UploadFile = File(...),
    facility: Optional[str] = Form(None),
    dept: Optional[str] = Form(None),
    author: Optional[str] = Form(None),
    admit_dt: Optional[str] = Form(None),
):
    # сохранить файл во временный каталог и прогнать db.ingest_local_file
    suffix = Path(file.filename).suffix or ".bin"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file.file.read())
        tmp_path = Path(tmp.name)
    try:
        res = db.ingest_local_file(tmp_path, facility=facility, dept=dept, author=author, admit_dt=admit_dt)
        if "error" in res:
            raise HTTPException(400, res["error"])
        return res
    finally:
        # не удаляем tmp файл сразу — ingest_local_file сам копирует в cases
        try: tmp_path.unlink(missing_ok=True)
        except Exception: pass

# ----------- pipeline (через orchestrator) -----------
@app.post(f"{API_PREFIX}/pipeline/{{doc_id}}", dependencies=[Depends(auth_dep)])
def pipeline(doc_id: str, body: PipelineBody):
    # Если rules_json передан — пишем во временный файл и передадим --rules
    rules_path = None
    if body.rules_json:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w", encoding="utf-8")
        json.dump(body.rules_json, tmp, ensure_ascii=False, indent=2)
        tmp.close()
        rules_path = tmp.name

    argv = [PY, str(SCRIPTS['orch']), "--doc-id", doc_id]
    if body.steps:
        argv += ["--steps", ",".join(body.steps)]
    if body.force:
        argv += ["--force", body.force]
    if rules_path:
        argv += ["--rules", rules_path]
    elif body.pkg and body.version:
        argv += ["--pkg", body.pkg, "--version", body.version]
        if body.profiles: argv += ["--profiles", ",".join(body.profiles)]
        if body.include_disabled: argv += ["--include-disabled"]
    if body.report_format:
        argv += ["--report-format", body.report_format]
    if body.out:
        argv += ["--out", body.out]

    try:
        res = run_cmd(argv)
    finally:
        if rules_path:
            try: Path(rules_path).unlink(missing_ok=True)
            except Exception: pass

    return res

# ----------- rules only -----------
@app.post(f"{API_PREFIX}/rules/{{doc_id}}", dependencies=[Depends(auth_dep)])
def apply_rules(doc_id: str, body: RulesBody):
    argv = [PY, str(SCRIPTS['rules']), "--doc-id", doc_id]
    rules_path = None
    if body.rules_json:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w", encoding="utf-8")
        json.dump(body.rules_json, tmp, ensure_ascii=False, indent=2)
        tmp.close()
        rules_path = tmp.name
        argv += ["--rules", rules_path]
    elif body.pkg and body.version:
        argv += ["--pkg", body.pkg, "--version", body.version]
        if body.profiles: argv += ["--profiles", ",".join(body.profiles)]
        if body.include_disabled: argv += ["--include-disabled"]

    try:
        res = run_cmd(argv)
    finally:
        if rules_path:
            try: Path(rules_path).unlink(missing_ok=True)
            except Exception: pass
    return res

# ----------- report -----------
@app.get(f"{API_PREFIX}/report/{{doc_id}}", dependencies=[Depends(auth_dep)])
def get_report(doc_id: str, format: str = Query("html", pattern="^(html|json|md)$")):
    argv = [PY, str(SCRIPTS['report']), "--doc-id", doc_id, "--format", format]
    res = run_cmd(argv)
    path = Path(res.get("path") or "")
    if not path.exists():
        # для json/мd мы можем вернуть сразу JSON stdout (в res уже всё есть)
        if format == "json":
            return Response(content=json.dumps(res, ensure_ascii=False, indent=2), media_type="application/json")
        raise HTTPException(500, "Report file not found")
    content = path.read_text(encoding="utf-8")
    media = "text/html" if format == "html" else ("application/json" if format == "json" else "text/markdown")
    return Response(content=content, media_type=media)

# ----------- просмотр данных -----------
def _paginate(q: List[sqlite3.Row], limit: int, offset: int):
    return [dict(r) for r in q[offset: offset+limit]]

@app.get(f"{API_PREFIX}/docs/{{doc_id}}", dependencies=[Depends(auth_dep)])
def get_doc(doc_id: str):
    d = db.get_doc(doc_id)
    if not d: raise HTTPException(404, "doc not found")
    sections = db.get_sections(doc_id)
    entities = db.get_entities(doc_id)
    events = db.get_events(doc_id)
    # нарушения (без падения, если таблицы нет)
    try:
        with db.connect() as c:
            vcnt = c.execute("SELECT COUNT(1) FROM violations WHERE doc_id=?", (doc_id,)).fetchone()[0]
    except sqlite3.OperationalError:
        vcnt = 0
    return {
        "doc": dict(d),
        "counts": {"sections": len(sections), "entities": len(entities), "events": len(events), "violations": vcnt}
    }

@app.get(f"{API_PREFIX}/docs/{{doc_id}}/violations", dependencies=[Depends(auth_dep)])
def get_violations(doc_id: str, limit: int = 500, offset: int = 0):
    try:
        with db.connect() as c:
            rows = c.execute(
                "SELECT * FROM violations WHERE doc_id=? ORDER BY severity DESC, rule_id LIMIT ? OFFSET ?",
                (doc_id, limit, offset)
            ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    return [dict(r) for r in rows]

@app.get(f"{API_PREFIX}/docs/{{doc_id}}/events", dependencies=[Depends(auth_dep)])
def get_events(doc_id: str, limit: int = 500, offset: int = 0):
    rows = db.get_events(doc_id)
    return _paginate(rows, limit, offset)

@app.get(f"{API_PREFIX}/docs/{{doc_id}}/entities", dependencies=[Depends(auth_dep)])
def get_entities(doc_id: str, limit: int = 500, offset: int = 0):
    rows = db.get_entities(doc_id)
    return _paginate(rows, limit, offset)

@app.get(f"{API_PREFIX}/docs/{{doc_id}}/sections", dependencies=[Depends(auth_dep)])
def get_sections(doc_id: str, limit: int = 500, offset: int = 0):
    rows = db.get_sections(doc_id)
    return _paginate(rows, limit, offset)

# ----------- админка норм (rules) -----------
def _ensure_norms_schema():
    with db.connect() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS norm_packages (
          pkg_id     INTEGER PRIMARY KEY AUTOINCREMENT,
          name       TEXT NOT NULL,
          version    TEXT NOT NULL,
          status     TEXT NOT NULL DEFAULT 'draft',
          locked     INTEGER NOT NULL DEFAULT 0,
          digest     TEXT,
          meta_json  TEXT,
          created_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_norm_pkg ON norm_packages(name, version);
        CREATE TABLE IF NOT EXISTS norm_rules (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          pkg_id         INTEGER NOT NULL,
          rule_id        TEXT NOT NULL,
          title          TEXT,
          profile        TEXT,
          severity       TEXT,
          params_json    TEXT,
          sources_json   TEXT,
          effective_from TEXT,
          effective_to   TEXT,
          enabled        INTEGER NOT NULL DEFAULT 1,
          notes          TEXT,
          created_at     TEXT NOT NULL,
          FOREIGN KEY(pkg_id) REFERENCES norm_packages(pkg_id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_norm_rule ON norm_rules(pkg_id, rule_id);
        """)
        c.commit()

def _get_pkg(name: str, version: str) -> Optional[sqlite3.Row]:
    with db.connect() as c:
        return c.execute("SELECT * FROM norm_packages WHERE name=? AND version=?", (name, version)).fetchone()

def _create_pkg(name: str, version: str, meta: Optional[dict]) -> int:
    with db.connect() as c:
        c.execute("INSERT INTO norm_packages(name, version, status, locked, digest, meta_json, created_at) VALUES(?,?,?,?,?,?,?)",
                  (name, version, "draft", 0, None, json.dumps(meta or {}, ensure_ascii=False), db.now_iso()))
        c.commit()
        return int(c.execute("SELECT pkg_id FROM norm_packages WHERE name=? AND version=?", (name, version)).fetchone()[0])

@app.get(f"{API_PREFIX}/norms/packages", dependencies=[Depends(auth_dep)])
def norms_list():
    _ensure_norms_schema()
    with db.connect() as c:
        rows = c.execute("""
        SELECT p.pkg_id, p.name, p.version, p.status, p.locked, p.created_at,
               COALESCE((SELECT COUNT(1) FROM norm_rules r WHERE r.pkg_id=p.pkg_id),0) AS rules
        FROM norm_packages p ORDER BY p.name, p.version
        """).fetchall()
    return [dict(r) for r in rows]

@app.get(f"{API_PREFIX}/norms/packages/{{name}}/{{version}}/rules", dependencies=[Depends(auth_dep)])
def norms_rules(name: str, version: str):
    _ensure_norms_schema()
    pkg = _get_pkg(name, version)
    if not pkg: raise HTTPException(404, "package not found")
    with db.connect() as c:
        rows = c.execute("SELECT rule_id,title,profile,severity,params_json,sources_json,enabled FROM norm_rules WHERE pkg_id=? ORDER BY rule_id", (pkg["pkg_id"],)).fetchall()
    out = []
    for r in rows:
        d = dict(r); 
        d["params"] = json.loads(d.pop("params_json") or "{}")
        d["sources"] = json.loads(d.pop("sources_json") or "[]")
        out.append(d)
    return {"package": name, "version": version, "rules": out}

@app.post(f"{API_PREFIX}/norms/packages", dependencies=[Depends(auth_dep)])
def norms_import(body: NormsImportBody):
    _ensure_norms_schema()
    pkg = _get_pkg(body.package, body.version)
    pkg_id = int(pkg["pkg_id"]) if pkg else _create_pkg(body.package, body.version, body.meta)
    with db.connect() as c:
        for r in body.rules:
            rid = r.get("id") or r.get("rule_id")
            if not rid: continue
            params_json = json.dumps(r.get("params") or {}, ensure_ascii=False)
            sources_json = json.dumps(r.get("sources") or [], ensure_ascii=False)
            cur = c.execute("SELECT id FROM norm_rules WHERE pkg_id=? AND rule_id=?", (pkg_id, rid)).fetchone()
            if cur:
                c.execute("""UPDATE norm_rules SET title=?,profile=?,severity=?,params_json=?,sources_json=?,effective_from=?,effective_to=?,enabled=? WHERE id=?""",
                          (r.get("title"), r.get("profile"), r.get("severity"), params_json, sources_json,
                           r.get("effective_from"), r.get("effective_to"),
                           0 if str(r.get("enabled")).lower() in ("0","false","no") else 1,
                           int(cur["id"])))
            else:
                c.execute("""INSERT INTO norm_rules(pkg_id,rule_id,title,profile,severity,params_json,sources_json,effective_from,effective_to,enabled,notes,created_at)
                             VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                          (pkg_id, rid, r.get("title"), r.get("profile"), r.get("severity"),
                           params_json, sources_json, r.get("effective_from"), r.get("effective_to"),
                           0 if str(r.get("enabled")).lower() in ("0","false","no") else 1,
                           r.get("notes"), db.now_iso()))
        c.commit()
    return {"status": "ok", "package": body.package, "version": body.version, "rules": len(body.rules)}

@app.put(f"{API_PREFIX}/norms/packages/{{name}}/{{version}}/rules/{{rule_id}}", dependencies=[Depends(auth_dep)])
@app.patch(f"{API_PREFIX}/norms/packages/{{name}}/{{version}}/rules/{{rule_id}}", dependencies=[Depends(auth_dep)])
def norms_patch(name: str, version: str, rule_id: str, body: NormsPatchBody):
    _ensure_norms_schema()
    pkg = _get_pkg(name, version)
    if not pkg: raise HTTPException(404, "package not found")
    with db.connect() as c:
        row = c.execute("SELECT * FROM norm_rules WHERE pkg_id=? AND rule_id=?", (pkg["pkg_id"], rule_id)).fetchone()
        if not row: raise HTTPException(404, "rule not found")
        params = json.loads(row["params_json"] or "{}")
        if body.params is not None: params.update(body.params)
        c.execute("""UPDATE norm_rules SET title=COALESCE(?,title), profile=COALESCE(?,profile), severity=COALESCE(?,severity),
                     params_json=?, sources_json=COALESCE(?,sources_json), enabled=COALESCE(?,enabled) WHERE id=?""",
                  (body.title, body.profile, body.severity, json.dumps(params, ensure_ascii=False),
                   json.dumps(body.sources, ensure_ascii=False) if body.sources is not None else None,
                   (1 if body.enabled else 0) if body.enabled is not None else None, row["id"]))
        c.commit()
    return {"status": "ok"}

@app.post(f"{API_PREFIX}/norms/export", dependencies=[Depends(auth_dep)])
def norms_export(name: str = Form(...), version: str = Form(...), profiles: Optional[str] = Form(None), include_disabled: Optional[bool] = Form(False)):
    # используем реализацию load_rules_from_db из medqc_rules для сборки
    import medqc_rules as mr
    prof_list = [p.strip() for p in profiles.split(",")] if profiles else None
    bundle = mr.load_rules_from_db(name, version, prof_list, include_disabled)
    if "error" in bundle:
        raise HTTPException(400, bundle["error"])
    return Response(content=json.dumps(bundle, ensure_ascii=False, indent=2), media_type="application/json")

# ----------- админка справочников (dicts) -----------
def _ensure_dicts_schema():
    with db.connect() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS dict_sets (
          set_id     INTEGER PRIMARY KEY AUTOINCREMENT,
          name       TEXT NOT NULL,
          version    TEXT NOT NULL,
          status     TEXT NOT NULL DEFAULT 'draft',
          digest     TEXT,
          meta_json  TEXT,
          created_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_dict_sets ON dict_sets(name, version);
        CREATE TABLE IF NOT EXISTS dict_items (
          id         INTEGER PRIMARY KEY AUTOINCREMENT,
          set_id     INTEGER NOT NULL,
          dtype      TEXT NOT NULL,
          key        TEXT NOT NULL,
          norm       TEXT,
          locale     TEXT,
          attrs_json TEXT,
          enabled    INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          FOREIGN KEY(set_id) REFERENCES dict_sets(set_id)
        );
        """)
        c.commit()

def _get_set(name: str, version: str) -> Optional[sqlite3.Row]:
    with db.connect() as c:
        return c.execute("SELECT * FROM dict_sets WHERE name=? AND version=?", (name, version)).fetchone()

def _create_set(name: str, version: str, meta: Optional[dict]) -> int:
    with db.connect() as c:
        c.execute("INSERT INTO dict_sets(name,version,status,digest,meta_json,created_at) VALUES(?,?,?,?,?,?)",
                  (name, version, "draft", None, json.dumps(meta or {}, ensure_ascii=False), db.now_iso()))
        c.commit()
        return int(c.execute("SELECT set_id FROM dict_sets WHERE name=? AND version=?", (name, version)).fetchone()[0])

@app.get(f"{API_PREFIX}/dicts/sets", dependencies=[Depends(auth_dep)])
def dicts_sets():
    _ensure_dicts_schema()
    with db.connect() as c:
        rows = c.execute("""
        SELECT s.set_id, s.name, s.version, s.status, s.created_at,
               COALESCE((SELECT COUNT(1) FROM dict_items i WHERE i.set_id=s.set_id),0) AS items
        FROM dict_sets s ORDER BY s.name, s.version
        """).fetchall()
    return [dict(r) for r in rows]

@app.get(f"{API_PREFIX}/dicts/sets/{{name}}/{{version}}/items", dependencies=[Depends(auth_dep)])
def dicts_items(name: str, version: str, dtype: Optional[str] = None):
    _ensure_dicts_schema()
    st = _get_set(name, version)
    if not st: raise HTTPException(404, "set not found")
    with db.connect() as c:
        if dtype:
            rows = c.execute("SELECT id,dtype,key,norm,locale,enabled,attrs_json FROM dict_items WHERE set_id=? AND dtype=? ORDER BY key",
                             (st["set_id"], dtype)).fetchall()
        else:
            rows = c.execute("SELECT id,dtype,key,norm,locale,enabled,attrs_json FROM dict_items WHERE set_id=? ORDER BY dtype,key",
                             (st["set_id"],)).fetchall()
    out = []
    for r in rows:
        d = dict(r); d["attrs"] = json.loads(d.pop("attrs_json") or "{}")
        out.append(d)
    return {"package": name, "version": version, "items": out}

@app.post(f"{API_PREFIX}/dicts/sets", dependencies=[Depends(auth_dep)])
def dicts_import(body: DictsImportBody):
    _ensure_dicts_schema()
    st = _get_set(body.package, body.version)
    set_id = int(st["set_id"]) if st else _create_set(body.package, body.version, body.meta)
    with db.connect() as c:
        for it in body.items:
            dtype = it.get("dtype"); key = it.get("key")
            if not dtype or not key: continue
            attrs_json = json.dumps(it.get("attrs") or {}, ensure_ascii=False)
            cur = c.execute("SELECT id FROM dict_items WHERE set_id=? AND dtype=? AND key=?", (set_id, dtype, key)).fetchone()
            if cur:
                c.execute("UPDATE dict_items SET norm=?, locale=?, attrs_json=?, enabled=? WHERE id=?",
                          (it.get("norm"), it.get("locale"), attrs_json,
                           0 if str(it.get("enabled")).lower() in ("0","false","no") else 1,
                           int(cur["id"])))
            else:
                c.execute("INSERT INTO dict_items(set_id,dtype,key,norm,locale,attrs_json,enabled,created_at) VALUES(?,?,?,?,?,?,?,?)",
                          (set_id, dtype, key, it.get("norm"), it.get("locale"), attrs_json,
                           0 if str(it.get("enabled")).lower() in ("0","false","no") else 1, db.now_iso()))
        c.commit()
    return {"status": "ok", "package": body.package, "version": body.version, "items": len(body.items)}

@app.patch(f"{API_PREFIX}/dicts/sets/{{name}}/{{version}}/items/{{item_id}}", dependencies=[Depends(auth_dep)])
def dicts_patch(name: str, version: str, item_id: int, body: DictItemPatchBody):
    _ensure_dicts_schema()
    st = _get_set(name, version)
    if not st: raise HTTPException(404, "set not found")
    with db.connect() as c:
        row = c.execute("SELECT * FROM dict_items WHERE set_id=? AND id=?", (st["set_id"], item_id)).fetchone()
        if not row: raise HTTPException(404, "item not found")
        attrs = json.loads(row["attrs_json"] or "{}")
        if body.attrs is not None: attrs.update(body.attrs)
        c.execute("UPDATE dict_items SET norm=COALESCE(?,norm), attrs_json=?, enabled=COALESCE(?,enabled) WHERE id=?",
                  (body.norm, json.dumps(attrs, ensure_ascii=False),
                   (1 if body.enabled else 0) if body.enabled is not None else None, row["id"]))
        c.commit()
    return {"status": "ok"}
