import os
import subprocess

MEDQC_DB = os.getenv("MEDQC_DB", "/app/medqc.db")
DEFAULT_RULES_PACKAGE = os.getenv("DEFAULT_RULES_PACKAGE", "kz-standards")
DEFAULT_RULES_VERSION = os.getenv("DEFAULT_RULES_VERSION", "2025-09-17")

def _run(cmd):
    env = os.environ.copy()
    env["MEDQC_DB"] = MEDQC_DB
    print(f"[orchestrator] RUN: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=env)

def run_all(doc_id: str, package: str = DEFAULT_RULES_PACKAGE, version: str = DEFAULT_RULES_VERSION):
    _run(["python", "medqc_extract.py",  "--doc-id", doc_id])
    _run(["python", "medqc_section.py",  "--doc-id", doc_id])
    _run(["python", "medqc_entities.py", "--doc-id", doc_id])
    _run(["python", "medqc_timeline.py","--doc-id", doc_id])
    _run([
        "python", "medqc_rules.py",
        "--doc-id", doc_id,
        "--package-name", package,
        "--package-version", version
    ])

def run_rules_only(doc_id: str, package: str = DEFAULT_RULES_PACKAGE, version: str = DEFAULT_RULES_VERSION):
    env = os.environ.copy()
    env["MEDQC_DB"] = MEDQC_DB
    cmd = [
        "python", "medqc_rules.py",
        "--doc-id", doc_id,
        "--package-name", package,
        "--package-version", version
    ]
    print(f"[orchestrator] RUN: {' '.join(cmd)}")
    cp = subprocess.run(cmd, check=True, env=env, capture_output=True, text=True)
    out = (cp.stdout or "").strip()
    if out.startswith("{") or out.startswith("["):
        import json
        try:
            return json.loads(out)
        except Exception:
            return {"ok": True, "raw": out}
    return {"ok": True, "raw": out}
