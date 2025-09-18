#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import sqlite3
from typing import Any, Dict, List, Optional
from datetime import datetime

import medqc_db as db

DB_PATH = os.getenv("MEDQC_DB", "/app/medqc.db")

def get_conn() -> sqlite3.Connection:
    return db.get_conn(DB_PATH)

def fetch_doc_meta(conn: sqlite3.Connection, doc_id: str) -> Dict[str, Any]:
    cur = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,))
    row = cur.fetchone()
    if not row:
        return {"doc_id": doc_id}
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))

def fetch_violations(conn: sqlite3.Connection, doc_id: str,
                     package_name: str = "", package_version: str = "") -> List[Dict[str, Any]]:
    """
    Если в violations нет привязки к пакету — просто возвращаем всё по doc_id.
    Если ты сохраняешь пакет в violations.extra_json -> фильтруй здесь (пример показываю как сделать).
    """
    cur = conn.execute("""
        SELECT id, doc_id, rule_id, severity, message, evidence_json, sources_json, created_at, profile, extra_json
          FROM violations
         WHERE doc_id=?
         ORDER BY created_at, id
    """, (doc_id,))
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    if package_name and package_version:
        # если extra_json содержит {"package":{"name":..., "version":...}} — фильтруем
        filt = []
        for v in rows:
            try:
                extra = json.loads(v.get("extra_json") or "{}")
            except Exception:
                extra = {}
            pkg = extra.get("package") or {}
            if not pkg:
                # если пакет не записан в violation — оставляем (или меняй на continue — по вкусу)
                filt.append(v); continue
            if pkg.get("name")==package_name and str(pkg.get("version"))==str(package_version):
                filt.append(v)
        rows = filt

    return rows

def mask_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    # очень простая маскировка: заменим 8+ подряд идущих букв/цифр на ***
    import re
    return re.sub(r"([A-Za-zА-Яа-яЁё0-9]{8,})", "***", s)

def build_json_report(doc_id: str, package_name: str, package_version: str, do_mask: bool) -> Dict[str, Any]:
    with get_conn() as conn:
        meta = fetch_doc_meta(conn, doc_id)
        violations = fetch_violations(conn, doc_id, package_name, package_version)

    if do_mask:
        for v in violations:
            v["message"] = mask_text(v.get("message"))
            # при желании можно замаскировать и evidence_json
            # v["evidence_json"] = mask_text(v.get("evidence_json"))

    return {
        "doc_id": doc_id,
        "package_name": package_name,
        "package_version": package_version,
        "meta": {
            "filename": meta.get("filename"),
            "mime": meta.get("mime"),
            "size": meta.get("size"),
            "facility": meta.get("facility"),
            "dept": meta.get("dept") or meta.get("department"),
            "author": meta.get("author"),
            "created_at": meta.get("created_at"),
        },
        "violations": violations,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def build_html_report(payload: Dict[str, Any]) -> str:
    title = f"Отчёт по документу {payload['doc_id']}"
    vlist = payload.get("violations", [])
    lines = []
    lines.append(f"<!doctype html><html><head><meta charset='utf-8'><title>{title}</title>")
    lines.append("""
    <style>
      body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:24px;}
      .hdr{font-size:20px;font-weight:600;margin-bottom:12px}
      table{border-collapse:collapse;width:100%}
      th,td{border:1px solid #ddd;padding:8px;font-size:14px;vertical-align:top}
      th{background:#f7f7f7;text-align:left}
      .sev-critical{color:#a00;font-weight:700}
      .sev-major{color:#a50}
      .sev-minor{color:#555}
      .muted{color:#777}
    </style>
    </head><body>
    """)
    lines.append(f"<div class='hdr'>{title}</div>")
    pkg = f"{payload.get('package_name') or ''} {payload.get('package_version') or ''}".strip()
    if pkg:
        lines.append(f"<div class='muted'>Пакет норм: {pkg}</div>")
    meta = payload.get("meta") or {}
    if any(meta.get(k) for k in ("filename","facility","dept","author","created_at")):
        lines.append("<p class='muted'>")
        if meta.get("filename"):  lines.append(f"Файл: <b>{meta['filename']}</b><br>")
        if meta.get("facility"):  lines.append(f"Учреждение: {meta['facility']}<br>")
        if meta.get("dept"):      lines.append(f"Отделение: {meta['dept']}<br>")
        if meta.get("author"):    lines.append(f"Автор: {meta['author']}<br>")
        if meta.get("created_at"):lines.append(f"Загружен: {meta['created_at']}<br>")
        lines.append("</p>")

    lines.append("<table><thead><tr><th>#</th><th>Правило</th><th>Профиль</th><th>Серьёзность</th><th>Сообщение</th><th>Источник</th><th>Время</th></tr></thead><tbody>")
    if not vlist:
        lines.append("<tr><td colspan='7' class='muted'>Нарушения не найдены</td></tr>")
    else:
        for i, v in enumerate(vlist, 1):
            sev = (v.get("severity") or "").lower()
            cls = f"sev-{sev}" if sev in ("critical","major","minor") else ""
            msg = v.get("message") or ""
            rid = v.get("rule_id") or ""
            prof = v.get("profile") or ""
            srcs = v.get("sources_json") or ""
            try:
                srcs_obj = json.loads(srcs) if isinstance(srcs, str) and srcs.strip().startswith("[") else (srcs or [])
            except Exception:
                srcs_obj = []
            sources_txt = ", ".join([s.get("ref","") for s in srcs_obj if isinstance(s, dict)]) or ""
            lines.append(f"<tr><td>{i}</td><td>{rid}</td><td>{prof}</td><td class='{cls}'>{sev}</td><td>{msg}</td><td>{sources_txt}</td><td>{v.get('created_at') or ''}</td></tr>")
    lines.append("</tbody></table>")
    lines.append(f"<p class='muted'>Сгенерировано: {payload['generated_at']}</p>")
    lines.append("</body></html>")
    return "".join(lines)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-id", required=True)
    ap.add_argument("--format", choices=["html","json","md"], default="json")
    ap.add_argument("--out", default="")
    # новые флаги — совместимы с API
    ap.add_argument("--package-name", default="")
    ap.add_argument("--package-version", default="")
    ap.add_argument("--mask", action="store_true")
    args = ap.parse_args()

    # Готовим схему (безопасно/идемпотентно)
    db.init_schema()

    payload = build_json_report(args.doc_id, args.package_name, args.package_version, args.mask)

    if args.format == "json":
        out = json.dumps(payload, ensure_ascii=False, indent=2)
    elif args.format == "html":
        out = build_html_report(payload)
    else:
        # простой markdown как запасной вариант
        lines = [f"# Отчёт по документу {payload['doc_id']}"]
        if payload.get("package_name") or payload.get("package_version"):
            lines.append(f"**Пакет норм:** {payload.get('package_name','')} {payload.get('package_version','')}".strip())
        lines.append("")
        if not payload["violations"]:
            lines.append("_Нарушения не найдены_")
        else:
            for v in payload["violations"]:
                lines.append(f"- **{v.get('rule_id','')}** ({v.get('severity','')}): {v.get('message','')}")
        out = "\n".join(lines)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out)
    else:
        print(out)

if __name__ == "__main__":
    main()
