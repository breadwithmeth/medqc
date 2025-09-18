#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import hashlib
import argparse
import mimetypes
import json
import sqlite3
from datetime import datetime
from pathlib import Path

from medqc_db import DB_PATH, UPLOADS_DIR, get_conn, ensure_docs_schema

def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_mime(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

def ensure_upload_dest(doc_id: str) -> str:
    dest_dir = os.path.join(UPLOADS_DIR, doc_id)
    os.makedirs(dest_dir, exist_ok=True)
    return dest_dir

def upsert_doc(conn: sqlite3.Connection, doc_id: str, src_abs: str, filename: str, mime: str, size: int,
               facility: str = "", dept: str = "", author: str = ""):
    """
    Гарантированно пишет запись в docs. Предполагает, что ensure_docs_schema(conn) уже вызван.
    """
    sha = sha256_of(src_abs)
    created_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    row = conn.execute("SELECT doc_id FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    if row:
        conn.execute("""
            UPDATE docs
               SET sha256=?,
                   src_path=?,
                   mime=?,
                   size=?,
                   filename=?,
                   path=?,
                   facility=COALESCE(facility,''),
                   dept=COALESCE(dept,''),
                   author=COALESCE(author,'')
             WHERE doc_id=?""",
             (sha, src_abs, mime, size, filename, src_abs, doc_id))
    else:
        conn.execute("""
            INSERT INTO docs(doc_id, sha256, src_path, mime, size, facility, dept, author, admit_dt, created_at, filename, path, department)
            VALUES(?,?,?,?,?,?,?,?,'',?, ?, ?, '')""",
            (doc_id, sha, src_abs, mime, size, facility, dept, author, created_at, filename, src_abs)
        )

def ingest_file(src_file: str, doc_id: str, facility: str = "", dept: str = "", author: str = "") -> dict:
    if not os.path.exists(src_file):
        raise FileNotFoundError(f"File not found: {src_file}")

    src_abs = os.path.abspath(src_file)
    filename = os.path.basename(src_abs)
    mime = safe_mime(src_abs)
    size = os.path.getsize(src_abs)

    with get_conn() as conn:
        # гарантируем наличие таблицы docs
        ensure_docs_schema(conn)

        # переносим в /app/uploads/<doc_id>/<filename>
        dest_dir = ensure_upload_dest(doc_id)
        dest_file = os.path.join(dest_dir, filename)
        if src_abs != dest_file:
            shutil.copy2(src_abs, dest_file)

        # обновляем запись в docs (src_path/path указывают на dest_file)
        upsert_doc(conn, doc_id, dest_file, filename, mime, size, facility, dept, author)
        conn.commit()

    return {
        "doc_id": doc_id,
        "status": "INGESTED",
        "filename": filename,
        "mime": mime,
        "size": size
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Путь к исходному файлу (pdf/docx/...).")
    ap.add_argument("--doc-id", required=True, help="Уникальный идентификатор документа")
    ap.add_argument("--facility", default="")
    ap.add_argument("--dept", default="")
    ap.add_argument("--author", default="")
    args = ap.parse_args()

    res = ingest_file(args.file, args.doc_id, args.facility, args.dept, args.author)
    print(json.dumps(res, ensure_ascii=False))

if __name__ == "__main__":
    main()
