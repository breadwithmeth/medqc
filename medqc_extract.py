#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import sqlite3
from typing import List

from medqc_db import DB_PATH, ensure_extract_tables, get_conn, get_doc_file_path

# опциональные парсеры
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import docx  # python-docx
except Exception:
    docx = None


def extract_pdf_pages(path: str) -> List[str]:
    if not fitz:
        raise RuntimeError("PyMuPDF (pymupdf) не установлен в окружении контейнера.")
    doc = fitz.open(path)
    pages = []
    for i in range(len(doc)):
        pages.append(doc.load_page(i).get_text("text"))
    doc.close()
    return pages


def extract_docx_paragraphs(path: str) -> List[str]:
    if not docx:
        raise RuntimeError("python-docx не установлен в окружении контейнера.")
    d = docx.Document(path)
    # соберём как pseudo-страницы по крупным блокам (для согласованности)
    text = []
    for p in d.paragraphs:
        text.append(p.text or "")
    content = "\n".join(text).strip()
    # разбивать по страницам DOCX сложно; положим всю «страницу» как idx=0
    return [content] if content else []


def save_pages(conn: sqlite3.Connection, doc_id: str, pages: List[str]):
    ensure_extract_tables(conn)
    # очищаем прежние
    conn.execute("DELETE FROM pages WHERE doc_id=?", (doc_id,))
    # вставляем
    for idx, txt in enumerate(pages):
        conn.execute("INSERT INTO pages(doc_id, idx, text) VALUES(?,?,?)", (doc_id, idx, txt))
    # также кладём «сырой» конкатенированный текст
    conn.execute("INSERT OR REPLACE INTO raw(doc_id, content) VALUES(?,?)", (doc_id, "\n\n".join(pages)))


def run_extract(doc_id: str) -> dict:
    with get_conn() as conn:
        src = get_doc_file_path(conn, doc_id)
        if not src or not os.path.exists(src):
            raise RuntimeError(f"Source file not found for doc_id={doc_id}. Проверь docs.src_path/path/filename и /app/uploads/{doc_id}/")
        # определим формат
        ext = os.path.splitext(src)[1].lower()
        if ext in (".pdf",):
            pages = extract_pdf_pages(src)
        elif ext in (".docx",):
            pages = extract_docx_paragraphs(src)
        else:
            raise RuntimeError(f"Unsupported file type: {ext}")
        save_pages(conn, doc_id, pages)
        conn.commit()
        return {"doc_id": doc_id, "status": "extracted", "pages": len(pages)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()
    print(json.dumps(run_extract(args.doc_id), ensure_ascii=False))


if __name__ == "__main__":
    main()
