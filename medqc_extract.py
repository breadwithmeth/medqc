#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, sys
from pathlib import Path
import medqc_db as db

import re

def extract_pdf_text(path: Path):
    import fitz  # PyMuPDF
    texts = []
    producer = None
    with fitz.open(str(path)) as doc:
        producer = doc.metadata.get("producer") or doc.metadata.get("Producer")
        for p in doc:
            texts.append(p.get_text("text"))
    return texts, producer


def extract_docx_text(path: Path):
    import docx
    d = docx.Document(str(path))
    # DOCX не знает про страницы — вернём как один «лист»
    text = "\n".join(p.text for p in d.paragraphs)
    return [text], "python-docx"


def main():
    ap = argparse.ArgumentParser(description="medqc-extract — извлечение текста")
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()

    db.init_schema()
    row = db.get_doc(args.doc_id)
    if not row:
        print(f"{{\"error\":{{\"code\":\"NO_DOC\",\"message\":\"unknown doc_id {args.doc_id}\"}}}}")
        sys.exit(1)
    src = Path(row["src_path"]).resolve()
    ext = src.suffix.lower()

    if ext == ".pdf":
        pages_text, producer = extract_pdf_text(src)
    elif ext == ".docx":
        pages_text, producer = extract_docx_text(src)
    else:
        print(f"{{\"error\":{{\"code\":\"UNSUPPORTED\",\"message\":\"{ext}\"}}}}")
        sys.exit(2)

    full = "\n".join(pages_text)
    # is_scanned для PDF: если суммарная длина текста слишком мала
    is_scanned = (len("".join(pages_text).strip()) < 10)

    # посчитать глобальные оффсеты
    pages_rows = []
    cur = 0
    for i, t in enumerate(pages_text, start=1):
        start = cur
        end = cur + len(t)
        pages_rows.append({"pageno": i, "start": start, "end": end, "text": t})
        cur = end + 1  # учитываем перевод строки при объединении

    db.upsert_raw_text(args.doc_id, is_scanned, len(pages_text), producer, "ru,kk", full)
    db.replace_pages(args.doc_id, pages_rows)

    import json
    print(json.dumps({
        "doc_id": args.doc_id,
        "status": "extracted",
        "pages": len(pages_rows),
        "is_scanned": is_scanned,
        "producer": producer
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()