import os
import json
import sqlite3

def ensure_text_tables(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS artifacts(
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      doc_id     TEXT NOT NULL,
      kind       TEXT NOT NULL,
      content    TEXT,
      meta_json  TEXT,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS raw(
      doc_id     TEXT PRIMARY KEY,
      content    TEXT,
      created_at TEXT NOT NULL
    );
    """)
    conn.commit()

def extract_pdf_text(path):
    try:
        import fitz  # PyMuPDF
    except Exception:
        raise RuntimeError("PyMuPDF (pymupdf) не установлен в контейнере.")
    doc = fitz.open(path)
    pages = []
    for p in doc:
        pages.append(p.get_text("text"))
    return pages, "pdf"

def extract_docx_paragraphs(path):
    try:
        import docx  # python-docx
    except Exception:
        raise RuntimeError("python-docx не установлен в окружении контейнера.")
    d = docx.Document(path)
    buf, pages = "", []
    for p in d.paragraphs:
        t = (p.text or "").strip()
        if not t:
            continue
        if len(buf) + len(t) > 1500:
            pages.append(buf)
            buf = t + "\n"
        else:
            buf += t + "\n"
    if buf:
        pages.append(buf)
    return pages, "docx"

def run_extract(doc_id: str):
    db = os.getenv("MEDQC_DB", "/app/medqc.db")
    con = sqlite3.connect(db); con.row_factory = sqlite3.Row
    ensure_text_tables(con)

    row = con.execute("SELECT path, filename FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    if not row:
        raise RuntimeError(f"doc_id={doc_id} не найден в docs.")
    src = row["path"]
    if not src or not os.path.exists(src):
        raise RuntimeError(f"Файл для doc_id={doc_id} не найден: {src}")

    ext = (os.path.splitext(src)[1] or "").lower()
    if ext == ".pdf":
        pages, producer = extract_pdf_text(src)
    elif ext == ".docx":
        pages, producer = extract_docx_paragraphs(src)
    else:
        if src.lower().endswith(".doc"):
            raise RuntimeError("Файл .doc не поддерживается. Конвертируйте в .docx.")
        try:
            pages, producer = extract_pdf_text(src)
        except Exception:
            pages, producer = extract_docx_paragraphs(src)

    # пишем pages в artifacts
    con.execute("""
        INSERT OR REPLACE INTO artifacts(doc_id, kind, content, meta_json, created_at)
        VALUES(?, 'text_pages', ?, ?, datetime('now'))
    """, (doc_id, json.dumps(pages, ensure_ascii=False), json.dumps({"producer": producer}, ensure_ascii=False)))

    # и обязательно склеенный текст в raw — для старых зависимостей
    full_text = "\n\n".join(pages)
    con.execute("""
        INSERT OR REPLACE INTO raw(doc_id, content, created_at)
        VALUES(?, ?, datetime('now'))
    """, (doc_id, full_text))

    con.commit(); con.close()
    return {"doc_id": doc_id, "pages": len(pages), "producer": producer}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc-id", required=True)
    args = parser.parse_args()
    print(json.dumps(run_extract(args.doc_id), ensure_ascii=False))

if __name__ == "__main__":
    main()
