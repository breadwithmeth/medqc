# medqc-cli v0.2 — первые 4 программы + БД (без нейросетей)

Ниже — **4 самостоятельные CLI-программы** и общий модуль БД. Все данные складываются в **SQLite** и каталог кейсов. Другие программы получают данные **по `doc_id`** из БД.

* `medqc_db.py` — общий модуль (SQLite + схема + утилиты)
* `medqc_ingest.py` — приём файла, хэши, копия в `/cases/<doc_id>/`, запись в БД
* `medqc_extract.py` — извлечение текста (PDF через PyMuPDF, DOCX через python-docx), запись в БД
* `medqc_section.py` — секционирование по шаблонам, запись в БД
* `medqc_entities.py` — извлечение сущностей (даты/время, диагнозы/МКБ, назначения, виталы), запись в БД

## Установка зависимостей

```bash
python -m pip install pymupdf python-docx
```

Опционально: `chardet` для лучшего чтения текстов (необязательно).

## Переменные окружения

* `MEDQC_DB` — путь к SQLite (по умолчанию `./medqc.db`)
* `MEDQC_CASES` — корень кейсов (по умолчанию `./cases`)

---

## medqc\_db.py

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Общий модуль для работы с БД и хранилищем кейсов."""
from __future__ import annotations
import os, sqlite3, hashlib, json, shutil, mimetypes, secrets
from pathlib import Path
from datetime import datetime
from typing import Optional, Iterable, Dict, Any

DB_PATH = Path(os.getenv("MEDQC_DB", "./medqc.db")).resolve()
CASES_ROOT = Path(os.getenv("MEDQC_CASES", "./cases")).resolve()

CASES_ROOT.mkdir(parents=True, exist_ok=True)

# ------------------ БД ------------------

def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

SCHEMA = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS docs (
  doc_id      TEXT PRIMARY KEY,
  sha256      TEXT NOT NULL,
  src_path    TEXT NOT NULL,
  mime        TEXT,
  size        INTEGER,
  facility    TEXT,
  dept        TEXT,
  author      TEXT,
  admit_dt    TEXT,
  created_at  TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_docs_sha256 ON docs(sha256);

CREATE TABLE IF NOT EXISTS artifacts (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id     TEXT NOT NULL,
  kind       TEXT NOT NULL,    -- e.g. 'source','raw','sections','entities'
  path       TEXT NOT NULL,
  sha256     TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS raw_text (
  doc_id     TEXT PRIMARY KEY,
  is_scanned INTEGER NOT NULL,
  pages      INTEGER,
  producer   TEXT,
  lang_hint  TEXT,
  full_text  TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS pages (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id  TEXT NOT NULL,
  pageno  INTEGER NOT NULL,
  start   INTEGER NOT NULL,
  end     INTEGER NOT NULL,
  text    TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS sections (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id      TEXT NOT NULL,
  section_id  TEXT NOT NULL,
  name        TEXT NOT NULL,
  kind        TEXT,
  start       INTEGER NOT NULL,
  end         INTEGER NOT NULL,
  pageno      INTEGER,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);

CREATE TABLE IF NOT EXISTS entities (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id      TEXT NOT NULL,
  section_id  TEXT,
  etype       TEXT NOT NULL,   -- 'datetime','diagnosis','medication','vital','signature',...
  start       INTEGER NOT NULL,
  end         INTEGER NOT NULL,
  value_json  TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
);
"""

def init_schema() -> None:
    with connect() as c:
        c.executescript(SCHEMA)
        c.commit()

# ------------------ Утилиты ------------------

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()

def guess_mime(path: Path) -> str:
    m, _ = mimetypes.guess_type(str(path))
    return m or 'application/octet-stream'

def ensure_doc_id() -> str:
    return f"KZ-{datetime.utcnow().strftime('%Y%m%d')}-{secrets.token_hex(4).upper()}"

# ------------------ docs ------------------

def find_doc_by_sha256(sha256: str) -> Optional[str]:
    with connect() as c:
        row = c.execute("SELECT doc_id FROM docs WHERE sha256=?", (sha256,)).fetchone()
        return row[0] if row else None

def insert_doc(doc_id: str, sha256: str, src_path: Path, mime: str, size: int,
               facility: Optional[str], dept: Optional[str], author: Optional[str],
               admit_dt: Optional[str]) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO docs(doc_id, sha256, src_path, mime, size, facility, dept, author, admit_dt, created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (doc_id, sha256, str(src_path), mime, size, facility, dept, author, admit_dt, now_iso()),
        )
        c.commit()

# ------------------ raw_text/pages ------------------

def upsert_raw_text(doc_id: str, is_scanned: bool, pages_count: int, producer: Optional[str],
                     lang_hint: Optional[str], full_text: str) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO raw_text(doc_id, is_scanned, pages, producer, lang_hint, full_text, created_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(doc_id) DO UPDATE SET
              is_scanned=excluded.is_scanned,
              pages=excluded.pages,
              producer=excluded.producer,
              lang_hint=excluded.lang_hint,
              full_text=excluded.full_text,
              created_at=excluded.created_at
            """,
            (doc_id, int(is_scanned), pages_count, producer, lang_hint, full_text, now_iso()),
        )
        c.commit()


def replace_pages(doc_id: str, pages: Iterable[Dict[str, Any]]) -> None:
    with connect() as c:
        c.execute("DELETE FROM pages WHERE doc_id=?", (doc_id,))
        c.executemany(
            "INSERT INTO pages(doc_id, pageno, start, end, text) VALUES(?,?,?,?,?)",
            ((doc_id, p["pageno"], p["start"], p["end"], p["text"]) for p in pages),
        )
        c.commit()

# ------------------ sections ------------------

def replace_sections(doc_id: str, sections: Iterable[Dict[str, Any]]) -> None:
    with connect() as c:
        c.execute("DELETE FROM sections WHERE doc_id=?", (doc_id,))
        c.executemany(
            """
            INSERT INTO sections(doc_id, section_id, name, kind, start, end, pageno)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                (doc_id, s["section_id"], s["name"], s.get("kind"), s["start"], s["end"], s.get("pageno"))
                for s in sections
            ),
        )
        c.commit()

# ------------------ entities ------------------

def replace_entities(doc_id: str, entities: Iterable[Dict[str, Any]]) -> None:
    with connect() as c:
        c.execute("DELETE FROM entities WHERE doc_id=?", (doc_id,))
        c.executemany(
            """
            INSERT INTO entities(doc_id, section_id, etype, start, end, value_json)
            VALUES(?,?,?,?,?,?)
            """,
            (
                (
                    doc_id,
                    e.get("section_id"),
                    e["etype"],
                    e["start"],
                    e["end"],
                    json.dumps(e["value"], ensure_ascii=False),
                )
                for e in entities
            ),
        )
        c.commit()

# ------------------ getters ------------------

def get_doc(doc_id: str) -> Optional[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()


def get_full_text(doc_id: str) -> Optional[str]:
    with connect() as c:
        row = c.execute("SELECT full_text FROM raw_text WHERE doc_id=?", (doc_id,)).fetchone()
        return row[0] if row else None


def get_sections(doc_id: str) -> list[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM sections WHERE doc_id=? ORDER BY start", (doc_id,)).fetchall()

# ------------------ storage ------------------

def ensure_case_dir(doc_id: str) -> Path:
    d = CASES_ROOT / doc_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def store_source(file_path: Path, doc_id: str) -> Path:
    dst_dir = ensure_case_dir(doc_id)
    dst = dst_dir / ("source" + file_path.suffix.lower())
    shutil.copy2(file_path, dst)
    return dst

# ------------------ high-level ingest ------------------

def ingest_local_file(path: Path, facility: Optional[str], dept: Optional[str],
                      author: Optional[str], admit_dt: Optional[str]) -> dict:
    init_schema()
    path = path.resolve()
    if not path.exists():
        return {"error": {"code": "NOT_FOUND", "message": f"no such file: {path}"}}
    # hash & duplicate check
    sha = file_sha256(path)
    existing = find_doc_by_sha256(sha)
    if existing:
        return {"doc_id": existing, "status": "duplicate", "sha256": sha}
    # new doc
    doc_id = ensure_doc_id()
    dst = store_source(path, doc_id)
    mime = guess_mime(dst)
    insert_doc(doc_id, sha, dst, mime, dst.stat().st_size, facility, dept, author, admit_dt)
    return {
        "doc_id": doc_id,
        "status": "ingested",
        "sha256": sha,
        "src_path": str(dst)
    }
```

---

## medqc\_ingest.py

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
from pathlib import Path
import medqc_db as db

def main():
    ap = argparse.ArgumentParser(description="medqc-ingest — приём документа")
    ap.add_argument("--in", dest="inp", required=True, help="Путь к PDF/DOCX")
    ap.add_argument("--facility", help="Код ЛПУ")
    ap.add_argument("--dept", help="Код отделения")
    ap.add_argument("--author", help="ФИО автора/оператора")
    ap.add_argument("--admit", help="Дата/время поступления, YYYY-MM-DD HH:MM")
    args = ap.parse_args()

    res = db.ingest_local_file(Path(args.inp), args.facility, args.dept, args.author, args.admit)
    import json
    print(json.dumps(res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
```

---

## medqc\_extract.py

```python
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
```

---

## medqc\_section.py

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, re, sys
import medqc_db as db

SECTION_PATTERNS = [
    ("Поступление", r"\b(Поступление|Госпитализац(ия|ии)|Время поступления)\b", "admit", 90),
    ("Триаж", r"\b(Триаж|Triage|Категория приоритета)\b", "triage", 80),
    ("Осмотр при поступлении", r"\b(Осмотр при поступлении|Первичный осмотр)\b", "initial_exam", 80),
    ("Ежедневная запись", r"\b(Ежедневн(ая|ые) запись|Дневниковая запись)\b", "daily_note", 50),
    ("План лечения", r"\b(План лечения|План обследования|План ведения)\b", "plan", 60),
    ("Лист назначений", r"\b(Лист назначений|Назначения|Ордер(-| )сет)\b", "orders", 70),
    ("Показатели здоровья", r"\b(Показатели здоровья|Температурный лист|Витальные|T°|ЧСС|АД|SpO₂)\b", "vitals", 40),
    ("ЭКГ", r"\b(ЭКГ|ECG)\b", "ecg", 60),
    ("Эпикриз", r"\b(Эпикриз|Выписной эпикриз|Переводной эпикриз)\b", "epicrisis", 70),
]


def main():
    ap = argparse.ArgumentParser(description="medqc-section — секционирование")
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()

    full = db.get_full_text(args.doc_id)
    if full is None:
        print(f"{{\"error\":{{\"code\":\"NO_TEXT\",\"message\":\"run medqc-extract first\"}}}}")
        sys.exit(1)

    # Собрать кандидаты: (name, kind, start, priority)
    candidates = []
    for name, rx, kind, prio in SECTION_PATTERNS:
        for m in re.finditer(rx, full, flags=re.I):
            candidates.append((name, kind, m.start(), prio))

    candidates.sort(key=lambda x: (x[2], -x[3]))
    # Строим непересекающиеся секции по первому в позиции (max priority уже учли)
    final = []
    taken_positions = []
    for name, kind, start, prio in candidates:
        if any(abs(start - s) < 2 for s in taken_positions):
            continue
        taken_positions.append(start)
        final.append((name, kind, start))
    final.sort(key=lambda x: x[2])

    # Завершаем границы end по следующему старту
    sections_rows = []
    for i, (name, kind, start) in enumerate(final):
        end = final[i+1][2] if i+1 < len(final) else len(full)
        sections_rows.append({
            "section_id": f"S{i+1}",
            "name": name,
            "kind": kind,
            "start": start,
            "end": end,
            "pageno": None
        })

    db.replace_sections(args.doc_id, sections_rows)

    import json
    print(json.dumps({
        "doc_id": args.doc_id,
        "status": "sectioned",
        "sections": len(sections_rows)
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
```

---

## medqc\_entities.py

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, re, json, sys
import medqc_db as db

# Дата/время: 21.08.2025, 21.08.2025 11:51, 11:51, 2025-08-21, 2025-08-21 11:51
DATE_RE = r"(?:(?:\d{2}[.]){2}\d{4}|\d{4}-\d{2}-\d{2})"
TIME_RE = r"\d{1,2}:\d{2}(?::\d{2})?"
DT_RE   = re.compile(rf"\b({DATE_RE}(?:\s+{TIME_RE})?|{TIME_RE})\b")

ICD_RE  = re.compile(r"\b([A-Z][0-9]{2}(?:\.[0-9A-Z]{1,4})?)\b")

DOSE_RE = re.compile(r"\b(\d+[\.,]?\d*)\s*(мг|г|мл|ЕД|IU|%)\b", re.I)
ROUTE_RE= re.compile(r"\b(в/в|в/м|п/о|перорально|сублингв|sublingual|п/к|ингаляционно)\b", re.I)
FREQ_RE = re.compile(r"\b((?:\d+\s*раз/сут)|(?:\d+\s*р/д)|(?:каждые\s*\d+\s*ч)|(?:q\d+h))\b", re.I)

TEMP_RE = re.compile(r"(?:T|Т|Температура)[^\n]{0,20}?(\d{1,2}[\.,]\d)")
BP_RE   = re.compile(r"(\d{2,3})\s*/\s*(\d{2,3})\s*(?:мм\s*рт\.?\s*ст\.?|mmHg)?", re.I)
SPO2_RE = re.compile(r"\b(SpO2|SpO₂)\s*[:=]?\s*(\d{2,3})\s*%\b", re.I)

SECTION_KIND_MAP = {
  "orders": "Лист назначений",
  "vitals": "Показатели здоровья",
  "initial_exam": "Осмотр при поступлении",
}


def main():
    ap = argparse.ArgumentParser(description="medqc-entities — извлечение сущностей")
    ap.add_argument("--doc-id", required=True)
    args = ap.parse_args()

    full = db.get_full_text(args.doc_id)
    if full is None:
        print(json.dumps({"error":{"code":"NO_TEXT","message":"run medqc-extract first"}}))
        sys.exit(1)

    sections = db.get_sections(args.doc_id)
    entities = []

    # 1) Дата/время — по всем секциям
    for s in sections:
        chunk = full[s["start"]:s["end"]]
        for m in DT_RE.finditer(chunk):
            start = s["start"] + m.start(1)
            end   = s["start"] + m.end(1)
            entities.append({
                "section_id": s["section_id"],
                "etype": "datetime",
                "start": start,
                "end": end,
                "value": {"raw": m.group(1)}
            })

    # 2) Диагнозы и коды МКБ — ищем по ключам и ICD-формату
    diag_keys = re.compile(r"\b(диагноз|заключительный диагноз|клинический диагноз)\b", re.I)
    for s in sections:
        chunk = full[s["start"]:s["end"]]
        if diag_keys.search(chunk) or ICD_RE.search(chunk):
            for m in ICD_RE.finditer(chunk):
                start = s["start"] + m.start(1)
                end   = s["start"] + m.end(1)
                entities.append({
                    "section_id": s["section_id"],
                    "etype": "diagnosis",
                    "start": start,
                    "end": end,
                    "value": {"icd": m.group(1)}
                })

    # 3) Назначения — разбираем построчно в секциях orders
    for s in sections:
        if s["kind"] != "orders":
            continue
        chunk = full[s["start"]:s["end"]]
        for line_m in re.finditer(r"[^\n]+", chunk):
            line = line_m.group(0).strip()
            if len(line) < 5:
                continue
            dose = DOSE_RE.search(line)
            route= ROUTE_RE.search(line)
            freq = FREQ_RE.search(line)
            if dose or route or freq:
                entities.append({
                    "section_id": s["section_id"],
                    "etype": "medication",
                    "start": s["start"] + line_m.start(0),
                    "end":   s["start"] + line_m.end(0),
                    "value": {
                        "line": line,
                        "dose": (dose.group(1).replace(',', '.') + " " + dose.group(2)) if dose else None,
                        "route": route.group(1) if route else None,
                        "freq": freq.group(1) if freq else None
                    }
                })

    # 4) Виталы (T, АД, SpO2) — секция vitals + весь документ на всякий
    def scan_vitals(text: str, offset: int, section_id: str):
        for m in TEMP_RE.finditer(text):
            entities.append({
                "section_id": section_id,
                "etype": "vital",
                "start": offset + m.start(1),
                "end":   offset + m.end(1),
                "value": {"kind": "temperature", "value": float(m.group(1).replace(',', '.')), "unit": "C"}
            })
        for m in BP_RE.finditer(text):
            entities.append({
                "section_id": section_id,
                "etype": "vital",
                "start": offset + m.start(1),
                "end":   offset + m.end(2),
                "value": {"kind": "blood_pressure", "syst": int(m.group(1)), "diast": int(m.group(2)), "unit": "mmHg"}
            })
        for m in SPO2_RE.finditer(text):
            entities.append({
                "section_id": section_id,
                "etype": "vital",
                "start": offset + m.start(2),
                "end":   offset + m.end(2),
                "value": {"kind": "spo2", "value": int(m.group(2)), "unit": "%"}
            })

    for s in sections:
        chunk = full[s["start"]:s["end"]]
        scan_vitals(chunk, s["start"], s["section_id"])

    # Запись в БД и вывод
    db.replace_entities(args.doc_id, entities)
    print(json.dumps({
        "doc_id": args.doc_id,
        "status": "entities",
        "entities": len(entities)
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
```

---

## Пример сценария запуска

```bash
# 0) окружение
export MEDQC_DB=./medqc.db
export MEDQC_CASES=./cases

# 1) ingest
python medqc_ingest.py --in "инфекция.pdf" \
  --facility PAVLODAR_HOSP1 --dept INF --author "Иванов И.И." \
  --admit "2025-08-21 11:51"
# => doc_id, например KZ-20250906-AB12CD34

# 2) extract
python medqc_extract.py --doc-id KZ-20250906-AB12CD34

# 3) section
python medqc_section.py --doc-id KZ-20250906-AB12CD34

# 4) entities
python medqc_entities.py --doc-id KZ-20250906-AB12CD34
```

## Что дальше

* Добавить `medqc_timeline.py` (объединение сущностей в события) и `medqc_rules.py` (движок правил) с чтением `rules.json`.
* Для DOCX постраничность можно приблизить через поиск разрывов разделов/таблиц.
* Для PDF определить `is_scanned` точнее (по отсутствию текстового слоя на страницах).
* Индексация «evidence»: хранить quote-кусок текста (start\:end) рядом с сущностью.
