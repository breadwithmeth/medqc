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