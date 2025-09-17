#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import sqlite3
from typing import List, Dict, Any

DB_PATH = os.getenv("MEDQC_DB", "./medqc.db")

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def _columns(conn: sqlite3.Connection, table: str) -> List[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return [r[1] for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []

def _affinity(decl: str) -> str:
    d = (decl or "").upper()
    if "INT" in d: return "INTEGER"
    if "CHAR" in d or "CLOB" in d or "TEXT" in d: return "TEXT"
    if "BLOB" in d: return "BLOB"
    if "REAL" in d or "FLOA" in d or "DOUB" in d: return "REAL"
    return "NUMERIC"

def _aff_map(conn: sqlite3.Connection, table: str) -> Dict[str, str]:
    m: Dict[str,str] = {}
    for _, name, decl, *_ in conn.execute(f"PRAGMA table_info({table})"):
        m[name] = _affinity(decl or "")
    return m

def _now_for(aff: str):
    return int(time.time()) if aff in ("INTEGER","REAL","NUMERIC") else time.strftime("%Y-%m-%d %H:%M:%S")

def _json_for(aff: str, obj: Any):
    s = json.dumps(obj, ensure_ascii=False)
    return sqlite3.Binary(s.encode("utf-8")) if aff == "BLOB" else s

def migrate_schema(conn: sqlite3.Connection):
    if not _table_exists(conn, "norm_packages"):
        conn.execute("""
        CREATE TABLE norm_packages(
          pkg_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          version TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'draft',
          locked INTEGER NOT NULL DEFAULT 0,
          digest TEXT,
          meta_json TEXT,
          created_at TEXT,
          title TEXT,
          description TEXT,
          active INTEGER NOT NULL DEFAULT 0
        );""")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_norm_packages_name_ver ON norm_packages(name,version)")

    if not _table_exists(conn, "norm_rules"):
        conn.execute("""
        CREATE TABLE norm_rules(
          id INTEGER PRIMARY KEY,
          pkg_id INTEGER NOT NULL,
          rule_id TEXT NOT NULL,
          title TEXT, profile TEXT, severity TEXT,
          params_json TEXT, sources_json TEXT,
          effective_from TEXT, effective_to TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          notes TEXT,
          created_at TEXT NOT NULL,
          package_name TEXT,
          package_version TEXT
        );""")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_norm_rules_key ON norm_rules(rule_id, package_name, package_version)")

    # мягко добавим недостающие поля
    def _add_missing(table: str, col: str, decl: str):
        if col not in _columns(conn, table):
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
            except sqlite3.OperationalError as e:
                print(f"[migrate] note: {table}.{col}: {e}")

    for col, decl in [
        ("title","TEXT"), ("description","TEXT"), ("created_at","TEXT"), ("active","INTEGER"),
        ("status","TEXT"), ("locked","INTEGER"), ("digest","TEXT"), ("meta_json","TEXT")
    ]:
        _add_missing("norm_packages", col, decl)

    for col, decl in [
        ("pkg_id","INTEGER"), ("rule_id","TEXT"),
        ("title","TEXT"), ("profile","TEXT"), ("severity","TEXT"),
        ("params_json","TEXT"), ("sources_json","TEXT"),
        ("effective_from","TEXT"), ("effective_to","TEXT"),
        ("enabled","INTEGER"), ("notes","TEXT"),
        ("created_at","TEXT"), ("package_name","TEXT"), ("package_version","TEXT")
    ]:
        _add_missing("norm_rules", col, decl)

    # инициализация created_at
    for t in ("norm_packages","norm_rules"):
        aff = _aff_map(conn, t)
        nowv = _now_for(aff.get("created_at","TEXT"))
        conn.execute(f"UPDATE {t} SET created_at=COALESCE(created_at,?)", (nowv,))
    conn.commit()

def _get_pkg_id(conn: sqlite3.Connection, name: str, version: str) -> int:
    cur = conn.execute("SELECT COALESCE(pkg_id, rowid) FROM norm_packages WHERE name=? AND version=? LIMIT 1", (name, version))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else -1

def upsert_package(conn: sqlite3.Connection, name: str, version: str, title: str, description: str) -> int:
    conn.execute("""
        UPDATE norm_packages
           SET title=?, description=?
         WHERE name=? AND version=?
    """, (title or "", description or "", name, version))
    if conn.total_changes == 0:
        aff = _aff_map(conn, "norm_packages")
        created_at = _now_for(aff.get("created_at","TEXT"))
        conn.execute("""
            INSERT INTO norm_packages(name, version, title, description, created_at, active)
            VALUES(?,?,?,?,?,0)
        """, (name, version, title or "", description or "", created_at))
    return _get_pkg_id(conn, name, version)

def upsert_rule(conn: sqlite3.Connection,
                pkg_id: int, package_name: str, package_version: str,
                rule_id: str, profile: str, title: str,
                severity: Any, enabled: Any,
                params_obj: dict, sources_obj: list):
    aff = _aff_map(conn, "norm_rules")
    created_at = _now_for(aff.get("created_at","TEXT"))
    params_val  = _json_for(aff.get("params_json","TEXT"),  params_obj or {})
    sources_val = _json_for(aff.get("sources_json","TEXT"), sources_obj or [])
    sev_val = str(severity) if severity is not None else "minor"
    en_val  = int(enabled)

    cur = conn.execute("""
        UPDATE norm_rules
           SET pkg_id=?, profile=?, title=?, severity=?, enabled=?,
               params_json=?, sources_json=?,
               package_name=?, package_version=?
         WHERE rule_id=? AND COALESCE(package_name,'')=? AND COALESCE(package_version,'')=?
    """, (pkg_id, profile or "", title or "", sev_val, en_val,
          params_val, sources_val, package_name, package_version,
          rule_id, package_name, package_version))
    if cur.rowcount == 0:
        conn.execute("""
            INSERT INTO norm_rules(
              pkg_id, rule_id, title, profile, severity,
              params_json, sources_json,
              enabled, created_at, package_name, package_version
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (pkg_id, rule_id, title or "", profile or "", sev_val,
              params_val, sources_val, en_val, created_at, package_name, package_version))

def cmd_migrate():
    with sqlite3.connect(DB_PATH) as conn:
        migrate_schema(conn)
    print(json.dumps({"status": "migrated"}, ensure_ascii=False))

def cmd_import(json_path: str):
    with sqlite3.connect(DB_PATH) as conn:
        migrate_schema(conn)
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        pkg_name = data["package"]
        pkg_version = data["version"]
        title = data.get("title","")
        description = data.get("description","")
        rules = data["rules"]

        pkg_id = upsert_package(conn, pkg_name, pkg_version, title, description)
        for r in rules:
            upsert_rule(
                conn=conn,
                pkg_id=pkg_id,
                package_name=pkg_name,
                package_version=pkg_version,
                rule_id=r["id"],
                profile=r["profile"],
                title=r.get("title", r["id"]),
                severity=r.get("severity","minor"),
                enabled=1 if r.get("enabled", True) else 0,
                params_obj=r.get("params", {}),
                sources_obj=r.get("sources", [])
            )
        conn.commit()
    print(json.dumps({"package": pkg_name, "version": pkg_version, "rules": len(rules), "status": "imported"}, ensure_ascii=False))

def cmd_set_active(name: str, version: str):
    with sqlite3.connect(DB_PATH) as conn:
        migrate_schema(conn)
        conn.execute("UPDATE norm_packages SET active=0")
        conn.execute("UPDATE norm_packages SET active=1 WHERE name=? AND version=?", (name, version))
        conn.commit()
    print(json.dumps({"package": name, "version": version, "active": True}, ensure_ascii=False))

def cmd_list_packages():
    with sqlite3.connect(DB_PATH) as conn:
        migrate_schema(conn)
        cur = conn.execute("""
            SELECT COALESCE(pkg_id,rowid) AS id, name, version, title, active, created_at
              FROM norm_packages
             ORDER BY name, version
        """)
        rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    print(json.dumps(rows, ensure_ascii=False, indent=2))

def cmd_list_rules(name: str, version: str):
    with sqlite3.connect(DB_PATH) as conn:
        migrate_schema(conn)
        cur = conn.execute("""
            SELECT rule_id, profile, title, severity, enabled
              FROM norm_rules
             WHERE package_name=? AND package_version=?
          ORDER BY profile, rule_id
        """, (name, version))
        rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    print(json.dumps(rows, ensure_ascii=False, indent=2))

def main():
    import argparse
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("migrate", help="Миграция схемы norm_* (мягкая)")

    p_imp = sub.add_parser("import", help="Импорт rules.json в БД")
    p_imp.add_argument("--file", required=True)

    p_act = sub.add_parser("set-active", help="Сделать пакет активным")
    p_act.add_argument("--name", required=True)
    p_act.add_argument("--version", required=True)

    sub.add_parser("list-packages", help="Список пакетов")
    p_lr = sub.add_parser("list-rules", help="Список правил пакета")
    p_lr.add_argument("--name", required=True)
    p_lr.add_argument("--version", required=True)

    args = ap.parse_args()

    if args.cmd == "migrate":
        cmd_migrate()
    elif args.cmd == "import":
        cmd_import(args.file)
    elif args.cmd == "set-active":
        cmd_set_active(args.name, args.version)
    elif args.cmd == "list-packages":
        cmd_list_packages()
    elif args.cmd == "list-rules":
        cmd_list_rules(args.name, args.version)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
