#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
medqc-norms-admin — CLI для управления нормативной базой правил.

Назначение:
- Вести пакеты правил (name+version) в SQLite: импорт/редактирование/включение/выгрузка.
- Импорт из JSON (формата rules.json) и CSV (простая таблица).
- Компиляция включённых правил в единый rules.json для medqc-rules.
- Верификация полей и простая схема версионирования (draft → frozen).

БД: используется тот же SQLite, что и у остальных программ (MEDQC_DB или ./medqc.db).
Схема хранится локально в этом скрипте (таблицы norm_packages, norm_rules).

Примеры:
  # инициализация
  python medqc_norms_admin.py init

  # импорт JSON-пакета
  python medqc_norms_admin.py import-json --in rules.json --name rules-pack-stationary-er --version 2025-09-07

  # импорт CSV (см. формат ниже)
  python medqc_norms_admin.py import-csv --in rules.csv --name rules-pack-stationary --version 2025-09-07

  # список пакетов и правил в пакете
  python medqc_norms_admin.py list-packages
  python medqc_norms_admin.py list-rules --name rules-pack-stationary --version 2025-09-07

  # правки
  python medqc_norms_admin.py set-param --name rules-pack-stationary --version 2025-09-07 \
      --rule STA-002 --key "ПЕРВИЧНЫЙ_ОСМОТР_ЧАСОВ" --value 6
  python medqc_norms_admin.py set-severity --name rules-pack-stationary --version 2025-09-07 \
      --rule STA-006 --severity major
  python medqc_norms_admin.py disable --name rules-pack-stationary --version 2025-09-07 --rule ER-004

  # компиляция
  python medqc_norms_admin.py export --name rules-pack-stationary-er --version 2025-09-07 --out compiled_rules.json

CSV-формат (минимум столбцов):
  rule_id,profile,title,severity,params_json,order_no,order_date,clause,effective_from,effective_to,enabled
- params_json — JSON-объект (например: {"TRIAGE_MAX_MIN":15})
- поля источника можно задать одной строкой; будут собраны в массив из одного элемента.
"""
from __future__ import annotations
import os, sys, json, csv, hashlib
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
import sqlite3

DB_PATH = Path(os.getenv("MEDQC_DB", "./medqc.db")).resolve()

# ---------------------- соединение и схема ----------------------

def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

SCHEMA = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS norm_packages (
  pkg_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL,
  version    TEXT NOT NULL,
  status     TEXT NOT NULL DEFAULT 'draft',   -- draft|frozen
  locked     INTEGER NOT NULL DEFAULT 0,
  digest     TEXT,                             -- sha256 от экспортированного файла
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
  params_json    TEXT,        -- JSON объект
  sources_json   TEXT,        -- JSON массив
  effective_from TEXT,
  effective_to   TEXT,
  enabled        INTEGER NOT NULL DEFAULT 1,
  notes          TEXT,
  created_at     TEXT NOT NULL,
  FOREIGN KEY(pkg_id) REFERENCES norm_packages(pkg_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_norm_rule ON norm_rules(pkg_id, rule_id);
CREATE INDEX IF NOT EXISTS idx_norm_rule_pkg ON norm_rules(pkg_id);
"""

# ---------------------- утилиты ----------------------

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

class UserError(Exception):
    pass

# ---------------------- пакет: CRUD ----------------------

def ensure_schema() -> None:
    with connect() as c:
        c.executescript(SCHEMA)
        c.commit()

def get_pkg(name: str, version: str) -> Optional[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM norm_packages WHERE name=? AND version=?", (name, version)).fetchone()

def create_pkg(name: str, version: str, meta: Optional[dict] = None) -> int:
    ensure_schema()
    with connect() as c:
        c.execute(
            """
            INSERT INTO norm_packages(name, version, status, locked, digest, meta_json, created_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (name, version, 'draft', 0, None, json.dumps(meta or {}, ensure_ascii=False), now_iso()),
        )
        c.commit()
        row = c.execute("SELECT pkg_id FROM norm_packages WHERE name=? AND version=?", (name, version)).fetchone()
        return int(row[0])

# ---------------------- правила: CRUD ----------------------

def upsert_rule(pkg_id: int, rule: dict) -> None:
    # normalize
    rid = rule.get('id') or rule.get('rule_id')
    if not rid:
        raise UserError("Правило без 'id' невозможно сохранить")
    title = rule.get('title')
    profile = rule.get('profile')
    severity = rule.get('severity')
    params_json = json.dumps(rule.get('params') or {}, ensure_ascii=False)
    sources_json = json.dumps(rule.get('sources') or [], ensure_ascii=False)
    eff_from = rule.get('effective_from')
    eff_to = rule.get('effective_to')
    enabled = 0 if (str(rule.get('enabled')).lower() in ('0','false','no')) else 1
    notes = rule.get('notes')
    with connect() as c:
        # попробуем обновить, если есть
        cur = c.execute("SELECT id FROM norm_rules WHERE pkg_id=? AND rule_id=?", (pkg_id, rid)).fetchone()
        if cur:
            c.execute(
                """
                UPDATE norm_rules SET title=?, profile=?, severity=?, params_json=?, sources_json=?,
                                      effective_from=?, effective_to=?, enabled=?, notes=?
                WHERE pkg_id=? AND rule_id=?
                """,
                (title, profile, severity, params_json, sources_json, eff_from, eff_to, enabled, notes, pkg_id, rid),
            )
        else:
            c.execute(
                """
                INSERT INTO norm_rules(pkg_id, rule_id, title, profile, severity, params_json, sources_json,
                                       effective_from, effective_to, enabled, notes, created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (pkg_id, rid, title, profile, severity, params_json, sources_json, eff_from, eff_to, enabled, notes, now_iso()),
            )
        c.commit()

# ---------------------- команды ----------------------

def cmd_init(args):
    ensure_schema()
    print(json.dumps({"status":"ok","db":str(DB_PATH)}, ensure_ascii=False))


def cmd_import_json(args):
    ensure_schema()
    path = Path(args.inp)
    data = json.loads(path.read_text(encoding='utf-8'))
    name = args.name or data.get('package') or data.get('name') or 'rules-pack'
    version = args.version or datetime.utcnow().strftime('%Y-%m-%d')
    pkg = get_pkg(name, version)
    if not pkg:
        pkg_id = create_pkg(name, version, meta={"source": str(path)})
    else:
        pkg_id = int(pkg['pkg_id'])
    rules = data.get('rules') or []
    for r in rules:
        upsert_rule(pkg_id, r)
    print(json.dumps({"status":"ok","imported":len(rules),"package":name,"version":version}, ensure_ascii=False))


def cmd_import_csv(args):
    ensure_schema()
    path = Path(args.inp)
    name = args.name or 'rules-pack'
    version = args.version or datetime.utcnow().strftime('%Y-%m-%d')
    pkg = get_pkg(name, version)
    if not pkg:
        pkg_id = create_pkg(name, version, meta={"source": str(path)})
    else:
        pkg_id = int(pkg['pkg_id'])
    cnt = 0
    with path.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rule = {
                'id': row.get('rule_id') or row.get('id'),
                'profile': row.get('profile'),
                'title': row.get('title'),
                'severity': row.get('severity'),
                'params': json.loads(row.get('params_json') or '{}'),
                'sources': [] if not (row.get('order_no') or row.get('order_date') or row.get('clause')) else [
                    {"order_no": row.get('order_no'), "date": row.get('order_date'), "clause": row.get('clause')}
                ],
                'effective_from': row.get('effective_from') or None,
                'effective_to': row.get('effective_to') or None,
                'enabled': row.get('enabled') if row.get('enabled') is not None else '1',
                'notes': row.get('notes')
            }
            if not rule['id']:
                continue
            upsert_rule(pkg_id, rule)
            cnt += 1
    print(json.dumps({"status":"ok","imported":cnt,"package":name,"version":version}, ensure_ascii=False))


def _resolve_pkg_or_fail(name: str, version: str) -> sqlite3.Row:
    pkg = get_pkg(name, version)
    if not pkg:
        raise UserError(f"Пакет не найден: {name}@{version}")
    return pkg


def cmd_list_packages(args):
    ensure_schema()
    with connect() as c:
        rows = c.execute(
            """
            SELECT p.pkg_id, p.name, p.version, p.status, p.locked, p.created_at,
                   COALESCE((SELECT COUNT(1) FROM norm_rules r WHERE r.pkg_id=p.pkg_id), 0) AS rules_count
            FROM norm_packages p
            ORDER BY p.name, p.version
            """
        ).fetchall()
    out = [dict(row) for row in rows]
    print(json.dumps(out, ensure_ascii=False, indent=2))


def cmd_list_rules(args):
    pkg = _resolve_pkg_or_fail(args.name, args.version)
    with connect() as c:
        if args.profile:
            rows = c.execute("SELECT rule_id, title, profile, severity, enabled FROM norm_rules WHERE pkg_id=? AND profile=? ORDER BY rule_id",
                             (pkg['pkg_id'], args.profile)).fetchall()
        else:
            rows = c.execute("SELECT rule_id, title, profile, severity, enabled FROM norm_rules WHERE pkg_id=? ORDER BY rule_id",
                             (pkg['pkg_id'],)).fetchall()
    out = [dict(row) for row in rows]
    print(json.dumps({"package": args.name, "version": args.version, "rules": out}, ensure_ascii=False, indent=2))


def cmd_show_rule(args):
    pkg = _resolve_pkg_or_fail(args.name, args.version)
    with connect() as c:
        row = c.execute("SELECT * FROM norm_rules WHERE pkg_id=? AND rule_id=?", (pkg['pkg_id'], args.rule)).fetchone()
    if not row:
        raise UserError("Правило не найдено")
    d = dict(row)
    # prettify json fields
    for k in ("params_json", "sources_json"):
        if d.get(k):
            try:
                d[k] = json.loads(d[k])
            except Exception:
                pass
    print(json.dumps(d, ensure_ascii=False, indent=2))


def cmd_set_param(args):
    pkg = _resolve_pkg_or_fail(args.name, args.version)
    with connect() as c:
        row = c.execute("SELECT id, params_json FROM norm_rules WHERE pkg_id=? AND rule_id=?", (pkg['pkg_id'], args.rule)).fetchone()
        if not row:
            raise UserError("Правило не найдено")
        params = {}
        if row['params_json']:
            try:
                params = json.loads(row['params_json'])
            except Exception:
                params = {}
        # тип значения: int/float/bool/str — автоопределение
        val: Any = args.value
        if args.type == 'int':
            val = int(args.value)
        elif args.type == 'float':
            val = float(args.value)
        elif args.type == 'bool':
            val = str(args.value).lower() in ('1','true','yes','y')
        params[args.key] = val
        c.execute("UPDATE norm_rules SET params_json=? WHERE id=?", (json.dumps(params, ensure_ascii=False), row['id']))
        c.commit()
    print(json.dumps({"status":"ok","rule":args.rule,"key":args.key,"value":val}, ensure_ascii=False))


def cmd_set_severity(args):
    pkg = _resolve_pkg_or_fail(args.name, args.version)
    with connect() as c:
        c.execute("UPDATE norm_rules SET severity=? WHERE pkg_id=? AND rule_id=?", (args.severity, pkg['pkg_id'], args.rule))
        c.commit()
    print(json.dumps({"status":"ok","rule":args.rule,"severity":args.severity}, ensure_ascii=False))


def cmd_enable_disable(args, enable: bool):
    pkg = _resolve_pkg_or_fail(args.name, args.version)
    with connect() as c:
        c.execute("UPDATE norm_rules SET enabled=? WHERE pkg_id=? AND rule_id=?", (1 if enable else 0, pkg['pkg_id'], args.rule))
        c.commit()
    print(json.dumps({"status":"ok","rule":args.rule,"enabled":bool(enable)}, ensure_ascii=False))


def cmd_add_rule(args):
    pkg = _resolve_pkg_or_fail(args.name, args.version)
    rule = {
        'id': args.rule,
        'title': args.title,
        'profile': args.profile,
        'severity': args.severity,
        'params': json.loads(args.params) if args.params else {},
        'sources': json.loads(args.sources) if args.sources else [],
        'effective_from': args.effective_from,
        'effective_to': args.effective_to,
        'enabled': True,
        'notes': args.notes
    }
    upsert_rule(int(pkg['pkg_id']), rule)
    print(json.dumps({"status":"ok","added":args.rule}, ensure_ascii=False))


def compile_rules_dict(pkg_row: sqlite3.Row, profiles: Optional[List[str]] = None, include_disabled=False) -> Dict[str, Any]:
    with connect() as c:
        if profiles:
            qmarks = ",".join(["?"]*len(profiles))
            rows = c.execute(f"SELECT * FROM norm_rules WHERE pkg_id=? AND profile IN ({qmarks}) ORDER BY rule_id", [pkg_row['pkg_id'], *profiles]).fetchall()
        else:
            rows = c.execute("SELECT * FROM norm_rules WHERE pkg_id=? ORDER BY rule_id", (pkg_row['pkg_id'],)).fetchall()
    rules: List[Dict[str, Any]] = []
    for r in rows:
        if not include_disabled and int(r['enabled']) == 0:
            continue
        item = {
            'id': r['rule_id'],
            'title': r['title'],
            'profile': r['profile'],
            'severity': r['severity'],
            'params': json.loads(r['params_json']) if r['params_json'] else {},
            'sources': json.loads(r['sources_json']) if r['sources_json'] else [],
        }
        if r['effective_from']:
            item['effective_from'] = r['effective_from']
        if r['effective_to']:
            item['effective_to'] = r['effective_to']
        rules.append(item)
    return {
        'schema_version': '1.0',
        'package': f"{pkg_row['name']}",
        'version': f"{pkg_row['version']}",
        'generated_at': now_iso(),
        'rules': rules
    }


def cmd_export(args):
    pkg = _resolve_pkg_or_fail(args.name, args.version)
    profiles = args.profiles.split(',') if args.profiles else None
    bundle = compile_rules_dict(pkg, profiles=profiles, include_disabled=args.include_disabled)
    out_path = Path(args.out)
    out_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding='utf-8')
    digest = hashlib.sha256(out_path.read_bytes()).hexdigest()
    # запишем digest и frozen (если указан --freeze)
    with connect() as c:
        c.execute("UPDATE norm_packages SET digest=?, status=?, locked=? WHERE pkg_id=?",
                  (digest, 'frozen' if args.freeze else pkg['status'], 1 if args.freeze else pkg['locked'], pkg['pkg_id']))
        c.commit()
    print(json.dumps({"status":"ok","out":str(out_path),"sha256":digest,"frozen":bool(args.freeze)}, ensure_ascii=False))


def cmd_diff(args):
    pkg_a = _resolve_pkg_or_fail(args.name_a, args.version_a)
    pkg_b = _resolve_pkg_or_fail(args.name_b, args.version_b)
    with connect() as c:
        rows_a = c.execute("SELECT rule_id, params_json, severity, enabled, sources_json FROM norm_rules WHERE pkg_id=?", (pkg_a['pkg_id'],)).fetchall()
        rows_b = c.execute("SELECT rule_id, params_json, severity, enabled, sources_json FROM norm_rules WHERE pkg_id=?", (pkg_b['pkg_id'],)).fetchall()
    map_a = {r['rule_id']: dict(r) for r in rows_a}
    map_b = {r['rule_id']: dict(r) for r in rows_b}
    added = [rid for rid in map_b.keys() if rid not in map_a]
    removed = [rid for rid in map_a.keys() if rid not in map_b]
    changed: List[str] = []
    for rid in map_a.keys() & map_b.keys():
        a = map_a[rid]; b = map_b[rid]
        if (a['params_json'] or '') != (b['params_json'] or '') or (a['severity'] or '') != (b['severity'] or '') or int(a['enabled']) != int(b['enabled']) or (a['sources_json'] or '') != (b['sources_json'] or ''):
            changed.append(rid)
    print(json.dumps({
        'left': f"{pkg_a['name']}@{pkg_a['version']}",
        'right': f"{pkg_b['name']}@{pkg_b['version']}",
        'added': sorted(added),
        'removed': sorted(removed),
        'changed': sorted(changed)
    }, ensure_ascii=False, indent=2))

# ---------------------- CLI ----------------------
import argparse

def main():
    ap = argparse.ArgumentParser(description='medqc-norms-admin — управление нормативной базой')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser('init', help='инициализировать БД (таблицы norm_*)')
    p.set_defaults(func=cmd_init)

    p = sub.add_parser('import-json', help='импортировать пакет из rules.json')
    p.add_argument('--in', dest='inp', required=True)
    p.add_argument('--name')
    p.add_argument('--version')
    p.set_defaults(func=cmd_import_json)

    p = sub.add_parser('import-csv', help='импортировать правила из CSV')
    p.add_argument('--in', dest='inp', required=True)
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.set_defaults(func=cmd_import_csv)

    p = sub.add_parser('list-packages', help='список пакетов')
    p.set_defaults(func=cmd_list_packages)

    p = sub.add_parser('list-rules', help='список правил в пакете')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--profile', help='фильтр по профилю')
    p.set_defaults(func=cmd_list_rules)

    p = sub.add_parser('show', help='показать правило')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--rule', required=True)
    p.set_defaults(func=cmd_show_rule)

    p = sub.add_parser('set-param', help='установить параметр правила')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--rule', required=True)
    p.add_argument('--key', required=True)
    p.add_argument('--value', required=True)
    p.add_argument('--type', choices=['str','int','float','bool'], default='str')
    p.set_defaults(func=cmd_set_param)

    p = sub.add_parser('set-severity', help='сменить серьёзность правила')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--rule', required=True)
    p.add_argument('--severity', required=True, choices=['critical','major','minor'])
    p.set_defaults(func=cmd_set_severity)

    p = sub.add_parser('enable', help='включить правило')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--rule', required=True)
    p.set_defaults(func=lambda a: cmd_enable_disable(a, True))

    p = sub.add_parser('disable', help='выключить правило')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--rule', required=True)
    p.set_defaults(func=lambda a: cmd_enable_disable(a, False))

    p = sub.add_parser('add-rule', help='добавить новое правило')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--rule', required=True)
    p.add_argument('--title', required=True)
    p.add_argument('--profile', required=True)
    p.add_argument('--severity', required=True, choices=['critical','major','minor'])
    p.add_argument('--params', help='JSON-объект')
    p.add_argument('--sources', help='JSON-массив источников')
    p.add_argument('--effective-from')
    p.add_argument('--effective-to')
    p.add_argument('--notes')
    p.set_defaults(func=cmd_add_rule)

    p = sub.add_parser('export', help='скомпилировать пакет в rules.json')
    p.add_argument('--name', required=True)
    p.add_argument('--version', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--profiles', help='через запятую; по умолчанию все')
    p.add_argument('--include-disabled', action='store_true')
    p.add_argument('--freeze', action='store_true', help='пометить пакет frozen и записать sha256')
    p.set_defaults(func=cmd_export)

    p = sub.add_parser('diff', help='сравнить два пакета правил')
    p.add_argument('--name-a', required=True); p.add_argument('--version-a', required=True)
    p.add_argument('--name-b', required=True); p.add_argument('--version-b', required=True)
    p.set_defaults(func=cmd_diff)

    args = ap.parse_args()
    try:
        args.func(args)
    except UserError as e:
        print(json.dumps({"error": str(e)}, ensure_ascii=False))
        sys.exit(2)

if __name__ == '__main__':
    main()
