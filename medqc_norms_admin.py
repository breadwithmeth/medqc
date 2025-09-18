# medqc_norms_admin.py
# Импорт правил из rules.json и активация пакета

import json
from typing import Any, Dict, List, Tuple
import sqlite3

from medqc_db import get_cursor, row_to_dict, set_active_rules_package

def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def migrate(conn: sqlite3.Connection, rules_path: str) -> Dict[str, Any]:
    """
    Импортирует правила из rules.json в таблицы rules_meta и rules.
    Активирует импортированный пакет.
    """
    with open(rules_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    package = data.get("package")
    version = data.get("version")
    title = data.get("title")
    description = data.get("description")
    rules_list = data.get("rules", [])

    if not package or not version:
        raise ValueError("rules.json must contain 'package' and 'version'")

    # upsert в rules_meta
    with get_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO rules_meta (package, version, title, description, active)
            VALUES (?, ?, ?, ?, 0)
            ON CONFLICT(package, version) DO UPDATE SET
              title=excluded.title,
              description=excluded.description
            """,
            (package, version, title, description),
        )
        conn.commit()

    # вставка/обновление правил
    inserted = 0
    updated = 0
    with get_cursor(conn) as cur:
        for r in rules_list:
            rule_id = r.get("id") or r.get("rule_id")
            if not rule_id:
                continue
            params_json = _json(r.get("params") or {})
            sources_json = _json(r.get("sources") or [])

            try:
                cur.execute(
                    """
                    INSERT INTO rules (
                      rule_id, package, version, title, profile, severity, enabled,
                      params_json, sources_json, effective_from, effective_to, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rule_id,
                        package,
                        version,
                        r.get("title"),
                        r.get("profile"),
                        r.get("severity"),
                        1 if r.get("enabled", True) else 0,
                        params_json,
                        sources_json,
                        r.get("effective_from"),
                        r.get("effective_to"),
                        r.get("notes"),
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                cur.execute(
                    """
                    UPDATE rules
                    SET title=?, profile=?, severity=?, enabled=?,
                        params_json=?, sources_json=?, effective_from=?, effective_to=?, notes=?
                    WHERE rule_id=? AND package=? AND version=?
                    """,
                    (
                        r.get("title"),
                        r.get("profile"),
                        r.get("severity"),
                        1 if r.get("enabled", True) else 0,
                        params_json,
                        sources_json,
                        r.get("effective_from"),
                        r.get("effective_to"),
                        r.get("notes"),
                        rule_id,
                        package,
                        version,
                    ),
                )
                updated += 1
        conn.commit()

    # активируем пакет
    set_active_rules_package(conn, package, version)

    return {
        "package": package,
        "version": version,
        "rules": len(rules_list),
        "inserted": inserted,
        "updated": updated,
        "status": "imported_and_activated"
    }
