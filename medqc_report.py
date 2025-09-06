#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
medqc-report — генератор человекочитаемого отчёта по одному документу.

Данные берутся из SQLite (через medqc_db): docs, sections, entities, events, violations.
Вывод: HTML (по умолчанию) или JSON/Markdown. Файл складывается в cases/<doc_id>/report.*
Также создаётся запись в таблице artifacts(kind='report').

Примеры запуска:
  python medqc_report.py --doc-id KZ-20250906-17E5F4FA
  python medqc_report.py --doc-id KZ-20250906-17E5F4FA --format json
  python medqc_report.py --doc-id KZ-20250906-17E5F4FA --out ./out/report.html

Зависимости: только стандартная библиотека + локальный модуль medqc_db.py
"""
from __future__ import annotations
import argparse, json, os, sqlite3, html
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import medqc_db as db

# ---------------------------- утилиты ----------------------------

def row_get(row, key, default=None):
    try:
        return row[key]
    except Exception:
        return default


def parse_iso_any(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s2 = s.replace("Z", "").replace(" ", "T")
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        return None


def day_span(start_dt: datetime, end_dt: datetime) -> List[date]:
    ds: List[date] = []
    d = start_dt.date()
    while d <= end_dt.date():
        ds.append(d)
        d = date.fromordinal(d.toordinal() + 1)
    return ds

# ---------------------------- чтение данных ----------------------------

def safe_get_violations(doc_id: str) -> List[sqlite3.Row]:
    try:
        with db.connect() as c:
            return c.execute(
                "SELECT * FROM violations WHERE doc_id=? ORDER BY severity DESC, rule_id",
                (doc_id,),
            ).fetchall()
    except sqlite3.OperationalError:
        return []


# ---------------------------- сборка сводки ----------------------------

def compute_stats(doc, sections, entities, events, violations) -> Dict[str, Any]:
    etype_counts: Dict[str, int] = {}
    for e in entities:
        etype = row_get(e, "etype")
        etype_counts[etype] = etype_counts.get(etype, 0) + 1

    event_counts: Dict[str, int] = {}
    last_ts = None
    for ev in events:
        k = row_get(ev, "kind")
        event_counts[k] = event_counts.get(k, 0) + 1
        t = row_get(ev, "ts") or row_get(ev, "when")
        dt = parse_iso_any(t)
        if dt and (not last_ts or dt > last_ts):
            last_ts = dt

    sev_counts = {"critical": 0, "major": 0, "minor": 0}
    for v in violations:
        sev = (row_get(v, "severity") or "minor").lower()
        if sev not in sev_counts:
            sev_counts[sev] = 0
        sev_counts[sev] += 1

    admit = parse_iso_any(row_get(doc, "admit_dt")) if row_get(doc, "admit_dt") else None

    # покрытие ежедневными записями
    daily_covered = None
    if admit and last_ts:
        days = day_span(admit, last_ts)
        marks = {d: False for d in days}
        for ev in events:
            if row_get(ev, "kind") != "daily_note":
                continue
            ts = parse_iso_any(row_get(ev, "ts") or row_get(ev, "when"))
            if ts and ts.date() in marks:
                marks[ts.date()] = True
        daily_covered = {
            "days_total": len(days),
            "days_with_note": sum(1 for v in marks.values() if v),
            "days_missing": [d.isoformat() for d, ok in marks.items() if not ok],
        }

    return {
        "etype_counts": etype_counts,
        "event_counts": event_counts,
        "last_ts": last_ts.isoformat() if last_ts else None,
        "sev_counts": sev_counts,
        "daily_covered": daily_covered,
    }


# ---------------------------- генерация HTML ----------------------------

def html_escape(s: Any) -> str:
    return html.escape(str(s))


def render_html(doc, sections, entities, events, violations, stats: Dict[str, Any]) -> str:
    def ths(cols):
        return "".join(f"<th>{html_escape(c)}</th>" for c in cols)
    def tds(cols):
        return "".join(f"<td>{html_escape(c) if c is not None else ''}</td>" for c in cols)

    doc_id = row_get(doc, "doc_id")
    head = f"""
<!doctype html>
<html lang=ru>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Отчёт medqc — {html_escape(doc_id)}</title>
<style>
  :root {{ --fg:#111; --muted:#666; --bg:#fff; --acc:#0b6cff; --crit:#d92b2b; --maj:#d98e2b; --min:#4a9c2b; }}
  body {{ font: 14px/1.45 system-ui, -apple-system, Segoe UI, Roboto, sans-serif; color:var(--fg); background:var(--bg); margin: 24px; }}
  h1 {{ font-size: 20px; margin: 0 0 8px; }}
  h2 {{ font-size: 16px; margin: 24px 0 8px; }}
  .muted {{ color: var(--muted); }}
  .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 12px; }}
  .card {{ border:1px solid #e6e6e6; border-radius:12px; padding:12px; box-shadow:0 1px 2px rgba(0,0,0,.03); }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 6px 8px; border-bottom: 1px solid #eee; text-align: left; vertical-align: top; }}
  th {{ background:#fafafa; position: sticky; top:0; z-index:10; }}
  code, .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
  .pill {{ display:inline-block; padding:2px 8px; border-radius:999px; font-weight:600; font-size:12px; }}
  .crit {{ background:#fde3e3; color:#7a0c0c; }}
  .maj  {{ background:#fff1db; color:#7a4b0c; }}
  .min  {{ background:#eaf6ea; color:#2d5c1a; }}
  .kv {{ display:flex; gap:8px; align-items:center; }}
  .sep {{ height:1px; background:#eee; margin:16px 0; }}
  .small {{ font-size:12px; }}
  .nowrap {{ white-space: nowrap; }}
</style>
</head>
<body>
"""

    # Header
    meta = [
        ("Документ", row_get(doc, "doc_id")),
        ("Организация", row_get(doc, "facility") or "—"),
        ("Отделение", row_get(doc, "dept") or "—"),
        ("Автор", row_get(doc, "author") or "—"),
        ("Поступление", row_get(doc, "admit_dt") or "—"),
        ("Источник", row_get(doc, "src_path") or "—"),
        ("Страниц", row_get(db.connect().execute("SELECT COUNT(1) FROM pages WHERE doc_id=?", (row_get(doc, "doc_id"),)).fetchone(), 0) or "—"),
        ("Извлечено символов", len(db.get_full_text(row_get(doc, "doc_id")) or "")),
    ]

    sev = stats.get("sev_counts", {})
    sev_html = f"""
    <div class="kv"><span class="pill crit">Critical: {sev.get('critical',0)}</span>
    <span class="pill maj">Major: {sev.get('major',0)}</span>
    <span class="pill min">Minor: {sev.get('minor',0)}</span></div>
    """

    summary = f"""
    <h1>Отчёт по документу <span class="mono">{html_escape(doc_id)}</span></h1>
    <div class="grid">
      <div class="card">
        <h2>Метаданные</h2>
        <table>
          {''.join(f'<tr><th class="small">{html_escape(k)}</th><td>{html_escape(v)}</td></tr>' for k,v in meta)}
        </table>
      </div>
      <div class="card">
        <h2>Нарушения</h2>
        {sev_html}
      </div>
      <div class="card">
        <h2>Сущности</h2>
        <table>{''.join(f'<tr><th class="small">{html_escape(k)}</th><td>{v}</td></tr>' for k,v in (stats.get('etype_counts') or {}).items())}</table>
      </div>
      <div class="card">
        <h2>События</h2>
        <table>{''.join(f'<tr><th class="small">{html_escape(k)}</th><td>{v}</td></tr>' for k,v in (stats.get('event_counts') or {}).items())}</table>
        <div class="small muted">Последнее событие: {html_escape(stats.get('last_ts') or '—')}</div>
      </div>
    </div>
    """

    # Violations table
    vio_rows = []
    for v in violations:
        evid = row_get(v, "evidence_json")
        try:
            evid_short = json.dumps(json.loads(evid)[:1], ensure_ascii=False) if evid else ""
        except Exception:
            evid_short = evid or ""
        vio_rows.append(
            f"<tr>" \
            f"<td class=nowrap>{html_escape(row_get(v,'rule_id'))}</td>" \
            f"<td>{html_escape(row_get(v,'message'))}</td>" \
            f"<td>{html_escape(evid_short)}</td>" \
            f"<td>{html_escape(row_get(v,'severity'))}</td>" \
            f"</tr>"
        )
    vio_table = f"""
    <h2>Нарушения ({len(violations)})</h2>
    <div class="card">
      <table>
        <thead><tr>{ths(["Правило","Сообщение","Доказательство","Уровень"])}</tr></thead>
        <tbody>{''.join(vio_rows) if vio_rows else '<tr><td colspan=4 class=muted>Нет нарушений</td></tr>'}</tbody>
      </table>
    </div>
    """

    # Events table (ограничим до 400 строк)
    ev_rows = []
    for i, ev in enumerate(events[:400]):
        ts = row_get(ev, "ts") or row_get(ev, "when")
        val = row_get(ev, "value_json")
        try:
            j = json.loads(val) if val else {}
            val_short = ", ".join(f"{k}: {j[k]}" for k in list(j.keys())[:3])
        except Exception:
            val_short = val or ""
        ev_rows.append(
            f"<tr>" \
            f"<td class=nowrap>{html_escape(ts or '—')}</td>" \
            f"<td>{html_escape(row_get(ev,'kind'))}</td>" \
            f"<td class=mono>{html_escape(row_get(ev,'section_id') or '')}</td>" \
            f"<td class=small>{html_escape(val_short)}</td>" \
            f"</tr>"
        )
    ev_table = f"""
    <h2>События (первые {min(400,len(events))} из {len(events)})</h2>
    <div class="card">
      <table>
        <thead><tr>{ths(["Время","Тип","Секция","Детали"])} </tr></thead>
        <tbody>{''.join(ev_rows) if ev_rows else '<tr><td colspan=4 class=muted>Нет событий</td></tr>'}</tbody>
      </table>
    </div>
    """

    # Sections (короткий список)
    sec_rows = []
    for s in sections[:200]:
        sec_rows.append(
            f"<tr>" \
            f"<td class=mono>{html_escape(row_get(s,'section_id'))}</td>" \
            f"<td>{html_escape(row_get(s,'name'))}</td>" \
            f"<td>{html_escape(row_get(s,'kind') or '')}</td>" \
            f"<td class=small>{html_escape(row_get(s,'start'))}–{html_escape(row_get(s,'end'))}</td>" \
            f"</tr>"
        )
    sec_table = f"""
    <h2>Секции (первые {min(200,len(sections))} из {len(sections)})</h2>
    <div class="card">
      <table>
        <thead><tr>{ths(["ID","Название","Тип","Диапазон" ])}</tr></thead>
        <tbody>{''.join(sec_rows) if sec_rows else '<tr><td colspan=4 class=muted>Нет секций</td></tr>'}</tbody>
      </table>
    </div>
    """

    foot = f"""
    <div class="sep"></div>
    <div class="small muted">Сгенерировано medqc-report • {html_escape(datetime.utcnow().isoformat()+'Z')}</div>
</body></html>
"""

    return head + summary + vio_table + ev_table + sec_table + foot


# ---------------------------- генерация Markdown ----------------------------

def render_md(doc, sections, entities, events, violations, stats: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"# Отчёт medqc — {row_get(doc,'doc_id')}")
    lines.append("")
    lines.append("## Метаданные")
    lines.append(f"- Организация: {row_get(doc,'facility') or '—'}")
    lines.append(f"- Отделение: {row_get(doc,'dept') or '—'}")
    lines.append(f"- Автор: {row_get(doc,'author') or '—'}")
    lines.append(f"- Поступление: {row_get(doc,'admit_dt') or '—'}")
    lines.append("")
    lines.append("## Нарушения")
    if violations:
        for v in violations:
            lines.append(f"- **{row_get(v,'rule_id')}** ({row_get(v,'severity')}): {row_get(v,'message')}")
    else:
        lines.append("Нет нарушений")
    lines.append("")
    lines.append("## События (первые 50)")
    for ev in events[:50]:
        ts = row_get(ev, 'ts') or row_get(ev, 'when')
        lines.append(f"- {ts or '—'} — {row_get(ev,'kind')} [{row_get(ev,'section_id') or ''}]")
    return "\n".join(lines)


# ---------------------------- CLI и сохранение ----------------------------

def main():
    ap = argparse.ArgumentParser(description="medqc-report — отчёт по документу")
    ap.add_argument("--doc-id", required=True)
    ap.add_argument("--format", choices=["html","json","md"], default="html")
    ap.add_argument("--out", help="Путь для сохранения (по умолчанию cases/<doc_id>/report.<ext>)")
    args = ap.parse_args()

    db.init_schema()
    doc = db.get_doc(args.doc_id)
    if not doc:
        print(json.dumps({"error":{"code":"NO_DOC","message":f"unknown doc_id {args.doc_id}"}}, ensure_ascii=False))
        return

    full = db.get_full_text(args.doc_id)  # не обязателен здесь
    sections = db.get_sections(args.doc_id)
    entities = db.get_entities(args.doc_id)
    events = db.get_events(args.doc_id)
    violations = safe_get_violations(args.doc_id)

    stats = compute_stats(doc, sections, entities, events, violations)

    # подготовим данные для JSON
    json_payload = {
        "doc": {k: row_get(doc,k) for k in doc.keys()},
        "stats": stats,
        "violations": [dict(v) for v in violations],
        "events": [dict(e) for e in events],
        "sections": [dict(s) for s in sections],
    }

    # путь вывода
    ext = args.format
    if args.out:
        out_path = Path(args.out)
    else:
        out_dir = db.ensure_case_dir(args.doc_id)
        out_path = out_dir / f"report.{ext}"

    # рендер
    if args.format == "json":
        content = json.dumps(json_payload, ensure_ascii=False, indent=2)
    elif args.format == "md":
        content = render_md(doc, sections, entities, events, violations, stats)
    else:
        content = render_html(doc, sections, entities, events, violations, stats)

    out_path.write_text(content, encoding="utf-8")

    # зарегистрируем артефакт
    try:
        with db.connect() as c:
            sha = db.file_sha256(out_path)
            c.execute(
                """
                INSERT INTO artifacts(doc_id, kind, path, sha256, created_at)
                VALUES(?,?,?,?,?)
                """,
                (args.doc_id, "report", str(out_path), sha, db.now_iso()),
            )
            c.commit()
    except Exception:
        pass

    # stdout — краткое резюме
    print(json.dumps({
        "doc_id": args.doc_id,
        "status": "report",
        "format": args.format,
        "path": str(out_path),
        "violations": len(violations),
        "events": len(events),
        "sections": len(sections)
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
