#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
medqc-orchestrator — единый CLI, который запускает конвейер обработки:

  [ingest] → extract → section → entities → timeline → rules → report

Он:
- сам понимает, какие шаги уже выполнены по doc_id, и пропускает их (если не указан --force)
- умеет принимать исходный файл (--file) и начинать с ingest, либо работать по существующему --doc-id
- поддерживает два режима правил: --rules <json> ИЛИ runtime из БД (--pkg/--version [--profiles])
- сохраняет человекочитаемый отчёт (HTML/JSON/MD) через medqc_report.py
- печатает краткое JSON-резюме по завершении

Примеры:
  # полный прогон по уже существующему документу
  python medqc_orchestrator.py --doc-id KZ-20250906-17E5F4FA

  # начать с локального файла, указав метаданные (создаст doc_id и пойдёт дальше)
  python medqc_orchestrator.py --file ./cases/infection.pdf \
      --facility "ГКБ №1" --dept "Инфекционное" --author "Иванова И.И." --admit-dt 2025-08-21T10:15

  # правила из пакета в БД (runtime)
  python medqc_orchestrator.py --doc-id KZ-... --pkg rules-pack-stationary-er --version 2025-09-07 --profiles STA,ER

  # только часть шагов, с форсом пере‑выполнения
  python medqc_orchestrator.py --doc-id KZ-... --steps extract,entities --force extract

  # отчёт в JSON вместо HTML
  python medqc_orchestrator.py --doc-id KZ-... --report-format json

Зависимости: стандартная библиотека + локальные скрипты из этого проекта (medqc_*.py)
"""
from __future__ import annotations
import argparse, json, os, sys, subprocess, logging, sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import medqc_db as db

HERE = Path(__file__).resolve().parent
PY = sys.executable

SCRIPTS = {
    'extract': HERE / 'medqc_extract.py',
    'section': HERE / 'medqc_section.py',
    'entities': HERE / 'medqc_entities.py',
    'timeline': HERE / 'medqc_timeline.py',
    'rules': HERE / 'medqc_rules.py',
    'report': HERE / 'medqc_report.py',
}

DEFAULT_STEPS = ['extract', 'section', 'entities', 'timeline', 'rules', 'report']
ALL_STEPS = ['ingest'] + DEFAULT_STEPS

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger('orchestrator')

# ------------------------------ helpers ------------------------------

def run_cmd(argv: List[str]) -> Dict[str, Any]:
    log.debug('RUN %s', ' '.join(argv))
    p = subprocess.run(argv, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(argv)}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")
    # попытка распарсить JSON из stdout; если не вышло — вернуть просто текст
    out: Dict[str, Any]
    try:
        out = json.loads(p.stdout)
    except Exception:
        out = {"stdout": p.stdout.strip()}
    if p.stderr.strip():
        out["stderr"] = p.stderr.strip()
    return out


def safe_count_violations(doc_id: str) -> int:
    try:
        with db.connect() as c:
            row = c.execute("SELECT COUNT(1) FROM violations WHERE doc_id=?", (doc_id,)).fetchone()
            return int(row[0]) if row else 0
    except sqlite3.OperationalError:
        return 0


def probe_state(doc_id: str) -> Dict[str, Any]:
    full = db.get_full_text(doc_id)
    sections = db.get_sections(doc_id)
    entities = db.get_entities(doc_id)
    events = db.get_events(doc_id)
    vcnt = safe_count_violations(doc_id)
    return {
        'has_text': bool(full),
        'sections': len(sections),
        'entities': len(entities),
        'events': len(events),
        'violations': vcnt,
    }

# ------------------------------ pipeline steps ------------------------------

def step_ingest(args) -> Dict[str, Any]:
    # Выполняем ingest через API (без отдельного скрипта)
    from pathlib import Path
    db.init_schema()
    path = Path(args.file).resolve()
    res = db.ingest_local_file(
        path=path,
        facility=args.facility,
        dept=args.dept,
        author=args.author,
        admit_dt=args.admit_dt,
    )
    if 'error' in res:
        raise RuntimeError(json.dumps(res, ensure_ascii=False))
    return res


def step_extract(doc_id: str) -> Dict[str, Any]:
    return run_cmd([PY, str(SCRIPTS['extract']), '--doc-id', doc_id])


def step_section(doc_id: str) -> Dict[str, Any]:
    return run_cmd([PY, str(SCRIPTS['section']), '--doc-id', doc_id])


def step_entities(doc_id: str) -> Dict[str, Any]:
    return run_cmd([PY, str(SCRIPTS['entities']), '--doc-id', doc_id])


def step_timeline(doc_id: str) -> Dict[str, Any]:
    return run_cmd([PY, str(SCRIPTS['timeline']), '--doc-id', doc_id])


def step_rules(doc_id: str, rjson: Optional[str], pkg: Optional[str], ver: Optional[str], profiles: Optional[str], include_disabled: bool) -> Dict[str, Any]:
    cmd = [PY, str(SCRIPTS['rules']), '--doc-id', doc_id]
    if rjson:
        cmd += ['--rules', rjson]
    elif pkg and ver:
        cmd += ['--pkg', pkg, '--version', ver]
        if profiles:
            cmd += ['--profiles', profiles]
        if include_disabled:
            cmd += ['--include-disabled']
    return run_cmd(cmd)


def step_report(doc_id: str, fmt: str, out: Optional[str]) -> Dict[str, Any]:
    cmd = [PY, str(SCRIPTS['report']), '--doc-id', doc_id, '--format', fmt]
    if out:
        cmd += ['--out', out]
    return run_cmd(cmd)

# ------------------------------ orchestrate one doc ------------------------------

def orchestrate_one(args, doc_id: Optional[str]) -> Dict[str, Any]:
    """Запускает конвейер для одного doc_id (или сначала делает ingest по файлу)."""
    created_doc_id: Optional[str] = None

    # 0) ingest, если пришёл файл и нет doc_id
    if args.file and not doc_id:
        log.info('Ingest: %s', args.file)
        res = step_ingest(args)
        created_doc_id = res.get('doc_id')
        doc_id = created_doc_id
        log.info('New doc_id: %s', doc_id)

    if not doc_id:
        raise RuntimeError('Нужно указать --doc-id или --file для ingest')

    db.init_schema()
    if not db.get_doc(doc_id):
        raise RuntimeError(f'Документ не найден в БД: {doc_id}')

    # 1) определить набор шагов
    steps = [s.strip() for s in (args.steps.split(',') if args.steps else DEFAULT_STEPS)]
    for s in steps:
        if s not in ALL_STEPS:
            raise RuntimeError(f'Неизвестный шаг: {s}. Допустимо: {ALL_STEPS}')

    # 2) состояние до
    state_before = probe_state(doc_id)

    # 3) выполнять по порядку
    results: Dict[str, Any] = {"doc_id": doc_id, "created": bool(created_doc_id)}

    for s in steps:
        if s == 'ingest':
            # ingest уже сделан выше при необходимости; пропускаем
            continue
        try:
            if s == 'extract':
                need = args.force == 'extract' or not state_before['has_text']
                if need:
                    log.info('Step: extract')
                    results['extract'] = step_extract(doc_id)
                else:
                    log.info('Skip extract (уже есть текст)')
            elif s == 'section':
                need = args.force == 'section' or state_before['sections'] == 0
                if need:
                    log.info('Step: section')
                    results['section'] = step_section(doc_id)
                else:
                    log.info('Skip section (sections=%s)', state_before['sections'])
            elif s == 'entities':
                need = args.force == 'entities' or state_before['entities'] == 0
                if need:
                    log.info('Step: entities')
                    results['entities'] = step_entities(doc_id)
                else:
                    log.info('Skip entities (entities=%s)', state_before['entities'])
            elif s == 'timeline':
                need = args.force == 'timeline' or state_before['events'] == 0
                if need:
                    log.info('Step: timeline')
                    results['timeline'] = step_timeline(doc_id)
                else:
                    log.info('Skip timeline (events=%s)', state_before['events'])
            elif s == 'rules':
                # rules всегда полезно прогнать, если хотим свежие нарушения
                log.info('Step: rules (%s)', 'JSON' if args.rules else ('DB pkg' if args.pkg else 'DEFAULT'))
                results['rules'] = step_rules(doc_id, args.rules, args.pkg, args.version, args.profiles, args.include_disabled)
            elif s == 'report':
                log.info('Step: report (%s)', args.report_format)
                results['report'] = step_report(doc_id, args.report_format, args.out)
        except Exception as e:
            log.error('Ошибка на шаге %s: %s', s, e)
            results.setdefault('errors', {})[s] = str(e)
            if not args.keep_going:
                break
        finally:
            # обновляем состояние после шага (для пропуска следующих)
            state_before = probe_state(doc_id)

    results['state'] = state_before
    return results

# ------------------------------ CLI ------------------------------

def main():
    ap = argparse.ArgumentParser(description='medqc-orchestrator — конвейер обработки мед.документа')
    # вход
    ap.add_argument('--doc-id', help='Существующий doc_id в БД')
    ap.add_argument('--file', help='Путь к файлу для ingest (pdf/docx и т.п.)')
    ap.add_argument('--facility'); ap.add_argument('--dept'); ap.add_argument('--author'); ap.add_argument('--admit-dt')

    # выбор шагов
    ap.add_argument('--steps', help='Список шагов через запятую. По умолчанию: extract,section,entities,timeline,rules,report')
    ap.add_argument('--force', choices=DEFAULT_STEPS, help='Форсировать только один шаг (перевыполнить)')
    ap.add_argument('--keep-going', action='store_true', help='Не останавливать конвейер при ошибке шага')

    # правила
    ap.add_argument('--rules', help='Путь к rules.json (замороженный файл)')
    ap.add_argument('--pkg', help='Имя пакета правил из БД (norm_packages.name)')
    ap.add_argument('--version', help='Версия пакета правил (norm_packages.version)')
    ap.add_argument('--profiles', help='Фильтр профиля (через запятую), для runtime-режима')
    ap.add_argument('--include-disabled', action='store_true', help='Включать отключённые правила (runtime)')

    # отчёт
    ap.add_argument('--report-format', choices=['html','json','md'], default='html')
    ap.add_argument('--out', help='Кастомный путь сохранения отчёта')

    args = ap.parse_args()

    try:
        res = orchestrate_one(args, args.doc_id)
        print(json.dumps(res, ensure_ascii=False, indent=2))
    except Exception as e:
        log.error('%s', e)
        print(json.dumps({"error": str(e)}, ensure_ascii=False))
        sys.exit(2)

if __name__ == '__main__':
    main()
