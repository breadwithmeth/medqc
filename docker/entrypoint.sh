#!/usr/bin/env bash
set -euo pipefail

# Готовим каталог под SQLite, если он используется
if [[ -n "${MEDQC_DB:-}" ]]; then
  db_dir="$(dirname "${MEDQC_DB}")"
  mkdir -p "$db_dir" || true
  chmod 777 "$db_dir" || true
fi

echo "[entrypoint] DB: ${MEDQC_DB}"

# Миграции/инициализация
python medqc_norms_admin.py migrate || true

# Импорт правил (опционально)
if [[ "${RULES_IMPORT_ON_START:-0}" == "1" ]] && [[ -f "${RULES_FILE:-/app/rules.json}" ]]; then
  echo "[entrypoint] Importing rules from ${RULES_FILE}"
  python medqc_norms_admin.py import --file "${RULES_FILE}" || true
fi

# Активный пакет (опционально)
if [[ -n "${DEFAULT_RULES_PACKAGE:-}" ]] && [[ -n "${DEFAULT_RULES_VERSION:-}" ]]; then
  echo "[entrypoint] Setting active rules: ${DEFAULT_RULES_PACKAGE} ${DEFAULT_RULES_VERSION}"
  python medqc_norms_admin.py set-active --name "${DEFAULT_RULES_PACKAGE}" --version "${DEFAULT_RULES_VERSION}" || true
fi

# Старт API — важно слушать 0.0.0.0 и тот же порт, что в Coolify
exec uvicorn medqc_api:app --host 0.0.0.0 --port "${PORT:-8000}"
