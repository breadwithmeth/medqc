#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] DB: ${MEDQC_DB}"
python medqc_norms_admin.py migrate || true

# опционально импорт правил при старте (если включено и файл существует)
if [[ "${RULES_IMPORT_ON_START:-0}" == "1" ]] && [[ -f "${RULES_FILE:-/app/rules.json}" ]]; then
  echo "[entrypoint] Importing rules from ${RULES_FILE}"
  python medqc_norms_admin.py import --file "${RULES_FILE}" || true
fi

# выставляем активный пакет (если заданы)
if [[ -n "${DEFAULT_RULES_PACKAGE:-}" ]] && [[ -n "${DEFAULT_RULES_VERSION:-}" ]]; then
  echo "[entrypoint] Setting active rules: ${DEFAULT_RULES_PACKAGE} ${DEFAULT_RULES_VERSION}"
  python medqc_norms_admin.py set-active --name "${DEFAULT_RULES_PACKAGE}" --version "${DEFAULT_RULES_VERSION}" || true
fi

# стартуем API
exec uvicorn medqc_api:app --host 0.0.0.0 --port 8000
