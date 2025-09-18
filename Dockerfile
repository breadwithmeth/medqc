FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

# Базовые системные пакеты. Для PyMuPDF обычно хватает колёс,
# но на slim добавим минимумы — пригодится для многих зависимостей.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl build-essential pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Зависимости — до кода (кэш сборки)
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Код
COPY . .

# Переменные по умолчанию (их можно переопределять в Coolify)
ENV PYTHONPATH=/app \
    PORT=8000 \
    MEDQC_DB=/data/medqc.db \
    DEFAULT_RULES_PACKAGE=kz-standards \
    DEFAULT_RULES_VERSION=2025-09-17 \
    RULES_IMPORT_ON_START=0 \
    RULES_FILE=/app/rules.json

# Entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/entrypoint.sh"]
