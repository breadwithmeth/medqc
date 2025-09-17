FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Кладём код и (опционально) rules.json внутрь образа
COPY . .
# Если rules.json хранится в репо:
COPY rules.json /app/rules.json

# Переменные окружения (можно переопределить в Coolify)
ENV MEDQC_DB=/app/medqc.db \
    DEFAULT_RULES_PACKAGE=kz-standards \
    DEFAULT_RULES_VERSION=2025-09-17 \
    RULES_IMPORT_ON_START=0 \
    RULES_FILE=/app/rules.json

# Entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/entrypoint.sh"]
