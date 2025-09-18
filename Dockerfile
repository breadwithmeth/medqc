FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Системные пакеты — добавь/убери при необходимости (psycopg2, Pillow, lxml и т.п.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала зависимости — для кэша
COPY requirements.txt .
RUN pip install -r requirements.txt

# Затем код
COPY . .

# Если rules.json в репо — этой строки НЕ нужно:
# COPY rules.json /app/rules.json

# Переменные окружения (можно переопределить в Coolify)
ENV MEDQC_DB=/app/medqc.db \
    DEFAULT_RULES_PACKAGE=kz-standards \
    DEFAULT_RULES_VERSION=2025-09-17 \
    RULES_IMPORT_ON_START=0 \
    RULES_FILE=/app/rules.json

# Entrypoint + CMD
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/entrypoint.sh"]
# ВАЖНО: Пусть CMD задаёт команду запуска веб-сервера.
# Для FastAPI/Starlette:
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "${PORT:-8000}"]
