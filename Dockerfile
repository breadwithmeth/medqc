FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# запускаем через uvicorn
CMD ["uvicorn", "medqc_api:app", "--host", "0.0.0.0", "--port", "8000"]
