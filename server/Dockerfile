# Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Устанавливаем зависимости для Parquet (опционально)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Копируем код
COPY marketplace_generator.py .
COPY server.py .
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Создаем пользователя
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# Запускаем сервер
CMD ["python", "server.py"]
