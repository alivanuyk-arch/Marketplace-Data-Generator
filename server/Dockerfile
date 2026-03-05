
FROM python:3.10-slim

WORKDIR /app


RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*


COPY marketplace_generator.py .
COPY server.py .
COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser


CMD ["python", "server.py"]
