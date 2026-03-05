FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY generator.py .
COPY Marketplace_Producer.py .

CMD ["python", "generator.py", "--count", "1000"]