FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY services/ ./services/
COPY pipelines/ ./pipelines/
COPY ml/ ./ml/
COPY scripts/ ./scripts/

ENV PYTHONUNBUFFERED=1
