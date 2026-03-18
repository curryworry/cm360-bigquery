FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY templates /app/templates
COPY configs /app/configs

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

ENV PORT=8080
CMD uvicorn src.main:app --host 0.0.0.0 --port ${PORT}

