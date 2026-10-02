FROM python:3.10-slim

WORKDIR /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-vetra.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements-vetra.txt

COPY . .

EXPOSE 8501 8000

# Não precisa de ENTRYPOINT ou CMD com start.sh! O fly.toml assume o controle via [processes].