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

# Script de inicialização unificado
RUN echo '#!/bin/sh\n\
python -m uvicorn utils.meu_agente_dados.main:app --host 0.0.0.0 --port 8000 &\n\
sleep 5\n\
python -m streamlit run utils/meu_agente_dados/app.py --server.port=8501 --server.address=0.0.0.0\n\
' > /app/start.sh && chmod +x /app/start.sh

CMD ["/app/start.sh"]