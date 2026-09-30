FROM python:3.10-slim

WORKDIR /app

ENV PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-vetra.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements-vetra.txt

COPY . .

EXPOSE 8501 8000

# Script de inicialização com tempo de espera para o Uvicorn abrir a porta 8000
RUN echo '#!/bin/sh\n\
python -m uvicorn utils.meu_agente_dados.main:app --host 127.0.0.1 --port 8000 &\n\
sleep 3\n\
python -m streamlit run utils/meu_agente_dados/app.py --server.port=8501 --server.address=0.0.0.0\n\
' > /app/start.sh && chmod +x /app/start.sh

CMD ["/app/start.sh"]