FROM python:3.10-slim

WORKDIR /app

# Define a raiz para o Python encontrar os imports
ENV PYTHONPATH=/app

# Instalar dependências essenciais do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar o requirements exclusivo da Vetra
COPY requirements-vetra.txt .

# Instalar as dependências leves da Vetra sem cache
RUN pip install --no-cache-dir --prefer-binary -r requirements-vetra.txt

# Copiar o código respeitando o .dockerignore
COPY . .

# Expor a porta do Streamlit (Web) e a porta interna da API FastAPI
EXPOSE 8501 8000

# Criar o script start.sh para subir a API Backend e o Frontend juntos
RUN echo '#!/bin/sh\n\
python -m uvicorn utils.meu_agente_dados.main:app --host 0.0.0.0 --port 8000 &\n\
python -m streamlit run utils/meu_agente_dados/app.py --server.port=8501 --server.address=0.0.0.0\n\
' > /app/start.sh && chmod +x /app/start.sh

# Executa o script de inicialização dupla
CMD ["/app/start.sh"]