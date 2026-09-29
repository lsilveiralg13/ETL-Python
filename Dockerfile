FROM python:3.10-slim

WORKDIR /app

# Define a raiz para o Python encontrar os imports
ENV PYTHONPATH=/app

# Instalar dependências essenciais do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements primeiro
COPY requirements.txt .

# Instalar dependências sem cache do pip
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código respeitando o .dockerignore
COPY . .

EXPOSE 8501

# Executa o Streamlit na pasta correta
CMD ["streamlit", "run", "utils/meu_agente_dados/app.py", "--server.port=8501", "--server.address=0.0.0.0"]