FROM python:3.10-slim

WORKDIR /app

# Instalar dependências do sistema essenciais para compilar pacotes se necessário
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar apenas os requisitos primeiro para aproveitar o cache de camadas do Docker
COPY requirements.txt .

# Instalar dependências Python sem cache
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código da aplicação
COPY . .

# Expor a porta do Streamlit
EXPOSE 8501

# Comando para iniciar a aplicação
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]