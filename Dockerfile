# Usa imagem leve e oficial do Python 3.11
FROM python:3.11-slim

# Evita arquivos .pyc e força logs imediatos no terminal
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Instala dependências essenciais do sistema operacional
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala as bibliotecas Python (Nome correto do arquivo no projeto)
COPY requirements-vetra.txt .
RUN pip install --no-cache-dir -r requirements-vetra.txt

# Copia todo o código-fonte da aplicação
COPY . .

# Expõe as portas utilizadas pelos processos
EXPOSE 8501 8000