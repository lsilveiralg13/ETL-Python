# Usa imagem leve e oficial do Python 3.11
FROM python:3.11-slim

# Evita arquivos .pyc e força logs imediatos no terminal
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Instala dependências essenciais do sistema operante
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala as bibliotecas Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código-fonte da aplicação
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Saúde do container (Healthcheck)
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Comando de inicialização oficial da plataforma Vetra
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]