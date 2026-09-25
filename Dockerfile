# Utiliza a imagem oficial do Python
FROM python:3.10-slim

# Define o diretório de trabalho no contentor
WORKDIR /app

# Copia os ficheiros de dependências e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || true

# Copia todo o código do projeto para o contentor
COPY . .

# Comando padrão ao iniciar o contentor
CMD ["python", "app.py"]