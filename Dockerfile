# Utiliza a imagem oficial do Python
FROM python:3.10-slim

# Evita a criação de ficheiros .pyc e garante output de logs em tempo real
ENV PYTHONUNBUFFERED=1

# Define o diretório de trabalho no contentor
WORKDIR /app

# Copia os ficheiros de dependências e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do projeto para o contentor
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Comando padrão ao iniciar o contentor a apontar para a tua app Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]