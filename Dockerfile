FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_MAXUPLOADSIZE=10 \
    STREAMLIT_SERVER_MAXMESSAGESIZE=32 \
    VETRA_OCR_LANG=por+eng \
    VETRA_MAX_UPLOAD_BYTES=10485760 \
    VETRA_MAX_ATTACHMENT_BYTES=10485760 \
    VETRA_MAX_TOTAL_ATTACHMENT_BYTES=26214400 \
    VETRA_MAX_ATTACHMENTS=10 \
    VETRA_MAX_ATTACHMENT_TEXT_CHARS=100000 \
    VETRA_MAX_PDF_PAGES=500 \
    VETRA_MAX_TEXTO_RAG=90000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-por \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-vetra.txt .
RUN pip install --no-cache-dir -r requirements-vetra.txt

COPY . .

EXPOSE 8501 8000