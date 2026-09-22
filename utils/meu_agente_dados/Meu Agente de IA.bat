@echo off
title Agente Consultivo de Dados - Streamlit (Gemini)

:: Coloque sua chave de API do Gemini aqui

set GEMINI_API_KEY=AQ.Ab8RN6LYXJPv3QQPlfOYIMEsJXmJFS1Zw-r-oCmBaECQWBI-dA
set QDRANT_URL=https://5748bd51-7954-4b2e-be8f-c17318e78aba.sa-east-1-0.aws.cloud.qdrant.io
set QDRANT_API_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwic3ViamVjdCI6ImFwaS1rZXk6MmJjODg1YTUtNTFmZC00NTQ1LTk2YWYtNDkyMTY2MDdiODA4In0.Qoe61u80Gzjil2aGov8rg9_kbsynQAAGgNE2yz68jk8


cd /d "%~dp0"

echo ===================================================
echo   Iniciando Agente com Gemini API (Gratuito)
echo ===================================================
echo.

python -m streamlit run app.py

pause