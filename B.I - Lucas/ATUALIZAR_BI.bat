@echo off
title MONITOR DE DADOS - BELMICRO (FABRICA)
color 0A
cls

echo ======================================================
echo    EXECUTOR DE ATUALIZACAO - INDICADORES FABRICA
echo ======================================================
echo.

:: 1. Entra na pasta Área de Trabalho
cd /d "%USERPROFILE%\OneDrive - BELMICRO TECNOLOGIA SA\Área de Trabalho"

:: 2. Executa o script Python
echo [STATUS] Iniciando script: automacao_fabrica.py
echo.

python "automacao_fabrica.py"

echo.
echo ======================================================
echo    OPERACAO FINALIZADA
echo ======================================================
pause