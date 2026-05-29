@echo off
title Restauracao Ambiente Python
echo.
echo ==========================================
echo RESTAURANDO AMBIENTE PYTHON
echo ==========================================
echo.
python -m pip install --upgrade pip
echo.
pip install --no-index --find-links=pacotes -r requirements.txt
echo.
echo Ambiente restaurado com sucesso.
pause
