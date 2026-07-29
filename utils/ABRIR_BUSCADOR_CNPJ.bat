@echo off
setlocal EnableExtensions

cd /d "%~dp0"

REM Sobe o Streamlit em segundo plano
start "" /B python "Launcher APP Buscador de CNPJ.py"

REM Espera o servidor responder em localhost:8502 (até ~60s)
set "URL=http://localhost:8502"
set /a "MAX=60"
set /a "i=0"

:WAIT
REM testa conexão no endpoint (se não responder, errolevel != 0)
powershell -NoProfile -Command ^
  "try { $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 '%URL%'; exit 0 } catch { exit 1 }" >nul 2>&1

if %errorlevel%==0 goto OPEN

set /a i+=1
if %i% GEQ %MAX% goto FAIL

timeout /t 1 /nobreak >nul
goto WAIT

:OPEN
start "" "%URL%"
exit /b 0

:FAIL
echo.
echo [ERRO] O app nao respondeu em %URL% depois de %MAX% segundos.
echo Abra o terminal do Streamlit e veja se apareceu algum erro.
echo.
pause
exit /b 1
