@echo off
:: ============================================================
::  CEE FX Volatility - Hourly Runner (Local)
::  Uruchamiany przez Windows Task Scheduler co godzine.
:: ============================================================

cd /d "%~dp0"
chcp 65001 > nul

if not exist "logs" mkdir logs

for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set "TODAY=%%d"
set "LOG_FILE=logs\cee_fx_%TODAY%.log"

echo [%time%] CEE FX start >> "%LOG_FILE%"

if not exist ".venv\Scripts\python.exe" (
    echo [FAIL] Nie znaleziono .venv\Scripts\python.exe >> "%LOG_FILE%"
    exit /b 1
)

.venv\Scripts\python.exe -X utf8 run_etl_local.py cee >> "%LOG_FILE%" 2>&1

echo [%time%] CEE FX done (exit=%errorlevel%) >> "%LOG_FILE%"

forfiles /p "logs" /m "cee_fx_*.log" /d -90 /c "cmd /c del @path" 2>nul

exit /b 0
