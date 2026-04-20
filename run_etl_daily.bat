@echo off
:: ============================================================
::  Portfolio Data Factory - Daily ETL Runner (Local)
::  Runs Energy Prophet + Gov Spending daily.
::  Uruchamiany przez Windows Task Scheduler codziennie o 08:00.
::  CEE FX ma osobny task co godzine.
:: ============================================================

cd /d "%~dp0"
chcp 65001 > nul

if not exist "logs" mkdir logs

for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set "TODAY=%%d"
set "LOG_FILE=logs\etl_daily_%TODAY%.log"

echo ============================================================ >> "%LOG_FILE%"
echo  Portfolio Data Factory - Daily ETL >> "%LOG_FILE%"
echo  Start: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

if not exist ".venv\Scripts\python.exe" (
    echo [FAIL] Nie znaleziono .venv\Scripts\python.exe >> "%LOG_FILE%"
    exit /b 1
)

:: Zapobiegaj usypianiu
for /f %%a in ('powershell -NoProfile -Command "(powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Select-String \"Current AC\") -replace \".*0x\",\"\" | ForEach-Object {[int]([Convert]::ToInt32($_.Trim(),16)/60)}"') do set "SLEEP_AC=%%a"
for /f %%a in ('powershell -NoProfile -Command "(powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Select-String \"Current DC\") -replace \".*0x\",\"\" | ForEach-Object {[int]([Convert]::ToInt32($_.Trim(),16)/60)}"') do set "SLEEP_DC=%%a"
powercfg /change standby-timeout-ac 0 > nul
powercfg /change standby-timeout-dc 0 > nul

echo [INFO] Running Energy Prophet... >> "%LOG_FILE%"
.venv\Scripts\python.exe -X utf8 run_etl_local.py energy >> "%LOG_FILE%" 2>&1

echo [INFO] Running Gov Spending Radar... >> "%LOG_FILE%"
.venv\Scripts\python.exe -X utf8 run_etl_local.py gov >> "%LOG_FILE%" 2>&1

:: Przywroc uspienie
powercfg /change standby-timeout-ac %SLEEP_AC% > nul 2>&1
powercfg /change standby-timeout-dc %SLEEP_DC% > nul 2>&1

echo ============================================================ >> "%LOG_FILE%"
echo  Koniec: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

forfiles /p "logs" /m "etl_daily_*.log" /d -90 /c "cmd /c del @path" 2>nul

exit /b 0
