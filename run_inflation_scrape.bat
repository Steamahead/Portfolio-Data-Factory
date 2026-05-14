@echo off
REM ============================================================
REM Inflation Basket — daily scrape (Task Scheduler entry point)
REM Runs Frisco + Auchan, validates coverage, emails report.
REM Schedule: Mon/Wed/Fri 22:00 (see scheduler_inflation_task.xml)
REM ============================================================

cd /d "C:\Users\sadza\PycharmProjects\portfolio-data-factory"

set LOG_DIR=C:\Users\sadza\PycharmProjects\portfolio-data-factory\inflation_basket\logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

set TS=%date:~10,4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%
set TS=%TS: =0%
set LOG=%LOG_DIR%\scrape_%TS%.log

echo [%date% %time%] Starting inflation_basket scrape >> "%LOG%"

".venv\Scripts\python.exe" -X utf8 -m inflation_basket.scrape_monitor >> "%LOG%" 2>&1
set EXITCODE=%ERRORLEVEL%

echo [%date% %time%] Finished with exit code %EXITCODE% >> "%LOG%"

REM Trim logs to last 60 days
forfiles /p "%LOG_DIR%" /m scrape_*.log /d -60 /c "cmd /c del @path" 2>nul

exit /b %EXITCODE%
