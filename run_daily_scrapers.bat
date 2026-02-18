/@echo off
:: ============================================================
::  Portfolio Data Factory - Daily Scraper Runner
::  Uruchamiany przez Windows Task Scheduler codziennie o 19:00
::  Kolejnosc: NoFluffJobs -> JustJoin.it -> Pracuj.pl
:: ============================================================

:: Ustaw katalog roboczy na folder projektu (kluczowe dla Task Scheduler,
:: ktory domyslnie startuje z C:\Windows\System32)
cd /d "%~dp0"

:: Polskie znaki w logach - UTF-8
chcp 65001 > nul

:: Przygotuj folder logow
if not exist "logs" mkdir logs

:: Nazwa pliku logu z dzisiejsza data (PowerShell - niezalezne od ustawien regionalnych)
for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set "TODAY=%%d"
set "LOG_FILE=logs\scrapers_%TODAY%.log"

echo ============================================================ >> "%LOG_FILE%"
echo  Portfolio Data Factory - Daily Run >> "%LOG_FILE%"
echo  Start: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

:: Sprawdz czy venv istnieje
if not exist ".venv\Scripts\python.exe" (
    echo [FAIL] Nie znaleziono .venv\Scripts\python.exe >> "%LOG_FILE%"
    echo [FAIL] Upewnij sie, ze venv jest zbudowany: python -m venv .venv >> "%LOG_FILE%"
    exit /b 1
)

:: Zapobiegaj usypianiu laptopa podczas scrapowania
:: powercfg /change ustawia timeout uśpienia na 0 (nigdy) na czas działania
:: ac = zasilanie sieciowe, dc = bateria
for /f %%a in ('powershell -NoProfile -Command "(powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Select-String \"Current AC\") -replace \".*0x\",\"\" | ForEach-Object {[int]([Convert]::ToInt32($_.Trim(),16)/60)}"') do set "SLEEP_AC=%%a"
for /f %%a in ('powershell -NoProfile -Command "(powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Select-String \"Current DC\") -replace \".*0x\",\"\" | ForEach-Object {[int]([Convert]::ToInt32($_.Trim(),16)/60)}"') do set "SLEEP_DC=%%a"
powercfg /change standby-timeout-ac 0 > nul
powercfg /change standby-timeout-dc 0 > nul
echo [INFO] Uśpienie tymczasowo wyłączone (AC=%SLEEP_AC%, DC=%SLEEP_DC%) >> "%LOG_FILE%"

:: Uruchom monitor - wszystkie 3 scrapery
:: -X utf8: wymuszenie UTF-8 na Windows (polskie znaki w logach)
echo [INFO] Uruchamiam scraper_monitor.py... >> "%LOG_FILE%"

.venv\Scripts\python.exe -X utf8 pracuj_scraper\scraper_monitor.py >> "%LOG_FILE%" 2>&1

set "EXIT_CODE=%errorlevel%"

:: Przywróć oryginalne ustawienia uśpienia
powercfg /change standby-timeout-ac %SLEEP_AC% > nul 2>&1
powercfg /change standby-timeout-dc %SLEEP_DC% > nul 2>&1
echo [INFO] Uśpienie przywrócone do oryginalnych ustawień >> "%LOG_FILE%"

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo  Koniec: %date% %time% >> "%LOG_FILE%"
echo  Exit code: %EXIT_CODE% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

:: Usun logi starsze niz 90 dni (katalog logs nie puchnie)
forfiles /p "logs" /m "scrapers_*.log" /d -90 /c "cmd /c del @path" 2>nul

exit /b %EXIT_CODE%
