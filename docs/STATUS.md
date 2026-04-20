# Portfolio Data Factory — Status

> Aktualizuj ten plik po każdej sesji. Po `/clear` — czytaj ten plik pierwszy.

## CSV-Only Mode (aktywny od 2026-04-20)

Azure subscription tymczasowo read-only (niezapłacona FV). Wszystkie pipeline'y działają w trybie `CSV_ONLY=1` — dane zapisywane do `csv_staging/` zamiast Azure SQL.

**Przywrócenie:** Usuń `CSV_ONLY=1` z `.env`, uruchom `python -X utf8 csv_to_db.py` (opcjonalnie `--dry-run` najpierw).

---

## Aktualny stan (2026-04-14)

### 1. Shiller Index (`shiller_index/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ ENABLED (local) | 21:30 UTC | `b655133` — Fix 429 rate limits, retry logic, model switch |

Model zmieniony z `gemini-2.5-flash` na `gemini-3.1-flash-live-preview`.
Fixy: 429 rate limit jako transient (nie permanent), 15s delay między tickerami, backoff [5,30,90], delayed retry dla ANY failures.
Backfill 2026-04-13 OK (3/3 tickerów w DB).
**CZEKA NA TEST:** wieczorny run 21:30 UTC — porównać jakość Flash Live vs stary 2.5 Flash.

### 2. Energy Prophet (`energy_prophet/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | 08:00 UTC daily | `eb39659` — Add --date CLI arg |

Stabilny. Brak otwartych problemów.

### 3. CEE FX Volatility (`cee_fx_volatility/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | Co godzinę | `3a996d5` — START/FINISH email alerts |

Stabilny. Model: `gemini-2.5-flash` (config.yaml). Kandydat na zmianę modelu na tańszy.

### 4. Gov Spending Radar (`gov_spending_radar/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | 06:00 UTC daily | `ad509b8` — Phase 3b: LLM classifier |

Stabilny. Model: `gemini-2.5-flash` (config.yaml). Kandydat na zmianę modelu na tańszy.

### 5. Job Scrapers (`pracuj_scraper/`, `nfj_scraper/`, `just_join_scraper/`)
| Scraper | Status | Ostatni commit |
|---------|--------|----------------|
| NoFluffJobs | ✅ OK | `349f441` |
| JustJoin.it | ✅ OK | `349f441` |
| Pracuj.pl | ✅ OK | `349f441` |

Orchestracja: `scraper_monitor.py` → subprocess isolation → `run_daily_scrapers.bat` → Task Scheduler 20:00.

---

## Niezacommitowane zmiany

Brak — wszystko zacommitowane i wypushowane do origin.

---

## Następny krok

1. **23:30 CEST (21:30 UTC)** — sprawdzić wynik Shillera z `gemini-3.1-flash-live-preview`, porównać jakość z 2.5 Flash
2. **Token optimization** — plan batch 3 tickery w 1 request (patrz memory: `project_shiller_token_optimization.md`)
3. **Zmiana modelu CEE FX + Gov Spending** — na tańszy (Flash-Lite 3.2 lub inny)

---

## Znane problemy

- Task Scheduler `Last Result = -2147023829` po timeout Pracuj → przesuwa następny run +1 dzień
- Pracuj circuit breaker nowy — nieprzetestowany przy dużym ruchu CF na prod
- Gemini 2.5 Flash ma 503 overload issues — Shiller przeszedł na 3.1 Flash Live
- Azure SQL firewall — IP `31.60.93.79` dodane ręcznie, dynamiczne IP może się zmienić
- Azure subscription read-only — CSV_ONLY=1 aktywne, dane w csv_staging/
