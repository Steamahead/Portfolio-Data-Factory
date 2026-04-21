# Portfolio Data Factory — Status

> Aktualizuj ten plik po każdej sesji. Po `/clear` — czytaj ten plik pierwszy.

## Aktualny stan (2026-04-21)

**CSV-Only mode ZAKOŃCZONY** — Azure SQL przywrócony, dane zaimportowane, pipeline'y wróciły pod Azure Functions.

### Co się wydarzyło (2026-04-20 → 2026-04-21):
1. Azure subscription read-only (niezapłacona FV) → włączono `CSV_ONLY=1`
2. Stworzono lokalny runner (`run_etl_local.py`) + Task Scheduler (CEE co 1h, ETL Daily 08:00)
3. Zebrano dane przez ~24h: Energy, Gov Spending, CEE FX (24 hourly runs), Job Scrapers
4. Azure odblokowany → dodano IP do firewall (dynamiczne IP: 185.203.173.180, potem 91.94.8.24)
5. `csv_to_db.py` zaimportował 82 pliki do Azure SQL (naprawiono 3 bugi: JustJoin ast.literal_eval, weather datetime, energy schema routing)
6. Usunięto lokalną automatyzację (Task Scheduler, bat files, runner), przywrócono Azure Functions
7. `CSV_ONLY=1` usunięte z `.env`

### Firewall Azure SQL
IP jest dynamiczne — przy zmianie ISP trzeba dodać nowe IP w Azure Portal → SQL Server → Networking.
Ostatnie znane IP: `91.94.8.24` (2026-04-21).

---

### 1. Shiller Index (`shiller_index/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ ENABLED (local) | 21:30 UTC | `b655133` — Fix 429 rate limits, retry logic, model switch |

Model: `gemini-3.1-flash-live-preview`. Backfill 2026-04-13 OK.

### 2. Energy Prophet (`energy_prophet/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | 08:00 UTC daily | `eb39659` — Add --date CLI arg |

Stabilny. CSV-Only guard dodany.

### 3. CEE FX Volatility (`cee_fx_volatility/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | Co godzinę | `3a996d5` — START/FINISH email alerts |

Stabilny. CSV-Only guard dodany. Gemini 2.5 Flash ma 503 overload issues (działa z retry).

### 4. Gov Spending Radar (`gov_spending_radar/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | 06:00 UTC daily | `ad509b8` — Phase 3b: LLM classifier |

Stabilny. CSV-Only guard dodany.

### 5. Job Scrapers (`pracuj_scraper/`, `nfj_scraper/`, `just_join_scraper/`)
| Scraper | Status | Ostatni commit |
|---------|--------|----------------|
| NoFluffJobs | ✅ OK | `349f441` |
| JustJoin.it | ✅ OK | `349f441` |
| Pracuj.pl | ✅ OK | `d7b08cf` — timeout 240min |

Orchestracja: `scraper_monitor.py` → `run_daily_scrapers.bat` → Task Scheduler 20:00.
Pracuj timeout zwiększony do 240min (CF_WAIT=7s).

---

## Niezacommitowane zmiany

Brak — wszystko zacommitowane i wypushowane do origin.

---

## Następny krok

1. **Token optimization** — plan batch 3 tickery Shillera w 1 request (patrz memory: `project_shiller_token_optimization.md`)
2. **Zmiana modelu CEE FX + Gov Spending** — na tańszy (Gemini 503 issues)
3. **Azure firewall** — rozważyć zakres IP zamiast pojedynczych adresów

---

## Znane problemy

- Task Scheduler `Last Result = -2147023829` po timeout Pracuj → przesuwa następny run +1 dzień
- Pracuj circuit breaker nowy — nieprzetestowany przy dużym ruchu CF na prod
- Gemini 2.5 Flash ma 503 overload issues — Shiller na 3.1 Flash Live, CEE/Gov nadal na 2.5
- Azure SQL firewall — dynamiczne IP wymaga ręcznego dodawania
