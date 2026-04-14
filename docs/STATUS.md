# Portfolio Data Factory — Status

> Aktualizuj ten plik po każdej sesji. Po `/clear` — czytaj ten plik pierwszy.

## Aktualny stan (2026-04-10)

### 1. Shiller Index (`shiller_index/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ⏸ DISABLED (prod + local) | 21:30 UTC | `967ea39` — 45-min delayed retry for Gemini 503 |

Pipeline działa, ale wyłączony (za drogie Gemini API calls). Gotowy do włączenia.

### 2. Energy Prophet (`energy_prophet/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | 08:00 UTC daily | `eb39659` — Add --date CLI arg |

Stabilny. 13 endpointów PSE + 16 lokalizacji pogodowych. Brak otwartych problemów.

### 3. CEE FX Volatility (`cee_fx_volatility/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | Co godzinę (0 0 * * * *) | `3a996d5` — START/FINISH email alerts |

Stabilny. FX + News + Gemini classifier. Nie wyłączony lokalnie (uwaga przy `func start`).

### 4. Gov Spending Radar (`gov_spending_radar/`)
| Status | Trigger | Ostatni commit |
|--------|---------|----------------|
| ✅ prod OK | 06:00 UTC daily | `ad509b8` — Phase 3b: LLM classifier |

Phase 1-3b DONE. Opcjonalne: htmlBody parsing, clientType mapping, NUTS2→nazwy.

### 5. Job Scrapers (`pracuj_scraper/`, `nfj_scraper/`, `just_join_scraper/`)
| Scraper | Status | Ostatni run | Wynik |
|---------|--------|-------------|-------|
| NoFluffJobs | ✅ OK | 2026-04-09 | 1571 ofert → SQL |
| JustJoin.it | ✅ OK | 2026-04-09 | 3043 ofert → SQL |
| Pracuj.pl | ✅ OK | 2026-04-10 | 591 nowych → SQL |

Orchestracja: `scraper_monitor.py` → subprocess isolation → `run_daily_scrapers.bat` → Task Scheduler 20:00.

---

## Niezacommitowane zmiany

Jeden logiczny commit — subprocess isolation + circuit breaker:

| Plik | Zmiana |
|------|--------|
| `pracuj_scraper/scraper_monitor.py` | Refaktor: subprocess isolation zamiast import+run() |
| `nfj_scraper/nfj_data_scraper.py` | Wynik przez `SCRAPER_RESULT_FILE` env var |
| `just_join_scraper/just_join_scraper.py` | j.w. + cache zagranicznych offer_id |
| `pracuj_scraper/pracuj_premium_scraper.py` | j.w. + circuit breaker CF + standalone email fix |
| `.gitignore` | `blog_articles.md`, `*.tmp` |
| `docs/PROJECT_SPEC.md` | NOWY — pełna specyfikacja techniczna |
| `docs/STATUS.md` | NOWY — ten plik |
| `CLAUDE.md` | Slim: z 275 do ~40 linii + linki do docs |

---

## Następny krok

1. **Commit** powyższych zmian
2. **Sprawdzić Task Scheduler** po dzisiejszym runie 20:00 — czy result code wrócił do 0
3. **Monitorować** Pracuj circuit breaker w kolejnych dniach

---

## Znane problemy

- Task Scheduler `Last Result = -2147023829` po timeout Pracuj → przesuwa następny run +1 dzień
- Pracuj circuit breaker nowy — nieprzetestowany przy dużym ruchu CF na prod
- ShillerDailyRun wyłączony — koszty Gemini API
