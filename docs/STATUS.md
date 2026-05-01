# Portfolio Data Factory — Status

> Aktualizuj ten plik po każdej sesji. Po `/clear` — czytaj ten plik pierwszy.

## Aktualny stan (2026-04-24)

**CSV-Only mode ZAKOŃCZONY** — Azure SQL przywrócony, dane zaimportowane. Architektura docelowa:
- **Azure Functions Timer** → Shiller / Energy / CEE FX / Gov Spending (4 triggery w `*DailyRun/` folderach)
- **Lokalny Windows Task Scheduler** → Job Scrapers (Azure Functions ma limit 10min; Pracuj potrzebuje do 240min)

### Co się wydarzyło (2026-04-20 → 2026-04-21):
1. Azure subscription read-only (niezapłacona FV) → włączono `CSV_ONLY=1`
2. Stworzono lokalny runner (`run_etl_local.py`) + Task Scheduler (CEE co 1h, ETL Daily 08:00) — **tylko dla CSV-Only**
3. Zebrano dane przez ~24h: Energy, Gov Spending, CEE FX (24 hourly runs), Job Scrapers
4. Azure odblokowany → dodano IP do firewall (dynamiczne IP: 185.203.173.180, potem 91.94.8.24)
5. `csv_to_db.py` zaimportował 82 pliki do Azure SQL (naprawiono 3 bugi: JustJoin ast.literal_eval, weather datetime, energy schema routing)
6. Usunięto **CSV-Only** automatyzację (`run_etl_local.py`, `run_cee_fx_hourly.bat`, `run_etl_daily.bat`, `setup_task_scheduler.ps1`) — CEE/Energy/Gov/Shiller wróciły pod Azure Functions
7. `CSV_ONLY=1` usunięte z `.env`

### Uwaga (2026-04-24):
Task `Portfolio Data Factory - Daily Scrapers` (lokalny Task Scheduler dla scraperów) **NIE jest częścią CSV-Only** — to stała produkcyjna orkiestracja. Commit `1f641cb` go nie dotykał. Jeśli przypadkowo wyrejestrowany, odtworzyć komendą:
```powershell
# Admin PowerShell:
Register-ScheduledTask -TaskName "Portfolio Data Factory - Daily Scrapers" `
  -Xml (Get-Content "C:\Users\sadza\PycharmProjects\portfolio-data-factory\scheduler_task.xml" | Out-String) `
  -User "Full STEAM Ahead"
```
Lub: `& .\setup_scheduler.ps1` jako admin.

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

Orchestracja: `scraper_monitor.py` → `run_daily_scrapers.bat` → Windows Task Scheduler (`scheduler_task.xml`).
Triggery: codziennie 19:00 (lokalny czas) + LogonTrigger+3min (catch-up po przerwie).
Pracuj timeout zwiększony do 240min (CF_WAIT=7s). **Nie przenosić na Azure Functions** — limit 10min.

---

## Niezacommitowane zmiany

Brak — wszystko zacommitowane i wypushowane do origin.

---

## Aktywne wątki (stan 2026-04-24, do kontynuacji)

### 🔥 Inflation Scraper — DECYZJE ZAMKNIĘTE (2026-04-30) → spec: `docs/INFLATION_BASKET_SPEC.md`

**Otwarte pytania zamknięte:**
- Filozofia koszyka: **(B)** Inżynier — własne zakupy, equal-weight. Wagi GUS dopiero V2 (= ścieżka D rozłożona w czasie).
- Sklepy: **Frisco + Auchan zakupy (Warszawa)**. Carrefour/Ceneo wycięte (2 wystarczą; research 2026-04-30 potwierdził feasibility tych dwóch).
- N produktów: **40** (3-4 per kategoria GUS, threshold reprezentatywności).
- Cadence: **3× tygodniowo** (pn/śr/pt 22:00) — nie codziennie, sustainability.
- AI: **NIE w MVP**, V1 dopiero (klasyfikacja kategorii GUS + shrinkflation detection przez Gemini Flash-Lite).
- Folder: `inflation_basket/`.
- Start: **teraz** (po sesji 2026-04-30).

**Harmonogram:** MVP 4 tyg. → V1 +4 tyg. → V2 po 6 mies. zbierania danych (= dual-index mój/GUS, public dashboard, LinkedIn launch).

**Sustainability gates** (z konstytucji mental state 2026-04-30): pauza po MVP/V1 dozwolona, stop-signs zdefiniowane w specu §11.

**Status (2026-05-01, koniec sesji — pauza ~3h, /clear wykonany):**
- ✅ Wybór produktów: **52 produktów** w `inflation_basket/seed/products.py`
- ✅ Matching strategy: 29 same_sku + 23 logical_only (po korektach: Erytrytol/Sól → logical_only z brand=None)
- ✅ Diversyfikacja dostawców: 24 marki, max 2 produkty per marka
- ✅ Mini-koszyk importowany: 9 produktów (~17%) — Grana Padano IT, Mutti IT, Monini IT, Granoro IT, Kalamata GR, banany EC, awokado/cytryny ES, mango BR
- ✅ `inflation_basket/db/schema.py` + `db/operations.py` (2-layer retry, MERGE)
- ✅ Master catalog w Azure SQL — 4 tabele, **52 produkty po DELETE+re-seed**
- ✅ `inflation_basket/url_mapper.py` (interactive manual fallback) + `auto_mapper.py` (algorithmic, scoring 40/30/30)
- ✅ **Branch `feat/inflation-basket`** + commit `254cf4a` (11 files, +1633). Pracujemy na branchu, nie main.

**🚧 GDZIE STANĘLIŚMY — następna sesja zaczyna tu:**
- ⏳ **Auchan setup**: Damian musi uruchomić url_mapper, wybrać sklep Warszawa, zapisać sesję. Plik docelowy: `inflation_basket/seed/playwright_state/auchan_warsaw.json`
- ⏳ **URL mapping wszystkich 52 produktów × 2 sklepy** — DB obecnie ma 0 URL (po re-seed wszystkie poprzednie 16 zostały DELETED). Po Auchan setup: spawn subagent Sonnet 4.6 z **poprawkami scoring**: threshold 0.7→0.5 dla logical_only, brand exact match bonus +0.2.
- ⏳ Frisco scraper + Auchan scraper — po URL mapping
- ⏳ Power BI minimal — po pierwszych obserwacjach

**Pełny handoff (komendy, prompt subagenta, mental state):** patrz `docs/SESSION_HANDOFF_2026-05-01.md`.
- ⏳ Auto-mapper re-run dla obu sklepów (oczekiwane ~40-45 saved per sklep)
- ⏳ Frisco + Auchan scraper — po URL mapping

### LinkedIn — urodziny 2026-05-03 jako symboliczny start
- Target pierwszego posta: **3 maja 2026** (41. urodziny, za 9 dni od 2026-04-24).
- Format: *„Dla uczczenia 41-tki — co buduję. Portfolio Data Factory, 7 pipeline'ów, Azure + AI"*.
- Status: **nie zdecydowane, nie zaczęte**. User ma to potwierdzić / napisać draft.

### Stare punkty (priorytet niższy niż powyższe):
1. **Token optimization** — plan batch 3 tickery Shillera w 1 request (patrz memory: `project_shiller_token_optimization.md`)
2. **Zmiana modelu CEE FX + Gov Spending** — na tańszy (Gemini 503 issues)
3. **Azure firewall** — rozważyć zakres IP zamiast pojedynczych adresów

## Backlog (portfolio-positioning)

- **Job Scrapers: ekstrakcja skillsów przez LLM** — z treści ofert wyciągać listę skillów + trendy (co rośnie/spada w kategoriach data/AI/BA). Double-use: portfolio (senior-level AI-where-it-adds-value) + własny radar rynku pracy. Ground truth = ręczna weryfikacja na ~50 ofertach. Wersja: pilot na NFJ, potem rozszerzyć.
- **Gov Spending: ewaluacja istniejącego klasyfikatora LLM** — ~100 ręcznie oznaczonych próbek, policzyć precision/recall/F1. Mocny bullet do CV: „evaluation-driven ML, nie wróżenie".
- **Power BI Executive Dashboard** — jeden raport spinający 7 pipeline'ów w widok wykonawczy (Shiller CAPE+sentyment / CEE FX vol / Gov Spending anomalie / Job Market trendy). Publiczny iframe → LinkedIn-embed. DirectQuery na Azure SQL. ~1 weekend. **Proof-points:** E2E + storytelling. Zamyka „visible front" którego obecnie brakuje.
- **RAG nad zamówieniami publicznymi** — Q&A nad `gov_spending_radar` z cytatami źródeł (vector DB, orchestracja LangGraph/prostsza, eval Q&A). ~2-3 weekendy. **Unikalny** (urząd-insider + AI + strukturalne dane), trudny do skopiowania. **Proof-points:** AI flagship + prod edge cases.
- **Kwartalny raport „Data Lens" na LinkedIn** — jednopager z `gov_spending_radar` (top anomalie kwartału, PDF + LinkedIn post + link do dashboardu). Niski koszt techniczny, najwyższy efekt contentowy. Wymaga dyscypliny (jeden skipped quarter psuje wrażenie).
- **🔥 Inflation Scraper** (patrz „Aktywne wątki" wyżej) — najmocniejszy projekt w roadmapie. Time-critical start. Filozofia koszyka do ustalenia (A/B/D).

---

## Znane problemy

- Task Scheduler `Last Result = -2147023829` po timeout Pracuj → przesuwa następny run +1 dzień
- Pracuj circuit breaker nowy — nieprzetestowany przy dużym ruchu CF na prod
- Gemini 2.5 Flash ma 503 overload issues — Shiller na 3.1 Flash Live, CEE/Gov nadal na 2.5
- Azure SQL firewall — dynamiczne IP wymaga ręcznego dodawania
