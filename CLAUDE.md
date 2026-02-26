# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Portfolio Data Factory** is a monorepo structured as an Azure Functions v2 app (Python 3.12) containing multiple independent ETL pipelines. All pipelines share a single Azure SQL database (`PortfolioMasterDB`). The core philosophy is **Long-Horizon Data Maturity** — autonomous data collection over 9-12 months for backtesting and predictive modeling.

## Architecture

```
Azure Functions (timer-triggered, serverless)
├── ShillerDailyRun  → shiller_index/shiller_logic.py       (21:30 UTC, currently DISABLED)
├── EnergyDailyRun   → energy_prophet/pse_connector.py      (08:00 UTC)
│                    → energy_prophet/weather_connector.py
├── CeeFxDailyRun   → cee_fx_volatility/main.py             (every hour, 0 0 * * * *)
└── GovSpendingRun  → gov_spending_radar/main.py             (06:00 UTC daily)

Standalone scrapers (run manually from CLI or via run_daily_scrapers.bat)
├── pracuj_scraper/pracuj_premium_scraper.py   (Playwright-based, pracuj.pl)
├── nfj_scraper/nfj_data_scraper.py            (REST API, nofluffjobs.com)
├── just_join_scraper/just_join_scraper.py      (REST API, justjoin.it)
└── gov_spending_radar/main.py                  (REST API, ezamowienia.gov.pl)
```

All three job scrapers follow a unified schema for Power BI comparability and upload to Azure SQL tables: `pracuj_offers`, `nfj_offers`, `justjoin_offers`.

### CEE FX Volatility Pipeline

Researches spillover effects of PLN volatility shocks onto CZK and HUF (hypothesis: foreign investors treat CEE as a basket).

```
cee_fx_volatility/
├── main.py                  # Orchestrator + CLI entry point
├── config.yaml              # RSS sources, spam filters, thresholds
├── collectors/
│   ├── fx_collector.py      # yfinance → 1h OHLCV for EUR/PLN, EUR/CZK, EUR/HUF
│   └── news_collector.py    # RSS from bankier.pl, money.pl → filtered headlines
├── db/
│   ├── operations.py        # Azure SQL upload with 2-layer retry
│   └── schema.py            # CREATE TABLE / MERGE SQL
├── ai/
│   └── classifier.py        # Gemini 2.5 Flash structured output classifier
└── utils/
    └── timezone.py          # UTC conversion helpers
```

Two independent data streams:
- **FX**: `yfinance` → validates OHLCV → computes `volatility_1h = (high-low)/open` → `cee_fx_rates`
- **News**: RSS feeds → spam/stale/auto-FX filtering → Gemini classification → `cee_news_headlines`

AI classifier categories: `POLITYKA_KRAJOWA`, `MAKROEKONOMIA`, `RPP_STOPY`, `GEOPOLITYKA`, `INNE`. If `GEMINI_API_KEY` is absent, news is stored without classification (NULLs).

### Gov Spending Radar Pipeline

Collects Polish public procurement data from BZP (Biuletyn Zamówień Publicznych) via ezamowienia.gov.pl API. Detects tech trends (AI, Cybersecurity) in public sector spending.

```
gov_spending_radar/
├── main.py                  # Orchestrator + CLI entry point
├── config.yaml              # CPV→sector mappings, API settings
├── api_recon.py             # Phase 1 recon script (diagnostic)
├── collectors/
│   └── bzp_client.py        # BZP API client with time-window pagination + dedup
├── db/
│   ├── schema.py            # CREATE TABLE / MERGE / migrations
│   └── operations.py        # Azure SQL upload with 2-layer retry
└── docs/
    └── API_RECON_REPORT.md  # API findings and limitations
```

Two notice types collected: `ContractNotice` (new procurements) and `TenderResultNotice` (awards with contractors). Linked by `bzp_number`/`tender_id`. API has broken pagination (PageNumber ignored, max 500/request) — uses 6h time-window splitting + dedup by `objectId`.

Classification: CPV code prefix matching (0.85 confidence) + title keyword matching (0.65 confidence). Sectors: `IT`, `CYBERSECURITY`, `AI`, `TELECOM`, `CONSTRUCTION`, `MEDICAL`, `ENERGY`. ~60% of notices get classified; rest await LLM classifier (Phase 3b).

## Build & Run

```bash
# Setup
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium  # only for pracuj_scraper

# Azure Functions locally
func start

# Job scrapers (from project root, Windows)
.venv\Scripts\python.exe -X utf8 nfj_scraper/nfj_data_scraper.py
.venv\Scripts\python.exe -X utf8 nfj_scraper/nfj_data_scraper.py --sample 20
.venv\Scripts\python.exe -X utf8 -m pracuj_scraper.pracuj_premium_scraper
.venv\Scripts\python.exe -X utf8 just_join_scraper/just_join_scraper.py

# CEE FX Volatility (from project root, as module)
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main                # current period (FX 5d + news)
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --backfill 30  # historical FX, last N days (max 730)
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --fx-only      # FX stream only
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --news-only    # news stream only
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --reclassify   # re-run Gemini on NULL-category rows
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --cleanup      # delete stale + auto-FX rows

# Gov Spending Radar (from project root, as module)
.venv\Scripts\python.exe -X utf8 -m gov_spending_radar.main                 # yesterday's notices
.venv\Scripts\python.exe -X utf8 -m gov_spending_radar.main --backfill 30   # last N days (max 730)
.venv\Scripts\python.exe -X utf8 -m gov_spending_radar.main --date 2026-02-20  # specific date
.venv\Scripts\python.exe -X utf8 -m gov_spending_radar.main --classify       # reclassify untagged notices
.venv\Scripts\python.exe -X utf8 -m gov_spending_radar.main --sample 5       # dry-run, no SQL upload

# Scraper monitor (runs scrapers + validates + email alerts)
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py --dry-run
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py --nfj-only
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py --pracuj-only

# Batch runner (run_daily_scrapers.bat)
# Runs scraper_monitor.py, disables Windows sleep during execution, 90-day log rotation

# Deploy
func azure functionapp publish <app-name>
```

**Windows encoding**: Always use `-X utf8` flag. Scrapers call `sys.stdout.reconfigure(encoding='utf-8')` internally.

## Configuration

- **Azure Functions**: read `os.environ` from `local.settings.json` (local) / Azure App Settings (prod)
- **Shiller**: additionally loads `local.settings.json` via `load_local_settings()` on import
- **CEE FX**: `db/operations.py` loads both `.env` and `local.settings.json`; `config.yaml` for RSS sources and filtering rules
- **Job scrapers**: each has `_load_env()` reading `../.env` (project root) via `os.environ.setdefault()`

Key env vars: `SqlConnectionString`, `GEMINI_API_KEY`, `NEWSAPI_KEY`, `ALERT_EMAIL_FROM/PASSWORD/TO`

## Shared Patterns

### Azure SQL Upload (all pipelines)
Each module defines: `CREATE_TABLE_SQL` (IF NOT EXISTS), `MERGE_SQL` (upsert), `upload_to_azure_sql()` with retry logic. Running twice is always safe — MERGE ensures idempotency.

### SQL Retry
- **Job scrapers / Shiller**: Linear backoff, 3-4 attempts, `time.sleep(attempt * 5)`. Shiller uses 4 retries with 10s base for serverless cold starts.
- **CEE FX**: Two-layer retry — Layer 1 (`_connect_with_retry`): 5 attempts, 10s linear backoff; Layer 2 (batch upload): 3 attempts with 15s backoff, each getting a fresh connection.

### Scraper Deduplication
- NFJ: dedup by `reference` field (same offer appears across regions)
- Pracuj/JustJoin: dedup by `url` (UNIQUE constraint + MERGE)
- NFJ tracks `first_seen_at`/`last_seen`/`is_active` for time-series

### Anti-bot Delays
All scrapers use `time.sleep(random.uniform(min, max))` between requests.

### News Filtering (CEE FX)
Three-layer filtering before SQL insert:
1. Spam phrases from `config.yaml` (e.g., "artykul sponsorowany")
2. Stale articles older than `max_article_age_days` (default 7 days)
3. Auto-generated FX headlines (Money.pl daily currency reports — regex patterns)

## Key Gotchas

1. **Pracuj.pl uses Playwright**: Phase 1 (listing) is headless, Phase 2 (detail) is headed (visible browser) to bypass Cloudflare Turnstile
2. **JustJoin requires session cookie**: `init_session()` visits homepage first to acquire `unleashSessionId`
3. **NFJ `withSalaryMatch=true`**: Unlocks ~49% more offers vs default API behavior
4. **ShillerDailyRun disabled locally**: `AzureWebJobs.ShillerDailyRun.Disabled=true` in `local.settings.json`
5. **CeeFxDailyRun runs hourly**: Unlike other functions, it fires every hour and is NOT disabled locally
6. **yfinance is unofficial**: FX data via Yahoo Finance unofficial API — may break without notice
7. **Gemini classifier is optional**: If `GEMINI_API_KEY` is missing, news headlines are stored with NULL category/sentiment
8. **No formal test suite**: Testing via `--sample N` mode, `--dry-run`, probe scripts, and manual runs
11. **BZP API pagination is broken**: `PageNumber` parameter is ignored — always returns same records. Workaround: time-window splitting (6h) + dedup by `objectId`
12. **BZP API max 500 per request**: Some 6h windows hit the cap (~95% daily capture rate)
13. **NIP format varies wildly**: Raw data has "NIP: 123...", "REGON:123...", "NIP 123; NIP 456" — `_normalize_nip()` handles common cases, stores raw when can't parse
14. **GovSpendingRun fires daily at 06:00 UTC**: Fetches yesterday's BZP notices. Can be disabled locally with `AzureWebJobs.GovSpendingRun.Disabled=true` in `local.settings.json`
9. **Scraper monitor history**: `scraper_run_history.json` stores last 90 runs; alerts on >50% drop in offer count
10. **run_daily_scrapers.bat**: Disables Windows sleep via `powercfg` during execution, restores after; deletes logs older than 90 days

## Azure SQL Tables

| Table | Module | Upsert Key |
|-------|--------|------------|
| `Shiller.DailyScores` | shiller_logic | date + ticker |
| `Shiller.Articles` | shiller_logic | date + ticker + article_num |
| `energy_prices`, `generation_mix`, `power_balance`, etc. | pse_connector | various |
| `weather_data` | weather_connector | location + timestamp |
| `pracuj_offers` | pracuj_scraper | url |
| `nfj_offers` | nfj_scraper | url |
| `justjoin_offers` | just_join_scraper | url |
| `cee_fx_rates` | cee_fx_volatility | timestamp + currency_pair |
| `cee_news_headlines` | cee_fx_volatility | url |
| `gov_notices` | gov_spending_radar | object_id |
| `gov_contractors` | gov_spending_radar | notice_object_id + part_index |
| `gov_classifications` | gov_spending_radar | notice_object_id + method |
