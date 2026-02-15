# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Portfolio Data Factory** is a monorepo structured as an Azure Functions v2 app (Python 3.12) containing multiple independent ETL pipelines. All pipelines share a single Azure SQL database (`PortfolioMasterDB`). The core philosophy is **Long-Horizon Data Maturity** — autonomous data collection over 9-12 months for backtesting and predictive modeling.

## Architecture

```
Azure Functions (timer-triggered, serverless)
├── ShillerDailyRun → shiller_index/shiller_logic.py    (21:30 UTC, currently DISABLED)
└── EnergyDailyRun  → energy_prophet/pse_connector.py   (08:00 UTC)
                    → energy_prophet/weather_connector.py

Standalone scrapers (run manually from CLI)
├── pracuj_scraper/pracuj_premium_scraper.py   (Playwright-based, pracuj.pl)
├── nfj_scraper/nfj_data_scraper.py            (REST API, nofluffjobs.com)
└── just_join_scraper/just_join_scraper.py      (REST API, justjoin.it)
```

All three job scrapers follow a unified schema for Power BI comparability and upload to Azure SQL tables: `pracuj_offers`, `nfj_offers`, `justjoin_offers`.

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

# Scraper monitor (runs scrapers + validates + email alerts)
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py --dry-run
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py --nfj-only
.venv\Scripts\python.exe -X utf8 pracuj_scraper/scraper_monitor.py --pracuj-only

# Deploy
func azure functionapp publish <app-name>
```

**Windows encoding**: Always use `-X utf8` flag. Scrapers call `sys.stdout.reconfigure(encoding='utf-8')` internally.

## Configuration

- **Azure Functions**: read `os.environ` from `local.settings.json` (local) / Azure App Settings (prod)
- **Shiller**: additionally loads `local.settings.json` via `load_local_settings()` on import
- **Job scrapers**: each has `_load_env()` reading `../.env` (project root) via `os.environ.setdefault()`

Key env vars: `SqlConnectionString`, `GEMINI_API_KEY`, `NEWSAPI_KEY`, `ALERT_EMAIL_FROM/PASSWORD/TO`

## Shared Patterns

### Azure SQL Upload (all job scrapers)
Each scraper defines inline: `CREATE_TABLE_SQL` (IF NOT EXISTS), `MERGE_SQL` (upsert on `url`), `upload_to_azure_sql()` with 3-retry logic. Running twice is always safe — MERGE ensures idempotency.

### SQL Retry
Linear backoff: 3 attempts, `time.sleep(attempt * 5)`. Shiller uses 4 retries with 10s base for serverless cold starts.

### Scraper Deduplication
- NFJ: dedup by `reference` field (same offer appears across regions)
- Pracuj/JustJoin: dedup by `url` (UNIQUE constraint + MERGE)
- NFJ tracks `first_seen_at`/`last_seen`/`is_active` for time-series

### Anti-bot Delays
All scrapers use `time.sleep(random.uniform(min, max))` between requests.

## Key Gotchas

1. **Pracuj.pl uses Playwright**: Phase 1 (listing) is headless, Phase 2 (detail) is headed (visible browser) to bypass Cloudflare Turnstile
2. **JustJoin requires session cookie**: `init_session()` visits homepage first to acquire `unleashSessionId`
3. **NFJ `withSalaryMatch=true`**: Unlocks ~49% more offers vs default API behavior
4. **ShillerDailyRun disabled locally**: `AzureWebJobs.ShillerDailyRun.Disabled=true` in `local.settings.json`
5. **No formal test suite**: Testing via `--sample N` mode, `--dry-run`, probe scripts, and manual runs
6. **Scraper monitor history**: `scraper_run_history.json` stores last 90 runs; alerts on >50% drop in offer count

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
