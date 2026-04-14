# Portfolio Data Factory — Project Specification

> Full technical reference. Linked from CLAUDE.md. Update this file when architecture changes.

## Philosophy

**Long-Horizon Data Maturity** — autonomous data collection over 9-12 months for backtesting and predictive modeling. Pipelines run autonomously; analyst intervenes only when broken.

Three principles:
1. **Autonomy** — timer triggers, Task Scheduler. Zero manual intervention.
2. **Resilience** — 2-layer SQL retry, graceful degradation without API keys, email alerts.
3. **Idempotency** — MERGE not INSERT. Running twice = same state.

---

## Architecture

```
Azure Functions (timer-triggered, serverless) — PortfolioMasterDB (Azure SQL)
├── ShillerDailyRun  → shiller_index/shiller_logic.py       (21:30 UTC, DISABLED locally)
├── EnergyDailyRun   → energy_prophet/pse_connector.py      (08:00 UTC)
│                    → energy_prophet/weather_connector.py
├── CeeFxDailyRun   → cee_fx_volatility/main.py             (hourly — NOT disabled locally)
└── GovSpendingRun  → gov_spending_radar/main.py             (06:00 UTC daily)

Standalone scrapers (CLI / Windows Task Scheduler → run_daily_scrapers.bat @ 20:00)
├── pracuj_scraper/pracuj_premium_scraper.py   (Playwright, pracuj.pl)
├── nfj_scraper/nfj_data_scraper.py            (REST API, nofluffjobs.com)
└── just_join_scraper/just_join_scraper.py      (REST API, justjoin.it)

Orchestration: pracuj_scraper/scraper_monitor.py
  → runs NFJ → JustJoin → Pracuj as subprocesses (fault isolation)
  → result passed via SCRAPER_RESULT_FILE env var (temp JSON)
  → sends email report via Gmail SMTP
  → logs/scrapers_YYYY-MM-DD.log (90-day rotation)
```

---

## Pipeline Details

### CEE FX Volatility
**Hypothesis**: PLN volatility shocks spill over to CZK/HUF (foreign investors treat CEE as a basket).

```
cee_fx_volatility/
├── main.py          # Orchestrator + CLI
├── config.yaml      # RSS sources, spam filters, thresholds
├── collectors/fx_collector.py     # yfinance → 1h OHLCV EUR/PLN, EUR/CZK, EUR/HUF
├── collectors/news_collector.py   # RSS bankier.pl, money.pl, investing.com
├── db/operations.py               # 2-layer retry SQL upload
├── ai/classifier.py               # Gemini 2.5 Flash: POLITYKA_KRAJOWA | MAKROEKONOMIA | RPP_STOPY | GEOPOLITYKA | INNE
└── utils/timezone.py
```

FX stream: `volatility_1h = (high-low)/open`. EUR base isolates CEE dynamics from USD noise.

News filtering (5 layers): URL dedup → title dedup → region filter → spam phrases → stale (>7 days).

### Gov Spending Radar
**Goal**: Detect tech trends (AI, Cybersecurity) in Polish public procurement (BZP API).

```
gov_spending_radar/
├── main.py, config.yaml
├── collectors/bzp_client.py   # 6h time-window pagination (PageNumber is broken), dedup by objectId
├── ai/classifier.py           # Two-pass: CPV+keyword (0.85/0.65 conf) → Gemini 2.5 Flash
└── db/schema.py, operations.py
```

Notice types: `ContractNotice` + `TenderResultNotice` (linked by bzp_number/tender_id).
Sectors: `IT`, `CYBERSECURITY`, `AI`, `TELECOM`, `CONSTRUCTION`, `MEDICAL`, `ENERGY`, `INNE`.
Volume: ~600-800 notices/day. Capture rate ~95%.

### Job Scrapers (unified schema → Power BI)

| Scraper | Method | Key quirk |
|---------|--------|-----------|
| **Pracuj.pl** | Playwright 2-phase | Phase 1 headless (listing), Phase 2 headed (Cloudflare Turnstile bypass). Circuit breaker: restart after 5 consecutive CF blocks, abort after 15. |
| **NoFluffJobs** | REST API | `withSalaryMatch=true` unlocks 49% more offers. Tracks `first_seen_at`/`last_seen`/`is_active`. Checkpoint every 50 enrichments. |
| **JustJoin.it** | REST API | `init_session()` visits homepage to get `unleashSessionId` cookie. Incremental cache by offer_id (incl. non-PL offers). |

---

## Shared Patterns

### SQL Upload
Every module: `CREATE_TABLE_SQL` (IF NOT EXISTS) + `MERGE_SQL` (upsert) + `upload_to_azure_sql()`. Running twice is always safe.

### SQL Retry
- **Job scrapers / Shiller**: linear backoff, 3-4 attempts, `time.sleep(attempt * 5)`
- **CEE FX**: 2-layer — Layer 1: 5 attempts, 10s backoff (cold start). Layer 2: 3 attempts, 15s, fresh connection each time.

### Anti-bot
`time.sleep(random.uniform(min, max))` between requests. Pracuj: new browser context per page (fingerprint evasion).

### Scraper Monitor
- `scraper_run_history.json` — last 90 runs, alert on >50% offer count drop
- Subprocess isolation: each scraper = own `python.exe`, result via `SCRAPER_RESULT_FILE` temp JSON
- Standalone mode fallback: `monitor_scraper(name, result)` inline email

---

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

---

## Key Gotchas

1. **Pracuj Phase 2 headed** — Cloudflare Turnstile requires visible browser window. Headless detected.
2. **Pracuj circuit breaker** — `CF_RESTART_AFTER=5`, `CF_ABORT_AFTER=15`. "Ups..." title = CF block.
3. **JustJoin session cookie** — `unleashSessionId` from homepage visit, else 403.
4. **NFJ withSalaryMatch=true** — undocumented param, unlocks 49% more offers.
5. **ShillerDailyRun disabled locally** — `AzureWebJobs.ShillerDailyRun.Disabled=true` in `local.settings.json`.
6. **CeeFxDailyRun runs hourly** — fires every hour, NOT disabled locally.
7. **yfinance unofficial** — Yahoo Finance unofficial API, may break.
8. **Gemini optional** — missing `GEMINI_API_KEY` → NULLs in classification fields, no crash.
9. **No formal test suite** — use `--sample N`, `--dry-run`, manual runs.
10. **BZP PageNumber broken** — ignored by API. Workaround: 6h time-window splitting + dedup by `objectId`.
11. **BZP max 500/request** — some 6h windows hit cap. ~95% daily capture.
12. **NIP format varies** — `_normalize_nip()` handles "NIP: 123", "NIP 123; NIP 456", stores raw on failure.
13. **ContractPerformingNotice skipped** — low ROI, null contractors, hits 500-cap. Needs 4h windows if added.
14. **Task Scheduler skips on error** — `Last Result != 0` can push Next Run by +1 day. Check `taskschd.msc` if scrapers miss a day.
15. **Subprocess isolation** — crash of one scraper does NOT block others. NFJ/JustJoin upload before Pracuj starts.

---

## Configuration

| Source | Used by |
|--------|---------|
| `local.settings.json` | Azure Functions (local), Shiller (`load_local_settings()`) |
| `.env` (project root) | Job scrapers (`_load_env()`), CEE FX (`db/operations.py`) |
| `config.yaml` | CEE FX (RSS, filters), Gov Spending (CPV mappings) |

Key env vars: `SqlConnectionString`, `GEMINI_API_KEY`, `NEWSAPI_KEY`, `ALERT_EMAIL_FROM`, `ALERT_EMAIL_PASSWORD`, `ALERT_EMAIL_TO`

**Windows encoding**: always `-X utf8`. Scrapers call `sys.stdout.reconfigure(encoding='utf-8')` internally.

---

## Roadmap

### Gov Spending Radar
- ✅ Phase 1: API Recon (`docs/API_RECON_REPORT.md`)
- ✅ Phase 2: Core pipeline (BZP client, 3-table schema, CPV+keyword classification)
- ✅ Phase 3b: LLM Classification (Gemini 2.5 Flash, two-pass via `--classify`)
- ⬜ Optional: htmlBody parsing (budget estimates, final prices)
- ⬜ Optional: clientType mapping (numeric codes → institution types)
- ⬜ Optional: NUTS2 → province names

### Job Scrapers
- ✅ Subprocess isolation (fault isolation between scrapers)
- ✅ Pracuj circuit breaker (CF_RESTART_AFTER=5, CF_ABORT_AFTER=15)
- ✅ JustJoin incremental cache (non-PL offers cached, not refetched)

### Infrastructure
- ✅ Task Scheduler daily run (20:00, + at logon catch-up)
- ✅ Email alerts (START/FINISH + per-scraper anomaly detection)
- ✅ 90-day log rotation
