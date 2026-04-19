# CSV-Only Mode — Design Spec

## Problem

Azure subscription is temporarily read-only (unpaid invoice). All 7 pipelines write to Azure SQL and will fail on upload. Data collection should continue — save locally to CSV, bulk-import to DB when subscription is restored.

## Solution

Environment variable `CSV_ONLY=1` in `.env` controls all pipelines. When set:
- All DB upload functions save data to `csv_staging/{pipeline}/{table}_{timestamp}.csv` instead
- DB-dependent features (classify from DB, reclassify, cleanup) are skipped with a clear log message
- Existing local saves (master CSV, JSON snapshots) continue as before
- A `csv_to_db.py` script bulk-imports staged CSVs after Azure is restored

## Affected Pipelines

| Pipeline | Upload Functions | Tables |
|----------|-----------------|--------|
| NFJ | `upload_to_azure_sql(df)` at line 863 | `nfj_offers` |
| JustJoin | `upload_to_azure_sql(offers)` at line 906 + `update_last_seen_sql()` at line 784 | `justjoin_offers` |
| Pracuj | `upload_to_azure_sql(df)` at line 880 | `pracuj_offers` |
| CEE FX | `upload_fx_rates(records)` in `_run_fx_pipeline` | `cee_fx_rates` |
| CEE FX | `upload_news(records)` in `_run_news_pipeline` | `cee_news_headlines` |
| CEE FX | `_run_reclassify()`, `_run_cleanup()` — skip entirely | (reads from DB) |
| Gov Spending | `upload_notices()`, `upload_contractors()`, `upload_classifications()` in `run()` | `gov_notices`, `gov_contractors`, `gov_classifications` |
| Gov Spending | `_run_classify()` — skip entirely | (reads from DB) |
| Shiller | `save_to_sql_database(final_data)` at lines 901, 964 | `Shiller.DailyScores`, `Shiller.Articles` |
| Energy Prophet | `PSEConnector.run_etl()` — SQL block at line 176 | 9 tables (energy_prices, generation_mix, etc.) |
| Energy Prophet | `WeatherConnector._save_to_sql()` | `weather_data` |

## Architecture

### Shared utility: `csv_staging_utils.py` (project root)

```python
def is_csv_only() -> bool
def save_to_staging(data, pipeline: str, table: str) -> str  # returns path
def staging_dir() -> Path  # csv_staging/
```

- `data` can be `pd.DataFrame`, `list[dict]`, or `dict` (Shiller format)
- For DataFrame/list[dict]: save as CSV directly
- For Shiller dict: flatten metadata + articles into two CSVs (daily_scores + articles)
- Filename: `{table}_{YYYY-MM-DD_HHMMSS}.csv`
- Returns saved file path for logging

### Per-pipeline changes

Each upload function gets a 3-line guard at the top:

```python
from csv_staging_utils import is_csv_only, save_to_staging

if is_csv_only():
    path = save_to_staging(data, "pipeline_name", "table_name")
    print(f"  [CSV-ONLY] Saved to {path}")
    return {"uploaded": 0, "errors": [], "csv_staged": path}
```

### DB-read features: skip with log

For `--classify`, `--reclassify`, `--cleanup` — check `is_csv_only()` in the CLI handler and print skip message.

### `csv_to_db.py` (project root)

Reads `csv_staging/*/`, maps pipeline+table to the correct MERGE SQL, uploads, moves processed files to `csv_staging/done/`.

## Staging directory structure

```
csv_staging/
  .gitkeep
  nfj/
    nfj_offers_2026-04-19_200000.csv
  justjoin/
    justjoin_offers_2026-04-19_200500.csv
  pracuj/
    pracuj_offers_2026-04-19_201000.csv
  cee_fx/
    cee_fx_rates_2026-04-19_213000.csv
    cee_news_headlines_2026-04-19_213000.csv
  gov_spending/
    gov_notices_2026-04-19_220000.csv
    gov_contractors_2026-04-19_220000.csv
    gov_classifications_2026-04-19_220000.csv
  shiller/
    shiller_daily_scores_2026-04-19_213000.csv
    shiller_articles_2026-04-19_213000.csv
  energy/
    energy_prices_2026-04-19_080000.csv
    generation_mix_2026-04-19_080000.csv
    ...
    weather_data_2026-04-19_080000.csv
  done/   # processed files moved here by csv_to_db.py
```

## What does NOT change

- Bat files, Task Scheduler — zero changes
- Existing local saves (master CSV, JSON snapshots, known_offers cache)
- Email notifications — still sent (will report 0 SQL uploads but no errors)
- Scraper monitor orchestration flow

## Rollback

Remove `CSV_ONLY=1` from `.env`. Run `python csv_to_db.py` to import staged data. Done.
