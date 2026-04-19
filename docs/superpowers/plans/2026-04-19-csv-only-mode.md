# CSV-Only Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow all 7 pipelines to run with `CSV_ONLY=1` in `.env`, saving data to local CSV staging instead of Azure SQL, with a bulk import script for when Azure is restored.

**Architecture:** Shared `csv_staging_utils.py` provides `is_csv_only()` and `save_to_staging()`. Each pipeline's upload function gets a 3-line guard clause. DB-read features (classify/reclassify/cleanup) are skipped. `csv_to_db.py` handles bulk import.

**Tech Stack:** Python 3.12, pandas, pyodbc, existing MERGE SQL from each pipeline

**Spec:** `docs/superpowers/specs/2026-04-19-csv-only-mode-design.md`

---

### Task 1: Create `csv_staging_utils.py` shared utility

**Files:**
- Create: `csv_staging_utils.py`

- [ ] **Step 1: Create the utility module**

```python
"""
CSV Staging Utilities — CSV-Only Mode for Portfolio Data Factory.
When CSV_ONLY=1 is set in .env, pipelines save data here instead of Azure SQL.
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
STAGING_DIR = PROJECT_ROOT / "csv_staging"


def is_csv_only() -> bool:
    """Check if CSV_ONLY mode is enabled via environment variable."""
    return os.environ.get("CSV_ONLY", "").strip() in ("1", "true", "yes")


def save_to_staging(data, pipeline: str, table: str) -> str:
    """
    Save data to csv_staging/{pipeline}/{table}_{timestamp}.csv.

    Args:
        data: pd.DataFrame or list[dict]
        pipeline: subdirectory name (e.g. "nfj", "cee_fx")
        table: table name for filename (e.g. "nfj_offers")

    Returns:
        Path to saved CSV file.
    """
    target_dir = STAGING_DIR / pipeline
    target_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{table}_{timestamp}.csv"
    path = target_dir / filename

    if isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        raise TypeError(f"Unsupported data type: {type(data)}")

    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  [CSV-ONLY] Staged {len(df)} rows -> {path}")
    return str(path)
```

- [ ] **Step 2: Create staging directory with .gitignore**

```bash
mkdir -p csv_staging
```

Create `csv_staging/.gitignore`:
```
# Ignore staged CSV data (local-only, not for git)
*
!.gitignore
```

- [ ] **Step 3: Add `csv_staging/` to root `.gitignore`**

Append to `.gitignore`:
```
# CSV staging (CSV-Only mode)
csv_staging/
```

- [ ] **Step 4: Commit**

```bash
git add csv_staging_utils.py csv_staging/.gitignore .gitignore
git commit -m "feat: add csv_staging_utils.py for CSV-Only mode"
```

---

### Task 2: Add CSV-Only guard to Job Scrapers (NFJ, JustJoin, Pracuj)

**Files:**
- Modify: `nfj_scraper/nfj_data_scraper.py:627` (upload_to_azure_sql)
- Modify: `just_join_scraper/just_join_scraper.py:602` (upload_to_azure_sql)
- Modify: `just_join_scraper/just_join_scraper.py:675` (update_last_seen_sql)
- Modify: `pracuj_scraper/pracuj_premium_scraper.py:664` (upload_to_azure_sql)

- [ ] **Step 1: Add guard to NFJ `upload_to_azure_sql`**

At the top of `nfj_scraper/nfj_data_scraper.py`, add import (near other imports):
```python
from csv_staging_utils import is_csv_only, save_to_staging
```

At the start of `upload_to_azure_sql(df)` function body (line ~628), add:
```python
def upload_to_azure_sql(df: pd.DataFrame) -> dict:
    if is_csv_only():
        path = save_to_staging(df, "nfj", "nfj_offers")
        return {"uploaded": 0, "errors": []}

    # ... existing code unchanged ...
```

- [ ] **Step 2: Add guard to JustJoin `upload_to_azure_sql`**

At the top of `just_join_scraper/just_join_scraper.py`, add import (near other imports):
```python
from csv_staging_utils import is_csv_only, save_to_staging
```

At the start of `upload_to_azure_sql(offers)` function body (line ~603), add:
```python
def upload_to_azure_sql(offers: list[dict]) -> dict:
    if is_csv_only():
        path = save_to_staging(offers, "justjoin", "justjoin_offers")
        return {"uploaded": 0, "errors": []}

    # ... existing code unchanged ...
```

Also guard `update_last_seen_sql` (line ~675):
```python
def update_last_seen_sql(offer_ids: list[str]):
    if is_csv_only():
        return
    # ... existing code unchanged ...
```

- [ ] **Step 3: Add guard to Pracuj `upload_to_azure_sql`**

At the top of `pracuj_scraper/pracuj_premium_scraper.py`, add import (near other imports):
```python
from csv_staging_utils import is_csv_only, save_to_staging
```

At the start of `upload_to_azure_sql(df)` function body (line ~665), add:
```python
def upload_to_azure_sql(df: pd.DataFrame) -> dict:
    if is_csv_only():
        path = save_to_staging(df, "pracuj", "pracuj_offers")
        return {"uploaded": 0, "errors": []}

    # ... existing code unchanged ...
```

- [ ] **Step 4: Verify imports work**

```bash
cd C:\Users\sadza\PycharmProjects\portfolio-data-factory
.venv\Scripts\python.exe -c "from csv_staging_utils import is_csv_only, save_to_staging; print('OK')"
```

Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add nfj_scraper/nfj_data_scraper.py just_join_scraper/just_join_scraper.py pracuj_scraper/pracuj_premium_scraper.py
git commit -m "feat: add CSV-Only guard to job scrapers (NFJ, JustJoin, Pracuj)"
```

---

### Task 3: Add CSV-Only guard to CEE FX Volatility

**Files:**
- Modify: `cee_fx_volatility/db/operations.py` (upload_fx_rates, upload_news)
- Modify: `cee_fx_volatility/main.py` (skip reclassify/cleanup in CSV-Only mode)

- [ ] **Step 1: Add guard to `upload_fx_rates` and `upload_news` in `db/operations.py`**

Add import at top of `cee_fx_volatility/db/operations.py`:
```python
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from csv_staging_utils import is_csv_only, save_to_staging
```

Find the `upload_fx_rates` function and add guard at start:
```python
def upload_fx_rates(records: list[dict]) -> dict:
    if is_csv_only():
        save_to_staging(records, "cee_fx", "cee_fx_rates")
        return {"uploaded": 0, "errors": []}
    # ... existing code ...
```

Find the `upload_news` function and add guard at start:
```python
def upload_news(records: list[dict]) -> dict:
    if is_csv_only():
        save_to_staging(records, "cee_fx", "cee_news_headlines")
        return {"uploaded": 0, "errors": []}
    # ... existing code ...
```

- [ ] **Step 2: Skip DB-read operations in `main.py`**

In `cee_fx_volatility/main.py`, add import at top:
```python
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from csv_staging_utils import is_csv_only
```

In `main()` function, where `args.reclassify` and `args.cleanup` are handled, add guard:
```python
    if args.reclassify:
        if is_csv_only():
            print("  [CSV-ONLY] --reclassify requires DB access, skipping")
            sys.exit(0)
        _run_reclassify()
        return

    if args.cleanup:
        if is_csv_only():
            print("  [CSV-ONLY] --cleanup requires DB access, skipping")
            sys.exit(0)
        _run_cleanup()
        return
```

- [ ] **Step 3: Commit**

```bash
git add cee_fx_volatility/db/operations.py cee_fx_volatility/main.py
git commit -m "feat: add CSV-Only guard to CEE FX Volatility pipeline"
```

---

### Task 4: Add CSV-Only guard to Gov Spending Radar

**Files:**
- Modify: `gov_spending_radar/db/operations.py` (upload_notices, upload_contractors, upload_classifications)
- Modify: `gov_spending_radar/main.py` (skip classify mode)

- [ ] **Step 1: Add guard to upload functions in `db/operations.py`**

Add import at top of `gov_spending_radar/db/operations.py`:
```python
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from csv_staging_utils import is_csv_only, save_to_staging
```

Add guard to each upload function:
```python
def upload_notices(notices: list[dict]) -> dict:
    if is_csv_only():
        save_to_staging(notices, "gov_spending", "gov_notices")
        return {"uploaded": 0, "errors": []}
    # ... existing code ...

def upload_contractors(contractors: list[dict]) -> dict:
    if is_csv_only():
        save_to_staging(contractors, "gov_spending", "gov_contractors")
        return {"uploaded": 0, "errors": []}
    # ... existing code ...

def upload_classifications(classifications: list[dict]) -> dict:
    if is_csv_only():
        save_to_staging(classifications, "gov_spending", "gov_classifications")
        return {"uploaded": 0, "errors": []}
    # ... existing code ...
```

- [ ] **Step 2: Skip classify mode in `main.py`**

In `gov_spending_radar/main.py`, add import at top:
```python
import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))
from csv_staging_utils import is_csv_only
```

In the `run()` function, before `mode == "classify"` block (around line 386):
```python
        if mode == "classify":
            if is_csv_only():
                print("  [CSV-ONLY] --classify requires DB access, skipping")
                result["success"] = True
                return result
            classify_result = _run_classify(config, llm_only=llm_only)
            # ... rest unchanged ...
```

- [ ] **Step 3: Commit**

```bash
git add gov_spending_radar/db/operations.py gov_spending_radar/main.py
git commit -m "feat: add CSV-Only guard to Gov Spending Radar pipeline"
```

---

### Task 5: Add CSV-Only guard to Shiller Index

**Files:**
- Modify: `shiller_index/shiller_logic.py` (save_to_sql_database)

- [ ] **Step 1: Add guard to `save_to_sql_database`**

Add import at top of `shiller_index/shiller_logic.py`:
```python
import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))
from csv_staging_utils import is_csv_only, save_to_staging
```

Add CSV-Only guard at the start of `save_to_sql_database` (line ~715):
```python
def save_to_sql_database(final_data: dict) -> bool:
    """Save analysis results to SQL database with retry logic. Returns True on success, False on failure."""
    if not final_data:
        return False

    if is_csv_only():
        _save_shiller_to_csv(final_data)
        return True

    # ... existing code unchanged ...
```

Add the helper function before `save_to_sql_database`:
```python
def _save_shiller_to_csv(final_data: dict) -> None:
    """Save Shiller analysis to CSV staging (two files: scores + articles)."""
    import pandas as pd

    meta = final_data["metadata"]
    scores = final_data["aggregated_scores"]

    # DailyScores row
    scores_row = {
        "date": meta["analysis_date"],
        "ticker": meta["ticker"],
        "price": meta["price"],
        "ma_30": meta["ma_30"],
        "gap_pct": meta["gap_pct"],
        "final_sentiment": scores["final_sentiment"],
        "final_hype": scores["final_hype"],
        "sentiment_confidence": scores["sentiment_confidence"],
        "hype_confidence": scores["hype_confidence"],
        "articles_received": meta["articles_received"],
        "articles_used_sentiment": scores["articles_used_sentiment"],
        "articles_used_hype": scores["articles_used_hype"],
    }
    save_to_staging([scores_row], "shiller", "shiller_daily_scores")

    # Articles rows
    articles_rows = []
    for art in final_data.get("articles", []):
        qual = calculate_quality_scores(art)
        qm = art.get("quality_metrics") or {}
        sc = art.get("scores") or {}
        filt = art.get("filter") or {}
        articles_rows.append({
            "date": meta["analysis_date"],
            "ticker": meta["ticker"],
            "article_num": art.get("article_num"),
            "headline_preview": (art.get("headline_preview") or "")[:250],
            "is_about_company": filt.get("is_about_company"),
            "sentiment_usable": filt.get("sentiment_usable"),
            "hype_usable": filt.get("hype_usable"),
            "excluded": 1 if filt.get("excluded") else 0,
            "exclusion_reason": filt.get("exclusion_reason"),
            "centrality": qm.get("centrality"),
            "credibility_sentiment": qm.get("credibility_sentiment"),
            "credibility_hype": qm.get("credibility_hype"),
            "recency": qm.get("recency"),
            "materiality": qm.get("materiality"),
            "speculation_signal": qm.get("speculation_signal"),
            "quality_sentiment": qual["quality_sentiment"],
            "quality_hype": qual["quality_hype"],
            "sentiment_raw": sc.get("sentiment_raw"),
            "hype_raw": sc.get("hype_raw"),
            "reasoning": (art.get("reasoning") or "")[:2000],
        })
    if articles_rows:
        save_to_staging(articles_rows, "shiller", "shiller_articles")
```

- [ ] **Step 2: Commit**

```bash
git add shiller_index/shiller_logic.py
git commit -m "feat: add CSV-Only guard to Shiller Index pipeline"
```

---

### Task 6: Add CSV-Only guard to Energy Prophet

**Files:**
- Modify: `energy_prophet/pse_connector.py` (run_etl SQL block)
- Modify: `energy_prophet/weather_connector.py` (_save_to_sql)

- [ ] **Step 1: Add guard to PSEConnector.run_etl**

Add import at top of `energy_prophet/pse_connector.py`:
```python
import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))
from csv_staging_utils import is_csv_only, save_to_staging
```

In `run_etl`, after the fetch loop builds `raw` dict (around line 170), add a CSV-only branch before the SQL block:

```python
            if not raw:
                continue

            # CSV-Only mode: save raw DataFrames to staging instead of SQL
            if is_csv_only():
                for ep, df in raw.items():
                    table = ENDPOINT_CONFIG[ep]["target_table"]
                    save_to_staging(df, "energy", table)
                continue

            # Upload do SQL (existing code unchanged)
            import time as _time
            # ... rest of existing SQL block ...
```

- [ ] **Step 2: Add guard to WeatherConnector._save_to_sql**

Add import at top of `energy_prophet/weather_connector.py`:
```python
import sys as _sys
_sys.path.insert(0, str(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))
from csv_staging_utils import is_csv_only, save_to_staging
```

Add guard at the start of `_save_to_sql`:
```python
def _save_to_sql(self, df):
    if is_csv_only():
        save_to_staging(df, "energy", "weather_data")
        return
    # ... existing code unchanged ...
```

- [ ] **Step 3: Commit**

```bash
git add energy_prophet/pse_connector.py energy_prophet/weather_connector.py
git commit -m "feat: add CSV-Only guard to Energy Prophet pipeline"
```

---

### Task 7: Create `csv_to_db.py` bulk import script

**Files:**
- Create: `csv_to_db.py`

- [ ] **Step 1: Create the import script**

```python
"""
CSV-to-DB Bulk Import — Portfolio Data Factory
===============================================
Imports staged CSV files from csv_staging/ into Azure SQL.
Run after Azure subscription is restored.

Usage:
    python -X utf8 csv_to_db.py                  # import all staged CSVs
    python -X utf8 csv_to_db.py --dry-run        # show what would be imported
    python -X utf8 csv_to_db.py --pipeline nfj   # import only NFJ data
"""

import argparse
import os
import shutil
import sys
import time
import pyodbc
import pandas as pd
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent
STAGING_DIR = PROJECT_ROOT / "csv_staging"
DONE_DIR = STAGING_DIR / "done"
ENV_FILE = PROJECT_ROOT / ".env"


def _load_env():
    """Load .env variables."""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def _connect(max_retries: int = 3) -> pyodbc.Connection:
    """Connect to Azure SQL with retry."""
    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        raise RuntimeError("SqlConnectionString not set in .env")

    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str, timeout=60)
            print(f"  [SQL] Connected (attempt {attempt + 1})")
            return conn
        except pyodbc.Error as e:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 15
                print(f"  [SQL] Connection failed, retrying in {wait}s... ({e})")
                time.sleep(wait)
            else:
                raise


# ── Pipeline-specific import logic ──────────────────────────────

def _import_generic_merge(conn, df: pd.DataFrame, merge_sql: str, param_builder, table_name: str) -> int:
    """Generic MERGE import. Returns number of rows imported."""
    cursor = conn.cursor()
    imported = 0
    for _, row in df.iterrows():
        try:
            params = param_builder(row)
            cursor.execute(merge_sql, params)
            imported += 1
        except Exception as e:
            print(f"    [!] Row failed: {e}")
    conn.commit()
    print(f"    {table_name}: {imported}/{len(df)} rows imported")
    return imported


# Each pipeline maps to its import function.
# Import functions read the CSV, call the pipeline's existing upload logic.

def _import_scraper_csv(conn, csv_path: Path, pipeline: str) -> int:
    """Import scraper CSV using the pipeline's own upload_to_azure_sql logic."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    # Temporarily disable CSV_ONLY to allow real upload
    old_val = os.environ.pop("CSV_ONLY", None)
    try:
        if pipeline == "nfj":
            from nfj_scraper.nfj_data_scraper import upload_to_azure_sql
        elif pipeline == "justjoin":
            from just_join_scraper.just_join_scraper import upload_to_azure_sql
        elif pipeline == "pracuj":
            from pracuj_scraper.pracuj_premium_scraper import upload_to_azure_sql
        else:
            print(f"    [!] Unknown scraper pipeline: {pipeline}")
            return 0

        if pipeline == "justjoin":
            # JustJoin expects list[dict], not DataFrame
            result = upload_to_azure_sql(df.to_dict("records"))
        else:
            result = upload_to_azure_sql(df)

        return result.get("uploaded", 0)
    finally:
        if old_val is not None:
            os.environ["CSV_ONLY"] = old_val


def _import_cee_fx_csv(conn, csv_path: Path, table: str) -> int:
    """Import CEE FX CSV using pipeline's upload functions."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    old_val = os.environ.pop("CSV_ONLY", None)
    try:
        if table == "cee_fx_rates":
            from cee_fx_volatility.db.operations import upload_fx_rates
            result = upload_fx_rates(df.to_dict("records"))
        elif table == "cee_news_headlines":
            from cee_fx_volatility.db.operations import upload_news
            result = upload_news(df.to_dict("records"))
        else:
            print(f"    [!] Unknown CEE FX table: {table}")
            return 0
        return result.get("uploaded", 0)
    finally:
        if old_val is not None:
            os.environ["CSV_ONLY"] = old_val


def _import_gov_csv(conn, csv_path: Path, table: str) -> int:
    """Import Gov Spending CSV using pipeline's upload functions."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    old_val = os.environ.pop("CSV_ONLY", None)
    try:
        if table == "gov_notices":
            from gov_spending_radar.db.operations import upload_notices
            result = upload_notices(df.to_dict("records"))
        elif table == "gov_contractors":
            from gov_spending_radar.db.operations import upload_contractors
            result = upload_contractors(df.to_dict("records"))
        elif table == "gov_classifications":
            from gov_spending_radar.db.operations import upload_classifications
            result = upload_classifications(df.to_dict("records"))
        else:
            print(f"    [!] Unknown gov table: {table}")
            return 0
        return result.get("uploaded", 0)
    finally:
        if old_val is not None:
            os.environ["CSV_ONLY"] = old_val


def _import_shiller_csv(conn, csv_path: Path, table: str) -> int:
    """Import Shiller CSV — reconstruct final_data dict and use save_to_sql_database."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    old_val = os.environ.pop("CSV_ONLY", None)
    try:
        if table == "shiller_daily_scores":
            from shiller_index.shiller_logic import _execute_database_save
            # For each row in scores CSV, we need the matching articles CSV
            imported = 0
            for _, row in df.iterrows():
                date_val = row["date"]
                ticker = row["ticker"]

                # Find matching articles file
                articles_df = pd.DataFrame()
                parent = csv_path.parent
                for art_file in parent.glob("shiller_articles_*.csv"):
                    art_df = pd.read_csv(art_file, encoding="utf-8-sig")
                    match = art_df[(art_df["date"] == date_val) & (art_df["ticker"] == ticker)]
                    if not match.empty:
                        articles_df = match
                        break

                # Reconstruct final_data dict
                final_data = {
                    "metadata": {
                        "analysis_date": date_val,
                        "ticker": ticker,
                        "price": row["price"],
                        "ma_30": row["ma_30"],
                        "gap_pct": row["gap_pct"],
                        "articles_received": row.get("articles_received", 0),
                    },
                    "aggregated_scores": {
                        "final_sentiment": row["final_sentiment"],
                        "final_hype": row["final_hype"],
                        "sentiment_confidence": row["sentiment_confidence"],
                        "hype_confidence": row["hype_confidence"],
                        "articles_used_sentiment": row.get("articles_used_sentiment", 0),
                        "articles_used_hype": row.get("articles_used_hype", 0),
                    },
                    "articles": articles_df.to_dict("records") if not articles_df.empty else [],
                }

                conn_str = os.environ.get("SqlConnectionString")
                if _execute_database_save(final_data, conn_str):
                    imported += 1
            return imported
        elif table == "shiller_articles":
            # Articles are imported together with scores — skip standalone
            return 0
        else:
            return 0
    finally:
        if old_val is not None:
            os.environ["CSV_ONLY"] = old_val


def _import_energy_csv(conn, csv_path: Path, table: str) -> int:
    """Import Energy Prophet CSV — use PSEConnector upsert methods."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    # Energy uses direct cursor.execute with MERGE SQL — reuse connector
    old_val = os.environ.pop("CSV_ONLY", None)
    try:
        from energy_prophet.pse_connector import PSEConnector
        connector = PSEConnector()
        conn_str = os.environ.get("SqlConnectionString")
        if not conn_str:
            print("    [!] SqlConnectionString not set")
            return 0

        with connector._connect_with_retry(conn_str) as sql_conn:
            cursor = sql_conn.cursor()

            upsert_map = {
                "energy_prices": connector._upsert_prices,
                "generation_mix": lambda c, d: connector._upsert_generation_mix(c, actuals=d, load_fcst=None, oze_fcst=None),
                "power_balance": lambda c, d: connector._upsert_power_balance(c, reserves=d, daily_plan=None),
                "cross_border_flows": connector._upsert_flows,
                "pse_alerts": connector._upsert_alerts,
                "oze_curtailment": connector._upsert_curtailment,
                "co2_prices": connector._upsert_co2,
                "weather_data": None,  # separate handler
            }

            if table == "weather_data":
                from energy_prophet.weather_connector import WeatherConnector
                wc = WeatherConnector.__new__(WeatherConnector)
                wc.sql_conn_str = conn_str
                wc._save_to_sql(df)
                return len(df)

            if table in upsert_map and upsert_map[table]:
                upsert_map[table](cursor, df)
                sql_conn.commit()
                return len(df)

            # Settlement tables
            if table in ("balancing_settlement", "planned_outages"):
                # These need endpoint name — use generic approach
                if table == "planned_outages":
                    connector._upsert_outages(cursor, df)
                else:
                    connector._upsert_settlement(cursor, df, "csv-import")
                sql_conn.commit()
                return len(df)

        return 0
    finally:
        if old_val is not None:
            os.environ["CSV_ONLY"] = old_val


# ── Pipeline router ─────────────────────────────────────────────

PIPELINE_HANDLERS = {
    "nfj": lambda conn, path, table: _import_scraper_csv(conn, path, "nfj"),
    "justjoin": lambda conn, path, table: _import_scraper_csv(conn, path, "justjoin"),
    "pracuj": lambda conn, path, table: _import_scraper_csv(conn, path, "pracuj"),
    "cee_fx": _import_cee_fx_csv,
    "gov_spending": _import_gov_csv,
    "shiller": _import_shiller_csv,
    "energy": _import_energy_csv,
}


def scan_staging(pipeline_filter: str | None = None) -> list[tuple[str, str, Path]]:
    """Scan csv_staging/ for files to import. Returns [(pipeline, table, path), ...]."""
    files = []
    if not STAGING_DIR.exists():
        return files

    for pipeline_dir in sorted(STAGING_DIR.iterdir()):
        if not pipeline_dir.is_dir() or pipeline_dir.name in ("done", ".gitignore"):
            continue
        if pipeline_filter and pipeline_dir.name != pipeline_filter:
            continue
        for csv_file in sorted(pipeline_dir.glob("*.csv")):
            # Extract table name from filename: {table}_{timestamp}.csv
            name_parts = csv_file.stem.rsplit("_", 2)  # table_YYYY-MM-DD_HHMMSS
            if len(name_parts) >= 3:
                table = "_".join(name_parts[:-2])
            else:
                table = csv_file.stem
            files.append((pipeline_dir.name, table, csv_file))

    return files


def import_all(pipeline_filter: str | None = None, dry_run: bool = False) -> dict:
    """Import all staged CSVs to Azure SQL."""
    _load_env()

    files = scan_staging(pipeline_filter)
    if not files:
        print("\n[CSV-TO-DB] No staged files found.")
        return {"imported": 0, "failed": 0, "files": 0}

    print(f"\n{'='*60}")
    print(f"  CSV-TO-DB Bulk Import")
    print(f"  Files: {len(files)}")
    print(f"  Mode: {'DRY-RUN' if dry_run else 'IMPORT'}")
    print(f"{'='*60}\n")

    if dry_run:
        for pipeline, table, path in files:
            df = pd.read_csv(path, encoding="utf-8-sig")
            print(f"  [{pipeline}] {table}: {len(df)} rows <- {path.name}")
        return {"imported": 0, "failed": 0, "files": len(files)}

    conn = _connect()
    total_imported = 0
    total_failed = 0

    DONE_DIR.mkdir(parents=True, exist_ok=True)

    for pipeline, table, path in files:
        print(f"\n  [{pipeline}] {table} <- {path.name}")
        handler = PIPELINE_HANDLERS.get(pipeline)
        if not handler:
            print(f"    [!] No handler for pipeline '{pipeline}' — skipping")
            total_failed += 1
            continue

        try:
            imported = handler(conn, path, table)
            total_imported += imported

            # Move to done/
            done_pipeline_dir = DONE_DIR / pipeline
            done_pipeline_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(done_pipeline_dir / path.name))
            print(f"    -> Moved to done/")

        except Exception as e:
            print(f"    [!] FAILED: {e}")
            total_failed += 1

    conn.close()

    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"  Total rows imported: {total_imported}")
    print(f"  Failed files: {total_failed}")
    print(f"{'='*60}\n")

    return {"imported": total_imported, "failed": total_failed, "files": len(files)}


def main():
    parser = argparse.ArgumentParser(
        description="CSV-to-DB Bulk Import — import staged CSVs to Azure SQL",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be imported")
    parser.add_argument("--pipeline", type=str, default=None,
                        help="Import only this pipeline (nfj, justjoin, pracuj, cee_fx, gov_spending, shiller, energy)")
    args = parser.parse_args()

    result = import_all(pipeline_filter=args.pipeline, dry_run=args.dry_run)
    sys.exit(0 if result["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add csv_to_db.py
git commit -m "feat: add csv_to_db.py bulk import script for CSV-Only mode"
```

---

### Task 8: Add `CSV_ONLY=1` to `.env` and test

- [ ] **Step 1: Add CSV_ONLY to `.env`**

Append to `.env`:
```
# CSV-Only mode — skip Azure SQL uploads, save to csv_staging/ instead
# Remove this line when Azure subscription is restored
CSV_ONLY=1
```

- [ ] **Step 2: Smoke test — run NFJ in sample mode to verify CSV staging works**

```bash
.venv\Scripts\python.exe -X utf8 -c "
import os
os.environ['CSV_ONLY'] = '1'
from csv_staging_utils import is_csv_only, save_to_staging
import pandas as pd
print(f'CSV_ONLY active: {is_csv_only()}')
df = pd.DataFrame([{'test': 'value', 'num': 42}])
path = save_to_staging(df, 'test', 'smoke_test')
print(f'Saved to: {path}')
# Cleanup
import shutil
shutil.rmtree('csv_staging/test')
print('Cleanup OK')
"
```

Expected output:
```
CSV_ONLY active: True
  [CSV-ONLY] Staged 1 rows -> csv_staging\test\smoke_test_2026-04-19_...csv
Saved to: csv_staging\test\smoke_test_2026-04-19_...csv
Cleanup OK
```

- [ ] **Step 3: Update `docs/STATUS.md`**

Add note about CSV-Only mode being active.

- [ ] **Step 4: Final commit**

```bash
git add docs/STATUS.md
git commit -m "docs: note CSV-Only mode active while Azure subscription is paused"
```

---

### Task 9: Update CLAUDE.md with CSV-Only mode docs

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add CSV-Only section to CLAUDE.md**

Add after the Quick Run section:

```markdown
## CSV-Only Mode

When Azure SQL is unavailable, set `CSV_ONLY=1` in `.env`. All pipelines save to `csv_staging/` instead of DB. DB-read features (--classify, --reclassify, --cleanup) are auto-skipped.

To restore:
1. Remove `CSV_ONLY=1` from `.env`
2. Run `python -X utf8 csv_to_db.py` to import staged data
3. Optionally `python -X utf8 csv_to_db.py --dry-run` to preview first
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add CSV-Only mode instructions to CLAUDE.md"
```
