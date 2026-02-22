"""
Azure SQL operations — upload FX rates and news headlines.
==========================================================
Two-layer retry (modeled after energy_prophet/pse_connector.py):
  Layer 1: _connect_with_retry() — 5 attempts, 10s/20s/30s/40s/50s backoff
  Layer 2: upload functions      — 3 batch attempts, 15s/30s/45s backoff
  Each batch attempt gets a fresh connection with its own 5 retries.
  Total worst-case: ~4 minutes of retries before giving up.
"""

import json
import os
import time
from pathlib import Path

import pyodbc

from .schema import (
    CREATE_FX_TABLE_SQL,
    CREATE_NEWS_TABLE_SQL,
    MERGE_FX_SQL,
    MERGE_NEWS_SQL,
    FX_SQL_COLUMNS,
    NEWS_SQL_COLUMNS,
)

# ── .env loading (same pattern as other scrapers) ──────────────────

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent.parent  # Portfolio-Data-Factory root
ENV_FILE = PROJECT_DIR / ".env"


def _load_env() -> None:
    """Load variables from project root .env and local.settings.json."""
    # 1. .env file (same as other scrapers)
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

    # 2. local.settings.json (same as Shiller — Azure Functions config)
    settings_file = PROJECT_DIR / "local.settings.json"
    if settings_file.exists():
        try:
            with open(settings_file, encoding="utf-8") as f:
                settings = json.load(f)
            for key, value in settings.get("Values", {}).items():
                if key not in os.environ:
                    os.environ[key] = str(value)
        except (json.JSONDecodeError, OSError):
            pass


_load_env()


# ── Layer 1: Connection with retry (PSE pattern) ──────────────────

def _connect_with_retry(max_retries: int = 5) -> pyodbc.Connection:
    """
    Connect to Azure SQL with retry logic for serverless cold starts.
    5 attempts with linear backoff: 10s, 20s, 30s, 40s, 50s.
    Raises on final failure.
    """
    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        raise RuntimeError("Brak SqlConnectionString w zmiennych środowiskowych")

    # Enforce connection timeout
    if "Connection Timeout" not in conn_str:
        conn_str += ";Connection Timeout=30"

    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str)
            if attempt > 0:
                print(f"  [SQL] Połączono (po {attempt + 1} próbach — baza się obudziła)")
            else:
                print(f"  [SQL] Połączono")
            return conn
        except Exception as e:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 10
                print(f"  [SQL] Baza niedostępna (próba {attempt + 1}/{max_retries}), "
                      f"czekam {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


# ── Layer 2: Batch upload with retry ──────────────────────────────

def upload_fx_rates(records: list[dict]) -> dict:
    """
    Upload FX rate records to Azure SQL (table cee_fx_rates).
    Uses MERGE (upsert) on (timestamp, currency_pair) — safe for repeated runs.

    Two-layer retry:
      - Each batch attempt gets a fresh connection (up to 5 connection retries)
      - Up to 3 batch attempts with 15s/30s/45s backoff

    Returns:
        {"uploaded": int, "errors": list[str]}
    """
    result = {"uploaded": 0, "errors": []}

    if not records:
        print("  [SQL] Brak rekordów FX do uploadu")
        return result

    print(f"\n[SQL] Upload {len(records)} rekordów FX...")

    max_batch_retries = 3
    for batch_attempt in range(max_batch_retries):
        try:
            with _connect_with_retry() as conn:
                cursor = conn.cursor()
                cursor.execute(CREATE_FX_TABLE_SQL)
                conn.commit()
                print("  [SQL] Tabela cee_fx_rates — OK")

                uploaded = 0
                row_errors = []
                for rec in records:
                    vals = [rec.get(col) for col in FX_SQL_COLUMNS]
                    try:
                        cursor.execute(MERGE_FX_SQL, *vals)
                        uploaded += 1
                    except Exception as e:
                        err = f"FX {rec.get('timestamp')} {rec.get('currency_pair')}: {e}"
                        print(f"  [SQL] BŁĄD: {err}")
                        row_errors.append(err)

                conn.commit()
                result["uploaded"] = uploaded
                result["errors"] = row_errors
                print(f"  [SQL] FX upload: {uploaded}/{len(records)} rekordów")
                return result  # success — exit retry loop

        except Exception as e:
            if batch_attempt < max_batch_retries - 1:
                wait = (batch_attempt + 1) * 15
                print(f"  [SQL] Batch FX nieudany (próba {batch_attempt + 1}/{max_batch_retries}), "
                      f"czekam {wait}s... [{e}]")
                time.sleep(wait)
            else:
                msg = f"FX batch failed po {max_batch_retries} próbach: {e}"
                print(f"  [SQL] {msg}")
                result["errors"].append(msg)

    return result


def upload_news(records: list[dict]) -> dict:
    """
    Upload news headline records to Azure SQL (table cee_news_headlines).
    Uses MERGE (upsert) on url — safe for repeated runs.

    Two-layer retry (same as upload_fx_rates).

    Returns:
        {"uploaded": int, "errors": list[str]}
    """
    result = {"uploaded": 0, "errors": []}

    if not records:
        print("  [SQL] Brak rekordów newsów do uploadu")
        return result

    print(f"\n[SQL] Upload {len(records)} newsów...")

    max_batch_retries = 3
    for batch_attempt in range(max_batch_retries):
        try:
            with _connect_with_retry() as conn:
                cursor = conn.cursor()
                cursor.execute(CREATE_NEWS_TABLE_SQL)
                conn.commit()
                print("  [SQL] Tabela cee_news_headlines — OK")

                uploaded = 0
                row_errors = []
                for rec in records:
                    vals = [rec.get(col) for col in NEWS_SQL_COLUMNS]
                    try:
                        cursor.execute(MERGE_NEWS_SQL, *vals)
                        uploaded += 1
                    except Exception as e:
                        err = f"News {rec.get('url', '?')}: {e}"
                        print(f"  [SQL] BŁĄD: {err}")
                        row_errors.append(err)

                conn.commit()
                result["uploaded"] = uploaded
                result["errors"] = row_errors
                print(f"  [SQL] News upload: {uploaded}/{len(records)} rekordów")
                return result  # success — exit retry loop

        except Exception as e:
            if batch_attempt < max_batch_retries - 1:
                wait = (batch_attempt + 1) * 15
                print(f"  [SQL] Batch news nieudany (próba {batch_attempt + 1}/{max_batch_retries}), "
                      f"czekam {wait}s... [{e}]")
                time.sleep(wait)
            else:
                msg = f"News batch failed po {max_batch_retries} próbach: {e}"
                print(f"  [SQL] {msg}")
                result["errors"].append(msg)

    return result
