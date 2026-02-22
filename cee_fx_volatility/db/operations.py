"""
Azure SQL operations — upload FX rates and news headlines.
==========================================================
Follows Portfolio Data Factory patterns:
  - _load_env() for .env config
  - 3-retry linear backoff for connection
  - Row-by-row MERGE (upsert)
  - Returns {"uploaded": int, "errors": list[str]}
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


# ── Connection helper ──────────────────────────────────────────────

def _get_connection(max_retries: int = 3) -> pyodbc.Connection | None:
    """Connect to Azure SQL with linear backoff retry."""
    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        print("  [SQL] Brak SqlConnectionString w .env")
        return None

    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=60)
            print(f"  [SQL] Połączono (próba {attempt}/{max_retries})")
            return conn
        except pyodbc.Error as e:
            if attempt < max_retries:
                wait = attempt * 15
                print(f"  [SQL] Baza niedostępna (próba {attempt}/{max_retries}), czekam {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [SQL] Błąd połączenia po {max_retries} próbach: {e}")
    return None


# ── FX upload ──────────────────────────────────────────────────────

def upload_fx_rates(records: list[dict]) -> dict:
    """
    Upload FX rate records to Azure SQL (table cee_fx_rates).
    Uses MERGE (upsert) on (timestamp, currency_pair) — safe for repeated runs.

    Args:
        records: list of dicts with keys matching FX_SQL_COLUMNS

    Returns:
        {"uploaded": int, "errors": list[str]}
    """
    result = {"uploaded": 0, "errors": []}

    if not records:
        print("  [SQL] Brak rekordów FX do uploadu")
        return result

    print(f"\n[SQL] Upload {len(records)} rekordów FX...")
    conn = _get_connection()
    if not conn:
        result["errors"].append("Nie udało się połączyć z Azure SQL")
        return result

    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_FX_TABLE_SQL)
            conn.commit()
            print("  [SQL] Tabela cee_fx_rates — OK")

            uploaded = 0
            for rec in records:
                vals = [rec.get(col) for col in FX_SQL_COLUMNS]
                try:
                    cursor.execute(MERGE_FX_SQL, *vals)
                    uploaded += 1
                except Exception as e:
                    err = f"FX {rec.get('timestamp')} {rec.get('currency_pair')}: {e}"
                    print(f"  [SQL] BŁĄD: {err}")
                    result["errors"].append(err)

            conn.commit()
            result["uploaded"] = uploaded
            print(f"  [SQL] FX upload: {uploaded}/{len(records)} rekordów")
    except pyodbc.Error as e:
        msg = f"Błąd SQL (FX): {e}"
        print(f"  [SQL] {msg}")
        result["errors"].append(msg)

    return result


# ── News upload ────────────────────────────────────────────────────

def upload_news(records: list[dict]) -> dict:
    """
    Upload news headline records to Azure SQL (table cee_news_headlines).
    Uses MERGE (upsert) on url — safe for repeated runs.

    Args:
        records: list of dicts with keys matching NEWS_SQL_COLUMNS

    Returns:
        {"uploaded": int, "errors": list[str]}
    """
    result = {"uploaded": 0, "errors": []}

    if not records:
        print("  [SQL] Brak rekordów newsów do uploadu")
        return result

    print(f"\n[SQL] Upload {len(records)} newsów...")
    conn = _get_connection()
    if not conn:
        result["errors"].append("Nie udało się połączyć z Azure SQL")
        return result

    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_NEWS_TABLE_SQL)
            conn.commit()
            print("  [SQL] Tabela cee_news_headlines — OK")

            uploaded = 0
            for rec in records:
                vals = [rec.get(col) for col in NEWS_SQL_COLUMNS]
                try:
                    cursor.execute(MERGE_NEWS_SQL, *vals)
                    uploaded += 1
                except Exception as e:
                    err = f"News {rec.get('url', '?')}: {e}"
                    print(f"  [SQL] BŁĄD: {err}")
                    result["errors"].append(err)

            conn.commit()
            result["uploaded"] = uploaded
            print(f"  [SQL] News upload: {uploaded}/{len(records)} rekordów")
    except pyodbc.Error as e:
        msg = f"Błąd SQL (news): {e}"
        print(f"  [SQL] {msg}")
        result["errors"].append(msg)

    return result
