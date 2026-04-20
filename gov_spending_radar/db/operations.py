"""
Azure SQL operations — upload procurement notices and contractors.
=================================================================
Two-layer retry (same pattern as cee_fx_volatility/db/operations.py):
  Layer 1: _connect_with_retry() — 5 attempts, 10s linear backoff
  Layer 2: upload functions      — 3 batch attempts, 15s backoff
  Each batch attempt gets a fresh connection with its own 5 retries.
"""

import json
import os
import time
from pathlib import Path

import pyodbc

from .schema import (
    CREATE_NOTICES_TABLE_SQL,
    CREATE_CONTRACTORS_TABLE_SQL,
    CREATE_CLASSIFICATIONS_TABLE_SQL,
    MIGRATE_NOTICES_SQL,
    MIGRATE_NOTICES_HTML_FIELDS_SQL,
    MIGRATE_CONTRACTORS_SQL,
    MIGRATE_CLASSIFICATIONS_V2_SQL,
    MERGE_NOTICES_SQL,
    MERGE_CONTRACTORS_SQL,
    MERGE_CLASSIFICATIONS_SQL,
    NOTICES_SQL_COLUMNS,
    CONTRACTORS_SQL_COLUMNS,
    CLASSIFICATIONS_SQL_COLUMNS,
)
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from csv_staging_utils import is_csv_only, save_to_staging

# ── .env loading (same pattern as other pipelines) ──────────────

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent.parent  # Portfolio-Data-Factory root
ENV_FILE = PROJECT_DIR / ".env"


def _load_env() -> None:
    """Load variables from project root .env and local.settings.json."""
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

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


# ── Layer 1: Connection with retry ──────────────────────────────

def _connect_with_retry(max_retries: int = 5) -> pyodbc.Connection:
    """
    Connect to Azure SQL with retry logic for serverless cold starts.
    5 attempts with linear backoff: 10s, 20s, 30s, 40s, 50s.
    """
    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        raise RuntimeError("Brak SqlConnectionString w zmiennych środowiskowych")

    if "Connection Timeout" not in conn_str:
        conn_str += ";Connection Timeout=30"

    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(conn_str)
            if attempt > 0:
                print(f"  [SQL] Połączono (po {attempt + 1} próbach)")
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


# ── Layer 2: Batch upload with retry ────────────────────────────

def upload_notices(records: list[dict]) -> dict:
    """
    Upload notice records to Azure SQL (table gov_notices).
    MERGE on object_id — safe for repeated runs.

    Returns:
        {"uploaded": int, "errors": list[str]}
    """
    if is_csv_only():
        save_to_staging(records, "gov_spending", "gov_notices")
        return {"uploaded": 0, "errors": []}
    result = {"uploaded": 0, "errors": []}

    if not records:
        print("  [SQL] Brak ogłoszeń do uploadu")
        return result

    print(f"\n[SQL] Upload {len(records)} ogłoszeń...")

    max_batch_retries = 3
    for batch_attempt in range(max_batch_retries):
        try:
            with _connect_with_retry() as conn:
                cursor = conn.cursor()
                cursor.execute(CREATE_NOTICES_TABLE_SQL)
                cursor.execute(MIGRATE_NOTICES_SQL)
                cursor.execute(MIGRATE_NOTICES_HTML_FIELDS_SQL)
                conn.commit()
                print("  [SQL] Tabela gov_notices — OK (HTML fields migration applied)")

                uploaded = 0
                row_errors = []
                for rec in records:
                    vals = [rec.get(col) for col in NOTICES_SQL_COLUMNS]
                    try:
                        cursor.execute(MERGE_NOTICES_SQL, *vals)
                        uploaded += 1
                    except Exception as e:
                        err = f"Notice {rec.get('object_id', '?')}: {e}"
                        print(f"  [SQL] BŁĄD: {err}")
                        row_errors.append(err)

                conn.commit()
                result["uploaded"] = uploaded
                result["errors"] = row_errors
                print(f"  [SQL] Upload ogłoszeń: {uploaded}/{len(records)}")
                return result

        except Exception as e:
            if batch_attempt < max_batch_retries - 1:
                wait = (batch_attempt + 1) * 15
                print(f"  [SQL] Batch notices nieudany (próba {batch_attempt + 1}/{max_batch_retries}), "
                      f"czekam {wait}s... [{e}]")
                time.sleep(wait)
            else:
                msg = f"Notices batch failed po {max_batch_retries} próbach: {e}"
                print(f"  [SQL] {msg}")
                result["errors"].append(msg)

    return result


def upload_contractors(records: list[dict]) -> dict:
    """
    Upload contractor records to Azure SQL (table gov_contractors).
    MERGE on (notice_object_id, part_index) — safe for repeated runs.

    Returns:
        {"uploaded": int, "errors": list[str]}
    """
    if is_csv_only():
        save_to_staging(records, "gov_spending", "gov_contractors")
        return {"uploaded": 0, "errors": []}
    result = {"uploaded": 0, "errors": []}

    if not records:
        print("  [SQL] Brak wykonawców do uploadu")
        return result

    print(f"\n[SQL] Upload {len(records)} wykonawców...")

    max_batch_retries = 3
    for batch_attempt in range(max_batch_retries):
        try:
            with _connect_with_retry() as conn:
                cursor = conn.cursor()
                cursor.execute(CREATE_CONTRACTORS_TABLE_SQL)
                cursor.execute(MIGRATE_CONTRACTORS_SQL)
                conn.commit()
                print("  [SQL] Tabela gov_contractors — OK")

                uploaded = 0
                row_errors = []
                for rec in records:
                    vals = [rec.get(col) for col in CONTRACTORS_SQL_COLUMNS]
                    try:
                        cursor.execute(MERGE_CONTRACTORS_SQL, *vals)
                        uploaded += 1
                    except Exception as e:
                        err = f"Contractor {rec.get('notice_object_id', '?')}[{rec.get('part_index')}]: {e}"
                        print(f"  [SQL] BŁĄD: {err}")
                        row_errors.append(err)

                conn.commit()
                result["uploaded"] = uploaded
                result["errors"] = row_errors
                print(f"  [SQL] Upload wykonawców: {uploaded}/{len(records)}")
                return result

        except Exception as e:
            if batch_attempt < max_batch_retries - 1:
                wait = (batch_attempt + 1) * 15
                print(f"  [SQL] Batch contractors nieudany (próba {batch_attempt + 1}/{max_batch_retries}), "
                      f"czekam {wait}s... [{e}]")
                time.sleep(wait)
            else:
                msg = f"Contractors batch failed po {max_batch_retries} próbach: {e}"
                print(f"  [SQL] {msg}")
                result["errors"].append(msg)

    return result


def upload_classifications(records: list[dict]) -> dict:
    """
    Upload classification records to Azure SQL (table gov_classifications).
    MERGE on (notice_object_id, method) — safe for repeated runs.

    Returns:
        {"uploaded": int, "errors": list[str]}
    """
    if is_csv_only():
        save_to_staging(records, "gov_spending", "gov_classifications")
        return {"uploaded": 0, "errors": []}
    result = {"uploaded": 0, "errors": []}

    if not records:
        print("  [SQL] Brak klasyfikacji do uploadu")
        return result

    print(f"\n[SQL] Upload {len(records)} klasyfikacji...")

    max_batch_retries = 3
    for batch_attempt in range(max_batch_retries):
        try:
            with _connect_with_retry() as conn:
                cursor = conn.cursor()
                cursor.execute(CREATE_CLASSIFICATIONS_TABLE_SQL)
                cursor.execute(MIGRATE_CLASSIFICATIONS_V2_SQL)
                conn.commit()
                print("  [SQL] Tabela gov_classifications — OK (v2 migration applied)")

                uploaded = 0
                row_errors = []
                for rec in records:
                    vals = [rec.get(col) for col in CLASSIFICATIONS_SQL_COLUMNS]
                    try:
                        cursor.execute(MERGE_CLASSIFICATIONS_SQL, *vals)
                        uploaded += 1
                    except Exception as e:
                        err = f"Classification {rec.get('notice_object_id', '?')}/{rec.get('method')}: {e}"
                        print(f"  [SQL] BŁĄD: {err}")
                        row_errors.append(err)

                conn.commit()
                result["uploaded"] = uploaded
                result["errors"] = row_errors
                print(f"  [SQL] Upload klasyfikacji: {uploaded}/{len(records)}")
                return result

        except Exception as e:
            if batch_attempt < max_batch_retries - 1:
                wait = (batch_attempt + 1) * 15
                print(f"  [SQL] Batch classifications nieudany (próba {batch_attempt + 1}/{max_batch_retries}), "
                      f"czekam {wait}s... [{e}]")
                time.sleep(wait)
            else:
                msg = f"Classifications batch failed po {max_batch_retries} próbach: {e}"
                print(f"  [SQL] {msg}")
                result["errors"].append(msg)

    return result


def fetch_unclassified_notices() -> list[dict]:
    """
    Fetch notices that have no classification yet (for --classify mode, CPV+keyword pass).
    Returns list of dicts with object_id, title, cpv_code, cpv_raw, buyer_name.
    """
    try:
        with _connect_with_retry() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT n.object_id, n.title, n.cpv_code, n.cpv_raw, n.buyer_name
                FROM gov_notices n
                WHERE NOT EXISTS (
                    SELECT 1 FROM gov_classifications c
                    WHERE c.notice_object_id = n.object_id
                )
            """)
            rows = cursor.fetchall()
            return [
                {
                    "object_id": r[0], "title": r[1], "cpv_code": r[2],
                    "cpv_raw": r[3], "buyer_name": r[4],
                }
                for r in rows
            ]
    except Exception as e:
        print(f"  [SQL] Błąd pobierania niesklasyfikowanych ogłoszeń: {e}")
        return []


def delete_all_classifications() -> int:
    """
    Delete all rows from gov_classifications. Used by reclassify_all script.
    Returns number of deleted rows.
    """
    try:
        with _connect_with_retry() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM gov_classifications")
            deleted = cursor.rowcount
            conn.commit()
            print(f"  [SQL] Usunięto {deleted} klasyfikacji")
            return deleted
    except Exception as e:
        print(f"  [SQL] Błąd usuwania klasyfikacji: {e}")
        return 0


def fetch_all_notices_for_classification() -> list[dict]:
    """
    Fetch ALL notices from gov_notices for full reclassification.
    Returns list of dicts with object_id, title, cpv_code, cpv_raw, buyer_name.
    """
    try:
        with _connect_with_retry() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT object_id, title, cpv_code, cpv_raw, buyer_name
                FROM gov_notices
            """)
            rows = cursor.fetchall()
            print(f"  [SQL] Pobrano {len(rows)} ogłoszeń do klasyfikacji")
            return [
                {
                    "object_id": r[0], "title": r[1], "cpv_code": r[2],
                    "cpv_raw": r[3], "buyer_name": r[4],
                }
                for r in rows
            ]
    except Exception as e:
        print(f"  [SQL] Błąd pobierania ogłoszeń: {e}")
        return []


def run_schema_migration() -> None:
    """Run all schema migrations (safe to call multiple times)."""
    try:
        with _connect_with_retry() as conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_CLASSIFICATIONS_TABLE_SQL)
            cursor.execute(MIGRATE_CLASSIFICATIONS_V2_SQL)
            conn.commit()
            print("  [SQL] Schema migration v2 — OK")
    except Exception as e:
        print(f"  [SQL] Błąd migracji: {e}")
        raise


def fetch_unclassified_for_llm() -> list[dict]:
    """
    Fetch notices that have no LLM classification yet (may have cpv_keyword).
    For --classify LLM pass.
    Returns list of dicts with object_id, title, cpv_code, cpv_raw, buyer_name.
    """
    try:
        with _connect_with_retry() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT n.object_id, n.title, n.cpv_code, n.cpv_raw, n.buyer_name
                FROM gov_notices n
                WHERE NOT EXISTS (
                    SELECT 1 FROM gov_classifications c
                    WHERE c.notice_object_id = n.object_id
                      AND c.method = 'llm_gemini'
                )
            """)
            rows = cursor.fetchall()
            return [
                {
                    "object_id": r[0], "title": r[1], "cpv_code": r[2],
                    "cpv_raw": r[3], "buyer_name": r[4],
                }
                for r in rows
            ]
    except Exception as e:
        print(f"  [SQL] Błąd pobierania ogłoszeń do klasyfikacji LLM: {e}")
        return []
