"""
Azure SQL operations — Inflation Basket pipeline.
==================================================
Two-layer retry (same pattern as gov_spending_radar/db/operations.py):
  Layer 1: _connect_with_retry() — 5 attempts, 10s linear backoff (cold start)
  Layer 2: batch upserts         — 3 batch attempts, 15s backoff, fresh conn each
  Total worst-case: ~3 minutes of retries.
"""

import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import pyodbc

from .schema import (
    CREATE_TABLE_SQLS,
    INSERT_SHRINKFLATION_SQL,
    MERGE_OBSERVATION_SQL,
    MERGE_PRODUCT_SQL,
    MERGE_PRODUCT_URL_SQL,
)

# ── .env loading (same as other pipelines) ────────────────────────────

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent.parent  # portfolio-data-factory root
ENV_FILE = PROJECT_DIR / ".env"


def _load_env() -> None:
    """Load variables from project root .env (idempotent, no override)."""
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


_load_env()

# ── CSV-Only mode guard (consistent with other pipelines) ─────────────
CSV_ONLY = os.environ.get("CSV_ONLY", "").strip() == "1"

# ── Connection (Layer 1) ──────────────────────────────────────────────


def _connect_with_retry(max_retries: int = 5) -> pyodbc.Connection:
    """Connect to Azure SQL with linear backoff for serverless cold starts."""
    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        raise RuntimeError("Brak SqlConnectionString w zmiennych środowiskowych")
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            return pyodbc.connect(conn_str, timeout=30)
        except pyodbc.Error as e:
            last_err = e
            if attempt < max_retries:
                wait = 10 * attempt
                print(f"[inflation_basket] connect attempt {attempt} failed: {str(e)[:150]}. Retry in {wait}s...")
                time.sleep(wait)
    raise RuntimeError(f"Failed to connect after {max_retries} attempts: {last_err}")


# ── Schema management ─────────────────────────────────────────────────


def ensure_tables() -> None:
    """Idempotent CREATE TABLE for all 4 tables. Safe to run repeatedly."""
    if CSV_ONLY:
        print("[inflation_basket] CSV_ONLY=1 — skipping ensure_tables")
        return
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        for sql in CREATE_TABLE_SQLS:
            cur.execute(sql)
        conn.commit()
    print(f"[inflation_basket] {len(CREATE_TABLE_SQLS)} tables ensured")


# ── Master catalog seeding ────────────────────────────────────────────


def seed_products(products: Iterable[Any]) -> int:
    """Upsert master catalog from seed/products.py.

    Natural key: (name_canonical, brand, capacity_value, capacity_unit).
    Returns number of products processed.
    """
    if CSV_ONLY:
        print("[inflation_basket] CSV_ONLY=1 — skipping seed_products")
        return 0

    products_list = list(products)
    count = 0
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        for p in products_list:
            alt = list(p.alternative_names) if p.alternative_names else []
            alt_json = json.dumps(alt, ensure_ascii=False) if alt else None
            cur.execute(
                MERGE_PRODUCT_SQL,
                (
                    p.ean,
                    p.name_canonical,
                    p.brand,
                    p.category_user,
                    p.matching_type,
                    float(p.capacity_value),
                    p.capacity_unit,
                    1 if p.is_imported else 0,
                    p.origin_country,
                    alt_json,
                ),
            )
            count += 1
        conn.commit()
    print(f"[inflation_basket] seeded {count} products")
    return count


# ── URL mapping ───────────────────────────────────────────────────────


def upsert_product_url(
    product_id: int,
    store: str,
    url: str,
    sku_store: Optional[str] = None,
    active: bool = True,
) -> None:
    if CSV_ONLY:
        return
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        cur.execute(
            MERGE_PRODUCT_URL_SQL,
            (product_id, store, url, sku_store, 1 if active else 0),
        )
        conn.commit()


# ── Observations (price snapshots) — Layer 2 batch retry ──────────────


def upsert_observations_batch(rows: list[dict]) -> int:
    """Batch upsert with 3-attempt retry, 15s linear backoff, fresh conn each."""
    if not rows:
        return 0
    if CSV_ONLY:
        print(f"[inflation_basket] CSV_ONLY=1 — would upsert {len(rows)} observations")
        return 0

    last_err: Optional[Exception] = None
    for batch_attempt in range(1, 4):
        try:
            params = [
                (
                    r["product_id"],
                    r["store"],
                    r["obs_date"],
                    r["obs_ts"],
                    r["price_regular"],
                    r.get("price_promo"),
                    1 if r.get("promo_active") else 0,
                    r.get("unit_price"),
                    r.get("capacity_seen"),
                    r.get("currency", "PLN"),
                )
                for r in rows
            ]
            with _connect_with_retry() as conn:
                cur = conn.cursor()
                cur.executemany(MERGE_OBSERVATION_SQL, params)
                conn.commit()
            print(f"[inflation_basket] upserted {len(rows)} observations")
            return len(rows)
        except pyodbc.Error as e:
            last_err = e
            if batch_attempt < 3:
                wait = 15 * batch_attempt
                print(f"[inflation_basket] batch attempt {batch_attempt} failed: {str(e)[:150]}. Retry in {wait}s...")
                time.sleep(wait)
    raise RuntimeError(f"Failed to upsert observations after 3 batch attempts: {last_err}")


# ── Read APIs (for scraper) ───────────────────────────────────────────


def get_active_products(store: Optional[str] = None) -> list[dict]:
    """Return products with active URLs in given store (or all stores).

    Used by scraper to know which URLs to hit on each run.
    """
    if CSV_ONLY:
        return []

    sql = """
    SELECT p.product_id, p.name_canonical, p.brand, p.matching_type,
           p.alternative_names, p.capacity_value, p.capacity_unit,
           p.is_imported, p.origin_country,
           u.store, u.url, u.sku_store
    FROM inflation_products p
    JOIN inflation_product_urls u ON u.product_id = p.product_id
    WHERE p.status = 'active' AND u.active = 1
    """
    params: tuple = ()
    if store:
        sql += " AND u.store = ?"
        params = (store,)
    sql += " ORDER BY p.product_id, u.store"

    with _connect_with_retry() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Shrinkflation events (V1) ─────────────────────────────────────────


def record_shrinkflation_event(
    product_id: int,
    store: str,
    capacity_before: float,
    capacity_after: float,
    price_before: float,
    price_after: float,
    real_increase_pct: float,
    gemini_confidence: Optional[float] = None,
    notes: Optional[str] = None,
) -> None:
    if CSV_ONLY:
        return
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        cur.execute(
            INSERT_SHRINKFLATION_SQL,
            (
                product_id,
                store,
                capacity_before,
                capacity_after,
                price_before,
                price_after,
                real_increase_pct,
                gemini_confidence,
                notes,
            ),
        )
        conn.commit()


# ── CLI smoke test ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    print("[inflation_basket.db.operations] smoke test")
    print(f"  CSV_ONLY={CSV_ONLY}")
    print(f"  ENV_FILE={ENV_FILE} exists={ENV_FILE.exists()}")
    print(f"  SqlConnectionString set: {bool(os.environ.get('SqlConnectionString'))}")

    if "--seed" in sys.argv:
        # Real seed against Azure SQL
        ensure_tables()
        from inflation_basket.seed.products import PRODUCTS
        n = seed_products(PRODUCTS)
        print(f"OK: {n} products seeded into master catalog")
    else:
        print("Pass --seed to run ensure_tables + seed_products against Azure SQL.")
