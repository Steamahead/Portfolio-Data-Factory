"""
Interactive URL mapper — for inflation_basket pipeline.

Per produkt bez URL w danym sklepie:
  1. Otwiera sklep w Playwright headed (browser podgląda się).
  2. Ty szukasz produktu w przeglądarce, kopiujesz URL z paska adresu.
  3. Wklejasz URL w terminal + Enter → ja zapisuję do `inflation_product_urls`.

Commands w terminalu:
  <URL>     paste produkt URL, save, next
  s         skip ten produkt (nie zapisuj)
  o         re-open homepage sklepu (jeśli przypadkiem zamknąłeś)
  ?         show product name + alternative_names again
  q         quit (resume next time z tego samego miejsca)

Usage:
  python -X utf8 -m inflation_basket.url_mapper --store frisco
  python -X utf8 -m inflation_basket.url_mapper --store auchan_warsaw

Auchan: pierwszy run prosi o wybór sklepu Warszawa; sesja zapisana do
seed/playwright_state/auchan_warsaw.json i wczytywana w kolejnych runach.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from playwright.sync_api import sync_playwright

from inflation_basket.db.operations import (
    _connect_with_retry,
    upsert_product_url,
)

VALID_STORES = ("frisco", "auchan_warsaw")

HOMEPAGE = {
    "frisco": "https://www.frisco.pl/",
    "auchan_warsaw": "https://zakupy.auchan.pl/",
}

# Best-effort search URL — jeśli sklep ma stabilny search endpoint,
# otwieram go zamiast homepage. Jeśli nie pasuje — user otwiera search ręcznie.
SEARCH_URL = {
    "frisco": "https://www.frisco.pl/q,{q}",
    "auchan_warsaw": "https://zakupy.auchan.pl/szukaj?q={q}",
}

STATE_DIR = Path(__file__).parent / "seed" / "playwright_state"


def _state_file(store: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"{store}.json"


def _extract_sku(store: str, url: str) -> Optional[str]:
    """Best-effort SKU extraction from product URL."""
    if store == "frisco":
        # Frisco: /pn,SLUG,2,SKU or /pn,SLUG,SKU
        m = re.search(r",2,(\d+)", url) or re.search(r",(\d{4,})(?:[/?#]|$)", url)
        return m.group(1) if m else None
    if store == "auchan_warsaw":
        # Auchan zakupy: /shop/.../<slug>-<id> or /produkt/<id>
        m = re.search(r"-(\d{5,})(?:[/?#]|$)", url) or re.search(r"/(\d{5,})(?:[/?#]|$)", url)
        return m.group(1) if m else None
    return None


def get_unmapped_products(store: str) -> list[dict]:
    """Products without an active URL for this store."""
    sql = """
    SELECT p.product_id, p.name_canonical, p.brand, p.matching_type,
           p.capacity_value, p.capacity_unit, p.alternative_names,
           p.is_imported, p.origin_country, p.category_user
    FROM inflation_products p
    WHERE p.status = 'active'
      AND NOT EXISTS (
          SELECT 1 FROM inflation_product_urls u
          WHERE u.product_id = p.product_id AND u.store = ? AND u.active = 1
      )
    ORDER BY p.category_user, p.product_id
    """
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        cur.execute(sql, (store,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _format_product(p: dict) -> str:
    parts = [
        f"product_id={p['product_id']}",
        p["category_user"],
        f"{p['brand']} {p['name_canonical']}".strip() if p["brand"] else p["name_canonical"],
        f"{p['capacity_value']:g} {p['capacity_unit']}",
        f"[{p['matching_type']}]",
    ]
    if p.get("is_imported"):
        parts.append(f"IMPORTED ({p['origin_country'] or '?'})")
    alt_raw = p.get("alternative_names")
    if alt_raw:
        try:
            alt = json.loads(alt_raw)
            if alt:
                parts.append(f"alt={alt}")
        except Exception:
            pass
    return " | ".join(parts)


def _build_search(store: str, p: dict) -> str:
    name = p["name_canonical"]
    brand = p.get("brand") or ""
    q = f"{brand} {name}".strip()
    return SEARCH_URL[store].format(q=quote(q))


def map_store(store: str) -> None:
    if store not in VALID_STORES:
        print(f"ERROR: store musi być jednym z {VALID_STORES}", file=sys.stderr)
        sys.exit(2)

    products = get_unmapped_products(store)
    total = len(products)
    print(f"\n=== URL mapper — store: {store} ===")
    print(f"Unmapped: {total}")
    if total == 0:
        print("Wszystkie produkty mają już URL w tym sklepie. Nic do zrobienia.")
        return

    print("\nCommands: <url> | s (skip) | o (re-open) | ? (info) | q (quit)\n")

    state = _state_file(store)
    state_kwargs = {"storage_state": str(state)} if state.exists() else {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(**state_kwargs, locale="pl-PL")
        page = ctx.new_page()

        # First-time setup for Auchan: pick Warsaw store, save state
        if store == "auchan_warsaw" and not state.exists():
            print("Pierwsze uruchomienie Auchan — wybierz sklep WARSZAWA w przeglądarce.")
            page.goto(HOMEPAGE[store])
            input("Po wyborze sklepu wciśnij Enter w terminalu... ")
            ctx.storage_state(path=str(state))
            print(f"Zapisano sesję do {state.name}\n")

        saved = skipped = 0
        idx = 0
        while idx < len(products):
            p = products[idx]
            print(f"\n[{idx+1}/{total}] {_format_product(p)}")
            try:
                page.goto(_build_search(store, p), timeout=20000)
            except Exception as e:
                print(f"  (search URL miss — fallback to homepage: {e!s:.100})")
                page.goto(HOMEPAGE[store])

            cmd = input("URL / s / o / ? / q > ").strip()
            low = cmd.lower()

            if low in ("q", "quit", "exit"):
                break
            if low in ("s", "skip"):
                skipped += 1
                idx += 1
                continue
            if low in ("o", "open"):
                page.goto(HOMEPAGE[store])
                continue
            if low == "?":
                print(f"  {_format_product(p)}")
                continue
            if cmd.startswith("http"):
                sku = _extract_sku(store, cmd)
                try:
                    upsert_product_url(p["product_id"], store, cmd, sku, active=True)
                    saved += 1
                    idx += 1
                    print(f"  ✓ saved (sku={sku})")
                except Exception as e:
                    print(f"  ✗ DB error: {e!s:.200}")
                continue
            print("  ?? nieznana komenda. Wklej URL albo użyj s/o/?/q.")

        try:
            ctx.storage_state(path=str(state))  # refresh session each run
        except Exception:
            pass
        ctx.close()
        browser.close()

    remaining = total - saved - skipped
    print(f"\n=== Done {store}: saved={saved}, skipped={skipped}, remaining={remaining} ===")


def main():
    ap = argparse.ArgumentParser(description="Interactive URL mapper for inflation_basket.")
    ap.add_argument("--store", required=True, choices=VALID_STORES,
                    help="Sklep do mapowania URL")
    args = ap.parse_args()
    map_store(args.store)


if __name__ == "__main__":
    main()
