"""Daily price scraper for inflation_basket pipeline.

Frisco: bulk GET /app/commerce/api/v1/offer/products?productIds=A&productIds=B...
        via Playwright session (cookies from homepage warm-up).
Auchan: TODO — separate branch (page.request.get on product URL with stored session).

Output: inflation_observations table (MERGE, idempotent — same-day rerun = update).
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, unquote

from playwright.sync_api import sync_playwright

from inflation_basket.db.operations import (
    _connect_with_retry,
    upsert_observations_batch,
)

VALID_STORES = ("frisco", "auchan_warsaw")

HOMEPAGE = {
    "frisco": "https://www.frisco.pl/",
    "auchan_warsaw": "https://zakupy.auchan.pl/",
}

STATE_DIR = Path(__file__).parent / "seed" / "playwright_state"

# Frisco unitOfMeasure → our capacity_unit
FRISCO_UNIT_MAP = {
    "Kilogram": "kg",
    "Gram": "g",
    "Liter": "l",
    "Mililiter": "ml",
    "Piece": "szt",
    "Pack": "pack",
}


def get_active_urls(store: str) -> list[dict]:
    """Active URL mappings + master catalog for given store."""
    sql = """
    SELECT u.product_id, u.url, u.sku_store,
           p.name_canonical, p.brand, p.matching_type,
           p.capacity_value, p.capacity_unit
    FROM inflation_product_urls u
    JOIN inflation_products p ON p.product_id = u.product_id
    WHERE u.store = ? AND u.active = 1 AND p.status = 'active'
    ORDER BY u.product_id
    """
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        cur.execute(sql, (store,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Frisco branch ────────────────────────────────────────────────────


def _frisco_bulk_fetch(page, sku_list: list[str]) -> dict[str, dict]:
    """One request fetches all products. Returns {sku: product_dict}."""
    if not sku_list:
        return {}
    qs = "&".join(f"productIds={sku}" for sku in sku_list)
    url = f"https://www.frisco.pl/app/commerce/api/v1/offer/products?{qs}"
    resp = page.request.get(url, headers={"Accept": "application/json"})
    if resp.status != 200:
        raise RuntimeError(f"Frisco bulk API {resp.status} for {len(sku_list)} ids")
    body = resp.json()
    products = body.get("products", [])
    out: dict[str, dict] = {}
    for entry in products:
        prod = entry.get("product", entry)
        pid = str(prod.get("productId", ""))
        if pid:
            out[pid] = prod
    return out


def _parse_frisco(prod: dict) -> Optional[dict]:
    """Extract price/promo/capacity/unit from Frisco product JSON.

    Schema (verified 2026-05-01):
      price.price                       → regular price
      price.priceAfterPromotion         → promotional price (when promo active)
      price.discountPercent             → 0 if no promo
      grammage                          → numeric capacity
      unitOfMeasure                     → "Kilogram"|"Liter"|"Piece"|...
      isAvailable                       → bool
    """
    if not prod.get("isAvailable", True):
        return None
    price_block = prod.get("price") or {}
    price_regular = price_block.get("price")
    if price_regular is None:
        return None
    price_regular = float(price_regular)
    discount = float(price_block.get("discountPercent") or 0)
    promo_after = price_block.get("priceAfterPromotion")
    promo_active = bool(discount > 0 and promo_after is not None and float(promo_after) < price_regular)
    price_promo = float(promo_after) if promo_active and promo_after is not None else None

    grammage = prod.get("grammage")
    capacity_seen = float(grammage) if grammage is not None else None
    unit_raw = prod.get("unitOfMeasure", "")
    capacity_unit_seen = FRISCO_UNIT_MAP.get(unit_raw, unit_raw)

    # Unit price: per kg / per liter — derive if possible
    unit_price = None
    if capacity_seen and capacity_seen > 0:
        # price per 100g / 100ml / 1szt (matches inflation_observations.unit_price scale 4dp)
        if capacity_unit_seen in ("kg", "g"):
            grams = capacity_seen * 1000 if capacity_unit_seen == "kg" else capacity_seen
            if grams > 0:
                unit_price = round(price_regular / (grams / 100.0), 4)
        elif capacity_unit_seen in ("l", "ml"):
            ml = capacity_seen * 1000 if capacity_unit_seen == "l" else capacity_seen
            if ml > 0:
                unit_price = round(price_regular / (ml / 100.0), 4)
        elif capacity_unit_seen == "szt":
            unit_price = round(price_regular / capacity_seen, 4)

    return {
        "price_regular": price_regular,
        "price_promo": price_promo,
        "promo_active": promo_active,
        "unit_price": unit_price,
        "capacity_seen": capacity_seen,
    }


def _scrape_frisco(products: list[dict]) -> tuple[list[dict], list[tuple]]:
    """Scrape all Frisco products. Returns (rows_to_upsert, errors)."""
    rows: list[dict] = []
    errors: list[tuple] = []

    state_file = STATE_DIR / "frisco.json"
    state_kwargs = {"storage_state": str(state_file)} if state_file.exists() else {}

    today = date.today()
    now = datetime.utcnow()

    sku_list = [p["sku_store"] for p in products if p.get("sku_store")]
    sku_to_meta = {p["sku_store"]: p for p in products if p.get("sku_store")}

    print(f"[frisco] Bulk fetching {len(sku_list)} products in 1 API call...")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(**state_kwargs, locale="pl-PL")
        page = ctx.new_page()
        try:
            page.goto(HOMEPAGE["frisco"], wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            print(f"  warm-up warning: {e!s:.150}")

        try:
            sku_to_prod = _frisco_bulk_fetch(page, sku_list)
        except Exception as e:
            print(f"[frisco] BULK FETCH FAILED: {e!s:.200}")
            ctx.close()
            browser.close()
            return [], [("BULK_FETCH", str(e))]

        ctx.close()
        browser.close()

    print(f"[frisco] API returned {len(sku_to_prod)}/{len(sku_list)} products")

    for sku, meta in sku_to_meta.items():
        prod = sku_to_prod.get(sku)
        if prod is None:
            errors.append((meta["product_id"], meta["name_canonical"], "missing in bulk response"))
            continue
        parsed = _parse_frisco(prod)
        if parsed is None:
            errors.append((meta["product_id"], meta["name_canonical"], "unavailable / no price"))
            continue
        rows.append({
            "product_id": meta["product_id"],
            "store": "frisco",
            "obs_date": today,
            "obs_ts": now,
            "price_regular": parsed["price_regular"],
            "price_promo": parsed["price_promo"],
            "promo_active": parsed["promo_active"],
            "unit_price": parsed["unit_price"],
            "capacity_seen": parsed["capacity_seen"],
            "currency": "PLN",
        })
    return rows, errors


# ── Auchan branch ────────────────────────────────────────────────────
# zakupy.auchan.pl = Ocado SPA + AWS WAF.
# Approach: page.request.get() on /search?q=... (SSR HTML, no JS execution
# required — bypasses WAF). Product cards in HTML carry data-test markers
# with prices in PLN format ("5,88 zł", with U+00A0 non-breaking space).
#
# Per-product flow:
#   1) build search query from brand+name
#   2) fetch /search?q=... HTML
#   3) locate the product card by retailerProductId
#   4) parse fop-price / fop-reference-price / fop-price-per-unit


_AUCHAN_PRICE_RE = re.compile(
    r'data-test="fop-price">\s*([\d,\. \s]+?)\s*z[łl]', re.IGNORECASE
)
_AUCHAN_REF_RE = re.compile(
    r'data-test="fop-reference-price"[^>]*>[^<]*?([\d,\. \s]+?)\s*z[łl]',
    re.IGNORECASE,
)
_AUCHAN_UNIT_RE = re.compile(
    r'data-test="fop-price-per-unit"[^>]*>\s*\(?\s*([\d,\. \s]+?)\s*z[łl]\s*/\s*(kg|l\b|szt|opak|ml|g)',
    re.IGNORECASE,
)
_AUCHAN_SIZE_RE = re.compile(
    r'data-test="fop-size"[^>]*>([^<]+)<', re.IGNORECASE
)


def _auchan_to_float(s: str) -> Optional[float]:
    s = s.replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _auchan_parse_price(chunk: str) -> Optional[dict]:
    """Parse price markers from a chunk of HTML containing one product card."""
    pm = _AUCHAN_PRICE_RE.search(chunk)
    if not pm:
        return None
    current = _auchan_to_float(pm.group(1))
    if current is None:
        return None

    regular = current
    promo = None
    promo_active = False

    rm = _AUCHAN_REF_RE.search(chunk)
    if rm:
        ref = _auchan_to_float(rm.group(1))
        if ref is not None and ref > current:
            # Reference is the pre-promo "regular" price — current is the promo
            regular = ref
            promo = current
            promo_active = True

    unit_price = None
    um = _AUCHAN_UNIT_RE.search(chunk)
    if um:
        amt = _auchan_to_float(um.group(1))
        kind = um.group(2).lower()
        if amt is not None:
            if kind == "kg":
                unit_price = round(amt / 10.0, 4)  # zł/kg → zł/100g
            elif kind == "l":
                unit_price = round(amt / 10.0, 4)  # zł/l → zł/100ml
            else:  # szt, opak, ml, g
                unit_price = round(amt, 4)

    return {
        "regular": regular,
        "promo": promo,
        "promo_active": promo_active,
        "unit_price": unit_price,
    }


_AUCHAN_QUERY_STOPS = {
    "auchan", "na", "wagę", "wage", "ok", "luz", "sztuka", "szt",
    "g", "kg", "l", "ml", "całe", "do", "i", "jodowana", "rolek",
    "pakowane", "próżniowo", "prozniowo", "warzywa", "owoce",
}


def _slug_query_from_url(url: str, max_words: int = 5) -> Optional[str]:
    """Extract a search query from the product URL slug.

    Pattern: /products/{slug}/{rid}. Empirically (2026-05-02), Auchan search
    indexes the slug words much better than our `name_canonical`, especially
    for branded items where our query word order differs from theirs.
    """
    m = re.search(r"/products/([^/]+)/\d{8}", url)
    if not m:
        return None
    slug = unquote(m.group(1)).replace("-", " ")
    words = [
        w for w in slug.split()
        if w.lower() not in _AUCHAN_QUERY_STOPS
        and not w.replace(",", "").replace(".", "").isdigit()
    ]
    if not words:
        return None
    return " ".join(words[:max_words])


def _auchan_search_html(page, query: str) -> str:
    url = f"https://zakupy.auchan.pl/search?q={quote(query)}"
    resp = page.request.get(url, headers={"Accept": "text/html"}, timeout=25000)
    if resp.status != 200:
        raise RuntimeError(f"Auchan search HTTP {resp.status} for {query!r}")
    return resp.text()


def _auchan_extract_card(html: str, retailer_product_id: str) -> Optional[str]:
    """Return the HTML chunk for the product card matching retailerProductId.

    Strategy: find LAST occurrence of `/products/{slug}/{rid}` in HTML, then
    take ~5000 chars after that point — covers fop-* markers for one card.
    """
    pattern = re.compile(rf'/products/[^"]+/{re.escape(retailer_product_id)}\b')
    matches = list(pattern.finditer(html))
    if not matches:
        return None
    start = matches[-1].end()
    return html[start : start + 5000]


def _scrape_auchan(products: list[dict]) -> tuple[list[dict], list[tuple]]:
    rows: list[dict] = []
    errors: list[tuple] = []
    state_file = STATE_DIR / "auchan_warsaw.json"
    if not state_file.exists():
        return [], [("NO_SESSION", "auchan_warsaw.json missing — run url_mapper first")]

    today = date.today()
    now = datetime.utcnow()

    print(f"[auchan_warsaw] Scraping {len(products)} products via search SSR...")

    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(storage_state=str(state_file), locale="pl-PL", user_agent=ua)
        page = ctx.new_page()
        try:
            page.goto(HOMEPAGE["auchan_warsaw"], wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            print(f"  warm-up warning: {e!s:.150}")
        time.sleep(1)

        for i, p in enumerate(products, 1):
            try:
                brand = (p.get("brand") or "").strip()
                name = p["name_canonical"]
                rid = p.get("sku_store")
                if not rid:
                    errors.append((p["product_id"], name, "no sku_store"))
                    continue

                # Primary: slug-derived query (matches Auchan's own indexing).
                # Fallback: brand + name_canonical (legacy, for products
                # whose URL slug stripped down to nothing).
                queries = []
                slug_q = _slug_query_from_url(p.get("url", ""))
                if slug_q:
                    queries.append(slug_q)
                queries.append(f"{brand} {name}".strip())

                chunk = None
                for q in queries:
                    html = _auchan_search_html(page, q)
                    chunk = _auchan_extract_card(html, rid)
                    if chunk is not None:
                        break
                    time.sleep(0.4)

                if chunk is None:
                    errors.append((p["product_id"], name, "card not found by rid"))
                    continue

                parsed = _auchan_parse_price(chunk)
                if parsed is None:
                    errors.append((p["product_id"], name, "price not found"))
                    continue

                rows.append({
                    "product_id": p["product_id"],
                    "store": "auchan_warsaw",
                    "obs_date": today,
                    "obs_ts": now,
                    "price_regular": parsed["regular"],
                    "price_promo": parsed["promo"],
                    "promo_active": parsed["promo_active"],
                    "unit_price": parsed["unit_price"],
                    "capacity_seen": None,
                    "currency": "PLN",
                })
                time.sleep(0.7)
            except Exception as e:
                errors.append((p["product_id"], p.get("name_canonical", "?"), str(e)[:120]))

        ctx.close()
        browser.close()

    print(f"[auchan_warsaw] Parsed {len(rows)}/{len(products)} products")
    return rows, errors


# ── Orchestration ────────────────────────────────────────────────────


def scrape_store(store: str) -> dict:
    products = get_active_urls(store)
    if not products:
        return {"store": store, "fetched": 0, "saved": 0, "errors": 0, "msg": "no active URLs"}

    print(f"\n=== {store}: {len(products)} active products ===")

    if store == "frisco":
        rows, errors = _scrape_frisco(products)
    elif store == "auchan_warsaw":
        rows, errors = _scrape_auchan(products)
    else:
        return {"store": store, "error": f"unsupported store {store}"}

    saved = 0
    if rows:
        saved = upsert_observations_batch(rows)

    return {
        "store": store,
        "active_products": len(products),
        "rows_built": len(rows),
        "saved": saved,
        "errors": len(errors),
        "error_samples": errors[:5],
    }


def main():
    ap = argparse.ArgumentParser(description="Scrape prices for inflation_basket pipeline.")
    ap.add_argument("--store", required=True, choices=VALID_STORES)
    args = ap.parse_args()

    t0 = time.time()
    result = scrape_store(args.store)
    elapsed = time.time() - t0

    print(f"\n=== Result ({elapsed:.1f}s) ===")
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
