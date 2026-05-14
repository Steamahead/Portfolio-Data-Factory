"""
auto_mapper.py — automated URL mapper for inflation_basket pipeline.

Frisco: GET /app/commerce/api/v1/offer/products/query via Playwright session
  (React SPA, API requires session cookies → warm homepage first, then call
   the REST endpoint via page.request which reuses the browser session).
  URL format: https://www.frisco.pl/pid,{productId}/n,{slug}/stn,product
  Fields used: productId, product.name.pl, product.brand, product.grammage,
               product.unitOfMeasure (Kilogram / Litre / Piece / Millilitre)

Auchan: requires playwright_state/auchan_warsaw.json (manual session setup).
  If file absent → skipped entirely.

Scoring (no LLM):
  40% capacity match, 30% brand match, 30% name token Jaccard
  >= 0.7 → auto-save to DB
  0.4-0.7 → needs_review.json
  < 0.4  → unavailable bucket in needs_review.json
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote, unquote

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

STATE_DIR = Path(__file__).parent / "seed" / "playwright_state"
REVIEW_FILE = Path(__file__).parent / "needs_review.json"

POLISH_STOPWORDS = {"w", "z", "na", "do", "i", "bez", "ze", "i"}

# Unit normalization to grams / ml
_UNIT_TO_ML = {"l": 1000, "ml": 1}
_UNIT_TO_G = {"kg": 1000, "g": 1}

# Frisco API unitOfMeasure → our capacity_unit
FRISCO_UNIT_MAP = {
    "kilogram": "kg",
    "litre": "l",
    "millilitre": "ml",
    "piece": "szt",
    "gram": "g",
}


# ── SKU extraction (reused from url_mapper) ───────────────────────────

def _extract_sku(store: str, url: str) -> Optional[str]:
    if store == "frisco":
        m = re.search(r"/pid,(\d+)/", url) or re.search(r",2,(\d+)", url) or re.search(r",(\d{4,})(?:[/?#]|$)", url)
        return m.group(1) if m else None
    if store == "auchan_warsaw":
        m = re.search(r"-(\d{5,})(?:[/?#]|$)", url) or re.search(r"/(\d{5,})(?:[/?#]|$)", url)
        return m.group(1) if m else None
    return None


# ── DB helpers ────────────────────────────────────────────────────────

def get_unmapped_products(store: str) -> list[dict]:
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


# ── Scoring ───────────────────────────────────────────────────────────

_STEM_MAP = {
    "jajka": "jaj", "jaja": "jaj", "jaj": "jaj",
    "fasolka": "fasol", "fasola": "fasol",
    "marchewka": "marchew", "marchew": "marchew",
    "pomidorki": "pomidor", "pomidory": "pomidor", "pomidor": "pomidor",
    "oliwki": "oliwka", "oliwka": "oliwka",
    "jabłko": "jabłk", "jabłka": "jabłk",
    "cytryny": "cytryn", "cytryna": "cytryn",
    "banany": "banan", "banan": "banan",
    "stek": "stek", "antrykot": "stek",
}


def _tokens(text: str) -> set[str]:
    words = re.findall(r"[a-ząćęłńóśźżА-Яа-я0-9]+", text.lower())
    result = set()
    for w in words:
        if w in POLISH_STOPWORDS or len(w) <= 1:
            continue
        result.add(_STEM_MAP.get(w, w))
    return result


def _normalize_capacity(value: float, unit: str) -> Optional[float]:
    """Convert to ml (for liquids) or g (for solids); return None for szt/rolek/pack/piece."""
    u = unit.lower()
    if u in _UNIT_TO_ML:
        return value * _UNIT_TO_ML[u]
    if u in _UNIT_TO_G:
        return value * _UNIT_TO_G[u]
    return None  # szt, rolek, pack → compare raw value


def _cap_score(prod: dict, cand_value: float, cand_unit: str, cand_name: str = "") -> float:
    pv = float(prod["capacity_value"])
    pu = prod["capacity_unit"].lower()
    cv = float(cand_value)
    cu = cand_unit.lower()

    # logical_only per-kg: stores sell in variable pack sizes → neutral 0.5
    if prod.get("matching_type") == "logical_only" and pu == "kg":
        return 0.5

    pn = _normalize_capacity(pv, pu)
    cn = _normalize_capacity(cv, cu)

    if pn is not None and cn is not None:
        # Allow ±10% tolerance for ml/g products (package variation)
        if pn > 0 and abs(pn - cn) / pn <= 0.10:
            return 1.0
        return 0.0

    # szt / rolek / pack / piece — compare raw value
    # For rolek: Frisco reports grammage=1.0 szt for any roll pack → try to extract from name
    if pu == "rolek":
        roll_m = re.search(r"(\d+)\s*ro(?:l|łek|lek)", cand_name.lower())
        if roll_m:
            cand_rolls = float(roll_m.group(1))
            return 1.0 if abs(pv - cand_rolls) < 1e-3 else 0.0
        # grammage=1.0 is just a placeholder for packs → neutral
        return 0.5

    if pu in ("szt", "pack") and cu in ("szt", "pack", "piece"):
        # Frisco reports pack-level grammage=1.0 for egg packs, roll packs, etc.
        # Try to extract pack size from candidate name to match against pv
        qty_m = re.search(r"(\d+)\s*(?:szt|sz\.|jaj|kaw|rolek|ról)", cand_name.lower())
        if qty_m:
            cand_qty = float(qty_m.group(1))
            return 1.0 if abs(pv - cand_qty) < 1e-3 else (0.5 if abs(pv - cand_qty) <= 2 else 0.0)
        # cv=1 means Frisco reports whole pack as 1 szt — if our product is also szt-unit, accept
        if cv <= 1.0 and pu == "szt":
            return 0.7  # likely correct pack, just different counting convention
        return 1.0 if abs(pv - cv) < 1e-3 else (0.5 if abs(pv - cv) <= 1 else 0.0)

    return 0.0


def _brand_score(prod: dict, cand_name: str) -> float:
    brand = prod.get("brand")
    if not brand:
        return 0.5  # logical_only neutral
    return 1.0 if brand.lower() in cand_name.lower() else 0.0


def _name_score(prod: dict, cand_name: str) -> float:
    base_tokens = _tokens(prod["name_canonical"])
    # include alternative_names for logical_only
    alt_raw = prod.get("alternative_names")
    if alt_raw and prod.get("matching_type") == "logical_only":
        try:
            alt_list = json.loads(alt_raw) if isinstance(alt_raw, str) else alt_raw
            for a in alt_list:
                base_tokens |= _tokens(a)
        except Exception:
            pass
    cand_tokens = _tokens(cand_name)
    if not base_tokens or not cand_tokens:
        return 0.0
    return len(base_tokens & cand_tokens) / len(base_tokens | cand_tokens)


def score_candidate(prod: dict, cand_name: str, cand_value: float, cand_unit: str) -> float:
    cap = _cap_score(prod, cand_value, cand_unit, cand_name)
    brand = _brand_score(prod, cand_name)
    name = _name_score(prod, cand_name)
    raw = 0.4 * cap + 0.3 * brand + 0.3 * name
    # Change B: brand exact match bonus (+0.2, capped at 1.0)
    brand_str = prod.get("brand") or ""
    if brand_str:
        pattern = r"(?<![a-ząćęłńóśźż])" + re.escape(brand_str.lower()) + r"(?![a-ząćęłńóśźż])"
        if re.search(pattern, cand_name.lower()):
            raw = min(1.0, raw + 0.2)
    return raw


# ── Frisco search ─────────────────────────────────────────────────────

def _frisco_build_query(prod: dict) -> str:
    brand = prod.get("brand") or ""
    name = prod["name_canonical"]
    return f"{brand} {name}".strip()


def _frisco_candidates(page, prod: dict, n: int = 10) -> list[dict]:
    query = _frisco_build_query(prod)
    resp = page.request.get(
        "https://www.frisco.pl/app/commerce/api/v1/offer/products/query",
        params={
            "purpose": "Listing",
            "pageIndex": "1",
            "search": query,
            "includeFacets": "false",
            "pageSize": str(n),
            "language": "pl",
        },
    )
    if resp.status != 200:
        raise RuntimeError(f"Frisco API {resp.status} for query={query!r}")
    body = resp.json()
    results = []
    for item in body.get("products", []):
        p = item["product"]
        name_pl = p["name"]["pl"] if isinstance(p.get("name"), dict) else str(p.get("name", ""))
        brand_str = p.get("brand", "")
        display_name = f"{brand_str} {name_pl}".strip() if brand_str else name_pl
        pid = p["productId"]
        slug = re.sub(r"[^a-z0-9]+", "-", display_name.lower()).strip("-")
        url = f"https://www.frisco.pl/pid,{pid}/n,{slug}/stn,product"
        grammage = p.get("grammage", 0) or 0
        unit_raw = (p.get("unitOfMeasure") or "").lower()
        unit = FRISCO_UNIT_MAP.get(unit_raw, unit_raw)
        # grammage is in base SI: kg→kg, litre→l, piece→szt
        cand_value = float(grammage)
        results.append({
            "name": display_name,
            "url": url,
            "sku": pid,
            "value": cand_value,
            "unit": unit,
        })
    return results


# ── Auchan search ─────────────────────────────────────────────────────
# Approach C: page.request.get() on the SSR search URL (no JS execution).
# zakupy.auchan.pl = Ocado SPA + AWS WAF.  Direct goto() destroys the JS
# execution context.  page.request bypasses the runtime and sends a plain
# HTTP request with session cookies attached.  The SSR HTML contains:
#  • href="/products/{slug}/{retailerProductId}"  → product URL (HTTP 200 verified)
#  • "productEntities" in __INITIAL_STATE__ → brand + canonical name (img.description)

_CAP_RE = re.compile(
    r'(\d+(?:[,\.]\d+)?)\s*(kg|g|l\b|ml|litr(?:ów|y|a)?|gram(?:ów|y|a)?|szt(?:uk)?\.?|rolek|sztuk)',
    re.IGNORECASE,
)
_UNIT_NORM = {"kg":"kg","g":"g","l":"l","ml":"ml","litrów":"l","litry":"l","litra":"l",
              "gramów":"g","gramy":"g","grama":"g","szt":"szt","szt.":"szt",
              "sztuk":"szt","sztuka":"szt","rolek":"rolek"}


def _parse_capacity(name: str) -> tuple[float, str]:
    m = _CAP_RE.search(name)
    if not m:
        return 0.0, "szt"
    unit = _UNIT_NORM.get(m.group(2).lower().rstrip("."), m.group(2).lower())
    return float(m.group(1).replace(",", ".")), unit


def _auchan_candidates(page, prod: dict, n: int = 15) -> list[dict]:
    """Fetch via page.request on the SSR search page (no JS execution needed)."""
    query = _frisco_build_query(prod)
    resp = page.request.get(
        f"https://zakupy.auchan.pl/search?q={quote(query)}", timeout=25000
    )
    if resp.status != 200:
        raise RuntimeError(f"Auchan search HTTP {resp.status} for {query!r}")
    html = resp.text()

    # Extract product hrefs: /products/{slug}/{8-digit-id}
    seen: set[str] = set()
    href_rows = []
    for m in re.finditer(r'href="(/products/([^/"]+)/([0-9]{8,9}))"', html):
        rid = m.group(3)
        if rid not in seen:
            seen.add(rid)
            href_rows.append((m.group(1), unquote(m.group(2)), rid))

    # Build brand + canonical-name map from productEntities in __INITIAL_STATE__
    entity_map: dict[str, dict] = {}
    pe_idx = html.find('"productEntities":{"')
    if pe_idx < 0:
        pe_idx = html.find('"productEntities": {"')
    if pe_idx >= 0:
        chunk = html[pe_idx : pe_idx + 600_000]
        for m in re.finditer(
            r'"retailerProductId"\s*:\s*"([0-9]{8,9})"[^}]*?"brand"\s*:\s*"([^"]*)"',
            chunk, re.DOTALL
        ):
            entity_map.setdefault(m.group(1), {})["brand"] = m.group(2)
        for m in re.finditer(
            r'"retailerProductId"\s*:\s*"([0-9]{8,9})".*?"description"\s*:\s*"([^"]{3,120})"',
            chunk, re.DOTALL
        ):
            entity_map.setdefault(m.group(1), {})["img_name"] = m.group(2)

    results = []
    for full_path, slug, rid in href_rows[:n]:
        ent = entity_map.get(rid, {})
        name = ent.get("img_name") or slug.replace("-", " ").strip()
        brand = ent.get("brand", "")
        if brand and brand.lower() not in name.lower():
            name = f"{brand} {name}".strip()
        value, unit = _parse_capacity(name)
        results.append({
            "name": name, "url": f"https://zakupy.auchan.pl{full_path}",
            "sku": rid, "value": value, "unit": unit,
        })
    return results


# ── Review log ────────────────────────────────────────────────────────

def _load_review() -> dict:
    if REVIEW_FILE.exists():
        try:
            return json.loads(REVIEW_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"needs_review": {}, "unavailable": {}, "errors": {}}


def _save_review(data: dict) -> None:
    REVIEW_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


# ── Main runner ───────────────────────────────────────────────────────

def run(store: str, dry_run: bool = False) -> None:
    if store not in VALID_STORES:
        print(f"ERROR: store must be one of {VALID_STORES}", file=sys.stderr)
        sys.exit(2)

    if store == "auchan_warsaw":
        state_file = STATE_DIR / "auchan_warsaw.json"
        if not state_file.exists():
            print("SKIP: Auchan needs manual session setup — user must run url_mapper once")
            print("  python -X utf8 -m inflation_basket.url_mapper --store auchan_warsaw")
            return

    products = get_unmapped_products(store)
    print(f"\n=== auto_mapper — store: {store} ===")
    print(f"Products to map: {len(products)}")
    if not products:
        print("Nothing to do.")
        return

    review = _load_review()
    saved = needs_review = unavailable = errors = 0
    start_ts = time.time()

    state_kwargs = {}
    if store == "auchan_warsaw":
        state_kwargs["storage_state"] = str(STATE_DIR / "auchan_warsaw.json")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(locale="pl-PL", **state_kwargs)
        page = ctx.new_page()

        # Warm session
        print(f"Warming {HOMEPAGE[store]} ...")
        page.goto(HOMEPAGE[store], timeout=25000, wait_until="domcontentloaded")
        time.sleep(1)

        for idx, prod in enumerate(products):
            pid = prod["product_id"]
            name = prod["name_canonical"]
            brand = prod.get("brand") or ""
            label = f"[{idx+1}/{len(products)}] {brand} {name}".strip()

            try:
                if store == "frisco":
                    candidates = _frisco_candidates(page, prod)
                else:
                    candidates = _auchan_candidates(page, prod)
            except Exception as e:
                print(f"  ERROR {label}: {e!s:.120}")
                review["errors"][f"{pid}__{store}"] = {"product_id": pid, "name": name, "error": str(e)[:200]}
                errors += 1
                _save_review(review)
                time.sleep(random.uniform(0.5, 1.5))
                continue

            if not candidates:
                print(f"  UNAVAILABLE {label} — no candidates")
                review["unavailable"][f"{pid}__{store}"] = {"product_id": pid, "name": name, "brand": brand}
                unavailable += 1
                _save_review(review)
                time.sleep(random.uniform(0.5, 1.5))
                continue

            # Score all candidates
            scored = []
            for c in candidates:
                s = score_candidate(prod, c["name"], c["value"], c["unit"])
                scored.append((s, c))
            scored.sort(key=lambda x: x[0], reverse=True)

            best_score, best = scored[0]

            if dry_run:
                top3 = scored[:3]
                print(f"\n{label}")
                for sc, c in top3:
                    print(f"  {sc:.2f} | {c['name'][:60]} | {c['value']} {c['unit']} | {c['url'][:80]}")
                time.sleep(random.uniform(0.3, 0.8))
                continue

            key = f"{pid}__{store}"
            # Change A: split threshold by matching_type
            mtype = prod.get("matching_type", "same_sku")
            save_threshold = 0.5 if mtype == "logical_only" else 0.7
            review_threshold = save_threshold - 0.3  # 0.2 for logical_only, 0.4 for same_sku

            if best_score >= save_threshold:
                sku = best.get("sku") or _extract_sku(store, best["url"])
                upsert_product_url(pid, store, best["url"], sku, active=True)
                saved += 1
                print(f"  SAVED   {label} -> score={best_score:.2f} url={best['url'][:80]}")
            elif best_score >= review_threshold:
                review["needs_review"][key] = {
                    "product_id": pid, "name": name, "brand": brand,
                    "matching_type": mtype,
                    "top3": [{"score": sc, "name": c["name"], "url": c["url"], "value": c["value"], "unit": c["unit"]} for sc, c in scored[:3]],
                }
                needs_review += 1
                print(f"  REVIEW  {label} -> score={best_score:.2f} ({best['name'][:50]})")
                _save_review(review)
            else:
                review["unavailable"][key] = {
                    "product_id": pid, "name": name, "brand": brand,
                    "matching_type": mtype,
                    "best_score": best_score,
                    "best_candidate": best["name"][:80] if best else None,
                }
                unavailable += 1
                print(f"  UNAVAIL {label} -> score={best_score:.2f}")
                _save_review(review)

            time.sleep(random.uniform(0.5, 1.5))

        ctx.close()
        browser.close()

    elapsed = time.time() - start_ts
    if not dry_run:
        _save_review(review)
        print(f"\n=== {store} done in {elapsed:.0f}s ===")
        print(f"  saved={saved}  needs_review={needs_review}  unavailable={unavailable}  errors={errors}")
        print(f"  review file: {REVIEW_FILE}")
    else:
        print(f"\n=== dry-run complete ({elapsed:.0f}s) ===")


def main() -> None:
    ap = argparse.ArgumentParser(description="Automated URL mapper for inflation_basket.")
    ap.add_argument("--store", required=True, choices=VALID_STORES)
    ap.add_argument("--dry-run", action="store_true", help="Print top-3 per product, no DB writes")
    args = ap.parse_args()
    run(args.store, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
