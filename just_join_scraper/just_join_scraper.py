"""
JustJoin.it Premium Selection Scraper - Portfolio Data Factory
==============================================================
Strategia "List -> Details" dla Koszyka AI Premium.

Kategorie: DATA, ANALYTICS, PM, AI
Cel: Budowa datasetu Time Series do analizy "AI Skill Premium".

Logika (100% REST API, bez Playwright):
  FAZA 1: GET /api/candidate-api/offers?categories={cat} -> slugi (100/strone)
  FAZA 2: GET /api/candidate-api/offers/{slug}            -> pelne dane oferty
  FAZA 3: Zapis do justjoin_premium_selection.json

Wymaga: requests (pip install requests)
"""

import requests
import json
import csv
import re
import time
import random
import sys
import io
from datetime import datetime
from typing import Optional


# --- Konfiguracja ---
BASE_URL = "https://justjoin.it"
API_BASE = f"{BASE_URL}/api/candidate-api/offers"

HEADERS_PAGE = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
}

HEADERS_API = {
    "User-Agent": HEADERS_PAGE["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/",
}

CATEGORIES = ["data", "analytics", "pm", "ai"]

# Ile ofert pobrac per kategoria (pierwsza strona)
# API zwraca max 100 per request
LISTING_PAGE_SIZE = 100

OUTPUT_FILE = "justjoin_premium_selection.json"
CSV_FILE = "justjoin_premium_selection.csv"


# --- Utility ---

def polite_delay(min_s: float = 1.0, max_s: float = 3.0):
    """Losowe opoznienie aby nie obciazyc serwera."""
    time.sleep(random.uniform(min_s, max_s))


def init_session() -> requests.Session:
    """
    Tworzy sesje HTTP i odwiedza strone glowna aby uzyskac ciasteczka sesji.
    API wymaga waznego unleashSessionId cookie.
    """
    session = requests.Session()
    session.headers.update(HEADERS_PAGE)
    session.get(BASE_URL, timeout=15)
    return session


# --- FAZA 1: Zbieranie slugow (REST API) ---

def collect_slugs_for_category(
    session: requests.Session,
    category: str,
) -> tuple[list[str], int]:
    """
    Pobiera liste ofert z API listingowego.
    GET /api/candidate-api/offers?categories={cat}&from=0&itemsCount=100

    Zwraca: (lista slugow, total dostepnych w kategorii)
    """
    url = f"{API_BASE}?categories={category}&from=0&itemsCount={LISTING_PAGE_SIZE}"

    try:
        resp = session.get(url, headers=HEADERS_API, timeout=15)
        if resp.status_code != 200:
            print(f"  [BLAD] HTTP {resp.status_code}")
            return [], 0

        payload = resp.json()
        items = payload.get("data", [])
        total = payload.get("meta", {}).get("totalItems", 0)

        slugs = []
        for item in items:
            slug = item.get("slug", "")
            if slug and slug not in slugs:
                slugs.append(slug)

        return slugs, total

    except Exception as e:
        print(f"  [BLAD] {e}")
        return [], 0


# --- FAZA 2: Pobieranie szczegulow oferty (REST API) ---

def fetch_offer_details(
    session: requests.Session,
    slug: str,
) -> Optional[dict]:
    """
    Pobiera szczegoly oferty:
    GET /api/candidate-api/offers/{slug}

    Filtruje: tylko oferty z countryCode == 'PL'.
    """
    url = f"{API_BASE}/{slug}"
    try:
        resp = session.get(url, headers=HEADERS_API, timeout=15)
        if resp.status_code != 200:
            return None

        raw = resp.json()
        if not isinstance(raw, dict) or "title" not in raw:
            return None

        # Filtr: tylko polskie oferty
        if raw.get("countryCode", "").upper() != "PL":
            return None

        return _parse_api_offer(raw, slug)

    except Exception:
        return None


def _clean_text(text: str) -> str:
    """Normalizuje whitespace: tabulatory, wielokrotne spacje -> pojedyncza spacja."""
    return re.sub(r'\s+', ' ', text).strip()


def _parse_api_offer(raw: dict, slug: str) -> dict:
    """
    Parsuje odpowiedz API /candidate-api/offers/{slug}.

    Struktura API:
      employmentTypes[]: {type, from, to, currency, currencySource, unit, gross}
      requiredSkills[]:  {name, level (1-5)}
      niceToHaveSkills[]: {name, level}
      experienceLevel, workplaceType, workingTime, body, locations[], ...
    """
    # --- Salary: podzial na typ umowy i okres ---
    salaries = []
    for emp in raw.get("employmentTypes") or []:
        salaries.append({
            "type":                emp.get("type", "unknown"),
            "salary_from":         emp.get("from"),
            "salary_to":           emp.get("to"),
            "salary_from_per_unit": emp.get("fromPerUnit"),
            "salary_to_per_unit":  emp.get("toPerUnit"),
            "currency":            emp.get("currency", "PLN"),
            "currency_source":     emp.get("currencySource", "original"),
            "unit":                emp.get("unit", "Month"),
            "gross":               emp.get("gross", False),
        })

    # --- Skills z poziomami ---
    required_skills = []
    for s in raw.get("requiredSkills") or []:
        if isinstance(s, dict):
            required_skills.append({
                "name":  s.get("name", ""),
                "level": s.get("level", 0),
            })

    nice_to_have_skills = []
    for s in raw.get("niceToHaveSkills") or []:
        if isinstance(s, dict):
            nice_to_have_skills.append({
                "name":  s.get("name", ""),
                "level": s.get("level", 0),
            })

    return {
        "slug":                slug,
        "title":               _clean_text(raw.get("title", "")),
        "company":             _clean_text(raw.get("companyName", "")),
        "company_size":        raw.get("companySize"),
        "city":                _clean_text(raw.get("city", "")),
        "country_code":        raw.get("countryCode", "PL"),
        "workplace_type":      raw.get("workplaceType", ""),
        "working_time":        raw.get("workingTime", ""),
        "experience_level":    raw.get("experienceLevel", ""),
        "salaries":            salaries,
        "required_skills":     required_skills,
        "nice_to_have_skills": nice_to_have_skills,
        "published_at":        raw.get("publishedAt", ""),
        "expired_at":          raw.get("expiredAt", ""),
        "locations":           raw.get("locations") or [],
        "body_html":           raw.get("body", ""),
        "url":                 f"{BASE_URL}/job-offer/{slug}",
    }


# --- Display ---

def display_sample_offer(offer: dict):
    """Wyswietla pelna strukture jednej oferty."""
    print(f"\n  {'='*60}")
    print(f"  PRZYKLADOWA OFERTA (struktura danych)")
    print(f"  {'='*60}")

    print(f"  Title:         {offer['title']}")
    print(f"  Company:       {offer['company']} (size: {offer.get('company_size', '?')})")
    print(f"  City:          {offer['city']}")
    print(f"  Country:       {offer['country_code']}")
    print(f"  Workplace:     {offer['workplace_type']}")
    print(f"  Working time:  {offer['working_time']}")
    print(f"  Experience:    {offer['experience_level']}")
    print(f"  Published:     {offer['published_at']}")
    print(f"  URL:           {offer['url']}")

    original_sal = [s for s in offer["salaries"] if s.get("currency_source") == "original"]
    display_sal = original_sal or offer["salaries"]

    print(f"\n  Salary ({len(offer['salaries'])} total, {len(original_sal)} original currency):")
    for sal in display_sal:
        s_from = f"{sal['salary_from']:,.0f}" if sal["salary_from"] else "?"
        s_to   = f"{sal['salary_to']:,.0f}"   if sal["salary_to"]   else "?"
        marker = "" if sal.get("currency_source") == "original" else " [converted]"
        print(f"    {sal['type']:18s}  {s_from} - {s_to} {sal['currency']}/{sal['unit']}"
              f"  (gross={sal['gross']}){marker}")

    print(f"\n  Required Skills ({len(offer['required_skills'])}):")
    for sk in offer["required_skills"][:10]:
        level_bar = "#" * sk["level"] + "." * (5 - sk["level"])
        print(f"    - {sk['name']:25s} [{level_bar}] ({sk['level']}/5)")

    if offer["nice_to_have_skills"]:
        print(f"\n  Nice-to-have Skills ({len(offer['nice_to_have_skills'])}):")
        for sk in offer["nice_to_have_skills"][:5]:
            print(f"    - {sk['name']:25s} ({sk['level']}/5)")

    body_text = re.sub(r'<[^>]+>', ' ', offer.get("body_html", ""))
    body_text = re.sub(r'\s+', ' ', body_text).strip()
    if body_text:
        print(f"\n  Body preview:  {body_text[:200]}...")

    print(f"  {'='*60}")


# --- CSV Export ---

def _salary_for_type(salaries: list[dict], emp_type: str) -> dict:
    """Zwraca dane salary dla danego typu umowy (original currency preferred)."""
    candidates = [s for s in salaries if s.get("type") == emp_type]
    # Preferuj oryginalna walute
    original = [s for s in candidates if s.get("currency_source") == "original"]
    return (original or candidates or [{}])[0]


def export_csv(offers: list[dict], filepath: str):
    """
    Eksportuje oferty do CSV: jeden wiersz per oferta.
    Oddzielne kolumny salary per typ umowy (b2b, permanent).
    """
    CSV_COLUMNS = [
        "slug", "title", "company", "company_size", "city",
        "workplace_type", "working_time", "experience_level",
        "search_category", "published_at",
        # B2B salary
        "b2b_from", "b2b_to", "b2b_currency", "b2b_unit", "b2b_gross",
        # Permanent (UoP) salary
        "perm_from", "perm_to", "perm_currency", "perm_unit", "perm_gross",
        # Other contract types (mandate, any, etc.)
        "other_type", "other_from", "other_to", "other_currency", "other_unit",
        # Skills
        "required_skills", "nice_to_have_skills",
        "url",
    ]

    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, delimiter=";",
                                extrasaction="ignore")
        writer.writeheader()

        for offer in offers:
            sals = offer.get("salaries", [])

            b2b = _salary_for_type(sals, "b2b")
            perm = _salary_for_type(sals, "permanent")

            # Inne typy umow (mandate_contract, any, internship)
            other = {}
            for s in sals:
                if s.get("type") not in ("b2b", "permanent", "unknown"):
                    other = s
                    break

            req_skills = ", ".join(
                f"{sk['name']}({sk['level']})" for sk in offer.get("required_skills", [])
            )
            nice_skills = ", ".join(
                f"{sk['name']}({sk['level']})" for sk in offer.get("nice_to_have_skills", [])
            )

            row = {
                "slug":             offer.get("slug", ""),
                "title":            offer.get("title", ""),
                "company":          offer.get("company", ""),
                "company_size":     offer.get("company_size", ""),
                "city":             offer.get("city", ""),
                "workplace_type":   offer.get("workplace_type", ""),
                "working_time":     offer.get("working_time", ""),
                "experience_level": offer.get("experience_level", ""),
                "search_category":  offer.get("search_category", ""),
                "published_at":     offer.get("published_at", ""),
                # B2B
                "b2b_from":     b2b.get("salary_from", ""),
                "b2b_to":       b2b.get("salary_to", ""),
                "b2b_currency": b2b.get("currency", ""),
                "b2b_unit":     b2b.get("unit", ""),
                "b2b_gross":    b2b.get("gross", ""),
                # Permanent
                "perm_from":     perm.get("salary_from", ""),
                "perm_to":       perm.get("salary_to", ""),
                "perm_currency": perm.get("currency", ""),
                "perm_unit":     perm.get("unit", ""),
                "perm_gross":    perm.get("gross", ""),
                # Other
                "other_type":     other.get("type", ""),
                "other_from":     other.get("salary_from", ""),
                "other_to":       other.get("salary_to", ""),
                "other_currency": other.get("currency", ""),
                "other_unit":     other.get("unit", ""),
                # Skills
                "required_skills":     req_skills,
                "nice_to_have_skills": nice_skills,
                "url":                 offer.get("url", ""),
            }
            writer.writerow(row)

    print(f"  CSV zapisano do:   {filepath}")


# --- Main ---

def main():
    """Glowna funkcja scrapera Premium Selection."""
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("=" * 65)
    print("  JustJoin.it  Premium Selection Scraper")
    print("  Portfolio Data Factory  -  AI Skill Premium")
    print(f"  Data:       {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Kategorie:  {', '.join(c.upper() for c in CATEGORIES)}")
    print(f"  Metoda:     100% REST API (bez Playwright)")
    print("=" * 65)

    # Inicjalizacja sesji (ciasteczka)
    print("\n[*] Inicjalizacja sesji HTTP...")
    session = init_session()
    print("    OK - sesja gotowa")

    # ---- FAZA 1: Zbieranie slugow ----
    print("\n[FAZA 1] Zbieranie listy ofert z kazdej kategorii...\n")
    category_slugs: dict[str, list[str]] = {}
    seen_slugs: set[str] = set()

    for cat in CATEGORIES:
        slugs, total = collect_slugs_for_category(session, cat)

        # Deduplikacja miedzy kategoriami
        unique = [s for s in slugs if s not in seen_slugs]
        seen_slugs.update(unique)
        category_slugs[cat] = unique

        print(f"  {cat.upper():12s}  pobrano {len(unique):>4d} slugow"
              f"  (total w kategorii: {total})")
        polite_delay(1.0, 2.0)

    grand_total = sum(len(v) for v in category_slugs.values())
    print(f"\n  Lacznie: {grand_total} unikalnych ofert do Deep Dive")

    # ---- FAZA 2: Deep Dive (tylko PL) ----
    print("\n[FAZA 2] Pobieranie szczegolow ofert (REST API, filtr: PL)...\n")
    all_offers: list[dict] = []
    stats: dict[str, int] = {}
    processed = 0
    skipped_non_pl = 0
    errors = 0

    for cat, slugs in category_slugs.items():
        print(f"  --- {cat.upper()} ({len(slugs)} ofert) ---")
        cat_offers = []

        for slug in slugs:
            processed += 1
            short = slug[:45] + ("..." if len(slug) > 45 else "")
            print(f"    [{processed:3d}/{grand_total}] {short:50s}", end="  ")

            offer = fetch_offer_details(session, slug)

            if offer:
                offer["search_category"] = cat
                offer["scraped_at"] = datetime.now().isoformat()
                cat_offers.append(offer)
                print("OK")
            else:
                errors += 1
                print("SKIP")

            polite_delay(1.0, 3.0)

        all_offers.extend(cat_offers)
        stats[cat] = len(cat_offers)
        print(f"  -> Pobrano: {len(cat_offers)}/{len(slugs)}\n")

    # ---- FAZA 3: Zapis ----
    if not all_offers:
        print("\n[FAIL] Nie udalo sie pobrac zadnych ofert.")
        return

    output = {
        "metadata": {
            "scraped_at":        datetime.now().isoformat(),
            "source":            "justjoin.it",
            "country_filter":    "PL",
            "categories":        CATEGORIES,
            "total_offers":      len(all_offers),
            "stats_by_category": stats,
            "errors_skipped":    errors,
        },
        "offers": all_offers,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)

    # ---- CSV Export ----
    export_csv(all_offers, CSV_FILE)

    # ---- Podsumowanie ----
    print("=" * 65)
    print("  PODSUMOWANIE")
    print("=" * 65)
    print(f"  Lacznie ofert:     {len(all_offers)}")
    for cat, count in stats.items():
        print(f"    {cat.upper():15s}  {count}")
    print(f"  Bledy/pominiete:   {errors}")
    print(f"  Zapisano do:       {OUTPUT_FILE}")

    sample = next(
        (o for o in all_offers
         if o.get("salaries") and o["salaries"][0].get("salary_from")),
        all_offers[0],
    )
    display_sample_offer(sample)

    print(f"\n[SUCCESS] Scraping zakonczony pomyslnie!")


if __name__ == "__main__":
    main()
