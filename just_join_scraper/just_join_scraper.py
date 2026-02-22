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
import os
import time
import random
import sys
import io
import traceback
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional


# --- Ladowanie .env (lokalny fallback) ---
_SCRAPER_DIR_PATH = Path(__file__).parent
_PROJECT_DIR = _SCRAPER_DIR_PATH.parent
_ENV_FILE = _PROJECT_DIR / ".env"


def _load_env():
    """Laduje zmienne z .env jesli plik istnieje. Niezalezne od pracuj_scraper."""
    if not _ENV_FILE.exists():
        return
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


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

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(_SCRIPT_DIR, "justjoin_premium_selection.json")
CSV_FILE = os.path.join(_SCRIPT_DIR, "justjoin_premium_selection.csv")
KNOWN_OFFERS_FILE = os.path.join(_SCRIPT_DIR, "justjoin_known_offers.json")


# --- Known offers cache (incremental scraping) ---

def load_known_offers() -> dict:
    """Laduje znane offer_id z pliku cache. Zwraca dict {offer_id: publishedAt}."""
    if not os.path.exists(KNOWN_OFFERS_FILE):
        return {}
    try:
        with open(KNOWN_OFFERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("offers", {})
    except (json.JSONDecodeError, OSError):
        return {}


def save_known_offers(known: dict):
    """Zapisuje znane offer_id do pliku cache."""
    data = {
        "last_updated": datetime.now().isoformat(),
        "offers": known,
    }
    with open(KNOWN_OFFERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# --- Azure SQL Constants ---

CREATE_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'justjoin_offers')
CREATE TABLE justjoin_offers (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    offer_id            NVARCHAR(200),
    slug                NVARCHAR(500),
    title               NVARCHAR(500),
    company             NVARCHAR(500),
    company_size        NVARCHAR(100),
    city                NVARCHAR(200),
    workplace_type      NVARCHAR(100),
    working_time        NVARCHAR(100),
    experience_level    NVARCHAR(100),
    search_category     NVARCHAR(50),
    published_at        NVARCHAR(50),
    b2b_from            NVARCHAR(50),
    b2b_to              NVARCHAR(50),
    b2b_currency        NVARCHAR(10),
    b2b_unit            NVARCHAR(20),
    b2b_gross           NVARCHAR(10),
    perm_from           NVARCHAR(50),
    perm_to             NVARCHAR(50),
    perm_currency       NVARCHAR(10),
    perm_unit           NVARCHAR(20),
    perm_gross          NVARCHAR(10),
    other_type          NVARCHAR(100),
    other_from          NVARCHAR(50),
    other_to            NVARCHAR(50),
    other_currency      NVARCHAR(10),
    other_unit          NVARCHAR(20),
    required_skills     NVARCHAR(MAX),
    nice_to_have_skills NVARCHAR(MAX),
    url                 NVARCHAR(1000) NOT NULL,
    body_html           NVARCHAR(MAX),
    scraped_at          NVARCHAR(50),
    first_seen_at       DATETIME DEFAULT GETDATE(),
    created_at          DATETIME DEFAULT GETDATE(),
    UNIQUE (url)
);
"""

MERGE_SQL = """
MERGE INTO justjoin_offers AS T
USING (SELECT ? as offer_id, ? as slug, ? as title, ? as company,
              ? as company_size, ? as city, ? as workplace_type,
              ? as working_time, ? as experience_level, ? as search_category,
              ? as published_at,
              ? as b2b_from, ? as b2b_to, ? as b2b_currency,
              ? as b2b_unit, ? as b2b_gross,
              ? as perm_from, ? as perm_to, ? as perm_currency,
              ? as perm_unit, ? as perm_gross,
              ? as other_type, ? as other_from, ? as other_to,
              ? as other_currency, ? as other_unit,
              ? as required_skills, ? as nice_to_have_skills,
              ? as url, ? as body_html, ? as scraped_at) AS S
ON T.url = S.url
WHEN MATCHED THEN UPDATE SET
    offer_id = S.offer_id, slug = S.slug, title = S.title,
    company = S.company, company_size = S.company_size, city = S.city,
    workplace_type = S.workplace_type, working_time = S.working_time,
    experience_level = S.experience_level, search_category = S.search_category,
    published_at = S.published_at,
    b2b_from = S.b2b_from, b2b_to = S.b2b_to, b2b_currency = S.b2b_currency,
    b2b_unit = S.b2b_unit, b2b_gross = S.b2b_gross,
    perm_from = S.perm_from, perm_to = S.perm_to, perm_currency = S.perm_currency,
    perm_unit = S.perm_unit, perm_gross = S.perm_gross,
    other_type = S.other_type, other_from = S.other_from, other_to = S.other_to,
    other_currency = S.other_currency, other_unit = S.other_unit,
    required_skills = S.required_skills, nice_to_have_skills = S.nice_to_have_skills,
    body_html = S.body_html, scraped_at = S.scraped_at,
    created_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (offer_id, slug, title, company, company_size, city,
     workplace_type, working_time, experience_level, search_category,
     published_at,
     b2b_from, b2b_to, b2b_currency, b2b_unit, b2b_gross,
     perm_from, perm_to, perm_currency, perm_unit, perm_gross,
     other_type, other_from, other_to, other_currency, other_unit,
     required_skills, nice_to_have_skills, url, body_html, scraped_at,
     first_seen_at)
    VALUES (S.offer_id, S.slug, S.title, S.company, S.company_size, S.city,
            S.workplace_type, S.working_time, S.experience_level, S.search_category,
            S.published_at,
            S.b2b_from, S.b2b_to, S.b2b_currency, S.b2b_unit, S.b2b_gross,
            S.perm_from, S.perm_to, S.perm_currency, S.perm_unit, S.perm_gross,
            S.other_type, S.other_from, S.other_to, S.other_currency, S.other_unit,
            S.required_skills, S.nice_to_have_skills, S.url, S.body_html, S.scraped_at,
            GETDATE());
"""

ALTER_TABLE_SQL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('justjoin_offers')
      AND name = 'first_seen_at'
)
ALTER TABLE justjoin_offers ADD first_seen_at DATETIME DEFAULT GETDATE();
"""


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
) -> tuple[list[dict], int]:
    """
    Pobiera WSZYSTKIE oferty z API listingowego (paginacja).
    GET /api/candidate-api/offers?categories={cat}&from={offset}&itemsCount=100

    Zwraca: (lista dict{slug, offer_id}, total dostepnych w kategorii)
    offer_id = guid z listing API = stabilny UUID oferty.
    """
    results = []
    seen_ids: set[str] = set()
    total = 0
    offset = 0

    while True:
        url = f"{API_BASE}?categories={category}&from={offset}&itemsCount={LISTING_PAGE_SIZE}"

        try:
            resp = session.get(url, headers=HEADERS_API, timeout=15)
            if resp.status_code != 200:
                print(f"  [BLAD] HTTP {resp.status_code} na offset={offset}")
                break

            payload = resp.json()
            items = payload.get("data", [])
            total = payload.get("meta", {}).get("totalItems", 0)

            if not items:
                break

            page_new = 0
            for item in items:
                slug = item.get("slug", "")
                offer_id = item.get("guid", "")
                if slug and offer_id and offer_id not in seen_ids:
                    seen_ids.add(offer_id)
                    results.append({"slug": slug, "offer_id": offer_id})
                    page_new += 1

            offset += LISTING_PAGE_SIZE

            if page_new == 0 or offset >= total:
                break

            polite_delay(0.5, 1.5)

        except Exception as e:
            print(f"  [BLAD] {e}")
            break

    return results, total


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
        "offer_id":            raw.get("id", ""),
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
        "offer_id", "slug", "title", "company", "company_size", "city",
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
        "url", "body_html", "scraped_at", "first_seen_at", "created_at",
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
                "offer_id":         offer.get("offer_id", ""),
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
                "body_html":           offer.get("body_html", ""),
                "scraped_at":          offer.get("scraped_at", ""),
                "first_seen_at":       offer.get("scraped_at", ""),
                "created_at":          offer.get("scraped_at", ""),
            }
            writer.writerow(row)

    print(f"  CSV zapisano do:   {filepath}")


# --- Azure SQL Upload ---

def _build_sql_params(offer: dict) -> tuple:
    """
    Buduje 31-elementowa krotke parametrow dla MERGE_SQL.
    Kolejnosc musi odpowiadac ? w MERGE_SQL.
    """
    sals = offer.get("salaries", [])
    b2b = _salary_for_type(sals, "b2b")
    perm = _salary_for_type(sals, "permanent")

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

    def _s(v):
        if v is None:
            return None
        return str(v).strip() if v != "" else None

    return (
        _s(offer.get("offer_id")),
        _s(offer.get("slug")),
        _s(offer.get("title")),
        _s(offer.get("company")),
        _s(offer.get("company_size")),
        _s(offer.get("city")),
        _s(offer.get("workplace_type")),
        _s(offer.get("working_time")),
        _s(offer.get("experience_level")),
        _s(offer.get("search_category")),
        _s(offer.get("published_at")),
        # B2B
        _s(b2b.get("salary_from")),
        _s(b2b.get("salary_to")),
        _s(b2b.get("currency")),
        _s(b2b.get("unit")),
        _s(b2b.get("gross")),
        # Permanent
        _s(perm.get("salary_from")),
        _s(perm.get("salary_to")),
        _s(perm.get("currency")),
        _s(perm.get("unit")),
        _s(perm.get("gross")),
        # Other
        _s(other.get("type")),
        _s(other.get("salary_from")),
        _s(other.get("salary_to")),
        _s(other.get("currency")),
        _s(other.get("unit")),
        # Skills
        req_skills or None,
        nice_skills or None,
        # url, body_html, scraped_at
        _s(offer.get("url")),
        _s(offer.get("body_html")),
        _s(offer.get("scraped_at")),
    )


def upload_to_azure_sql(offers: list[dict]) -> dict:
    """
    Wysyla oferty do Azure SQL (tabela justjoin_offers).
    Uzywa MERGE (upsert) po kluczu url - bezpieczne wielokrotne uruchomienie.

    Zwraca dict: {"uploaded": int, "errors": list[str]}
    """
    import pyodbc

    result = {"uploaded": 0, "errors": []}

    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        msg = "Brak SqlConnectionString w zmiennych srodowiskowych (.env)"
        print(f"  [SQL] {msg}")
        result["errors"].append(msg)
        return result

    print(f"\n[SQL] Laczenie z Azure SQL...")

    max_retries = 3
    conn = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=60)
            print(f"  [SQL] Polaczono (proba {attempt}/{max_retries})")
            break
        except pyodbc.Error as e:
            if attempt < max_retries:
                wait = attempt * 15
                print(f"  [SQL] Baza niedostepna (proba {attempt}/{max_retries}), czekam {wait}s...")
                time.sleep(wait)
            else:
                msg = f"Blad polaczenia z Azure SQL po {max_retries} probach: {e}"
                print(f"  [SQL] {msg}")
                result["errors"].append(msg)
                return result

    try:
        with conn:
            cursor = conn.cursor()

            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(ALTER_TABLE_SQL)
            conn.commit()
            print("  [SQL] Tabela justjoin_offers - OK")

            uploaded = 0
            for i, offer in enumerate(offers):
                params = _build_sql_params(offer)
                try:
                    cursor.execute(MERGE_SQL, *params)
                    uploaded += 1
                except Exception as e:
                    err = f"Wiersz {i} ({offer.get('url', '?')}): {e}"
                    print(f"  [SQL] BLAD: {err}")
                    result["errors"].append(err)

            conn.commit()
            result["uploaded"] = uploaded
            print(f"  [SQL] Upload zakonczony: {uploaded}/{len(offers)} ofert")

    except pyodbc.Error as e:
        msg = f"Blad SQL: {e}"
        print(f"  [SQL] {msg}")
        result["errors"].append(msg)

    return result


def update_last_seen_sql(offer_ids: list[str]):
    """
    Aktualizuje created_at (last_seen) dla wszystkich aktywnych ofert w SQL.
    Wywolywane po Fazie 1 — obejmuje WSZYSTKIE oferty z listingu, nie tylko nowe.
    """
    import pyodbc

    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str or not offer_ids:
        return

    print(f"\n[SQL] Aktualizacja last_seen dla {len(offer_ids)} ofert...")

    max_retries = 3
    conn = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=60)
            break
        except Exception:
            if attempt < max_retries:
                time.sleep(attempt * 15)
            else:
                print(f"  [SQL] Nie udalo sie polaczyc - pomijam UPDATE last_seen")
                return

    try:
        with conn:
            cursor = conn.cursor()
            # Batch po 500 offer_id (limit parametrow SQL)
            batch_size = 500
            updated = 0
            for i in range(0, len(offer_ids), batch_size):
                batch = offer_ids[i:i + batch_size]
                placeholders = ",".join(["?"] * len(batch))
                sql = f"UPDATE justjoin_offers SET created_at = GETDATE() WHERE offer_id IN ({placeholders})"
                cursor.execute(sql, *batch)
                updated += cursor.rowcount
            conn.commit()
            print(f"  [SQL] last_seen zaktualizowano dla {updated} ofert")
    except Exception as e:
        print(f"  [SQL] Blad UPDATE last_seen: {e}")


# --- Main pipeline ---

def run(progress_callback=None, full_mode: bool = False) -> dict:
    """
    Glowna logika scrapera. Zwraca result dict kompatybilny z scraper_monitor.

    Args:
        progress_callback: Optional callback(current, total, phase) for progress tracking.
        full_mode: Jesli True, pomija cache i pobiera detale wszystkich ofert.
    """
    incremental = not full_mode

    result = {
        "success": False,
        "total_offers": 0,
        "new_offers": 0,
        "skipped_known": 0,
        "categories_ok": [],
        "categories_empty": [],
        "errors": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    print("=" * 65)
    print("  JustJoin.it  Premium Selection Scraper")
    print("  Portfolio Data Factory  -  AI Skill Premium")
    print(f"  Data:       {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Kategorie:  {', '.join(c.upper() for c in CATEGORIES)}")
    print(f"  Metoda:     100% REST API (bez Playwright)")
    print(f"  Tryb:       {'PELNY (--full)' if full_mode else 'INKREMENTALNY'}")
    print("=" * 65)

    # Inicjalizacja sesji (ciasteczka)
    print("\n[*] Inicjalizacja sesji HTTP...")
    session = init_session()
    print("    OK - sesja gotowa")

    # ---- FAZA 1: Zbieranie slugow ----
    print("\n[FAZA 1] Zbieranie listy ofert z kazdej kategorii...\n")
    category_items: dict[str, list[dict]] = {}
    seen_ids: set[str] = set()

    for cat in CATEGORIES:
        items, total = collect_slugs_for_category(session, cat)

        # Deduplikacja miedzy kategoriami po offer_id (stabilny UUID)
        unique = [it for it in items if it["offer_id"] not in seen_ids]
        seen_ids.update(it["offer_id"] for it in unique)
        category_items[cat] = unique

        print(f"  {cat.upper():12s}  pobrano {len(unique):>4d} ofert"
              f"  (total w kategorii: {total})")
        polite_delay(1.0, 2.0)

    grand_total = sum(len(v) for v in category_items.values())
    print(f"\n  Lacznie: {grand_total} unikalnych ofert w listingu")

    if grand_total == 0:
        result["errors"].append("API zwrocilo 0 ofert we wszystkich kategoriach!")
        return result

    # Zapisujemy listing count dla monitora (PRZED filtrowaniem)
    result["total_offers"] = grand_total

    # ---- UPDATE last_seen w SQL dla WSZYSTKICH aktywnych ofert ----
    all_listing_ids = [it["offer_id"] for items in category_items.values() for it in items]
    update_last_seen_sql(all_listing_ids)

    # ---- Filtracja known_offers (tryb inkrementalny) ----
    known_offers = {}
    if incremental:
        known_offers = load_known_offers()
        if known_offers:
            total_before = sum(len(v) for v in category_items.values())
            for cat in category_items:
                category_items[cat] = [
                    it for it in category_items[cat]
                    if it["offer_id"] not in known_offers
                ]
            new_total = sum(len(v) for v in category_items.values())
            skipped = total_before - new_total
            result["skipped_known"] = skipped
            print(f"\n[INCREMENTAL] Znanych ofert: {skipped}, nowych do pobrania: {new_total}")
            for cat in CATEGORIES:
                cat_count = len(category_items.get(cat, []))
                if cat_count > 0:
                    print(f"  {cat.upper():12s}  {cat_count} nowych")
        else:
            print(f"\n[INCREMENTAL] Brak cache — pierwsze uruchomienie, pobieram wszystko")

    grand_total_details = sum(len(v) for v in category_items.values())

    if grand_total_details == 0 and incremental and known_offers:
        print(f"\n[OK] Brak nowych ofert — wszystkie {grand_total} juz znane.")
        result["success"] = True
        result["new_offers"] = 0
        for cat in CATEGORIES:
            result["categories_ok"].append(cat)
        save_known_offers(known_offers)
        return result

    # ---- FAZA 2: Deep Dive (tylko PL) ----
    mode_label = "tylko NOWE" if incremental and known_offers else "wszystkie"
    print(f"\n[FAZA 2] Pobieranie szczegolow ofert ({mode_label}, filtr: PL)...\n")
    all_offers: list[dict] = []
    stats: dict[str, int] = {}
    processed = 0
    errors = 0

    for cat, items in category_items.items():
        print(f"  --- {cat.upper()} ({len(items)} ofert) ---")
        cat_offers = []

        for item in items:
            slug = item["slug"]
            processed += 1
            short = slug[:45] + ("..." if len(slug) > 45 else "")
            print(f"    [{processed:3d}/{grand_total_details}] {short:50s}", end="  ")

            if progress_callback:
                progress_callback(processed, grand_total_details, "details")

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
        print(f"  -> Pobrano: {len(cat_offers)}/{len(items)}\n")

    # ---- FAZA 3: Zapis ----
    if not all_offers:
        print("\n[FAIL] Nie udalo sie pobrac zadnych ofert.")
        result["errors"].append("Pobrano 0 ofert po filtrze PL!")
        return result

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

    # ---- Upload do Azure SQL ----
    sql_result = upload_to_azure_sql(all_offers)
    result["sql_uploaded"] = sql_result["uploaded"]
    if sql_result["errors"]:
        result["errors"].extend(sql_result["errors"])

    # ---- Zapis known_offers cache ----
    for offer in all_offers:
        known_offers[offer["offer_id"]] = offer.get("published_at", "")
    save_known_offers(known_offers)
    print(f"  Cache zapisano:    {KNOWN_OFFERS_FILE} ({len(known_offers)} ofert)")

    # ---- Podsumowanie ----
    print("=" * 65)
    print("  PODSUMOWANIE")
    print("=" * 65)
    print(f"  Ofert w listingu:  {grand_total}")
    print(f"  Nowych (detale):   {len(all_offers)}")
    if incremental and result["skipped_known"] > 0:
        print(f"  Pominiete (znane): {result['skipped_known']}")
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

    # ---- Build monitor result ----
    result["success"] = True
    # total_offers = listing count (ustawiony wczesniej), nie detale
    result["new_offers"] = len(all_offers)
    for cat in CATEGORIES:
        if stats.get(cat, 0) > 0:
            result["categories_ok"].append(cat)
        else:
            result["categories_empty"].append(cat)

    return result


def main():
    """Entry point: run scraper z monitoringiem."""
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    # Lokalny _load_env() — scraper dziala niezaleznie od pracuj_scraper
    _load_env()

    full_mode = "--full" in sys.argv

    try:
        result = run(full_mode=full_mode)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"\n[MONITOR] Scraper rzucil wyjatek:\n{tb}")
        result = {
            "success": False,
            "total_offers": 0,
            "categories_ok": [],
            "categories_empty": [],
            "errors": [f"Nieobsluzony wyjatek: {e}"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # Opcjonalny monitoring — nie blokuje jesli pracuj_scraper niedostepny
    try:
        from pracuj_scraper.scraper_monitor import monitor_scraper
        monitor_scraper("JustJoin.it", result)
    except ImportError:
        print("  [INFO] scraper_monitor niedostepny - pomijam monitoring email")


if __name__ == "__main__":
    main()
