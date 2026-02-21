"""
Pracuj.pl Premium Scraper v3 - Portfolio Data Factory
=====================================================
Two-phase scraper: listing (headless) → detail (headed).

Faza 1 (headless):  Listing pages → __NEXT_DATA__ → Top N URLs per kategoria.
Faza 2 (headed):    Detail pages → __NEXT_DATA__ → pełne dane oferty.
                    Headed mode omija Cloudflare Turnstile na stronach detalu.

Dane z detail page:
  - Salary osobno dla UoP i B2B
  - Technologies: Required vs Nice-to-have (tagi)
  - Requirements: Expected vs Optional (opisy bullet-point)
  - Pełny Body HTML z wszystkich sekcji

Kategorie (7):
  Non-IT:  Bankowość, Finanse/Ekonomia, Marketing
  IT:      Business Analytics, Data/BI, AI/ML, Project Management

Wymaga: playwright, pandas
  pip install playwright pandas
  playwright install chromium
"""

import sys
import io
import json
import os
import re
import time
import random
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyodbc
from playwright.sync_api import sync_playwright


# --- Fix kodowania Windows ---
if sys.platform == "win32" and getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")


# --- Ładowanie .env (z katalogu nadrzędnego) ---
SCRAPER_DIR = Path(__file__).parent
PROJECT_DIR = SCRAPER_DIR.parent
ENV_FILE = PROJECT_DIR / ".env"


def _load_env():
    """Ładuje zmienne z .env jeśli plik istnieje."""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


_load_env()


# --- Konfiguracja ---

# --- Pełna lista kategorii (produkcja) ---
CATEGORIES = {
    # Non-IT (www.pracuj.pl) — kody z /dictionary/categories
    "Bankowosc": "https://www.pracuj.pl/praca/bankowosc;cc,5003",
    "Finanse_Ekonomia": "https://www.pracuj.pl/praca/finanse%20-%20ekonomia;cc,5008",
    "Marketing": "https://www.pracuj.pl/praca/marketing;cc,5018",
    # IT — kody z /dictionary/itSpecializations
    "IT_Business_Analytics": "https://it.pracuj.pl/praca?its=business-analytics",
    "IT_Data_BI": "https://it.pracuj.pl/praca?its=data-analytics-and-bi",
    "IT_AI_ML": "https://it.pracuj.pl/praca?its=ai-ml",
    "IT_Project_Management": "https://it.pracuj.pl/praca?its=project-management",
}

MAX_PAGES = 10  # max stron paginacji per kategoria (50 ofert/strone)
TOP_N = 0       # 0 = bez limitu, pobieraj wszystkie oferty
OUTPUT_CSV = "pracuj_premium_data.csv"

def get_output_path() -> str:
    """Zwraca ścieżkę do CSV. Jeśli plik zablokowany, dodaje timestamp."""
    import os
    path = OUTPUT_CSV
    try:
        with open(path, "a"):
            pass
        return path
    except PermissionError:
        base, ext = os.path.splitext(OUTPUT_CSV)
        alt = f"{base}_{datetime.now().strftime('%H%M%S')}{ext}"
        print(f"  [!] {OUTPUT_CSV} zablokowany - zapisuję do {alt}")
        return alt

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

CF_WAIT_SECONDS = 10  # czas na rozwiązanie Cloudflare w headed mode


# --- Utility ---

def _extract_offer_id(url: str) -> str:
    """Wyciaga offer_id z URL-a Pracuj.pl (np. ',oferta,1004604482' -> '1004604482')."""
    m = re.search(r',oferta,(\d+)', url)
    return m.group(1) if m else ""


def polite_delay(min_s: float = 2.0, max_s: float = 4.0):
    time.sleep(random.uniform(min_s, max_s))


def handle_cookie_consent(page) -> bool:
    """Zamyka banner RODO / cookie consent."""
    selectors = [
        'button[data-test="button-submitCookie"]',
        'button[data-test="button-accept-all"]',
        '#onetrust-accept-btn-handler',
        'button:has-text("Akceptuję")',
        'button:has-text("Zaakceptuj")',
        'button:has-text("Accept")',
        'button:has-text("Zgadzam się")',
        '[class*="cookie"] button',
        '[class*="consent"] button:first-child',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2000):
                btn.click()
                page.wait_for_timeout(1000)
                return True
        except Exception:
            continue
    return False


def get_next_data(page) -> dict | None:
    """Wyciąga i parsuje __NEXT_DATA__ JSON ze strony."""
    raw = page.evaluate(
        '() => { const el = document.getElementById("__NEXT_DATA__"); '
        "return el ? el.textContent : null; }"
    )
    if not raw:
        return None
    return json.loads(raw)


# ===================================================================
# FAZA 1: Listing (headless) → zbierz URL-e ofert
# ===================================================================

def _build_page_url(base_url: str, page_num: int) -> str:
    """Dodaje parametr paginacji &pn= lub ?pn= do URL-a."""
    if page_num <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}pn={page_num}"


def phase1_collect_urls(playwright) -> list[dict]:
    """
    Otwiera listing pages każdej kategorii w headless mode.
    Paginacja: iteruje po stronach (?pn=1,2,3...) aż groupedOffers == 0.
    Nowy kontekst per stronę (wymóg Cloudflare).
    """
    print("\n[FAZA 1] Zbieranie URL-i z listing pages (headless, paginacja)...\n")
    browser = playwright.chromium.launch(headless=True)
    stubs: list[dict] = []
    seen_ids: set[str] = set()

    for cat_name, cat_url in CATEGORIES.items():
        print(f"  [{cat_name}] {cat_url}")
        cat_count = 0
        cat_total = "?"

        for page_num in range(1, MAX_PAGES + 1):
            page_url = _build_page_url(cat_url, page_num)

            ctx = browser.new_context(
                user_agent=BROWSER_UA,
                viewport={"width": 1920, "height": 1080},
                locale="pl-PL",
            )
            page = ctx.new_page()

            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(3000)
                handle_cookie_consent(page)
                page.wait_for_timeout(2000)

                nd = get_next_data(page)
                if not nd:
                    print(f"    Str. {page_num}: BRAK __NEXT_DATA__ - przerywam")
                    ctx.close()
                    break

                queries = (
                    nd.get("props", {})
                    .get("pageProps", {})
                    .get("dehydratedState", {})
                    .get("queries", [])
                )

                grouped_offers = []
                for q in queries:
                    sd = q.get("state", {}).get("data", {})
                    if isinstance(sd, dict) and "groupedOffers" in sd:
                        grouped_offers = sd["groupedOffers"]
                        if cat_total == "?":
                            cat_total = sd.get("groupedOffersTotalCount", "?")
                        break

                if not grouped_offers:
                    print(f"    Str. {page_num}: 0 ofert - koniec kategorii")
                    ctx.close()
                    break

                page_count = 0
                for offer in grouped_offers:
                    offers_list = offer.get("offers") or []
                    if not offers_list:
                        continue
                    url = offers_list[0].get("offerAbsoluteUri", "")
                    if not url:
                        continue
                    oid = _extract_offer_id(url) or url
                    if oid in seen_ids:
                        continue
                    seen_ids.add(oid)
                    stubs.append({
                        "category": cat_name,
                        "url": url,
                        "offer_id": oid,
                        "title": offer.get("jobTitle", ""),
                        "company": offer.get("companyName", ""),
                    })
                    page_count += 1
                    cat_count += 1
                    if TOP_N and cat_count >= TOP_N:
                        break

                print(f"    Str. {page_num}: +{page_count} URL-i (łącznie kat.: {cat_count})")

                # Przerwij paginację jeśli osiągnięto TOP_N
                if TOP_N and cat_count >= TOP_N:
                    ctx.close()
                    break

            except Exception as e:
                print(f"    Str. {page_num} BŁĄD: {e}")
                ctx.close()
                break
            finally:
                try:
                    ctx.close()
                except Exception:
                    pass
                polite_delay(1.5, 3.0)

        print(f"    SUMA [{cat_name}]: {cat_count} ofert (serwer: {cat_total})")

    browser.close()
    print(f"\n  Łącznie URL-i do Deep Dive: {len(stubs)}")
    return stubs


# ===================================================================
# FAZA 2: Detail pages (headed) → pełne dane z __NEXT_DATA__
# ===================================================================

def format_salary(salary_obj: dict | None) -> str:
    """Formatuje obiekt salary z detail JSON."""
    if not salary_obj:
        return "Hidden"
    s_from = salary_obj.get("from")
    s_to = salary_obj.get("to")
    currency = salary_obj.get("currency", {}).get("code", "PLN")
    kind = salary_obj.get("salaryKind", {}).get("name", "")
    unit = salary_obj.get("timeUnit", {}).get("shortForm", {}).get("name", "mies.")
    if s_from is None and s_to is None:
        return "Hidden"
    parts = []
    if s_from is not None:
        parts.append(f"{s_from:,.0f}".replace(",", " "))
    if s_to is not None:
        parts.append(f"{s_to:,.0f}".replace(",", " "))
    salary_str = " – ".join(parts)
    return f"{salary_str} {currency} {kind}/{unit}".strip()


def parse_detail_page(nd: dict, category: str, url: str) -> dict:
    """Parsuje __NEXT_DATA__ z detail page do docelowego schematu."""
    queries = (
        nd.get("props", {})
        .get("pageProps", {})
        .get("dehydratedState", {})
        .get("queries", [])
    )

    offer = {}
    for q in queries:
        qkey = q.get("queryKey", [])
        if qkey and qkey[0] == "jobOffer":
            offer = q.get("state", {}).get("data", {})
            break

    if not offer:
        return {"Category": category, "Url": url, "Job_Title": "", "Company": ""}

    attrs = offer.get("attributes", {})
    employment = attrs.get("employment", {})

    # --- Job Title & Company ---
    job_title = attrs.get("jobTitle", "")
    company = attrs.get("displayEmployerName", "")

    # --- Location ---
    workplaces = attrs.get("workplaces") or []
    location = workplaces[0].get("displayAddress", "") if workplaces else ""

    # --- Salary per contract type ---
    salary_uop = "Hidden"
    salary_b2b = "Hidden"
    for tc in employment.get("typesOfContracts") or []:
        name = tc.get("name", "").lower()
        salary = tc.get("salary")
        if "pracę" in name or "praca" in name:
            salary_uop = format_salary(salary)
        elif "b2b" in name:
            salary_b2b = format_salary(salary)

    # --- Position Level ---
    pos_levels = [pl.get("name", "") for pl in employment.get("positionLevels") or []]

    # --- Work Mode ---
    work_modes = [wm.get("name", "") for wm in employment.get("workModes") or []]

    # --- Contract Types (lista) ---
    contract_types = [tc.get("name", "") for tc in employment.get("typesOfContracts") or []]

    # --- textSections: structured flat data ---
    text_sections = {ts["sectionType"]: ts for ts in offer.get("textSections") or []}

    # Technologies Required (tagi IT)
    tech_req_ts = text_sections.get("technologies-expected", {})
    tech_required = tech_req_ts.get("textElements") or []

    # Technologies Nice-to-have (tagi IT)
    tech_opt_ts = text_sections.get("technologies-optional", {})
    tech_nice = tech_opt_ts.get("textElements") or []

    # Requirements Expected (bullet opisy - kluczowe dla non-IT)
    req_exp_ts = text_sections.get("requirements-expected", {})
    req_expected_text = req_exp_ts.get("plainText", "")

    # Requirements Optional (bullet opisy)
    req_opt_ts = text_sections.get("requirements-optional", {})
    req_optional_text = req_opt_ts.get("plainText", "")

    # --- Skills_Required: tagi + opis ---
    # Dla IT: technologies-expected tagi.
    # Dla non-IT: technologies-expected puste → użyj requirements-expected.
    if tech_required:
        skills_required = "; ".join(tech_required)
    else:
        skills_required = req_expected_text

    # --- Skills_Nice_To_Have ---
    if tech_nice:
        skills_nice = "; ".join(tech_nice)
    else:
        skills_nice = req_optional_text

    # --- Body HTML: wszystkie sekcje tekstowe ---
    body_parts = []
    for ts in offer.get("textSections") or []:
        section_type = ts.get("sectionType", "")
        plain = ts.get("plainText", "")
        if plain:
            body_parts.append(f"[{section_type}] {plain}")
    body_html = "\n---\n".join(body_parts)

    # --- Published At ---
    pub = offer.get("publicationDetails", {})
    published_at = pub.get("lastPublishedUtc", "")

    # --- Leading Category (Pracuj.pl own categorization) ---
    leading_cat = attrs.get("leadingCategory", {})
    pracuj_category = leading_cat.get("name", "")

    return {
        "Offer_ID": _extract_offer_id(url),
        "Category": category,
        "Job_Title": job_title,
        "Company": company,
        "Location": location,
        "Salary_UoP": salary_uop,
        "Salary_B2B": salary_b2b,
        "Skills_Required": skills_required,
        "Skills_Nice_To_Have": skills_nice,
        "Requirements_Expected": req_expected_text,
        "Requirements_Nice_To_Have": req_optional_text,
        "Body_HTML": body_html,
        "Url": url,
        "Position_Level": "; ".join(pos_levels),
        "Contract_Types": "; ".join(contract_types),
        "Work_Mode": "; ".join(work_modes),
        "Pracuj_Category": pracuj_category,
        "Published_At": published_at,
        "Scraped_At": datetime.now().isoformat(),
    }


def _launch_headed_browser(playwright):
    """Tworzy nowy headed browser + context + page."""
    browser = playwright.chromium.launch(
        headless=False,
        args=["--disable-blink-features=AutomationControlled"],
    )
    ctx = browser.new_context(
        user_agent=BROWSER_UA,
        viewport={"width": 1920, "height": 1080},
        locale="pl-PL",
    )
    page = ctx.new_page()
    return browser, ctx, page


def phase2_deep_dive(playwright, stubs: list[dict], progress_callback=None) -> list[dict]:
    """
    Otwiera każdą ofertę w headed browser (Cloudflare bypass).
    Wyciąga pełne dane z __NEXT_DATA__ detail page.
    Auto-recovery: restartuje browser po crashu.
    """
    print(f"\n[FAZA 2] Deep Dive - {len(stubs)} ofert (headed browser)...\n")

    browser, ctx, page = _launch_headed_browser(playwright)
    all_rows: list[dict] = []

    for idx, stub in enumerate(stubs, 1):
        url = stub["url"]
        cat = stub["category"]
        short_title = stub["title"][:50]
        print(f"  [{idx}/{len(stubs)}] {cat} | {short_title}...")

        if progress_callback:
            progress_callback(idx, len(stubs), "details")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Czekamy na Cloudflare Turnstile
            time.sleep(CF_WAIT_SECONDS)

            handle_cookie_consent(page)
            time.sleep(1)

            nd = get_next_data(page)
            if not nd:
                title = page.title()
                print(f"    BRAK __NEXT_DATA__ (title: {title}) - używam danych z listingu")
                all_rows.append({
                    "Offer_ID": stub.get("offer_id", _extract_offer_id(url)),
                    "Category": cat,
                    "Job_Title": stub["title"],
                    "Company": stub["company"],
                    "Url": url,
                    "Scraped_At": datetime.now().isoformat(),
                })
                continue

            row = parse_detail_page(nd, cat, url)
            all_rows.append(row)

            print(f"    Tytuł:     {row['Job_Title']}")
            print(f"    Firma:     {row['Company']}")
            print(f"    UoP:       {row['Salary_UoP']}")
            print(f"    B2B:       {row['Salary_B2B']}")
            skills_r = row["Skills_Required"][:60] or "(brak)"
            skills_n = row["Skills_Nice_To_Have"][:60] or "(brak)"
            print(f"    Required:  {skills_r}")
            print(f"    Nice2Have: {skills_n}")

        except Exception as e:
            err_msg = str(e)
            print(f"    BŁĄD: {err_msg}")
            all_rows.append({
                "Offer_ID": stub.get("offer_id", _extract_offer_id(url)),
                "Category": cat,
                "Job_Title": stub["title"],
                "Company": stub["company"],
                "Url": url,
                "Scraped_At": datetime.now().isoformat(),
            })

            # Auto-recovery: jeśli browser/page padł, restartuj
            if "closed" in err_msg.lower() or "crash" in err_msg.lower():
                print("    [recovery] Restartuję przeglądarkę...")
                try:
                    ctx.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass
                time.sleep(2)
                browser, ctx, page = _launch_headed_browser(playwright)
                print("    [recovery] Przeglądarka gotowa")

        polite_delay(2.0, 4.0)

    try:
        ctx.close()
        browser.close()
    except Exception:
        pass
    return all_rows


# ===================================================================
# Azure SQL Upload
# ===================================================================

CREATE_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pracuj_offers')
CREATE TABLE pracuj_offers (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    offer_id                NVARCHAR(50),
    category                NVARCHAR(50)   NOT NULL,
    job_title               NVARCHAR(500),
    company                 NVARCHAR(500),
    location                NVARCHAR(500),
    salary_uop              NVARCHAR(200),
    salary_b2b              NVARCHAR(200),
    skills_required         NVARCHAR(MAX),
    skills_nice_to_have     NVARCHAR(MAX),
    requirements_expected   NVARCHAR(MAX),
    requirements_nice_to_have NVARCHAR(MAX),
    body_html               NVARCHAR(MAX),
    url                     NVARCHAR(1000) NOT NULL,
    position_level          NVARCHAR(200),
    contract_types          NVARCHAR(200),
    work_mode               NVARCHAR(200),
    pracuj_category         NVARCHAR(200),
    published_at            NVARCHAR(50),
    scraped_at              NVARCHAR(50),
    first_seen_at           DATETIME DEFAULT GETDATE(),
    created_at              DATETIME DEFAULT GETDATE(),
    UNIQUE (url)
);
"""

MERGE_SQL = """
MERGE INTO pracuj_offers AS T
USING (SELECT ? as offer_id, ? as category, ? as job_title, ? as company,
              ? as location, ? as salary_uop, ? as salary_b2b,
              ? as skills_required, ? as skills_nice_to_have,
              ? as requirements_expected, ? as requirements_nice_to_have,
              ? as body_html, ? as url, ? as position_level,
              ? as contract_types, ? as work_mode, ? as pracuj_category,
              ? as published_at, ? as scraped_at) AS S
ON T.url = S.url
WHEN MATCHED THEN UPDATE SET
    offer_id = S.offer_id, category = S.category,
    job_title = S.job_title, company = S.company,
    location = S.location, salary_uop = S.salary_uop, salary_b2b = S.salary_b2b,
    skills_required = S.skills_required, skills_nice_to_have = S.skills_nice_to_have,
    requirements_expected = S.requirements_expected,
    requirements_nice_to_have = S.requirements_nice_to_have,
    body_html = S.body_html, position_level = S.position_level,
    contract_types = S.contract_types, work_mode = S.work_mode,
    pracuj_category = S.pracuj_category, published_at = S.published_at,
    scraped_at = S.scraped_at, created_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (offer_id, category, job_title, company, location, salary_uop, salary_b2b,
     skills_required, skills_nice_to_have, requirements_expected,
     requirements_nice_to_have, body_html, url, position_level,
     contract_types, work_mode, pracuj_category, published_at, scraped_at,
     first_seen_at)
    VALUES (S.offer_id, S.category, S.job_title, S.company, S.location,
            S.salary_uop, S.salary_b2b, S.skills_required, S.skills_nice_to_have,
            S.requirements_expected, S.requirements_nice_to_have, S.body_html,
            S.url, S.position_level, S.contract_types, S.work_mode,
            S.pracuj_category, S.published_at, S.scraped_at,
            GETDATE());
"""

# Mapowanie kolumn DataFrame → parametry MERGE (kolejność musi pasować!)
_SQL_COLUMNS = [
    "Offer_ID", "Category", "Job_Title", "Company", "Location",
    "Salary_UoP", "Salary_B2B", "Skills_Required",
    "Skills_Nice_To_Have", "Requirements_Expected",
    "Requirements_Nice_To_Have", "Body_HTML", "Url",
    "Position_Level", "Contract_Types", "Work_Mode",
    "Pracuj_Category", "Published_At", "Scraped_At",
]


def upload_to_azure_sql(df: pd.DataFrame) -> dict:
    """
    Wysyła DataFrame z ofertami do Azure SQL (tabela pracuj_offers).
    Używa MERGE (upsert) po kluczu url - bezpieczne wielokrotne uruchomienie.

    Zwraca dict: {"uploaded": int, "errors": list[str]}
    """
    result = {"uploaded": 0, "errors": []}

    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        msg = "Brak SqlConnectionString w zmiennych środowiskowych (.env)"
        print(f"  [SQL] {msg}")
        result["errors"].append(msg)
        return result

    print(f"\n[SQL] Łączenie z Azure SQL...")

    # Retry logic - Azure SQL serverless może być uśpiony
    max_retries = 3
    conn = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=60)
            print(f"  [SQL] Połączono (próba {attempt}/{max_retries})")
            break
        except pyodbc.Error as e:
            if attempt < max_retries:
                wait = attempt * 15  # 15s, 30s
                print(f"  [SQL] Baza niedostępna (próba {attempt}/{max_retries}), czekam {wait}s...")
                time.sleep(wait)
            else:
                msg = f"Błąd połączenia z Azure SQL po {max_retries} próbach: {e}"
                print(f"  [SQL] {msg}")
                result["errors"].append(msg)
                return result

    try:
        with conn:
            cursor = conn.cursor()

            # Auto-create tabeli
            cursor.execute(CREATE_TABLE_SQL)
            # Migracja: dodaj first_seen_at jeśli brakuje (tabela mogła istnieć wcześniej)
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID('pracuj_offers')
                      AND name = 'first_seen_at'
                )
                ALTER TABLE pracuj_offers ADD first_seen_at DATETIME DEFAULT GETDATE();
            """)
            # Migracja: dodaj offer_id jeśli brakuje
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID('pracuj_offers')
                      AND name = 'offer_id'
                )
                ALTER TABLE pracuj_offers ADD offer_id NVARCHAR(50);
            """)
            conn.commit()
            print("  [SQL] Tabela pracuj_offers - OK")

            # MERGE row-by-row
            uploaded = 0
            for idx, row in df.iterrows():
                vals = []
                for col in _SQL_COLUMNS:
                    v = row.get(col, "")
                    if pd.isna(v):
                        v = None
                    elif isinstance(v, str):
                        v = v.strip()
                    vals.append(v)

                try:
                    cursor.execute(MERGE_SQL, *vals)
                    uploaded += 1
                except Exception as e:
                    err = f"Wiersz {idx} ({row.get('Url', '?')}): {e}"
                    print(f"  [SQL] BŁĄD: {err}")
                    result["errors"].append(err)

            conn.commit()
            result["uploaded"] = uploaded
            print(f"  [SQL] Upload zakończony: {uploaded}/{len(df)} ofert")

    except pyodbc.Error as e:
        msg = f"Błąd połączenia z Azure SQL: {e}"
        print(f"  [SQL] {msg}")
        result["errors"].append(msg)

    return result


# ===================================================================
# Main
# ===================================================================

def run(progress_callback=None) -> dict:
    """
    Uruchamia scraper i zwraca strukturyzowany wynik.

    Args:
        progress_callback: Optional callback(current, total, phase) for progress tracking.

    Zwraca dict z kluczami:
      - success: bool
      - total_offers: int
      - categories_ok: list[str]   — kategorie z >= 1 ofertą
      - categories_empty: list[str] — kategorie z 0 ofert
      - errors: list[str]          — lista błędów
      - output_path: str | None
      - timestamp: str
    """
    result = {
        "success": False,
        "total_offers": 0,
        "sql_uploaded": 0,
        "categories_ok": [],
        "categories_empty": [],
        "errors": [],
        "output_path": None,
        "timestamp": datetime.now().isoformat(),
    }

    print("=" * 70)
    print("  Pracuj.pl  Premium Scraper v3  -  Portfolio Data Factory")
    print("  Faza 1: Listing (headless) → URL-e")
    print("  Faza 2: Detail  (headed)   → pełne dane")
    print(f"  Data:       {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Kategorie:  {len(CATEGORIES)} ({', '.join(CATEGORIES.keys())})")
    print(f"  Paginacja:  WSZYSTKIE oferty (max {MAX_PAGES} stron per kat.)")
    print("=" * 70)

    try:
        with sync_playwright() as p:
            # FAZA 1
            stubs = phase1_collect_urls(p)
            if not stubs:
                result["errors"].append("Faza 1: nie zebrano żadnych URL-i")
                print("\n[FAIL] Nie zebrano żadnych URL-i.")
                return result

            if progress_callback:
                progress_callback(0, len(stubs), "listings_done")

            # FAZA 2
            all_rows = phase2_deep_dive(p, stubs, progress_callback=progress_callback)
    except Exception as e:
        result["errors"].append(f"Krytyczny wyjątek: {e}")
        print(f"\n[FAIL] Krytyczny wyjątek: {e}")
        return result

    if not all_rows:
        result["errors"].append("Faza 2: nie udało się pobrać żadnych ofert")
        print("\n[FAIL] Nie udało się pobrać żadnych ofert.")
        return result

    # --- Zapis do CSV ---
    df = pd.DataFrame(all_rows)

    # Uzupełnij brakujące kolumny dla ofert które failowały
    for col in [
        "Location", "Salary_UoP", "Salary_B2B",
        "Skills_Required", "Skills_Nice_To_Have",
        "Requirements_Expected", "Requirements_Nice_To_Have",
        "Body_HTML", "Position_Level", "Contract_Types",
        "Work_Mode", "Pracuj_Category", "Published_At",
    ]:
        if col not in df.columns:
            df[col] = ""

    output_path = get_output_path()
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    result["output_path"] = output_path

    # --- Upload do Azure SQL ---
    sql_result = upload_to_azure_sql(df)
    result["sql_uploaded"] = sql_result["uploaded"]
    if sql_result["errors"]:
        result["errors"].extend(sql_result["errors"])

    # --- Statystyki per kategoria ---
    result["total_offers"] = len(df)
    for cat in CATEGORIES:
        cat_df = df[df["Category"] == cat]
        if len(cat_df) > 0:
            result["categories_ok"].append(cat)
        else:
            result["categories_empty"].append(cat)

    # Sprawdź oferty bez Job_Title (oznaka złamania parsera)
    empty_titles = df[df["Job_Title"].fillna("").str.strip() == ""]
    if len(empty_titles) > 0:
        result["errors"].append(
            f"{len(empty_titles)} ofert bez Job_Title (możliwa zmiana struktury strony)"
        )

    result["success"] = len(result["errors"]) == 0 and result["total_offers"] > 0

    # --- Podsumowanie ---
    print(f"\n{'='*70}")
    print("  PODSUMOWANIE")
    print(f"{'='*70}")
    print(f"  Łącznie ofert:  {len(df)}")
    print(f"  Zapisano do:    {output_path}\n")

    for _, row in df.iterrows():
        sr = str(row.get("Skills_Required", ""))[:45] or "(brak)"
        sn = str(row.get("Skills_Nice_To_Have", ""))[:45] or "(brak)"
        salary_uop = str(row.get("Salary_UoP", "Hidden"))
        salary_b2b = str(row.get("Salary_B2B", "Hidden"))
        print(f"  [{row['Category']:25s}] {str(row['Job_Title'])[:35]:35s}")
        print(f"    UoP: {salary_uop:30s} B2B: {salary_b2b}")
        print(f"    Req: {sr}")
        print(f"    N2H: {sn}")

    print(f"\n  Rozkład per kategoria:")
    for cat in CATEGORIES:
        cat_df = df[df["Category"] == cat]
        if cat_df.empty:
            continue
        uop_count = (cat_df["Salary_UoP"] != "Hidden").sum()
        b2b_count = (cat_df["Salary_B2B"] != "Hidden").sum()
        print(f"    {cat:25s}  {len(cat_df)} ofert  (UoP: {uop_count}, B2B: {b2b_count})")

    if result["sql_uploaded"] > 0:
        print(f"\n  Azure SQL:  {result['sql_uploaded']}/{len(df)} ofert wysłanych do pracuj_offers")

    status = "[SUCCESS]" if result["success"] else "[WARNING]"
    print(f"\n{'='*70}")
    print(f"{status} Scraping Pracuj.pl zakończony. Ofert: {result['total_offers']}, SQL: {result['sql_uploaded']}")
    if result["errors"]:
        for err in result["errors"]:
            print(f"  [!] {err}")

    return result


def main():
    run()


if __name__ == "__main__":
    main()
