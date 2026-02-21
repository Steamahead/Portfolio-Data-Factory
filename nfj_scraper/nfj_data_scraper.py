# -*- coding: utf-8 -*-
"""
NoFluffJobs Data Scraper - Premium Basket v6 (Search API + withSalaryMatch)
Two-stage pipeline: paginated Search API → detail enrichment → master dataset.
Stage 1 uses POST /api/search/posting with withSalaryMatch=true for full coverage.
Deduplicates by stable `reference` key (returned directly by Search API).
Tracks first_seen / last_seen / is_active for time-series analysis.
Part of Portfolio Data Factory — "AI Skill Premium" analysis.
"""

import json
import os
import random
import sys
import time
import traceback
import requests
import pyodbc
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- .env loading ---

SCRAPER_DIR = Path(__file__).parent
PROJECT_DIR = SCRAPER_DIR.parent
ENV_FILE = PROJECT_DIR / ".env"


def _load_env():
    """Load variables from .env if file exists."""
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

# --- Configuration ---

API_SEARCH_URL = "https://nofluffjobs.com/api/search/posting"
API_DETAIL_URL = "https://nofluffjobs.com/api/posting"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://nofluffjobs.com",
    "Referer": "https://nofluffjobs.com/pl/praca-it",
}

# Premium basket: display groups → search-style category names
PREMIUM_BASKET = {
    "tech_core": [
        "data",
        "artificial-intelligence",
        "project-manager",
        "business-analyst",
    ],
    "business_control": [
        "marketing",
        "sales",
        "finance",
    ],
}

MASTER_CSV = SCRAPER_DIR / "nfj_master.csv"
SNAPSHOT_JSON = SCRAPER_DIR / "nfj_latest_snapshot.json"
CHECKPOINT_FILE = SCRAPER_DIR / "nfj_checkpoint.json"
DETAIL_DELAY = (1, 2)
LISTING_DELAY = (1, 3)
CHECKPOINT_EVERY = 50


# ============================================================
# MASTER DATASET
# ============================================================

def load_master() -> pd.DataFrame:
    """Load existing master CSV or return empty DataFrame."""
    path = Path(MASTER_CSV)
    if path.exists() and path.stat().st_size > 0:
        df = pd.read_csv(path, encoding="utf-8-sig")

        if "reference" not in df.columns:
            print("  WARNING: Master CSV lacks 'reference' column (legacy format).")
            print("  Starting fresh — old master backed up as nfj_master_legacy.csv")
            df.to_csv(SCRAPER_DIR / "nfj_master_legacy.csv", index=False, encoding="utf-8-sig")
            return pd.DataFrame()

        print(f"  Loaded master: {len(df)} rows, {df['reference'].nunique()} unique references")
        return df
    print("  No master file found — starting fresh")
    return pd.DataFrame()


def save_master(df: pd.DataFrame) -> None:
    """Save master dataset to CSV."""
    df.to_csv(MASTER_CSV, index=False, encoding="utf-8-sig")
    active = df["is_active"].sum() if "is_active" in df.columns else len(df)
    print(f"[OK] Master saved: {len(df)} total rows, {int(active)} active -> {MASTER_CSV}")


# ============================================================
# STAGE 1: LISTINGS (Search API with withSalaryMatch)
# ============================================================

def fetch_category(category: str) -> list[dict]:
    """Fetch all pages for one search category with withSalaryMatch=true.

    Uses page as query parameter (confirmed working in v4 pagination fix).
    Each posting includes a stable `reference` field for deduplication.
    """
    all_offers = []
    page = 1
    while True:
        payload = {
            "criteriaSearch": {
                "category": [category],
                "withSalaryMatch": ["true"],
            },
        }
        resp = requests.post(
            API_SEARCH_URL,
            params={"salaryCurrency": "PLN", "salaryPeriod": "month", "page": page},
            json=payload,
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        postings = data.get("postings", [])
        total_pages = data.get("totalPages", 1)
        all_offers.extend(postings)
        if page >= total_pages:
            break
        page += 1
        time.sleep(random.uniform(*LISTING_DELAY))
    return all_offers


def fetch_listings() -> list[dict]:
    """Stage 1: Fetch postings via paginated Search API with withSalaryMatch=true.

    Uses POST /api/search/posting per category. The withSalaryMatch parameter
    unlocks offers without explicit salary data (~49% more coverage vs default).
    Each posting has a stable `reference` field for deduplication.
    Cross-category duplicates are removed by reference.
    """
    all_categories = [c for g in PREMIUM_BASKET.values() for c in g]
    all_postings = []
    total_raw = 0

    for i, cat in enumerate(all_categories, 1):
        print(f"  [{i}/{len(all_categories)}] Fetching: {cat}...", end=" ", flush=True)
        offers = fetch_category(cat)
        for o in offers:
            o["scraper_category"] = cat
        all_postings.extend(offers)
        total_raw += len(offers)
        print(f"OK — {len(offers)} offers")
        time.sleep(random.uniform(*LISTING_DELAY))

    # Dedup by reference (stable key from Search API)
    # First occurrence wins — preserves the first category assignment
    seen: dict[str, dict] = {}
    for p in all_postings:
        ref = p.get("reference") or p.get("id")
        if ref not in seen:
            seen[ref] = p
    unique = list(seen.values())

    print(f"\n  Total: {total_raw} raw, {len(unique)} unique (dedup by reference)")

    # Per-category unique counts
    for cat in all_categories:
        count = sum(1 for p in unique if p["scraper_category"] == cat)
        print(f"    {cat:<25} {count:>5} unique")

    return unique


# ============================================================
# STAGE 2: DETAILS ENRICHMENT
# ============================================================

def fetch_posting_detail(posting_id: str) -> dict | None:
    """Fetch full details for a single posting. Returns reference + enrichment data."""
    response = requests.get(
        f"{API_DETAIL_URL}/{posting_id}",
        headers=HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    reqs = data.get("requirements", {})
    essentials = data.get("essentials", {})
    original_salary = essentials.get("originalSalary", {})
    basics = data.get("basics", {})
    specs = data.get("specs", {})
    details = data.get("details", {})

    musts = [m["value"] for m in reqs.get("musts", [])]
    nices = [n["value"] for n in reqs.get("nices", [])]

    languages = []
    for lang in reqs.get("languages", []):
        code = lang.get("code", "").upper()
        level = lang.get("level", "")
        languages.append(f"{code} ({level})" if level else code)

    salary_types = original_salary.get("types", {})

    return {
        "reference": data.get("reference"),
        "must_have_skills": musts,
        "nice_to_have_skills": nices,
        "languages": languages,
        "original_salary": salary_types,
        "body_description": details.get("description", ""),
        "daily_tasks": specs.get("dailyTasks", []),
        "requirements_description": reqs.get("description", ""),
        "nfj_category": basics.get("category", ""),
        "seniority_detail": basics.get("seniority", []),
        "contract_types_detail": essentials.get("contract", []),
        "location_detail": data.get("location", {}),
    }


def load_checkpoint() -> dict:
    """Load checkpoint of already-fetched detail IDs."""
    path = Path(CHECKPOINT_FILE)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(details: dict) -> None:
    """Save checkpoint to disk."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False)


def fetch_details(postings: list[dict], progress_callback=None) -> list[dict]:
    """Stage 2: Enrich each posting with full details from detail endpoint.

    Checkpoint keyed by posting ID (stable for resume).
    """
    if not postings:
        print("  No new postings to enrich.")
        return postings

    checkpoint = load_checkpoint()
    total = len(postings)
    enriched = 0
    skipped = 0
    failed = 0
    start_time = time.time()

    for i, posting in enumerate(postings):
        pid = posting["id"]

        # Resume from checkpoint (keyed by posting id)
        if pid in checkpoint:
            detail = checkpoint[pid]
            for key in detail:
                posting[key] = detail[key]
            skipped += 1
            continue

        if progress_callback:
            progress_callback(i + 1, total, "details")

        elapsed = time.time() - start_time
        rate = (enriched + skipped) / elapsed if elapsed > 0 else 0
        remaining = (total - i) / rate / 60 if rate > 0 else 0
        print(f"  [{i + 1}/{total}] {pid[:60]}...", end=" ")

        try:
            detail = fetch_posting_detail(pid)
            for key in detail:
                posting[key] = detail[key]
            checkpoint[pid] = detail
            enriched += 1
            musts_count = len(detail["must_have_skills"])
            nices_count = len(detail["nice_to_have_skills"])
            print(f"OK ({musts_count}m/{nices_count}n) ~{remaining:.0f}min left")
        except requests.exceptions.HTTPError as e:
            print(f"FAILED (HTTP {e.response.status_code})")
            failed += 1
        except requests.exceptions.RequestException as e:
            print(f"FAILED ({e})")
            failed += 1

        if enriched % CHECKPOINT_EVERY == 0 and enriched > 0:
            save_checkpoint(checkpoint)

        time.sleep(random.uniform(*DETAIL_DELAY))

    save_checkpoint(checkpoint)
    print(f"\n  Details: {enriched} fetched, {skipped} from cache, {failed} failed")

    return postings


# ============================================================
# TRANSFORM: FLATTEN TO DATAFRAME
# ============================================================

def _format_salary(salary_data: dict) -> str | None:
    """Format NFJ salary range like Pracuj: '15 325 – 22 500 PLN/mies.'"""
    if not salary_data:
        return None
    rng = salary_data.get("range", [])
    if not rng or len(rng) < 2 or rng[0] is None:
        return None
    lo, hi = rng[0], rng[1]
    currency = salary_data.get("currency", "PLN")
    period = salary_data.get("period", "month")
    period_map = {"month": "mies.", "hour": "godz.", "day": "dzień", "year": "rok"}
    period_label = period_map.get(period, period)
    lo_str = f"{lo:,.0f}".replace(",", " ")
    hi_str = f"{hi:,.0f}".replace(",", " ")
    return f"{lo_str} – {hi_str} {currency}/{period_label}"


def _build_work_mode(posting: dict) -> str:
    """Determine work mode: Remote / Hybrid / Office.

    Checks multiple sources because Listing API uses ``fullyRemote`` (bool)
    while Detail API uses ``location.remote`` (int, e.g. 100 = fully remote).
    """
    location_detail = posting.get("location_detail") or {}
    location_listing = posting.get("location", {})

    # 1. Listing API top-level flag (most reliable)
    if posting.get("fullyRemote", False):
        return "Remote"
    # 2. Listing API location.fullyRemote
    if location_listing.get("fullyRemote", False):
        return "Remote"
    # 3. Detail API location.remote (int: 0 = office, 100 = fully remote)
    remote_int = location_detail.get("remote")
    if remote_int is not None and remote_int >= 100:
        return "Remote"

    # Hybrid: hybridDesc present or partial remote
    hybrid = location_detail.get("hybridDesc") or location_listing.get("hybridDesc") or ""
    if hybrid:
        return "Hybrid"
    if remote_int and remote_int not in (0, False, None):
        return "Hybrid"

    return "Office"


def _build_body_html(posting: dict) -> str | None:
    """Build combined body_html from detail sections."""
    parts = []
    desc = posting.get("body_description", "")
    if desc:
        parts.append(f"[description]\n{desc}")
    tasks = posting.get("daily_tasks", [])
    if tasks:
        if isinstance(tasks, list):
            tasks_html = "\n".join(f"<li>{t}</li>" for t in tasks)
            parts.append(f"[dailyTasks]\n<ul>{tasks_html}</ul>")
        else:
            parts.append(f"[dailyTasks]\n{tasks}")
    req_desc = posting.get("requirements_description", "")
    if req_desc:
        parts.append(f"[requirements]\n{req_desc}")
    return "\n---\n".join(parts) if parts else None


def flatten_posting(posting: dict) -> dict:
    """Extract a flat dict from one enriched posting — unified schema matching Pracuj.pl."""
    location = posting.get("location_detail") or posting.get("location", {})
    places = location.get("places", [])

    # Build location string: "Remote; Kraków; Warszawa"
    is_remote = location.get("fullyRemote", False)
    cities = []
    for place in places:
        city = place.get("city", "")
        if city and city != "Remote":
            cities.append(city)
    if is_remote or any(p.get("city") == "Remote" for p in places):
        cities.insert(0, "Remote")

    musts = posting.get("must_have_skills", [])
    nices = posting.get("nice_to_have_skills", [])

    orig = posting.get("original_salary", {})
    b2b = orig.get("b2b", {})
    perm = orig.get("permanent", {})

    # Contract types from salary keys
    contract_parts = []
    if perm:
        contract_parts.append("UoP")
    if b2b:
        contract_parts.append("B2B")
    # Also check detail-level contract info
    contract_detail = posting.get("contract_types_detail", [])
    if isinstance(contract_detail, list):
        for c in contract_detail:
            label = c if isinstance(c, str) else c.get("type", "")
            if label and label not in contract_parts:
                contract_parts.append(label)

    # Seniority from detail or listing
    seniority = posting.get("seniority_detail") or posting.get("seniority", [])
    if isinstance(seniority, list) and seniority:
        position_level = "; ".join(seniority)
    elif isinstance(seniority, str):
        position_level = seniority
    else:
        position_level = None

    # Published at
    posted_ts = posting.get("posted")
    published_at = (
        datetime.fromtimestamp(posted_ts / 1000, tz=timezone.utc).isoformat()
        if posted_ts else None
    )

    # Full URL
    default_url = posting.get("defaultUrl") or posting.get("url") or posting.get("id", "")
    full_url = f"https://nofluffjobs.com/pl/job/{default_url}"

    return {
        "reference": posting.get("reference"),
        "category": posting.get("scraper_category"),
        "job_title": posting.get("title"),
        "company": posting.get("name"),
        "location": "; ".join(cities) if cities else None,
        "salary_uop": _format_salary(perm),
        "salary_b2b": _format_salary(b2b),
        "skills_required": "; ".join(musts) if musts else None,
        "skills_nice_to_have": "; ".join(nices) if nices else None,
        "requirements_expected": posting.get("requirements_description") or None,
        "requirements_nice_to_have": None,
        "body_html": _build_body_html(posting),
        "url": full_url,
        "position_level": position_level,
        "contract_types": "; ".join(contract_parts) if contract_parts else None,
        "work_mode": _build_work_mode(posting),
        "nfj_category": posting.get("nfj_category") or posting.get("category"),
        "published_at": published_at,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# INCREMENTAL RECONCILIATION
# ============================================================

def reconcile(master: pd.DataFrame, current_listings: list[dict]) -> tuple[list[dict], pd.DataFrame]:
    """
    Compare current Search API listings with master dataset.

    Search API provides `reference` directly — matching is straightforward.

    Returns:
        new_postings: list of raw postings that need detail fetching
        master: updated master DataFrame with last_seen/is_active refreshed
    """
    now = datetime.now(timezone.utc).isoformat()

    if master.empty:
        print(f"  First run — all {len(current_listings)} offers are NEW")
        return current_listings, master

    # Build current reference set
    current_refs: dict[str, dict] = {}
    for p in current_listings:
        ref = p.get("reference") or p.get("id")
        current_refs[ref] = p

    known_refs = set(master["reference"].dropna().unique())
    current_ref_set = set(current_refs.keys())

    still_active = current_ref_set & known_refs
    new_refs = current_ref_set - known_refs
    expired = known_refs - current_ref_set

    # Update last_seen for still-active offers
    if still_active:
        mask = master["reference"].isin(still_active)
        master.loc[mask, "last_seen_at"] = now
        master.loc[mask, "is_active"] = True

    # Mark expired offers
    if expired:
        master.loc[master["reference"].isin(expired), "is_active"] = False

    new_postings = [p for p in current_listings if (p.get("reference") or p.get("id")) in new_refs]

    print(f"  Reconciliation:")
    print(f"    Still active:  {len(still_active)}")
    print(f"    New:           {len(new_postings)}")
    print(f"    Expired:       {len(expired)}")

    return new_postings, master


# ============================================================
# OUTPUT
# ============================================================

def print_summary(master: pd.DataFrame, new_count: int, run_timestamp: str) -> None:
    """Print run summary."""
    print("\n" + "=" * 55)
    print(f"  RUN SUMMARY — {run_timestamp}")
    print("=" * 55)

    active = master[master["is_active"] == True] if "is_active" in master.columns else master
    expired = master[master["is_active"] == False] if "is_active" in master.columns else pd.DataFrame()

    print(f"\n  New offers this run:     {new_count}")
    print(f"  Active offers total:     {len(active)}")
    print(f"  Expired offers total:    {len(expired)}")
    print(f"  Master dataset size:     {len(master)}")

    print(f"\n  [BY CATEGORY — ACTIVE]")
    for group_name, categories in PREMIUM_BASKET.items():
        label = group_name.replace("_", " ").upper()
        print(f"\n  [{label}]")
        for cat in categories:
            count = len(active[active["category"] == cat]) if len(active) else 0
            status = f"{count:>5} offers" if count else "    — EMPTY"
            print(f"    {cat:<25} {status}")

    print("=" * 55)


# ============================================================
# AZURE SQL UPLOAD
# ============================================================

CREATE_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'nfj_offers')
CREATE TABLE nfj_offers (
    id                        INT IDENTITY(1,1) PRIMARY KEY,
    reference                 NVARCHAR(200),
    category                  NVARCHAR(50)   NOT NULL,
    job_title                 NVARCHAR(500),
    company                   NVARCHAR(500),
    location                  NVARCHAR(500),
    salary_uop                NVARCHAR(200),
    salary_b2b                NVARCHAR(200),
    skills_required           NVARCHAR(MAX),
    skills_nice_to_have       NVARCHAR(MAX),
    requirements_expected     NVARCHAR(MAX),
    requirements_nice_to_have NVARCHAR(MAX),
    body_html                 NVARCHAR(MAX),
    url                       NVARCHAR(1000) NOT NULL,
    position_level            NVARCHAR(200),
    contract_types            NVARCHAR(200),
    work_mode                 NVARCHAR(200),
    nfj_category              NVARCHAR(200),
    published_at              NVARCHAR(50),
    scraped_at                NVARCHAR(50),
    first_seen_at             DATETIME DEFAULT GETDATE(),
    created_at                DATETIME DEFAULT GETDATE(),
    UNIQUE (url)
);
"""

MERGE_SQL = """
MERGE INTO nfj_offers AS T
USING (SELECT ? as reference, ? as category, ? as job_title, ? as company,
              ? as location, ? as salary_uop, ? as salary_b2b,
              ? as skills_required, ? as skills_nice_to_have,
              ? as requirements_expected, ? as requirements_nice_to_have,
              ? as body_html, ? as url, ? as position_level,
              ? as contract_types, ? as work_mode, ? as nfj_category,
              ? as published_at, ? as scraped_at) AS S
ON T.url = S.url
WHEN MATCHED THEN UPDATE SET
    reference = S.reference, category = S.category, job_title = S.job_title,
    company = S.company, location = S.location, salary_uop = S.salary_uop,
    salary_b2b = S.salary_b2b, skills_required = S.skills_required,
    skills_nice_to_have = S.skills_nice_to_have,
    requirements_expected = S.requirements_expected,
    requirements_nice_to_have = S.requirements_nice_to_have,
    body_html = S.body_html, position_level = S.position_level,
    contract_types = S.contract_types, work_mode = S.work_mode,
    nfj_category = S.nfj_category, published_at = S.published_at,
    scraped_at = S.scraped_at, created_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (reference, category, job_title, company, location, salary_uop, salary_b2b,
     skills_required, skills_nice_to_have, requirements_expected,
     requirements_nice_to_have, body_html, url, position_level,
     contract_types, work_mode, nfj_category, published_at, scraped_at,
     first_seen_at)
    VALUES (S.reference, S.category, S.job_title, S.company, S.location,
            S.salary_uop, S.salary_b2b, S.skills_required,
            S.skills_nice_to_have, S.requirements_expected,
            S.requirements_nice_to_have, S.body_html, S.url,
            S.position_level, S.contract_types, S.work_mode,
            S.nfj_category, S.published_at, S.scraped_at,
            GETDATE());
"""

_SQL_COLUMNS = [
    "reference", "category", "job_title", "company", "location",
    "salary_uop", "salary_b2b", "skills_required", "skills_nice_to_have",
    "requirements_expected", "requirements_nice_to_have", "body_html",
    "url", "position_level", "contract_types", "work_mode",
    "nfj_category", "published_at", "scraped_at",
]


def upload_to_azure_sql(df: pd.DataFrame) -> dict:
    """
    Upload DataFrame to Azure SQL (table nfj_offers).
    Uses MERGE (upsert) by url key — safe for repeated runs.
    Returns dict: {"uploaded": int, "errors": list[str]}
    """
    result = {"uploaded": 0, "errors": []}

    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        msg = "Brak SqlConnectionString w zmiennych środowiskowych (.env)"
        print(f"  [SQL] {msg}")
        result["errors"].append(msg)
        return result

    print(f"\n[SQL] Łączenie z Azure SQL...")

    # Retry logic — Azure SQL serverless może być uśpiony
    max_retries = 3
    conn = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_str, timeout=60)
            print(f"  [SQL] Połączono (próba {attempt}/{max_retries})")
            break
        except pyodbc.Error as e:
            if attempt < max_retries:
                wait = attempt * 15
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

            # Auto-create table
            cursor.execute(CREATE_TABLE_SQL)
            # Migration: add first_seen_at if missing (table may have existed before)
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID('nfj_offers')
                      AND name = 'first_seen_at'
                )
                ALTER TABLE nfj_offers ADD first_seen_at DATETIME DEFAULT GETDATE();
            """)
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID('nfj_offers')
                      AND name = 'created_at'
                )
                ALTER TABLE nfj_offers ADD created_at DATETIME DEFAULT GETDATE();
            """)
            conn.commit()
            print("  [SQL] Tabela nfj_offers — OK")

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
                    err = f"Wiersz {idx} ({row.get('url', '?')}): {e}"
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


# ============================================================
# MAIN
# ============================================================

def run(progress_callback=None) -> dict:
    """
    Main pipeline. Returns a result dict compatible with scraper_monitor.

    Args:
        progress_callback: Optional callback(current, total, phase) for progress tracking.
    """
    all_categories = [c for g in PREMIUM_BASKET.values() for c in g]
    result = {
        "success": False,
        "total_offers": 0,
        "sql_uploaded": 0,
        "categories_ok": [],
        "categories_empty": [],
        "errors": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    run_timestamp = result["timestamp"]

    print(f"\n[*] NoFluffJobs Premium Basket Scraper v6 (Search API + withSalaryMatch)")
    print(f"[*] Run: {run_timestamp}")

    # Load master
    print(f"\n{'─' * 55}")
    print("  LOADING MASTER DATASET")
    print(f"{'─' * 55}")
    master = load_master()

    # Stage 1: Listings
    print(f"\n{'─' * 55}")
    print("  STAGE 1: LISTINGS")
    print(f"{'─' * 55}")
    current_listings = fetch_listings()

    if not current_listings:
        result["errors"].append("API zwróciło 0 ofert we wszystkich kategoriach!")
        return result

    # Reconcile with master
    print(f"\n{'─' * 55}")
    print("  RECONCILIATION")
    print(f"{'─' * 55}")
    new_postings, master = reconcile(master, current_listings)

    # Stage 2: Details — ONLY for new postings
    print(f"\n{'─' * 55}")
    print(f"  STAGE 2: DETAILS ({len(new_postings)} new offers)")
    print(f"{'─' * 55}")
    new_postings = fetch_details(new_postings, progress_callback=progress_callback)

    # Stage 2b: Backfill details for active rows missing body_html
    listing_by_ref = {}
    for p in current_listings:
        ref = p.get("reference") or p.get("id")
        if ref:
            listing_by_ref[ref] = p

    backfill_postings = []
    if not master.empty and "body_html" in master.columns:
        active_no_body = master[
            (master["is_active"] == True) &
            (master["body_html"].isna() | (master["body_html"] == ""))
        ]
        for _, row in active_no_body.iterrows():
            ref = row.get("reference")
            if ref and ref in listing_by_ref:
                backfill_postings.append(listing_by_ref[ref])

    if backfill_postings:
        print(f"\n{'─' * 55}")
        print(f"  STAGE 2b: BACKFILL ({len(backfill_postings)} active offers missing details)")
        print(f"{'─' * 55}")
        backfill_postings = fetch_details(backfill_postings)

        backfilled = 0
        for p in backfill_postings:
            ref = p.get("reference") or p.get("id")
            flat = flatten_posting(p)
            ref_mask = master["reference"] == ref
            if ref_mask.any():
                for col, val in flat.items():
                    if val is not None and col in master.columns:
                        master.loc[ref_mask, col] = val
                backfilled += 1
        print(f"  Backfilled: {backfilled} master rows updated with unified columns")

    # Flatten new postings and append to master
    if new_postings:
        new_rows = []
        for p in new_postings:
            row = flatten_posting(p)
            row["first_seen_at"] = run_timestamp
            row["last_seen_at"] = run_timestamp
            row["created_at"] = run_timestamp
            row["is_active"] = True
            new_rows.append(row)

        new_df = pd.DataFrame(new_rows)
        master = pd.concat([master, new_df], ignore_index=True)

    # Dedup master by reference — keep earliest first_seen_at per reference
    if "reference" in master.columns and len(master) > 0:
        before = len(master)
        master = master.sort_values("first_seen_at").drop_duplicates(
            subset="reference", keep="first"
        ).reset_index(drop=True)
        if before != len(master):
            print(f"  Master dedup: {before} → {len(master)} ({before - len(master)} duplicates removed)")

    # Save master
    save_master(master)

    # Save latest snapshot JSON (for debugging)
    active_count = int(master["is_active"].sum()) if "is_active" in master.columns else len(master)
    snapshot = {
        "run_timestamp": run_timestamp,
        "new_count": len(new_postings),
        "active_count": active_count,
        "total_count": len(master),
    }
    with open(SNAPSHOT_JSON, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    # Build monitor result — check categories
    active = master[master["is_active"] == True] if "is_active" in master.columns else master
    result["total_offers"] = len(active)
    for cat in all_categories:
        if len(active[active["category"] == cat]) > 0:
            result["categories_ok"].append(cat)
        else:
            result["categories_empty"].append(cat)

    result["success"] = True

    # Upload active offers to Azure SQL
    print(f"\n{'─' * 55}")
    print("  AZURE SQL UPLOAD")
    print(f"{'─' * 55}")
    sql_result = upload_to_azure_sql(active)
    result["sql_uploaded"] = sql_result["uploaded"]
    result["errors"].extend(sql_result["errors"])

    # Summary
    print_summary(master, len(new_postings), run_timestamp)

    # Cleanup checkpoint
    Path(CHECKPOINT_FILE).unlink(missing_ok=True)

    print(f"\n[DONE] Pipeline finished.\n")
    return result


def run_sample(n: int) -> None:
    """
    Sample mode: fetch N offers from 1-2 categories, display table, save CSV.
    Does NOT modify master dataset.
    """
    sample_categories = ["data", "marketing"]
    print(f"\n[*] NFJ Sample Mode — fetching up to {n} offers from {sample_categories}")

    all_offers = []
    for cat in sample_categories:
        print(f"  Fetching: {cat}...", end=" ", flush=True)
        offers = fetch_category(cat)
        for o in offers:
            o["scraper_category"] = cat
        all_offers.extend(offers)
        print(f"OK — {len(offers)} offers")
        time.sleep(random.uniform(*LISTING_DELAY))

    # Dedup by reference
    seen: dict[str, dict] = {}
    for p in all_offers:
        ref = p.get("reference") or p.get("id")
        if ref not in seen:
            seen[ref] = p
    unique = list(seen.values())[:n]
    print(f"  Using {len(unique)} unique offers (capped at {n})")

    # Fetch details for each
    print(f"\n  Fetching details...")
    for i, posting in enumerate(unique):
        pid = posting["id"]
        print(f"  [{i + 1}/{len(unique)}] {pid[:60]}...", end=" ")
        try:
            detail = fetch_posting_detail(pid)
            for key in detail:
                posting[key] = detail[key]
            print("OK")
        except Exception as e:
            print(f"FAILED ({e})")
        time.sleep(random.uniform(*DETAIL_DELAY))

    # Flatten
    rows = [flatten_posting(p) for p in unique]
    df = pd.DataFrame(rows)

    # Display
    display_cols = [
        "job_title", "company", "location", "salary_uop", "salary_b2b",
        "work_mode", "contract_types", "position_level", "category",
    ]
    available = [c for c in display_cols if c in df.columns]
    print(f"\n{'=' * 80}")
    print(f"  SAMPLE RESULTS ({len(df)} offers)")
    print(f"{'=' * 80}")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 40)
    print(df[available].to_string(index=False))

    # Check body_html
    has_body = df["body_html"].notna().sum() if "body_html" in df.columns else 0
    print(f"\n  body_html filled: {has_body}/{len(df)}")
    print(f"  salary_uop filled: {df['salary_uop'].notna().sum()}/{len(df)}")
    print(f"  salary_b2b filled: {df['salary_b2b'].notna().sum()}/{len(df)}")

    # Save sample CSV
    sample_file = SCRAPER_DIR / f"nfj_sample_{n}.csv"
    df.to_csv(sample_file, index=False, encoding="utf-8-sig")
    print(f"\n  [OK] Sample saved: {sample_file}")

    # Show all columns
    print(f"\n  Columns: {list(df.columns)}")


def main():
    """Entry point: run scraper with monitoring or sample mode."""
    import argparse

    parser = argparse.ArgumentParser(description="NFJ Scraper")
    parser.add_argument("--sample", type=int, default=None,
                        help="Sample mode: fetch N offers, display table, save CSV (no master update)")
    args = parser.parse_args()

    if args.sample:
        run_sample(args.sample)
        return

    try:
        result = run()
    except Exception as e:
        tb = traceback.format_exc()
        print(f"\n[MONITOR] Scraper rzucił wyjątek:\n{tb}")
        result = {
            "success": False,
            "total_offers": 0,
            "sql_uploaded": 0,
            "categories_ok": [],
            "categories_empty": [],
            "errors": [f"Nieobsłużony wyjątek: {e}"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    try:
        from pracuj_scraper.scraper_monitor import monitor_scraper
        monitor_scraper("NoFluffJobs", result)
    except ImportError:
        print("  [INFO] scraper_monitor niedostepny - pomijam monitoring email")

    # Print final status
    s = result.get("success", False)
    o = result.get("total_offers", 0)
    sql = result.get("sql_uploaded", 0)
    e = len(result.get("errors", []))
    print(f"\n\n=== NFJ FINAL: success={s}, offers={o}, sql={sql}, errors={e} ===")


if __name__ == "__main__":
    main()
