"""
BZP API client — fetches procurement notices from ezamowienia.gov.pl.
=====================================================================
Handles the broken PageNumber parameter by splitting date ranges into
time windows and deduplicating by objectId.

Key constraints discovered in Phase 1 recon:
  - PageNumber is ignored by the API (always returns same records)
  - Max PageSize = 500
  - PublicationDateFrom/To filtering is imprecise (slight overlap)
  - Solution: time-window splitting + dedup by objectId
"""

import re
import sys
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

BASE_URL = "https://ezamowienia.gov.pl/mo-board/api/v1/notice"

# Notice types we collect (ContractPerformingNotice skipped per Phase 1 decision)
NOTICE_TYPES = ["ContractNotice", "TenderResultNotice"]

# Max records per API call (API hard limit)
MAX_PAGE_SIZE = 500

# Time window for splitting days (hours).
# API time filtering is imprecise — returns similar results regardless of hour range.
# 6h gives 4 windows/day/type = 8 requests total. Captures ~95%+ of daily records.
# Finer windows (1h) add <2% more records at 10x more requests — not worth it.
DEFAULT_WINDOW_HOURS = 6

# Anti-bot delay between API calls (seconds)
MIN_DELAY = 0.5
MAX_DELAY = 1.5

# Request timeout (seconds)
REQUEST_TIMEOUT = 30

# Max retries per HTTP request
MAX_HTTP_RETRIES = 3


def _normalize_nip(raw_nip: str | None) -> str | None:
    """
    Normalize NIP (Polish tax ID).
    Handles: plain digits, dashes, "NIP: 123...", "NIP:123...", "REGON:123...".
    Returns 10-digit string when possible, or raw value as fallback.
    Returns None if input is empty.
    """
    if not raw_nip:
        return None
    # Strip common prefixes
    cleaned = raw_nip.strip()
    for prefix in ("NIP:", "NIP ", "REGON:", "REGON "):
        if cleaned.upper().startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break
    # If contains semicolon or comma (multiple IDs), take first
    for sep in (";", ","):
        if sep in cleaned:
            cleaned = cleaned.split(sep)[0].strip()
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) == 9:
        digits = "0" + digits  # leading zero stripped by source
    if len(digits) == 10:
        return digits
    # Can't normalize — return raw (truncated to 50 chars for SQL)
    return raw_nip[:50]


def _parse_cpv_code(cpv_raw: str | None) -> str | None:
    """Extract first CPV code (e.g., '72260000-5') from raw API string."""
    if not cpv_raw:
        return None
    match = re.match(r"(\d{8}-\d)", cpv_raw)
    return match.group(1) if match else None


def _parse_iso_datetime(dt_str: str | None) -> str | None:
    """Parse ISO datetime string, return as ISO format or None."""
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, AttributeError):
        return dt_str


def _transform_notice(raw: dict[str, Any]) -> dict[str, Any]:
    """Transform raw API notice into SQL-ready dict matching NOTICES_SQL_COLUMNS."""
    return {
        "object_id": raw.get("objectId"),
        "notice_number": raw.get("noticeNumber"),
        "bzp_number": raw.get("bzpNumber"),
        "tender_id": raw.get("tenderId"),
        "notice_type": raw.get("noticeType"),
        "title": raw.get("orderObject"),
        "cpv_code": _parse_cpv_code(raw.get("cpvCode")),
        "cpv_raw": raw.get("cpvCode"),
        "order_type": raw.get("orderType"),
        "publication_date": _parse_iso_datetime(raw.get("publicationDate")),
        "deadline_date": _parse_iso_datetime(raw.get("submittingOffersDate")),
        "procedure_result": raw.get("procedureResult"),
        "is_below_eu_threshold": raw.get("isTenderAmountBelowEU", False),
        "client_type": raw.get("clientType"),
        "tender_type": raw.get("tenderType"),
        "buyer_name": raw.get("organizationName"),
        "buyer_city": raw.get("organizationCity"),
        "buyer_province": raw.get("organizationProvince"),
        "buyer_country": raw.get("organizationCountry"),
        "buyer_nip": _normalize_nip(raw.get("organizationNationalId")),
        "buyer_org_id": raw.get("organizationId"),
    }


def _transform_contractors(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Flatten contractors array from TenderResultNotice into SQL-ready dicts.
    Splits semicolon-separated procedureResult into per-part results.
    Returns empty list for ContractNotice (no contractors).
    """
    contractors_raw = raw.get("contractors")
    if not contractors_raw:
        return []

    object_id = raw.get("objectId")
    procedure_result = raw.get("procedureResult") or ""
    part_results = procedure_result.split(";") if procedure_result else []

    result = []
    for i, contractor in enumerate(contractors_raw):
        if not contractor:
            continue
        # Skip fully-null contractor entries (unieważnione parts)
        if all(v is None for v in contractor.values()):
            continue
        result.append({
            "notice_object_id": object_id,
            "contractor_name": contractor.get("contractorName"),
            "contractor_city": contractor.get("contractorCity"),
            "contractor_province": contractor.get("contractorProvince"),
            "contractor_country": contractor.get("contractorCountry"),
            "contractor_nip": _normalize_nip(contractor.get("contractorNationalId")),
            "part_index": i,
            "part_result": part_results[i] if i < len(part_results) else None,
        })
    return result


def _fetch_page(
    notice_type: str,
    date_from: str,
    date_to: str,
    page_size: int = MAX_PAGE_SIZE,
) -> list[dict]:
    """
    Fetch a single page of notices from BZP API.
    Returns list of raw notice dicts (without htmlBody).
    Retries up to MAX_HTTP_RETRIES times on failure.
    """
    params = {
        "NoticeType": notice_type,
        "PublicationDateFrom": date_from,
        "PublicationDateTo": date_to,
        "PageSize": page_size,
    }

    for attempt in range(MAX_HTTP_RETRIES):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            notices = data if isinstance(data, list) else data.get("notices", [])
            # Strip htmlBody to save memory
            for n in notices:
                n.pop("htmlBody", None)
            return notices
        except requests.RequestException as e:
            if attempt < MAX_HTTP_RETRIES - 1:
                wait = (attempt + 1) * 3
                print(f"  [API] Błąd HTTP (próba {attempt + 1}/{MAX_HTTP_RETRIES}): {e}, "
                      f"czekam {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [API] Nie udało się pobrać danych po {MAX_HTTP_RETRIES} próbach: {e}")
                return []


def fetch_notices_for_date_range(
    date_from: datetime,
    date_to: datetime,
    notice_types: list[str] | None = None,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> tuple[list[dict], list[dict]]:
    """
    Fetch all notices for a date range using time-window splitting + dedup.

    Args:
        date_from: Start of range (UTC)
        date_to: End of range (UTC)
        notice_types: List of notice types to fetch (default: NOTICE_TYPES)
        window_hours: Size of time windows in hours

    Returns:
        Tuple of (notice_records, contractor_records) — SQL-ready dicts
    """
    if notice_types is None:
        notice_types = NOTICE_TYPES

    seen_ids: set[str] = set()
    all_notices: list[dict] = []
    all_contractors: list[dict] = []

    for ntype in notice_types:
        print(f"\n[BZP] Pobieram {ntype}: {date_from:%Y-%m-%d %H:%M} → {date_to:%Y-%m-%d %H:%M}")

        window_start = date_from
        type_count = 0
        type_dupes = 0
        type_capped = 0

        while window_start < date_to:
            window_end = min(window_start + timedelta(hours=window_hours), date_to)

            dt_from = window_start.strftime("%Y-%m-%dT%H:%M:%S")
            dt_to = window_end.strftime("%Y-%m-%dT%H:%M:%S")

            raw_notices = _fetch_page(ntype, dt_from, dt_to)
            window_new = 0

            for raw in raw_notices:
                oid = raw.get("objectId")
                if not oid or oid in seen_ids:
                    type_dupes += 1
                    continue
                seen_ids.add(oid)

                notice = _transform_notice(raw)
                all_notices.append(notice)

                contractors = _transform_contractors(raw)
                all_contractors.extend(contractors)
                window_new += 1

            if len(raw_notices) >= MAX_PAGE_SIZE:
                type_capped += 1

            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            type_count += window_new
            window_start = window_end

        print(f"  [BZP] {ntype}: {type_count} nowych, {type_dupes} duplikatów"
              + (f", {type_capped} okien z limitem 500" if type_capped else ""))

    print(f"\n[BZP] Łącznie: {len(all_notices)} ogłoszeń, {len(all_contractors)} wykonawców")
    return all_notices, all_contractors


def fetch_daily(
    target_date: datetime | None = None,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> tuple[list[dict], list[dict]]:
    """
    Fetch all notices published on a single day (default: yesterday).

    Returns:
        Tuple of (notice_records, contractor_records) — SQL-ready dicts
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc) - timedelta(days=1)

    date_from = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    date_to = date_from + timedelta(days=1)

    return fetch_notices_for_date_range(date_from, date_to, window_hours=window_hours)


def fetch_backfill(
    days_back: int,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> tuple[list[dict], list[dict]]:
    """
    Fetch notices for the last N days (backfill mode).

    Args:
        days_back: Number of days to go back from today
        window_hours: Time window size for splitting

    Returns:
        Tuple of (notice_records, contractor_records) — SQL-ready dicts
    """
    now = datetime.now(timezone.utc)
    date_to = now.replace(hour=0, minute=0, second=0, microsecond=0)
    date_from = date_to - timedelta(days=days_back)

    print(f"[BZP] Backfill: {days_back} dni ({date_from:%Y-%m-%d} → {date_to:%Y-%m-%d})")
    return fetch_notices_for_date_range(date_from, date_to, window_hours=window_hours)
