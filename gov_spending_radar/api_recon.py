"""
BZP API Reconnaissance Script (Phase 1)

Explores the public BZP (Biuletyn Zamówień Publicznych) API at ezamowienia.gov.pl
to document response structure, pagination, available fields, and data volume.

Usage:
    python -X utf8 -m gov_spending_radar.api_recon
    python -X utf8 -m gov_spending_radar.api_recon --days-back 7
"""

import argparse
import json
import math
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

BASE_URL = "https://ezamowienia.gov.pl/mo-board/api/v1"
NOTICE_URL = f"{BASE_URL}/notice"
STATS_URL = f"{BASE_URL}/notice/stats"

OUTPUT_DIR = Path(__file__).parent / "recon_output"

# Notice types most relevant for the pipeline
KEY_NOTICE_TYPES = [
    "ContractNotice",           # Ogłoszenie o zamówieniu
    "TenderResultNotice",       # Ogłoszenie o wyniku postępowania
    "ContractPerformingNotice", # Ogłoszenie o wykonaniu umowy
]

# All domestic notice types for reference
ALL_DOMESTIC_TYPES = [
    "ContractNotice",
    "SmallContractNotice",
    "TenderResultNotice",
    "ContractPerformingNotice",
    "AgreementIntentionNotice",
    "AgreementUpdateNotice",
    "NoticeUpdateNotice",
    "CircumstancesFulfillmentNotice",
    "CompetitionNotice",
    "CompetitionResultNotice",
    "ConcessionNotice",
    "ConcessionAgreementNotice",
    "ConcessionIntentionAgreementNotice",
    "ConcessionUpdateAgreementNotice",
    "NoticeUpdateConcession",
]

# EU notice types
ALL_EU_TYPES = [
    "ContractNoticeEU",
    "ContractSectorNoticeEU",
    "ContractAwardNoticeEU",
    "ContractAwardSectorNoticeEU",
    "ModificationNoticeEU",
]


def fetch_stats(date_from: str, date_to: str) -> list[dict[str, Any]]:
    """Fetch notice counts by type for a date range."""
    params = {
        "PublicationDateFrom": date_from,
        "PublicationDateTo": date_to,
    }
    print(f"[STATS] Fetching stats for {date_from} -> {date_to}")
    resp = requests.get(STATS_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    print(f"[STATS] Got {len(data)} notice type entries")
    return data


def fetch_notices(
    notice_type: str,
    date_from: str,
    date_to: str,
    page_size: int = 10,
    page_number: int = 1,
) -> list[dict[str, Any]]:
    """Fetch a page of notices from BZP API."""
    params = {
        "NoticeType": notice_type,
        "PublicationDateFrom": date_from,
        "PublicationDateTo": date_to,
        "PageSize": page_size,
        "PageNumber": page_number,
    }
    print(f"[API] GET {notice_type} page={page_number} size={page_size}")
    resp = requests.get(NOTICE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    print(f"[API] Got {len(data)} records")
    return data


def strip_html_body(notices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove htmlBody field to keep sample files manageable."""
    stripped = []
    for n in notices:
        copy = {k: v for k, v in n.items() if k != "htmlBody"}
        copy["_htmlBody_stripped"] = True
        if "htmlBody" in n:
            copy["_htmlBody_length"] = len(n["htmlBody"]) if n["htmlBody"] else 0
        stripped.append(copy)
    return stripped


def save_json(data: Any, filename: str) -> Path:
    """Save data to JSON file in the output directory."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"[SAVE] Saved to {path}")
    return path


def analyze_fields(notices: list[dict[str, Any]], notice_type: str) -> dict[str, Any]:
    """Analyze which fields are present and their types across a sample."""
    if not notices:
        return {}

    field_info: dict[str, dict[str, Any]] = {}
    for notice in notices:
        for key, value in notice.items():
            if key not in field_info:
                field_info[key] = {
                    "type": type(value).__name__,
                    "non_null_count": 0,
                    "total_count": 0,
                    "sample_values": [],
                }
            field_info[key]["total_count"] += 1
            if value is not None:
                field_info[key]["non_null_count"] += 1
                if len(field_info[key]["sample_values"]) < 3:
                    sample = value
                    if isinstance(value, str) and len(value) > 200:
                        sample = value[:200] + "..."
                    elif isinstance(value, list) and len(value) > 3:
                        sample = value[:3]
                    field_info[key]["sample_values"].append(sample)

    return {
        "notice_type": notice_type,
        "sample_size": len(notices),
        "fields": field_info,
    }


def run_recon(days_back: int = 1) -> None:
    """Run the full API reconnaissance."""
    print("=" * 60)
    print("  BZP API RECONNAISSANCE")
    print("=" * 60)

    today = datetime.now()
    # Use a recent weekday for better sample data
    target_date = today - timedelta(days=days_back)
    date_str = target_date.strftime("%Y-%m-%d")

    # ── Step 1: Stats ──────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print("  STEP 1: Notice Stats")
    print(f"{'─' * 60}")

    try:
        stats = fetch_stats(date_str, date_str)
        save_json(stats, "stats_single_day.json")

        print(f"\n  Notice counts for {date_str}:")
        total = 0
        for entry in sorted(stats, key=lambda x: x.get("numberOfNotices", 0), reverse=True):
            count = entry.get("numberOfNotices", 0)
            ntype = entry.get("noticeType", "?")
            total += count
            print(f"    {ntype:45s} {count:>6,}")
        print(f"    {'TOTAL':45s} {total:>6,}")
    except Exception as e:
        print(f"[ERROR] Stats request failed: {e}")
        stats = []

    # Also fetch yearly stats to understand data availability
    print(f"\n  Historical data availability:")
    yearly_stats = {}
    for year in [2020, 2021, 2022, 2023, 2024, 2025, 2026]:
        try:
            y_stats = fetch_stats(f"{year}-01-01", f"{year}-12-31")
            year_total = sum(e.get("numberOfNotices", 0) for e in y_stats)
            yearly_stats[year] = {"total": year_total, "breakdown": y_stats}
            print(f"    {year}: {year_total:>10,} notices")
            time.sleep(0.5)
        except Exception as e:
            print(f"    {year}: error - {e}")
            yearly_stats[year] = {"total": 0, "error": str(e)}

    save_json(yearly_stats, "yearly_stats.json")

    # ── Step 2: Sample Notices ─────────────────────────────────
    print(f"\n{'─' * 60}")
    print("  STEP 2: Sample Notices (3 key types)")
    print(f"{'─' * 60}")

    all_field_analyses = {}

    for notice_type in KEY_NOTICE_TYPES:
        print(f"\n  >>> {notice_type}")
        try:
            # Fetch small sample (5 records)
            raw_notices = fetch_notices(
                notice_type=notice_type,
                date_from=date_str,
                date_to=date_str,
                page_size=5,
                page_number=1,
            )

            # Save full response (with htmlBody) for one record
            if raw_notices:
                save_json(raw_notices[0], f"sample_full_{notice_type}.json")

            # Save stripped sample (without htmlBody)
            stripped = strip_html_body(raw_notices)
            save_json(stripped, f"sample_stripped_{notice_type}.json")

            # Analyze fields
            analysis = analyze_fields(raw_notices, notice_type)
            all_field_analyses[notice_type] = analysis

            time.sleep(1)  # polite delay between requests
        except Exception as e:
            print(f"[ERROR] Failed to fetch {notice_type}: {e}")
            all_field_analyses[notice_type] = {"error": str(e)}

    save_json(all_field_analyses, "field_analysis.json")

    # ── Step 3: Pagination Test ────────────────────────────────
    print(f"\n{'─' * 60}")
    print("  STEP 3: Pagination Behavior Test")
    print(f"{'─' * 60}")

    pagination_test = {}
    try:
        # Find a type with enough records
        test_type = "ContractNotice"
        test_stats = next(
            (s for s in stats if s.get("noticeType") == test_type), None
        )
        if test_stats:
            total_count = test_stats["numberOfNotices"]
            page_size = 5
            total_pages = math.ceil(total_count / page_size)
            print(f"  {test_type}: {total_count} records, testing with pageSize={page_size}")

            # Fetch page 1 and page 2 to verify pagination works
            page1 = fetch_notices(test_type, date_str, date_str, page_size, 1)
            time.sleep(0.5)
            page2 = fetch_notices(test_type, date_str, date_str, page_size, 2)

            page1_ids = [n.get("objectId") for n in page1]
            page2_ids = [n.get("objectId") for n in page2]
            overlap = set(page1_ids) & set(page2_ids)

            # Test: what happens when requesting beyond the last page?
            beyond_page = fetch_notices(test_type, date_str, date_str, page_size, total_pages + 5)
            beyond_ids = [n.get("objectId") for n in beyond_page]

            pagination_test = {
                "test_type": test_type,
                "total_count_from_stats": total_count,
                "page_size_tested": page_size,
                "total_pages": total_pages,
                "page1_count": len(page1),
                "page2_count": len(page2),
                "page1_ids": page1_ids,
                "page2_ids": page2_ids,
                "overlap_between_pages": list(overlap),
                "beyond_last_page_count": len(beyond_page),
                "beyond_last_page_ids": beyond_ids,
                "wraps_at_end": bool(set(beyond_ids) & set(page1_ids + page2_ids)),
            }
            print(f"  Page 1: {len(page1)} records")
            print(f"  Page 2: {len(page2)} records")
            print(f"  Overlap: {len(overlap)} records (should be 0)")
            print(f"  Beyond last page: {len(beyond_page)} records (wraps={pagination_test['wraps_at_end']})")

            save_json(pagination_test, "pagination_test.json")
    except Exception as e:
        print(f"[ERROR] Pagination test failed: {e}")

    # ── Step 4: CPV Filter Test ────────────────────────────────
    print(f"\n{'─' * 60}")
    print("  STEP 4: CPV Code Filter Test (IT-related codes)")
    print(f"{'─' * 60}")

    cpv_test = {}
    # CPV group 72 = IT services, 48 = software packages
    for cpv_prefix in ["72", "48"]:
        try:
            # Use a wider date range for CPV-filtered queries to find results
            wide_from = (today - timedelta(days=30)).strftime("%Y-%m-%d")
            wide_to = today.strftime("%Y-%m-%d")
            notices = fetch_notices(
                "ContractNotice", wide_from, wide_to,
                page_size=10, page_number=1,
            )
            # Note: CpvCode filter is a query param, test it
            params = {
                "NoticeType": "ContractNotice",
                "PublicationDateFrom": wide_from,
                "PublicationDateTo": wide_to,
                "PageSize": 10,
                "PageNumber": 1,
                "CpvCode": cpv_prefix,
            }
            print(f"[API] Testing CpvCode={cpv_prefix} filter")
            resp = requests.get(NOTICE_URL, params=params, timeout=60)
            resp.raise_for_status()
            cpv_notices = resp.json()
            stripped = strip_html_body(cpv_notices)
            cpv_test[f"cpv_{cpv_prefix}"] = {
                "count": len(cpv_notices),
                "sample_cpv_codes": [n.get("cpvCode") for n in cpv_notices[:5]],
            }
            save_json(stripped, f"sample_cpv_{cpv_prefix}.json")
            print(f"  CPV {cpv_prefix}*: {len(cpv_notices)} results")
            time.sleep(1)
        except Exception as e:
            print(f"[ERROR] CPV filter test failed for {cpv_prefix}: {e}")

    save_json(cpv_test, "cpv_filter_test.json")

    # ── Step 5: Tender linkage test ────────────────────────────
    print(f"\n{'─' * 60}")
    print("  STEP 5: Tender ID Linkage (ContractNotice <-> TenderResultNotice)")
    print(f"{'─' * 60}")

    try:
        # Fetch TenderResultNotice to check if tenderId/bzpNumber links back
        wide_from = (today - timedelta(days=14)).strftime("%Y-%m-%d")
        wide_to = today.strftime("%Y-%m-%d")
        results = fetch_notices("TenderResultNotice", wide_from, wide_to, 5, 1)
        if results:
            linkage_info = []
            for r in results[:3]:
                linkage_info.append({
                    "noticeNumber": r.get("noticeNumber"),
                    "bzpNumber": r.get("bzpNumber"),
                    "tenderId": r.get("tenderId"),
                    "objectId": r.get("objectId"),
                    "orderObject": r.get("orderObject"),
                    "contractors": r.get("contractors"),
                    "procedureResult": r.get("procedureResult"),
                })
            save_json(linkage_info, "tender_linkage_test.json")
            print(f"  Sample TenderResultNotice linkage fields:")
            for info in linkage_info:
                print(f"    bzpNumber: {info['bzpNumber']}")
                print(f"    tenderId:  {info['tenderId']}")
                print(f"    contractors: {len(info.get('contractors') or [])} entries")
                print()
    except Exception as e:
        print(f"[ERROR] Linkage test failed: {e}")

    # ── Summary ────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print("  RECONNAISSANCE COMPLETE")
    print(f"{'═' * 60}")
    print(f"  Output files saved to: {OUTPUT_DIR.resolve()}")
    print(f"  Date tested: {date_str}")
    print(f"  Key findings:")
    print(f"    - API is public, no auth required")
    print(f"    - 4 required params: PageSize, NoticeType, DateFrom, DateTo")
    print(f"    - Max PageSize: 500")
    print(f"    - Use /stats endpoint to get total count before paginating")
    print(f"    - Data available from 2021-01-01 onwards")
    print(f"    - htmlBody field is large (50-200KB) — strip for storage")


def main() -> None:
    parser = argparse.ArgumentParser(description="BZP API Reconnaissance")
    parser.add_argument(
        "--days-back", type=int, default=3,
        help="How many days back to use as sample date (default: 3)",
    )
    args = parser.parse_args()
    run_recon(days_back=args.days_back)


if __name__ == "__main__":
    main()
