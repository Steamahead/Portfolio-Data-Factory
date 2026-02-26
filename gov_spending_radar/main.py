"""
Gov Spending Radar — orchestrator + CLI entry point.
=====================================================
Collects Polish public procurement data from BZP (Biuletyn Zamówień Publicznych)
and uploads to Azure SQL. Classifies notices by sector using CPV codes and title keywords.

Usage (from project root):
    python -X utf8 -m gov_spending_radar.main                 # yesterday's notices
    python -X utf8 -m gov_spending_radar.main --backfill 30   # last 30 days
    python -X utf8 -m gov_spending_radar.main --classify       # classify untagged notices
    python -X utf8 -m gov_spending_radar.main --sample 5       # dry-run: fetch 1 day, no SQL
    python -X utf8 -m gov_spending_radar.main --date 2026-02-20  # specific date
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from .collectors.bzp_client import fetch_daily, fetch_backfill, fetch_notices_for_date_range
from .db.operations import (
    upload_notices,
    upload_contractors,
    upload_classifications,
    fetch_unclassified_notices,
)

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def _load_config() -> dict:
    """Load config.yaml with CPV sector mappings."""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── CPV + Keyword Classifier ────────────────────────────────────

def classify_notice(
    title: str,
    cpv_code: str | None,
    cpv_raw: str | None,
    config: dict,
) -> dict | None:
    """
    Classify a notice by sector using CPV prefix matching + title keywords.

    Returns:
        Dict matching CLASSIFICATIONS_SQL_COLUMNS or None if no match.
    """
    cpv_sectors = config.get("cpv_sectors", {})
    priority = config.get("sector_priority", list(cpv_sectors.keys()))

    matches: dict[str, float] = {}  # sector -> confidence

    # 1. CPV prefix matching (high confidence)
    all_cpv_codes = []
    if cpv_raw:
        import re
        all_cpv_codes = re.findall(r"(\d{8}-\d)", cpv_raw)

    for sector_name in priority:
        sector_cfg = cpv_sectors.get(sector_name, {})
        prefixes = sector_cfg.get("prefixes", [])

        for code in all_cpv_codes:
            code_digits = code.replace("-", "")
            for prefix in prefixes:
                if code_digits.startswith(prefix):
                    matches[sector_name] = max(matches.get(sector_name, 0), 0.85)
                    break

    # 2. Title keyword matching (medium confidence)
    title_lower = (title or "").lower()
    for sector_name in priority:
        sector_cfg = cpv_sectors.get(sector_name, {})
        keywords = sector_cfg.get("keywords_pl", [])

        for kw in keywords:
            if kw.lower() in title_lower:
                matches[sector_name] = max(matches.get(sector_name, 0), 0.65)
                break

    if not matches:
        return None

    # Pick highest-priority sector among matches
    for sector_name in priority:
        if sector_name in matches:
            return {
                "sector": sector_name,
                "confidence": matches[sector_name],
                "method": "cpv_keyword",
            }

    return None


def classify_batch(notices: list[dict], config: dict) -> list[dict]:
    """
    Classify a batch of notices. Returns classification records ready for SQL.
    """
    results = []
    for notice in notices:
        cls = classify_notice(
            title=notice.get("title", ""),
            cpv_code=notice.get("cpv_code"),
            cpv_raw=notice.get("cpv_raw"),
            config=config,
        )
        if cls:
            results.append({
                "notice_object_id": notice.get("object_id"),
                "method": cls["method"],
                "sector": cls["sector"],
                "confidence": cls["confidence"],
                "raw_response": None,
            })
    return results


# ── Run modes ────────────────────────────────────────────────────

def run(
    mode: str = "daily",
    days_back: int = 1,
    target_date: datetime | None = None,
    sample: int | None = None,
    window_hours: int = 6,
) -> dict:
    """
    Main entry point (also callable from Azure Function).

    Args:
        mode: "daily" | "backfill" | "classify"
        days_back: Number of days for backfill mode
        target_date: Specific date for daily mode
        sample: If set, fetch but don't upload (dry-run)
        window_hours: Time window size for API pagination

    Returns:
        {"success": bool, "notices_uploaded": int, "contractors_uploaded": int,
         "classifications_uploaded": int, "errors": list[str]}
    """
    config = _load_config()
    result = {
        "success": False,
        "notices_uploaded": 0,
        "contractors_uploaded": 0,
        "classifications_uploaded": 0,
        "errors": [],
    }

    start_time = time.time()

    try:
        if mode == "classify":
            return _run_classify(config)

        # ── Fetch from API ──
        if mode == "backfill":
            notices, contractors = fetch_backfill(days_back, window_hours=window_hours)
        else:
            notices, contractors = fetch_daily(target_date, window_hours=window_hours)

        if not notices:
            print("\n[GOV] Brak ogłoszeń do przetworzenia")
            result["success"] = True
            return result

        # ── Classify ──
        print(f"\n[GOV] Klasyfikacja {len(notices)} ogłoszeń (CPV + keyword)...")
        classifications = classify_batch(notices, config)
        classified_count = len(classifications)
        total = len(notices)
        print(f"  [GOV] Sklasyfikowano: {classified_count}/{total} "
              f"({classified_count * 100 // total}%)")

        # ── Sample mode: print stats and exit ──
        if sample is not None:
            _print_sample_stats(notices, contractors, classifications, sample)
            result["success"] = True
            return result

        # ── Upload to SQL ──
        n_result = upload_notices(notices)
        result["notices_uploaded"] = n_result["uploaded"]
        result["errors"].extend(n_result["errors"])

        c_result = upload_contractors(contractors)
        result["contractors_uploaded"] = c_result["uploaded"]
        result["errors"].extend(c_result["errors"])

        if classifications:
            cl_result = upload_classifications(classifications)
            result["classifications_uploaded"] = cl_result["uploaded"]
            result["errors"].extend(cl_result["errors"])

        result["success"] = len(result["errors"]) == 0

    except Exception as e:
        msg = f"Pipeline error: {e}"
        print(f"\n[GOV] {msg}")
        result["errors"].append(msg)

    elapsed = time.time() - start_time
    print(f"\n[GOV] Zakończono w {elapsed:.1f}s — "
          f"ogłoszenia: {result['notices_uploaded']}, "
          f"wykonawcy: {result['contractors_uploaded']}, "
          f"klasyfikacje: {result['classifications_uploaded']}, "
          f"błędy: {len(result['errors'])}")

    return result


def _run_classify(config: dict) -> dict:
    """Re-classify notices that have no classification yet."""
    result = {
        "success": False,
        "notices_uploaded": 0,
        "contractors_uploaded": 0,
        "classifications_uploaded": 0,
        "errors": [],
    }

    print("\n[GOV] Tryb --classify: pobieranie niesklasyfikowanych ogłoszeń...")
    unclassified = fetch_unclassified_notices()

    if not unclassified:
        print("  [GOV] Brak niesklasyfikowanych ogłoszeń")
        result["success"] = True
        return result

    print(f"  [GOV] Znaleziono {len(unclassified)} niesklasyfikowanych ogłoszeń")
    classifications = classify_batch(unclassified, config)
    print(f"  [GOV] Sklasyfikowano: {len(classifications)}/{len(unclassified)}")

    if classifications:
        cl_result = upload_classifications(classifications)
        result["classifications_uploaded"] = cl_result["uploaded"]
        result["errors"].extend(cl_result["errors"])

    result["success"] = len(result["errors"]) == 0
    return result


def _print_sample_stats(
    notices: list[dict],
    contractors: list[dict],
    classifications: list[dict],
    limit: int,
) -> None:
    """Print sample data for dry-run mode."""
    print(f"\n{'='*60}")
    print(f"SAMPLE MODE — bez uploadu do SQL")
    print(f"{'='*60}")
    print(f"Ogłoszenia: {len(notices)}")
    print(f"Wykonawcy:  {len(contractors)}")
    print(f"Klasyfikacje: {len(classifications)}")

    # Type breakdown
    types = {}
    for n in notices:
        t = n.get("notice_type", "?")
        types[t] = types.get(t, 0) + 1
    print(f"\nTypy ogłoszeń:")
    for t, count in sorted(types.items()):
        print(f"  {t}: {count}")

    # Sector breakdown
    sectors = {}
    for c in classifications:
        s = c.get("sector", "?")
        sectors[s] = sectors.get(s, 0) + 1
    if sectors:
        print(f"\nSektory (klasyfikacja):")
        for s, count in sorted(sectors.items(), key=lambda x: -x[1]):
            print(f"  {s}: {count}")

    # Sample notices
    print(f"\nPrzykładowe ogłoszenia (max {limit}):")
    for n in notices[:limit]:
        print(f"  [{n.get('notice_type', '?')[:2]}] {n.get('title', '?')[:80]}")
        print(f"       CPV: {n.get('cpv_code', '-')} | {n.get('buyer_name', '?')[:50]}")
        print(f"       NIP: {n.get('buyer_nip', '-')} | {n.get('buyer_province', '-')}")

    # Sample contractors
    if contractors:
        print(f"\nPrzykładowi wykonawcy (max {limit}):")
        for c in contractors[:limit]:
            print(f"  {c.get('contractor_name', '?')[:60]} "
                  f"[NIP: {c.get('contractor_nip', '-')}] → {c.get('part_result', '-')}")

    # Sample classifications
    if classifications:
        print(f"\nPrzykładowe klasyfikacje (max {limit}):")
        for cl in classifications[:limit]:
            print(f"  {cl.get('sector', '?')} (confidence: {cl.get('confidence', 0):.2f}) "
                  f"— {cl.get('notice_object_id', '?')[:30]}...")


# ── CLI ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Gov Spending Radar — Polish public procurement pipeline"
    )
    parser.add_argument(
        "--backfill", type=int, metavar="DAYS",
        help="Backfill last N days (max 730, API has data from 2021)"
    )
    parser.add_argument(
        "--date", type=str, metavar="YYYY-MM-DD",
        help="Fetch specific date (default: yesterday)"
    )
    parser.add_argument(
        "--classify", action="store_true",
        help="Re-classify notices that have no classification"
    )
    parser.add_argument(
        "--sample", type=int, metavar="N",
        help="Dry-run: fetch 1 day, print N sample records, no SQL upload"
    )
    parser.add_argument(
        "--window-hours", type=int, default=6,
        help="Time window size for API pagination (default: 6)"
    )

    args = parser.parse_args()

    if args.classify:
        result = run(mode="classify")
    elif args.backfill:
        if args.backfill > 730:
            print("[GOV] Max backfill: 730 dni (API limit)")
            args.backfill = 730
        result = run(mode="backfill", days_back=args.backfill, window_hours=args.window_hours)
    elif args.date:
        try:
            target = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"[GOV] Nieprawidłowy format daty: {args.date} (oczekiwany: YYYY-MM-DD)")
            sys.exit(1)
        result = run(mode="daily", target_date=target, sample=args.sample,
                     window_hours=args.window_hours)
    else:
        result = run(mode="daily", sample=args.sample, window_hours=args.window_hours)

    if not result.get("success"):
        sys.exit(1)


if __name__ == "__main__":
    main()
