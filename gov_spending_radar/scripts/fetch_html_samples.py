"""
Krok 1: Pobierz sample HTML-ow z BZP API (z pelnym htmlBody).
=================================================================
Pobiera po 5 ogloszen ContractNotice i TenderResultNotice,
plus 2-3 ogloszenia IT/Cyber (CPV 72* lub keyword).
Dodatkowo pobiera 2-3 STARSZE ogloszenia (luty 2026) dla porownania struktury.

Zapisuje kazdy HTML do gov_spending_radar/recon_html/.
Wypisuje metadane + rozmiar htmlBody.

Uzycie:
    .venv\\Scripts\\python.exe -X utf8 -m gov_spending_radar.scripts.fetch_html_samples
"""

import json
import sys
import time
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

BASE_URL = "https://ezamowienia.gov.pl/mo-board/api/v1/notice"
RECON_DIR = Path(__file__).resolve().parent.parent / "recon_html"
RECON_DIR.mkdir(exist_ok=True)

REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
RETRY_BASE_DELAY = 5  # seconds


def fetch_one_by_one(notice_type: str, date_from: str, date_to: str,
                     count: int = 5) -> list[dict]:
    """
    Fetch notices one at a time to avoid 503 on large payloads with htmlBody.
    Uses PageSize=count but with aggressive retry + longer timeouts.
    Falls back to PageSize=1 loop if bulk fails.
    """
    # Try bulk first
    params = {
        "NoticeType": notice_type,
        "PublicationDateFrom": date_from,
        "PublicationDateTo": date_to,
        "PageSize": count,
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            notices = data if isinstance(data, list) else data.get("notices", [])
            return notices[:count]
        except requests.RequestException as e:
            wait = RETRY_BASE_DELAY * (attempt + 1)
            print(f"  Retry {attempt+1}/{MAX_RETRIES}: {e}, waiting {wait}s...")
            time.sleep(wait)

    # Fallback: fetch PageSize=1, shift time windows to get different records
    print(f"  Bulk failed, falling back to single-record fetches...")
    collected = []
    seen_ids = set()
    # Split date range into small windows
    dt_from = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
    dt_to = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
    total_hours = (dt_to - dt_from).total_seconds() / 3600
    window_hours = max(1, total_hours / (count * 2))

    cursor = dt_from
    while cursor < dt_to and len(collected) < count:
        w_end = min(cursor + timedelta(hours=window_hours), dt_to)
        params = {
            "NoticeType": notice_type,
            "PublicationDateFrom": cursor.strftime("%Y-%m-%dT%H:%M:%S"),
            "PublicationDateTo": w_end.strftime("%Y-%m-%dT%H:%M:%S"),
            "PageSize": 1,
        }
        try:
            resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            notices = data if isinstance(data, list) else data.get("notices", [])
            for n in notices:
                oid = n.get("objectId")
                if oid and oid not in seen_ids:
                    seen_ids.add(oid)
                    collected.append(n)
        except requests.RequestException as e:
            print(f"  Window fetch failed: {e}")
        cursor = w_end
        time.sleep(random.uniform(1.5, 3))

    return collected[:count]


def save_html(notice: dict, prefix: str, index: int) -> dict | None:
    """Save htmlBody to file, return metadata summary."""
    html_body = notice.get("htmlBody")
    object_id = notice.get("objectId", "unknown")
    title = notice.get("orderObject", "")[:80]
    cpv = notice.get("cpvCode", "")
    pub_date = notice.get("publicationDate", "")[:10]

    if not html_body:
        print(f"  [{prefix}_{index}] NO htmlBody! objectId={object_id}")
        return None

    html_size_kb = len(html_body.encode("utf-8")) / 1024

    filename = f"{prefix}_{index}.html"
    filepath = RECON_DIR / filename
    filepath.write_text(html_body, encoding="utf-8")

    meta = {
        "objectId": object_id,
        "noticeType": notice.get("noticeType"),
        "title": title,
        "cpvCode": cpv,
        "publicationDate": pub_date,
        "htmlBody_size_kb": round(html_size_kb, 1),
        "orderType": notice.get("orderType"),
        "procedureResult": notice.get("procedureResult"),
        "has_contractors": bool(notice.get("contractors")),
        "contractors_count": len(notice.get("contractors", []) or []),
    }

    meta_path = RECON_DIR / f"{prefix}_{index}_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"  [{filename}] {html_size_kb:.1f} KB | CPV: {cpv[:15]} | {title}")
    return meta


def main():
    now = datetime.now(timezone.utc)
    # Use last work week (Mon-Fri)
    recent_to = now.replace(hour=0, minute=0, second=0, microsecond=0)
    recent_from = recent_to - timedelta(days=5)

    # Old date range (February 2026 — structure stability check)
    old_from = datetime(2026, 2, 10, tzinfo=timezone.utc)
    old_to = datetime(2026, 2, 12, tzinfo=timezone.utc)

    all_meta = []

    # ── 1. ContractNotice (recent) — 5 samples ──
    print("\n" + "=" * 60)
    print("1. ContractNotice (recent)")
    print("=" * 60)
    notices = fetch_one_by_one(
        "ContractNotice",
        recent_from.strftime("%Y-%m-%dT%H:%M:%S"),
        recent_to.strftime("%Y-%m-%dT%H:%M:%S"),
        count=5,
    )
    for i, n in enumerate(notices[:5], 1):
        meta = save_html(n, "contract_notice", i)
        if meta:
            all_meta.append(meta)
    time.sleep(random.uniform(2, 4))

    # ── 2. TenderResultNotice (recent) — 5 samples ──
    print("\n" + "=" * 60)
    print("2. TenderResultNotice (recent)")
    print("=" * 60)
    notices = fetch_one_by_one(
        "TenderResultNotice",
        recent_from.strftime("%Y-%m-%dT%H:%M:%S"),
        recent_to.strftime("%Y-%m-%dT%H:%M:%S"),
        count=5,
    )
    for i, n in enumerate(notices[:5], 1):
        meta = save_html(n, "result_notice", i)
        if meta:
            all_meta.append(meta)
    time.sleep(random.uniform(2, 4))

    # ── 3. IT/Cyber sector (CPV 72*) — fetch bigger batch, filter client-side ──
    print("\n" + "=" * 60)
    print("3. IT/Cyber sector (looking for CPV 72*)")
    print("=" * 60)
    # Wider window to find IT notices
    notices = fetch_one_by_one(
        "ContractNotice",
        (recent_to - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S"),
        recent_to.strftime("%Y-%m-%dT%H:%M:%S"),
        count=50,
    )
    it_cyber = [
        n for n in notices
        if (n.get("cpvCode") or "").startswith("72")
        or any(kw in (n.get("orderObject") or "").lower()
               for kw in ("cyberbezpiecze", "informatyk", "it ", "oprogramow",
                           "system inform", "serwer"))
    ]
    print(f"  Found {len(it_cyber)} IT/Cyber notices out of {len(notices)}")
    for i, n in enumerate(it_cyber[:3], 1):
        meta = save_html(n, "it_cyber_notice", i)
        if meta:
            all_meta.append(meta)
    time.sleep(random.uniform(2, 4))

    # ── 4. OLD notices (February) — structure stability check ──
    print("\n" + "=" * 60)
    print("4. OLD notices (Feb 2026) — structure stability check")
    print("=" * 60)
    for ntype, prefix in [("ContractNotice", "old_contract"),
                          ("TenderResultNotice", "old_result")]:
        notices = fetch_one_by_one(
            ntype,
            old_from.strftime("%Y-%m-%dT%H:%M:%S"),
            old_to.strftime("%Y-%m-%dT%H:%M:%S"),
            count=2,
        )
        for i, n in enumerate(notices[:2], 1):
            meta = save_html(n, prefix, i)
            if meta:
                all_meta.append(meta)
        time.sleep(random.uniform(2, 4))

    # ── Summary ──
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total HTML files saved: {len(all_meta)}")
    sizes = [m["htmlBody_size_kb"] for m in all_meta]
    if sizes:
        print(f"HTML sizes: min={min(sizes):.1f}KB, max={max(sizes):.1f}KB, "
              f"avg={sum(sizes)/len(sizes):.1f}KB")
        print(f"Total sample size: {sum(sizes):.1f}KB")
        # Estimate daily storage cost
        avg_kb = sum(sizes) / len(sizes)
        daily_notices = 800
        daily_mb = (avg_kb * daily_notices) / 1024
        print(f"\n  Estimate: {avg_kb:.0f}KB avg x {daily_notices} notices/day "
              f"= {daily_mb:.1f} MB/day raw HTML")
        print(f"  But we only store EXTRACTED fields (budget, price, count) "
              f"= ~50 bytes/notice = {daily_notices * 50 / 1024:.1f} KB/day")

    summary_path = RECON_DIR / "_samples_summary.json"
    summary_path.write_text(
        json.dumps(all_meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\nMetadata saved to: {summary_path}")
    print(f"HTML files saved to: {RECON_DIR}")


if __name__ == "__main__":
    main()
