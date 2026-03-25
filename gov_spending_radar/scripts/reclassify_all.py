"""
Reclassify ALL gov_notices using the 3-layer classifier v2.0.
==============================================================
Steps:
1. Run schema migration (v2 unique constraint)
2. DELETE all existing classifications
3. Fetch ALL notices from gov_notices
4. Run 3-layer classifier on each notice
5. Upload all classification records (multi-label)
6. Print statistics
7. Export validation CSV

Usage (from project root):
    python -X utf8 -m gov_spending_radar.scripts.reclassify_all
"""

import csv
import sys
import time
from collections import Counter
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from gov_spending_radar.db.operations import (
    run_schema_migration,
    delete_all_classifications,
    fetch_all_notices_for_classification,
    upload_classifications,
)
from gov_spending_radar.main import classify_notice_multilabel


EXPORT_PATH = Path(__file__).parent.parent / "validation_export.csv"

# Sectors to include in validation CSV
VALIDATION_SECTORS = {"AI", "CYBERSECURITY", "CLOUD", "DATA_ANALYTICS"}


def main():
    start = time.time()

    # ── Step 1: Schema migration ──
    print("\n[RECLASSIFY] Step 1: Schema migration...")
    run_schema_migration()

    # ── Step 2: Delete all classifications ──
    print("\n[RECLASSIFY] Step 2: Deleting all existing classifications...")
    deleted = delete_all_classifications()
    print(f"  Deleted {deleted} rows")

    # ── Step 3: Fetch all notices ──
    print("\n[RECLASSIFY] Step 3: Fetching all notices...")
    notices = fetch_all_notices_for_classification()
    if not notices:
        print("  No notices found. Exiting.")
        return

    print(f"  Total notices: {len(notices)}")

    # ── Step 4: Classify ──
    print(f"\n[RECLASSIFY] Step 4: Running 3-layer classifier on {len(notices)} notices...")
    all_classifications = []
    notices_with_labels = 0
    multilabel_count = 0

    for i, notice in enumerate(notices):
        results = classify_notice_multilabel(
            title=notice.get("title", ""),
            cpv_code=notice.get("cpv_code"),
            cpv_raw=notice.get("cpv_raw"),
        )

        if results:
            notices_with_labels += 1
            # Count unique sectors per notice (multi-label if >1)
            unique_sectors = {r["sector"] for r in results}
            if len(unique_sectors) > 1:
                multilabel_count += 1

        for cls in results:
            all_classifications.append({
                "notice_object_id": notice["object_id"],
                "method": cls["method"],
                "sector": cls["sector"],
                "confidence": cls["confidence"],
                "raw_response": None,
                # Keep for CSV export
                "_title": notice.get("title", ""),
                "_cpv_raw": notice.get("cpv_raw", ""),
                "_buyer_name": notice.get("buyer_name", ""),
            })

        if (i + 1) % 5000 == 0:
            print(f"  Processed {i + 1}/{len(notices)}...")

    print(f"  Total classification records: {len(all_classifications)}")
    print(f"  Notices with at least 1 label: {notices_with_labels}/{len(notices)}")
    print(f"  Multi-label notices: {multilabel_count}")

    # ── Step 5: Upload ──
    print(f"\n[RECLASSIFY] Step 5: Uploading {len(all_classifications)} records...")
    # Strip internal fields before upload
    upload_records = [
        {k: v for k, v in rec.items() if not k.startswith("_")}
        for rec in all_classifications
    ]
    result = upload_classifications(upload_records)
    print(f"  Uploaded: {result['uploaded']}, Errors: {len(result['errors'])}")

    # ── Step 6: Statistics ──
    print(f"\n{'='*60}")
    print("CLASSIFICATION STATISTICS")
    print(f"{'='*60}")

    sector_counts = Counter(r["sector"] for r in all_classifications)
    method_counts = Counter(r["method"] for r in all_classifications)

    print("\nPer sector:")
    for sector, count in sector_counts.most_common():
        print(f"  {sector:20s} {count:6d}")

    print("\nPer method:")
    for method, count in method_counts.most_common():
        print(f"  {method:20s} {count:6d}")

    print(f"\nMulti-label notices: {multilabel_count}")
    print(f"Total records: {len(all_classifications)}")

    # Top titles per tech sector
    for sector in ["AI", "CYBERSECURITY", "CLOUD", "DATA_ANALYTICS"]:
        sector_recs = [r for r in all_classifications if r["sector"] == sector]
        if sector_recs:
            print(f"\nTop 5 titles — {sector}:")
            # Sort by confidence ascending (lowest first = most suspicious)
            sector_recs.sort(key=lambda r: r["confidence"])
            for rec in sector_recs[:5]:
                print(f"  [{rec['confidence']:.2f} {rec['method']:12s}] {rec['_title'][:80]}")

    # ── Step 7: Export validation CSV ──
    print(f"\n[RECLASSIFY] Step 7: Exporting validation CSV...")
    validation_recs = [
        r for r in all_classifications
        if r["sector"] in VALIDATION_SECTORS and r["confidence"] >= 0.5
    ]
    validation_recs.sort(key=lambda r: (r["sector"], r["confidence"]))

    with open(EXPORT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["sector", "confidence", "method", "title", "cpv_raw", "buyer_name", "object_id"])
        for rec in validation_recs:
            writer.writerow([
                rec["sector"],
                f"{rec['confidence']:.2f}",
                rec["method"],
                rec["_title"],
                rec["_cpv_raw"],
                rec["_buyer_name"],
                rec["notice_object_id"],
            ])

    print(f"  Exported {len(validation_recs)} records to {EXPORT_PATH}")

    elapsed = time.time() - start
    print(f"\n[RECLASSIFY] Done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
