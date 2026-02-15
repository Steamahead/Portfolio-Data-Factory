# -*- coding: utf-8 -*-
"""Quick probe: fetch 1 NFJ listing + its detail JSON to see available fields."""
import json
import sys
import requests

sys.stdout.reconfigure(encoding='utf-8')

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://nofluffjobs.com",
    "Referer": "https://nofluffjobs.com/pl/praca-it",
}

# 1. Get one listing
payload = {"criteriaSearch": {"category": ["data"]}, "rawSearch": "category=data", "page": 1}
r = requests.post(
    "https://nofluffjobs.com/api/search/posting",
    params={"salaryCurrency": "PLN", "salaryPeriod": "month"},
    headers=HEADERS, json=payload, timeout=30,
)
r.raise_for_status()
posting = r.json()["postings"][0]
pid = posting["id"]
print(f"Listing ID: {pid}")
print(f"Listing keys: {list(posting.keys())}")
print(f"\nFull listing JSON:\n{json.dumps(posting, indent=2, ensure_ascii=False)[:3000]}")

# 2. Fetch detail
print(f"\n{'='*60}\nFetching detail: /api/posting/{pid}\n{'='*60}")
r2 = requests.get(f"https://nofluffjobs.com/api/posting/{pid}", headers=HEADERS, timeout=30)
r2.raise_for_status()
detail = r2.json()

# Show top-level keys
print(f"\nDetail top-level keys: {list(detail.keys())}")
for key in detail.keys():
    val = detail[key]
    if isinstance(val, dict):
        print(f"\n  [{key}] (dict) sub-keys: {list(val.keys())}")
    elif isinstance(val, list):
        print(f"\n  [{key}] (list) len={len(val)}, first={val[0] if val else 'empty'}")
    elif isinstance(val, str) and len(val) > 200:
        print(f"\n  [{key}] (str) len={len(val)}: {val[:200]}...")
    else:
        print(f"\n  [{key}] = {val}")

# Dump full JSON to file for inspection
with open("nfj_scraper/probe_detail_dump.json", "w", encoding="utf-8") as f:
    json.dump(detail, f, indent=2, ensure_ascii=False)
print("\nFull detail JSON saved to nfj_scraper/probe_detail_dump.json")
