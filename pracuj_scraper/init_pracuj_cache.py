"""
Jednorazowy skrypt: tworzy pracuj_known_offers.json z istniejących offer_id w Azure SQL.
Dzięki temu pierwszy inkrementalny run nie musi pobierać detali dla ~2450 ofert.

Użycie:
  .venv\Scripts\python.exe -X utf8 pracuj_scraper/init_pracuj_cache.py
"""

import os
import sys
import json
from pathlib import Path

# Dodaj root projektu do path
PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

# Załaduj .env
ENV_FILE = PROJECT_DIR / ".env"
if ENV_FILE.exists():
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

import pyodbc

KNOWN_OFFERS_FILE = Path(__file__).parent / "pracuj_known_offers.json"

conn_str = os.environ.get("SqlConnectionString")
if not conn_str:
    print("[FAIL] Brak SqlConnectionString w .env")
    sys.exit(1)

print("[*] Łączenie z Azure SQL...")
conn = pyodbc.connect(conn_str, timeout=60)
cursor = conn.cursor()

cursor.execute("SELECT offer_id, scraped_at FROM pracuj_offers WHERE offer_id IS NOT NULL AND offer_id != ''")
rows = cursor.fetchall()
conn.close()

known = {row[0]: (row[1] or "") for row in rows}
KNOWN_OFFERS_FILE.write_text(json.dumps(known, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"[OK] Zapisano {len(known)} offer_id do {KNOWN_OFFERS_FILE.name}")
