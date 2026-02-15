"""
Scraper Monitor - Portfolio Data Factory
=========================================
Wrapper uruchamiający scrapery z walidacją wyników i powiadomieniami email.

Wykrywa:
  - Scraper się wysypał (exception)
  - Brak __NEXT_DATA__ (zmiana struktury strony / Cloudflare)
  - 0 ofert w kategorii (URL-e mogły się zmienić)
  - Oferty bez tytułu (parser przestał działać)
  - Spadek liczby ofert vs. poprzedni run (opcjonalnie)

Powiadomienia:
  Gmail SMTP z App Password. Konfiguracja w .env lub zmiennych środowiskowych.

Użycie:
  python scraper_monitor.py                  # uruchom wszystkie scrapery
  python scraper_monitor.py --test-email     # wyślij testowego maila
  python scraper_monitor.py --pracuj-only    # tylko Pracuj.pl
  python scraper_monitor.py --nfj-only       # tylko NoFluffJobs
  python scraper_monitor.py --dry-run        # walidacja bez wysyłania maila

Setup (jednorazowy):
  1. Włącz 2FA na koncie Google: https://myaccount.google.com/security
  2. Wygraj App Password: https://myaccount.google.com/apppasswords
     - Wybierz "Mail" i "Windows Computer"
     - Skopiuj 16-znakowe hasło (bez spacji)
  3. Utwórz plik .env w katalogu projektu:
     ALERT_EMAIL_FROM=twoj.email@gmail.com
     ALERT_EMAIL_PASSWORD=xxxx xxxx xxxx xxxx
     ALERT_EMAIL_TO=twoj.email@gmail.com

Wymaga: (brak dodatkowych zależności - smtp i email są w stdlib)
"""

import io
import os
import sys
import json
import smtplib
import argparse
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path

# --- Fix kodowania Windows ---
if sys.platform == "win32" and getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# --- Ścieżki ---
SCRAPER_DIR = Path(__file__).parent
PROJECT_DIR = SCRAPER_DIR.parent  # root projektu (Portfolio-Data-Factory)
HISTORY_FILE = SCRAPER_DIR / "scraper_run_history.json"
ENV_FILE = PROJECT_DIR / ".env"   # .env leży w roocie projektu

# --- Konfiguracja alertów ---
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Minimalne progi - jeśli wynik jest poniżej, wysyłamy alert
MIN_TOTAL_OFFERS = 5          # minimum ofert łącznie
MIN_OFFERS_PER_CATEGORY = 1   # minimum ofert per kategoria


def load_env():
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


def get_email_config() -> dict | None:
    """Zwraca konfigurację email lub None jeśli nie skonfigurowano."""
    email_from = os.environ.get("ALERT_EMAIL_FROM", "").strip()
    password = os.environ.get("ALERT_EMAIL_PASSWORD", "").strip()
    email_to = os.environ.get("ALERT_EMAIL_TO", "").strip()

    if not all([email_from, password, email_to]):
        return None

    return {
        "from": email_from,
        "password": password,
        "to": email_to,
    }


def send_email(subject: str, body_html: str, config: dict) -> bool:
    """Wysyła email przez Gmail SMTP. Zwraca True jeśli się udało."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config["from"]
    msg["To"] = config["to"]
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(config["from"], config["password"])
            server.sendmail(config["from"], config["to"], msg.as_string())
        print(f"  [EMAIL] Wysłano alert na {config['to']}")
        return True
    except Exception as e:
        print(f"  [EMAIL FAIL] Nie udało się wysłać: {e}")
        return False


# --- Historia runów ---

def load_history() -> list[dict]:
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, Exception):
            return []
    return []


def save_history(history: list[dict]):
    # Zachowaj max 90 ostatnich runów (~3 miesiące przy co-tygodniowym)
    history = history[-90:]
    HISTORY_FILE.write_text(
        json.dumps(history, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def get_last_successful_run(history: list[dict], scraper_name: str | None = None) -> dict | None:
    for entry in reversed(history):
        if entry.get("success"):
            if scraper_name and entry.get("scraper") != scraper_name:
                continue
            return entry
    return None


# --- Health Checks ---

def validate_result(result: dict, history: list[dict], scraper_name: str | None = None) -> list[str]:
    """
    Sprawdza wynik scrapera i zwraca listę problemów (warnings/errors).
    Pusta lista = wszystko OK.
    """
    problems = []

    # 1. Scraper się wysypał
    if not result.get("success") and result.get("errors"):
        for err in result["errors"]:
            problems.append(f"BLAD: {err}")

    # 2. Za mało ofert
    total = result.get("total_offers", 0)
    if total == 0:
        problems.append("KRYTYCZNY: Scraper zwrócił 0 ofert!")
    elif total < MIN_TOTAL_OFFERS:
        problems.append(f"OSTRZEZENIE: Tylko {total} ofert (minimum: {MIN_TOTAL_OFFERS})")

    # 3. Puste kategorie
    empty_cats = result.get("categories_empty", [])
    if empty_cats:
        problems.append(
            f"PUSTE KATEGORIE ({len(empty_cats)}): {', '.join(empty_cats)}"
        )

    # 4. Porównanie z poprzednim runem TEGO SAMEGO scrapera - spadek > 50%
    last_ok = get_last_successful_run(history, scraper_name)
    if last_ok and total > 0:
        prev_total = last_ok.get("total_offers", 0)
        if prev_total > 0 and total < prev_total * 0.5:
            problems.append(
                f"SPADEK OFERT: {prev_total} → {total} "
                f"(poprzedni run: {last_ok.get('timestamp', '?')})"
            )

    return problems


def build_alert_html(scraper_name: str, result: dict, problems: list[str]) -> str:
    """Buduje HTML emaila z raportem alertu."""
    ts = result.get("timestamp", datetime.now().isoformat())
    total = result.get("total_offers", 0)
    cats_ok = result.get("categories_ok", [])
    cats_empty = result.get("categories_empty", [])
    errors = result.get("errors", [])

    problem_rows = ""
    for p in problems:
        color = "#dc3545" if "KRYTYCZNY" in p or "BLAD" in p else "#ffc107"
        problem_rows += f'<tr><td style="color:{color};padding:4px 8px;">&#9888; {p}</td></tr>\n'

    cat_ok_str = ", ".join(cats_ok) if cats_ok else "(brak)"
    cat_empty_str = ", ".join(cats_empty) if cats_empty else "(brak)"

    error_rows = ""
    if errors:
        for e in errors:
            error_rows += f"<li>{e}</li>\n"
        error_section = f'<h3 style="color:#dc3545;">Errory scrapera:</h3><ul>{error_rows}</ul>'
    else:
        error_section = ""

    html = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:600px;">
    <h2 style="color:#dc3545;">&#128680; Alert: {scraper_name}</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <p><strong>Ofert:</strong> {total}</p>

    <h3>Wykryte problemy:</h3>
    <table style="border-collapse:collapse;width:100%;">
    {problem_rows}
    </table>

    <h3>Kategorie OK ({len(cats_ok)}):</h3>
    <p style="color:green;">{cat_ok_str}</p>

    <h3 style="color:#dc3545;">Kategorie puste ({len(cats_empty)}):</h3>
    <p>{cat_empty_str}</p>

    {error_section}

    <hr>
    <p style="color:gray;font-size:12px;">
      Portfolio Data Factory - Scraper Monitor<br>
      Ten alert został wygenerowany automatycznie.
    </p>
    </body></html>
    """
    return html


def build_success_summary(scraper_name: str, result: dict) -> str:
    """Krótki log sukcesu (nie wysyłamy maila, tylko logujemy)."""
    total = result.get("total_offers", 0)
    cats = len(result.get("categories_ok", []))
    return f"[OK] {scraper_name}: {total} ofert, {cats} kategorii"


# --- Runner ---

def run_pracuj(dry_run: bool = False) -> dict:
    """Uruchamia Pracuj.pl scraper i zwraca wynik."""
    print("\n" + "=" * 70)
    print("  MONITOR: Uruchamiam Pracuj.pl scraper...")
    print("=" * 70)

    try:
        from pracuj_premium_scraper import run as pracuj_run
        result = pracuj_run()
    except Exception as e:
        tb = traceback.format_exc()
        print(f"\n[MONITOR] Scraper rzucił wyjątek:\n{tb}")
        result = {
            "success": False,
            "total_offers": 0,
            "categories_ok": [],
            "categories_empty": [],
            "errors": [f"Nieobsłużony wyjątek: {e}"],
            "output_path": None,
            "timestamp": datetime.now().isoformat(),
        }

    return result


def run_nfj(dry_run: bool = False) -> dict:
    """Uruchamia NoFluffJobs scraper i zwraca wynik."""
    print("\n" + "=" * 70)
    print("  MONITOR: Uruchamiam NoFluffJobs scraper...")
    print("=" * 70)

    try:
        sys.path.insert(0, str(PROJECT_DIR))
        from nfj_scraper.nfj_data_scraper import run as nfj_run
        result = nfj_run()
    except Exception as e:
        tb = traceback.format_exc()
        print(f"\n[MONITOR] Scraper rzucił wyjątek:\n{tb}")
        result = {
            "success": False,
            "total_offers": 0,
            "categories_ok": [],
            "categories_empty": [],
            "errors": [f"Nieobsłużony wyjątek: {e}"],
            "timestamp": datetime.now().isoformat(),
        }

    return result


def run_justjoin(dry_run: bool = False) -> dict:
    """Uruchamia JustJoin.it scraper i zwraca wynik."""
    print("\n" + "=" * 70)
    print("  MONITOR: Uruchamiam JustJoin.it scraper...")
    print("=" * 70)

    try:
        sys.path.insert(0, str(PROJECT_DIR))
        from just_join_scraper.just_join_scraper import run as justjoin_run
        result = justjoin_run()
    except Exception as e:
        tb = traceback.format_exc()
        print(f"\n[MONITOR] Scraper rzucił wyjątek:\n{tb}")
        result = {
            "success": False,
            "total_offers": 0,
            "categories_ok": [],
            "categories_empty": [],
            "errors": [f"Nieobsłużony wyjątek: {e}"],
            "timestamp": datetime.now().isoformat(),
        }

    return result


def monitor_scraper(scraper_name: str, result: dict, dry_run: bool = False):
    """Waliduje wynik, zapisuje historię, wysyła alert jeśli trzeba."""
    history = load_history()
    problems = validate_result(result, history, scraper_name)

    # Zapisz do historii
    history.append({
        "scraper": scraper_name,
        "timestamp": result.get("timestamp"),
        "success": result.get("success", False),
        "total_offers": result.get("total_offers", 0),
        "categories_ok": result.get("categories_ok", []),
        "categories_empty": result.get("categories_empty", []),
        "errors": result.get("errors", []),
        "problems": problems,
    })
    save_history(history)

    if not problems:
        print(f"\n  {build_success_summary(scraper_name, result)}")
        return

    # Jest problem - wysyłamy alert
    print(f"\n  [MONITOR] Wykryto {len(problems)} problemów:")
    for p in problems:
        print(f"    - {p}")

    if dry_run:
        print("  [MONITOR] --dry-run: pomijam wysyłkę emaila")
        return

    email_config = get_email_config()
    if not email_config:
        print("  [MONITOR] Brak konfiguracji email (.env) - nie mogę wysłać alertu!")
        print("  Skonfiguruj plik .env (patrz docstring na górze pliku)")
        return

    subject = f"[ALERT] {scraper_name} - {len(problems)} problemów ({datetime.now().strftime('%Y-%m-%d')})"
    body = build_alert_html(scraper_name, result, problems)
    send_email(subject, body, email_config)


def test_email():
    """Wysyła testowego maila żeby sprawdzić konfigurację."""
    config = get_email_config()
    if not config:
        print("[FAIL] Brak konfiguracji email.")
        print("Utwórz plik .env z:")
        print("  ALERT_EMAIL_FROM=twoj.email@gmail.com")
        print("  ALERT_EMAIL_PASSWORD=xxxx xxxx xxxx xxxx")
        print("  ALERT_EMAIL_TO=twoj.email@gmail.com")
        return

    subject = f"[TEST] Portfolio Data Factory Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = """
    <html><body style="font-family:Segoe UI,Arial,sans-serif;">
    <h2 style="color:green;">&#9989; Test alertu - działa!</h2>
    <p>Jeśli widzisz ten email, konfiguracja powiadomień jest poprawna.</p>
    <p>Portfolio Data Factory - Scraper Monitor</p>
    </body></html>
    """
    ok = send_email(subject, body, config)
    if ok:
        print("[SUCCESS] Testowy email wysłany - sprawdź skrzynkę!")
    else:
        print("[FAIL] Nie udało się wysłać testowego emaila.")


def main():
    load_env()

    parser = argparse.ArgumentParser(description="Scraper Monitor - Portfolio Data Factory")
    parser.add_argument("--test-email", action="store_true", help="Wyślij testowego maila")
    parser.add_argument("--pracuj-only", action="store_true", help="Tylko Pracuj.pl")
    parser.add_argument("--nfj-only", action="store_true", help="Tylko NoFluffJobs")
    parser.add_argument("--justjoin-only", action="store_true", help="Tylko JustJoin.it")
    parser.add_argument("--dry-run", action="store_true", help="Bez wysyłania emaila")
    args = parser.parse_args()

    if args.test_email:
        test_email()
        return

    # Determine which scrapers to run
    run_all = not (args.pracuj_only or args.nfj_only or args.justjoin_only)

    # --- Uruchom scrapery ---
    # Kolejność: NFJ (szybkie API) → JustJoin (REST API) → Pracuj (Playwright, najcięższy)
    results = {}

    # NoFluffJobs
    if run_all or args.nfj_only:
        result = run_nfj(dry_run=args.dry_run)
        results["NoFluffJobs"] = result
        monitor_scraper("NoFluffJobs", result, dry_run=args.dry_run)

    # JustJoin.it
    if run_all or args.justjoin_only:
        result = run_justjoin(dry_run=args.dry_run)
        results["JustJoin.it"] = result
        monitor_scraper("JustJoin.it", result, dry_run=args.dry_run)

    # Pracuj.pl (Playwright - uruchamiany jako ostatni, największe ryzyko RAM)
    if run_all or args.pracuj_only:
        result = run_pracuj(dry_run=args.dry_run)
        results["Pracuj.pl"] = result
        monitor_scraper("Pracuj.pl", result, dry_run=args.dry_run)

    # --- Podsumowanie ---
    print(f"\n{'='*70}")
    print("  MONITOR - PODSUMOWANIE")
    print(f"{'='*70}")
    all_ok = True
    for name, r in results.items():
        status = "OK" if r.get("success") else "FAIL"
        if not r.get("success"):
            all_ok = False
        print(f"  {name:20s} [{status}] {r.get('total_offers', 0)} ofert")

    if all_ok:
        print("\n  Wszystko działa poprawnie.")
    else:
        print("\n  Wykryto problemy - sprawdź alerty powyżej.")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
