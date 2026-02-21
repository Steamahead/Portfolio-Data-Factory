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
  python scraper_monitor.py --status         # pokaż status aktywnego runu
  python scraper_monitor.py --status --watch # auto-odświeżanie co 5s

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
import tempfile
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

# Scrapery wymagane w każdym daily run - brak któregoś → ostrzeżenie w raporcie
EXPECTED_SCRAPERS = ["NoFluffJobs", "JustJoin.it", "Pracuj.pl"]

# Progress file — aktualizowany na bieżąco, odczytywany przez --status
PROGRESS_FILE = SCRAPER_DIR / "scraper_progress.json"


class ProgressTracker:
    """Tracks scraper run progress via a JSON file for real-time monitoring."""

    def __init__(self):
        self._data = {}
        self._scraper_start = None
        self._item_times = []

    def start_run(self, scrapers_planned: list[str]):
        self._data = {
            "run_started": datetime.now().isoformat(),
            "status": "running",
            "current_scraper": None,
            "scraper_index": 0,
            "scrapers_total": len(scrapers_planned),
            "scrapers_planned": scrapers_planned,
            "phase": None,
            "progress": None,
            "percent": 0,
            "elapsed": "00:00:00",
            "eta": None,
            "completed_scrapers": [],
        }
        self._run_start = datetime.now()
        self._save()

    def start_scraper(self, name: str):
        self._scraper_start = datetime.now()
        self._item_times = []
        self._data["current_scraper"] = name
        self._data["scraper_index"] = len(self._data["completed_scrapers"]) + 1
        self._data["phase"] = "starting"
        self._data["progress"] = None
        self._data["percent"] = 0
        self._data["eta"] = None
        self._update_elapsed()
        self._save()

    def update(self, current: int, total: int, phase: str):
        self._item_times.append(datetime.now())
        self._data["phase"] = phase
        self._data["progress"] = f"{current}/{total}"
        self._data["percent"] = round(current / total * 100) if total > 0 else 0
        self._update_elapsed()
        # ETA based on average time per item
        if len(self._item_times) >= 2:
            elapsed_items = (self._item_times[-1] - self._item_times[0]).total_seconds()
            items_done = len(self._item_times) - 1
            if items_done > 0:
                avg_per_item = elapsed_items / items_done
                remaining = (total - current) * avg_per_item
                mins, secs = divmod(int(remaining), 60)
                hours, mins = divmod(mins, 60)
                self._data["eta"] = f"~{hours:02d}:{mins:02d}:{secs:02d}"
        self._save()

    def finish_scraper(self, name: str, result: dict):
        elapsed = datetime.now() - self._scraper_start if self._scraper_start else None
        elapsed_str = str(elapsed).split(".")[0] if elapsed else "?"
        status = "OK" if result.get("success") else "FAIL"
        self._data["completed_scrapers"].append({
            "name": name,
            "offers": result.get("total_offers", 0),
            "duration": elapsed_str,
            "status": status,
        })
        self._data["current_scraper"] = None
        self._data["phase"] = None
        self._data["progress"] = None
        self._data["percent"] = 0
        self._data["eta"] = None
        self._update_elapsed()
        self._save()

    def finish_run(self):
        self._data["status"] = "finished"
        self._data["current_scraper"] = None
        self._update_elapsed()
        self._save()

    def _update_elapsed(self):
        if hasattr(self, "_run_start"):
            elapsed = datetime.now() - self._run_start
            self._data["elapsed"] = str(elapsed).split(".")[0]

    def _save(self):
        """Atomic write: write to temp file, then rename."""
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=str(SCRAPER_DIR), suffix=".tmp", prefix="progress_"
            )
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            # On Windows, os.replace is atomic if on the same volume
            os.replace(tmp_path, str(PROGRESS_FILE))
        except Exception:
            # Non-critical — don't break the scraper run
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


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


def build_start_email_html(scrapers_to_run: list[str]) -> str:
    """Buduje HTML emaila informującego o starcie daily run."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scraper_list = "".join(f"<li>{s}</li>" for s in scrapers_to_run)
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:600px;color:#333;">
    <h2 style="color:#007bff;">&#9654; Daily Run Started — {ts}</h2>
    <p>Scraper Monitor wystartował. Planowane scrapery:</p>
    <ul>{scraper_list}</ul>
    <p style="color:gray;font-size:12px;">
      Jeśli nie otrzymasz maila FINISH w ciągu kilku godzin,
      sprawdź logi lub Task Scheduler.
    </p>
    <hr>
    <p style="color:gray;font-size:12px;">
      Portfolio Data Factory — Scraper Monitor · {ts}
    </p>
    </body></html>
    """


def build_daily_report_html(results: dict, history: list[dict]) -> str:
    """Buduje HTML z codziennym raportem podsumowującym wszystkie scrapery.

    Zawsze pokazuje wszystkie EXPECTED_SCRAPERS — nawet te, które nie były częścią
    bieżącego runu. Dla brakujących szuka w historii dnia; jeśli brak → "NIE URUCHOMIONY".
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    today_date = datetime.now().strftime("%Y-%m-%d")

    # Indeks historii dzisiejszego dnia: {scraper_name: [entries]}
    today_history: dict[str, list[dict]] = {}
    for h in history:
        name = h.get("scraper", "")
        ts_h = h.get("timestamp", "")
        if name and (ts_h or "").startswith(today_date):
            today_history.setdefault(name, []).append(h)

    # Połącz bieżący run z brakującymi z historii / brakującymi zupełnie
    all_scraper_rows: list[tuple[str, dict, str]] = []  # (name, result_dict, source)
    for name in EXPECTED_SCRAPERS:
        if name in results:
            all_scraper_rows.append((name, results[name], "current"))
        elif name in today_history:
            # Weź ostatni run z dzisiaj
            last_today = today_history[name][-1]
            all_scraper_rows.append((name, last_today, "history"))
        else:
            all_scraper_rows.append((name, {}, "missing"))

    # Czy wszystko OK (brak brakujących i brak błędów)
    all_ok = all(
        source != "missing" and r.get("success", False)
        for _, r, source in all_scraper_rows
    )
    status_color = "#28a745" if all_ok else "#dc3545"
    status_icon = "✅" if all_ok else "⚠️"
    status_text = "Wszystko OK" if all_ok else "Wykryto problemy"

    rows = ""
    total_all = 0
    for name, r, source in all_scraper_rows:
        if source == "missing":
            rows += f"""
        <tr style="border-top:1px solid #eee;">
          <td style="padding:8px;font-weight:bold;">&#9888; {name}</td>
          <td style="padding:8px;color:#dc3545;font-weight:bold;">NIE URUCHOMIONY DZISIAJ</td>
        </tr>
        <tr>
          <td style="padding:2px 8px;color:#dc3545;font-size:12px;" colspan="2">
            Scraper nie uruchomił się w dniu {today_date}. Sprawdź Task Scheduler lub uruchom ręcznie.
          </td>
        </tr>
        """
            continue

        ok = r.get("success", False)
        total = r.get("total_offers", 0)
        total_all += total
        icon = "✅" if ok else "❌"
        color = "#28a745" if ok else "#dc3545"

        source_label = ""
        if source == "history":
            run_ts = r.get("timestamp", "?")[:16].replace("T", " ")
            source_label = f' <span style="color:#888;font-size:11px;">(run z {run_ts})</span>'

        # Porównanie z poprzednim runem
        last = get_last_successful_run(
            [h for h in history if h.get("scraper") == name and h.get("timestamp") != r.get("timestamp")],
            name
        )
        if last and last.get("total_offers", 0) > 0:
            prev = last["total_offers"]
            diff = total - prev
            diff_str = f"+{diff}" if diff >= 0 else str(diff)
            diff_color = "#28a745" if diff >= 0 else "#dc3545"
            trend = f' <span style="color:{diff_color};font-size:12px;">({diff_str} vs poprzedni)</span>'
        else:
            trend = ""

        cats_ok = ", ".join(r.get("categories_ok", [])) or "—"
        errors = "; ".join(r.get("errors", [])) or "—"
        error_row = "" if ok else f'<tr><td colspan="2" style="color:#dc3545;font-size:12px;padding:2px 8px;">&#9888; {errors}</td></tr>'

        rows += f"""
        <tr style="border-top:1px solid #eee;">
          <td style="padding:8px;font-weight:bold;">{icon} {name}{source_label}</td>
          <td style="padding:8px;color:{color};">{total} ofert{trend}</td>
        </tr>
        <tr>
          <td style="padding:2px 8px;color:gray;font-size:12px;" colspan="2">Kategorie: {cats_ok}</td>
        </tr>
        {error_row}
        """

    html = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:600px;color:#333;">
    <h2 style="color:{status_color};">{status_icon} Daily Report — {ts}</h2>
    <p style="color:gray;">Portfolio Data Factory · Scrapers Summary</p>

    <table style="border-collapse:collapse;width:100%;background:#f9f9f9;border-radius:4px;">
      {rows}
      <tr style="border-top:2px solid #ccc;background:#fff;">
        <td style="padding:8px;font-weight:bold;">ŁĄCZNIE</td>
        <td style="padding:8px;font-weight:bold;">{total_all} ofert</td>
      </tr>
    </table>

    <p style="margin-top:16px;color:{'#28a745' if all_ok else '#dc3545'};font-weight:bold;">{status_text}</p>

    <hr>
    <p style="color:gray;font-size:12px;">
      Portfolio Data Factory — Scraper Monitor<br>
      Raport wygenerowany automatycznie · {ts}
    </p>
    </body></html>
    """
    return html


# --- Runner ---

def run_pracuj(dry_run: bool = False, progress_callback=None) -> dict:
    """Uruchamia Pracuj.pl scraper i zwraca wynik."""
    print("\n" + "=" * 70)
    print("  MONITOR: Uruchamiam Pracuj.pl scraper...")
    print("=" * 70)

    try:
        from pracuj_premium_scraper import run as pracuj_run
        result = pracuj_run(progress_callback=progress_callback)
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


def run_nfj(dry_run: bool = False, progress_callback=None) -> dict:
    """Uruchamia NoFluffJobs scraper i zwraca wynik."""
    print("\n" + "=" * 70)
    print("  MONITOR: Uruchamiam NoFluffJobs scraper...")
    print("=" * 70)

    try:
        sys.path.insert(0, str(PROJECT_DIR))
        from nfj_scraper.nfj_data_scraper import run as nfj_run
        result = nfj_run(progress_callback=progress_callback)
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


def run_justjoin(dry_run: bool = False, progress_callback=None) -> dict:
    """Uruchamia JustJoin.it scraper i zwraca wynik."""
    print("\n" + "=" * 70)
    print("  MONITOR: Uruchamiam JustJoin.it scraper...")
    print("=" * 70)

    try:
        sys.path.insert(0, str(PROJECT_DIR))
        from just_join_scraper.just_join_scraper import run as justjoin_run
        result = justjoin_run(progress_callback=progress_callback)
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


def show_status():
    """Wyświetla aktualny status runu na podstawie scraper_progress.json."""
    if not PROGRESS_FILE.exists():
        print("Brak aktywnego runu (scraper_progress.json nie istnieje).")
        return False

    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, Exception) as e:
        print(f"Błąd odczytu progress file: {e}")
        return False

    status = data.get("status", "?")
    elapsed = data.get("elapsed", "?")
    started = data.get("run_started", "?")
    if isinstance(started, str) and len(started) > 16:
        started = started[11:16]  # HH:MM

    print(f"=== Scraper Monitor — {status} ===")
    print(f"Start: {started} | Elapsed: {elapsed}")
    print()

    for cs in data.get("completed_scrapers", []):
        icon = "[OK]" if cs["status"] == "OK" else "[!!]"
        print(f"  {icon} {cs['name']:15s} {cs['offers']:>5d} ofert  ({cs['duration']})")

    current = data.get("current_scraper")
    if current and status == "running":
        phase = data.get("phase", "?")
        progress = data.get("progress", "?")
        percent = data.get("percent", 0)
        eta = data.get("eta", "?")
        scraper_idx = data.get("scraper_index", "?")
        scrapers_total = data.get("scrapers_total", "?")
        print(f"  [>>] {current:15s} {phase} {progress} ({percent}%)  ETA: {eta}")
        print(f"\n  Scraper {scraper_idx}/{scrapers_total}")

    if status == "finished":
        print(f"\n  Run zakończony.")

    return True


def watch_status(interval: int = 5):
    """Auto-odświeżanie statusu co `interval` sekund. Ctrl+C żeby wyjść."""
    import time
    print(f"Tryb watch — odświeżanie co {interval}s. Ctrl+C żeby wyjść.\n")
    try:
        while True:
            # Clear screen (works on Windows and Unix)
            os.system("cls" if os.name == "nt" else "clear")
            show_status()
            print(f"\n  (auto-refresh co {interval}s, Ctrl+C = stop)")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nZatrzymano watch.")


def main():
    load_env()

    parser = argparse.ArgumentParser(description="Scraper Monitor - Portfolio Data Factory")
    parser.add_argument("--test-email", action="store_true", help="Wyślij testowego maila")
    parser.add_argument("--pracuj-only", action="store_true", help="Tylko Pracuj.pl")
    parser.add_argument("--nfj-only", action="store_true", help="Tylko NoFluffJobs")
    parser.add_argument("--justjoin-only", action="store_true", help="Tylko JustJoin.it")
    parser.add_argument("--dry-run", action="store_true", help="Bez wysyłania emaila")
    parser.add_argument("--status", action="store_true", help="Pokaż status aktywnego runu")
    parser.add_argument("--watch", action="store_true", help="Auto-odświeżanie statusu co 5s (z --status)")
    args = parser.parse_args()

    if args.test_email:
        test_email()
        return

    if args.status:
        if args.watch:
            watch_status()
        else:
            show_status()
        return

    # Determine which scrapers to run
    run_all = not (args.pracuj_only or args.nfj_only or args.justjoin_only)

    # Lista scraperów do uruchomienia (na potrzeby emaila START)
    scrapers_planned = []
    if run_all or args.nfj_only:
        scrapers_planned.append("NoFluffJobs")
    if run_all or args.justjoin_only:
        scrapers_planned.append("JustJoin.it")
    if run_all or args.pracuj_only:
        scrapers_planned.append("Pracuj.pl")

    # --- Email START ---
    start_time = datetime.now()
    if not args.dry_run:
        email_config = get_email_config()
        if email_config:
            start_subject = f"[START] Daily Run {start_time.strftime('%Y-%m-%d %H:%M')} — Portfolio Data Factory"
            start_body = build_start_email_html(scrapers_planned)
            send_email(start_subject, start_body, email_config)

    # --- Progress Tracker ---
    tracker = ProgressTracker()
    tracker.start_run(scrapers_planned)

    # --- Uruchom scrapery ---
    # Kolejność: NFJ (szybkie API) → JustJoin (REST API) → Pracuj (Playwright, najcięższy)
    results = {}
    fatal_error = None

    try:
        # NoFluffJobs
        if run_all or args.nfj_only:
            tracker.start_scraper("NoFluffJobs")
            result = run_nfj(dry_run=args.dry_run, progress_callback=tracker.update)
            results["NoFluffJobs"] = result
            tracker.finish_scraper("NoFluffJobs", result)
            monitor_scraper("NoFluffJobs", result, dry_run=args.dry_run)

        # JustJoin.it
        if run_all or args.justjoin_only:
            tracker.start_scraper("JustJoin.it")
            result = run_justjoin(dry_run=args.dry_run, progress_callback=tracker.update)
            results["JustJoin.it"] = result
            tracker.finish_scraper("JustJoin.it", result)
            monitor_scraper("JustJoin.it", result, dry_run=args.dry_run)

        # Pracuj.pl (Playwright - uruchamiany jako ostatni, największe ryzyko RAM)
        if run_all or args.pracuj_only:
            tracker.start_scraper("Pracuj.pl")
            result = run_pracuj(dry_run=args.dry_run, progress_callback=tracker.update)
            results["Pracuj.pl"] = result
            tracker.finish_scraper("Pracuj.pl", result)
            monitor_scraper("Pracuj.pl", result, dry_run=args.dry_run)

    except Exception as e:
        fatal_error = f"{type(e).__name__}: {e}"
        print(f"\n  [MONITOR] FATAL ERROR: {fatal_error}")
        traceback.print_exc()

    # --- Podsumowanie ---
    elapsed = datetime.now() - start_time
    elapsed_str = str(elapsed).split(".")[0]  # HH:MM:SS bez mikrosekund

    print(f"\n{'='*70}")
    print("  MONITOR - PODSUMOWANIE")
    print(f"{'='*70}")
    all_ok = True
    for name, r in results.items():
        status = "OK" if r.get("success") else "FAIL"
        if not r.get("success"):
            all_ok = False
        print(f"  {name:20s} [{status}] {r.get('total_offers', 0)} ofert")

    if fatal_error:
        all_ok = False

    # Sprawdź czy wszystkie planowane scrapery się uruchomiły
    missing_scrapers = [s for s in scrapers_planned if s not in results]
    if missing_scrapers:
        all_ok = False
        print(f"\n  [MONITOR] UWAGA: nie ukończono: {', '.join(missing_scrapers)}")

    # Sprawdź czy wszystkie oczekiwane scrapery uruchomiono dzisiaj
    today_date = datetime.now().strftime("%Y-%m-%d")
    history_for_check = load_history()
    today_ran = {
        h["scraper"] for h in history_for_check
        if (h.get("timestamp", "") or "").startswith(today_date)
    }
    missing_today = [s for s in EXPECTED_SCRAPERS if s not in today_ran]
    if missing_today:
        all_ok = False
        print(f"\n  [MONITOR] UWAGA: nie uruchomiono dzisiaj: {', '.join(missing_today)}")

    print(f"\n  Czas trwania: {elapsed_str}")
    if all_ok:
        print("  Wszystko działa poprawnie.")
    else:
        print("  Wykryto problemy - sprawdź alerty powyżej.")

    # --- Email FINISH (wysyłany ZAWSZE - nawet po crash) ---
    if not args.dry_run and (results or fatal_error):
        email_config = get_email_config()
        if email_config:
            status_label = "SUCCESS" if all_ok else "FAILURE"
            subject = f"[{status_label}] Daily Report {datetime.now().strftime('%Y-%m-%d')} ({elapsed_str}) — Portfolio Data Factory"
            if fatal_error:
                # Dodaj info o fatal error do raportu
                crash_note = f'<p style="color:#dc3545;font-weight:bold;">FATAL: {fatal_error}</p>'
                body = build_daily_report_html(results, history_for_check)
                body = body.replace("</h2>", f"</h2>{crash_note}", 1)
            else:
                body = build_daily_report_html(results, history_for_check)
            send_email(subject, body, email_config)
        else:
            print("\n  [MONITOR] Brak konfiguracji email - nie wysłano raportu dziennego.")
            print("  Skonfiguruj ALERT_EMAIL_FROM/PASSWORD/TO w pliku .env")

    tracker.finish_run()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
