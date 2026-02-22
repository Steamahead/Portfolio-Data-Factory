"""
CEE FX Volatility Pipeline — Orchestrator
==========================================
"Zloty pod Presja — CEE Edition"

Bada Spillover Effect zmiennosci walutowej w regionie CEE.
Hipoteza: szoki na PLN przenosza sie na CZK i HUF.

Dwa niezalezne strumienie:
  1. FX: kursy EUR/PLN, EUR/CZK, EUR/HUF (yfinance, 1h)
  2. Newsy: naglowki z polskich zrodel RSS + klasyfikacja Gemini

Usage:
  python cee_fx_volatility/main.py                  # biezacy okres (FX 5d + newsy)
  python cee_fx_volatility/main.py --backfill 30    # historyczne FX z ostatnich 30 dni
  python cee_fx_volatility/main.py --fx-only         # tylko kursy walut
  python cee_fx_volatility/main.py --news-only       # tylko newsy
"""

import argparse
import logging
import os
import smtplib
import sys
import traceback
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Windows UTF-8 fix
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

logger = logging.getLogger("cee_fx_volatility")

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


# ── Email alerts (same pattern as scraper_monitor) ─────────────────

def _get_email_config() -> dict | None:
    """Load email config from env. Returns None if not configured."""
    email_from = os.environ.get("ALERT_EMAIL_FROM", "").strip()
    password = os.environ.get("ALERT_EMAIL_PASSWORD", "").strip()
    email_to = os.environ.get("ALERT_EMAIL_TO", "").strip()

    if not all([email_from, password, email_to]):
        return None

    return {"from": email_from, "password": password, "to": email_to}


def _send_email(subject: str, body_html: str, config: dict) -> bool:
    """Send email via Gmail SMTP. Returns True on success."""
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
        print(f"  [EMAIL] Alert wyslany na {config['to']}")
        return True
    except Exception as e:
        print(f"  [EMAIL FAIL] Nie udalo sie wyslac: {e}")
        return False


def _build_alert_html(result: dict, errors: list[str]) -> str:
    """Build HTML email body for pipeline failure alert."""
    ts = result.get("timestamp", "?")

    error_rows = ""
    for err in errors[:20]:
        error_rows += f'<tr><td style="padding:4px 8px;border:1px solid #ddd;">{err}</td></tr>\n'

    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#dc3545;">&#128680; CEE FX Volatility — Pipeline Alert</h2>
    <p><strong>Czas:</strong> {ts}</p>

    <table style="border-collapse:collapse;width:100%;margin:10px 0;">
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>FX pobrano</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['fx_fetched']}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>FX upload</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['fx_uploaded']}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>News pobrano</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['news_fetched']}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>News upload</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['news_uploaded']}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Sklasyfikowano</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['news_classified']}</td>
      </tr>
    </table>

    <h3 style="color:#dc3545;">Bledy ({len(errors)}):</h3>
    <table style="border-collapse:collapse;width:100%;">
      {error_rows}
    </table>

    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: cee_fx_volatility | Azure Function: CeeFxDailyRun
    </p>
    </body></html>
    """


def _build_success_html(result: dict) -> str:
    """Build HTML email body for daily success report."""
    ts = result.get("timestamp", "?")

    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#28a745;">&#9989; CEE FX Volatility — OK</h2>
    <p><strong>Czas:</strong> {ts}</p>

    <table style="border-collapse:collapse;width:100%;margin:10px 0;">
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>FX upload</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['fx_uploaded']} rekordow</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>News upload</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['news_uploaded']} naglowkow</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Sklasyfikowano</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['news_classified']} przez Gemini</td>
      </tr>
    </table>

    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: cee_fx_volatility | Azure Function: CeeFxDailyRun
    </p>
    </body></html>
    """


def _send_alert(result: dict) -> None:
    """Send email alert based on pipeline result."""
    # Ensure env is loaded (for Azure Function context)
    from .db.operations import _load_env
    _load_env()

    email_config = _get_email_config()
    if not email_config:
        print("  [EMAIL] Brak konfiguracji email w .env — pomijam alert")
        return

    errors = result.get("fx_errors", []) + result.get("news_errors", [])

    if not result["success"]:
        subject = f"[ALERT] CEE FX Pipeline FAIL ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_alert_html(result, errors)
        _send_email(subject, body, email_config)
    elif errors:
        subject = f"[WARN] CEE FX Pipeline OK z bledami ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_alert_html(result, errors)
        _send_email(subject, body, email_config)


# ── Pipeline streams ───────────────────────────────────────────────

def _run_fx_pipeline(backfill_days: int | None = None) -> dict:
    """Run FX data collection pipeline."""
    from .collectors.fx_collector import fetch_fx_data
    from .db.operations import upload_fx_rates

    print(f"\n{'─' * 55}")
    print("  STRUMIEN FX — Kursy walut CEE")
    print(f"{'─' * 55}")

    if backfill_days:
        print(f"  Tryb: backfill {backfill_days} dni")
    else:
        print("  Tryb: biezacy (ostatnie 5 dni)")

    records = fetch_fx_data(backfill_days=backfill_days)
    if not records:
        return {"fx_fetched": 0, "fx_uploaded": 0, "fx_errors": ["Brak danych FX"]}

    upload_result = upload_fx_rates(records)
    return {
        "fx_fetched": len(records),
        "fx_uploaded": upload_result["uploaded"],
        "fx_errors": upload_result["errors"],
    }


def _run_news_pipeline() -> dict:
    """Run news collection + AI classification pipeline."""
    from .collectors.news_collector import fetch_news
    from .ai.classifier import classify_batch
    from .db.operations import upload_news

    print(f"\n{'─' * 55}")
    print("  STRUMIEN NEWS — Polskie naglowki finansowe")
    print(f"{'─' * 55}")

    records = fetch_news()
    if not records:
        return {"news_fetched": 0, "news_classified": 0, "news_uploaded": 0, "news_errors": ["Brak newsow"]}

    # Classify headlines with Gemini (graceful — failure = None fields)
    records = classify_batch(records)
    classified = sum(1 for r in records if r.get("category") is not None)

    upload_result = upload_news(records)
    return {
        "news_fetched": len(records),
        "news_classified": classified,
        "news_uploaded": upload_result["uploaded"],
        "news_errors": upload_result["errors"],
    }


# ── Orchestrator ───────────────────────────────────────────────────

def run(backfill_days: int | None = None, fx_only: bool = False, news_only: bool = False) -> dict:
    """
    Main pipeline. Runs FX and News streams independently.
    Failure of one stream does not stop the other.
    Sends email alert on failure or partial errors.

    Returns:
        Result dict with metrics from both streams.
    """
    result = {
        "success": False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fx_fetched": 0,
        "fx_uploaded": 0,
        "fx_errors": [],
        "news_fetched": 0,
        "news_classified": 0,
        "news_uploaded": 0,
        "news_errors": [],
    }

    # Stream 1: FX
    if not news_only:
        try:
            fx_result = _run_fx_pipeline(backfill_days=backfill_days)
            result.update(fx_result)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"\n  [FX] KRYTYCZNY BLAD:\n{tb}")
            result["fx_errors"].append(f"Unhandled: {e}")

    # Stream 2: News (backfill nie dotyczy newsow — RSS nie ma paginacji)
    if not fx_only:
        try:
            news_result = _run_news_pipeline()
            result.update(news_result)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"\n  [NEWS] KRYTYCZNY BLAD:\n{tb}")
            result["news_errors"].append(f"Unhandled: {e}")

    # Overall success if at least one stream delivered data
    result["success"] = result["fx_uploaded"] > 0 or result["news_uploaded"] > 0

    # Summary
    print(f"\n{'═' * 55}")
    print("  PODSUMOWANIE")
    print(f"{'═' * 55}")
    print(f"  FX:    {result['fx_uploaded']} rekordow → Azure SQL")
    print(f"  News:  {result['news_uploaded']} naglowkow → Azure SQL")
    print(f"         {result['news_classified']} sklasyfikowanych przez Gemini")

    errors = result["fx_errors"] + result["news_errors"]
    if errors:
        print(f"  Bledy: {len(errors)}")
        for err in errors[:5]:
            print(f"    - {err}")

    print(f"  Status: {'OK' if result['success'] else 'FAIL'}")
    print(f"{'═' * 55}\n")

    # Email alert on failure or errors
    try:
        _send_alert(result)
    except Exception as e:
        print(f"  [EMAIL] Blad wysylki alertu: {e}")

    return result


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="CEE FX Volatility Pipeline — Zloty pod Presja",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Przyklady:
  python -X utf8 cee_fx_volatility/main.py                  # biezacy okres
  python -X utf8 cee_fx_volatility/main.py --backfill 30    # FX z ostatnich 30 dni
  python -X utf8 cee_fx_volatility/main.py --fx-only        # tylko kursy
  python -X utf8 cee_fx_volatility/main.py --news-only      # tylko newsy
        """,
    )
    parser.add_argument(
        "--backfill",
        type=int,
        default=None,
        metavar="N",
        help="Pobierz historyczne dane FX z ostatnich N dni (max 730). Dotyczy WYLACZNIE yfinance.",
    )
    parser.add_argument(
        "--fx-only",
        action="store_true",
        help="Uruchom tylko strumien FX (bez newsow)",
    )
    parser.add_argument(
        "--news-only",
        action="store_true",
        help="Uruchom tylko strumien newsow (bez FX)",
    )

    args = parser.parse_args()

    if args.fx_only and args.news_only:
        print("  [!] --fx-only i --news-only wzajemnie sie wykluczaja")
        sys.exit(1)

    print(f"\n{'═' * 55}")
    print("  CEE FX Volatility Pipeline")
    print("  Zloty pod Presja — CEE Edition")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═' * 55}")

    result = run(
        backfill_days=args.backfill,
        fx_only=args.fx_only,
        news_only=args.news_only,
    )

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
