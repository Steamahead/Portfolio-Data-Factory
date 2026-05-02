"""Inflation Basket scrape monitor (Task Scheduler entry point).

Runs both stores in sequence, validates the saved row counts against the
expected coverage, and emits email reports.

Email config (re-uses pracuj_scraper convention via .env):
  ALERT_EMAIL_FROM      — Gmail account
  ALERT_EMAIL_PASSWORD  — Gmail App Password
  ALERT_EMAIL_TO        — recipient

Triggers an alert when:
  - any store crashes (raises) or returns 0 rows
  - saved < 90 % of active URLs (cross-store data integrity gate)

CLI:
  python -m inflation_basket.scrape_monitor              # full run
  python -m inflation_basket.scrape_monitor --dry-run    # no email send
  python -m inflation_basket.scrape_monitor --test-email # smtp sanity check
"""

from __future__ import annotations

import argparse
import os
import smtplib
import sys
import time
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from inflation_basket.scrape import scrape_store, VALID_STORES

PROJECT_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_DIR / ".env"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Coverage gate — alert when saved/active < this fraction.
COVERAGE_THRESHOLD = 0.90


def load_env() -> None:
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def email_config() -> dict | None:
    f = os.environ.get("ALERT_EMAIL_FROM", "").strip()
    p = os.environ.get("ALERT_EMAIL_PASSWORD", "").strip()
    t = os.environ.get("ALERT_EMAIL_TO", "").strip()
    if not all([f, p, t]):
        return None
    return {"from": f, "password": p, "to": t}


def send_email(subject: str, html: str, cfg: dict) -> bool:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["from"]
    msg["To"] = cfg["to"]
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as s:
            s.ehlo(); s.starttls(); s.ehlo()
            s.login(cfg["from"], cfg["password"])
            s.sendmail(cfg["from"], cfg["to"], msg.as_string())
        print(f"  [EMAIL] sent to {cfg['to']}")
        return True
    except Exception as e:
        print(f"  [EMAIL FAIL] {e}")
        return False


def evaluate(results: list[dict]) -> tuple[bool, list[str]]:
    """Return (all_ok, problems)."""
    problems: list[str] = []
    for r in results:
        store = r.get("store", "?")
        if "error" in r:
            problems.append(f"{store}: hard error — {r['error']}")
            continue
        active = r.get("active_products", 0)
        saved = r.get("saved", 0)
        if active == 0:
            problems.append(f"{store}: 0 active URLs (catalog empty?)")
            continue
        ratio = saved / active
        if saved == 0:
            problems.append(f"{store}: 0 rows saved (scraper crashed silently)")
        elif ratio < COVERAGE_THRESHOLD:
            problems.append(
                f"{store}: only {saved}/{active} saved "
                f"({100*ratio:.0f}% < {100*COVERAGE_THRESHOLD:.0f}% threshold)"
            )
    return (not problems, problems)


def build_html(results: list[dict], problems: list[str], elapsed_s: float) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    ok = not problems
    color = "#28a745" if ok else "#dc3545"
    icon = "OK" if ok else "ALERT"
    rows_html = ""
    for r in results:
        store = r.get("store", "?")
        active = r.get("active_products", 0)
        saved = r.get("saved", 0)
        errs = r.get("errors", 0)
        ratio = (100 * saved / active) if active else 0
        row_color = "#28a745" if (active and saved / active >= COVERAGE_THRESHOLD) else "#dc3545"
        sample = r.get("error_samples") or []
        err_html = ""
        if sample:
            items = "".join(f"<li>ID {pid}: {name} — {reason}</li>" for pid, name, reason in sample[:5])
            err_html = f'<ul style="margin:4px 0;color:#888;font-size:12px;">{items}</ul>'
        rows_html += f"""
        <tr style="border-top:1px solid #eee;">
          <td style="padding:8px;font-weight:bold;">{store}</td>
          <td style="padding:8px;color:{row_color};">{saved}/{active} ({ratio:.0f}%)</td>
          <td style="padding:8px;color:#888;">{errs} err</td>
        </tr>
        <tr><td colspan="3">{err_html}</td></tr>
        """
    problems_html = ""
    if problems:
        problems_html = "<h3 style='color:#dc3545;'>Problems</h3><ul>" + \
            "".join(f"<li>{p}</li>" for p in problems) + "</ul>"
    return f"""<html><body style="font-family:Segoe UI,Arial;max-width:600px;color:#333;">
    <h2 style="color:{color};">[{icon}] Inflation Basket — {ts}</h2>
    <p>Total elapsed: {elapsed_s:.1f}s</p>
    <table style="border-collapse:collapse;width:100%;background:#f9f9f9;">{rows_html}</table>
    {problems_html}
    <hr><p style="color:gray;font-size:12px;">Portfolio Data Factory — Inflation Basket monitor</p>
    </body></html>"""


def test_email_send():
    load_env()
    cfg = email_config()
    if not cfg:
        print("[FAIL] missing ALERT_EMAIL_FROM/PASSWORD/TO in .env")
        return 1
    html = "<p>Inflation basket monitor: SMTP sanity check OK.</p>"
    ok = send_email(f"[TEST] inflation_basket {datetime.now():%Y-%m-%d %H:%M}", html, cfg)
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="run scrapes but skip email")
    ap.add_argument("--test-email", action="store_true", help="send a test email and exit")
    args = ap.parse_args()

    if args.test_email:
        return test_email_send()

    load_env()
    t0 = time.time()
    results: list[dict] = []
    for store in VALID_STORES:
        try:
            print(f"\n>>> scraping {store}")
            results.append(scrape_store(store))
        except Exception as e:
            tb = traceback.format_exc(limit=2)
            print(f"[CRASH] {store}: {e}\n{tb}")
            results.append({"store": store, "error": str(e)[:200]})

    elapsed = time.time() - t0
    all_ok, problems = evaluate(results)

    print("\n=== Monitor summary ===")
    for r in results:
        print(" ", r)
    print(f"  problems: {problems or 'none'}")
    print(f"  elapsed: {elapsed:.1f}s")

    if args.dry_run:
        print("[DRY-RUN] skipping email")
        return 0 if all_ok else 1

    cfg = email_config()
    if not cfg:
        print("[WARN] no email config — skipping notification")
        return 0 if all_ok else 1

    label = "OK" if all_ok else "ALERT"
    subject = f"[{label}] inflation_basket {datetime.now():%Y-%m-%d}"
    send_email(subject, build_html(results, problems, elapsed), cfg)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
