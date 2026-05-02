"""Inflation Basket scrape monitor (Task Scheduler entry point).

Runs both stores, builds a structured quality report (quality_report.py),
asks Gemini Flash-Lite to grade it (llm_review.py), and emails the
verdict + raw metrics. Subject prefix reflects the LLM verdict so you
can decide at a glance whether to open the mail or zlej.

Email config (re-uses pracuj_scraper convention via .env):
  ALERT_EMAIL_FROM      - Gmail account
  ALERT_EMAIL_PASSWORD  - Gmail App Password
  ALERT_EMAIL_TO        - recipient
  GEMINI_API_KEY        - for the LLM review (optional, falls back to threshold-only verdict)

CLI:
  python -m inflation_basket.scrape_monitor              # full run
  python -m inflation_basket.scrape_monitor --dry-run    # scrape + report, no email
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
from inflation_basket.quality_report import build_quality_report
from inflation_basket.llm_review import review_quality

PROJECT_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_DIR / ".env"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


def load_env() -> None:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

    # Fallback: pull missing keys from local.settings.json (Azure Functions config).
    # Only fills in env vars not already set by the OS or .env above.
    settings_file = PROJECT_DIR / "local.settings.json"
    if settings_file.exists():
        try:
            import json
            data = json.loads(settings_file.read_text(encoding="utf-8"))
            for k, v in (data.get("Values") or {}).items():
                if isinstance(v, str):
                    os.environ.setdefault(k, v)
        except Exception:
            pass


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


def _severity_color(sev: str) -> str:
    return {"ok": "#28a745", "warning": "#ffc107", "critical": "#dc3545"}.get(sev, "#888")


def _severity_icon(sev: str) -> str:
    return {"ok": "OK", "warning": "WARN", "critical": "ALERT"}.get(sev, "?")


def build_html(results: list[dict], report: dict, verdict: dict, elapsed_s: float) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    sev = verdict.get("severity", "ok")
    color = _severity_color(sev)
    icon = _severity_icon(sev)
    intervention = " - INTERVENTION" if verdict.get("needs_intervention") else ""

    # Per-store summary
    rows_html = ""
    for r in results:
        store = r.get("store", "?")
        active = r.get("active_products", 0)
        saved = r.get("saved", 0)
        errs = r.get("errors", 0)
        ratio = (100 * saved / active) if active else 0
        row_color = "#28a745" if (active and saved == active) else "#dc3545"
        sample = r.get("error_samples") or []
        err_html = ""
        if sample:
            items = "".join(f"<li>ID {pid}: {name} - {reason}</li>" for pid, name, reason in sample[:5])
            err_html = f'<ul style="margin:4px 0;color:#888;font-size:12px;">{items}</ul>'
        rows_html += f"""
        <tr style="border-top:1px solid #eee;">
          <td style="padding:8px;font-weight:bold;">{store}</td>
          <td style="padding:8px;color:{row_color};">{saved}/{active} ({ratio:.0f}%)</td>
          <td style="padding:8px;color:#888;">{errs} err</td>
        </tr>
        <tr><td colspan="3">{err_html}</td></tr>
        """

    # LLM concerns
    concerns_html = ""
    for c in verdict.get("concerns", []):
        c_color = _severity_color(c.get("severity", "warning"))
        concerns_html += f"""
        <li style="margin-bottom:10px;">
          <span style="color:{c_color};font-weight:bold;">[{c.get('severity', 'warn').upper()}]</span> {c.get('what', '')}<br>
          <span style="color:#666;font-size:13px;">Why: {c.get('why', '')}</span><br>
          <span style="color:#0066cc;font-size:13px;">Action: {c.get('action', '')}</span>
        </li>
        """
    concerns_section = ""
    if concerns_html:
        concerns_section = f"<h3>Concerns z analizy LLM</h3><ul style='padding-left:20px;'>{concerns_html}</ul>"

    # Detailed metric tables (collapsible-style summaries)
    def _tbl(title: str, rows: list[dict], cols: list[tuple[str, str]]) -> str:
        if not rows:
            return ""
        head = "".join(f"<th style='text-align:left;padding:4px 8px;background:#eee;'>{label}</th>" for _, label in cols)
        body = ""
        for row in rows[:10]:
            cells = "".join(f"<td style='padding:4px 8px;'>{row.get(key, '')}</td>" for key, _ in cols)
            body += f"<tr>{cells}</tr>"
        return f"<h4>{title} ({len(rows)})</h4><table style='border-collapse:collapse;font-size:12px;'><tr>{head}</tr>{body}</table>"

    metrics_html = ""
    metrics_html += _tbl("Brakujące dziś", report.get("missing_today", []),
                         [("product_id", "ID"), ("name", "Nazwa"), ("store", "Sklep"),
                          ("days_since", "Dni od last"), ("severity", "Sev")])
    metrics_html += _tbl("Skoki cen vs avg(7d)", report.get("price_moves", []),
                         [("name", "Nazwa"), ("store", "Sklep"), ("avg7d", "Avg7d"),
                          ("current", "Teraz"), ("pct_change", "%"), ("severity", "Sev")])
    metrics_html += _tbl("Stale prices", report.get("stale_prices", []),
                         [("name", "Nazwa"), ("store", "Sklep"), ("price", "Cena"),
                          ("cycles_same", "Cykli"), ("severity", "Sev")])
    metrics_html += _tbl("Shrinkflation candidates", report.get("shrinkflation", []),
                         [("name", "Nazwa"), ("store", "Sklep"),
                          ("capacity_prev", "Cap prev"), ("capacity_now", "Cap now"),
                          ("price_change_pct", "Δ price %"), ("severity", "Sev")])
    metrics_html += _tbl("Cross-store anomalie", report.get("cross_store_anomalies", []),
                         [("name", "Nazwa"), ("frisco_price", "Frisco"),
                          ("auchan_price", "Auchan"), ("delta_pct", "Δ %"), ("severity", "Sev")])

    # Promo flips one-liner
    flips = report.get("promo_flips", {})
    flips_html = ""
    if flips:
        parts = [f"{s}: +{v['entered']} / -{v['left']}" for s, v in flips.items()]
        flips_html = f"<p style='font-size:12px;color:#666;'>Promo flips: {' | '.join(parts)}</p>"

    return f"""<html><body style="font-family:Segoe UI,Arial;max-width:700px;color:#333;">
    <h2 style="color:{color};">[{icon}{intervention}] Inflation Basket — {ts}</h2>
    <p style="background:#f0f0f0;padding:10px;border-left:4px solid {color};font-size:14px;">
      <strong>Werdykt LLM:</strong> {verdict.get('summary_pl', '(brak)')}
    </p>
    <p>Czas: {elapsed_s:.1f}s</p>

    <h3>Coverage per store</h3>
    <table style="border-collapse:collapse;width:100%;background:#f9f9f9;">{rows_html}</table>
    {flips_html}

    {concerns_section}

    <h3>Szczegóły metryk</h3>
    {metrics_html or '<p style="color:#888;">(brak anomalii powyżej progów)</p>'}

    <hr><p style="color:gray;font-size:12px;">Portfolio Data Factory - Inflation Basket monitor · model: gemini-3.1-flash-lite-preview</p>
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

    print("\n=== Building quality report ===")
    try:
        report = build_quality_report(results)
    except Exception as e:
        print(f"[CRASH] quality_report: {e}")
        report = {"scrape_date": datetime.now().date().isoformat(), "scrape_results": results, "error": str(e)[:200]}

    print("=== LLM review ===")
    try:
        verdict = review_quality(report)
    except Exception as e:
        print(f"[CRASH] llm_review: {e}")
        verdict = {"severity": "critical", "needs_intervention": True,
                   "summary_pl": f"LLM review crashed: {str(e)[:120]}",
                   "concerns": []}

    sev = verdict.get("severity", "ok")
    needs = verdict.get("needs_intervention", False)
    print(f"  verdict: {sev} (needs_intervention={needs})")
    print(f"  summary: {verdict.get('summary_pl', '')[:200]}")
    print(f"  concerns: {len(verdict.get('concerns', []))}")
    print(f"  elapsed total: {elapsed:.1f}s")

    if args.dry_run:
        print("[DRY-RUN] skipping email")
        return 0 if sev != "critical" else 1

    cfg = email_config()
    if not cfg:
        print("[WARN] no email config — skipping notification")
        return 0 if sev != "critical" else 1

    label = _severity_icon(sev) + (" - INTERVENTION" if needs else "")
    subject = f"[{label}] inflation_basket {datetime.now():%Y-%m-%d}"
    send_email(subject, build_html(results, report, verdict, elapsed), cfg)
    return 0 if sev != "critical" else 1


if __name__ == "__main__":
    sys.exit(main())
