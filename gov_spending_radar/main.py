"""
Gov Spending Radar — orchestrator + CLI entry point.
=====================================================
Collects Polish public procurement data from BZP (Biuletyn Zamówień Publicznych)
and uploads to Azure SQL. Classifies notices by sector using CPV codes and title keywords,
with optional Gemini LLM second pass for unclassified notices.

Usage (from project root):
    python -X utf8 -m gov_spending_radar.main                   # yesterday's notices
    python -X utf8 -m gov_spending_radar.main --backfill 30     # last 30 days
    python -X utf8 -m gov_spending_radar.main --classify         # CPV+keyword + LLM (two-pass)
    python -X utf8 -m gov_spending_radar.main --classify-llm     # LLM only (skip CPV+keyword)
    python -X utf8 -m gov_spending_radar.main --sample 5         # dry-run: fetch 1 day, no SQL
    python -X utf8 -m gov_spending_radar.main --date 2026-02-20  # specific date
"""

import argparse
import json
import os
import re
import smtplib
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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
    fetch_unclassified_for_llm,
)
from .config.classification_rules import (
    KEYWORD_RULES,
    CPV_IT_PREFIXES,
    CPV_NON_IT_HARD,
    CPV_SECTOR_MAP,
)

CONFIG_PATH = Path(__file__).parent / "config.yaml"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


# ── Email alerts (same pattern as cee_fx_volatility + scraper_monitor) ──

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


def _build_start_html(mode: str) -> str:
    """Build HTML email body for pipeline start notification."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#007bff;">&#9654; Gov Spending Radar — START</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <p><strong>Tryb:</strong> {mode}</p>
    <p>Pipeline Gov Spending Radar zostal uruchomiony.</p>
    <p style="color:gray;font-size:12px;">
      Jesli nie otrzymasz maila FINISH w ciagu kilkunastu minut, sprawdz logi.
    </p>
    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: gov_spending_radar | Azure Function: GovSpendingRun
    </p>
    </body></html>
    """


def _build_success_html(result: dict) -> str:
    """Build HTML email body for pipeline success report."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#28a745;">&#9989; Gov Spending Radar — OK</h2>
    <p><strong>Czas:</strong> {ts}</p>

    <table style="border-collapse:collapse;width:100%;margin:10px 0;">
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Ogloszenia</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['notices_uploaded']} rekordow</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Wykonawcy</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['contractors_uploaded']} rekordow</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Klasyfikacje</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result['classifications_uploaded']} rekordow</td>
      </tr>
    </table>

    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: gov_spending_radar | Azure Function: GovSpendingRun
    </p>
    </body></html>
    """


def _build_alert_html(result: dict, errors: list[str]) -> str:
    """Build HTML email body for pipeline failure alert."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    error_rows = ""
    for err in errors[:20]:
        error_rows += f'<tr><td style="padding:4px 8px;border:1px solid #ddd;">{err}</td></tr>\n'

    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#dc3545;">&#128680; Gov Spending Radar — Pipeline Alert</h2>
    <p><strong>Czas:</strong> {ts}</p>

    <table style="border-collapse:collapse;width:100%;margin:10px 0;">
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Ogloszenia</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result.get('notices_uploaded', 0)}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Wykonawcy</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result.get('contractors_uploaded', 0)}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Klasyfikacje</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{result.get('classifications_uploaded', 0)}</td>
      </tr>
    </table>

    <h3 style="color:#dc3545;">Bledy ({len(errors)}):</h3>
    <table style="border-collapse:collapse;width:100%;">
      {error_rows}
    </table>

    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: gov_spending_radar | Azure Function: GovSpendingRun
    </p>
    </body></html>
    """


def _send_start_email(mode: str) -> None:
    """Send pipeline start notification email."""
    email_config = _get_email_config()
    if not email_config:
        return

    subject = f"[START] Gov Spending Radar ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
    body = _build_start_html(mode)
    _send_email(subject, body, email_config)


def _send_finish_email(result: dict) -> None:
    """Send email with pipeline result — always sends (success or failure)."""
    email_config = _get_email_config()
    if not email_config:
        print("  [EMAIL] Brak konfiguracji email — pomijam alert")
        return

    errors = result.get("errors", [])

    if not result["success"]:
        subject = f"[FAIL] Gov Spending Radar ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_alert_html(result, errors)
        _send_email(subject, body, email_config)
    elif errors:
        subject = f"[WARN] Gov Spending Radar OK z bledami ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_alert_html(result, errors)
        _send_email(subject, body, email_config)
    else:
        subject = f"[SUCCESS] Gov Spending Radar ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_success_html(result)
        _send_email(subject, body, email_config)


def _load_config() -> dict:
    """Load config.yaml with CPV sector mappings."""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── 3-Layer Classifier (v2.0) ───────────────────────────────────

def _extract_cpv_codes(cpv_raw: str | None) -> list[str]:
    """Extract all CPV codes (digits only) from cpv_raw field."""
    if not cpv_raw:
        return []
    return [code.replace("-", "") for code in re.findall(r"(\d{8}-\d)", cpv_raw)]


def _cpv_matches_any(cpv_codes: list[str], prefixes: list[str]) -> bool:
    """Check if any CPV code starts with any of the given prefixes."""
    for code in cpv_codes:
        for prefix in prefixes:
            if code.startswith(prefix):
                return True
    return False


def classify_notice_multilabel(
    title: str,
    cpv_code: str | None,
    cpv_raw: str | None,
) -> list[dict]:
    """
    3-layer classifier producing 0-N classification records per notice.

    Layer 1 (KEYWORD): title matching with negative phrase exclusion.
    Layer 2 (CPV): boost/penalize keyword matches; CPV-only for unmatched.
    Layer 3 (LLM): skipped (preserved in ai/classifier.py for future use).

    Returns:
        List of dicts with keys: method, sector, confidence.
        Empty list if no classification.
    """
    title_lower = (title or "").lower()
    cpv_codes = _extract_cpv_codes(cpv_raw)
    results = []

    # ── Layer 1: KEYWORD ──
    keyword_matched_sectors: set[str] = set()

    for sector, rules in KEYWORD_RULES.items():
        # Check negative phrases first — skip if any match
        negatives = rules.get("negative_phrases", [])
        if any(neg.lower() in title_lower for neg in negatives):
            continue

        # Check positive phrases
        phrases = rules.get("phrases", [])
        if any(phrase.lower() in title_lower for phrase in phrases):
            keyword_matched_sectors.add(sector)
            results.append({
                "method": "KEYWORD",
                "sector": sector,
                "confidence": 0.5,
            })

    # ── Layer 2: CPV ──
    if keyword_matched_sectors:
        # For each keyword-matched sector, boost or penalize
        for sector in keyword_matched_sectors:
            if _cpv_matches_any(cpv_codes, CPV_IT_PREFIXES):
                results.append({
                    "method": "CPV_BOOST",
                    "sector": sector,
                    "confidence": 0.75,
                })
            elif _cpv_matches_any(cpv_codes, CPV_NON_IT_HARD):
                # Exception: AI + CPV "33" (medical) + "sztuczna inteligencja" in title
                if (sector == "AI"
                        and _cpv_matches_any(cpv_codes, ["33"])
                        and "sztuczna inteligencja" in title_lower):
                    continue  # don't penalize
                results.append({
                    "method": "CPV_PENALTY",
                    "sector": sector,
                    "confidence": 0.2,
                })
            # else: neutral CPV — keyword confidence 0.5 stands, no extra record
    else:
        # No keyword match — check CPV-only classifications
        if _cpv_matches_any(cpv_codes, CPV_IT_PREFIXES):
            results.append({
                "method": "CPV_ONLY",
                "sector": "IT_GENERAL",
                "confidence": 0.4,
            })

        for sector, prefixes in CPV_SECTOR_MAP.items():
            if _cpv_matches_any(cpv_codes, prefixes):
                results.append({
                    "method": "CPV_ONLY",
                    "sector": sector,
                    "confidence": 0.85,
                })

    return results


def classify_batch_multilabel(notices: list[dict]) -> list[dict]:
    """
    Classify a batch of notices using 3-layer logic. Returns classification
    records ready for SQL (multi-label: 0-N records per notice).
    """
    results = []
    for notice in notices:
        classifications = classify_notice_multilabel(
            title=notice.get("title", ""),
            cpv_code=notice.get("cpv_code"),
            cpv_raw=notice.get("cpv_raw"),
        )
        for cls in classifications:
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
    llm_only: bool = False,
) -> dict:
    """
    Main entry point (also callable from Azure Function).

    Args:
        mode: "daily" | "backfill" | "classify"
        days_back: Number of days for backfill mode
        target_date: Specific date for daily mode
        sample: If set, fetch but don't upload (dry-run)
        window_hours: Time window size for API pagination
        llm_only: If True, skip CPV+keyword pass and run only LLM classification

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

    # Email START
    try:
        _send_start_email(mode)
    except Exception as e:
        print(f"  [EMAIL] Blad wysylki START: {e}")

    try:
        if mode == "classify":
            classify_result = _run_classify(config, llm_only=llm_only)
            try:
                _send_finish_email(classify_result)
            except Exception as e:
                print(f"  [EMAIL] Blad wysylki FINISH: {e}")
            return classify_result

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
        print(f"\n[GOV] Klasyfikacja {len(notices)} ogłoszeń (3-layer v2.0)...")
        classifications = classify_batch_multilabel(notices)
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

    # Email FINISH (always — success, warning, or failure)
    try:
        _send_finish_email(result)
    except Exception as e:
        print(f"  [EMAIL] Blad wysylki FINISH: {e}")

    return result


def _run_classify(config: dict, llm_only: bool = False) -> dict:
    """
    Two-pass classification pipeline.
    Pass 1 (CPV+keyword): notices with no classification at all (skipped if llm_only).
    Pass 2 (Gemini LLM): notices without llm_gemini classification.
    """
    result = {
        "success": False,
        "notices_uploaded": 0,
        "contractors_uploaded": 0,
        "classifications_uploaded": 0,
        "errors": [],
    }

    total_uploaded = 0

    # ── Pass 1: 3-layer (KEYWORD + CPV) ──
    if not llm_only:
        print("\n[GOV] Pass 1/2: 3-layer (KEYWORD + CPV) — pobieranie niesklasyfikowanych ogłoszeń...")
        unclassified = fetch_unclassified_notices()

        if unclassified:
            print(f"  [GOV] Znaleziono {len(unclassified)} ogłoszeń bez klasyfikacji")
            classifications = classify_batch_multilabel(unclassified)
            print(f"  [GOV] 3-layer: {len(classifications)} rekordów z {len(unclassified)} ogłoszeń")

            if classifications:
                cl_result = upload_classifications(classifications)
                total_uploaded += cl_result["uploaded"]
                result["errors"].extend(cl_result["errors"])
        else:
            print("  [GOV] Brak ogłoszeń do klasyfikacji")
    else:
        print("\n[GOV] Pominięto Pass 1 (KEYWORD+CPV) — tryb --classify-llm")

    # ── Pass 2: Gemini LLM ──
    print(f"\n[GOV] Pass 2/2: Gemini LLM — pobieranie ogłoszeń bez klasyfikacji LLM...")
    llm_unclassified = fetch_unclassified_for_llm()

    if llm_unclassified:
        print(f"  [GOV] Znaleziono {len(llm_unclassified)} ogłoszeń do klasyfikacji LLM")

        from .ai.classifier import classify_batch as llm_classify_batch
        llm_classifications = llm_classify_batch(llm_unclassified)

        if llm_classifications:
            cl_result = upload_classifications(llm_classifications)
            total_uploaded += cl_result["uploaded"]
            result["errors"].extend(cl_result["errors"])
    else:
        print("  [GOV] Brak ogłoszeń do klasyfikacji LLM")

    result["classifications_uploaded"] = total_uploaded
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

    # HTML parser stats
    result_notices = [n for n in notices if n.get('notice_type') == 'TenderResultNotice']
    contract_notices = [n for n in notices if n.get('notice_type') == 'ContractNotice']
    if result_notices:
        print(f"\nHTML parser — TenderResultNotice ({len(result_notices)}):")
        for field in ['budget_estimated', 'final_price', 'offers_count', 'lowest_price',
                       'highest_price', 'contract_value', 'description']:
            count = sum(1 for n in result_notices if n.get(field) is not None)
            print(f"  {field}: {count}/{len(result_notices)} ({100*count//len(result_notices)}%)")
    if contract_notices:
        print(f"\nHTML parser — ContractNotice ({len(contract_notices)}):")
        for field in ['budget_estimated', 'description']:
            count = sum(1 for n in contract_notices if n.get(field) is not None)
            print(f"  {field}: {count}/{len(contract_notices)} ({100*count//len(contract_notices)}%)")

    # Sample notices
    print(f"\nPrzykładowe ogłoszenia (max {limit}):")
    for n in notices[:limit]:
        print(f"  [{n.get('notice_type', '?')[:2]}] {n.get('title', '?')[:80]}")
        print(f"       CPV: {n.get('cpv_code', '-')} | {n.get('buyer_name', '?')[:50]}")
        print(f"       NIP: {n.get('buyer_nip', '-')} | {n.get('buyer_province', '-')}")
        if n.get('final_price'):
            print(f"       Cena: {n['final_price']:,.2f} {n.get('currency', '?')} "
                  f"| Oferty: {n.get('offers_count', '-')}")
        if n.get('description'):
            print(f"       Opis: {n['description'][:100]}...")

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
        help="Two-pass classification: CPV+keyword then Gemini LLM"
    )
    parser.add_argument(
        "--classify-llm", action="store_true",
        help="LLM-only classification (skip CPV+keyword pass)"
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

    if args.classify or args.classify_llm:
        result = run(mode="classify", llm_only=args.classify_llm)
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
