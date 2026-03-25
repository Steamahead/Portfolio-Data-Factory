import datetime
import json
import logging
import os
import smtplib
import traceback
import azure.functions as func
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# Importujemy OBA konektory: stary (PSE) i nowy (Weather)
from energy_prophet.pse_connector import PSEConnector
from energy_prophet.weather_connector import WeatherConnector

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# ── Load .env (email config lives here, not in local.settings.json) ──
_PROJECT_DIR = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_DIR / ".env"

if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#"):
            continue
        if "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())


# ── Email alerts (same pattern as other pipelines) ──────────────

def _get_email_config() -> dict | None:
    email_from = os.environ.get("ALERT_EMAIL_FROM", "").strip()
    password = os.environ.get("ALERT_EMAIL_PASSWORD", "").strip()
    email_to = os.environ.get("ALERT_EMAIL_TO", "").strip()
    if not all([email_from, password, email_to]):
        return None
    return {"from": email_from, "password": password, "to": email_to}


def _send_email(subject: str, body_html: str, config: dict) -> bool:
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
        logging.info(f"[EMAIL] Alert wyslany na {config['to']}")
        return True
    except Exception as e:
        logging.warning(f"[EMAIL FAIL] Nie udalo sie wyslac: {e}")
        return False


def _build_start_html() -> str:
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#007bff;">&#9654; Energy Prophet — START</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <p>Pipeline Energy Prophet zostal uruchomiony. Etapy:</p>
    <ul><li>PSE — dane rynkowe</li><li>Weather — dane meteo OZE</li></ul>
    <p style="color:gray;font-size:12px;">
      Jesli nie otrzymasz maila FINISH w ciagu kilkunastu minut, sprawdz logi.
    </p>
    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: energy_prophet | Azure Function: EnergyDailyRun
    </p>
    </body></html>
    """


def _build_success_html(pse_ok: bool, weather_ok: bool) -> str:
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pse_icon = "&#9989;" if pse_ok else "&#10060;"
    weather_icon = "&#9989;" if weather_ok else "&#10060;"
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#28a745;">&#9989; Energy Prophet — OK</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <table style="border-collapse:collapse;width:100%;margin:10px 0;">
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>PSE ETL</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{pse_icon} {'OK' if pse_ok else 'FAIL'}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Weather ETL</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{weather_icon} {'OK' if weather_ok else 'FAIL'}</td>
      </tr>
    </table>
    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: energy_prophet | Azure Function: EnergyDailyRun
    </p>
    </body></html>
    """


def _build_alert_html(pse_ok: bool, weather_ok: bool, errors: list[str]) -> str:
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pse_icon = "&#9989;" if pse_ok else "&#10060;"
    weather_icon = "&#9989;" if weather_ok else "&#10060;"
    error_rows = "".join(
        f'<tr><td style="padding:4px 8px;border:1px solid #ddd;">{err}</td></tr>\n'
        for err in errors[:20]
    )
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#dc3545;">&#128680; Energy Prophet — Alert</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <table style="border-collapse:collapse;width:100%;margin:10px 0;">
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>PSE ETL</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{pse_icon} {'OK' if pse_ok else 'FAIL'}</td>
      </tr>
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;"><strong>Weather ETL</strong></td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{weather_icon} {'OK' if weather_ok else 'FAIL'}</td>
      </tr>
    </table>
    <h3 style="color:#dc3545;">Bledy ({len(errors)}):</h3>
    <table style="border-collapse:collapse;width:100%;">{error_rows}</table>
    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: energy_prophet | Azure Function: EnergyDailyRun
    </p>
    </body></html>
    """


def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # Email START
    try:
        email_config = _get_email_config()
        if email_config:
            subject = f"[START] Energy Prophet ({datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M')})"
            _send_email(subject, _build_start_html(), email_config)
    except Exception as e:
        logging.warning(f"[EMAIL] Blad wysylki START: {e}")

    pse_ok = False
    weather_ok = False
    errors = []

    try:
        # 1. Konfiguracja wstępna
        today = datetime.date.today()
        # Pobieramy Connection String raz, żeby przekazać go do WeatherConnector
        conn_str = os.environ.get('SqlConnectionString')

        logging.info(f"Starting Energy Prophet ETL for execution date: {today}")

        # --- ETAP 1: PSE (Dane rynkowe) ---
        logging.info("--- PHASE 1: PSE DATA ---")
        try:
            pse = PSEConnector()
            pse.run_etl(today)
            pse_ok = True
            logging.info("✓ PSE ETL finished.")
        except Exception as e:
            logging.error(f"❌ PSE ETL Failed: {e}")
            errors.append(f"PSE ETL: {e}")

        # --- ETAP 2: POGODA (Nowość - Dane meteo dla klastrów OZE) ---
        logging.info("--- PHASE 2: WEATHER DATA ---")
        if conn_str:
            try:
                weather = WeatherConnector(conn_str)
                weather.run_etl(today)
                weather_ok = True
                logging.info("✓ Weather ETL finished.")
            except Exception as e:
                logging.error(f"❌ Weather ETL Failed: {e}")
                errors.append(f"Weather ETL: {e}")
        else:
            logging.error("❌ Skipping Weather ETL: Missing SqlConnectionString environment variable.")
            errors.append("Weather ETL: Missing SqlConnectionString")

        logging.info("ALL ETL TASKS COMPLETED.")

    except Exception as e:
        # Ten blok łapie błędy krytyczne (np. brak bibliotek, awaria systemu)
        logging.error(f"CRITICAL FUNCTION FAILURE: {str(e)}")
        errors.append(f"CRITICAL: {e}")

        # Email FINISH (failure) before re-raising
        try:
            email_config = _get_email_config()
            if email_config:
                subject = f"[FAIL] Energy Prophet ({datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M')})"
                _send_email(subject, _build_alert_html(pse_ok, weather_ok, errors), email_config)
        except Exception as email_err:
            logging.warning(f"[EMAIL] Blad wysylki FINISH: {email_err}")

        raise e

    # Email FINISH (always — success or partial failure)
    try:
        email_config = _get_email_config()
        if email_config:
            if errors:
                subject = f"[WARN] Energy Prophet ({datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M')})"
                _send_email(subject, _build_alert_html(pse_ok, weather_ok, errors), email_config)
            else:
                subject = f"[SUCCESS] Energy Prophet ({datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M')})"
                _send_email(subject, _build_success_html(pse_ok, weather_ok), email_config)
    except Exception as e:
        logging.warning(f"[EMAIL] Blad wysylki FINISH: {e}")