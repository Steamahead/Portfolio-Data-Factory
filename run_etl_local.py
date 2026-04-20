"""
Local ETL Runner — Portfolio Data Factory
==========================================
Runs ETL pipelines locally when Azure Functions are unavailable.
Works with CSV_ONLY=1 mode (saves to csv_staging/).

Usage:
    python -X utf8 run_etl_local.py energy        # Energy Prophet
    python -X utf8 run_etl_local.py gov            # Gov Spending Radar
    python -X utf8 run_etl_local.py cee            # CEE FX Volatility
    python -X utf8 run_etl_local.py all            # all three
"""

import datetime
import logging
import os
import sys
import traceback
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent
ENV_FILE = PROJECT_ROOT / ".env"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _load_env():
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def run_energy():
    """Run Energy Prophet (PSE + Weather)."""
    log.info("=== Energy Prophet ===")
    today = datetime.date.today()
    errors = []

    # PSE
    try:
        from energy_prophet.pse_connector import PSEConnector
        pse = PSEConnector()
        pse.run_etl(today)
        log.info("PSE ETL OK")
    except Exception as e:
        log.error(f"PSE ETL FAILED: {e}")
        errors.append(f"PSE: {e}")

    # Weather
    conn_str = os.environ.get("SqlConnectionString")
    try:
        from energy_prophet.weather_connector import WeatherConnector
        weather = WeatherConnector(conn_str or "")
        weather.run_etl(today)
        log.info("Weather ETL OK")
    except Exception as e:
        log.error(f"Weather ETL FAILED: {e}")
        errors.append(f"Weather: {e}")

    return errors


def run_gov():
    """Run Gov Spending Radar (yesterday's notices)."""
    log.info("=== Gov Spending Radar ===")
    try:
        from gov_spending_radar.main import run
        result = run()
        if result.get("success"):
            log.info(f"Gov Spending OK: {result}")
        else:
            log.warning(f"Gov Spending partial: {result}")
        return []
    except Exception as e:
        log.error(f"Gov Spending FAILED: {e}")
        traceback.print_exc()
        return [str(e)]


def run_cee():
    """Run CEE FX Volatility (FX + news)."""
    log.info("=== CEE FX Volatility ===")
    try:
        from cee_fx_volatility.main import main as cee_main
        # Simulate empty args
        sys.argv = ["cee_fx_volatility"]
        cee_main()
        log.info("CEE FX OK")
        return []
    except SystemExit:
        # main() may call sys.exit(0) on success
        return []
    except Exception as e:
        log.error(f"CEE FX FAILED: {e}")
        traceback.print_exc()
        return [str(e)]


PIPELINES = {
    "energy": run_energy,
    "gov": run_gov,
    "cee": run_cee,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    target = sys.argv[1].lower()
    _load_env()

    csv_only = os.environ.get("CSV_ONLY", "").strip() in ("1", "true", "yes")
    log.info(f"CSV_ONLY mode: {'ON' if csv_only else 'OFF'}")
    log.info(f"Date: {datetime.date.today()}")

    all_errors = []

    if target == "all":
        for name, func in PIPELINES.items():
            try:
                errs = func()
                all_errors.extend(errs)
            except Exception as e:
                log.error(f"Pipeline {name} crashed: {e}")
                all_errors.append(f"{name}: {e}")
    elif target in PIPELINES:
        all_errors = PIPELINES[target]()
    else:
        print(f"Unknown pipeline: {target}")
        print(f"Available: {', '.join(PIPELINES.keys())}, all")
        sys.exit(1)

    if all_errors:
        log.warning(f"Finished with {len(all_errors)} error(s): {all_errors}")
        sys.exit(1)
    else:
        log.info("All pipelines finished OK")
        sys.exit(0)


if __name__ == "__main__":
    main()
