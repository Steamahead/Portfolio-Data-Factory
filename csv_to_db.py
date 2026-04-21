"""
CSV-to-DB Bulk Import — Portfolio Data Factory
===============================================
Imports staged CSV files from csv_staging/ into Azure SQL.
Run after Azure subscription is restored.

Usage:
    python -X utf8 csv_to_db.py                  # import all staged CSVs
    python -X utf8 csv_to_db.py --dry-run        # show what would be imported
    python -X utf8 csv_to_db.py --pipeline nfj   # import only NFJ data
"""

import argparse
import os
import shutil
import sys
import time
import pandas as pd
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent
STAGING_DIR = PROJECT_ROOT / "csv_staging"
DONE_DIR = STAGING_DIR / "done"
ENV_FILE = PROJECT_ROOT / ".env"


def _load_env():
    """Load .env variables."""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def scan_staging(pipeline_filter: str | None = None) -> list[tuple[str, str, Path]]:
    """Scan csv_staging/ for files to import. Returns [(pipeline, table, path), ...]."""
    files = []
    if not STAGING_DIR.exists():
        return files

    for pipeline_dir in sorted(STAGING_DIR.iterdir()):
        if not pipeline_dir.is_dir() or pipeline_dir.name in ("done",):
            continue
        if pipeline_filter and pipeline_dir.name != pipeline_filter:
            continue
        for csv_file in sorted(pipeline_dir.glob("*.csv")):
            # Extract table name from filename: {table}_{YYYY-MM-DD}_{HHMMSS}.csv
            name_parts = csv_file.stem.rsplit("_", 2)
            if len(name_parts) >= 3:
                table = "_".join(name_parts[:-2])
            else:
                table = csv_file.stem
            files.append((pipeline_dir.name, table, csv_file))

    return files


def _with_csv_only_disabled(func):
    """Run func with CSV_ONLY temporarily unset so upload functions hit real DB."""
    old_val = os.environ.pop("CSV_ONLY", None)
    try:
        return func()
    finally:
        if old_val is not None:
            os.environ["CSV_ONLY"] = old_val


def _import_scraper(csv_path: Path, pipeline: str) -> int:
    """Import scraper CSV using the pipeline's own upload_to_azure_sql."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    def do_upload():
        if pipeline == "nfj":
            from nfj_scraper.nfj_data_scraper import upload_to_azure_sql
            result = upload_to_azure_sql(df)
        elif pipeline == "justjoin":
            import ast as _ast
            import math as _math
            from just_join_scraper.just_join_scraper import upload_to_azure_sql
            records = df.to_dict("records")
            # CSV stores nested dicts/lists as Python repr strings — parse back
            list_keys = ("salaries", "locations", "required_skills", "nice_to_have_skills",
                         "skills", "multilocation")
            for rec in records:
                for key in list_keys:
                    val = rec.get(key)
                    if isinstance(val, float) and _math.isnan(val):
                        rec[key] = []
                    elif isinstance(val, str):
                        try:
                            rec[key] = _ast.literal_eval(val)
                        except (ValueError, SyntaxError):
                            rec[key] = []
            result = upload_to_azure_sql(records)
        elif pipeline == "pracuj":
            from pracuj_scraper.pracuj_premium_scraper import upload_to_azure_sql
            result = upload_to_azure_sql(df)
        else:
            return 0
        return result.get("uploaded", 0)

    return _with_csv_only_disabled(do_upload)


def _import_cee_fx(csv_path: Path, table: str) -> int:
    """Import CEE FX CSV using pipeline's upload functions."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    def do_upload():
        if table == "cee_fx_rates":
            from cee_fx_volatility.db.operations import upload_fx_rates
            result = upload_fx_rates(df.to_dict("records"))
        elif table == "cee_news_headlines":
            from cee_fx_volatility.db.operations import upload_news
            result = upload_news(df.to_dict("records"))
        else:
            print(f"    [!] Unknown CEE FX table: {table}")
            return 0
        return result.get("uploaded", 0)

    return _with_csv_only_disabled(do_upload)


def _import_gov(csv_path: Path, table: str) -> int:
    """Import Gov Spending CSV."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    def do_upload():
        if table == "gov_notices":
            from gov_spending_radar.db.operations import upload_notices
            result = upload_notices(df.to_dict("records"))
        elif table == "gov_contractors":
            from gov_spending_radar.db.operations import upload_contractors
            result = upload_contractors(df.to_dict("records"))
        elif table == "gov_classifications":
            from gov_spending_radar.db.operations import upload_classifications
            result = upload_classifications(df.to_dict("records"))
        else:
            print(f"    [!] Unknown gov table: {table}")
            return 0
        return result.get("uploaded", 0)

    return _with_csv_only_disabled(do_upload)


def _import_shiller(csv_path: Path, table: str) -> int:
    """Import Shiller CSV — reconstruct final_data and use _execute_database_save."""
    if table == "shiller_articles":
        # Articles are imported together with scores — skip standalone
        return 0

    if table != "shiller_daily_scores":
        return 0

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    def do_upload():
        from shiller_index.shiller_logic import _execute_database_save
        conn_str = os.environ.get("SqlConnectionString")
        if not conn_str:
            print("    [!] SqlConnectionString not set")
            return 0

        imported = 0
        for _, row in df.iterrows():
            date_val = row["date"]
            ticker = row["ticker"]

            # Find matching articles file
            articles_rows = []
            parent = csv_path.parent
            for art_file in parent.glob("shiller_articles_*.csv"):
                art_df = pd.read_csv(art_file, encoding="utf-8-sig")
                match = art_df[(art_df["date"].astype(str) == str(date_val)) & (art_df["ticker"] == ticker)]
                if not match.empty:
                    articles_rows = match.to_dict("records")
                    break

            final_data = {
                "metadata": {
                    "analysis_date": date_val,
                    "ticker": ticker,
                    "price": row["price"],
                    "ma_30": row["ma_30"],
                    "gap_pct": row["gap_pct"],
                    "articles_received": int(row.get("articles_received", 0)),
                },
                "aggregated_scores": {
                    "final_sentiment": row["final_sentiment"],
                    "final_hype": row["final_hype"],
                    "sentiment_confidence": row["sentiment_confidence"],
                    "hype_confidence": row["hype_confidence"],
                    "articles_used_sentiment": int(row.get("articles_used_sentiment", 0)),
                    "articles_used_hype": int(row.get("articles_used_hype", 0)),
                },
                "articles": articles_rows,
            }

            try:
                if _execute_database_save(final_data, conn_str):
                    imported += 1
            except Exception as e:
                print(f"    [!] Failed {ticker}/{date_val}: {e}")

        return imported

    return _with_csv_only_disabled(do_upload)


def _import_energy(csv_path: Path, table: str) -> int:
    """Import Energy Prophet CSV using PSEConnector upsert methods."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return 0

    def do_upload():
        conn_str = os.environ.get("SqlConnectionString")
        if not conn_str:
            print("    [!] SqlConnectionString not set")
            return 0

        from energy_prophet.pse_connector import PSEConnector
        connector = PSEConnector()

        if table == "weather_data":
            from energy_prophet.weather_connector import WeatherConnector
            # CSV stores datetime as string — convert back for SQL params
            for col in ("dtime",):
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            wc = WeatherConnector(conn_str)
            wc._save_to_sql(df)
            return len(df)

        with connector._connect_with_retry(conn_str) as conn:
            cursor = conn.cursor()

            upsert_map = {
                "energy_prices": connector._upsert_prices,
                "cross_border_flows": connector._upsert_flows,
                "pse_alerts": connector._upsert_alerts,
                "oze_curtailment": connector._upsert_curtailment,
                "co2_prices": connector._upsert_co2,
                "planned_outages": connector._upsert_outages,
            }

            if table == "generation_mix":
                # CSV may contain actuals (has dtime) or oze_fcst (has plan_dtime)
                if "dtime" in df.columns:
                    connector._upsert_generation_mix(cursor, actuals=df, load_fcst=None, oze_fcst=None)
                elif "plan_dtime" in df.columns:
                    connector._upsert_generation_mix(cursor, actuals=None, load_fcst=None, oze_fcst=df)
                else:
                    connector._upsert_generation_mix(cursor, actuals=None, load_fcst=df, oze_fcst=None)
                conn.commit()
                return len(df)

            if table == "power_balance":
                # CSV may contain reserves (has peak_type) or daily_plan (has gen_fv)
                if "peak_type" in df.columns or "rez_sr" in df.columns:
                    connector._upsert_power_balance(cursor, reserves=df, daily_plan=None)
                else:
                    connector._upsert_power_balance(cursor, reserves=None, daily_plan=df)
                conn.commit()
                return len(df)

            if table == "balancing_settlement":
                connector._upsert_settlement(cursor, df, "csv-import")
                conn.commit()
                return len(df)

            if table in upsert_map:
                upsert_map[table](cursor, df)
                conn.commit()
                return len(df)

            print(f"    [!] Unknown energy table: {table}")
            return 0

    return _with_csv_only_disabled(do_upload)


# Pipeline router
PIPELINE_HANDLERS = {
    "nfj": lambda path, table: _import_scraper(path, "nfj"),
    "justjoin": lambda path, table: _import_scraper(path, "justjoin"),
    "pracuj": lambda path, table: _import_scraper(path, "pracuj"),
    "cee_fx": _import_cee_fx,
    "gov_spending": _import_gov,
    "shiller": _import_shiller,
    "energy": _import_energy,
}


def import_all(pipeline_filter: str | None = None, dry_run: bool = False) -> dict:
    """Import all staged CSVs to Azure SQL."""
    _load_env()

    files = scan_staging(pipeline_filter)
    if not files:
        print("\n[CSV-TO-DB] No staged files found.")
        return {"imported": 0, "failed": 0, "files": 0}

    print(f"\n{'='*60}")
    print(f"  CSV-TO-DB Bulk Import")
    print(f"  Files: {len(files)}")
    print(f"  Mode: {'DRY-RUN' if dry_run else 'IMPORT'}")
    print(f"{'='*60}\n")

    if dry_run:
        for pipeline, table, path in files:
            df = pd.read_csv(path, encoding="utf-8-sig")
            print(f"  [{pipeline}] {table}: {len(df)} rows <- {path.name}")
        return {"imported": 0, "failed": 0, "files": len(files)}

    total_imported = 0
    total_failed = 0

    DONE_DIR.mkdir(parents=True, exist_ok=True)

    for pipeline, table, path in files:
        print(f"\n  [{pipeline}] {table} <- {path.name}")
        handler = PIPELINE_HANDLERS.get(pipeline)
        if not handler:
            print(f"    [!] No handler for pipeline '{pipeline}' — skipping")
            total_failed += 1
            continue

        try:
            imported = handler(path, table)
            total_imported += imported

            # Move to done/
            done_pipeline_dir = DONE_DIR / pipeline
            done_pipeline_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(done_pipeline_dir / path.name))
            print(f"    -> Moved to done/")

        except Exception as e:
            print(f"    [!] FAILED: {e}")
            total_failed += 1

    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"  Total rows imported: {total_imported}")
    print(f"  Failed files: {total_failed}")
    print(f"{'='*60}\n")

    return {"imported": total_imported, "failed": total_failed, "files": len(files)}


def main():
    parser = argparse.ArgumentParser(
        description="CSV-to-DB Bulk Import — import staged CSVs to Azure SQL",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be imported")
    parser.add_argument("--pipeline", type=str, default=None,
                        help="Import only this pipeline (nfj, justjoin, pracuj, cee_fx, gov_spending, shiller, energy)")
    args = parser.parse_args()

    result = import_all(pipeline_filter=args.pipeline, dry_run=args.dry_run)
    sys.exit(0 if result["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
