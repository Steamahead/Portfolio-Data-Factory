"""
CSV Staging Utilities — CSV-Only Mode for Portfolio Data Factory.
When CSV_ONLY=1 is set in .env, pipelines save data here instead of Azure SQL.
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
STAGING_DIR = PROJECT_ROOT / "csv_staging"


def is_csv_only() -> bool:
    """Check if CSV_ONLY mode is enabled via environment variable."""
    return os.environ.get("CSV_ONLY", "").strip() in ("1", "true", "yes")


def save_to_staging(data, pipeline: str, table: str) -> str:
    """
    Save data to csv_staging/{pipeline}/{table}_{timestamp}.csv.

    Args:
        data: pd.DataFrame or list[dict]
        pipeline: subdirectory name (e.g. "nfj", "cee_fx")
        table: table name for filename (e.g. "nfj_offers")

    Returns:
        Path to saved CSV file.
    """
    target_dir = STAGING_DIR / pipeline
    target_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{table}_{timestamp}.csv"
    path = target_dir / filename

    if isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        raise TypeError(f"Unsupported data type: {type(data)}")

    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  [CSV-ONLY] Staged {len(df)} rows -> {path}")
    return str(path)
