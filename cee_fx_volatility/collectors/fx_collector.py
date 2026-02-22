"""
FX Collector — hourly OHLCV data from yfinance.
================================================
Pairs: EUR/PLN, EUR/CZK, EUR/HUF (EUR base — isolates European dynamics).
Interval: 1h. Volatility metric: (high - low) / open.
Validates ranges, rejects anomalies, handles missing data gracefully.

UWAGA: yfinance to nieoficjalne API Yahoo Finance — może przestać działać.
"""

import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
import yfinance as yf

from ..utils.timezone import to_utc_iso

# ── Load config ────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Validation ─────────────────────────────────────────────────────

def _is_valid_bar(row: dict, pair_name: str, ranges: dict) -> tuple[bool, str]:
    """
    Validate a single OHLCV bar. Returns (is_valid, rejection_reason).

    Checks:
    1. open/high/low/close must be numeric, positive, not NaN
    2. high >= low
    3. Values within configured range for this pair
    """
    for field in ("open", "high", "low", "close"):
        val = row.get(field)
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return False, f"{field} is NaN/None"
        if not isinstance(val, (int, float)):
            return False, f"{field} not numeric: {type(val)}"
        if val <= 0:
            return False, f"{field} <= 0: {val}"

    if row["high"] < row["low"]:
        return False, f"high ({row['high']}) < low ({row['low']})"

    pair_range = ranges.get(pair_name)
    if pair_range:
        lo, hi = pair_range["min"], pair_range["max"]
        for field in ("open", "high", "low", "close"):
            val = row[field]
            if val < lo or val > hi:
                return False, f"{field}={val:.4f} outside range [{lo}, {hi}] for {pair_name}"

    return True, ""


# ── Fetch ──────────────────────────────────────────────────────────

def fetch_fx_data(backfill_days: int | None = None) -> list[dict]:
    """
    Fetch hourly FX data from yfinance for all configured pairs.

    Args:
        backfill_days: If set, fetch last N days of history (max 730).
                       If None, fetch last 5 days (covers weekends + buffer).

    Returns:
        List of validated FX records ready for DB upload.
    """
    config = _load_config()
    pairs = config["fx_pairs"]
    ranges = config["fx_validation"]
    interval = config["yfinance"]["interval"]
    max_backfill = config["yfinance"]["max_backfill_days"]

    if backfill_days is not None:
        if backfill_days > max_backfill:
            print(f"  [FX] UWAGA: backfill ograniczony do {max_backfill} dni (limit yfinance dla 1h)")
            backfill_days = max_backfill
        period = None
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=backfill_days)
    else:
        period = "5d"
        start = None
        end = None

    all_records: list[dict] = []
    rejected = 0

    for pair_cfg in pairs:
        ticker = pair_cfg["ticker"]
        pair_name = pair_cfg["name"]
        print(f"\n  [FX] Pobieram {pair_name} ({ticker})...")

        try:
            tk = yf.Ticker(ticker)
            if period:
                df = tk.history(period=period, interval=interval)
            else:
                df = tk.history(start=start, end=end, interval=interval)
        except Exception as e:
            print(f"  [FX] BŁĄD pobierania {pair_name}: {e}")
            continue

        if df.empty:
            print(f"  [FX] Brak danych dla {pair_name}")
            continue

        pair_count = 0
        pair_rejected = 0

        for idx, row in df.iterrows():
            ts = idx.to_pydatetime()
            ts_utc = to_utc_iso(ts)

            bar = {
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]) if not math.isnan(row["Volume"]) else None,
            }

            valid, reason = _is_valid_bar(bar, pair_name, ranges)
            if not valid:
                print(f"  [FX] Odrzucono {pair_name} {ts_utc}: {reason}")
                pair_rejected += 1
                continue

            volatility = (bar["high"] - bar["low"]) / bar["open"] if bar["open"] > 0 else 0.0

            all_records.append({
                "timestamp": ts_utc,
                "currency_pair": pair_name,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar["volume"],
                "volatility_1h": round(volatility, 8),
            })
            pair_count += 1

        rejected += pair_rejected
        print(f"  [FX] {pair_name}: {pair_count} rekordów OK, {pair_rejected} odrzuconych")

    print(f"\n  [FX] Łącznie: {len(all_records)} rekordów, {rejected} odrzuconych")
    return all_records
