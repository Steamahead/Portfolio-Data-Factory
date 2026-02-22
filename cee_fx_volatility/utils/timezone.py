"""
Timezone utilities — all timestamps normalized to UTC before storage.
=====================================================================
RSS feeds from Polish sources may report CET/CEST without explicit offset.
yfinance returns timezone-aware or naive timestamps depending on the ticker.
This module ensures everything ends up as ISO 8601 UTC with 'Z' suffix.
"""

from datetime import datetime, timezone, tzinfo
from zoneinfo import ZoneInfo

WARSAW_TZ = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc


def to_utc_iso(dt: datetime) -> str:
    """
    Convert a datetime to ISO 8601 UTC string with 'Z' suffix.

    Rules:
    - If naive (no tzinfo) → assume Europe/Warsaw, convert to UTC
    - If aware → convert to UTC
    - Returns format: '2025-06-15T14:00:00Z'
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=WARSAW_TZ)

    dt_utc = dt.astimezone(UTC)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def now_utc_iso() -> str:
    """Current UTC time as ISO 8601 string with 'Z' suffix."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_rss_datetime(date_str: str) -> str | None:
    """
    Parse RSS date string to UTC ISO 8601.

    Common RSS formats:
    - RFC 822: 'Mon, 15 Jun 2025 14:00:00 +0200'
    - RFC 822 no offset: 'Mon, 15 Jun 2025 14:00:00'  (assume Warsaw)
    - ISO 8601: '2025-06-15T14:00:00+02:00'

    Returns None if parsing fails.
    """
    import email.utils

    # Try email.utils (handles RFC 822 well)
    parsed = email.utils.parsedate_to_datetime(date_str) if date_str else None
    if parsed is not None:
        return to_utc_iso(parsed)

    # Fallback: try common ISO formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(date_str, fmt)
            return to_utc_iso(dt)
        except (ValueError, TypeError):
            continue

    return None
