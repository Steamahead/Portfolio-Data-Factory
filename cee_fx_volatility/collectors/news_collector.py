"""
News Collector — RSS headlines from Polish financial sources.
=============================================================
Sources: Bankier.pl, Money.pl, PAP Biznes.
Fetches headlines, filters spam, deduplicates by URL.
Timestamps converted to UTC before storage.

Filters:
  1. Spam phrases (config.yaml)
  2. Stale articles older than max_article_age_days (config.yaml)
  3. Auto-generated FX headlines (Money.pl "Ile kosztuje" / "Kurs ... PLN/")

UWAGA: RSS nie wspiera paginacji wstecz — zawsze pobiera aktualny stan feedu.
"""

import re
import urllib.request
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path

import feedparser
import yaml

# User-Agent for RSS fetching (some sites like Investing.com may require it)
_RSS_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

from ..utils.timezone import now_utc_iso, parse_rss_datetime


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities from RSS description."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", "", text)
    clean = unescape(clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean

# ── Load config ────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Spam filter ────────────────────────────────────────────────────

_AUTO_FX_PATTERNS = [
    re.compile(r"^ile kosztuje\b", re.IGNORECASE),
    re.compile(r"kurs .+ do złotego PLN/", re.IGNORECASE),
]


def _is_spam(title: str, spam_phrases: list[str], min_length: int) -> tuple[bool, str]:
    """Check if headline is spam. Returns (is_spam, reason)."""
    if not title or len(title.strip()) < min_length:
        return True, f"za krótki ({len(title.strip()) if title else 0} znaków)"

    title_lower = title.lower()
    for phrase in spam_phrases:
        if phrase.lower() in title_lower:
            return True, f"zawiera '{phrase}'"

    # Auto-generated FX headlines (Money.pl daily currency reports)
    for pattern in _AUTO_FX_PATTERNS:
        if pattern.search(title):
            return True, "auto-generated FX headline"

    return False, ""


# ── Stale article filter ──────────────────────────────────────────

def _is_stale(published_at_iso: str | None, max_age_days: int) -> bool:
    """Check if article is older than max_age_days."""
    if not published_at_iso:
        return False  # no date — can't determine, let it through

    try:
        # Parse ISO 8601 UTC timestamp (e.g. "2020-12-31T10:00:00Z")
        pub_str = published_at_iso.rstrip("Z")
        pub_dt = datetime.fromisoformat(pub_str).replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        return pub_dt < cutoff
    except (ValueError, TypeError):
        return False  # can't parse — let it through


# ── Fetch ──────────────────────────────────────────────────────────

def fetch_news() -> list[dict]:
    """
    Fetch and filter news headlines from all configured RSS feeds.

    Returns:
        List of news records (without AI classification — that's done separately).
        Each record has keys matching NEWS_SQL_COLUMNS (except AI fields set to None).
    """
    config = _load_config()
    feeds = config["rss_feeds"]
    spam_phrases = config["spam_phrases"]
    min_length = config["min_headline_length"]
    max_age_days = config.get("max_article_age_days", 7)
    irrelevant_regions = config.get("irrelevant_regions", [])

    all_news: list[dict] = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()  # dedup across feeds (Bankier waluty + gielda overlap)
    fetched_at = now_utc_iso()

    for feed_cfg in feeds:
        source = feed_cfg["name"]
        url = feed_cfg["url"]
        print(f"\n  [NEWS] Pobieram {source} ({url})...")

        try:
            req = urllib.request.Request(url, headers={"User-Agent": _RSS_USER_AGENT})
            resp = urllib.request.urlopen(req, timeout=15)
            feed = feedparser.parse(resp.read())
        except Exception as e:
            print(f"  [NEWS] BŁĄD parsowania {source}: {e}")
            continue

        if feed.bozo and not feed.entries:
            print(f"  [NEWS] Feed {source} niedostępny: {feed.bozo_exception}")
            continue

        accepted = 0
        spam_count = 0
        dedup_count = 0
        stale_count = 0
        region_count = 0

        for entry in feed.entries:
            title = entry.get("title", "").strip()
            entry_url = entry.get("link", "").strip()

            if not entry_url:
                continue

            # Deduplicate by URL within this batch
            if entry_url in seen_urls:
                dedup_count += 1
                continue
            seen_urls.add(entry_url)

            # Deduplicate by title across feeds (Bankier waluty+gielda overlap)
            title_norm = title.lower().strip()
            if title_norm in seen_titles:
                dedup_count += 1
                continue
            seen_titles.add(title_norm)

            # Irrelevant region filter (global PMI noise from Investing.com)
            if irrelevant_regions:
                title_lower = title.lower()
                region_match = next(
                    (r for r in irrelevant_regions if r.lower() in title_lower), None
                )
                if region_match:
                    print(f"  [NEWS] Region odrzucony ({source}): '{title[:50]}...' — zawiera '{region_match}'")
                    region_count += 1
                    continue

            # Spam filter (includes auto-generated FX headlines)
            is_spam, reason = _is_spam(title, spam_phrases, min_length)
            if is_spam:
                print(f"  [NEWS] Spam odrzucony ({source}): '{title[:50]}...' — {reason}")
                spam_count += 1
                continue

            # Parse published date
            published_raw = entry.get("published") or entry.get("updated") or ""
            published_at = parse_rss_datetime(published_raw)

            # Stale article filter
            if _is_stale(published_at, max_age_days):
                print(f"  [NEWS] Stale article ({source}): '{title[:50]}...' — published {published_at}")
                stale_count += 1
                continue

            # Extract and clean description/summary from RSS
            # feedparser stores RSS <description> as 'summary' key; 'description' is an alias
            desc_raw = (
                entry.get("summary")
                or entry.get("description")
                or (entry.get("content", [{}])[0].get("value", "") if entry.get("content") else "")
            )
            description = _strip_html(desc_raw) or None

            all_news.append({
                "published_at": published_at,
                "fetched_at": fetched_at,
                "source": source,
                "title": title,
                "description": description,
                "url": entry_url,
                "category": None,
                "sentiment": None,
                "is_surprising": None,
                "raw_ai_response": None,
            })
            accepted += 1

        desc_count = sum(1 for n in all_news if n["source"] == source and n.get("description"))
        print(f"  [NEWS] {source}: {accepted} przyjętych, {spam_count} spam, "
              f"{stale_count} stale, {region_count} region, {dedup_count} duplikatów, {desc_count} z opisem")

    print(f"\n  [NEWS] Łącznie: {len(all_news)} nagłówków do przetworzenia")
    return all_news
