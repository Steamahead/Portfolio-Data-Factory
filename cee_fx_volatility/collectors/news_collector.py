"""
News Collector — RSS headlines from Polish financial sources.
=============================================================
Sources: Bankier.pl, Money.pl, PAP Biznes.
Fetches headlines, filters spam, deduplicates by URL.
Timestamps converted to UTC before storage.

UWAGA: RSS nie wspiera paginacji wstecz — zawsze pobiera aktualny stan feedu.
"""

from pathlib import Path

import feedparser
import yaml

from ..utils.timezone import now_utc_iso, parse_rss_datetime

# ── Load config ────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Spam filter ────────────────────────────────────────────────────

def _is_spam(title: str, spam_phrases: list[str], min_length: int) -> tuple[bool, str]:
    """Check if headline is spam. Returns (is_spam, reason)."""
    if not title or len(title.strip()) < min_length:
        return True, f"za krótki ({len(title.strip()) if title else 0} znaków)"

    title_lower = title.lower()
    for phrase in spam_phrases:
        if phrase.lower() in title_lower:
            return True, f"zawiera '{phrase}'"

    return False, ""


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

    all_news: list[dict] = []
    seen_urls: set[str] = set()
    fetched_at = now_utc_iso()

    for feed_cfg in feeds:
        source = feed_cfg["name"]
        url = feed_cfg["url"]
        print(f"\n  [NEWS] Pobieram {source} ({url})...")

        try:
            feed = feedparser.parse(url)
        except Exception as e:
            print(f"  [NEWS] BŁĄD parsowania {source}: {e}")
            continue

        if feed.bozo and not feed.entries:
            print(f"  [NEWS] Feed {source} niedostępny: {feed.bozo_exception}")
            continue

        accepted = 0
        spam_count = 0
        dedup_count = 0

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

            # Spam filter
            is_spam, reason = _is_spam(title, spam_phrases, min_length)
            if is_spam:
                print(f"  [NEWS] Spam odrzucony ({source}): '{title[:50]}...' — {reason}")
                spam_count += 1
                continue

            # Parse published date
            published_raw = entry.get("published") or entry.get("updated") or ""
            published_at = parse_rss_datetime(published_raw)

            all_news.append({
                "published_at": published_at,
                "fetched_at": fetched_at,
                "source": source,
                "title": title,
                "url": entry_url,
                "category": None,
                "sentiment": None,
                "is_surprising": None,
                "raw_ai_response": None,
            })
            accepted += 1

        print(f"  [NEWS] {source}: {accepted} przyjętych, {spam_count} spam, {dedup_count} duplikatów")

    print(f"\n  [NEWS] Łącznie: {len(all_news)} nagłówków do przetworzenia")
    return all_news
