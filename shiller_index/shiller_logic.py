"""
Shiller Hybrid Index - Speculative Bubble Detection Tool

Compares stock price trends (hard data) vs media hype (news sentiment).
Uses Google Gemini AI to analyze speculative hype with detailed article evaluation.
"""

import os
import csv
import json
import logging
import smtplib
import time
import traceback
import pyodbc
import requests
import yfinance as yf
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google import genai
from google.genai import errors as genai_errors
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# --- CONFIGURATION ---

def load_local_settings():
    """Load local.settings.json and set environment variables."""
    settings_paths = [
        Path(__file__).parent.parent / "local.settings.json",
        Path("local.settings.json"),
    ]

    for settings_path in settings_paths:
        if settings_path.exists():
            try:
                with open(settings_path, "r", encoding="utf-8") as f:
                    settings = json.load(f)

                values = settings.get("Values", {})
                for key, value in values.items():
                    if key not in os.environ:
                        os.environ[key] = value

                logger.info(f"Loaded settings from {settings_path}")
                return True
            except Exception as e:
                logger.warning(f"Failed to load {settings_path}: {e}")
                continue

    logger.warning("local.settings.json not found, using existing environment variables.")
    return False


# Load settings on import
load_local_settings()

# Load .env (email config lives here, not in local.settings.json)
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#"):
            continue
        if "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())


# ── Email alerts (same pattern as cee_fx_volatility + scraper_monitor) ──

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


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
        logger.info(f"[EMAIL] Alert wyslany na {config['to']}")
        return True
    except Exception as e:
        logger.warning(f"[EMAIL FAIL] Nie udalo sie wyslac: {e}")
        return False


def _build_start_html() -> str:
    """Build HTML email body for pipeline start notification."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ticker_list = "".join(f"<li>{t}</li>" for t in TICKERS)
    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#007bff;">&#9654; Shiller Hybrid Index — START</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <p>Pipeline Shiller Hybrid Index zostal uruchomiony. Tickery:</p>
    <ul>{ticker_list}</ul>
    <p style="color:gray;font-size:12px;">
      Jesli nie otrzymasz maila FINISH w ciagu kilkunastu minut, sprawdz logi.
    </p>
    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: shiller_index | Azure Function: ShillerDailyRun
    </p>
    </body></html>
    """


def _build_shiller_success_html(results: list[dict]) -> str:
    """Build HTML email body for pipeline success report."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    ticker_rows = ""
    for r in results:
        meta = r["metadata"]
        agg = r["aggregated_scores"]
        ticker_rows += f"""
      <tr>
        <td style="padding:6px 12px;border:1px solid #ddd;">{meta['ticker']}</td>
        <td style="padding:6px 12px;border:1px solid #ddd;">${meta['price']}</td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{agg['final_sentiment']}</td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{agg['final_hype']}</td>
        <td style="padding:6px 12px;border:1px solid #ddd;">{agg['sentiment_confidence']}</td>
      </tr>"""

    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#28a745;">&#9989; Shiller Hybrid Index — OK</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <p><strong>Przetworzono:</strong> {len(results)}/{len(TICKERS)} tickerow</p>

    <table style="border-collapse:collapse;width:100%;margin:10px 0;">
      <tr style="background:#f8f9fa;">
        <th style="padding:6px 12px;border:1px solid #ddd;">Ticker</th>
        <th style="padding:6px 12px;border:1px solid #ddd;">Cena</th>
        <th style="padding:6px 12px;border:1px solid #ddd;">Sentiment</th>
        <th style="padding:6px 12px;border:1px solid #ddd;">Hype</th>
        <th style="padding:6px 12px;border:1px solid #ddd;">Confidence</th>
      </tr>
      {ticker_rows}
    </table>

    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: shiller_index | Azure Function: ShillerDailyRun
    </p>
    </body></html>
    """


def _build_shiller_alert_html(results: list[dict], failed_tickers: list, error_msg: str | None = None) -> str:
    """Build HTML email body for pipeline failure alert."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    error_section = ""
    if error_msg:
        error_section = f'<p style="color:#dc3545;"><strong>Blad:</strong> {error_msg}</p>'

    failed_info = ""
    if failed_tickers:
        items = "".join(f"<li>{t} — {e}</li>" for t, e, _ in failed_tickers)
        failed_info = f"""
        <h3 style="color:#dc3545;">Nieudane tickery:</h3>
        <ul>{items}</ul>
        """

    return f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;max-width:650px;">
    <h2 style="color:#dc3545;">&#128680; Shiller Hybrid Index — Alert</h2>
    <p><strong>Czas:</strong> {ts}</p>
    <p><strong>Przetworzono:</strong> {len(results)}/{len(TICKERS)} tickerow</p>
    {error_section}
    {failed_info}

    <p style="color:#6c757d;font-size:12px;margin-top:20px;">
      Pipeline: shiller_index | Azure Function: ShillerDailyRun
    </p>
    </body></html>
    """


def _send_shiller_start_email() -> None:
    """Send pipeline start notification email."""
    email_config = _get_email_config()
    if not email_config:
        return

    subject = f"[START] Shiller Index ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
    body = _build_start_html()
    _send_email(subject, body, email_config)


def _send_shiller_finish_email(results: list[dict], failed_tickers: list | None = None,
                               error_msg: str | None = None) -> None:
    """Send email with pipeline result — always sends."""
    email_config = _get_email_config()
    if not email_config:
        logger.info("[EMAIL] Brak konfiguracji email — pomijam alert")
        return

    if error_msg:
        subject = f"[FAIL] Shiller Index ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_shiller_alert_html(results, failed_tickers or [], error_msg=error_msg)
        _send_email(subject, body, email_config)
    elif failed_tickers:
        subject = f"[WARN] Shiller Index — {len(results)}/{len(TICKERS)} OK ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_shiller_alert_html(results, failed_tickers)
        _send_email(subject, body, email_config)
    else:
        subject = f"[SUCCESS] Shiller Index ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})"
        body = _build_shiller_success_html(results)
        _send_email(subject, body, email_config)


# Configure Gemini client
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    logger.error("GEMINI_API_KEY is missing!")
    gemini_client = None
else:
    gemini_client = genai.Client(api_key=api_key)

# Tickers to analyze
TICKERS = ["NVDA", "WMT", "TSLA"]

TICKER_NEWS_CONFIG = {
    "NVDA": {"query": "Nvidia AND (stock OR shares OR AI OR chips OR earnings)", "company_name": "NVIDIA Corporation"},
    "WMT": {"query": "Walmart AND (stock OR shares OR earnings OR retail)", "company_name": "Walmart Inc."},
    "TSLA": {"query": "Tesla AND (stock OR shares OR Musk OR EV OR earnings)", "company_name": "Tesla Inc."},
}

# --- 1. PROMPT DEFINITION (Complete V3.0) ---

SHILLER_MEGA_PROMPT = """You are the Data Gatekeeper for "The Shiller Hybrid Index" — a behavioral finance system designed to detect speculative bubbles before they burst.

## STRATEGIC CONTEXT

**THE PROBLEM WE SOLVE:**
Markets crash when crowd emotion decouples from business reality. Media screams "revolution!" while earnings whisper "stagnation." We detect this divergence.

**CORE HYPOTHESIS (Divergence Theory):**
A speculative bubble forms when:
- HYPE is EXTREME (emotional language, FOMO, "to the moon")
- But FUNDAMENTALS are WEAK (no earnings growth, no real news)

Your job: Separate signal from noise. Measure both tracks independently.

**DUAL-TRACK ANALYSIS:**
- Track A (SENTIMENT): What is the REAL business signal? (Earnings, deals, regulatory — ignore hype)
- Track B (HYPE): How loud is the crowd? (Speculation intensity — ignore whether it's true)

## CURRENT ANALYSIS

Ticker: {ticker} ({company_name})
Date: {analysis_date}
Stock Price: ${price} | 30-day MA: ${ma_30} | Gap: {gap_pct}%

You will analyze {num_articles} articles. Be rigorous — this data feeds a 12-month time-series that will be audited.

---

# PHASE 1: ARTICLE FILTERING

For each article, answer THREE gateway questions:

**Q1: Is this article ABOUT {ticker}?**
- "PRIMARY" = {ticker} is THE main subject (in headline, entire article about them)
- "MENTIONED" = {ticker} is discussed but shares focus with others
- "NO" = {ticker} not mentioned, or only in passing (e.g., list of ETF holdings)

→ If "NO": Mark article as EXCLUDED, skip remaining questions.

**Q2: Can we assess SENTIMENT (business reality) for {ticker}?**
- "YES" = Clear bullish/bearish signal about company's actual business
- "PARTIAL" = Weak or indirect signal (e.g., industry trend that may affect company)
- "NO" = No information about business impact

**Q3: Can we assess HYPE (crowd emotion) level?**
**IMPORTANT DISTINCTION FOR Q3 (Hype Usable):**
We measure MARKET HYPE, not SOCIAL MEDIA BUZZ.
- "Elon Musk says Tesla will hit $500" → YES (market speculation)
- "Elon Musk calls someone an idiot" → NO (personal drama, no price signal)
- "Is Nvidia overvalued?" → YES (market speculation)
- "Jensen Huang's leadership style" → NO (human interest, no price signal)

Ask yourself: "Does this article contain language about STOCK PRICE, VALUATION, or INVESTMENT THESIS?"
If NO → Hype Usable = NO.

- "YES" = Article has clear character (obviously factual OR obviously speculative)
- "PARTIAL" = Mixed tone, hard to classify
- "NO" = Cannot determine speculation level

→ If BOTH Q2 and Q3 are "NO": Mark article as EXCLUDED.

---

# PHASE 2: QUALITY METRICS

Score each dimension for non-excluded articles. These metrics determine how much WEIGHT each article gets in final calculations.

## CENTRALITY (0-15)
How central is {ticker} to this article?

| Score | Criteria |
|-------|----------|
| 0-5 | {ticker} is one of 5+ companies mentioned |
| 6-10 | {ticker} is one of 2-4 companies, shared focus |
| 11-15 | {ticker} is THE subject — headline focus, entire article about them |

## CREDIBILITY_SENTIMENT (0-35) — For SENTIMENT track
How reliable is this source for BUSINESS TRUTH?

| Score | Criteria |
|-------|----------|
| 0-7 | Rumors, forums, anonymous "sources say", unverified claims |
| 8-15 | Opinion pieces, blogs, social media commentary, no hard data |
| 16-25 | Analyst opinions with rationale, trade publications, named sources |
| 26-35 | Reuters/Bloomberg/WSJ with verified facts, SEC filings, official company announcements with numbers |

## CREDIBILITY_HYPE (0-10) — For HYPE track
How valid is this source for measuring CROWD EMOTION?

| Score | Criteria |
|-------|----------|
| 0-3 | Unknown source, cannot verify it reflects real sentiment |
| 4-6 | Reddit, Twitter, blogs — these MATTER for hype detection, valid signal |
| 7-10 | Any established source with real audience (Bloomberg and Reddit equally valid for measuring mania) |

## RECENCY (0-15)
How fresh is this information?

| Score | Criteria |
|-------|----------|
| 0-5 | >3 days old, or rehashing known information |
| 6-10 | 1-3 days old, developing story or recap |
| 11-15 | Breaking news (<24h) or unique new analysis |

## MATERIALITY (0-35) — For SENTIMENT track only
Would this realistically move the stock price?

| Score | Criteria |
|-------|----------|
| 0-7 | **NOISE**: CEO personal drama, Twitter fights, lifestyle content, clickbait |
| 8-15 | **SOFT**: Analyst price targets, industry trends, product rumors, interviews without news |
| 16-25 | **MODERATE**: Product launches, partnerships (no $ disclosed), management changes |
| 26-35 | **HARD**: Earnings reports, M&A, regulatory decisions, contracts with $ amounts, guidance changes |

## SPECULATION_SIGNAL (0-60) — For HYPE track only
How speculative is the language? This is your "volume knob" for hype detection.

| Score | Category | Indicators |
|-------|----------|------------|
| 0-10 | **COLD/FACTUAL** | Earnings reports, SEC filings, neutral language, pure data, no adjectives |
| 11-25 | **MILD SPECULATION** | "analysts expect", "outlook", "guidance", "potential growth" — professional forecasting |
| 26-40 | **ACTIVE SPECULATION** | "strong buy", "significant upside", "market leader" — clear investment thesis |
| 41-55 | **HIGH EMOTION** | "surge", "plunge", "panic", "FOMO", "skyrocket" — emotional clickbait language |
| 56-60 | **MANIA/DELUSION** | "once in a lifetime", "unlimited potential", "parabolic", "crash imminent", "to the moon 🚀", cult-like language, religious comparisons |

---

# PHASE 3: RAW SCORES

For non-excluded articles, provide raw scores. These will be weighted by quality metrics in post-processing.

## SENTIMENT_RAW (-100 to +100)
What is the BUSINESS signal for {ticker}? Ignore hype — focus on real impact.

## HYPE_RAW (0 to 100)
How much SPECULATION and EMOTION is in this article? Ignore whether claims are true.

---

# OUTPUT FORMAT

Return ONLY valid JSON. No markdown code blocks.
{{
  "analysis_metadata": {{
    "ticker": "{ticker}",
    "company_name": "{company_name}",
    "analysis_date": "{analysis_date}",
    "price": {price},
    "ma_30": {ma_30},
    "gap_pct": {gap_pct},
    "articles_received": {num_articles},
    "articles_included": <int>,
    "articles_excluded": <int>
  }},
  "articles": [
    {{
      "article_num": 1,
      "headline_preview": "<first 250 characters>",
      "filter": {{
        "is_about_company": "PRIMARY|MENTIONED|NO",
        "sentiment_usable": "YES|PARTIAL|NO",
        "hype_usable": "YES|PARTIAL|NO",
        "excluded": false,
        "exclusion_reason": null
      }},
      "quality_metrics": {{
        "centrality": <0-15>,
        "credibility_sentiment": <0-35>,
        "credibility_hype": <0-10>,
        "recency": <0-15>,
        "materiality": <0-35>,
        "speculation_signal": <0-60>
      }},
      "scores": {{
        "sentiment_raw": <-100 to +100>,
        "hype_raw": <0 to 100>
      }},
      "reasoning": "<One sentence summary>"
    }}
  ]
}}

# ARTICLES TO ANALYZE

{articles_formatted}
"""

# --- 2. HELPER FUNCTIONS ---

def format_articles_for_prompt(articles: list[str]) -> str:
    # Uproszczona wersja przyjmująca listę stringów (headlines), bo tak mamy w głównym kodzie
    lines = []
    for i, headline in enumerate(articles, 1):
        lines.append(f"**Article {i}:**")
        lines.append(f"Headline: {headline}")
        lines.append("")
    return "\n".join(lines)

def calculate_quality_scores(article: dict) -> dict:
    if article.get("quality_metrics") is None:
        return {"quality_sentiment": 0, "quality_hype": 0}

    metrics = article["quality_metrics"]

    # Safe getter that handles None values
    def safe_get(key):
        val = metrics.get(key, 0)
        return val if val is not None else 0

    # Quality for Sentiment (max 100): 15 + 35 + 15 + 35
    quality_sentiment = (
        min(safe_get("centrality"), 15) +
        min(safe_get("credibility_sentiment"), 35) +
        min(safe_get("recency"), 15) +
        min(safe_get("materiality"), 35)
    )

    # Quality for Hype (max 100): 15 + 10 + 15 + 60
    quality_hype = (
        min(safe_get("centrality"), 15) +
        min(safe_get("credibility_hype"), 10) +
        min(safe_get("recency"), 15) +
        min(safe_get("speculation_signal"), 60)
    )

    if quality_sentiment > 100:
        logger.warning(f"quality_sentiment exceeded 100: {quality_sentiment}")
    if quality_hype > 100:
        logger.warning(f"quality_hype exceeded 100: {quality_hype}")

    return {
        "quality_sentiment": quality_sentiment,
        "quality_hype": quality_hype
    }

def calculate_weighted_averages(articles: list[dict]) -> dict:
    sentiment_sum = 0.0
    sentiment_weight_sum = 0.0
    hype_sum = 0.0
    hype_weight_sum = 0.0

    articles_used_sentiment = 0
    articles_used_hype = 0

    for article in articles:
        filt = article.get("filter") or {}
        if filt.get("excluded"):
            continue

        quality = calculate_quality_scores(article)
        scores = article.get("scores")

        if scores is None:
            continue

        # Sentiment weighted average
        if scores.get("sentiment_raw") is not None and filt.get("sentiment_usable") != "NO":
            weight = quality["quality_sentiment"]
            if filt.get("sentiment_usable") == "PARTIAL":
                weight *= 0.7
            sentiment_sum += scores["sentiment_raw"] * weight
            sentiment_weight_sum += weight
            articles_used_sentiment += 1

        # Hype weighted average (WEIGHTED BY SPECULATION SIGNAL)
        if scores.get("hype_raw") is not None and filt.get("hype_usable") != "NO":
            weight = quality["quality_hype"]
            if filt.get("hype_usable") == "PARTIAL":
                weight *= 0.7
            hype_sum += scores["hype_raw"] * weight
            hype_weight_sum += weight
            articles_used_hype += 1

    def get_confidence(weight_sum, articles_used):
        if articles_used == 0: return "INSUFFICIENT"
        elif weight_sum >= 200 and articles_used >= 4: return "HIGH"
        elif weight_sum >= 100 and articles_used >= 2: return "MEDIUM"
        else: return "LOW"

    return {
        "final_sentiment": round(sentiment_sum / sentiment_weight_sum, 1) if sentiment_weight_sum > 0 else None,
        "final_hype": round(hype_sum / hype_weight_sum, 1) if hype_weight_sum > 0 else None,
        "sentiment_confidence": get_confidence(sentiment_weight_sum, articles_used_sentiment),
        "hype_confidence": get_confidence(hype_weight_sum, articles_used_hype),
        "articles_used_sentiment": articles_used_sentiment,
        "articles_used_hype": articles_used_hype
    }

# --- 3. MAIN ANALYSIS FUNCTION ---

# Stores last LLM error per ticker for diagnostics in alerts
_last_llm_error: dict[str, dict] = {}

def analyze_hype_score(headlines: list[str], ticker: str, company_name: str, price_data: dict) -> dict | None:
    valid_headlines = [h for h in headlines if h != "N/A"]
    if not valid_headlines:
        return None

    # Context data
    analysis_date = price_data.get("trading_date", "Unknown")
    price = price_data.get("current_price", 0)
    ma_30 = price_data.get("ma_30", 0)
    gap_pct = price_data.get("gap_percent", 0)

    # Format Prompt
    articles_formatted = format_articles_for_prompt(valid_headlines)
    prompt = SHILLER_MEGA_PROMPT.format(
        ticker=ticker,
        company_name=company_name,
        analysis_date=analysis_date,
        price=price,
        ma_30=ma_30,
        gap_pct=gap_pct,
        num_articles=len(valid_headlines),
        articles_formatted=articles_formatted
    )

    # Use Gemini client
    if not gemini_client:
        logger.error("Gemini client not initialized")
        return None

    backoff_schedule = [2, 10, 30]  # seconds — escalating backoff for transient errors
    last_error = None

    for attempt in range(3):
        try:
            response = gemini_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt
            )
            text = response.text if hasattr(response, 'text') else str(response)
            if "```" in text:
                text = text.replace("```json", "").replace("```", "")

            llm_result = json.loads(text.strip())

            # Post-Processing (Python Math)
            aggregated = calculate_weighted_averages(llm_result["articles"])

            return {
                "metadata": llm_result["analysis_metadata"],
                "aggregated_scores": aggregated,
                "articles": llm_result["articles"]
            }

        except genai_errors.ClientError as e:
            # 4xx — permanent (auth, quota, bad request). No point retrying.
            logger.error(f"Attempt {attempt+1}/3 PERMANENT error (HTTP {e.code}): {e}. Aborting retries.")
            last_error = e
            break

        except genai_errors.ServerError as e:
            # 5xx — transient (overload, timeout). Worth retrying.
            wait = backoff_schedule[attempt]
            logger.warning(f"Attempt {attempt+1}/3 transient error (HTTP {e.code}): {e}. Retrying in {wait}s...")
            last_error = e
            time.sleep(wait)

        except Exception as e:
            wait = backoff_schedule[attempt]
            logger.warning(f"Attempt {attempt+1}/3 failed: {e}. Retrying in {wait}s...")
            last_error = e
            time.sleep(wait)

    is_permanent = isinstance(last_error, genai_errors.ClientError)
    error_detail = f"HTTP {last_error.code}: {last_error}" if isinstance(last_error, genai_errors.APIError) else str(last_error)
    logger.error(f"All attempts failed for {ticker}. Permanent={is_permanent}. Last error: {error_detail}")
    _last_llm_error[ticker] = {"permanent": is_permanent, "detail": error_detail}
    return None

# --- 4. DATABASE FUNCTION ---

# Retry configuration (tuned for Azure SQL Serverless cold starts which can take 30-60s)
DB_MAX_RETRIES = 5
DB_RETRY_BASE_DELAY = 15  # seconds -> delays: 15s, 30s, 45s, 60s (total ~150s coverage)


def _execute_database_save(final_data: dict, conn_str: str) -> bool:
    """Execute the actual database save operation. Returns True on success."""
    conn = None
    try:
        # Add connection timeout to prevent long hangs
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()

        meta = final_data["metadata"]
        scores = final_data["aggregated_scores"]

        sql_daily = """
            MERGE INTO Shiller.DailyScores AS target
            USING (SELECT ? AS date, ? AS ticker) AS source
            ON (target.date = source.date AND target.ticker = source.ticker)
            WHEN MATCHED THEN
                UPDATE SET final_sentiment = ?, final_hype = ?, sentiment_confidence = ?, hype_confidence = ?
            WHEN NOT MATCHED THEN
                INSERT (date, ticker, price, ma_30, gap_pct, final_sentiment, final_hype, sentiment_confidence, hype_confidence, articles_received, articles_used_sentiment, articles_used_hype)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        sent_val = float(scores["final_sentiment"]) if scores["final_sentiment"] is not None else 0.0
        hype_val = float(scores["final_hype"]) if scores["final_hype"] is not None else 0.0

        params_daily = (
            meta["analysis_date"], meta["ticker"],
            sent_val, hype_val, scores["sentiment_confidence"], scores["hype_confidence"],
            meta["analysis_date"], meta["ticker"], float(meta["price"]), float(meta["ma_30"]), float(meta["gap_pct"]),
            sent_val, hype_val, scores["sentiment_confidence"], scores["hype_confidence"],
            meta["articles_received"], scores["articles_used_sentiment"], scores["articles_used_hype"]
        )
        cursor.execute(sql_daily, params_daily)

        cursor.execute("DELETE FROM Shiller.Articles WHERE date = ? AND ticker = ?", meta["analysis_date"], meta["ticker"])

        sql_art = """
            INSERT INTO Shiller.Articles
            (date, ticker, article_num, headline_preview, is_about_company, sentiment_usable, hype_usable, excluded, exclusion_reason,
             centrality, credibility_sentiment, credibility_hype, recency, materiality, speculation_signal,
             quality_sentiment, quality_hype, sentiment_raw, hype_raw, reasoning)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for art in final_data["articles"]:
            qual = calculate_quality_scores(art)
            qm = art.get("quality_metrics") or {}
            sc = art.get("scores") or {}
            filt = art.get("filter") or {}

            params_art = (
                meta["analysis_date"], meta["ticker"], art.get("article_num"), (art.get("headline_preview") or "")[:250],
                filt.get("is_about_company"), filt.get("sentiment_usable"), filt.get("hype_usable"),
                1 if filt.get("excluded") else 0, filt.get("exclusion_reason"),
                qm.get("centrality"), qm.get("credibility_sentiment"), qm.get("credibility_hype"), qm.get("recency"), qm.get("materiality"), qm.get("speculation_signal"),
                qual["quality_sentiment"], qual["quality_hype"],
                sc.get("sentiment_raw"), sc.get("hype_raw"),
                (art.get("reasoning") or "")[:2000]
            )
            cursor.execute(sql_art, params_art)

        conn.commit()
        return True

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def save_to_sql_database(final_data: dict) -> bool:
    """Save analysis results to SQL database with retry logic. Returns True on success, False on failure."""
    if not final_data:
        return False

    conn_str = os.environ.get("SqlConnectionString")
    if not conn_str:
        logger.error("❌ SqlConnectionString not configured")
        return False

    ticker = final_data.get("metadata", {}).get("ticker", "unknown")
    last_error = None

    for attempt in range(DB_MAX_RETRIES):
        try:
            if _execute_database_save(final_data, conn_str):
                if attempt > 0:
                    logger.info(f"✅ Data saved for {ticker} (succeeded on attempt {attempt + 1})")
                else:
                    logger.info(f"✅ Data saved for {ticker}")
                return True
        except Exception as e:
            last_error = e
            if attempt < DB_MAX_RETRIES - 1:
                wait_time = DB_RETRY_BASE_DELAY * (attempt + 1)  # 10s, 20s, 30s
                logger.warning(f"⚠️ Database save attempt {attempt + 1}/{DB_MAX_RETRIES} failed for {ticker}: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Database save failed for {ticker} after {DB_MAX_RETRIES} attempts: {e}")

    return False


# --- 5. DATA FETCHING FUNCTIONS ---

def fetch_price_data(ticker: str) -> dict | None:
    """Fetch price data and check if market is open today."""
    logger.info(f"Fetching price data for {ticker}")

    try:
        stock = yf.Ticker(ticker)
        # Fetch slightly more data to ensure we have context even after long holidays
        df = stock.history(period="60d")

        if df is None or df.empty:
            logger.warning(f"No price data for {ticker}")
            return None

        # === STALE DATA GUARD ===
        # Ensure we are comparing dates correctly (UTC vs Date object)
        last_market_date = df.index[-1].date()
        today = datetime.now(timezone.utc).date()

        if last_market_date < today:
            logger.warning(
                f"🛑 Market closed today (Weekend/Holiday). "
                f"Yahoo data is stale (from {last_market_date}). "
                f"Skipping {ticker} to protect DB from overwrite."
            )
            return None
        # === END GUARD ===

        price = df['Close'].iloc[-1]
        ma_30 = df['Close'].tail(30).mean()
        # Calculate gap between current price and 30-day Moving Average
        gap_pct = ((price - ma_30) / ma_30) * 100

        return {
            "trading_date": last_market_date,
            "current_price": round(price, 2),
            "ma_30": round(ma_30, 2),
            "gap_percent": round(gap_pct, 2),
        }

    except Exception as e:
        logger.error(f"Failed to fetch price for {ticker}: {e}")
        return None


def fetch_news(ticker: str, trading_date) -> list[str]:
    """Fetch top 10 news articles from NewsAPI with 3-day lookback window."""
    api_key = os.environ.get("NEWSAPI_KEY")
    if not api_key:
        logger.warning("NEWSAPI_KEY not set, returning N/A.")
        return ["N/A"] * 10

    config = TICKER_NEWS_CONFIG.get(ticker, {"query": ticker})

    from_date = (trading_date - timedelta(days=3)).strftime("%Y-%m-%d")
    to_date = trading_date.strftime("%Y-%m-%d")

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": config["query"],
        "language": "en",
        "from": from_date,
        "to": to_date,
        "sortBy": "relevancy",
        "pageSize": 10,
        "apiKey": api_key,
    }

    logger.info(f"Fetching news for {ticker} | Date: {from_date} to {to_date}")

    news_max_retries = 3
    for attempt in range(news_max_retries):
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            articles = data.get("articles", [])
            headlines = []

            for article in articles[:10]:
                title = article.get("title") or ""
                desc = article.get("description") or ""
                full_text = f"{title}. {desc}".strip().replace("\n", " ").replace("\r", " ")
                headlines.append(full_text if full_text != "." else "N/A")

            while len(headlines) < 10:
                headlines.append("N/A")

            return headlines

        except requests.RequestException as e:
            if attempt < news_max_retries - 1:
                logger.warning(f"NewsAPI request failed for {ticker} (attempt {attempt + 1}/{news_max_retries}): {e}. Retrying in 5s...")
                time.sleep(5)
            else:
                logger.error(f"NewsAPI request failed for {ticker} after {news_max_retries} attempts: {e}")

    return ["N/A"] * 10


# --- 6. MAIN ORCHESTRATION ---

# Orchestration retry configuration
ORCHESTRATION_MAX_RETRIES = 2  # Retry failed tickers up to 2 more times
GEMINI_OVERLOAD_DELAY = 45 * 60  # 45 minutes — delayed retry when Gemini returns 503 for all tickers


def _process_single_ticker(ticker: str) -> tuple[dict | None, str | None]:
    """
    Process a single ticker through the full pipeline.
    Returns (analysis_result, error_type) where error_type is None on success.
    Error types prefixed with "PERMANENT:" won't be retried.
    """
    try:
        logger.info(f"{'='*60}")
        logger.info(f"ANALYZING: {ticker}")
        logger.info(f"{'='*60}")

        # 1. Fetch price data
        price_data = fetch_price_data(ticker)
        if price_data is None:
            logger.warning(f"⚠️ Skipping {ticker}: Could not fetch price data")
            return None, "price_data_fetch_failed"

        logger.info(f"Price: ${price_data['current_price']} | MA30: ${price_data['ma_30']} | Gap: {price_data['gap_percent']}%")

        # 2. Fetch news
        headlines = fetch_news(ticker, price_data["trading_date"])
        valid_count = len([h for h in headlines if h != "N/A"])
        logger.info(f"Fetched {valid_count} valid headlines")

        # 3. Run LLM analysis
        company_name = TICKER_NEWS_CONFIG.get(ticker, {}).get("company_name", ticker)

        logger.info("Sending to Gemini for detailed analysis...")
        analysis_result = analyze_hype_score(headlines, ticker, company_name, price_data)

        if not analysis_result:
            llm_err = _last_llm_error.get(ticker, {})
            detail = llm_err.get("detail", "unknown")
            prefix = "PERMANENT:" if llm_err.get("permanent") else ""
            logger.error(f"❌ Analysis failed for {ticker}: {detail}")
            return None, f"{prefix}llm_analysis_failed ({detail})"

        agg = analysis_result["aggregated_scores"]
        logger.info(f"✅ {ticker}: Sentiment={agg['final_sentiment']} ({agg['sentiment_confidence']}), Hype={agg['final_hype']} ({agg['hype_confidence']})")

        # Store original headlines for CSV export
        analysis_result["original_headlines"] = headlines

        # 4. Save to database (has its own retry logic)
        if save_to_sql_database(analysis_result):
            return analysis_result, None
        else:
            # Return the analysis result so we can retry just the DB save later
            return analysis_result, "database_save_failed"

    except Exception as e:
        logger.error(f"❌ Unexpected error processing {ticker}: {e}")
        return None, str(e)


def run_shiller_analysis() -> list[dict]:
    """Run the full Shiller Hybrid Index analysis for all tickers with retry logic."""
    # Email START
    try:
        _send_shiller_start_email()
    except Exception as e:
        logger.warning(f"[EMAIL] Blad wysylki START: {e}")

    results = []
    # Track failures with their analysis results (for DB-only retries)
    failed_tickers: list[tuple[str, str, dict | None]] = []  # (ticker, error_type, analysis_result)

    # First pass: process all tickers
    for ticker in TICKERS:
        analysis_result, error_type = _process_single_ticker(ticker)
        if error_type is None:
            results.append(analysis_result)
        else:
            failed_tickers.append((ticker, error_type, analysis_result))

    # Retry pass: attempt to recover failed tickers
    if failed_tickers:
        logger.info(f"\n{'='*60}")
        logger.info(f"RETRY PHASE: Attempting to recover {len(failed_tickers)} failed ticker(s)")
        logger.info(f"{'='*60}")

        still_failed = []

        for attempt in range(ORCHESTRATION_MAX_RETRIES):
            if not failed_tickers:
                break

            logger.info(f"\n--- Retry attempt {attempt + 1}/{ORCHESTRATION_MAX_RETRIES} ---")
            # Wait before retry to let transient issues resolve
            time.sleep(10)

            retry_queue = failed_tickers
            failed_tickers = []

            for ticker, error_type, cached_result in retry_queue:
                # Skip permanent errors — no point retrying (e.g. auth, quota)
                if error_type.startswith("PERMANENT:"):
                    logger.warning(f"⛔ Skipping retry for {ticker} — permanent error: {error_type}")
                    failed_tickers.append((ticker, error_type, cached_result))
                    continue

                # If we have cached analysis result and only DB failed, just retry DB save
                if error_type == "database_save_failed" and cached_result is not None:
                    logger.info(f"Retrying database save for {ticker}...")
                    if save_to_sql_database(cached_result):
                        logger.info(f"✅ Recovery successful for {ticker}")
                        results.append(cached_result)
                    else:
                        failed_tickers.append((ticker, error_type, cached_result))
                else:
                    # Need to re-run the full pipeline
                    logger.info(f"Retrying full pipeline for {ticker}...")
                    analysis_result, new_error_type = _process_single_ticker(ticker)
                    if new_error_type is None:
                        logger.info(f"✅ Recovery successful for {ticker}")
                        results.append(analysis_result)
                    else:
                        failed_tickers.append((ticker, new_error_type, analysis_result))

        still_failed = failed_tickers

        # Delayed retry wave: if ALL tickers failed with LLM errors (transient only), wait and retry
        llm_failures = [f for f in still_failed if "llm_analysis_failed" in f[1] and not f[1].startswith("PERMANENT:")]
        if llm_failures and len(llm_failures) == len(still_failed):
            logger.warning(
                f"⏳ All {len(llm_failures)} tickers failed with LLM errors (likely Gemini overload). "
                f"Waiting {GEMINI_OVERLOAD_DELAY // 60} minutes before delayed retry..."
            )
            time.sleep(GEMINI_OVERLOAD_DELAY)

            logger.info(f"\n{'='*60}")
            logger.info(f"DELAYED RETRY: Attempting {len(llm_failures)} ticker(s) after cooldown")
            logger.info(f"{'='*60}")

            still_failed = []
            for ticker, error_type, _ in llm_failures:
                logger.info(f"Delayed retry for {ticker}...")
                analysis_result, new_error_type = _process_single_ticker(ticker)
                if new_error_type is None:
                    logger.info(f"✅ Delayed recovery successful for {ticker}")
                    results.append(analysis_result)
                else:
                    still_failed.append((ticker, new_error_type, analysis_result))

            failed_tickers = still_failed

        # Final summary
        if still_failed:
            logger.error(f"❌ Permanently failed tickers after all retries: {[(t, e) for t, e, _ in still_failed]}")

    logger.info(f"✅ Successfully processed {len(results)}/{len(TICKERS)} tickers")

    # Email FINISH (always — success, warning, or failure)
    try:
        _send_shiller_finish_email(results, failed_tickers if failed_tickers else None)
    except Exception as e:
        logger.warning(f"[EMAIL] Blad wysylki FINISH: {e}")

    return results


def save_debug_csv(results: list[dict]):
    """Save results to local CSV files for debugging."""
    if not results:
        logger.warning("No results to save to CSV.")
        return

    # 1. Save Daily Summary
    with open("debug_shiller_daily.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Ticker", "Date", "Price", "Gap%", "Sentiment", "Hype", "Sent_Conf", "Hype_Conf", "Articles_Used"])

        for r in results:
            meta = r["metadata"]
            agg = r["aggregated_scores"]
            writer.writerow([
                meta["ticker"],
                meta["analysis_date"],
                meta["price"],
                meta["gap_pct"],
                agg["final_sentiment"],
                agg["final_hype"],
                agg["sentiment_confidence"],
                agg["hype_confidence"],
                f"'{agg['articles_used_sentiment']}/{meta['articles_received']}"
            ])

    # 2. Save Article Details
    with open("debug_shiller_articles.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Ticker", "Headline", "Full_Content", "Excluded?", "Reason",
            "Centrality", "Cred_Sent", "Cred_Hype", "Recency", "Materiality", "Speculation",
            "Quality_Sent", "Quality_Hype", "Raw_Sent", "Raw_Hype", "Reasoning"
        ])

        for r in results:
            ticker = r["metadata"]["ticker"]
            original_headlines = r.get("original_headlines", [])
            for art in r["articles"]:
                qm = art.get("quality_metrics") or {}
                sc = art.get("scores") or {}
                filt = art.get("filter") or {}

                # Calculate quality scores
                qual = calculate_quality_scores(art)

                # Get full content from original headlines by article_num (1-indexed)
                art_num = art.get("article_num", 1)
                full_content = original_headlines[art_num - 1] if art_num <= len(original_headlines) else ""

                writer.writerow([
                    ticker,
                    art.get("headline_preview", "")[:100],
                    full_content,
                    "YES" if filt.get("excluded") else "NO",
                    filt.get("exclusion_reason", ""),
                    qm.get("centrality", ""),
                    qm.get("credibility_sentiment", ""),
                    qm.get("credibility_hype", ""),
                    qm.get("recency", ""),
                    qm.get("materiality", ""),
                    qm.get("speculation_signal", ""),
                    qual["quality_sentiment"],
                    qual["quality_hype"],
                    sc.get("sentiment_raw", ""),
                    sc.get("hype_raw", ""),
                    art.get("reasoning", "")
                ])

    logger.info(f"Debug CSVs saved: debug_shiller_daily.csv & debug_shiller_articles.csv")


# --- MAIN ---
if __name__ == "__main__":
    logger.info("Starting Shiller Hybrid Index Analysis")
    results = run_shiller_analysis()

    # Save debug CSVs
    save_debug_csv(results)

    logger.info(f"\nAnalysis complete. Processed {len(results)} tickers.")

    # Print summary
    for r in results:
        meta = r["metadata"]
        agg = r["aggregated_scores"]
        print(f"\n{meta['ticker']}: Sentiment={agg['final_sentiment']}, Hype={agg['final_hype']}")