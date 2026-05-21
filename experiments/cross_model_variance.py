"""
Cross-model + variance experiment for blog article.

Tests 3 Gemini models × 3 tickers × 5 repeats = 45 calls.
NOTHING goes to prod DB. Results saved locally:
  - experiments/cross_model_results.csv  (raw 45 rows)
  - experiments/cross_model_summary.md   (aggregated tables — mean/std)

Headlines + price are cached ONCE per ticker so all 45 calls see identical
input payload. Only model and run_idx vary.
"""

import csv
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Reuse shiller_index utilities (no DB writes — we only call Gemini directly)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shiller_index.shiller_logic import (  # noqa: E402
    fetch_news,
    format_articles_for_prompt,
    calculate_weighted_averages,
    SHILLER_MEGA_PROMPT,
    SHILLER_RESPONSE_SCHEMA,
    gemini_client,
    genai_types,
    TICKER_NEWS_CONFIG,
    TICKERS,
)
import yfinance as yf  # noqa: E402


def fetch_price_for_experiment(ticker: str):
    """Like fetch_price_data() but WITHOUT the prod stale-data guard.
    Used only for this offline experiment — never writes to DB.
    Uses last available yfinance close as analysis date.
    """
    stock = yf.Ticker(ticker)
    df = stock.history(period="60d")
    if df is None or df.empty:
        return None
    last_market_date = df.index[-1].date()
    price = df["Close"].iloc[-1]
    ma_30 = df["Close"].tail(30).mean()
    gap_pct = ((price - ma_30) / ma_30) * 100
    return {
        "trading_date": last_market_date,
        "current_price": round(price, 2),
        "ma_30": round(ma_30, 2),
        "gap_percent": round(gap_pct, 2),
    }

# Quiet shiller_logic INFO/DEBUG noise
import logging
logging.getLogger().setLevel(logging.WARNING)

MODELS = [
    "gemini-2.5-flash",
    "gemini-3-flash-preview",
    "gemini-3.1-flash-lite-preview",
]
RUNS_PER_COMBO = 5
SLEEP_BETWEEN_CALLS = 15  # seconds — RPM safety

OUTPUT_DIR = Path(__file__).resolve().parent
CSV_PATH = OUTPUT_DIR / "cross_model_results.csv"
MD_PATH = OUTPUT_DIR / "cross_model_summary.md"


def analyze_with_model(model_name: str, headlines, ticker, company_name, price_data):
    """Call Gemini with explicit model + response_schema. Returns aggregated dict or {'error': ...}."""
    valid = [h for h in headlines if h != "N/A"]
    if not valid:
        return {"error": "no_valid_headlines"}

    articles_formatted = format_articles_for_prompt(valid)
    prompt = SHILLER_MEGA_PROMPT.format(
        ticker=ticker,
        company_name=company_name,
        analysis_date=price_data["trading_date"],
        price=price_data["current_price"],
        ma_30=price_data["ma_30"],
        gap_pct=price_data["gap_percent"],
        num_articles=len(valid),
        articles_formatted=articles_formatted,
    )

    cfg_kwargs = dict(
        response_mime_type="application/json",
        response_schema=SHILLER_RESPONSE_SCHEMA,
        temperature=0.1,
    )
    # thinking_config only meaningful for Gemini 3 (3.0 has it on by default)
    if "3-flash-preview" in model_name:
        cfg_kwargs["thinking_config"] = genai_types.ThinkingConfig(
            thinking_level=genai_types.ThinkingLevel.MINIMAL
        )

    try:
        response = gemini_client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=genai_types.GenerateContentConfig(**cfg_kwargs),
        )
        text = response.text if hasattr(response, "text") else str(response)
        if "```" in text:
            text = text.replace("```json", "").replace("```", "")
        llm_result = json.loads(text.strip())
        agg = calculate_weighted_averages(llm_result["articles"])
        return agg
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:160]}"}


def cache_inputs():
    """Fetch yfinance + NewsAPI ONCE per ticker. Returns dict[ticker] = {price, headlines, company}."""
    cache = {}
    for ticker in TICKERS:
        price = fetch_price_for_experiment(ticker)
        if not price:
            print(f"  X {ticker}: no price data, skipping")
            continue
        headlines = fetch_news(ticker, price["trading_date"])
        company = TICKER_NEWS_CONFIG.get(ticker, {}).get("company_name", ticker)
        valid_count = len([h for h in headlines if h != "N/A"])
        cache[ticker] = {"price": price, "headlines": headlines, "company": company}
        print(f"  + {ticker}: {valid_count} headlines, ${price['current_price']}, MA30 ${price['ma_30']}")
    return cache


def write_csv(results):
    fieldnames = list(results[0].keys())
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def write_md(results, cache):
    lines = []
    lines.append("# Cross-Model Variance Experiment")
    lines.append("")
    lines.append(f"**Date:** {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}  ")
    lines.append(f"**Total calls:** {len(results)}  ")
    lines.append(
        f"**Setup:** {len(MODELS)} models x {len(cache)} tickers x {RUNS_PER_COMBO} runs, "
        "identical headlines+price payload (cached once per ticker)."
    )
    lines.append("")

    lines.append("## Aggregated (mean +/- std across 5 runs)")
    lines.append("")
    lines.append("| Ticker | Model | Sentiment mean | Sentiment std | Hype mean | Hype std | Avg latency | Errors |")
    lines.append("|---|---|---|---|---|---|---|---|")

    for ticker in cache:
        for model in MODELS:
            cell = [r for r in results if r["ticker"] == ticker and r["model"] == model]
            ok = [r for r in cell if not r.get("error")]
            err_count = len(cell) - len(ok)
            sent_vals = [r["final_sentiment"] for r in ok if r["final_sentiment"] is not None]
            hype_vals = [r["final_hype"] for r in ok if r["final_hype"] is not None]
            lat_vals = [r["elapsed_sec"] for r in ok]

            sent_m = round(statistics.mean(sent_vals), 2) if sent_vals else "-"
            sent_sd = round(statistics.stdev(sent_vals), 2) if len(sent_vals) > 1 else "-"
            hype_m = round(statistics.mean(hype_vals), 2) if hype_vals else "-"
            hype_sd = round(statistics.stdev(hype_vals), 2) if len(hype_vals) > 1 else "-"
            lat = round(statistics.mean(lat_vals), 1) if lat_vals else "-"
            lines.append(f"| {ticker} | {model} | {sent_m} | {sent_sd} | {hype_m} | {hype_sd} | {lat}s | {err_count}/{len(cell)} |")

    lines.append("")
    lines.append("## Raw results (45 rows)")
    lines.append("")
    lines.append("| Run | Ticker | Model | Sentiment | Hype | S-Conf | H-Conf | Latency | Error |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for r in results:
        err = r.get("error") or ""
        if err:
            lines.append(f"| {r['run_idx']} | {r['ticker']} | {r['model']} | - | - | - | - | {r['elapsed_sec']}s | {err[:80]} |")
        else:
            lines.append(
                f"| {r['run_idx']} | {r['ticker']} | {r['model']} | "
                f"{r['final_sentiment']} | {r['final_hype']} | "
                f"{r['sentiment_confidence']} | {r['hype_confidence']} | {r['elapsed_sec']}s |  |"
            )

    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main():
    start = datetime.now(timezone.utc)
    print(f"[{start:%H:%M:%S}] Cross-model variance experiment")
    print(f"  Models:  {MODELS}")
    print(f"  Tickers: {TICKERS}")
    print(f"  Runs/combo: {RUNS_PER_COMBO}")
    print(f"  Sleep between calls: {SLEEP_BETWEEN_CALLS}s")

    print(f"\n[Phase 1] Caching prices + headlines...")
    cache = cache_inputs()
    if not cache:
        print("X No tickers with valid data. Aborting.")
        return

    total = len(cache) * len(MODELS) * RUNS_PER_COMBO
    print(f"\n[Phase 2] Running experiment ({total} calls, est. ~{total * SLEEP_BETWEEN_CALLS // 60} min)...")

    results = []
    counter = 0
    for run_idx in range(1, RUNS_PER_COMBO + 1):
        for ticker, data in cache.items():
            for model in MODELS:
                counter += 1
                t0 = time.time()
                agg = analyze_with_model(
                    model, data["headlines"], ticker, data["company"], data["price"]
                )
                elapsed = round(time.time() - t0, 2)

                row = {
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "run_idx": run_idx,
                    "ticker": ticker,
                    "model": model,
                    "elapsed_sec": elapsed,
                    "final_sentiment": agg.get("final_sentiment"),
                    "final_hype": agg.get("final_hype"),
                    "sentiment_confidence": agg.get("sentiment_confidence"),
                    "hype_confidence": agg.get("hype_confidence"),
                    "articles_used_sentiment": agg.get("articles_used_sentiment"),
                    "articles_used_hype": agg.get("articles_used_hype"),
                    "error": agg.get("error"),
                }
                results.append(row)

                short = model.replace("gemini-", "").replace("-preview", "")
                if row["error"]:
                    print(f"  [{counter:2d}/{total}] r{run_idx} {ticker:4s} {short:24s} {elapsed:5.1f}s  X {row['error'][:50]}")
                else:
                    print(f"  [{counter:2d}/{total}] r{run_idx} {ticker:4s} {short:24s} {elapsed:5.1f}s  S={row['final_sentiment']} H={row['final_hype']}")

                if counter < total:
                    time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"\n[Phase 3] Writing outputs...")
    write_csv(results)
    print(f"  CSV: {CSV_PATH}")
    write_md(results, cache)
    print(f"  MD:  {MD_PATH}")

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\n[Done] Elapsed: {int(elapsed // 60)}m {int(elapsed % 60)}s")


if __name__ == "__main__":
    main()
