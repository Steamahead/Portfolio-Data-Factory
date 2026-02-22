"""
CEE FX Volatility Pipeline — Orchestrator
==========================================
"Złoty pod Presją — CEE Edition"

Bada Spillover Effect zmienności walutowej w regionie CEE.
Hipoteza: szoki na PLN przenoszą się na CZK i HUF.

Dwa niezależne strumienie:
  1. FX: kursy EUR/PLN, EUR/CZK, EUR/HUF (yfinance, 1h)
  2. Newsy: nagłówki z polskich źródeł RSS + klasyfikacja Gemini

Usage:
  python cee_fx_volatility/main.py                  # bieżący okres (FX 5d + newsy)
  python cee_fx_volatility/main.py --backfill 30    # historyczne FX z ostatnich 30 dni
  python cee_fx_volatility/main.py --fx-only         # tylko kursy walut
  python cee_fx_volatility/main.py --news-only       # tylko newsy
"""

import argparse
import sys
import traceback
from datetime import datetime, timezone

# Windows UTF-8 fix
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def _run_fx_pipeline(backfill_days: int | None = None) -> dict:
    """Run FX data collection pipeline."""
    from .collectors.fx_collector import fetch_fx_data
    from .db.operations import upload_fx_rates

    print(f"\n{'─' * 55}")
    print("  STRUMIEŃ FX — Kursy walut CEE")
    print(f"{'─' * 55}")

    if backfill_days:
        print(f"  Tryb: backfill {backfill_days} dni")
    else:
        print("  Tryb: bieżący (ostatnie 5 dni)")

    records = fetch_fx_data(backfill_days=backfill_days)
    if not records:
        return {"fx_fetched": 0, "fx_uploaded": 0, "fx_errors": ["Brak danych FX"]}

    upload_result = upload_fx_rates(records)
    return {
        "fx_fetched": len(records),
        "fx_uploaded": upload_result["uploaded"],
        "fx_errors": upload_result["errors"],
    }


def _run_news_pipeline() -> dict:
    """Run news collection + AI classification pipeline."""
    from .collectors.news_collector import fetch_news
    from .ai.classifier import classify_batch
    from .db.operations import upload_news

    print(f"\n{'─' * 55}")
    print("  STRUMIEŃ NEWS — Polskie nagłówki finansowe")
    print(f"{'─' * 55}")

    records = fetch_news()
    if not records:
        return {"news_fetched": 0, "news_classified": 0, "news_uploaded": 0, "news_errors": ["Brak newsów"]}

    # Classify headlines with Gemini (graceful — failure = None fields)
    records = classify_batch(records)
    classified = sum(1 for r in records if r.get("category") is not None)

    upload_result = upload_news(records)
    return {
        "news_fetched": len(records),
        "news_classified": classified,
        "news_uploaded": upload_result["uploaded"],
        "news_errors": upload_result["errors"],
    }


def run(backfill_days: int | None = None, fx_only: bool = False, news_only: bool = False) -> dict:
    """
    Main pipeline. Runs FX and News streams independently.
    Failure of one stream does not stop the other.

    Returns:
        Result dict with metrics from both streams.
    """
    result = {
        "success": False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fx_fetched": 0,
        "fx_uploaded": 0,
        "fx_errors": [],
        "news_fetched": 0,
        "news_classified": 0,
        "news_uploaded": 0,
        "news_errors": [],
    }

    # Stream 1: FX
    if not news_only:
        try:
            fx_result = _run_fx_pipeline(backfill_days=backfill_days)
            result.update(fx_result)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"\n  [FX] KRYTYCZNY BŁĄD:\n{tb}")
            result["fx_errors"].append(f"Unhandled: {e}")

    # Stream 2: News (backfill nie dotyczy newsów — RSS nie ma paginacji)
    if not fx_only:
        try:
            news_result = _run_news_pipeline()
            result.update(news_result)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"\n  [NEWS] KRYTYCZNY BŁĄD:\n{tb}")
            result["news_errors"].append(f"Unhandled: {e}")

    # Overall success if at least one stream delivered data
    result["success"] = result["fx_uploaded"] > 0 or result["news_uploaded"] > 0

    # Summary
    print(f"\n{'═' * 55}")
    print("  PODSUMOWANIE")
    print(f"{'═' * 55}")
    print(f"  FX:    {result['fx_uploaded']} rekordów → Azure SQL")
    print(f"  News:  {result['news_uploaded']} nagłówków → Azure SQL")
    print(f"         {result['news_classified']} sklasyfikowanych przez Gemini")

    errors = result["fx_errors"] + result["news_errors"]
    if errors:
        print(f"  Błędy: {len(errors)}")
        for err in errors[:5]:
            print(f"    - {err}")

    print(f"  Status: {'OK' if result['success'] else 'FAIL'}")
    print(f"{'═' * 55}\n")

    return result


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="CEE FX Volatility Pipeline — Złoty pod Presją",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Przykłady:
  python -X utf8 cee_fx_volatility/main.py                  # bieżący okres
  python -X utf8 cee_fx_volatility/main.py --backfill 30    # FX z ostatnich 30 dni
  python -X utf8 cee_fx_volatility/main.py --fx-only        # tylko kursy
  python -X utf8 cee_fx_volatility/main.py --news-only      # tylko newsy
        """,
    )
    parser.add_argument(
        "--backfill",
        type=int,
        default=None,
        metavar="N",
        help="Pobierz historyczne dane FX z ostatnich N dni (max 730). Dotyczy WYŁĄCZNIE yfinance.",
    )
    parser.add_argument(
        "--fx-only",
        action="store_true",
        help="Uruchom tylko strumień FX (bez newsów)",
    )
    parser.add_argument(
        "--news-only",
        action="store_true",
        help="Uruchom tylko strumień newsów (bez FX)",
    )

    args = parser.parse_args()

    if args.fx_only and args.news_only:
        print("  [!] --fx-only i --news-only wzajemnie się wykluczają")
        sys.exit(1)

    print(f"\n{'═' * 55}")
    print("  CEE FX Volatility Pipeline")
    print("  Złoty pod Presją — CEE Edition")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═' * 55}")

    result = run(
        backfill_days=args.backfill,
        fx_only=args.fx_only,
        news_only=args.news_only,
    )

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
