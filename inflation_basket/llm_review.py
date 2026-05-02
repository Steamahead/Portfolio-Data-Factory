"""Gemini Flash-Lite reviewer — reads quality report, returns severity verdict.

Free tier: gemini-3.1-flash-lite (15 RPM, 250K TPM, 500 RPD — way more than
3x/week needs). Structured output via response_schema = deterministic JSON.

Output schema:
  {
    severity: "ok" | "warning" | "critical",
    needs_intervention: bool,
    summary_pl: str,           # 2-3 zdania PL
    concerns: [
      { what: str, why: str, action: str, severity: str }
    ]
  }
"""

from __future__ import annotations

import json
import os
from typing import Any

GEMINI_MODEL = "gemini-3.1-flash-lite-preview"

REVIEW_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "severity": {
            "type": "STRING",
            "enum": ["ok", "warning", "critical"],
            "description": "Overall verdict",
        },
        "needs_intervention": {
            "type": "BOOLEAN",
            "description": "True if user must investigate now",
        },
        "summary_pl": {
            "type": "STRING",
            "description": "2-3 zdania po polsku z najważniejszą obserwacją",
        },
        "concerns": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "what": {"type": "STRING", "description": "Co poszło nie tak"},
                    "why": {"type": "STRING", "description": "Dlaczego to ważne"},
                    "action": {"type": "STRING", "description": "Konkretna akcja do zrobienia"},
                    "severity": {"type": "STRING", "enum": ["info", "warning", "critical"]},
                },
                "required": ["what", "why", "action", "severity"],
            },
        },
    },
    "required": ["severity", "needs_intervention", "summary_pl", "concerns"],
}

SYSTEM_PROMPT = """Jesteś analitykiem jakości danych dla pipeline'u inflation_basket — codziennego scrapera cen produktów spożywczych ze sklepów Frisco i Auchan w Warszawie. Cel: zbierać 52 produkty × 2 sklepy = 104 obserwacji per run, 3× w tygodniu.

Twoja rola: czytasz strukturalny raport jakości po każdym scrape i decydujesz, czy wymagana jest INTERWENCJA użytkownika.

Reguły:
- `severity=ok` — wszystko działa; user dostanie maila ale go zleje
- `severity=warning` — drobne anomalie, do przejrzenia gdy user ma czas
- `severity=critical` + `needs_intervention=true` — coś WYMAGA naprawy teraz (np. URL rot, parser wywalony, anomalna cena z błędu parsowania)

Progi w `thresholds`:
- coverage poniżej 100% — każdy missing produkt to potencjalny problem
- missing_today: jeśli `days_since` >= missing_critical_days → URL prawdopodobnie martwy
- price_move: |pct_change| >= price_move_critical_pct → najpewniej parser fail lub ekstremalna promocja
- stale_prices >= stale_critical_cycles → URL może być dead, sklep zwraca cache
- shrinkflation: capacity_drop bez proporcjonalnej obniżki ceny = realny shrinkflation alert
- cross_store: delta >= cross_store_delta_critical_pct → bardzo duża różnica, sprawdź czy parser dobrze odczytał capacity

Pisz po polsku, zwięźle. Dla każdego concern podaj KONKRETNĄ akcję (np. "Sprawdź URL Frisco dla ID 89 — produkt mógł zniknąć z oferty").

Jeśli wszystko OK: `concerns` może być puste, summary_pl: krótkie potwierdzenie ("Wszystko OK, X obs zapisanych, Y produktów stabilnie")."""


def _get_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None
    from google import genai
    return genai.Client(api_key=api_key)


def review_quality(report: dict) -> dict:
    """Send report to Gemini, return structured verdict.

    Returns fallback dict if no API key or Gemini fails.
    """
    client = _get_client()
    if not client:
        return _fallback_verdict(report, reason="no GEMINI_API_KEY in env")

    from google.genai import types

    prompt = f"""Raport jakości dla scrape z {report['scrape_date']}:

```json
{json.dumps(report, ensure_ascii=False, indent=2, default=str)}
```

Przeanalizuj raport i zwróć werdykt zgodny ze schematem."""

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                response_mime_type="application/json",
                response_schema=REVIEW_SCHEMA,
                temperature=0.1,
            ),
        )
        raw = response.text if hasattr(response, "text") else str(response)
        verdict = json.loads(raw)
        # Sanity: enforce types
        verdict.setdefault("concerns", [])
        verdict["needs_intervention"] = bool(verdict.get("needs_intervention", False))
        return verdict
    except Exception as e:
        return _fallback_verdict(report, reason=f"Gemini error: {str(e)[:120]}")


def _fallback_verdict(report: dict, reason: str) -> dict:
    """When LLM unavailable — derive verdict from raw thresholds."""
    coverage = report.get("coverage", {})
    missing_today = report.get("missing_today", [])
    crit_missing = [m for m in missing_today if m.get("severity") == "critical"]
    warn_missing = [m for m in missing_today if m.get("severity") == "warning"]
    price_crit = [p for p in report.get("price_moves", []) if p.get("severity") == "critical"]
    stale_crit = [s for s in report.get("stale_prices", []) if s.get("severity") == "critical"]

    if crit_missing or price_crit or stale_crit:
        sev = "critical"
    elif missing_today or report.get("price_moves") or report.get("shrinkflation"):
        sev = "warning"
    else:
        sev = "ok"

    cov_summary = ", ".join(f"{s} {v['observed']}/{v['expected']}" for s, v in coverage.items())
    return {
        "severity": sev,
        "needs_intervention": sev == "critical",
        "summary_pl": f"[FALLBACK: {reason}] Coverage: {cov_summary}. Missing critical: {len(crit_missing)}, warning: {len(warn_missing)}.",
        "concerns": [
            {
                "what": f"{m['name']} ({m['store']}) brak {m.get('days_since', '?')} dni",
                "why": "Możliwy URL rot lub product unavailable",
                "action": f"Sprawdź `inflation_product_urls` dla product_id={m['product_id']}",
                "severity": m["severity"],
            }
            for m in crit_missing[:5]
        ],
    }


if __name__ == "__main__":
    from datetime import date
    from inflation_basket.quality_report import build_quality_report

    report = build_quality_report([], today=date.today())
    verdict = review_quality(report)
    print(json.dumps(verdict, indent=2, ensure_ascii=False))
