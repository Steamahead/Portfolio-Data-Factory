"""
Gemini AI Classifier — structured headline classification.
============================================================
Uses Google GenAI SDK with response_schema (Structured Outputs)
to classify Polish financial news headlines.

Categories: POLITYKA_KRAJOWA, MAKROEKONOMIA, RPP_STOPY, GEOPOLITYKA, INNE
Sentiment: -1.0 (negative) to 1.0 (positive)
Is_surprising: true/false (low reliability — LLM doesn't know market consensus)

UWAGA: Klasyfikacja AI jest przybliżona — brak ground truth do walidacji.
Pipeline pobiera nagłówki, nie pełne artykuły — kontekst jest ograniczony.
"""

import json
import os
import time
from enum import Enum
from pathlib import Path

import yaml

# ── Load config ────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Gemini client (lazy init) ─────────────────────────────────────

_gemini_client = None


def _get_client():
    """Lazy-init Gemini client from .env API key."""
    global _gemini_client
    if _gemini_client is not None:
        return _gemini_client

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("  [AI] Brak GEMINI_API_KEY — klasyfikacja wyłączona")
        return None

    from google import genai
    _gemini_client = genai.Client(api_key=api_key)
    return _gemini_client


# ── Classification schema ─────────────────────────────────────────

CLASSIFICATION_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "category": {
            "type": "STRING",
            "enum": [
                "POLITYKA_KRAJOWA",
                "MAKROEKONOMIA",
                "RPP_STOPY",
                "GEOPOLITYKA",
                "INNE",
            ],
            "description": "Kategoria nagłówka newsowego",
        },
        "sentiment": {
            "type": "NUMBER",
            "description": "Sentyment od -1.0 (negatywny) do 1.0 (pozytywny)",
        },
        "is_surprising": {
            "type": "BOOLEAN",
            "description": "Czy news jest zaskakujący dla rynku",
        },
    },
    "required": ["category", "sentiment", "is_surprising"],
}

SYSTEM_PROMPT = """Jesteś analitykiem rynków finansowych CEE. Klasyfikujesz nagłówki polskich newsów finansowych.

Kategorie:
- POLITYKA_KRAJOWA: polityka wewnętrzna Polski (wybory, rząd, partie, ustawy)
- MAKROEKONOMIA: dane makro (PKB, inflacja, bezrobocie, produkcja przemysłowa, PMI)
- RPP_STOPY: Rada Polityki Pieniężnej, stopy procentowe NBP, polityka monetarna
- GEOPOLITYKA: relacje międzynarodowe, UE, NATO, konflikty, sankcje
- INNE: wszystko inne (sport, kultura, technologia, krypto, itp.)

Sentyment: -1.0 (bardzo negatywny dla PLN) do 1.0 (bardzo pozytywny dla PLN).
Is_surprising: true jeśli news jest niespodziewany/nietypowy, false jeśli rutynowy."""


# ── Classification ─────────────────────────────────────────────────

def classify_headline(title: str) -> dict | None:
    """
    Classify a single headline using Gemini structured output.

    Returns:
        Dict with keys: category, sentiment, is_surprising, raw_ai_response
        Or None if classification fails.
    """
    client = _get_client()
    if not client:
        return None

    config = _load_config()
    model_name = config["gemini"]["model"]

    from google.genai import types

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=f"Klasyfikuj ten nagłówek: {title}",
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=CLASSIFICATION_SCHEMA,
                    temperature=0.1,
                ),
            )

            raw_text = response.text if hasattr(response, "text") else str(response)
            parsed = json.loads(raw_text)

            # Clamp sentiment to [-1.0, 1.0]
            sentiment = max(-1.0, min(1.0, float(parsed["sentiment"])))

            return {
                "category": parsed["category"],
                "sentiment": sentiment,
                "is_surprising": 1 if parsed.get("is_surprising") else 0,
                "raw_ai_response": raw_text,
            }

        except Exception as e:
            print(f"  [AI] Próba {attempt + 1}/3 nieudana dla '{title[:40]}...': {e}")
            if attempt < 2:
                time.sleep(2)

    return None


def classify_batch(news_records: list[dict]) -> list[dict]:
    """
    Classify all headlines in a batch. Modifies records in-place.
    If classification fails for a headline, AI fields remain None.

    Args:
        news_records: list of news dicts (from news_collector)

    Returns:
        Same list with AI fields populated where possible.
    """
    client = _get_client()
    if not client:
        print("  [AI] Gemini niedostępny — wszystkie newsy bez klasyfikacji")
        return news_records

    classified = 0
    failed = 0

    for i, rec in enumerate(news_records):
        title = rec.get("title", "")
        print(f"  [AI] [{i + 1}/{len(news_records)}] {title[:60]}...", end=" ", flush=True)

        result = classify_headline(title)
        if result:
            rec["category"] = result["category"]
            rec["sentiment"] = result["sentiment"]
            rec["is_surprising"] = result["is_surprising"]
            rec["raw_ai_response"] = result["raw_ai_response"]
            classified += 1
            print(f"→ {result['category']} ({result['sentiment']:+.2f})")
        else:
            failed += 1
            print("→ BRAK KLASYFIKACJI")

        # Rate limiting — be polite to the API
        time.sleep(0.5)

    print(f"\n  [AI] Klasyfikacja: {classified}/{len(news_records)} OK, {failed} bez klasyfikacji")
    return news_records
