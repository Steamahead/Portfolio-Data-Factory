"""
Gemini AI Classifier — structured BZP notice classification.
=============================================================
Uses Google GenAI SDK with response_schema (Structured Outputs)
to classify Polish public procurement notices by sector.

Sectors: IT, CYBERSECURITY, AI, TELECOM, CONSTRUCTION, MEDICAL, ENERGY, INNE
Confidence: 0.0–1.0
"""

import json
import os
import time
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
    """Lazy-init Gemini client from env API key."""
    global _gemini_client
    if _gemini_client is not None:
        return _gemini_client

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("  [AI] Brak GEMINI_API_KEY — klasyfikacja LLM wyłączona")
        return None

    from google import genai
    _gemini_client = genai.Client(api_key=api_key)
    return _gemini_client


# ── Classification schema ─────────────────────────────────────────

CLASSIFICATION_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "sector": {
            "type": "STRING",
            "enum": [
                "IT",
                "CYBERSECURITY",
                "AI",
                "TELECOM",
                "CONSTRUCTION",
                "MEDICAL",
                "ENERGY",
                "INNE",
            ],
            "description": "Sektor zamówienia publicznego",
        },
        "confidence": {
            "type": "NUMBER",
            "description": "Pewność klasyfikacji od 0.0 do 1.0",
        },
    },
    "required": ["sector", "confidence"],
}

SYSTEM_PROMPT = """Jesteś ekspertem ds. zamówień publicznych w Polsce. Klasyfikujesz ogłoszenia z BZP (Biuletyn Zamówień Publicznych) do sektorów.

Sektory:
- IT: usługi informatyczne, oprogramowanie, sprzęt komputerowy, systemy informacyjne, bazy danych, chmura, ERP/CRM
- CYBERSECURITY: bezpieczeństwo IT, audyty bezpieczeństwa, SOC/SIEM, pentesty, ochrona danych
- AI: sztuczna inteligencja, uczenie maszynowe, deep learning, modele językowe, chatboty
- TELECOM: telekomunikacja, łączność, 5G, światłowody, sieci radiowe
- CONSTRUCTION: roboty budowlane, usługi architektoniczne/inżynieryjne, remonty, infrastruktura
- MEDICAL: sprzęt medyczny, usługi zdrowotne, leki, aparatura diagnostyczna
- ENERGY: paliwa, energia elektryczna, OZE, usługi komunalne, gaz
- INNE: wszystko co nie pasuje do powyższych sektorów (catering, ochrona fizyczna, transport, sprzątanie, itp.)

Confidence: 0.0 (brak pewności) do 1.0 (pewność całkowita).
Typowe wartości: 0.9+ gdy tytuł i CPV jednoznacznie wskazują sektor, 0.6-0.8 gdy jest pewna niejednoznaczność, <0.5 dla INNE."""


# ── Single notice classification ──────────────────────────────────

def classify_notice(
    title: str,
    cpv_code: str | None = None,
    cpv_raw: str | None = None,
    buyer_name: str | None = None,
) -> dict | None:
    """
    Classify a single BZP notice using Gemini structured output.

    Returns:
        Dict with keys: sector, confidence, raw_ai_response
        Or None if classification fails.
    """
    client = _get_client()
    if not client:
        return None

    config = _load_config()
    model_name = config.get("gemini", {}).get("model", "gemini-2.5-flash")

    parts = [f"Tytuł: {title}"]
    if cpv_code:
        parts.append(f"Kod CPV: {cpv_code}")
    if cpv_raw:
        parts.append(f"Kody CPV (pełne): {cpv_raw}")
    if buyer_name:
        parts.append(f"Zamawiający: {buyer_name}")

    prompt_text = "\n".join(parts)

    from google.genai import types

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=f"Sklasyfikuj to zamówienie publiczne:\n{prompt_text}",
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=CLASSIFICATION_SCHEMA,
                    temperature=0.1,
                ),
            )

            raw_text = response.text if hasattr(response, "text") else str(response)
            parsed = json.loads(raw_text)

            confidence = max(0.0, min(1.0, float(parsed["confidence"])))

            return {
                "sector": parsed["sector"],
                "confidence": confidence,
                "raw_ai_response": raw_text,
            }

        except Exception as e:
            print(f"  [AI] Próba {attempt + 1}/3 nieudana dla '{title[:40]}...': {e}")
            if attempt < 2:
                time.sleep(30)

    return None


# ── Batch classification ──────────────────────────────────────────

def classify_batch(notices: list[dict]) -> list[dict]:
    """
    Classify notices using Gemini LLM. Returns classification records
    ready for upload_classifications().

    Args:
        notices: list of dicts with object_id, title, cpv_code, cpv_raw, buyer_name

    Returns:
        List of classification dicts (notice_object_id, method, sector, confidence, raw_response).
        Empty list if Gemini is unavailable.
    """
    client = _get_client()
    if not client:
        print("  [AI] Gemini niedostępny — pomijam klasyfikację LLM")
        return []

    config = _load_config()
    delay = config.get("gemini", {}).get("classify_delay_seconds", 1.0)

    results = []
    classified = 0
    failed = 0

    for i, notice in enumerate(notices):
        title = notice.get("title", "")
        print(f"  [AI] [{i + 1}/{len(notices)}] {title[:60]}...", end=" ", flush=True)

        result = classify_notice(
            title=title,
            cpv_code=notice.get("cpv_code"),
            cpv_raw=notice.get("cpv_raw"),
            buyer_name=notice.get("buyer_name"),
        )

        if result:
            results.append({
                "notice_object_id": notice.get("object_id"),
                "method": "llm_gemini",
                "sector": result["sector"],
                "confidence": result["confidence"],
                "raw_response": result["raw_ai_response"],
            })
            classified += 1
            print(f"→ {result['sector']} ({result['confidence']:.2f})")
        else:
            failed += 1
            print("→ BRAK KLASYFIKACJI")

        if i < len(notices) - 1:
            time.sleep(delay)

    print(f"\n  [AI] Klasyfikacja LLM: {classified}/{len(notices)} OK, {failed} bez klasyfikacji")
    return results
