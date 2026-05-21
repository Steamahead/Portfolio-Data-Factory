# Gemini 3 Flash zwraca uszkodzony JSON co trzecie wywołanie. Diagnoza i fix w jednym commicie

**TL;DR:** Mój pipeline finansowy padł na produkcji o 21:31 UTC z alertem "0/3 tickerów". Diagnoza zajęła dwa kroki, bo pierwsza hipoteza (thinking mode) była błędna. Druga hipoteza okazała się prawdziwa: Flash 3.0 bez `response_schema` produkuje **uszkodzony JSON ~33% wywołań** przy długim outputzie. Fix to **8 linii** w `GenerateContentConfig`. Po — 6/6 first-try success w smoke testach. Materiał empiryczny + kod + lekcje pod koniec.

---

## Pierwszy sygnał: alert email z prod

Wieczorem dostaję maila z mojego pipeline'u Shiller Hybrid Index (analiza spekulacyjnego hype'u na akcjach przez Gemini):

```
🚨 Shiller Hybrid Index — Alert
Czas: 2026-04-27 21:31 UTC
Przetworzono: 0/3 tickerów
Nieudane tickery:
•  NVDA — llm_analysis_failed
•  WMT  — llm_analysis_failed
•  TSLA — llm_analysis_failed
```

Trzy ticker'y, trzy `llm_analysis_failed`. Nie 429, nie 503, nie timeout. **LLM odpowiedział, ale parsing padł.**

Wchodzę do logów:

```
Attempt 1/3 failed: Expecting ',' delimiter: line 93 column 5 (char 2995). Retrying...
Attempt 2/3 failed: Expecting ',' delimiter: line 93 column 5 (char 2995). Retrying...
Attempt 3/3 failed: Expecting ',' delimiter: line 93 column 5 (char 2995). Aborting.
```

Trzy próby, trzy razy ten sam błąd. Czyli **nie transient — model konsekwentnie produkuje broken JSON**.

## Pierwsza diagnoza (błędna): "to thinking mode"

Model w produkcji to wtedy `gemini-3-flash-preview`. Kilka tygodni wcześniej zapisałem sobie w notatkach feedback:

> Flash 3.0 ma thinking mode włączone domyślnie — emituje chain-of-thought przed JSON payload. `json.loads()` próbuje parsować "Let me analyze... {json}" → fail.

Brzmiało jak match. Pisałem patch:

```python
config=types.GenerateContentConfig(
    thinking_config=types.ThinkingConfig(
        thinking_level=types.ThinkingLevel.MINIMAL
    ),
)
```

Plus defensywne filtrowanie thought parts (na wypadek gdyby MINIMAL nie wystarczyło):

```python
parts = response.candidates[0].content.parts
text = "".join(p.text for p in parts if not getattr(p, "thought", False) and p.text)
```

Deploy. Smoke test. Lokalnie odpalam pipeline:

```
NVDA: attempt 1 — HTTP 503 (transient overload)
NVDA: attempt 2 — HTTP 200 ALE Expecting ',' delimiter: line 93 column 5
NVDA: attempt 3 — HTTP 200, parsed OK ✓
WMT: attempt 1 — OK
TSLA: attempt 1 — OK
```

**3/3 zapisane**, ale NVDA potrzebowała 3 prób, w tym jednej z **identycznym broken JSON** jak wczoraj. Czyli thinking nie był (jedynym) problemem.

## Druga diagnoza: to NIE thinking, to długość outputu

Komunikat błędu jest czujnym sygnałem: `Expecting ',' delimiter: line 93 column 5 (char 2995)`.

Gdyby thinking "wyciekał", widziałbym `Expecting value` na linii 1 — bo `json.loads()` próbowałby parsować "Let me analyze..." od początku. Tu mam **strukturalnie poprawny początek JSON-a, który gubi przecinek w 93. linii**. Model nie myśli "obok" — model **psuje JSON w trakcie pisania**.

Plus: filtruję `part.thought` defensywnie, więc do `json.loads` trafia wyłącznie non-thought content. Thinking jest wykluczone.

Trzy alternatywne hipotezy:

### 1. Long-output coherence loss

LLM-y bez gramatycznego constraintu tracą spójność syntaktyczną im dłuższy output. Output Shillera to ~3000 znaków JSON: tablica 10 obiektów `articles[]`, każdy z 4 zagnieżdżonymi obiektami (`filter`, `quality_metrics`, `scores`, `reasoning`). To duża gęstość przecinków, nawiasów i cudzysłowów do utrzymania w głowie.

Flash 3.0 wydaje się słabiej wytrenowany na trzymanie JSON syntaxy w długich strukturach niż 2.5. Przypomina to znany efekt "drift" przy generowaniu długich list.

### 2. Brak constrained decoding = zero gwarancji

Bez `response_schema` Gemini "improwizuje" JSON na każdym tokenie. Sampler nie ma żadnego sygnału że "tu MUSI być przecinek bo właśnie skończyłeś wartość" — może wybrać dowolny token z dystrybucji, a w 1/3 przypadków wybierze cudzysłów lub nawias zamiast przecinka.

Z `response_schema` SDK przekazuje schemę do API, a sampler **wymusza per-token że każdy następny token musi być valid w grammar danej schemy**. Cała klasa błędów syntaktycznych znika z definicji.

### 3. Preview model immaturity

`gemini-3-flash-preview` to wersja zapowiadana. Production-ready GA może być stabilniejsza. Ale na dziś — workujemy z tym co jest.

Hipoteza #2 jest najbardziej testowalna i obiecuje deterministyczny fix.

## Fix: 8 linii w `GenerateContentConfig`

Schema musi mirrorować strukturę output JSON-a z prompta. Mój wzorzec wygląda tak:

```python
SHILLER_RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "analysis_metadata": {
            "type": "OBJECT",
            "properties": {
                "ticker": {"type": "STRING"},
                "price": {"type": "NUMBER"},
                # ... pozostałe pola
            },
            "required": ["ticker", "price", ...],
        },
        "articles": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "filter": {
                        "type": "OBJECT",
                        "properties": {
                            "is_about_company": {
                                "type": "STRING",
                                "enum": ["PRIMARY", "MENTIONED", "NO"],
                            },
                            "excluded": {"type": "BOOLEAN"},
                            "exclusion_reason": {
                                "type": "STRING",
                                "nullable": True,
                            },
                            # ...
                        },
                    },
                    "scores": {
                        "type": "OBJECT",
                        "properties": {
                            "sentiment_raw": {"type": "NUMBER", "nullable": True},
                            "hype_raw": {"type": "NUMBER", "nullable": True},
                        },
                    },
                    "reasoning": {"type": "STRING"},
                },
            },
        },
    },
    "required": ["analysis_metadata", "articles"],
}
```

Kluczowe drobiazgi:
- **`enum`** na polach kategorialnych (model nie może wymyślić `MAYBE` zamiast `YES|PARTIAL|NO`)
- **`nullable: True`** na polach które bywają puste (excluded articles → `sentiment_raw: null`)
- **`required`** na każdym poziomie — model musi wypełnić wszystko, koniec z "zapomniał o `articles_excluded`"

Wpięcie do `generate_content`:

```python
response = client.models.generate_content(
    model="gemini-3-flash-preview",
    contents=prompt,
    config=types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(
            thinking_level=types.ThinkingLevel.MINIMAL
        ),
        response_mime_type="application/json",
        response_schema=SHILLER_RESPONSE_SCHEMA,
        temperature=0.1,
    ),
)
```

Schemę zostawiam jako **lider obrony** — `thinking_level=MINIMAL` i filter `part.thought` zostają jako defense in depth (gdyby Google coś zmienili w API).

## Empiryczne porównanie before/after

Smoke test: dwa runy ze schemą, każdy 3 tickery, ten sam input data (NewsAPI window 2026-04-24 do 2026-04-27).

| Metryka | Przed schemą (rano) | Po schemie (run #1) | Po schemie (run #2) |
|---|---|---|---|
| First-try success | 2/3 (NVDA padła 2× przed OK) | **3/3** | **3/3** |
| Suma JSON parse errors | 1 | **0** | **0** |
| Czas wykonania | ~3 min (z retry'ami) | ~90s | ~90s |

**6/6 first-try success ze schemą** vs 2/3 bez. Próba mała, ale różnica jakościowa: w obu runach **zero JSONDecodeError** w logach, vs 1 na 3 ticker'y bez schemy.

Dodałem `[METRIC] json_decode_failure` tag w except handlerze, żeby przez tydzień zbierać dane z Application Insights:

```python
except json.JSONDecodeError as e:
    wait = backoff_schedule[attempt]
    logger.warning(
        f"[METRIC] json_decode_failure attempt={attempt+1}/3 "
        f"ticker={ticker} error={e}. Retrying in {wait}s..."
    )
```

KQL żeby policzyć w Application Insights:

```kql
traces
| where message contains "[METRIC] json_decode_failure"
| summarize count() by bin(timestamp, 1d)
```

Przewidywanie: 0 wpisów przez tydzień. Jeśli się nie sprawdzi — wracam i piszę errata.

## Secondary finding: schema rozwiązuje **syntax**, nie **variance**

Coś czego nie spodziewałem się przed testem: porównanie wartości scoringu między run #1 i run #2 ze schemą:

| Ticker | Run #1 sentiment | Run #2 sentiment | Run #1 hype | Run #2 hype |
|---|---|---|---|---|
| NVDA | -7.5 | **+4.7** | 62.4 | 62.5 |
| WMT  | 42.7 | 39.6 | 32.1 | 34.1 |
| TSLA | 14.3 | **-6.2** | 67.8 | **87.7** |

NVDA i WMT hype są stabilne (Δ < 2). Ale **NVDA sentiment skacze z -7.5 na +4.7** (przesunięcie sign w skali -100 do +100) i **TSLA hype +20 punktów**. Same dane wejściowe, ten sam config, `temperature=0.1`.

To **inherent variance Flash 3.0** — niezależna od schemy. Tak samo skakałoby bez schemy. Schema fixuje deterministyczność **formatu**, nie **treści**.

Praktyczna konsekwencja: jeśli Twój pipeline polega na precyzyjnych liczbach z LLM-a (mój nie — agreguję ważoną średnią z 10 artykułów, więc szum się znosi), `response_schema` Cię nie uratuje. Będziesz potrzebował:
- Wielu zapytań i agregacji (jak ja), ALBO
- `temperature=0`, ALBO
- Większego modelu (Pro), ALBO
- Postprocessingu, który zaakceptuje przedział zamiast pojedynczej liczby

## Lekcje

1. **Pierwsze podejrzenie nie zawsze trafia.** Mój feedback memory podpowiadał "to thinking mode". Było to częściowo prawdziwe (thinking mode JEST problemem dla Flash 3.0 generally), ale **nie tłumaczyło wczorajszego incydentu**. Dane w error message — `Expecting ',' delimiter: line 93` — wyraźnie wskazywały że to coś innego. Czytaj komunikaty błędów dokładnie.

2. **`json.loads(response.text)` to hack.** Działa w 95% przypadków, ale jeśli puścisz to do prod bez `response_schema`, masz ukrytą bombę zegarową. Im dłuższy output i mniej "dotrenowany" model, tym częściej wybuchnie.

3. **Structured outputs są tańsze niż retry loop.** 1 poprawne wywołanie z schemą < 3 retry'e. Mniej kwoty API, mniej czasu, mniej alertów email. Inwestycja w schemę zwraca się od pierwszego dnia.

4. **Schema nie psuje rozumowania.** Bałem się że ograniczę modelowi kreatywność. Empirycznie — kategorie confidence (`HIGH/MEDIUM/LOW`) zostały bez zmian, scores w realistic range, jakość treści `reasoning` field bez różnicy. Schema to gramatyka formatu, nie ograniczenie semantyczne.

5. **Defense in depth.** Trzymam jednocześnie: `thinking_level=MINIMAL`, filter `part.thought`, `response_schema`, retry loop. Każde z nich łapie inną klasę problemów. Jeśli Google jutro zmieni domyślne zachowanie API — przynajmniej jedna warstwa zadziała.

## Co dalej

Przez tydzień zbieram metryki z Application Insights. Jeśli `json_decode_failure` count zostaje 0 — hipoteza #2 potwierdzona, fix stabilny, można wrócić do tematu z "case closed". Jeśli pojawi się choć jeden — diagnoza wraca na warsztat (możliwe że potrzeba też temperature=0 albo wymiana modelu na 2.5 z `thinking_budget=0`).

Jeśli macie pipeline z `gemini-3-flash-preview` + `json.loads()` na końcu, zerknijcie do logów na wpisy "Expecting ',' delimiter" lub "Expecting property name". Jeśli są — schemę macie do napisania, ale fix to dosłownie popołudnie pracy.

---

**Stack:** Python 3.12, `google-genai` SDK v1.33, Gemini 3.0 Flash Preview, Azure Functions Linux Consumption.

**Repo:** [Portfolio-Data-Factory](https://github.com/Steamahead/Portfolio-Data-Factory) (branch `main`, commit `552d5f5`).
