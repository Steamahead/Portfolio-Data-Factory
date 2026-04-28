# Jak nie zbankrutować na Gemini API: dzień z życia portfolio engineera po końcu trialu

**TL;DR:** Skończył mi się 90-dniowy trial Google AI Studio. Limity Free tier spadły z ~250 RPD na 20 RPD. Pipeline padał o połowie dnia. Zamiast wbić kartę i przejść na Tier 1, spędziłem popołudnie na trzech zmianach: wymiana modeli, sleep w klasyfikatorze, dedup po URL. Efekt: zero kosztu, zero 429-ek, ten sam wynik biznesowy. Oto jak.

---

## Pierwszy sygnał: dashboard świeci na czerwono

Wchodzę rano do AI Studio sprawdzić zużycie i widzę:

- **Total API Requests**: peak 2.5K/dzień
- **Total API Errors**: peak 2.3K (sic!) — głównie 429 TooManyRequests
- **Success rate**: leci w dół, w jednym punkcie do **0%**

Po kliknięciu w Rate Limits dla projektu:

| Model | RPM | TPM | RPD |
|---|---|---|---|
| `gemini-2.5-flash` (text-out) | **7 / 5** | 2.3K / 250K | **40 / 20** |

Czerwone paski. Limit 5 RPM, robię 7. Limit 20 RPD, robię 40.

Pierwsza myśl: **trial się skończył wczoraj**. To nie 250 RPD jak typowy publiczny Free tier — to 20 RPD post-trial. Spadek **12,5×** z dnia na dzień.

Druga myśl: kartę podpinać dopiero gdy mam dane, a nie gdy panikuję.

## Co tam siedzi w pipeline'ach

Trzy projekty z portfolio używają Gemini:

1. **CEE FX Volatility** — co godzinę pobiera RSS-y z polskich serwisów finansowych, klasyfikuje 30-40 nagłówków przez Gemini (kategoria + sentyment), wrzuca do Azure SQL. Korelacja sentymentu z volem EUR/PLN/CZK/HUF.
2. **Gov Spending Radar** — raz dziennie pobiera ogłoszenia z BZP, klasyfikuje sektorowo (IT/AI/Cybersec/Telecom/...).
3. **Shiller Index** — raz dziennie analizuje hype score dla 3 tickerów (NVDA/WMT/TSLA) na podstawie nagłówków newsowych. Mega-prompt z reasoning.

Wszystkie trzy lecą `gemini-2.5-flash` lub jego wariant. Wszystkie waliły 429 albo zaraz miały walić.

## Krok 1: zrozumieć krajobraz modeli

Włączam toggle "All models" w AI Studio i znajduję perełkę:

| Model | RPM | TPM | RPD |
|---|---|---|---|
| `gemini-2.5-flash` | 5 | 250K | 20 |
| `gemini-2.5-flash-lite` | 10 | 250K | 20 |
| `gemini-3-flash-preview` | 5 | 250K | 20 |
| **`gemini-3.1-flash-lite-preview`** | **15** | **250K** | **500** |
| `gemini-2.5-pro` | 0 / 0 | — | — |

Flash-Lite 3.1 ma **25× więcej RPD** niż Flash 3.0 i 50% więcej RPM. Pro modele są niedostępne na Free.

Dlaczego ta dysproporcja? Czytam dokumentację Google:

> Gemini 3.1 Flash-Lite is best for **high-volume agentic tasks, simple data extraction**, and extremely low-latency applications. Use cases: translation, **classification**, lightweight data processing, model routing.

Czyli Lite jest **literalnie zaprojektowany** pod klasyfikację nagłówków. To, co ja robię.

A Flash 3.0?

> The most powerful agentic and vibe-coding model, built on a foundation of **state-of-the-art reasoning**.

To jest do rozumowania. Do mojego Shillera, gdzie mega-prompt analizuje hype score, sentyment, kontekst rynkowy — czyli **reasoning**.

Mam dwa różne use case'y, każdy z innym idealnym modelem.

## Krok 2: kompromitujące odkrycie z poprzedniej iteracji

Otwieram kod Shillera i widzę:

```python
response = gemini_client.models.generate_content(
    model="gemini-3.1-flash-live-preview",
    ...
)
```

`flash-live-preview`. **Live**. Sprawdzam dokumentację:

> Live API supports low-latency, **bidirectional voice and video interactions** with Gemini.

To jest endpoint do streamingu audio/video, jak rozmowa głosowa z asystentem. Ja z niego strzelam batch-owym `generate_content()`. To jest jak wpisywać SQL do `tail -f`.

Sprawdzam `git log -p` — zmiana sprzed dwóch tygodni, commit message: *"Switch model from gemini-2.5-flash to gemini-3.1-flash-live-preview — handle 429 rate limits"*. Co-author: ja sam (Claude). Wybrałem Live, bo nazwa brzmiała lepiej. Pomyłka, której nikt do tej pory nie wyłapał, bo... działało (Live API ma wysokie limity).

Wniosek do CV: **engineering humility — verify the choice, not just the outcome**. Działający kod ≠ poprawny kod.

## Krok 3: split decision

```yaml
# cee_fx_volatility/config.yaml
gemini:
  model: "gemini-3.1-flash-lite-preview"  # high-volume classification

# gov_spending_radar/config.yaml
gemini:
  model: "gemini-3.1-flash-lite-preview"  # same — classification

# shiller_index/shiller_logic.py
model="gemini-3-flash-preview"  # smart reasoning for hype score
```

To nie jest "wybrałem ten droższy bo lepszy". To jest **dobranie modelu do problemu**. Lite klasyfikuje nagłówki, Flash 3.0 robi reasoning. Każdy sweetspotuje swoje RPD.

## Krok 4: smoke test, problem nr 2

Odpalam CEE FX news pipeline po zmianach. Po 16 nagłówkach:

```
[AI] Próba 1/3 nieudana: 429 RESOURCE_EXHAUSTED.
Quota exceeded for metric: GenerateRequestsPerMinutePerProjectPerModel
limit: 15, model: gemini-3.1-flash-lite
Please retry in 9.4s
```

Pierwsze 16 sklasyfikowanych poprawnie, potem ścina o RPM (limit 15). Patrzę w `classifier.py`:

```python
# Rate limiting — be polite to the API
time.sleep(0.5)  # 0.5s = 120 RPM. 8× ponad limit.
```

I retry sleep `time.sleep(2)` — czyli kolejna próba leci w **tej samej minucie** kiedy quota wciąż wybita. Naiwna ochrona.

Fix prosty:

```python
# Free tier 15 RPM = 1 req / 4s. 5s = 12 RPM with safety margin.
time.sleep(5)

# Retry: 30s czeka aż quota minutowa się odnowi
time.sleep(30)
```

39 nagłówków × 5s = 3.5 min runtime. Mieści się w 10-min limicie Azure Functions.

## Krok 5: prawdziwy problem (i prawdziwe rozwiązanie)

Kalkuluję RPD przy hourly cronie CEE FX:

> 24 runy/dzień × 39 nagłówków = **936 RPD**

Limit Lite to 500 RPD. Nawet z mądrzejszym modelem **wybijesz kwotę około południa**.

Czemu w ogóle klasyfikuję 39 nagłówków co godzinę? Otwieram kod:

```python
def _run_news_pipeline():
    records = fetch_news()        # pobiera RSS-y
    records = classify_batch(records)  # klasyfikuje WSZYSTKIE
    upload_news(records)          # MERGE po URL — DB deduplikuje
```

Aha. RSS feedy zwracają ostatnie 10-20 newsów na każdy feed. Dziennie pojawia się 5-10 **nowych** nagłówków, reszta to te same artykuły co godzinę. UNIQUE constraint w DB filtruje na uploadzie, ale **klasyfikator nie wie i strzela do Gemini niezależnie**.

To było marnowanie quoty od początku. Trial je tuszował.

Idempotentny fix:

```python
# Pobierz URL-e już sklasyfikowane (z non-NULL category)
already_classified = fetch_classified_news_urls()

# Klasyfikuj tylko nowe
new_records = [r for r in records if r["url"] not in already_classified]
print(f"[DEDUP] {len(records) - len(new_records)}/{len(records)} pominiętych")

classify_batch(new_records)
upload_news(records)  # upload wszystkich (DB MERGE i tak skleja)
```

Plus nowa funkcja w `db/operations.py`:

```python
def fetch_classified_news_urls() -> set[str]:
    """URLs już sklasyfikowane (category IS NOT NULL)."""
    cursor.execute("SELECT url FROM cee_news_headlines WHERE category IS NOT NULL")
    return {row[0] for row in cursor.fetchall()}
```

Set, bo `in` na secie to O(1) zamiast O(n) na liście.

## E2E test

```
[NEWS] Łącznie: 40 nagłówków do przetworzenia
[SQL] Połączono
[DEDUP] 14/40 już sklasyfikowanych w DB — pomijam, klasyfikuję 26 nowych
[AI] [1/26] Kurs euro dotarł do technicznych oporów... → GEOPOLITYKA (-0.20)
[AI] [2/26] Polska z drugim największym deficytem fiskalnym... → MAKROEKONOMIA (-0.70)
...
[AI] [26/26] Zyski przemysłu w Chinach wzrosły o 15,8%... → GEOPOLITYKA (+0.20)

[AI] Klasyfikacja: 26/26 OK, 0 bez klasyfikacji
[SQL] News upload: 40/40 rekordów
═══════════════════════════════════════════════════════
  PODSUMOWANIE
  News: 40 naglowkow → Azure SQL
        26 sklasyfikowanych przez Gemini
  Status: OK
═══════════════════════════════════════════════════════
```

- **40 → 14 z dedup → 26 nowych do Gemini**
- **26/26 OK, zero failed**
- **0 hits 429 RESOURCE_EXHAUSTED**
- 3× pojawiło się 503 UNAVAILABLE (server-side load Google) — retry sobie poradził

Projekcja produkcji przy 24 hourly runach:
- Pierwszy run dnia: ~30-40 nowych = 30-40 Gemini calls
- Kolejne runy: 0-5 nowych
- **~50-150 RPD** vs limit **500**. Zapas 3-10×.

Szafirowo zielony plan dla pipeline'u który dotąd musiał albo padać, albo kosztować.

## Czego się nauczyłem (i co warto przekazać dalej)

**1. Czytaj rate limits dla swojego projektu, nie publiczne docsy.**
Generic Free tier mówi "250 RPD". Mój projekt po trialu miał 20. Różnica 12,5×. AI Studio → Rate Limits → twój projekt → "All models". Tam są prawdziwe liczby.

**2. Model bywa pomyłką, której produkcja nie zauważy.**
`gemini-3.1-flash-live-preview` w batch `generate_content()` po prostu działał. Endpoint Live ma wyższe limity, więc nikt nigdy nie dostał 429. Przy migracji z 2.5 → 3.x pierwsze co warto zrobić to **nie ufać dokumentacji ani sobie**, tylko sprawdzić *category* modelu (Text-out vs Live API vs TTS).

**3. Najtańsza optymalizacja kosztów to "nie rób tego znowu".**
Dedup po URL to 30 linii kodu. Eliminuje 95% requestów. Zero quality loss — wręcz przeciwnie, każdy nagłówek dostaje pełen focus modelu raz. To jest lepsze niż batch (który by zaszumiał kontekst), lepsze niż większy quota (kosztowny), lepsze niż downgrade modelu (gorsza klasyfikacja).

**4. Free tier wystarczy, jeśli zaprojektujesz pod niego.**
Zacząłem dzień myśląc że muszę przejść na Tier 1 ($5–10/mies). Skończyłem na: Free wystarczy, ale po refactorze. To samo ML, ten sam wynik biznesowy, $0/mies. Czasami "skip the upgrade" to najlepsza inżynieria.

**5. Eval-driven decisions > intuition-driven decisions.**
Pokusa: "Dodajmy Flash 3.0 do klasyfikatorów, będzie mądrzejszy". Reality check: 20 RPD limit zabije pipeline w 4 godziny. Decyzja "Lite vs Flash 3.0" idzie do backlogu jako **pomiar** — 100 ręcznie oznaczonych próbek, precision/recall, F1. *Wtedy* decyzja oparta na danych, nie na "droższy = lepszy".

## Co dalej

Przed sobą mam:
- Eval klasyfikatora Gov na ~100 ręcznie oznaczonych próbkach (Lite vs Flash 3.0)
- Power BI dashboard spinający 7 pipeline'ów
- LinkedIn quarterly report z gov_spending anomaliami

Ale dziś — jedno przekierowanie modeli, jeden sleep, jeden dedup. Pipeline z padającego stał się tańszy, szybszy i bardziej idempotentny niż przed trialem.

To jest data engineering: nie "dodać AI", tylko "policzyć **co naprawdę kosztuje** i **kiedy** to jest darmowe".

---

*Kod tych zmian: [Portfolio-Data-Factory](https://github.com/Steamahead/Portfolio-Data-Factory) — commit `d464ad4`. Ten blog to część portfolio z domeny data eng / AI infra. Jeśli budujesz coś podobnego i odbijasz się od rate limitów Gemini, daj znać — chętnie porównam liczby.*
