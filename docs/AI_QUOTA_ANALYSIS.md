# AI Quota Analysis — 2026-04-26

## Diagnoza ze screenshota (Free tier, 7 dni)

- **1500–2500 requests/dzień** w peak'ach (Apr 21, 24, 25)
- **Bardzo dużo błędów 429 + 503** — czyli *już* uderzasz w limit Free tier
- Per-model graph: **Gemini 2.5 Flash, max ~40 req/dzień, 10K input tok**
- Reszta (~95% requestów) to **inne modele / inne projekty** lub zaokrąglenie wykresu

## Gdzie idą tokeny — analiza kodu

| Pipeline | Plik | Wzorzec wywołań | Model | Skala |
|---|---|---|---|---|
| **CEE FX classifier** | `cee_fx_volatility/ai/classifier.py:176` | `for rec in news_records` → 1 request/artykuł | `gemini-2.5-flash` | **dziesiątki–setki req/run** |
| **Gov Spending classifier** | `gov_spending_radar/ai/classifier.py:184` | `for notice in notices` → 1 request/notice | `gemini-2.5-flash` | **dziesiątki–setki req/run** |
| **Shiller** | `shiller_index/shiller_logic.py:589` | 3 mega-prompty (per ticker) | `gemini-3.1-flash-live-preview` | **3 req/run, ~22.5k tok** |

**Wniosek:** Główny żarł requestów to CEE + Gov klasyfikatory (loop per-record). Shiller to mały wolumen requestów, ale duże prompty.

## Pełne limity Free tier (Data Factory, 2026-04-26)

| Model | RPM | TPM | **RPD** | Notes |
|---|---|---|---|---|
| `gemini-2.5-flash` | 5 | 250K | **20** | obecny model klasyfikatorów — peak 40/20 = wybity |
| `gemini-2.5-flash-lite` | 10 | 250K | 20 | tylko 10 RPM |
| `gemini-3-flash` | 5 | 250K | 20 | jak 2.5 Flash |
| **`gemini-3.1-flash-lite`** | **15** | **250K** | **500** | **25× więcej RPD** ← BEST FREE OPTION |
| `gemini-3-flash-live` | unlimited | 65K | unlimited | Live API (streaming) |
| `gemini-2.5-flash-native-audio-dialog` | unlimited | 1M | unlimited | Live API |
| `gemini-2.5-pro`, `gemini-3.1-pro` | 0/0 | — | — | **niedostępne na Free** |

**Wniosek:** przełączenie klasyfikatorów na **`gemini-3.1-flash-lite`** daje:
- 500 RPD vs 20 RPD → **+2400% headroom**
- 15 RPM vs 5 RPM → 3× więcej tempo
- TPM ten sam (250K) → bez kompromisu na jakość batch

**Shiller** używa `gemini-3.1-flash-live-preview` (kod: `shiller_index/shiller_logic.py:589`) — to **Live API model**, który na Free ma **unlimited RPM/RPD** ale TPM 65K. Ponieważ Shiller leci 3×/dzień z mega-promptem, **TPM 65K może być za mało** dla 22.5k tok promptu — sprawdzić czy nie wybijasz TPM. Bezpieczniej przełączyć Shiller też na `gemini-3.1-flash-lite` (TPM 250K).

## Plan: zostać na Free tier z `gemini-3.1-flash-lite`

### Krok 1 — Zmień model klasyfikatorów (1-line change × 2)

```yaml
# cee_fx_volatility/config.yaml
gemini:
  model: "gemini-3.1-flash-lite"

# gov_spending_radar/config.yaml
gemini:
  model: "gemini-3.1-flash-lite"
```

**Efekt:** 500 RPD vs 20. Nawet bez batchu setki requestów/dzień zmieszczą się.

### Krok 2 — Batch klasyfikatorów (i tak warto)

Cel: zamiast `for rec in records → 1 req each`, wyślij wszystko jako JSON array w 1–3 requestach. Pliki:
- `cee_fx_volatility/ai/classifier.py:157` — funkcja `classify_batch`
- `gov_spending_radar/ai/classifier.py:160` — funkcja `classify_batch`

Mając 15 RPM = 1 req / 4s (z safety). 250K TPM input = ~5000 nagłówków × 50 tok każdy. Praktycznie żaden run się nie wybije.

### Krok 3 — Shiller: weryfikacja modelu

`shiller_index/shiller_logic.py:589` używa `gemini-3.1-flash-live-preview`. To Live API — TPM 65K, a Twój prompt ma ~7.5k tok × 3 tickery. Per-request mieści się, ale **Live API jest do streamingu**, nie do batch generate_content. Przełącz na `gemini-3.1-flash-lite`:

```python
response = gemini_client.models.generate_content(
    model="gemini-3.1-flash-lite",  # było: gemini-3.1-flash-live-preview
    ...
)
```

500 RPD vs unlimited nie ma znaczenia (Shiller robi 3 req/dzień), a TPM 250K vs 65K daje 4× zapas.

### Krok 4 — Pozostaw Tier 1 jako rezerwę

Po krokach 1–3 zużycie spadnie z ~2500 req/dzień do **<10 req/dzień**, mieszcząc się w Free z gigantycznym zapasem. Tier 1 staje się niepotrzebny. Jeśli portfolio urośnie 50× (więcej tickerów, krajów, pipelinów) — wracamy do tematu, koszt nadal byłby $1–5/mies.

## Akcja teraz (kolejność egzekucji)

1. **Edit `cee_fx_volatility/config.yaml`** — model na `gemini-3.1-flash-lite`
2. **Edit `gov_spending_radar/config.yaml`** — to samo
3. **Edit `shiller_index/shiller_logic.py:589`** — model na `gemini-3.1-flash-lite`
4. **Smoke test** — odpal CEE FX `--news-only` i Shiller na 1 tickerze, zweryfikuj że odpowiedź jest sensowna
5. **(Opcjonalnie) Batch klasyfikator** — jeśli chcesz przyszłościowo, ale to nie jest blokerem

Akcje 1–3 to ~3 linijki edycji i daje **25× więcej RPD od ręki, $0 koszt.**
