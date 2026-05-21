# Pracuj.pl scraper — diagnoza spadku 1747 → 629

**Data analizy:** 2026-05-14
**Sytuacja:** Daily run 2026-05-13 zwrócił 629 ofert (vs 1747 dzień wcześniej). Alert mówił "Kategorie OK (7), Puste (0)" — false positive.

## Fakty z logów

### Wczoraj (2026-05-12 22:14) — log `pracuj_scraper/full_run_2026-05-12.log`
Faza 1 zebrała 1747 URL-i, ale **bardzo nierówno**:

| Kategoria | Domena | Ofert (Faza 1) |
|---|---|---|
| Bankowosc | www.pracuj.pl | 500 (cap 10 str) |
| Finanse_Ekonomia | www.pracuj.pl | 471 |
| Marketing | www.pracuj.pl | 494 |
| IT_Business_Analytics | it.pracuj.pl | 282 (CF od str. 7) |
| IT_Data_BI | it.pracuj.pl | **0** (CF str. 1) |
| IT_AI_ML | it.pracuj.pl | **0** |
| IT_Project_Management | it.pracuj.pl | **0** |
| **SUMA** | | **1747** |

Faza 2: circuit-breaker abort po 15/1747 (CF "Cierpliwości..." na detail pages). Tylko **15 ofert trafiło do DB**. Mailowy "1747" to listing count, nie inserty.

### Dziś (2026-05-13 19:46) — log `logs/scrapers_2026-05-13.log`
Faza 1 zebrała 629 URL-i, **odwrotny układ**:

| Kategoria | Domena | Ofert (Faza 1) |
|---|---|---|
| Bankowosc | www.pracuj.pl | **0** (CF str. 1) |
| Finanse_Ekonomia | www.pracuj.pl | **0** |
| Marketing | www.pracuj.pl | **0** |
| IT_Business_Analytics | it.pracuj.pl | **0** |
| IT_Data_BI | it.pracuj.pl | **0** |
| IT_AI_ML | it.pracuj.pl | 307 |
| IT_Project_Management | it.pracuj.pl | 322 (serwer: 334) |
| **SUMA** | | **629** |

Faza 2 zadziałała poprawnie (cf_clearance z Faza 1 reused dla detail pages). 214 nowych ofert (po dedup) → pełne dane.

## Root cause

### Bug #1 (główny) — niestabilny CF Turnstile bypass w Faza 1
Camoufox + persistent context resolves CF **niedeterministycznie per-kategoria/per-domena**. Każdy run inna podzbiór kategorii przechodzi — wczoraj 4/7, dziś 2/7. Faza 1 nie ma retry — pojedyncza porażka "Cierpliwości..." na str. 1 = `break` całej kategorii (`pracuj_premium_scraper.py:241`).

### Bug #2 (validation gap) — wszystkie kategorie zawsze "OK"
`pracuj_premium_scraper.py:866-867`:
```python
for cat in CATEGORIES:
    result["categories_ok"].append(cat)   # bezwarunkowo!
```
Stąd email: "Kategorie OK (7), Puste (0)" mimo że 5 było pustych. Monitor nie alarmuje. Spadek 1747→629 zauważył tylko `_check_drop()` (heurystyka %), ale nie pokazał *które kategorie* są puste.

## Konsekwencje
- **Dni puste:** nawet 7 kategorii może paść (gdyby CF miał gorszy dzień), wtedy 0 ofert i dopiero `KRYTYCZNY: 0 ofert` zaalarmuje
- **DB cherry-pick:** każdy dzień ląduje skrajnie inny zbiór kategorii → niereprezentatywna baza
- **Wczorajsze 15 ofert w DB** to ekstremum (Faza 2 + circuit-breaker)

## Plan naprawy (3 etapy)

### Etap 1 — Fix walidacji (10 min, low risk) — REKOMENDOWANE NA POCZĄTEK
1. `collect_listing_urls()` zwraca dodatkowo dict `cat_counts: dict[str, int]`
2. W `run()` (linia 866): zamiast bezwarunkowego append, sprawdź `cat_counts[cat] > 0` → `categories_ok`, inaczej → `categories_empty`
3. Efekt: email pokaże "Kategorie puste (5): Bankowosc, Finanse_Ekonomia, ..." → wiemy realny stan

### Etap 2 — Retry per kategoria w Faza 1 (~2h + testy)
1. Jeśli kategoria zwraca 0 ofert (CF blok na str. 1) → zamknij Camoufox, sleep 30-60s, restart persistent context, retry. Max 2 razy.
2. Pomocniczo: rotacja kolejności kategorii (random.shuffle) — żeby nie zawsze ta sama kat. dostawała "pierwszy CF cios"

### Etap 3 — Warmup CF + cross-domain handling (optional)
1. Przed Faza 1: goto `pracuj.pl/` + `it.pracuj.pl/` z 5-10s sleep — żeby cf_clearance osiadł dla obu domen przed pętlą kategorii
2. Polite_delay między kategoriami zwiększyć z 1.5-3s do 5-10s
