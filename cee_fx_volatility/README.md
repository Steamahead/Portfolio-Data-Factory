# Zloty pod Presja — CEE Edition

Data pipeline badajacy Spillover Effect zmiennosci walutowej w regionie CEE.

**Hipoteza**: szoki na PLN (wywolane polskimi newsami politycznymi/makro) przenosza sie na CZK i HUF, bo inwestorzy traktuja CEE jako koszyk.

## Architektura

Dwa niezalezne strumienie danych:

```
main.py (orchestrator + CLI)
├── Strumien FX:    yfinance → walidacja → Azure SQL (cee_fx_rates)
└── Strumien News:  RSS (tytul+opis) → filtr spamu → Gemini AI → Azure SQL (cee_news_headlines)
```

Strumienie dzialaja niezaleznie — awaria jednego nie blokuje drugiego. W weekendy newsy moga splywac bez danych FX.

## Uruchomienie

```bash
# Z katalogu Portfolio-Data-Factory (projekt-root)
.venv\Scripts\activate

# Biezacy okres (FX z ostatnich 5 dni + aktualne newsy)
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main

# Backfill — historyczne dane FX z ostatnich N dni (max 730)
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --backfill 30

# Tylko kursy walut
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --fx-only

# Tylko newsy
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --news-only

# Reklasyfikacja newsow z NULL category (po podpieciu klucza Gemini)
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --reclassify

# Jednorazowe czyszczenie: usun stare artykuly i auto-generated FX headlines z bazy
.venv\Scripts\python.exe -X utf8 -m cee_fx_volatility.main --cleanup
```

**Wymagane zmienne srodowiskowe** (w `.env` w katalogu projektu):
- `SqlConnectionString` — connection string do Azure SQL
- `GEMINI_API_KEY` — klucz API Google Gemini (opcjonalny — bez niego newsy sa zapisywane bez klasyfikacji)

## Data Dictionary

### Tabela `cee_fx_rates`

| Pole | Typ | Zrodlo | Opis |
|------|-----|--------|------|
| `timestamp` | NVARCHAR(30), PK | yfinance | Timestamp swieczki w UTC (ISO 8601) |
| `currency_pair` | NVARCHAR(10), PK | config | EUR/PLN, EUR/CZK lub EUR/HUF |
| `open` | REAL | yfinance | Cena otwarcia |
| `high` | REAL | yfinance | Najwyzsza cena w okresie |
| `low` | REAL | yfinance | Najnizsza cena w okresie |
| `close` | REAL | yfinance | Cena zamkniecia |
| `volume` | REAL, nullable | yfinance | Tick volume — zawsze 0 dla par EUR/CEE (FX OTC, brak danych) |
| `volatility_1h` | REAL | obliczane | (high - low) / open — rozstep swiecy |
| `created_at` | DATETIME | system | Timestamp insertu/updatu w bazie |

**Zakresy walidacji** (rekordy poza zakresem sa odrzucane):
- EUR/PLN: 3.0 – 6.0
- EUR/CZK: 20.0 – 30.0
- EUR/HUF: 300.0 – 500.0

### Tabela `cee_news_headlines`

| Pole | Typ | Zrodlo | Opis |
|------|-----|--------|------|
| `id` | INT, PK, IDENTITY | system | Auto-increment ID |
| `published_at` | NVARCHAR(30), nullable | RSS feed | Data publikacji skonwertowana do UTC |
| `fetched_at` | NVARCHAR(30) | system | Moment pobrania w UTC |
| `source` | NVARCHAR(20) | config | bankier_waluty / bankier_gielda / money / investing_economy |
| `title` | NVARCHAR(1000) | RSS feed | Naglowek artykulu |
| `description` | NVARCHAR(MAX), nullable | RSS feed | Opis/summary z RSS (HTML stripped). Uzywany przez Gemini do klasyfikacji razem z tytulem. |
| `url` | NVARCHAR(2000), UNIQUE | RSS feed | URL artykulu (klucz deduplikacji) |
| `category` | NVARCHAR(30), nullable | Gemini AI | POLITYKA_KRAJOWA / MAKROEKONOMIA / RPP_STOPY / GEOPOLITYKA / INNE |
| `sentiment` | REAL, nullable | Gemini AI | -1.0 (negatywny) do 1.0 (pozytywny) |
| `is_surprising` | BIT, nullable | Gemini AI | 1 = zaskakujacy, 0 = rutynowy (niska wiarygodnosc) |
| `raw_ai_response` | NVARCHAR(MAX), nullable | Gemini AI | Pelna odpowiedz JSON z Gemini (do audytu) |
| `created_at` | DATETIME | system | Timestamp insertu/updatu w bazie |

## Przykladowe SQL Queries

```sql
-- Top 10 godzin z najwyzsza zmiennoscia EUR/PLN
SELECT TOP 10 timestamp, volatility_1h, [open], high, low, [close]
FROM cee_fx_rates
WHERE currency_pair = 'EUR/PLN'
ORDER BY volatility_1h DESC;

-- Srednia zmiennosc per para walutowa
SELECT currency_pair, AVG(volatility_1h) AS avg_vol, COUNT(*) AS n_bars
FROM cee_fx_rates
GROUP BY currency_pair;

-- Rozklad kategorii newsow
SELECT category, COUNT(*) AS cnt,
       AVG(sentiment) AS avg_sentiment
FROM cee_news_headlines
WHERE category IS NOT NULL
GROUP BY category
ORDER BY cnt DESC;

-- Newsy z najsilniejszym negatywnym sentymentem
SELECT TOP 20 published_at, source, title, category, sentiment
FROM cee_news_headlines
WHERE sentiment IS NOT NULL
ORDER BY sentiment ASC;

-- Korelacja: zmiennosc EUR/PLN vs liczba newsow per godzina
SELECT
    SUBSTRING(f.timestamp, 1, 13) AS hour,
    AVG(f.volatility_1h) AS avg_vol,
    COUNT(DISTINCT n.id) AS news_count
FROM cee_fx_rates f
LEFT JOIN cee_news_headlines n
    ON SUBSTRING(f.timestamp, 1, 13) = SUBSTRING(n.published_at, 1, 13)
WHERE f.currency_pair = 'EUR/PLN'
GROUP BY SUBSTRING(f.timestamp, 1, 13)
ORDER BY hour DESC;
```

## Filtry newsow

Pipeline stosuje trzy warstwy filtrowania:

1. **Spam phrases** — naglowki zawierajace frazy z `config.yaml` (np. "artykul sponsorowany")
2. **Stale articles** — artykuly starsze niz `max_article_age_days` (domyslnie 7 dni). Bankier.pl trzyma w RSS archiwalne artykuly z lat poprzednich.
3. **Auto-generated FX headlines** — automatyczne raporty walutowe Money.pl (np. "Ile kosztuje euro?", "Kurs euro do zlotego PLN/EUR"). Zero wartosci informacyjnej, zawyza kategorie MAKROEKONOMIA.

Tryby specjalne:
- `--reclassify` — pobiera z bazy newsy z `category IS NULL`, przepuszcza przez Gemini (tytul + opis), robi UPDATE. Przydatne po podpieciu klucza API.
- `--cleanup` — jednorazowo usuwa z bazy rekordy, ktore zostaly zapisane przed wdrozeniem filtrow (stale articles + auto-generated FX headlines).

## Zrodla RSS

| Zrodlo | Feed | Source w bazie | Description w RSS |
|--------|------|----------------|-------------------|
| Bankier.pl — Waluty | `waluty.xml` | bankier_waluty | tak (summary z HTML) |
| Bankier.pl — Gielda | `gielda.xml` | bankier_gielda | tak (summary z HTML) |
| Money.pl | `rss.xml` | money | tak (summary z HTML) |
| Investing.com PL Economy | `news_95.rss` | investing_economy | nie — klasyfikacja tylko po tytule |
| ISBnews.pl | — | — | feed zwraca HTTP 404 (sprawdzono 2026-02-24) |
| PAP Biznes | — | — | brak publicznego RSS |

Dwa feedy Bankiera (waluty + gielda) moga zwracac te same artykuly (np. decyzje RPP). Deduplikacja: batch-level (seen_urls) + UNIQUE constraint na URL + MERGE SQL.

Pole `description` w bazie jest czyszczone z tagow HTML (`_strip_html()`) i uzywane przez Gemini razem z tytulem do klasyfikacji. Format promptu: "Tytul: {title}\nOpis: {description}". Investing.com nie dostarcza description — klasyfikacja oparta wylacznie na tytule.

## Znane ograniczenia

1. **yfinance to nieoficjalne API** — moze przestac dzialac bez ostrzezenia. Yahoo Finance nie oferuje oficjalnego darmowego API.
2. **Klasyfikacja AI (Gemini) jest przyblizona** — brak ground truth do walidacji. Model widzi naglowek + opis RSS (jesli dostepny), nie pelny artykul.
3. **Pole `is_surprising` ma niska wiarygodnosc** — LLM nie zna konsensusu rynkowego ani oczekiwan analitykow.
4. **Pole `volume` (tick volume)** nie odzwierciedla rzeczywistego wolumenu na rynku FX. FX jest rynkiem OTC.
5. **Pipeline pobiera naglowki + opisy RSS** — kontekst klasyfikacji obejmuje tytul i opis (jesli feed go dostarcza). Bankier i Money zwracaja opisy. Investing.com nie zwraca opisu.
6. **Tylko polskie newsy** — brak grupy kontrolnej dla newsow CZ/HU. To confounding variable w analizie spillover.
7. **Investing.com feed zawiera newsy globalne** — PMI Mozambiku, Zambii itp. trafiaja jako INNE. Rozwazyc dodatkowy filtr CEE-only w przyszlosci.
7. **Limit backfillu: 730 dni** — ograniczenie yfinance dla danych godzinowych (1h interval).
8. **RSS nie wspiera paginacji wstecz** — modul newsow zawsze pobiera tylko aktualny stan feedu (ostatnie ~20-50 artykulow).
9. **Brak formalnych testow** — walidacja przez `--fx-only`, `--news-only`, oraz manualne uruchomienia.
