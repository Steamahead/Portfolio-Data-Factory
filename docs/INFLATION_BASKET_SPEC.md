# Inflation Basket — Spec

> Status: **DRAFT — przed kodowaniem**. Domyka wątek "Inflation Scraper" otwarty w `STATUS.md` (2026-04-24).
> Owner: Damian. Sesja decyzyjna: 2026-04-30.

## 1. Cel projektu

Zbudować potok śledzący ceny **40 produktów spożywczych i FMCG** w **2 sklepach internetowych** (Frisco + Auchan zakupy/Warszawa) przez **6 miesięcy**. Wynik: dashboard Power BI pokazujący *"moją realną inflację"* z tezą porównawczą do GUS oraz wykrywaniem **shrinkflation** (zmniejszanie pojemności przy stałej cenie).

**Proof-points z konstytucji portfolio:**
- #1 E2E lifecycle (API/scrape → SQL → BI) ✅
- #2 Realne edge cases (404, ceny "z aplikacją", zmiany EAN, region pricing) ✅
- #3 AI + analityka — w **V1** (po MVP) ✅
- #4 Storytelling — *"moja inflacja vs GUS"* + shrinkflation jako narracja viralowa ✅

## 2. Decision log (sesja 2026-04-30)

| Decyzja | Wartość | Uzasadnienie |
|---------|---------|--------------|
| Filozofia koszyka | **B (Inżynier)** — własne zakupy, equal-weight | Domyka A/B/D z STATUS. Narracja "tak wyglądały MOJE zakupy" silniejsza niż abstrakcyjne GUS-stratified. Wagi GUS dopiero w V2 (ścieżka D). |
| Liczba produktów | **40** | 3-4 per kategoria GUS (12 grup) = threshold reprezentatywności. 30 = za mało (1 outlier psuje grupę). 50 = +25% maintenance dla +25% danych (diminishing returns). |
| Sklepy | **Frisco + Auchan zakupy (Warszawa)** | Research 2026-04-30: oba SPA z API/SSR. Frisco mniej agresywne anti-bot niż Pracuj. Auchan: wybór sklepu Warszawa raz na zawsze (ceny per region). Rezygnacja z Carrefour/Ceneo z STATUS — nie sprawdzone, dwóch wystarczy. |
| Cadence | **3× tygodniowo** (pn/śr/pt) | Wystarczy dla detekcji trendu cen FMCG (zmiany rzadko codzienne). 50% mniej maintenance load niż codziennie. |
| AI w MVP | **NIE** — V1 dopiero | "Tanio" + faster delivery → MVP w 4 tyg. AI dorzucone w V1 (+4 tyg.). |
| Folder | `inflation_basket/` | Spójność z `cee_fx_volatility/`, `gov_spending_radar/`, `shiller_index/`. |
| Kiedy start | **Teraz** (po tej sesji) | Time-critical — 6 mies. monitoringu = MVP musi ruszyć przed czerwcem. |

## 3. Hipotezy do udowodnienia

**Główna:**
> Realna inflacja koszyka konsumenta różni się istotnie od oficjalnego CPI GUS — w jakim kierunku i o ile?

**Podhipotezy:**
1. **Inflacja w premium (Frisco) vs hipermarket (Auchan)** różni się — która rośnie szybciej w 6-miesięcznym oknie?
2. **Shrinkflation** jest niewidoczny w klasycznym CPI — ile % pozornie stabilnych cen to faktyczne podwyżki ukryte przez zmniejszanie opakowań?
3. **Inflacja FX-exposed (importowane) vs lokalna** — produkty importowane (~17% koszyka, głównie IT/GR/EC) reagują na kurs EUR/USD/CHF inaczej niż produkty PL. Hipoteza testowalna: korelacja cen importu z FX rates z `cee_fx_volatility/`.

## 4. Scope i boundary

**IN scope (MVP):**
- 40 produktów × 2 sklepy × 3×/tydz. × 6 mies. = ~3120 obserwacji
- Schemat DB z `EAN` jako primary key (zgodnie z STATUS 2026-04-24)
- Idempotentne MERGE upserty (konwencja portfolio)
- Detekcja `unit_price_per_100g` dla cross-store comparison
- Monitoring przez `scraper_monitor` (reuse istniejącego)

**IN scope (V1, po MVP):**
- Gemini Flash-Lite klasyfikuje `category_gus` per produkt (jednorazowo)
- Detekcja shrinkflation: capacity_seen drift → `inflation_shrinkflation_events`
- Power BI z storytelling visuals

**IN scope (V2, +6 mies.):**
- Wagi GUS CPI → dual-index (mój / mój-GUS-weighted / oficjalny GUS)
- Public dashboard / LinkedIn launch
- Backfill GUS open data jako benchmark

**OUT of scope:**
- Biedronka, Lidl (anti-bot, ToS)
- Carrefour, Ceneo (rezygnacja — 2 sklepy wystarczą)
- Promocje "z aplikacją" (zbyt zmienne, kapitał na V2)
- Real-time alerts (codziennie wystarczy)

## 5. Sklepy — flow i zagrożenia

### Frisco
- **Tech**: React SPA, REST API `/api/v1/offer/products/query/`, ale API wymaga session cookie z frontu
- **Approach**: Playwright headless → odwiedź home (cookie session) → wywołaj API z tym samym browserem (page.evaluate fetch)
- **URL pattern**: `https://www.frisco.pl/pn,SLUG,2,SKU` (do potwierdzenia z prawdziwym SKU)
- **Anti-bot**: brak Cloudflare Turnstile w research (lekkie zabezpieczenie standard)
- **Ryzyko**: zmiana wersji bundle (`frisco-react-master-VERSION`) co kilka dni — endpointy API stabilne historycznie

### Auchan zakupy (Warszawa)
- **Tech**: SSR HTML z `window.__INITIAL_STATE__` 360KB. Bez wybranego sklepu ceny = `0,00 zł` placeholder
- **Approach**: Playwright headed → wybór sklepu Warszawa **raz** → cookie persistence → kategorie + produkty
- **URL pattern**: `https://zakupy.auchan.pl/shop-in-shop/SLUG?source=...` (do mapowania)
- **Anti-bot**: GraphQL endpoint zwraca 403 anonymously — workaround przez headed browser z session
- **Ryzyko**: zmiana strategii cenowej per sklep — hardcode warsaw-Modlińska jako baseline

## 6. Matching strategy (decyzja 2026-04-30)

Kluczowa decyzja design'u eksperymentu: **nie wszystkie produkty da się porównać 1:1 cross-store**. Dwa typy:

### `same_sku` (31 produktów ≈ 61% koszyka)
- Markowy FMCG, ten sam EAN spodziewany w obu sklepach
- Cross-store comparison **na poziomie SKU** — pełnoprawny signal "premium vs hipermarket dla TEGO SAMEGO towaru"
- Przykłady: Mleko Łaciate 1L, Czekolada Wedel 64% 100g, Mutti Polpa 400g, Vizir 1kg, Sensodyne 100ml, Hortex Groszek 450g, Velvet 8 rolek

### `logical_only` (20 produktów ≈ 39% koszyka)
- Świeże/luz/unbranded — fizycznie różne kawałki/sztuki, ale tej samej kategorii logicznej
- Cross-store comparison **na poziomie kategorii**, nie SKU — porównujemy "średnią cenę polędwicy wieprzowej /kg w Frisco vs Auchan"
- Pole `alternative_names` w schemacie pozwala scraperowi przyjąć zamienniki (np. stek wołowy ⊕ antrykot ⊕ roast beef = jedna pozycja)
- Przykłady: świeże owoce/warzywa, mięso/ryba luz, mąki, jajka, sałaty

**Implikacja dla analiz:**
- Visual "Frisco vs Auchan, ten sam produkt" — tylko `same_sku`, czysty signal
- Visual "Inflacja w mojej kategorii X" — oba typy, ale `logical_only` z większym confidence interval

## 7. Schemat DB

Konwencja: snake_case, `inflation_*` prefix, MERGE upsert, IF NOT EXISTS.

### `inflation_products` — master katalog (slowly changing)
| Kolumna | Typ | Notes |
|---|---|---|
| `product_id` | INT IDENTITY PK | auto |
| `ean` | NVARCHAR(13) UNIQUE | nullable (czasem brak, zwłaszcza `logical_only`) |
| `name_canonical` | NVARCHAR(200) | manualna nazwa kanoniczna |
| `brand` | NVARCHAR(100) | nullable |
| `category_user` | NVARCHAR(50) | nabial, mieso, wedlina, owoce, warzywa, tluszcze, przyprawy, slodycze, napoje, chemia, konserwy, mrozonki, maki, jajka, zboza |
| `category_gus` | NVARCHAR(50) | nullable do V1 (Gemini wypełnia) |
| `matching_type` | NVARCHAR(20) | `same_sku` \| `logical_only` (decyzja 2026-04-30 §6) |
| `capacity_value` | DECIMAL(10,3) | np. 1.000 (L), 500 (g) |
| `capacity_unit` | NVARCHAR(10) | g \| ml \| l \| kg \| szt \| rolek \| pack |
| `is_imported` | BIT | flag dla FX exposure analysis |
| `origin_country` | CHAR(2) | ISO-3166-1 alpha-2, nullable |
| `alternative_names` | NVARCHAR(500) | JSON array dla `logical_only` (np. ["antrykot", "roast beef"]) |
| `status` | NVARCHAR(20) | active \| discontinued |
| `created_at`, `updated_at` | DATETIME2 | |

**Upsert key:** `ean` (gdy NOT NULL) lub `product_id` (gdy ean IS NULL).

### `inflation_product_urls` — mapping produkt → sklep
| Kolumna | Typ | Notes |
|---|---|---|
| `product_id` | INT FK | |
| `store` | NVARCHAR(20) | `frisco` \| `auchan_warsaw` |
| `url` | NVARCHAR(500) | |
| `sku_store` | NVARCHAR(50) | wewnętrzny ID sklepu |
| `active` | BIT | false gdy 404 |
| `last_seen_at` | DATETIME2 | |

**Upsert key:** `(product_id, store)`.

### `inflation_observations` — fact table (historia cen)
| Kolumna | Typ | Notes |
|---|---|---|
| `product_id` | INT FK | |
| `store` | NVARCHAR(20) | |
| `obs_date` | DATE | dzień pomiaru |
| `obs_ts` | DATETIME2 | dokładny moment |
| `price_regular` | DECIMAL(10,2) | cena podstawowa |
| `price_promo` | DECIMAL(10,2) | nullable, gdy promo aktywne |
| `promo_active` | BIT | |
| `unit_price` | DECIMAL(10,4) | cena za 100g/100ml/szt — KLUCZOWE dla shrinkflation |
| `capacity_seen` | DECIMAL(10,3) | co sklep deklarował (do detekcji drift vs `inflation_products.capacity_value`) |
| `currency` | CHAR(3) | `PLN` |
| `created_at` | DATETIME2 | |

**Upsert key:** `(product_id, store, obs_date)`. Idempotentne — drugi run tego samego dnia nadpisuje.

### `inflation_shrinkflation_events` — V1 (Gemini-detected)
| Kolumna | Typ | Notes |
|---|---|---|
| `event_id` | INT IDENTITY PK | |
| `product_id` | INT FK | |
| `store` | NVARCHAR(20) | |
| `detected_at` | DATETIME2 | |
| `capacity_before`, `capacity_after` | DECIMAL(10,3) | |
| `price_before`, `price_after` | DECIMAL(10,2) | |
| `real_increase_pct` | DECIMAL(6,3) | (price_after/cap_after) / (price_before/cap_before) - 1 |
| `gemini_confidence` | DECIMAL(3,2) | 0.00 - 1.00 |
| `notes` | NVARCHAR(500) | opcjonalnie evidence |

## 7. Architektura kodu

```
inflation_basket/
├── main.py                    # CLI: --backfill N, --store frisco|auchan, --product-id X
├── config.yaml                # store config, Auchan store ID, retry params
├── stores/
│   ├── __init__.py
│   ├── frisco.py              # Playwright headless + API call
│   └── auchan.py              # Playwright headed (Cloudflare-safe)
├── ai/                        # V1 only — pusty katalog w MVP
│   └── classifier.py          # Gemini Flash-Lite: category_gus + shrinkflation detection
├── db/
│   ├── schema.py              # CREATE_TABLE_SQL (4 tabele)
│   └── operations.py          # MERGE_SQL, upsert_observation, upload_to_azure_sql
└── seed/
    └── products.py            # Lista 40 produktów + URL × 2 sklepy (manual fixture)
```

**Orkiestracja:** lokalny Task Scheduler, 3×/tydz. (pn/śr/pt 22:00 — po Pracuj 19:00, brak konfliktu).

**Reuse z portfolio:**
- `pracuj_scraper/` — Playwright headed pattern, browser context per page, anti-bot sleep
- `gov_spending_radar/db/` — schema + operations boilerplate
- `scraper_monitor.py` — monitoring + alert email

## 8. Plan AI (V1, po MVP)

**Gdzie LLM realnie zarabia (nie dorzucony "dla zasady"):**

1. **Klasyfikacja `category_gus`** — Gemini Flash-Lite mapuje `name_canonical` → kategoria GUS CPI. Jednorazowo dla 40 produktów. Koszt: ~$0.10 łącznie.
2. **Ekstrakcja capacity z nazwy** — gdy sklep nie podaje strukturalnie ("Mleko Łaciate UHT 3,2% 1L" → `{capacity: 1, unit: L}`).
3. **Detection shrinkflation** — gdy `capacity_seen` z observation różni się od `inflation_products.capacity_value`, wywołaj Gemini z evidence (URL, opis) → confidence + notes.
4. **Auto-insighty dla dashboardu** (V2) — generowanie tygodniowego summary: "ten tydzień pieczywo +X%, najmocniej Y, podejrzenie shrinkflation w Z".

**Koszty AI estymacja:** <$2/mies. dla całego pipelinu V1.

## 9. Power BI mockup (V1)

6 visuals:
1. **Time series**: index inflacji koszyka (mój) vs CPI GUS (line chart, 6-mies.)
2. **Heatmap**: % zmiana cen × kategoria × miesiąc
3. **Premium vs Hipermarket**: Frisco vs Auchan jako 2 linie. Filtr: tylko `matching_type='same_sku'` (czysty signal, 31 produktów)
4. **Shrinkflation table**: produkty z wykrytym zjawiskiem, real_increase_pct
5. **Dashboard hero**: "Moja inflacja w X tygodniu: +Y%" — KPI card
6. **Inflacja importowa vs lokalna**: 9 produktów `is_imported=true` (Grana, Mutti, Monini, Granoro, Kalamata, owoce egzotyczne) jako osobny indeks. Overlay z EUR/PLN i USD/PLN z `cee_fx_volatility/` — testuje hipotezę 3 (FX exposure)

## 10. Harmonogram

| Faza | Czas | Deliverables |
|------|------|--------------|
| **MVP** | 4 tyg. | Schema + Frisco scraper + Auchan scraper + monitoring + minimal Power BI (1 visual: time series) |
| **V1** | +4 tyg. | AI klasyfikacja kategorii, shrinkflation detection, Power BI (5 visuals) |
| **V2** | po 6 mies. zbierania | GUS weights, dual-index, public dashboard, LinkedIn launch |

**MVP breakdown** (przy 3-4h/tydz. budżetu):
- Tydz 1: schema DB, wybór 40 produktów, ręczne mapowanie URL × 2 sklepy (~10h setup)
- Tydz 2: Frisco scraper + tests
- Tydz 3: Auchan scraper + tests + monitoring integration
- Tydz 4: Power BI minimal + Task Scheduler setup + first prod run

**V1 breakdown:**
- Tydz 5-6: AI category mapper (jednorazowy)
- Tydz 7: shrinkflation detection logic
- Tydz 8: Power BI 5 visuals + insighty narracyjne

## 11. Sustainability gates (dla mojego stanu psychicznego)

> Z konstytucji 2026-04-30: tracę wiarę, lęk przed depresją. Sustainability > velocity.

**Gate 1 (po MVP, tydz. 4):** Czy mam siły na V1?
- TAK → kontynuuj
- NIE → MVP idzie w produkcję, V1 pauza 2-3 mies. Nie strata — zbieram dane, AI dorzucę gdy energia wróci.

**Gate 2 (po V1, tydz. 8):** Czy projekt działa stabilnie?
- TAK → 6 mies. cierpliwego zbierania, brak nowych features
- NIE → backlog cleanup, redukcja scope

**Gate 3 (mies. 6):** V2 launch?
- Naturalny milestone — masz 6 mies. danych, możesz pokazać. Tu max impact dla LinkedIn/portfolio.

**Stop sign — kiedy przerwać projekt:**
- 2 tyg. z rzędu bez sukcesu w naprawie scrapera
- > 2h/tydz. maintenance przez 4 tyg. z rzędu
- Lub: drugi pipeline z portfolio idzie w dół z powodu czasu na ten

## 12. Risk register

| Ryzyko | Prawdopod. | Impact | Mitigation |
|--------|---|---|---|
| Frisco zmienia API/bundle | Średnie | Wysoki | Monitoring + alert; reuse Pracuj recovery patterns |
| Auchan blokuje session bot | Średnie | Wysoki | Headed browser, polite delay, manual cookie refresh |
| Produkt zostaje wycofany | Wysokie | Niski | `status='discontinued'`, dodaj zamiennik; planuj 5 zapasowych URL |
| Cena per region (Auchan) | Pewne | Średni | Hardcode 1 sklep Warszawa, dokumentuj |
| Wypalenie owner'a | Średnie | Krytyczny | Sustainability gates 1-3, pause-not-kill default |
| GUS zmienia metodologię CPI | Niskie | Średni | Wagi GUS dopiero V2, mała inwestycja |
| ToS Frisco/Auchan zmiana | Niskie | Wysoki | Review robots.txt co 3 mies., backup plan: GUS open data only |

## 13. References

- `docs/PROJECT_SPEC.md` — architektura monorepo, konwencje DB
- `docs/STATUS.md` — wątek "Inflation Scraper" (2026-04-24) zamknięty tym specem
- `pracuj_scraper/` — wzorzec Playwright headed
- `gov_spending_radar/db/` — wzorzec schema + operations
- `scraper_monitor.py` — wzorzec monitoringu
- Memory: `user_mental_state_and_deep_motive.md` — sustainability constraints
- Memory: `project_career_strategy.md` — deadline maj 2027

---

**Następny krok:**
- ✅ Lista 51 produktów wybrana (sesja 2026-04-30) — patrz `inflation_basket/seed/products.py`
- ⏳ `inflation_basket/db/schema.py` — CREATE_TABLE_SQL dla 4 tabel (z polami matching_type, is_imported, origin_country, alternative_names)
- ⏳ Frisco scraper stub — Playwright headless + page.evaluate() na `/api/v1/offer/products/query/`
- ⏳ Manual URL mapping × 51 produktów × 2 sklepy (~10h, jednorazowe) — do zrobienia przez owner'a po przygotowaniu scrapera

**Lista produktów — final stats (z `seed/products.py`, korekty 2026-05-01):**
- Total: **52** (zatwierdzona)
- Per kategoria: nabial=7, mieso=6 (z osobnym antrykotem), warzywa=7, owoce=5, chemia=5, przyprawy=4, slodycze=4, wedlina=3, konserwy=2, mrozonki=2, maki=2, zboza=2, tluszcze=1, napoje=1, jajka=1
- Matching: 29 same_sku (56%) / 23 logical_only (44%)
- Imported: 9 (~17%) — IT × 5 (Grana, Mutti, Monini, Granoro, ...), GR × 1, EC/ES/BR × 3 (owoce egzotyczne)
- Marek unikalnych: 24 (Kotanyi i Targroch przeszły do logical_only z brand=None bo nie ma w Frisco)

**Korekty 2026-05-01 (po Frisco recce):**
- **Stek wołowy ⊕ Antrykot wołowy** rozdzielone na 2 osobne wpisy (były razem przez `alternative_names`). Damian: "stek + antrykot to 2 fizyczne kawałki, śledzimy oba".
- **Vizir 1kg → 2.42kg/44 prań** (Frisco nie ma 1kg)
- **Muszynianka zgrzewka 9L → butelka 1.5L** (zgrzewka niedostępna online)
- **Erytrytol Targroch → logical_only** (Frisco ma Big Nature, Auchan może mieć inne)
- **Sól himalajska Kotanyi → logical_only** (Frisco ma Sante)
