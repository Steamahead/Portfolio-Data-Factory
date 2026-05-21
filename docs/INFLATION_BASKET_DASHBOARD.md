# Inflation Basket — Dashboard Strategy (V2 portfolio piece)

**Data:** 2026-05-07
**Cel:** Public dashboard pod LinkedIn launch. Audience: rekruterzy DA / DE / AI. Czas uwagi: 30 sekund.

## Two artefakty — nie myl ich

| Artefakt | Audience | Format | Cel |
|---|---|---|---|
| **Operacyjny raport** | Ja (autor) | Email co scrape | "Czy pipeline żyje" |
| **Public dashboard** | Rekruter / LinkedIn | Web (live URL) | "Czy autor umie myśleć analitycznie" |

Operacyjny raport NIE jest portfolio piece. Dashboard NIE jest QA tool. Decyzje per-artefakt różne.

## Public dashboard — co przekonuje rekrutera

### Co rekruter widzi w 30 sekund

1. Hero number — jedna duża teza ("+8.4% YoY w moim koszyku vs +3.2% GUS")
2. Methodology badge — "104 obs/scrape, 3-7×/tydz, Azure SQL, GitHub link"
3. Co najmniej 1 nieoczywisty insight (nie "ceny rosną" — coś specyficznego)

### Sekcje (kolejność = priorytet uwagi)

#### 1. Hero: headline thesis
> "Mój koszyk drożeje 2.6× szybciej niż CPI GUS — przez 3 produkty"

Jedna liczba (delta YoY mój vs GUS), jedna teza. Rekruter pamięta to po opuszczeniu strony.

#### 2. Drivery inflacji (decomposition)
Top 5 produktów odpowiadających za X% wzrostu koszyka. Bar chart "kontrybucja per produkt do total CPI mojego koszyka". To analityczne myślenie — nie pokazujesz "co drożeje", pokazujesz "kto napędza".

#### 3. Per kategoria GUS — mój vs GUS
Bar chart: kategoria GUS (Żywność / Mieszkanie / itd.) na osi X, dwa słupki (mój delta, GUS delta). Pokazuje GDZIE się nie zgadzasz z oficjalną statystyką. To **wartość edytorska** projektu — challenge GUS metodologii.

#### 4. Shrinkflation events
"Producent X zmniejszył opakowanie z 250g → 220g, cena trzyma się — realna inflacja 13.6%". Memorable, viralny temat na LinkedIn. Tabela: produkt, sklep, data, capacity before/after, implied inflation.

#### 5. Cross-store średnie (NIE per-produkt)
"Frisco średnio 12% drożej niż Auchan w warzywach, ale 5% taniej w mięsie".
Aggregate per kategoria, nie per-produkt. Rekruter widzi że umiesz robić cuts.

#### 6. Time-series eksplorator (power-user)
Interaktywny — wybierasz produkt, sklep → wykres. Mniej istotne dla 30s view, ale pokazuje głębokość.

#### 7. Methodology / pipeline
Diagram: scrape (Azure Functions / Task Scheduler) → Azure SQL → dashboard. Github link. Source code dostępny. Buduje zaufanie do danych.

### Czego NIE pokazywać na dashboardzie

- **Cross-store per produkt** (sól 138%, papier 798% itp.) — to QA debug, nie analiza. Średnie agregowane per kategoria — TAK.
- **Surowe tabele** z nazwami SKU.
- **Logi scrape**, coverage %, missing_today.
- **Anomalie parsera** — niech master catalog będzie czysty zanim sprawą będzie się chwalić.

### Storytelling rules

1. **Każda wizualizacja ma tezę w tytule** — nie "Cena jabłek per miesiąc" lecz "Cena jabłek +47% w lipcu — sezonowość, nie inflacja".
2. **Annotations** na ważnych zdarzeniach (Black Friday, święta, wojna).
3. **Methodology disclosure** — jeden klik dystans. Rekruter sprawdza.

### Tech stack — opcje

| Stack | Pros | Cons | Pasuje do |
|---|---|---|---|
| **Streamlit** | Python-native, szybko | Wygląda "data-sciency" | DA position |
| **Next.js + Recharts + Azure Functions API** | "Production-grade", impresses DE | Więcej pracy | DE / Full-stack |
| **Tableau Public** | Polished out-of-box | Mniej "engineering" cred | Pure DA / BI |

User profile (data engineer) → **Next.js + API endpoint** najlepiej rezonuje. Pokazuje że projekt to nie notebook, tylko produkt.

## Operacyjny raport — co tam zostawiamy

Raport email = **operacyjne**, NIE analityczne:
- ✅ Coverage, missing_today, stale_prices — sanity
- ✅ price_moves — sygnał trendu w czasie
- ✅ shrinkflation_candidates — sygnał capacity drop
- ❌ cross_store_anomalies — ŁADUJ tylko do internal QA panelu (np. Streamlit-only-for-me), nie do prompta LLM ani emaila operacyjnego
- ❌ aggregate cross-store per kategoria — to dashboardowy feature, nie QA

## Decyzja teraz (2026-05-07, faza kalibracji)

1. **Funkcja `_cross_store_anomalies` zostaje w kodzie** — bo dashboard później będzie z niej korzystał (do agregacji per kategoria, nie surowo).
2. **Z prompta LLM i z tabeli email — wycinamy.** Operacyjny raport ma być zwięzły.
3. **Public dashboard** to V2 — po 6 mies. zbierania danych (zgodnie z planem ze spec'u).

## Stop-signs / nie skupować

- Nie buduj dashboardu zanim mamy ≥3 mies. danych — wykresy będą puste, anti-portfolio.
- Nie publikuj LinkedIn-launch zanim master catalog jest 100% czysty (papier toaletowy 798% to cringe na publicznym dashboardzie).
- Nie pokazuj nikomu surowego raportu email — to debug, nie produkt.
