# BZP API Reconnaissance Report

**Date:** 2026-02-26
**Endpoint:** `https://ezamowienia.gov.pl/mo-board/api/v1/notice`
**Test date used:** 2026-02-23

---

## 1. Podsumowanie (Executive Summary)

API BZP jest publiczne, nie wymaga uwierzytelnienia i zwraca dane w formacie JSON. Dane dostępne od 2021-01-01 (start nowej platformy e-Zamówienia). Średnio ~1500-2500 ogłoszeń dziennie (wszystkie typy łącznie).

**Krytyczny gotcha:** parametr `PageNumber` jest **ignorowany** — API zawsze zwraca te same rekordy niezależnie od numeru strony. Max `PageSize=500`. Dla typów z >500 rekordów/dzień potrzebna strategia okienek czasowych + deduplikacja.

---

## 2. Endpointy

### 2.1 Lista ogłoszeń
```
GET /mo-board/api/v1/notice
```

**Parametry wymagane** (brak któregokolwiek → HTTP 400):

| Parametr | Typ | Opis | Przykład |
|----------|-----|------|----------|
| `PageSize` | int | 1-500 (max 500) | `500` |
| `NoticeType` | enum | Typ ogłoszenia | `ContractNotice` |
| `PublicationDateFrom` | date/datetime | Start zakresu | `2026-02-23` lub `2026-02-23T06:00:00Z` |
| `PublicationDateTo` | date/datetime | Koniec zakresu | `2026-02-23` |

**Parametry opcjonalne:**

| Parametr | Działa? | Opis |
|----------|---------|------|
| `PageNumber` | **NIE** | Ignorowany — zawsze zwraca te same rekordy |
| `CpvCode` | TAK | Filtr po prefiksie kodu CPV (np. `72` = IT services) |
| `SearchPhrase` | **NIE** | Ignorowany server-side |

### 2.2 Statystyki (do planowania pobrań)
```
GET /mo-board/api/v1/notice/stats
  ?PublicationDateFrom=2026-02-23
  &PublicationDateTo=2026-02-23
```

Zwraca liczbę ogłoszeń per typ. **Nie wymaga** `NoticeType` ani `PageSize`.
Kluczowy do sprawdzenia czy trzeba dzielić pobranie na okienka czasowe.

### 2.3 Szczegóły ogłoszenia
```
GET /mo-board/api/v1/Board/GetNoticeDetails?noticeId={objectId}
```
Zwraca metadane: `id`, `tenderId`, `tenderState`, `publicationDate`, `noticeType`, `noticeNumber`.

### 2.4 PDF ogłoszenia
```
GET /mo-board/api/v1/Board/GetNoticePdfById?noticeId={objectId}
```

---

## 3. Struktura Response

Response to **flat JSON array** (brak envelope, brak total count). Przykładowy rekord `ContractNotice`:

```json
{
  "clientType": "1.5",
  "orderType": "Services",
  "tenderType": "1.1.1",
  "noticeType": "ContractNotice",
  "noticeNumber": "2026/BZP 00122714/01",
  "bzpNumber": "2026/BZP 00122714",
  "isTenderAmountBelowEU": true,
  "publicationDate": "2026-02-23T05:15:29.9682466Z",
  "orderObject": "Świadczenie usług hotelarskich...",
  "cpvCode": "55110000-4 (Hotelarskie usługi noclegowe)",
  "submittingOffersDate": "2026-03-02T14:00:00Z",
  "procedureResult": null,
  "organizationName": "KRAKOWSKA FUNDACJA FILMOWA",
  "organizationCity": "Kraków",
  "organizationProvince": "PL12",
  "organizationCountry": "PL",
  "organizationNationalId": "6762250313",
  "organizationId": "15904",
  "tenderId": "ocds-148610-01c5f998-f6da-48a3-92d1-08fab3cd5508",
  "contractors": null,
  "objectId": "08de729a-9002-b57b-056e-e50001abbbc3",
  "htmlBody": "<html>...</html>"
}
```

### Mapowanie pól API → docelowy schemat SQL

| Pole API | Typ | Opis | Null? | Docelowa kolumna |
|----------|-----|------|-------|------------------|
| `objectId` | GUID | Wewnętrzny ID rekordu | Nie | PK kandydat (unikalny per notice) |
| `noticeNumber` | string | `"2026/BZP 00122714/01"` — numer ogłoszenia + wersja | Nie | `notice_number` |
| `bzpNumber` | string | `"2026/BZP 00122714"` — numer postępowania (bez wersji) | Nie | `bzp_number` (klucz linkujący notice↔result) |
| `tenderId` | string | OCDS identifier — wspólny dla wszystkich ogłoszeń tego samego przetargu | Nie | `tender_id` (klucz linkujący notice↔result) |
| `noticeType` | enum | Typ ogłoszenia | Nie | `notice_type` |
| `orderObject` | string | Tytuł/opis zamówienia | Nie | `title` |
| `cpvCode` | string | Kod CPV + opis, rozdzielone przecinkami | Tak* | `cpv_code` (po parsingu) |
| `orderType` | string | `"Services"`, `"Delivery"`, `"Construction"` | Nie | `order_type` |
| `publicationDate` | datetime | Data publikacji (UTC) | Nie | `publication_date` |
| `submittingOffersDate` | datetime | Deadline składania ofert | Tak** | `deadline_date` |
| `procedureResult` | string | Wynik: `"zawarcieUmowy"`, `"uniewaznienie"` (semicolon-separated per part) | Tak** | `procedure_result` |
| `isTenderAmountBelowEU` | bool | Czy poniżej progu unijnego | Nie | `is_below_eu_threshold` |
| `organizationName` | string | Nazwa zamawiającego | Nie | Buyers.`institution_name` |
| `organizationCity` | string | Miasto | Tak | Buyers.`city` |
| `organizationProvince` | string | Kod NUTS2 (np. `"PL12"`) | Tak | Buyers.`province_nuts2` |
| `organizationNationalId` | string | NIP zamawiającego | Tak | Buyers.`nip` |
| `organizationId` | string | ID wewnętrzne w e-Zamówienia | Nie | Buyers.`organization_id` |
| `contractors` | array/null | Lista wykonawców (name, city, province, country, NIP) | Tak** | Contractors table |
| `clientType` | string | Kod typu zamawiającego | Nie | `client_type` |
| `tenderType` | string | Kod trybu zamówienia | Nie | `tender_type` |
| `htmlBody` | string | Pełny HTML ogłoszenia (20-200KB) | Nie | **POMIJAMY** w standardowym pobraniu |

*`cpvCode` = `null` na niektórych `ContractPerformingNotice`
**Zależy od typu ogłoszenia — szczegóły w sekcji 4

---

## 4. Typy ogłoszeń (NoticeType)

### Kluczowe dla pipeline'u (krajowe):

| NoticeType | Nazwa PL | Wolumen/dzień | Zawiera contractors? | procedureResult? |
|------------|----------|---------------|---------------------|-----------------|
| `ContractNotice` | Ogłoszenie o zamówieniu | ~300-400 | NIE (null) | NIE (null) |
| `TenderResultNotice` | Ogłoszenie o wyniku | ~300-400 | **TAK** (z NIP) | **TAK** (`zawarcieUmowy`/`uniewaznienie`) |
| `ContractPerformingNotice` | Ogłoszenie o wykonaniu umowy | ~800-2500 | Częściowo (często null) | NIE (null) |

**Powiązanie między typami:** `tenderId` (OCDS) jest wspólny dla `ContractNotice` i `TenderResultNotice` dotyczących tego samego przetargu. `bzpNumber` bez sufiksu `/01` też jest klucz linkujący.

**Wieloczęściowe przetargi:** `procedureResult` może być semicolon-separated: `"zawarcieUmowy;zawarcieUmowy;uniewaznienie"` — po jednym wyniku per część zamówienia. `contractors` array ma odpowiednio wiele wpisów (pozycyjnie 1:1 z częściami).

### Mniej istotne typy (do rozważenia w przyszłości):

| NoticeType | Wolumen/dzień |
|------------|---------------|
| `NoticeUpdateNotice` (zmiany ogłoszeń) | ~200-300 |
| `AgreementUpdateNotice` (zmiany umów) | ~20-30 |
| `SmallContractNotice` (bagatelne) | ~5-15 |

### Typy EU (osobne ogłoszenia):
`ContractNoticeEU`, `ContractAwardNoticeEU`, itp. — oddzielne NoticeType, ale ten sam endpoint.

---

## 5. Paginacja — KRYTYCZNY PROBLEM

### Problem
Parametr `PageNumber` jest **ignorowany** przez API. Każdy request zwraca te same rekordy niezależnie od wartości `PageNumber`. Potwierdzone testami:
- Page 1, 2, 3 z `PageSize=5` → identyczne `objectId` we wszystkich
- Page 83 (beyond last) → te same rekordy co page 1

### Limit
`PageSize` max = **500**. Request z `PageSize=501` → HTTP 400.

### Strategia obejścia

**Dla typów ≤ 500 rekordów/dzień** (`ContractNotice`, `TenderResultNotice`):
→ Jeden request z `PageSize=500` — wystarczy.

**Dla typów > 500 rekordów/dzień** (`ContractPerformingNotice`):
→ Użyj datetime w parametrach date: `PublicationDateFrom=2026-02-23T00:00:00Z`, `PublicationDateTo=2026-02-23T08:00:00Z`
→ Dziel dzień na okienka czasowe (np. 4h lub 6h)
→ **Deduplikacja po `objectId`** — okienka mogą zwracać nakładające się rekordy

**Algorytm paginacji:**
```python
1. GET /stats → total = 818
2. if total <= 500:
       single request, done
3. else:
       split day into N windows (N = ceil(total / 400))
       for each window:
           fetch with PageSize=500
           dedup by objectId
       verify: len(unique) >= total (from stats)
       if still missing: split into narrower windows
```

### Weryfikacja
Endpoint `/stats` daje dokładną liczbę rekordów. Po pobraniu porównaj `len(unique_objectIds)` z wartością ze stats. Jeśli się nie zgadza — podziel na mniejsze okienka.

---

## 6. Dane historyczne

| Rok | Ogłoszeń łącznie | ContractNotice | TenderResultNotice | ContractPerformingNotice |
|-----|-------------------|-----------------|---------------------|--------------------------|
| 2020 | 0 | — | — | — |
| 2021 | 200,872 | 83,902 | 73,074 | 33,362 |
| 2022 | 460,240 | 131,205 | 134,357 | 123,530 |
| 2023 | 520,234 | 122,863 | 125,593 | 190,964 |
| 2024 | 573,373 | 129,974 | 136,217 | 224,791 |
| 2025 | 619,354 | 142,424 | 148,561 | 241,687 |
| 2026 (do 23.02) | 122,404 | — | — | — |

**Dane zaczynają się od 2021-01-01** (start platformy e-Zamówienia). Starsze dane → legacy system `bzp.uzp.gov.pl` (SOAP, inna struktura).

**Backfill:** Przy ~500k rekordów/rok i max 500/request, pełny backfill 2021-2025 to ~6000+ requestów. Z rate limiting 1 req/s = ~2h.

---

## 7. Rate Limiting

**Brak wykrytego rate limitingu** dla anonimowych odczytów. 10 szybkich requestów → wszystkie HTTP 200. Regulamin mówi o blokowaniu „nadmiernego obciążenia", ale to skierowane do zintegrowanych klientów (OAuth).

**Rekomendacja:** 1-2 sekundy delay między requestami (polityka dobrego sąsiedztwa), szczególnie przy backfill.

---

## 8. Czego API NIE zwraca (w list endpoint)

| Dane | Status | Gdzie szukać |
|------|--------|-------------|
| **Szacowany budżet** | BRAK w list response | Wewnątrz `htmlBody` (trzeba parsować HTML) |
| **Ostateczna cena** | BRAK w list response | Wewnątrz `htmlBody` TenderResultNotice |
| **Liczba złożonych ofert** | BRAK w list response | Wewnątrz `htmlBody` TenderResultNotice |
| **Opis zamówienia** (rozszerzony) | BRAK (tylko `orderObject` = tytuł) | Wewnątrz `htmlBody` |
| **Typ instytucji** zamawiającego | Tylko `clientType` (kod numeryczny) | Mapowanie kodów (do ustalenia) |
| **Województwo** (nazwa) | Tylko kod NUTS2 (`PL12`) | Mapowanie NUTS2 → województwo |

**Ważna decyzja projektowa:** Czy parsować `htmlBody` (20-200KB per rekord) po dodatkowe dane (budżet, cenę, liczbę ofert)? To zwiększa złożoność i objętość danych, ale daje wartościowe pola analityczne.

**Rekomendacja na start:** Zacząć BEZ htmlBody. Pola z list response wystarczą do klasyfikacji technologicznej, analizy trendów CPV, mapowania zamawiających i wykonawców. Parsowanie HTML = future enhancement.

---

## 9. Obserwacje dot. jakości danych

### NIP
- Format niespójny: `"6762250313"` (same cyfry) vs `"895-22-39-965"` (z kreskami) vs `"015318360"` (9 cyfr, bez wiodącego zera)
- **Wymaga normalizacji:** strip non-digits + walidacja (10 cyfr)
- Uwaga: `organizationNationalId` to NIP zamawiającego, `contractorNationalId` to NIP wykonawcy

### CPV Code
- Format: `"55110000-4 (Hotelarskie usługi noclegowe)"` — kod + opis w jednym stringu
- Multiple CPV codes: rozdzielone przecinkiem: `"71322000-1 (Usługi...),71242000-6 (Przygotowanie...)"`
- **Wymaga parsingu:** wydziel kod (pierwsze 10 znaków) i opis

### Contractors
- Na `TenderResultNotice`: zazwyczaj wypełnione (name, city, NIP)
- Na `ContractPerformingNotice`: **często null** (nawet w array: `[{"contractorName": null, ...}]`)
- Przy `procedureResult = "uniewaznienie"`: contractors mają same null-e

### Province
- Kody NUTS2: `PL06`, `PL12`, `PL14`, `PL22`, `PL24`, `PL26`, `PL28`, `PL30`, `PL32`
- Mapowanie na województwa jest stałe i dobrze udokumentowane

---

## 10. Rekomendowany schemat (adaptacja promptu)

Na podstawie tego co API faktycznie zwraca, proponuję **uproszczony schemat** vs prompt:

### Zmiany vs pierwotny prompt:

1. **`Raw_API_Responses`** → zachować, ale bez `htmlBody` (oszczędność miejsca)
2. **`Buyers`** → NIP wymaga normalizacji; dodać `organization_id` (wewnętrzne ID z API); `province` to NUTS2 kod, nie nazwa; brak `Institution_Type` — jest `clientType` (kod numeryczny)
3. **`Contractors`** → ditto na NIP; danych kontraktora często brak na list endpoint
4. **`Tenders`** → `Budget_Estimated` i `Description` NIE SĄ dostępne w list response (wymagałyby parsowania htmlBody); dodać pola: `order_type`, `tender_type`, `client_type`, `is_below_eu_threshold`, `bzp_number`; `Tender_ID` → użyć `tenderId` (OCDS) jako natural key, nie `objectId`
5. **`Tender_Results`** → `Final_Price` i `Offers_Count` NIE SĄ dostępne w list response; `procedureResult` jest semicolon-separated per część zamówienia; linkowanie przez `tenderId`
6. **`AI_Classifications`** → bez zmian, schemat z promptu jest OK

### Uproszczona architektura pipeline'u:

```
Stream 1 (ContractNotice):  → Tenders + Buyers
Stream 2 (TenderResultNotice): → UPDATE Tenders + Tender_Results + Contractors
Stream 3 (ContractPerformingNotice): → UPDATE Tenders (status zamknięcia)

Linkowanie: tenderId jest wspólny między streamami.
```

---

## 11. Recon output files

Wszystkie surowe response z testów: `gov_spending_radar/recon_output/`

| Plik | Opis |
|------|------|
| `stats_single_day.json` | Statystyki per typ dla 2026-02-23 |
| `yearly_stats.json` | Wolumeny roczne 2020-2026 |
| `sample_stripped_ContractNotice.json` | 5 rekordów (bez htmlBody) |
| `sample_stripped_TenderResultNotice.json` | 5 rekordów (bez htmlBody) |
| `sample_stripped_ContractPerformingNotice.json` | 5 rekordów (bez htmlBody) |
| `sample_full_ContractNotice.json` | 1 pełny rekord (z htmlBody) |
| `field_analysis.json` | Analiza pól: typy, null-rate, sample values |
| `pagination_test.json` | Dowód że PageNumber nie działa |
| `tender_linkage_test.json` | Test linkowania po tenderId/bzpNumber |
| `sample_cpv_72.json` | IT services (CPV 72*) |
| `sample_cpv_48.json` | Software packages (CPV 48*) |
