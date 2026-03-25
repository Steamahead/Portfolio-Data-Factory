# HTML Parser — Raport Wykonalności

**Data:** 2026-03-16
**Próbka:** 15 ogłoszeń (8 ContractNotice, 7 TenderResultNotice), w tym stare (luty) i nowe (marzec)

---

## 1. Dostępność danych

- **htmlBody jest w list response** — to samo API co pipeline, ten sam endpoint. Nie trzeba osobnego requestu per ogłoszenie.
- Pipeline celowo odrzuca htmlBody (`n.pop("htmlBody", None)` w `bzp_client.py:185`).
- Żeby wyciągnąć dane, wystarczy **nie odrzucać** htmlBody i przeparsować go w locie — potem wyrzucić surowy HTML z pamięci.
- **Dodatkowe requesty: ZERO**. Dane już przychodzą z API, po prostu je ignorujemy.
- **Rate limiting**: bez zmian — ta sama liczba requestów co dziś.
- **Rozmiar payloadu**: średnio 22 KB/ogłoszenie (min 10 KB, max 45 KB). Przy 800 ogłoszeń/dzień = ~17.5 MB sumarycznego transferu (vs ~1 MB bez htmlBody). Transfer jest jednorazowy i nie wpływa na bazę.

## 2. Struktura HTML

HTML jest generowany z szablonu BZP. Struktura jest **wysoce stabilna**:

- Sekcje oznaczone `<h2 class="bg-light p-3 mt-4">SEKCJA X — NAZWA</h2>`
- Pola oznaczone `<h3 class="mb-0">X.Y.Z.) Etykieta: <span class="normal">WARTOŚĆ</span></h3>`
- Opisy w `<p class="mb-0">treść</p>` pod odpowiednim `<h3>`
- Wersja szablonu: `<!-- Version 1.0.0 -->` — identyczna we wszystkich 15 sample'ach (luty i marzec 2026)

**Stare ogłoszenia (luty) mają identyczną strukturę jak nowe (marzec)** — szablon się nie zmienił.

## 3. Zmapowane pola → tabela

### TenderResultNotice (ogłoszenie o wyniku)

| Pole docelowe | Lokalizacja w DOM | Stabilność | Pokrycie | Uwagi |
|---|---|---|---|---|
| `budget_estimated` | `4.3.) Wartość zamówienia` lub `4.3.1) Wartość zamówienia` | Wysoka | 5/7 (71%) | Nie zawsze wypełniane. Kwota netto (bez VAT) |
| `final_price` | `6.4.) Cena lub koszt oferty wykonawcy, któremu udzielono zamówienia` | Wysoka | 6/7 (86%) | Cena brutto zwycięzcy. Brak = unieważnienie |
| `offers_count` | `6.1.) Liczba otrzymanych ofert lub wniosków` | Wysoka | **7/7 (100%)** | Zawsze obecne |
| `offers_count_sme` | `6.1.3.) Liczba otrzymanych od MŚP` | Wysoka | 7/7 (100%) | Bonus — ile od MŚP |
| `lowest_price` | `6.2.) Cena lub koszt oferty z najniższą ceną` | Wysoka | **7/7 (100%)** | |
| `highest_price` | `6.3.) Cena lub koszt oferty z najwyższą ceną` | Wysoka | **7/7 (100%)** | |
| `contract_value` | `8.2.) Wartość umowy/umowy ramowej` | Wysoka | 6/7 (86%) | Może różnić się od final_price (aneksy, VAT) |
| `description` | `4.5.1.) Krótki opis przedmiotu zamówienia` → `<p>` | Wysoka | **7/7 (100%)** | 218–6909 znaków |
| `currency` | Suffix w kwocie (np. "PLN", "EUR") | Wysoka | 7/7 | PLN dominuje, EUR sporadycznie |

### ContractNotice (ogłoszenie o zamówieniu)

| Pole docelowe | Lokalizacja w DOM | Stabilność | Pokrycie | Uwagi |
|---|---|---|---|---|
| `budget_estimated` | `4.1.5.) Wartość zamówienia` | Wysoka | **1/8 (12%)** | Rzadko wypełniane! |
| `description` | `4.2.2.) Krótki opis przedmiotu zamówienia` → `<p>` | Wysoka | **8/8 (100%)** | 208–3907 znaków |

## 4. Edge cases i ryzyka

### Zamówienia wieloczęściowe (multi-part)
- **Żaden z 15 sample'ów nie był multi-part**. W produkcji multi-part ogłoszenia mają powtarzające się sekcje VI/VII per część. Parser musiałby obsłużyć listę wyników per część.
- **Rekomendacja**: Na start — brać TYLKO dane z pierwszego wystąpienia każdego pola (lub sumę). Multi-part to edge case wymagający osobnej logiki.

### Brakujące pola
- `budget_estimated` w ContractNotice: 88% brak! Pole opcjonalne w formularzu BZP. **Nie da się na nim polegać.**
- `budget_estimated` w TenderResultNotice: 71% pokrycie — dużo lepiej, ale nie 100%.
- `final_price` brak = unieważnione postępowanie (brak zwycięzcy) — to poprawne zachowanie.

### Formaty kwot
- **Format stabilny**: `123456,78 PLN` (przecinek dziesiętny, spacja przed walutą)
- EUR sporadycznie (zamówienia zagraniczne)
- Nie spotkano formatów z kropką tysięczną ani formatów netto/brutto explicite — zawsze jedna kwota per pole

### Zmiany struktury HTML w czasie
- **Brak zmian** między luty a marzec 2026 (Version 1.0.0 w obu)
- Ryzyko: BZP może zmienić szablon w przyszłości. Rekomendacja: walidacja parsera (jeśli żadne pole nie zwróci wartości → log warning)

## 5. Plan implementacji

### Architektura: parsowanie w locie (inline)

**NIE przechowujemy surowego HTML w bazie.** Parser działa w pipeline:

```
API response (z htmlBody)
  → parse_html_fields(htmlBody) → {budget, price, offers_count, description}
  → pop("htmlBody")  # wyrzuć z pamięci
  → MERGE do SQL (dodatkowe kolumny)
```

### Wpływ na rozmiar bazy (kluczowe!)

| Co | Rozmiar per record | 800 records/day | 365 dni |
|---|---|---|---|
| Surowy HTML (NIE zapisujemy) | ~22 KB | 17.5 MB | 6.4 GB |
| Wyekstrahowane pola numeryczne | ~50 B | 40 KB | 14.6 MB |
| Wyekstrahowane pola + description (500 znaków max) | ~1.1 KB | 880 KB | 322 MB |
| Wyekstrahowane pola + description (200 znaków max) | ~0.5 KB | 400 KB | 146 MB |

**Rekomendacja**: Zapisywać description obcięty do 500 znaków (wystarczy do klasyfikacji i przeszukiwania). Roczny koszt: ~320 MB — bezpieczne przy limicie 20 GB.

### Nowe kolumny w `gov_notices`

```sql
-- dodać do istniejącej tabeli (ALTER TABLE, safe migration)
budget_estimated    DECIMAL(18,2)   NULL,   -- już istnieje, tylko zacząć wypełniać
final_price         DECIMAL(18,2)   NULL,   -- już istnieje, tylko zacząć wypełniać
offers_count        SMALLINT        NULL,   -- NOWE
lowest_price        DECIMAL(18,2)   NULL,   -- NOWE
highest_price       DECIMAL(18,2)   NULL,   -- NOWE
contract_value      DECIMAL(18,2)   NULL,   -- NOWE
currency            NVARCHAR(5)     NULL,   -- NOWE (PLN/EUR)
description         NVARCHAR(500)   NULL,   -- NOWE (skrócony opis)
```

### Wydajność

- Parsowanie 1 HTML: **3-10 ms** (BeautifulSoup, avg 5.4 ms)
- 800 ogłoszeń/dzień: **~4.3 sekundy** parsowania
- Transfer API: +17.5 MB/dzień (jednorazowy, nie do bazy)
- **Zmieści się w 10-minutowym timeout Azure Function bez problemu**

### Estymacja pracy

1. Modyfikacja `bzp_client.py` — nie pop-ować htmlBody, parsować inline → **30 min**
2. Dodanie kolumn SQL (migration) + update MERGE → **30 min**
3. Obsługa multi-part notices (opcjonalnie) → **1-2h**
4. Testy na --sample + backfill istniejących ogłoszeń → **30 min**

### Backfill

**TAK, da się przeparsować historyczne ogłoszenia** — wystarczy pobrać je ponownie z API z htmlBody. Obecny backfill (--backfill N) pobiera dane z API, więc wystarczy odpalić go z nowym kodem.

## 6. Rekomendacja

### **GO — z zastrzeżeniami**

**Pełne GO dla TenderResultNotice** — dane finansowe dostępne w 71-100% ogłoszeń, stabilna struktura, zerowy koszt dodatkowych requestów, minimalny wpływ na bazę (~320 MB/rok).

**Ograniczone GO dla ContractNotice** — jedyny zysk to `description` (100% pokrycie). Budget dostępny tylko w ~12% przypadków. Warto zbierać description dla lepszej klasyfikacji, ale nie liczyć na dane finansowe.

**Zastrzeżenia**:
1. Multi-part notices wymagają osobnej logiki (ale nie blokują wdrożenia — na start bierzemy pola z pierwszego wystąpienia)
2. BZP może zmienić szablon HTML — parser powinien mieć fallback (NULL jeśli nie znajdzie pola, nie crash)
3. Description 500 znaków = ~320 MB/rok — przy limicie 20 GB to bezpieczne, ale warto monitorować
