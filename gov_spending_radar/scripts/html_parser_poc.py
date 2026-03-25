"""
Krok 3: Proof-of-concept HTML parser for BZP notices.
======================================================
Parses htmlBody to extract financial data and descriptions
that are not available in the API list response metadata.

Target fields:
  - budget_estimated  (szacowana wartość zamówienia)
  - final_price       (cena zwycięzcy / wartość umowy)
  - offers_count      (liczba złożonych ofert)
  - description       (krótki opis przedmiotu zamówienia)
  - lowest_price      (najniższa oferta)
  - highest_price     (najwyższa oferta)
  - contract_value    (wartość umowy/umowy ramowej)
  - currency          (PLN / EUR / etc.)

Usage:
    .venv\\Scripts\\python.exe -X utf8 -m gov_spending_radar.scripts.html_parser_poc
"""

import json
import re
import sys
import time
from pathlib import Path

from bs4 import BeautifulSoup

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

RECON_DIR = Path(__file__).resolve().parent.parent / "recon_html"


def _parse_amount(raw: str | None) -> tuple[float | None, str | None]:
    """
    Parse amount string like '690120,00 PLN' or '31321,72 EUR'.
    Returns (amount_float, currency) or (None, None).
    """
    if not raw:
        return None, None
    raw = raw.strip()
    # Match: digits with optional spaces/dots as thousand separators, comma as decimal, then currency
    match = re.search(r'([\d\s.]+,\d{2})\s*([A-Z]{3})?', raw)
    if not match:
        # Try integer amount
        match = re.search(r'([\d\s.]+)\s*([A-Z]{3})?', raw)
        if not match:
            return None, None
    amount_str = match.group(1).replace(' ', '').replace('.', '').replace(',', '.')
    currency = match.group(2) if match.group(2) else None
    try:
        return float(amount_str), currency
    except ValueError:
        return None, None


def _get_h3_value(soup: BeautifulSoup, field_prefix: str) -> str | None:
    """Find h3 containing field_prefix and return its span.normal text."""
    for h3 in soup.find_all('h3'):
        text = h3.get_text()
        if field_prefix in text:
            span = h3.find('span', class_='normal')
            if span:
                return span.get_text().strip()
            # Sometimes value is directly in h3 text after the label
            parts = text.split(field_prefix, 1)
            if len(parts) > 1 and parts[1].strip():
                return parts[1].strip()
    return None


def _get_description(soup: BeautifulSoup) -> str | None:
    """Extract short description (4.2.2 for ContractNotice, 4.5.1 for TenderResultNotice)."""
    for h3 in soup.find_all('h3'):
        text = h3.get_text()
        if '4.2.2.)' in text or '4.5.1.)' in text:
            # Description is in the next <p> sibling
            next_p = h3.find_next_sibling('p')
            if next_p:
                desc = next_p.get_text().strip()
                return desc if desc else None
    return None


def parse_contract_notice(html: str) -> dict:
    """Parse ContractNotice HTML for budget and description."""
    soup = BeautifulSoup(html, 'html.parser')
    result = {
        'notice_type': 'ContractNotice',
        'budget_estimated': None,
        'budget_currency': None,
        'description': None,
        'description_length': 0,
    }

    # Budget: "4.1.5.) Wartość zamówienia" (rarely present)
    budget_raw = _get_h3_value(soup, '4.1.5.)')
    if budget_raw:
        amount, currency = _parse_amount(budget_raw)
        result['budget_estimated'] = amount
        result['budget_currency'] = currency

    # Also check "4.2.9) Rodzaj i maksymalna wartość opcji" for option value
    # (less useful, skip for now)

    # Description
    desc = _get_description(soup)
    if desc:
        result['description'] = desc[:500]  # truncate for display
        result['description_length'] = len(desc)

    return result


def parse_result_notice(html: str) -> dict:
    """Parse TenderResultNotice HTML for prices, offers, and description."""
    soup = BeautifulSoup(html, 'html.parser')
    result = {
        'notice_type': 'TenderResultNotice',
        'budget_estimated': None,
        'budget_currency': None,
        'final_price': None,
        'final_currency': None,
        'lowest_price': None,
        'highest_price': None,
        'offers_count': None,
        'offers_count_sme': None,
        'contract_value': None,
        'contract_currency': None,
        'description': None,
        'description_length': 0,
    }

    # Budget estimated: "4.3.) Wartość zamówienia" or "4.3.1) Wartość zamówienia"
    for prefix in ['4.3.) Wartość zamówienia', '4.3.1) Wartość zamówienia',
                    '4.1.5.) Wartość zamówienia']:
        budget_raw = _get_h3_value(soup, prefix.split(')')[0] + ')')
        if budget_raw and any(c.isdigit() for c in budget_raw):
            amount, currency = _parse_amount(budget_raw)
            if amount:
                result['budget_estimated'] = amount
                result['budget_currency'] = currency
                break

    # Offers count: "6.1.) Liczba otrzymanych ofert"
    offers_raw = _get_h3_value(soup, '6.1.)')
    if offers_raw:
        match = re.search(r'\d+', offers_raw)
        if match:
            result['offers_count'] = int(match.group())

    # Offers from SME: "6.1.3.) Liczba otrzymanych od MŚP"
    sme_raw = _get_h3_value(soup, '6.1.3.)')
    if sme_raw:
        match = re.search(r'\d+', sme_raw)
        if match:
            result['offers_count_sme'] = int(match.group())

    # Lowest price: "6.2.) Cena lub koszt oferty z najniższą ceną"
    low_raw = _get_h3_value(soup, '6.2.)')
    if low_raw:
        amount, _ = _parse_amount(low_raw)
        result['lowest_price'] = amount

    # Highest price: "6.3.) Cena lub koszt oferty z najwyższą ceną"
    high_raw = _get_h3_value(soup, '6.3.)')
    if high_raw:
        amount, _ = _parse_amount(high_raw)
        result['highest_price'] = amount

    # Winner price: "6.4.) Cena lub koszt oferty wykonawcy"
    winner_raw = _get_h3_value(soup, '6.4.)')
    if winner_raw:
        amount, currency = _parse_amount(winner_raw)
        result['final_price'] = amount
        result['final_currency'] = currency

    # Contract value: "8.2.) Wartość umowy/umowy ramowej"
    contract_raw = _get_h3_value(soup, '8.2.)')
    if contract_raw:
        amount, currency = _parse_amount(contract_raw)
        result['contract_value'] = amount
        result['contract_currency'] = currency

    # Description
    desc = _get_description(soup)
    if desc:
        result['description'] = desc[:500]
        result['description_length'] = len(desc)

    return result


def main():
    all_results = []
    parse_times = []

    for f in sorted(RECON_DIR.glob('*.html')):
        html = f.read_text(encoding='utf-8')
        html_size_kb = len(html.encode('utf-8')) / 1024

        t0 = time.perf_counter()

        # Detect notice type from filename or HTML content
        if 'result' in f.name:
            parsed = parse_result_notice(html)
        else:
            parsed = parse_contract_notice(html)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        parse_times.append(elapsed_ms)

        parsed['filename'] = f.name
        parsed['html_size_kb'] = round(html_size_kb, 1)
        parsed['parse_time_ms'] = round(elapsed_ms, 1)
        all_results.append(parsed)

        # Print results
        print(f"\n{'='*60}")
        print(f"FILE: {f.name} ({html_size_kb:.1f} KB, parsed in {elapsed_ms:.1f}ms)")
        print(f"{'='*60}")

        for key, val in parsed.items():
            if key in ('filename', 'html_size_kb', 'parse_time_ms', 'notice_type'):
                continue
            if key == 'description' and val:
                print(f"  {key}: {val[:120]}...")
                continue
            if val is not None:
                print(f"  {key}: {val}")

    # Summary
    print(f"\n{'='*60}")
    print("PARSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total files parsed: {len(all_results)}")
    print(f"Parse times: min={min(parse_times):.1f}ms, max={max(parse_times):.1f}ms, "
          f"avg={sum(parse_times)/len(parse_times):.1f}ms")
    print(f"Estimated for 800 notices/day: {800 * sum(parse_times)/len(parse_times) / 1000:.1f}s total")

    # Field coverage
    result_notices = [r for r in all_results if r['notice_type'] == 'TenderResultNotice']
    contract_notices = [r for r in all_results if r['notice_type'] == 'ContractNotice']

    print(f"\n--- TenderResultNotice ({len(result_notices)} samples) ---")
    for field in ['budget_estimated', 'final_price', 'offers_count', 'contract_value',
                   'lowest_price', 'highest_price', 'description']:
        count = sum(1 for r in result_notices if r.get(field) is not None)
        print(f"  {field}: {count}/{len(result_notices)} ({100*count/len(result_notices):.0f}%)")

    print(f"\n--- ContractNotice ({len(contract_notices)} samples) ---")
    for field in ['budget_estimated', 'description']:
        count = sum(1 for r in contract_notices if r.get(field) is not None)
        print(f"  {field}: {count}/{len(contract_notices)} ({100*count/len(contract_notices):.0f}%)")

    # Save results
    output_path = RECON_DIR / "_parser_results.json"
    # Strip long descriptions for JSON
    for r in all_results:
        if r.get('description') and len(r['description']) > 200:
            r['description'] = r['description'][:200] + '...'
    output_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    main()
