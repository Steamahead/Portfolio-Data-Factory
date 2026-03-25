"""
HTML parser for BZP notice htmlBody — extracts financial data and descriptions.
================================================================================
Parses inline (in memory), extracts structured fields, never stores raw HTML.
Based on POC validated against 15 BZP notices (Version 1.0.0 template).

Fields extracted:
  TenderResultNotice: budget_estimated, final_price, offers_count, lowest_price,
                      highest_price, contract_value, currency, description
  ContractNotice:     budget_estimated, currency, description

Parser is defensive — returns NULLs on any parse failure (never crashes pipeline).
"""

import re

from bs4 import BeautifulSoup

# Truncate descriptions to keep DB footprint low (~320 MB/year at 500 chars)
# Use 490 to leave headroom for Unicode chars that SQL Server counts as 2 in NVARCHAR
MAX_DESCRIPTION_LENGTH = 490


def _parse_amount(raw: str | None) -> tuple[float | None, str | None]:
    """
    Parse BZP amount string like '690120,00 PLN' or '31 321,72 EUR'.
    Returns (amount_float, currency) or (None, None).
    """
    if not raw:
        return None, None
    raw = raw.strip()
    # Match: digits with optional spaces/dots as thousand separators, comma decimal, currency
    match = re.search(r'([\d\s.]+,\d{2})\s*([A-Z]{3})?', raw)
    if not match:
        # Try integer amount (no decimal)
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
            parts = text.split(field_prefix, 1)
            if len(parts) > 1 and parts[1].strip():
                return parts[1].strip()
    return None


def _get_description(soup: BeautifulSoup) -> str | None:
    """Extract short description (4.2.2 for ContractNotice, 4.5.1 for TenderResultNotice)."""
    for h3 in soup.find_all('h3'):
        text = h3.get_text()
        if '4.2.2.)' in text or '4.5.1.)' in text:
            next_p = h3.find_next_sibling('p')
            if next_p:
                desc = next_p.get_text().strip()
                return desc[:MAX_DESCRIPTION_LENGTH] if desc else None
    return None


def parse_notice_html(html: str, notice_type: str) -> dict:
    """
    Parse BZP htmlBody and return extracted fields.

    Args:
        html: Raw HTML string from API response
        notice_type: 'ContractNotice' or 'TenderResultNotice'

    Returns:
        Dict with extracted fields (keys match SQL columns).
        Missing fields are None (never raises).
    """
    fields = {
        'budget_estimated': None,
        'final_price': None,
        'offers_count': None,
        'lowest_price': None,
        'highest_price': None,
        'contract_value': None,
        'currency': None,
        'description': None,
    }

    if not html:
        return fields

    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        return fields

    try:
        # Description (both notice types)
        fields['description'] = _get_description(soup)

        if notice_type == 'TenderResultNotice':
            _parse_result_notice(soup, fields)
        else:
            _parse_contract_notice(soup, fields)
    except Exception:
        # Defensive — never crash the pipeline
        pass

    return fields


def _parse_result_notice(soup: BeautifulSoup, fields: dict) -> None:
    """Extract financial data from TenderResultNotice HTML."""
    # Budget estimated: try multiple section numbers
    for prefix in ['4.3.)', '4.3.1)', '4.1.5.)']:
        budget_raw = _get_h3_value(soup, prefix)
        if budget_raw and any(c.isdigit() for c in budget_raw):
            amount, currency = _parse_amount(budget_raw)
            if amount:
                fields['budget_estimated'] = amount
                if currency:
                    fields['currency'] = currency
                break

    # Offers count: "6.1.) Liczba otrzymanych ofert"
    offers_raw = _get_h3_value(soup, '6.1.)')
    if offers_raw:
        match = re.search(r'\d+', offers_raw)
        if match:
            fields['offers_count'] = int(match.group())

    # Lowest price: "6.2.)"
    low_raw = _get_h3_value(soup, '6.2.)')
    if low_raw:
        amount, currency = _parse_amount(low_raw)
        fields['lowest_price'] = amount
        if currency and not fields['currency']:
            fields['currency'] = currency

    # Highest price: "6.3.)"
    high_raw = _get_h3_value(soup, '6.3.)')
    if high_raw:
        amount, _ = _parse_amount(high_raw)
        fields['highest_price'] = amount

    # Winner price: "6.4.)"
    winner_raw = _get_h3_value(soup, '6.4.)')
    if winner_raw:
        amount, currency = _parse_amount(winner_raw)
        fields['final_price'] = amount
        if currency and not fields['currency']:
            fields['currency'] = currency

    # Contract value: "8.2.)"
    contract_raw = _get_h3_value(soup, '8.2.)')
    if contract_raw:
        amount, currency = _parse_amount(contract_raw)
        fields['contract_value'] = amount
        if currency and not fields['currency']:
            fields['currency'] = currency


def _parse_contract_notice(soup: BeautifulSoup, fields: dict) -> None:
    """Extract budget from ContractNotice HTML (rarely available ~12%)."""
    budget_raw = _get_h3_value(soup, '4.1.5.)')
    if budget_raw and any(c.isdigit() for c in budget_raw):
        amount, currency = _parse_amount(budget_raw)
        fields['budget_estimated'] = amount
        if currency:
            fields['currency'] = currency
