"""
Tests for Gov Spending Radar 3-layer classifier.
=================================================
Validates false positive exclusion, true positive detection,
and multi-label output.

Run: python -m pytest gov_spending_radar/tests/test_classifier.py -v
"""

from gov_spending_radar.main import classify_notice_multilabel


def _sectors(results: list[dict]) -> set[str]:
    """Extract unique sector names from classification results."""
    return {r["sector"] for r in results}


# ── AI false positives (must NOT classify as AI) ─────────────────

def test_sztuczna_nawierzchnia_not_ai():
    results = classify_notice_multilabel(
        title="Budowa boiska ze sztuczną nawierzchnią",
        cpv_code="45212221",
        cpv_raw="45212221-1 Roboty budowlane związane z obiektami na terenach sportowych",
    )
    assert "AI" not in _sectors(results)


def test_sztuczna_trawa_not_ai():
    results = classify_notice_multilabel(
        title="Wymiana sztucznej trawy na boisku piłkarskim",
        cpv_code="45212221",
        cpv_raw="45212221-1",
    )
    assert "AI" not in _sectors(results)


# ── AI true positives (must classify as AI) ──────────────────────

def test_sztuczna_inteligencja_is_ai():
    results = classify_notice_multilabel(
        title="Wdrożenie systemu sztucznej inteligencji w urzędzie",
        cpv_code="72000000",
        cpv_raw="72000000-5 Usługi informatyczne",
    )
    assert "AI" in _sectors(results)


def test_machine_learning_is_ai():
    results = classify_notice_multilabel(
        title="Platforma machine learning do analizy danych",
        cpv_code="72000000",
        cpv_raw="72000000-5",
    )
    assert "AI" in _sectors(results)


# ── CYBERSECURITY false positives (must NOT classify) ────────────

def test_bezpieczenstwo_ruchu_drogowego_not_cyber():
    results = classify_notice_multilabel(
        title="Poprawa bezpieczeństwa ruchu drogowego na skrzyżowaniu",
        cpv_code="45233000",
        cpv_raw="45233000-9 Roboty w zakresie konstruowania",
    )
    assert "CYBERSECURITY" not in _sectors(results)


def test_ochrona_fizyczna_not_cyber():
    results = classify_notice_multilabel(
        title="Ochrona fizyczna budynku urzędu",
        cpv_code="79710000",
        cpv_raw="79710000-4 Usługi ochroniarskie",
    )
    assert "CYBERSECURITY" not in _sectors(results)


# ── CYBERSECURITY true positives ─────────────────────────────────

def test_cyberbezpieczenstwo_is_cyber():
    results = classify_notice_multilabel(
        title="Wdrożenie systemu cyberbezpieczeństwa",
        cpv_code="72800000",
        cpv_raw="72800000-8",
    )
    assert "CYBERSECURITY" in _sectors(results)


# ── Multi-label: notice matching AI + IT ─────────────────────────

def test_multilabel_ai_and_it():
    results = classify_notice_multilabel(
        title="Wdrożenie systemu informatycznego sztucznej inteligencji",
        cpv_code="72000000",
        cpv_raw="72000000-5",
    )
    sectors = _sectors(results)
    assert "AI" in sectors
    assert "IT" in sectors


# ── CPV-only classification ──────────────────────────────────────

def test_cpv_only_construction():
    results = classify_notice_multilabel(
        title="Remont dachu budynku szkoły",
        cpv_code="45000000",
        cpv_raw="45000000-7 Roboty budowlane",
    )
    assert "CONSTRUCTION" in _sectors(results)
    assert any(r["method"] == "CPV_ONLY" for r in results)


def test_cpv_only_it_general():
    results = classify_notice_multilabel(
        title="Dostawa sprzętu komputerowego",
        cpv_code="30200000",
        cpv_raw="30200000-1 Urządzenia komputerowe",
    )
    assert "IT_GENERAL" in _sectors(results)
    assert any(r["method"] == "CPV_ONLY" for r in results)


# ── CPV boost/penalty ────────────────────────────────────────────

def test_keyword_with_it_cpv_gets_boost():
    results = classify_notice_multilabel(
        title="Wdrożenie systemu informatycznego ERP",
        cpv_code="72000000",
        cpv_raw="72000000-5",
    )
    methods = {r["method"] for r in results if r["sector"] == "IT"}
    assert "KEYWORD" in methods
    assert "CPV_BOOST" in methods


def test_keyword_with_nonit_cpv_gets_penalty():
    results = classify_notice_multilabel(
        title="Dostawa oprogramowania do szpitala",
        cpv_code="33000000",
        cpv_raw="33000000-0 Urządzenia medyczne",
    )
    methods = {r["method"] for r in results if r["sector"] == "IT"}
    assert "KEYWORD" in methods
    assert "CPV_PENALTY" in methods
