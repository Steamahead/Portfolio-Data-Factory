"""Master product fixture for the inflation_basket pipeline.

52 products across 15 micro-categories. Selected by the owner as a representative
sample of personal weekly grocery + chemia purchases. Decisions documented in
docs/INFLATION_BASKET_SPEC.md (sesja 2026-04-30, korekty 2026-05-01).

Two matching modes (see spec §6):
- "same_sku"     -- branded FMCG, identical EAN expected in both stores;
                    direct cross-store comparison is meaningful.
- "logical_only" -- fresh / loose / unbranded; physical product differs per store,
                    but same logical category (e.g. "polędwica wieprzowa luz /kg").
                    Cross-store comparison is on category-level price, not SKU.

EAN is filled later during the URL mapping step (Playwright on each store).
"""

from dataclasses import dataclass, field
from typing import Literal, Optional

CategoryUser = Literal[
    "nabial", "mieso", "wedlina", "owoce", "warzywa",
    "tluszcze", "przyprawy", "slodycze", "napoje",
    "chemia", "konserwy", "mrozonki", "maki", "jajka", "zboza",
]

MatchingType = Literal["same_sku", "logical_only"]
CapacityUnit = Literal["g", "ml", "l", "kg", "szt", "rolek", "pack"]


@dataclass(frozen=True)
class Product:
    name_canonical: str
    category_user: CategoryUser
    matching_type: MatchingType
    capacity_value: float
    capacity_unit: CapacityUnit
    brand: Optional[str] = None
    ean: Optional[str] = None  # filled during URL mapping
    is_imported: bool = False
    origin_country: Optional[str] = None  # ISO-3166-1 alpha-2
    alternative_names: tuple[str, ...] = field(default_factory=tuple)
    notes: Optional[str] = None


PRODUCTS: list[Product] = [
    # === NABIAŁ (7) ===
    Product("Mleko bez laktozy", "nabial", "same_sku", 1.0, "l", brand="Łaciate"),
    Product("Twaróg chudy", "nabial", "same_sku", 250, "g", brand="Piątnica"),
    Product("Masło Extra", "nabial", "same_sku", 200, "g", brand="Łaciate"),
    Product("Ser Cheddar plastry", "nabial", "same_sku", 150, "g", brand="Mlekovita"),
    Product("Jogurt naturalny", "nabial", "same_sku", 400, "g", brand="Bakoma"),
    Product("Kefir naturalny", "nabial", "same_sku", 400, "g", brand="Mlekovita"),
    Product("Ser Grana Padano kawałek", "nabial", "same_sku", 100, "g",
            brand="Galbani", is_imported=True, origin_country="IT"),

    # === MIĘSO / RYBY (5, logical_only — luz, fizycznie różne kawałki) ===
    Product("Polędwica wieprzowa", "mieso", "logical_only", 1.0, "kg"),
    Product("Pierś z indyka", "mieso", "logical_only", 1.0, "kg"),
    Product("Stek wołowy", "mieso", "logical_only", 1.0, "kg",
            alternative_names=("ribeye", "T-bone", "rib eye", "stek z polędwicy")),
    Product("Antrykot wołowy", "mieso", "logical_only", 1.0, "kg",
            alternative_names=("roast beef wołowy", "rostbef wołowy", "rostbef")),
    Product("Dorsz filet", "mieso", "logical_only", 1.0, "kg"),
    Product("Łosoś filet", "mieso", "logical_only", 1.0, "kg"),

    # === WĘDLINA (3) ===
    Product("Polędwica sopocka plastry", "wedlina", "same_sku", 100, "g", brand="Krakus"),
    Product("Kabanosy wieprzowe", "wedlina", "same_sku", 130, "g", brand="Tarczyński"),
    Product("Boczek wędzony plastry", "wedlina", "same_sku", 100, "g", brand="Sokołów"),

    # === OWOCE (5) ===
    Product("Banany", "owoce", "logical_only", 1.0, "kg",
            is_imported=True, origin_country="EC", notes="default origin: Ekwador"),
    Product("Jabłka", "owoce", "logical_only", 1.0, "kg"),
    Product("Awokado", "owoce", "logical_only", 1, "szt",
            is_imported=True, origin_country="ES", notes="default origin: Hiszpania/Peru"),
    Product("Mango", "owoce", "logical_only", 1, "szt",
            is_imported=True, origin_country="BR", notes="default origin: Brazylia/Peru"),
    Product("Cytryny", "owoce", "logical_only", 1.0, "kg",
            is_imported=True, origin_country="ES"),

    # === WARZYWA (7) ===
    Product("Pomidory świeże", "warzywa", "logical_only", 1.0, "kg"),
    Product("Marchew", "warzywa", "logical_only", 1.0, "kg"),
    Product("Ogórek zielony długi", "warzywa", "logical_only", 1, "szt"),
    Product("Szalotka", "warzywa", "logical_only", 1.0, "kg"),
    Product("Rukola opakowanie", "warzywa", "logical_only", 65, "g"),
    Product("Sałata rzymska mini", "warzywa", "logical_only", 1, "szt"),
    Product("Włoszczyzna", "warzywa", "logical_only", 500, "g"),

    # === TŁUSZCZE (1) ===
    Product("Oliwa z oliwek extra virgin", "tluszcze", "same_sku", 500, "ml",
            brand="Monini", is_imported=True, origin_country="IT"),

    # === PRZYPRAWY (4, dywersyfikacja marek) ===
    Product("Sól himalajska mielona", "przyprawy", "logical_only", 350, "g",
            notes="brand free — Frisco ma Sante, inne sklepy różne marki"),
    Product("Pieprz czarny mielony", "przyprawy", "same_sku", 10, "g", brand="Kamis"),
    Product("Liść laurowy", "przyprawy", "same_sku", 10, "g", brand="Prymat"),
    Product("Ziele angielskie", "przyprawy", "same_sku", 10, "g", brand="Galeo"),

    # === SŁODYCZE / SŁODZIKI (4) ===
    Product("Czekolada gorzka 64%", "slodycze", "same_sku", 100, "g", brand="Wedel"),
    Product("Kakao naturalne", "slodycze", "same_sku", 80, "g", brand="Wedel"),
    Product("Miód wielokwiatowy", "slodycze", "same_sku", 400, "g", brand="Sądecki Bartnik"),
    Product("Erytrytol", "slodycze", "logical_only", 1.0, "kg",
            notes="brand free — Frisco ma Big Nature, inne sklepy mogą mieć Targroch/Ostrovit"),

    # === NAPOJE (1) ===
    Product("Woda mineralna butelka", "napoje", "same_sku", 1.5, "l",
            brand="Muszynianka",
            notes="zgrzewka 6×1.5L niedostępna online — śledzimy butelkę 1.5L"),

    # === CHEMIA (5) ===
    Product("Pasta do zębów", "chemia", "same_sku", 100, "ml", brand="Sensodyne"),
    Product("Proszek do prania kolor", "chemia", "same_sku", 2.42, "kg", brand="Vizir",
            notes="Frisco ma 2.42kg/44 prań — 1kg nie istnieje online"),
    Product("Mydło w kostce Naturals", "chemia", "same_sku", 90, "g", brand="Palmolive"),
    Product("Płyn do naczyń", "chemia", "same_sku", 900, "ml", brand="Ludwik"),
    Product("Papier toaletowy", "chemia", "same_sku", 8, "rolek", brand="Velvet"),

    # === KONSERWY (2) ===
    Product("Pomidory krojone Polpa puszka", "konserwy", "same_sku", 400, "g",
            brand="Mutti", is_imported=True, origin_country="IT"),
    Product("Oliwki Kalamata słoik", "konserwy", "same_sku", 200, "g",
            is_imported=True, origin_country="GR",
            notes="brand TBD podczas URL mapping — co dostępne w obu sklepach"),

    # === MROŻONKI (2) ===
    Product("Groszek zielony mrożony", "mrozonki", "same_sku", 450, "g", brand="Hortex"),
    Product("Fasolka szparagowa mrożona", "mrozonki", "same_sku", 450, "g", brand="Hortex"),

    # === MĄKI (2, logical_only — marka rzadko ta sama cross-store) ===
    Product("Mąka żytnia typ 700", "maki", "logical_only", 1.0, "kg"),
    Product("Mąka orkiszowa", "maki", "logical_only", 1.0, "kg"),

    # === JAJKA (1) ===
    Product("Jajka zielononóżki", "jajka", "logical_only", 10, "szt",
            notes="rozmiar M, klasa A"),

    # === ZBOŻA / KASZE (2) ===
    Product("Ryż basmati", "zboza", "same_sku", 1.0, "kg", brand="Britta",
            notes="brand fallback: Lubella jeśli Britta nieobecna"),
    Product("Makaron pełnoziarnisty penne", "zboza", "same_sku", 500, "g",
            brand="Granoro", is_imported=True, origin_country="IT"),
]


def summary() -> dict:
    """Quick stats — useful sanity check after edits."""
    by_cat: dict[str, int] = {}
    for p in PRODUCTS:
        by_cat[p.category_user] = by_cat.get(p.category_user, 0) + 1
    return {
        "total": len(PRODUCTS),
        "by_category": by_cat,
        "same_sku": sum(1 for p in PRODUCTS if p.matching_type == "same_sku"),
        "logical_only": sum(1 for p in PRODUCTS if p.matching_type == "logical_only"),
        "imported": sum(1 for p in PRODUCTS if p.is_imported),
        "brands": len({p.brand for p in PRODUCTS if p.brand}),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(summary(), indent=2, ensure_ascii=False))
