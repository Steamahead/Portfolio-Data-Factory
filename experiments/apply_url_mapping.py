"""Apply manual URL mapping from user input (2026-05-02 session).

Three operations in order:
  1. UPDATE master catalog (12 products) — brand/name/matching_type/capacity changes
  2. UPSERT 40 product_url rows (some INSERT, some UPDATE existing Frisco mappings)
  3. Print final coverage report

Run:  .venv/Scripts/python.exe -X utf8 experiments/apply_url_mapping.py [--apply]
Without --apply = dry-run (prints SQL but does not execute).
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from inflation_basket.db.operations import _connect_with_retry, upsert_product_url


CATALOG_UPDATES = [
    (58, {"brand": "Castelli"}),
    (62, {"name_canonical": "Udziec wołowy", "matching_type": "logical_only"}),
    (65, {"brand": "Wierzejki"}),
    (73, {"name_canonical": "Pomidor malinowy", "capacity_value": 0.5, "capacity_unit": "kg"}),
    (75, {"name_canonical": "Ogórki krótkie", "capacity_value": 0.5, "capacity_unit": "kg"}),
    (84, {"brand": "Kamis"}),
    (91, {"matching_type": "logical_only"}),
    (94, {"brand": "Regina"}),
    (96, {"matching_type": "logical_only"}),
    (99, {"name_canonical": "Mąka żytnia chlebowa", "matching_type": "logical_only"}),
    (102, {"brand": "Kupiec"}),
    (103, {"name_canonical": "Makaron penne", "brand": "Lubella", "capacity_value": 0.4, "capacity_unit": "kg"}),
]


URLS = [
    # (pid, store, url, sku)
    (58, "frisco", "https://www.frisco.pl/pid,8164/n,castelli-ser-grana-padano-(klinek)-d.o.p./stn,product", "8164"),
    (58, "auchan_warsaw", "https://zakupy.auchan.pl/products/ser-grana-padano-półtłusty-wolno-dojrzewający-twardy-castelli-125-g/00210933", "00210933"),
    (60, "frisco", "https://www.frisco.pl/pid,10239/n,frisco-fresh-filet-z-indyka-(pakowany-prozniowo)/stn,product", "10239"),
    (60, "auchan_warsaw", "https://zakupy.auchan.pl/products/filet-z-indyka-luz-auchan-na-wagę-ok-900-g/00069966", "00069966"),
    (62, "frisco", "https://www.frisco.pl/pid,95231/n,frisco-fresh-udziec-wolowy-zrazowka-gorna/stn,product", "95231"),
    (62, "auchan_warsaw", "https://zakupy.auchan.pl/products/udziec-wołowy-auchan-na-wagę-ok-1-kg/00283977", "00283977"),
    (65, "frisco", "https://www.frisco.pl/pid,89339/n,wierzejki-poledwica-sopocka---plastry/stn,product", "89339"),
    (65, "auchan_warsaw", "https://zakupy.auchan.pl/products/polędwica-sopocka-plastry-auchan-na-wagę-ok-100-g/00851926", "00851926"),
    (68, "frisco", "https://www.frisco.pl/pid,4094/n,frisco-fresh-banany-kisc-4-6-szt./stn,product", "4094"),
    (68, "auchan_warsaw", "https://zakupy.auchan.pl/products/banany-premium-owoce-auchan-na-wagę-kiść-4-6-szt-ok-1-kg/00034041", "00034041"),
    (70, "frisco", "https://www.frisco.pl/pid,91475/n,frisco-fresh-avocado-hass-dojrzale/stn,product", "91475"),
    (70, "auchan_warsaw", "https://zakupy.auchan.pl/products/awokado-hass-owoce-auchan-sztuka/00221740", "00221740"),
    (72, "frisco", "https://www.frisco.pl/pid,12074/n,frisco-fresh-cytryny-luz-4-6-szt./stn,product", "12074"),
    (72, "auchan_warsaw", "https://zakupy.auchan.pl/products/cytryny-owoce-auchan-1-kg/00851403", "00851403"),
    (73, "frisco", "https://www.frisco.pl/pid,99999/n,frisco-fresh-pomidory-malinowe-3-4szt./stn,product", "99999"),
    (73, "auchan_warsaw", "https://zakupy.auchan.pl/products/pomidor-malinowy-warzywa-auchan-na-wagę-ok-500-g/00723870", "00723870"),
    (75, "frisco", "https://www.frisco.pl/pid,95329/n,frisco-fresh-ogorki-krotkie-2-4-szt./stn,product", "95329"),
    (75, "auchan_warsaw", "https://zakupy.auchan.pl/products/ogórek-krótki-pewni-dobrego-na-wagę-ok-500-g/00671800", "00671800"),
    (77, "auchan_warsaw", "https://zakupy.auchan.pl/products/rukola-warzywa-auchan-100-g/00535761", "00535761"),
    (81, "auchan_warsaw", "https://zakupy.auchan.pl/products/sól-drobnoziarnista-himalajska-jodowana-auchan-600-g/00586754", "00586754"),
    (83, "frisco", "https://www.frisco.pl/pid,144537/n,prymat-lisc-laurowy-suszony-xl/stn,product", "144537"),
    (83, "auchan_warsaw", "https://zakupy.auchan.pl/products/liść-laurowy-prymat-6-g/00192102", "00192102"),
    (84, "frisco", "https://www.frisco.pl/pid,141513/n,kamis-ziele-angielskie/stn,product", "141513"),
    (84, "auchan_warsaw", "https://zakupy.auchan.pl/products/ziele-angielskie-kamis-12-g/00811103", "00811103"),
    (89, "frisco", "https://www.frisco.pl/pid,153950/n,muszynianka-naturalna-woda-mineralna-czesciowo-odgazowana/stn,product", "153950"),
    (89, "auchan_warsaw", "https://zakupy.auchan.pl/products/naturalna-woda-mineralna-lekko-gazowana-muszynianka-6-x-1-5-l/00634310", "00634310"),
    (91, "frisco", "https://www.frisco.pl/pid,141772/n,vizir-alpine-fresh-proszek-do-prania-bialych-i-jasnych-tkanin-60-pran/stn,product", "141772"),
    (91, "auchan_warsaw", "https://zakupy.auchan.pl/products/proszek-do-prania-do-kolorów-xxl-60-prań-vizir-3-3-kg/00802778", "00802778"),
    (94, "frisco", "https://www.frisco.pl/pid,4048/n,regina-papier-rumiankowy-papier-toaletowy-8-rolek/stn,product", "4048"),
    (94, "auchan_warsaw", "https://zakupy.auchan.pl/products/papier-toaletowy-regina-8-rolek/00273931", "00273931"),
    (95, "frisco", "https://www.frisco.pl/pid,894/n,mutti-pomidory-drobno-krojone-(bez-skorki)/stn,product", "894"),
    (95, "auchan_warsaw", "https://zakupy.auchan.pl/products/pomidory-drobno-krojone-bez-skórek-mutti-400-g/00473147", "00473147"),
    (96, "frisco", "https://www.frisco.pl/pid,118593/n,selia-oliwki-kalamata-z-pestka/stn,product", "118593"),
    (96, "auchan_warsaw", "https://zakupy.auchan.pl/products/oliwki-kalamata-całe-athina-360-g/00219144", "00219144"),
    (99, "frisco", "https://www.frisco.pl/pid,14675/n,bio-planet-maka-zytnia-(typ-960)-bio/stn,product", "14675"),
    (99, "auchan_warsaw", "https://zakupy.auchan.pl/products/bio-mąka-żytnia-chlebowa-podlaskie-mąki-ekologiczne-1-kg/00962263", "00962263"),
    (102, "frisco", "https://www.frisco.pl/pid,148250/n,kupiec-ryz-basmati-(3x100g)/stn,product", "148250"),
    (102, "auchan_warsaw", "https://zakupy.auchan.pl/products/ryż-basmati-kupiec-3-x-100-g/00030947", "00030947"),
    (103, "frisco", "https://www.frisco.pl/pid,363/n,lubella-makaron-piora-(penne-rigate)/stn,product", "363"),
    (103, "auchan_warsaw", "https://zakupy.auchan.pl/products/makaron-pióra-lubella-400-g/00318965", "00318965"),
]


def update_catalog(apply: bool) -> None:
    print(f"\n=== STEP 1: Master catalog updates ({len(CATALOG_UPDATES)}) ===")
    if not apply:
        for pid, fields in CATALOG_UPDATES:
            sets = ", ".join(f"{k}={v!r}" for k, v in fields.items())
            print(f"  [DRY] UPDATE inflation_products SET {sets} WHERE product_id={pid}")
        return

    with _connect_with_retry() as conn:
        cur = conn.cursor()
        for pid, fields in CATALOG_UPDATES:
            set_clause = ", ".join(f"{k} = ?" for k in fields)
            params = list(fields.values()) + [pid]
            sql = f"UPDATE inflation_products SET {set_clause}, updated_at = SYSUTCDATETIME() WHERE product_id = ?"
            cur.execute(sql, params)
            print(f"  ID {pid:3d} updated: {fields}")
        conn.commit()
    print(f"  {len(CATALOG_UPDATES)} catalog rows updated")


def upsert_urls(apply: bool) -> None:
    print(f"\n=== STEP 2: URL upserts ({len(URLS)}) ===")
    by_store: dict[str, int] = defaultdict(int)
    for pid, store, url, sku in URLS:
        by_store[store] += 1
        if not apply:
            print(f"  [DRY] {pid:3d} {store:14s} sku={sku:8s} {url[:80]}")
            continue
        upsert_product_url(pid, store, url, sku, active=True)
        print(f"  ID {pid:3d} | {store:14s} | sku={sku}")
    print(f"  By store: {dict(by_store)}")


def report_coverage() -> None:
    print("\n=== STEP 3: Final coverage ===")
    sql = """
    SELECT p.product_id, p.name_canonical, p.brand, p.matching_type,
           SUM(CASE WHEN u.store='frisco' AND u.active=1 THEN 1 ELSE 0 END) AS has_frisco,
           SUM(CASE WHEN u.store='auchan_warsaw' AND u.active=1 THEN 1 ELSE 0 END) AS has_auchan
    FROM inflation_products p
    LEFT JOIN inflation_product_urls u ON u.product_id = p.product_id
    WHERE p.status='active'
    GROUP BY p.product_id, p.name_canonical, p.brand, p.matching_type
    ORDER BY p.product_id;
    """
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

    both = sum(1 for r in rows if r[4] and r[5])
    only_f = sum(1 for r in rows if r[4] and not r[5])
    only_a = sum(1 for r in rows if not r[4] and r[5])
    none = sum(1 for r in rows if not r[4] and not r[5])
    print(f"  total={len(rows)}  both={both}  only_frisco={only_f}  only_auchan={only_a}  none={none}")
    print(f"  cross-store coverage: {both}/{len(rows)} = {100*both/len(rows):.0f}%")
    if only_f or only_a or none:
        print("  Still incomplete:")
        for r in rows:
            if not (r[4] and r[5]):
                miss = []
                if not r[4]: miss.append("frisco")
                if not r[5]: miss.append("auchan")
                print(f"    ID {r[0]:3d} | {r[1]:35s} | brand={r[2] or '-':12s} | missing: {','.join(miss)}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Execute (default: dry-run)")
    args = ap.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== inflation_basket URL remap — {mode} ===")
    update_catalog(args.apply)
    upsert_urls(args.apply)
    if args.apply:
        report_coverage()
    return 0


if __name__ == "__main__":
    sys.exit(main())
