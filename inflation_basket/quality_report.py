"""Quality report builder — runs after each scrape, produces structured metrics.

Pure SQL on Azure SQL. No LLM. Output is consumed by llm_review.py and
the email body. Thresholds are explicit so the LLM (and humans) can
re-grade severities with full context.
"""

from __future__ import annotations

from datetime import date, timedelta

from inflation_basket.db.operations import _connect_with_retry


THRESHOLDS = {
    "missing_warning_days": 3,
    "missing_critical_days": 4,
    "stale_warning_cycles": 6,
    "stale_critical_cycles": 10,
    "price_move_warning_pct": 15.0,
    "price_move_critical_pct": 40.0,
    "shrinkflation_capacity_drop_pct": 5.0,
}


def _coverage(cur, today: date) -> dict:
    cur.execute(
        """
        SELECT u.store,
               (SELECT COUNT(*) FROM inflation_product_urls
                WHERE store = u.store AND active = 1) AS expected,
               COUNT(DISTINCT o.product_id) AS observed
        FROM inflation_product_urls u
        LEFT JOIN inflation_observations o
          ON o.product_id = u.product_id AND o.store = u.store AND o.obs_date = ?
        WHERE u.active = 1
        GROUP BY u.store
        """,
        (today,),
    )
    return {row[0]: {"expected": row[1], "observed": row[2]} for row in cur.fetchall()}


def _missing_today(cur, today: date) -> list[dict]:
    cur.execute(
        """
        SELECT u.product_id, u.store, p.name_canonical, p.brand,
               (SELECT MAX(obs_date) FROM inflation_observations o
                WHERE o.product_id = u.product_id AND o.store = u.store) AS last_seen
        FROM inflation_product_urls u
        JOIN inflation_products p ON p.product_id = u.product_id
        WHERE u.active = 1 AND p.status = 'active'
          AND NOT EXISTS (
            SELECT 1 FROM inflation_observations o
            WHERE o.product_id = u.product_id AND o.store = u.store AND o.obs_date = ?
          )
        ORDER BY u.store, u.product_id
        """,
        (today,),
    )
    rows = []
    for r in cur.fetchall():
        last_seen = r[4]
        days_since = (today - last_seen).days if last_seen else None
        if days_since is not None and days_since >= THRESHOLDS["missing_critical_days"]:
            sev = "critical"
        elif days_since is None or days_since >= THRESHOLDS["missing_warning_days"]:
            sev = "warning"
        else:
            sev = "info"
        rows.append({
            "product_id": r[0], "store": r[1], "name": r[2], "brand": r[3] or "",
            "last_seen": last_seen.isoformat() if last_seen else None,
            "days_since": days_since,
            "severity": sev,
        })
    return rows


def _price_moves(cur, today: date, top_n: int = 10) -> list[dict]:
    """Today vs avg(last 7 days excluding today)."""
    cur.execute(
        """
        WITH today_obs AS (
          SELECT product_id, store, price_regular FROM inflation_observations WHERE obs_date = ?
        ),
        prev7 AS (
          SELECT product_id, store, AVG(price_regular) AS avg7
          FROM inflation_observations
          WHERE obs_date BETWEEN ? AND ?
          GROUP BY product_id, store
        )
        SELECT t.product_id, t.store, p.name_canonical, p.brand,
               t.price_regular, prev7.avg7
        FROM today_obs t
        JOIN prev7 ON prev7.product_id = t.product_id AND prev7.store = t.store
        JOIN inflation_products p ON p.product_id = t.product_id
        WHERE prev7.avg7 > 0
        """,
        (today, today - timedelta(days=7), today - timedelta(days=1)),
    )
    moves = []
    for r in cur.fetchall():
        current, avg7 = float(r[4]), float(r[5])
        pct = (current - avg7) / avg7 * 100.0
        if abs(pct) < THRESHOLDS["price_move_warning_pct"]:
            continue
        sev = "critical" if abs(pct) >= THRESHOLDS["price_move_critical_pct"] else "warning"
        moves.append({
            "product_id": r[0], "store": r[1], "name": r[2], "brand": r[3] or "",
            "current": round(current, 2), "avg7d": round(avg7, 2),
            "pct_change": round(pct, 1), "severity": sev,
        })
    moves.sort(key=lambda x: abs(x["pct_change"]), reverse=True)
    return moves[:top_n]


def _promo_flips(cur, today: date) -> dict:
    """Count of products entering/leaving promo vs their LAST previous obs."""
    cur.execute(
        """
        WITH t AS (SELECT product_id, store, promo_active FROM inflation_observations WHERE obs_date = ?),
        prev_ranked AS (
          SELECT product_id, store, promo_active,
                 ROW_NUMBER() OVER (PARTITION BY product_id, store ORDER BY obs_date DESC) rn
          FROM inflation_observations WHERE obs_date < ?
        ),
        prev AS (SELECT product_id, store, promo_active FROM prev_ranked WHERE rn = 1)
        SELECT t.store,
               SUM(CASE WHEN t.promo_active = 1 AND ISNULL(prev.promo_active, 0) = 0 THEN 1 ELSE 0 END) AS entered,
               SUM(CASE WHEN t.promo_active = 0 AND ISNULL(prev.promo_active, 0) = 1 THEN 1 ELSE 0 END) AS left_promo
        FROM t LEFT JOIN prev ON prev.product_id = t.product_id AND prev.store = t.store
        GROUP BY t.store
        """,
        (today, today),
    )
    return {r[0]: {"entered": int(r[1] or 0), "left": int(r[2] or 0)} for r in cur.fetchall()}


def _stale_prices(cur, today: date) -> list[dict]:
    """Products whose price has not changed for >= warning_cycles distinct obs_dates."""
    warn = THRESHOLDS["stale_warning_cycles"]
    crit = THRESHOLDS["stale_critical_cycles"]
    cur.execute(
        """
        SELECT o.product_id, o.store, p.name_canonical, p.brand,
               COUNT(DISTINCT o.obs_date) AS cycles_same,
               MAX(o.price_regular) AS price,
               MIN(o.obs_date) AS first_seen, MAX(o.obs_date) AS last_seen
        FROM inflation_observations o
        JOIN inflation_products p ON p.product_id = o.product_id
        WHERE o.obs_date >= ?
        GROUP BY o.product_id, o.store, p.name_canonical, p.brand
        HAVING COUNT(DISTINCT o.price_regular) = 1 AND COUNT(DISTINCT o.obs_date) >= ?
        ORDER BY COUNT(DISTINCT o.obs_date) DESC
        """,
        (today - timedelta(days=30), warn),
    )
    rows = []
    for r in cur.fetchall():
        cycles = int(r[4])
        sev = "critical" if cycles >= crit else "warning"
        rows.append({
            "product_id": r[0], "store": r[1], "name": r[2], "brand": r[3] or "",
            "cycles_same": cycles, "price": float(r[5]) if r[5] else None,
            "first_seen": r[6].isoformat() if r[6] else None,
            "last_seen": r[7].isoformat() if r[7] else None,
            "severity": sev,
        })
    return rows


def _shrinkflation_candidates(cur, today: date) -> list[dict]:
    """capacity_seen drop vs LAST previous obs (within store)."""
    drop_pct = THRESHOLDS["shrinkflation_capacity_drop_pct"]
    cur.execute(
        """
        WITH t AS (
          SELECT product_id, store, capacity_seen, price_regular
          FROM inflation_observations
          WHERE obs_date = ? AND capacity_seen IS NOT NULL
        ),
        prev_ranked AS (
          SELECT product_id, store, capacity_seen, price_regular,
                 ROW_NUMBER() OVER (PARTITION BY product_id, store ORDER BY obs_date DESC) rn
          FROM inflation_observations WHERE obs_date < ? AND capacity_seen IS NOT NULL
        ),
        prev AS (SELECT * FROM prev_ranked WHERE rn = 1)
        SELECT t.product_id, t.store, p.name_canonical, p.brand,
               t.capacity_seen, prev.capacity_seen, t.price_regular, prev.price_regular
        FROM t JOIN prev ON prev.product_id = t.product_id AND prev.store = t.store
        JOIN inflation_products p ON p.product_id = t.product_id
        WHERE prev.capacity_seen > 0
          AND (prev.capacity_seen - t.capacity_seen) / prev.capacity_seen * 100.0 >= ?
        """,
        (today, today, drop_pct),
    )
    rows = []
    for r in cur.fetchall():
        cap_now, cap_prev = float(r[4]), float(r[5])
        price_now, price_prev = float(r[6]), float(r[7])
        cap_drop = (cap_prev - cap_now) / cap_prev * 100.0
        price_change = (price_now - price_prev) / price_prev * 100.0 if price_prev else 0
        # critical if capacity dropped but price did NOT drop proportionally
        sev = "critical" if price_change >= -1.0 else "warning"
        rows.append({
            "product_id": r[0], "store": r[1], "name": r[2], "brand": r[3] or "",
            "capacity_now": cap_now, "capacity_prev": cap_prev,
            "capacity_drop_pct": round(cap_drop, 1),
            "price_now": round(price_now, 2), "price_prev": round(price_prev, 2),
            "price_change_pct": round(price_change, 1),
            "severity": sev,
        })
    return rows


def _cross_store_anomalies(cur, today: date) -> list[dict]:
    """Cross-store snapshot for products flagged cross_store_eligible.

    Returns ALL eligible products (not just those exceeding a threshold).
    Severity is always 'info' — this is a dashboard widget feed, not an
    anomaly detector. Sort by delta_pct desc so the largest gaps appear first.
    """
    cur.execute(
        """
        WITH t AS (
            SELECT product_id, store, price_regular, unit_price
            FROM inflation_observations WHERE obs_date = ?
        )
        SELECT f.product_id, p.name_canonical, p.brand, p.capacity_unit,
               f.price_regular, a.price_regular,
               f.unit_price, a.unit_price
        FROM t f JOIN t a ON a.product_id = f.product_id
        JOIN inflation_products p ON p.product_id = f.product_id
        WHERE f.store = 'frisco' AND a.store = 'auchan_warsaw'
          AND p.cross_store_eligible = 1
        """,
        (today,),
    )
    rows = []
    for r in cur.fetchall():
        pid, name, brand, cap_unit = r[0], r[1], r[2] or "", r[3]
        f_price, a_price = float(r[4]), float(r[5])
        f_unit = float(r[6]) if r[6] is not None else None
        a_unit = float(r[7]) if r[7] is not None else None

        if f_unit is not None and a_unit is not None and min(f_unit, a_unit) > 0:
            delta_pct = abs(f_unit - a_unit) / min(f_unit, a_unit) * 100.0
            basis = "unit_price"
        elif min(f_price, a_price) > 0:
            delta_pct = abs(f_price - a_price) / min(f_price, a_price) * 100.0
            basis = "price_regular"
        else:
            delta_pct = None
            basis = "no_data"

        cheaper = None
        if delta_pct is not None:
            ref_f = f_unit if f_unit is not None else f_price
            ref_a = a_unit if a_unit is not None else a_price
            cheaper = "frisco" if ref_f < ref_a else ("auchan_warsaw" if ref_a < ref_f else "tie")

        rows.append({
            "product_id": pid, "name": name, "brand": brand,
            "capacity_unit": cap_unit,
            "frisco_price": round(f_price, 2),
            "auchan_price": round(a_price, 2),
            "frisco_unit_price": round(f_unit, 4) if f_unit is not None else None,
            "auchan_unit_price": round(a_unit, 4) if a_unit is not None else None,
            "delta_pct": round(delta_pct, 1) if delta_pct is not None else None,
            "basis": basis,
            "cheaper": cheaper,
            "severity": "info",
        })

    rows.sort(key=lambda x: (x["delta_pct"] is None, -(x["delta_pct"] or 0)))
    return rows


def _basket_index(cur, today: date) -> list[dict]:
    """Per-store basket total today vs last prior scrape.

    Restricted to products observed in BOTH runs so adding/removing a product
    does not artificially move the index — the headline KPI for the dashboard
    must reflect price drift, not catalog churn.
    """
    cur.execute(
        "SELECT MAX(obs_date) FROM inflation_observations WHERE obs_date < ?",
        (today,),
    )
    row = cur.fetchone()
    prev_date = row[0] if row else None
    if prev_date is None:
        return []

    cur.execute(
        """
        WITH common AS (
            SELECT t.store, t.product_id,
                   t.price_regular AS now_p, p.price_regular AS prev_p
            FROM inflation_observations t
            JOIN inflation_observations p
              ON p.product_id = t.product_id AND p.store = t.store
             AND p.obs_date = ?
            WHERE t.obs_date = ?
        )
        SELECT store, COUNT(*) AS n,
               SUM(now_p) AS now_total, SUM(prev_p) AS prev_total
        FROM common
        GROUP BY store
        """,
        (prev_date, today),
    )
    rows = []
    for r in cur.fetchall():
        store, n = r[0], int(r[1])
        now_total = float(r[2]) if r[2] is not None else 0.0
        prev_total = float(r[3]) if r[3] is not None else 0.0
        delta_pct = (now_total - prev_total) / prev_total * 100.0 if prev_total > 0 else None
        rows.append({
            "store": store,
            "products_compared": n,
            "now_total": round(now_total, 2),
            "prev_total": round(prev_total, 2),
            "prev_date": prev_date.isoformat(),
            "delta_pct": round(delta_pct, 2) if delta_pct is not None else None,
        })
    return rows


def build_quality_report(today_results: list[dict], today: date | None = None) -> dict:
    """Aggregate metrics. `today_results` = output from scrape_store calls."""
    if today is None:
        today = date.today()
    with _connect_with_retry() as conn:
        cur = conn.cursor()
        return {
            "scrape_date": today.isoformat(),
            "scrape_results": today_results,
            "thresholds": THRESHOLDS,
            "coverage": _coverage(cur, today),
            "basket_index": _basket_index(cur, today),
            "missing_today": _missing_today(cur, today),
            "price_moves": _price_moves(cur, today),
            "promo_flips": _promo_flips(cur, today),
            "stale_prices": _stale_prices(cur, today),
            "shrinkflation": _shrinkflation_candidates(cur, today),
            "cross_store_anomalies": _cross_store_anomalies(cur, today),
        }


if __name__ == "__main__":
    import json
    report = build_quality_report([], today=date.today())
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
