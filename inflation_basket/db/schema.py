"""
Azure SQL schema — Inflation Basket pipeline.
=============================================
4 tables: inflation_products, inflation_product_urls,
          inflation_observations, inflation_shrinkflation_events.
Pattern: IF NOT EXISTS -> safe for repeated runs.
MERGE on natural keys -> idempotent upserts.

Decisions documented in docs/INFLATION_BASKET_SPEC.md (sesja 2026-04-30).
Matching strategy: same_sku (31) | logical_only (20) — see seed/products.py.
"""

# ============================================================================
# CREATE TABLE statements (IF NOT EXISTS)
# ============================================================================

CREATE_PRODUCTS_TABLE_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'inflation_products')
BEGIN
    CREATE TABLE inflation_products (
        product_id        INT IDENTITY(1,1) PRIMARY KEY,
        ean               NVARCHAR(13) NULL,
        name_canonical    NVARCHAR(200) NOT NULL,
        brand             NVARCHAR(100) NULL,
        category_user     NVARCHAR(50)  NOT NULL,
        category_gus      NVARCHAR(50)  NULL,
        matching_type     NVARCHAR(20)  NOT NULL,
        capacity_value    DECIMAL(10,3) NOT NULL,
        capacity_unit     NVARCHAR(10)  NOT NULL,
        is_imported       BIT           NOT NULL DEFAULT 0,
        origin_country    CHAR(2)       NULL,
        alternative_names NVARCHAR(500) NULL,
        status            NVARCHAR(20)  NOT NULL DEFAULT 'active',
        created_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT CK_inflation_products_matching
            CHECK (matching_type IN ('same_sku', 'logical_only')),
        CONSTRAINT CK_inflation_products_status
            CHECK (status IN ('active', 'discontinued'))
    );
    CREATE UNIQUE INDEX UX_inflation_products_ean
        ON inflation_products(ean) WHERE ean IS NOT NULL;
    CREATE INDEX IX_inflation_products_category
        ON inflation_products(category_user);
END
"""

CREATE_PRODUCT_URLS_TABLE_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'inflation_product_urls')
BEGIN
    CREATE TABLE inflation_product_urls (
        product_id    INT          NOT NULL,
        store         NVARCHAR(20) NOT NULL,
        url           NVARCHAR(500) NOT NULL,
        sku_store     NVARCHAR(50) NULL,
        active        BIT          NOT NULL DEFAULT 1,
        last_seen_at  DATETIME2    NULL,
        created_at    DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at    DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_inflation_product_urls PRIMARY KEY (product_id, store),
        CONSTRAINT FK_inflation_product_urls_products
            FOREIGN KEY (product_id) REFERENCES inflation_products(product_id),
        CONSTRAINT CK_inflation_product_urls_store
            CHECK (store IN ('frisco', 'auchan_warsaw'))
    );
END
"""

CREATE_OBSERVATIONS_TABLE_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'inflation_observations')
BEGIN
    CREATE TABLE inflation_observations (
        product_id     INT           NOT NULL,
        store          NVARCHAR(20)  NOT NULL,
        obs_date       DATE          NOT NULL,
        obs_ts         DATETIME2     NOT NULL,
        price_regular  DECIMAL(10,2) NOT NULL,
        price_promo    DECIMAL(10,2) NULL,
        promo_active   BIT           NOT NULL DEFAULT 0,
        unit_price     DECIMAL(10,4) NULL,
        capacity_seen  DECIMAL(10,3) NULL,
        currency       CHAR(3)       NOT NULL DEFAULT 'PLN',
        created_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_inflation_observations PRIMARY KEY (product_id, store, obs_date),
        CONSTRAINT FK_inflation_observations_products
            FOREIGN KEY (product_id) REFERENCES inflation_products(product_id)
    );
    CREATE INDEX IX_inflation_observations_date  ON inflation_observations(obs_date);
    CREATE INDEX IX_inflation_observations_store ON inflation_observations(store, obs_date);
END
"""

CREATE_SHRINKFLATION_TABLE_SQL = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'inflation_shrinkflation_events')
BEGIN
    CREATE TABLE inflation_shrinkflation_events (
        event_id           INT IDENTITY(1,1) PRIMARY KEY,
        product_id         INT           NOT NULL,
        store              NVARCHAR(20)  NOT NULL,
        detected_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        capacity_before    DECIMAL(10,3) NOT NULL,
        capacity_after     DECIMAL(10,3) NOT NULL,
        price_before       DECIMAL(10,2) NOT NULL,
        price_after        DECIMAL(10,2) NOT NULL,
        real_increase_pct  DECIMAL(6,3)  NOT NULL,
        gemini_confidence  DECIMAL(3,2)  NULL,
        notes              NVARCHAR(500) NULL,
        CONSTRAINT FK_inflation_shrinkflation_products
            FOREIGN KEY (product_id) REFERENCES inflation_products(product_id)
    );
    CREATE INDEX IX_inflation_shrinkflation_product
        ON inflation_shrinkflation_events(product_id, detected_at);
END
"""

CREATE_TABLE_SQLS: list[str] = [
    CREATE_PRODUCTS_TABLE_SQL,
    CREATE_PRODUCT_URLS_TABLE_SQL,
    CREATE_OBSERVATIONS_TABLE_SQL,
    CREATE_SHRINKFLATION_TABLE_SQL,
]

# ============================================================================
# MERGE / INSERT statements (idempotent upserts)
# ============================================================================

# Natural key for products: (name_canonical, brand, capacity_value, capacity_unit).
# EAN cannot be the matching key — nullable for logical_only items.
# IS NULL safe matching for brand (logical_only products often have no brand).
MERGE_PRODUCT_SQL = """
MERGE inflation_products AS target
USING (
    SELECT
        ? AS ean, ? AS name_canonical, ? AS brand, ? AS category_user,
        ? AS matching_type, ? AS capacity_value, ? AS capacity_unit,
        ? AS is_imported, ? AS origin_country, ? AS alternative_names
) AS source
ON  target.name_canonical = source.name_canonical
AND (target.brand = source.brand OR (target.brand IS NULL AND source.brand IS NULL))
AND target.capacity_value = source.capacity_value
AND target.capacity_unit  = source.capacity_unit
WHEN MATCHED THEN
    UPDATE SET
        ean = source.ean,
        category_user = source.category_user,
        matching_type = source.matching_type,
        is_imported = source.is_imported,
        origin_country = source.origin_country,
        alternative_names = source.alternative_names,
        updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (ean, name_canonical, brand, category_user, matching_type,
            capacity_value, capacity_unit, is_imported, origin_country, alternative_names)
    VALUES (source.ean, source.name_canonical, source.brand, source.category_user,
            source.matching_type, source.capacity_value, source.capacity_unit,
            source.is_imported, source.origin_country, source.alternative_names);
"""

MERGE_PRODUCT_URL_SQL = """
MERGE inflation_product_urls AS target
USING (
    SELECT ? AS product_id, ? AS store, ? AS url, ? AS sku_store, ? AS active
) AS source
ON target.product_id = source.product_id AND target.store = source.store
WHEN MATCHED THEN
    UPDATE SET
        url = source.url,
        sku_store = source.sku_store,
        active = source.active,
        last_seen_at = SYSUTCDATETIME(),
        updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (product_id, store, url, sku_store, active, last_seen_at)
    VALUES (source.product_id, source.store, source.url, source.sku_store,
            source.active, SYSUTCDATETIME());
"""

# Observations: PK = (product_id, store, obs_date). Same-day rerun = UPDATE last write.
MERGE_OBSERVATION_SQL = """
MERGE inflation_observations AS target
USING (
    SELECT ? AS product_id, ? AS store, ? AS obs_date, ? AS obs_ts,
           ? AS price_regular, ? AS price_promo, ? AS promo_active,
           ? AS unit_price, ? AS capacity_seen, ? AS currency
) AS source
ON  target.product_id = source.product_id
AND target.store      = source.store
AND target.obs_date   = source.obs_date
WHEN MATCHED THEN
    UPDATE SET
        obs_ts = source.obs_ts,
        price_regular = source.price_regular,
        price_promo = source.price_promo,
        promo_active = source.promo_active,
        unit_price = source.unit_price,
        capacity_seen = source.capacity_seen
WHEN NOT MATCHED THEN
    INSERT (product_id, store, obs_date, obs_ts, price_regular, price_promo,
            promo_active, unit_price, capacity_seen, currency)
    VALUES (source.product_id, source.store, source.obs_date, source.obs_ts,
            source.price_regular, source.price_promo, source.promo_active,
            source.unit_price, source.capacity_seen, source.currency);
"""

# Shrinkflation events are append-only — every detection is a new record.
INSERT_SHRINKFLATION_SQL = """
INSERT INTO inflation_shrinkflation_events
    (product_id, store, capacity_before, capacity_after,
     price_before, price_after, real_increase_pct, gemini_confidence, notes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
