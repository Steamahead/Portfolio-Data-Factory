"""
Azure SQL schema definitions — Gov Spending Radar tables.
==========================================================
Tables: gov_notices, gov_contractors, gov_classifications
Pattern: IF NOT EXISTS -> safe for repeated runs.
MERGE on natural keys -> idempotent upserts.
"""

# ── Table: gov_notices (main procurement notices) ────────────────

CREATE_NOTICES_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gov_notices')
CREATE TABLE gov_notices (
    object_id               NVARCHAR(50)    NOT NULL,
    notice_number           NVARCHAR(30)    NOT NULL,
    bzp_number              NVARCHAR(30)    NOT NULL,
    tender_id               NVARCHAR(100)   NOT NULL,
    notice_type             NVARCHAR(30)    NOT NULL,
    title                   NVARCHAR(1000)  NOT NULL,
    cpv_code                NVARCHAR(15)    NULL,
    cpv_raw                 NVARCHAR(MAX)   NULL,
    order_type              NVARCHAR(20)    NOT NULL,
    publication_date        DATETIME2       NOT NULL,
    deadline_date           DATETIME2       NULL,
    procedure_result        NVARCHAR(MAX)   NULL,
    is_below_eu_threshold   BIT             NOT NULL,
    client_type             NVARCHAR(20)    NOT NULL,
    tender_type             NVARCHAR(20)    NOT NULL,
    buyer_name              NVARCHAR(500)   NOT NULL,
    buyer_city              NVARCHAR(100)   NOT NULL,
    buyer_province          NVARCHAR(10)    NULL,
    buyer_country           NVARCHAR(5)     NOT NULL,
    buyer_nip               NVARCHAR(50)    NULL,
    buyer_org_id            NVARCHAR(50)    NOT NULL,
    budget_estimated        DECIMAL(18,2)   NULL,
    final_price             DECIMAL(18,2)   NULL,
    created_at              DATETIME        DEFAULT GETDATE(),
    PRIMARY KEY (object_id)
);
"""

# ── Table: gov_contractors (winners from TenderResultNotice) ─────

CREATE_CONTRACTORS_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gov_contractors')
CREATE TABLE gov_contractors (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    notice_object_id        NVARCHAR(50)    NOT NULL,
    contractor_name         NVARCHAR(500)   NULL,
    contractor_city         NVARCHAR(100)   NULL,
    contractor_province     NVARCHAR(10)    NULL,
    contractor_country      NVARCHAR(5)     NULL,
    contractor_nip          NVARCHAR(50)    NULL,
    part_index              SMALLINT        NOT NULL,
    part_result             NVARCHAR(30)    NULL,
    created_at              DATETIME        DEFAULT GETDATE(),
    UNIQUE (notice_object_id, part_index)
);
"""

# ── Table: gov_classifications (AI sector tagging) ──────────────

CREATE_CLASSIFICATIONS_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gov_classifications')
CREATE TABLE gov_classifications (
    id                      INT IDENTITY(1,1) PRIMARY KEY,
    notice_object_id        NVARCHAR(50)    NOT NULL,
    method                  NVARCHAR(20)    NOT NULL,
    sector                  NVARCHAR(30)    NULL,
    confidence              REAL            NULL,
    raw_response            NVARCHAR(MAX)   NULL,
    classified_at           DATETIME        DEFAULT GETDATE(),
    UNIQUE (notice_object_id, method)
);
"""

# ── MERGE: gov_notices (upsert on object_id) ────────────────────

MERGE_NOTICES_SQL = """
MERGE INTO gov_notices AS T
USING (SELECT ? AS object_id, ? AS notice_number, ? AS bzp_number,
              ? AS tender_id, ? AS notice_type, ? AS title,
              ? AS cpv_code, ? AS cpv_raw, ? AS order_type,
              ? AS publication_date, ? AS deadline_date,
              ? AS procedure_result, ? AS is_below_eu_threshold,
              ? AS client_type, ? AS tender_type,
              ? AS buyer_name, ? AS buyer_city, ? AS buyer_province,
              ? AS buyer_country, ? AS buyer_nip, ? AS buyer_org_id) AS S
ON T.object_id = S.object_id
WHEN MATCHED THEN UPDATE SET
    notice_number = S.notice_number, bzp_number = S.bzp_number,
    tender_id = S.tender_id, notice_type = S.notice_type,
    title = S.title, cpv_code = S.cpv_code, cpv_raw = S.cpv_raw,
    order_type = S.order_type, publication_date = S.publication_date,
    deadline_date = S.deadline_date, procedure_result = S.procedure_result,
    is_below_eu_threshold = S.is_below_eu_threshold,
    client_type = S.client_type, tender_type = S.tender_type,
    buyer_name = S.buyer_name, buyer_city = S.buyer_city,
    buyer_province = S.buyer_province, buyer_country = S.buyer_country,
    buyer_nip = S.buyer_nip, buyer_org_id = S.buyer_org_id,
    created_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (object_id, notice_number, bzp_number, tender_id, notice_type,
     title, cpv_code, cpv_raw, order_type, publication_date, deadline_date,
     procedure_result, is_below_eu_threshold, client_type, tender_type,
     buyer_name, buyer_city, buyer_province, buyer_country,
     buyer_nip, buyer_org_id)
    VALUES (S.object_id, S.notice_number, S.bzp_number, S.tender_id, S.notice_type,
            S.title, S.cpv_code, S.cpv_raw, S.order_type, S.publication_date,
            S.deadline_date, S.procedure_result, S.is_below_eu_threshold,
            S.client_type, S.tender_type, S.buyer_name, S.buyer_city,
            S.buyer_province, S.buyer_country, S.buyer_nip, S.buyer_org_id);
"""

# ── MERGE: gov_contractors (upsert on notice_object_id + part_index) ─

MERGE_CONTRACTORS_SQL = """
MERGE INTO gov_contractors AS T
USING (SELECT ? AS notice_object_id, ? AS contractor_name,
              ? AS contractor_city, ? AS contractor_province,
              ? AS contractor_country, ? AS contractor_nip,
              ? AS part_index, ? AS part_result) AS S
ON T.notice_object_id = S.notice_object_id AND T.part_index = S.part_index
WHEN MATCHED THEN UPDATE SET
    contractor_name = S.contractor_name, contractor_city = S.contractor_city,
    contractor_province = S.contractor_province,
    contractor_country = S.contractor_country,
    contractor_nip = S.contractor_nip, part_result = S.part_result,
    created_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (notice_object_id, contractor_name, contractor_city, contractor_province,
     contractor_country, contractor_nip, part_index, part_result)
    VALUES (S.notice_object_id, S.contractor_name, S.contractor_city,
            S.contractor_province, S.contractor_country, S.contractor_nip,
            S.part_index, S.part_result);
"""

# ── MERGE: gov_classifications (upsert on notice_object_id + method) ─

MERGE_CLASSIFICATIONS_SQL = """
MERGE INTO gov_classifications AS T
USING (SELECT ? AS notice_object_id, ? AS method,
              ? AS sector, ? AS confidence, ? AS raw_response) AS S
ON T.notice_object_id = S.notice_object_id AND T.method = S.method
WHEN MATCHED THEN UPDATE SET
    sector = S.sector, confidence = S.confidence,
    raw_response = S.raw_response, classified_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (notice_object_id, method, sector, confidence, raw_response)
    VALUES (S.notice_object_id, S.method, S.sector, S.confidence, S.raw_response);
"""

# ── Column order (must match MERGE parameter order) ─────────────

NOTICES_SQL_COLUMNS = [
    "object_id", "notice_number", "bzp_number", "tender_id", "notice_type",
    "title", "cpv_code", "cpv_raw", "order_type", "publication_date",
    "deadline_date", "procedure_result", "is_below_eu_threshold",
    "client_type", "tender_type", "buyer_name", "buyer_city",
    "buyer_province", "buyer_country", "buyer_nip", "buyer_org_id",
]

CONTRACTORS_SQL_COLUMNS = [
    "notice_object_id", "contractor_name", "contractor_city",
    "contractor_province", "contractor_country", "contractor_nip",
    "part_index", "part_result",
]

CLASSIFICATIONS_SQL_COLUMNS = [
    "notice_object_id", "method", "sector", "confidence", "raw_response",
]

# ── Migrations (safe to run multiple times) ──────────────────────
# Fix columns that were too narrow or too restrictive in initial CREATE.

MIGRATE_NOTICES_SQL = """
-- procedure_result: multi-part tenders can have 20+ semicolon-separated values
IF EXISTS (SELECT 1 FROM sys.columns
           WHERE object_id = OBJECT_ID('gov_notices') AND name = 'procedure_result'
           AND max_length != -1)
    ALTER TABLE gov_notices ALTER COLUMN procedure_result NVARCHAR(MAX) NULL;

-- buyer_province: some organizations don't have NUTS2 code
IF EXISTS (SELECT 1 FROM sys.columns
           WHERE object_id = OBJECT_ID('gov_notices') AND name = 'buyer_province'
           AND is_nullable = 0)
    ALTER TABLE gov_notices ALTER COLUMN buyer_province NVARCHAR(10) NULL;

-- buyer_nip: dirty data has prefixes like "NIP: 123...", "REGON:123..."
IF EXISTS (SELECT 1 FROM sys.columns
           WHERE object_id = OBJECT_ID('gov_notices') AND name = 'buyer_nip'
           AND max_length < 100)
    ALTER TABLE gov_notices ALTER COLUMN buyer_nip NVARCHAR(50) NULL;
"""

MIGRATE_CONTRACTORS_SQL = """
-- contractor_nip: dirty data — "NIP: 123...", "REGON:123...", "ABN 12 004 251", etc.
IF EXISTS (SELECT 1 FROM sys.columns
           WHERE object_id = OBJECT_ID('gov_contractors') AND name = 'contractor_nip'
           AND max_length < 100)
    ALTER TABLE gov_contractors ALTER COLUMN contractor_nip NVARCHAR(50) NULL;
"""
