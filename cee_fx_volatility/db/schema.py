"""
Azure SQL schema definitions — CEE FX Volatility tables.
=========================================================
Tables: cee_fx_rates, cee_news_headlines
Pattern: IF NOT EXISTS → safe for repeated runs.
"""

CREATE_FX_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cee_fx_rates')
CREATE TABLE cee_fx_rates (
    timestamp           NVARCHAR(30)   NOT NULL,
    currency_pair       NVARCHAR(10)   NOT NULL,
    [open]              REAL           NOT NULL,
    high                REAL           NOT NULL,
    low                 REAL           NOT NULL,
    [close]             REAL           NOT NULL,
    volume              REAL           NULL,
    volatility_1h       REAL           NOT NULL,
    created_at          DATETIME       DEFAULT GETDATE(),
    PRIMARY KEY (timestamp, currency_pair)
);
"""

CREATE_NEWS_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cee_news_headlines')
CREATE TABLE cee_news_headlines (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    published_at        NVARCHAR(30)   NULL,
    fetched_at          NVARCHAR(30)   NOT NULL,
    source              NVARCHAR(20)   NOT NULL,
    title               NVARCHAR(1000) NOT NULL,
    url                 NVARCHAR(2000) NOT NULL,
    category            NVARCHAR(30)   NULL,
    sentiment           REAL           NULL,
    is_surprising       BIT            NULL,
    raw_ai_response     NVARCHAR(MAX)  NULL,
    created_at          DATETIME       DEFAULT GETDATE(),
    UNIQUE (url)
);
"""

MERGE_FX_SQL = """
MERGE INTO cee_fx_rates AS T
USING (SELECT ? AS timestamp, ? AS currency_pair,
              ? AS [open], ? AS high, ? AS low, ? AS [close],
              ? AS volume, ? AS volatility_1h) AS S
ON T.timestamp = S.timestamp AND T.currency_pair = S.currency_pair
WHEN MATCHED THEN UPDATE SET
    [open] = S.[open], high = S.high, low = S.low, [close] = S.[close],
    volume = S.volume, volatility_1h = S.volatility_1h,
    created_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (timestamp, currency_pair, [open], high, low, [close], volume, volatility_1h)
    VALUES (S.timestamp, S.currency_pair, S.[open], S.high, S.low, S.[close],
            S.volume, S.volatility_1h);
"""

MERGE_NEWS_SQL = """
MERGE INTO cee_news_headlines AS T
USING (SELECT ? AS published_at, ? AS fetched_at, ? AS source,
              ? AS title, ? AS url, ? AS category,
              ? AS sentiment, ? AS is_surprising, ? AS raw_ai_response) AS S
ON T.url = S.url
WHEN MATCHED THEN UPDATE SET
    published_at = S.published_at, fetched_at = S.fetched_at,
    source = S.source, title = S.title,
    category = S.category, sentiment = S.sentiment,
    is_surprising = S.is_surprising, raw_ai_response = S.raw_ai_response,
    created_at = GETDATE()
WHEN NOT MATCHED THEN INSERT
    (published_at, fetched_at, source, title, url,
     category, sentiment, is_surprising, raw_ai_response)
    VALUES (S.published_at, S.fetched_at, S.source, S.title, S.url,
            S.category, S.sentiment, S.is_surprising, S.raw_ai_response);
"""

# Column order must match MERGE parameter order
FX_SQL_COLUMNS = [
    "timestamp", "currency_pair", "open", "high", "low", "close",
    "volume", "volatility_1h",
]

NEWS_SQL_COLUMNS = [
    "published_at", "fetched_at", "source", "title", "url",
    "category", "sentiment", "is_surprising", "raw_ai_response",
]
