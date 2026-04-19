-- ============================================================
--  TRADER.IO — ClickHouse Schema
--  Engine choices:
--    trades/candles: ReplacingMergeTree for idempotent inserts
--    indicators:     MergeTree (always append, query latest)
--    alerts:         MergeTree (append-only log)
-- ============================================================

CREATE DATABASE IF NOT EXISTS trader_io;

USE trader_io;

-- ─────────────────────────────────────────────
--  RAW TRADES
--  Partitioned by month for efficient range scans.
--  ReplacingMergeTree deduplicates on (symbol, trade_id).
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trades
(
    symbol          LowCardinality(String),
    trade_id        String,
    price           Float64,
    quantity        Float64,
    is_buyer_maker  UInt8,
    source          LowCardinality(String),
    event_time      Int64,
    insert_time     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(toDateTime(event_time / 1000))
ORDER BY (symbol, trade_id)
TTL toDateTime(event_time / 1000) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────
--  1-MINUTE CANDLES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candles_1m
(
    symbol          LowCardinality(String),
    interval        LowCardinality(String),
    open_time       Int64,
    close_time      Int64,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    trade_count     UInt32,
    vwap            Nullable(Float64),
    insert_time     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(toDateTime(open_time / 1000))
ORDER BY (symbol, open_time)
TTL toDateTime(open_time / 1000) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────
--  5-MINUTE CANDLES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candles_5m
(
    symbol          LowCardinality(String),
    interval        LowCardinality(String),
    open_time       Int64,
    close_time      Int64,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    trade_count     UInt32,
    vwap            Nullable(Float64),
    insert_time     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(toDateTime(open_time / 1000))
ORDER BY (symbol, open_time)
TTL toDateTime(open_time / 1000) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────
--  15-MINUTE CANDLES (kept longer for analysis)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candles_15m
(
    symbol          LowCardinality(String),
    interval        LowCardinality(String),
    open_time       Int64,
    close_time      Int64,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    trade_count     UInt32,
    vwap            Nullable(Float64),
    insert_time     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(toDateTime(open_time / 1000))
ORDER BY (symbol, open_time)
TTL toDateTime(open_time / 1000) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────
--  INDICATORS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS indicators
(
    symbol          LowCardinality(String),
    event_time      Int64,
    sma_7           Nullable(Float64),
    sma_14          Nullable(Float64),
    sma_50          Nullable(Float64),
    ema_12          Nullable(Float64),
    ema_26          Nullable(Float64),
    rsi_14          Nullable(Float64),
    macd            Nullable(Float64),
    macd_signal     Nullable(Float64),
    macd_histogram  Nullable(Float64),
    insert_time     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(event_time / 1000))
ORDER BY (symbol, event_time)
TTL toDateTime(event_time / 1000) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────
--  ALERTS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts
(
    id              UUID DEFAULT generateUUIDv4(),
    symbol          LowCardinality(String),
    alert_type      LowCardinality(String),
    message         String,
    price           Float64,
    indicator_value Nullable(Float64),
    triggered_at    Int64,
    notified        UInt8 DEFAULT 0,
    insert_time     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(triggered_at / 1000))
ORDER BY (symbol, triggered_at)
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────
--  DAILY AGGREGATIONS (populated by Airflow)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_ohlcv
(
    symbol          LowCardinality(String),
    date            Date,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    trade_count     UInt32,
    avg_price       Float64,
    price_change    Float64,
    price_change_pct Float64,
    insert_time     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;


-- ─────────────────────────────────────────────
--  MATERIALIZED VIEW: latest price per symbol
--  Used by API /prices/latest endpoint
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS latest_prices_mv_target
(
    symbol      LowCardinality(String),
    price       Float64,
    event_time  Int64
)
ENGINE = ReplacingMergeTree(event_time)
ORDER BY (symbol);

CREATE MATERIALIZED VIEW IF NOT EXISTS latest_prices_mv
TO latest_prices_mv_target
AS
SELECT symbol, price, event_time
FROM trades;
