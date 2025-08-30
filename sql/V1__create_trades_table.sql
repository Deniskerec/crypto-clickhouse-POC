CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.trades
(
    symbol         LowCardinality(String),
    trade_id       UInt64,
    price          Float64,
    qty            Float64,
    ts             DateTime,                 -- trade time (UTC)
    is_buyer_maker UInt8,
    ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, symbol, trade_id)
SETTINGS index_granularity = 8192;

-- Optional retention
ALTER TABLE crypto.trades
  MODIFY TTL ts + INTERVAL 90 DAY DELETE;