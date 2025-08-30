CREATE TABLE IF NOT EXISTS crypto.trades_1m
(
  minute  DateTime,
  symbol  LowCardinality(String),
  open    Float64,
  high    Float64,
  low     Float64,
  close   Float64,
  volume  Float64,
  trades  UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, symbol);

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.trades_to_1m
TO crypto.trades_1m
AS
SELECT
  toStartOfMinute(ts) AS minute,
  symbol,
  argMin(price, ts) AS open,
  max(price)        AS high,
  min(price)        AS low,
  anyLast(price)    AS close,
  sum(qty)          AS volume,
  count()           AS trades
FROM crypto.trades
GROUP BY minute, symbol;