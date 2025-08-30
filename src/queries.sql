-- Count rows
SELECT count() FROM crypto.trades;

-- See latest 10 trades
SELECT * FROM crypto.trades ORDER BY ts DESC LIMIT 10;

-- Top 10 symbols by volume in last 10 minutes
SELECT symbol, sum(qty) AS volume, count() AS trades
FROM crypto.trades
PREWHERE ts >= now() - INTERVAL 10 MINUTE
GROUP BY symbol
ORDER BY volume DESC
LIMIT 10;

-- BTC trend over last hour (raw trades)
SELECT toStartOfMinute(ts) AS minute, avg(price) AS avg_price, sum(qty) AS volume
FROM crypto.trades
WHERE symbol = 'BTCUSDT' AND ts >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;

-- If MV exists: query candlestick aggregates
SELECT * FROM crypto.trades_1m
WHERE symbol = 'BTCUSDT' AND minute >= now() - INTERVAL 1 HOUR
ORDER BY minute;