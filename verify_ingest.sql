SELECT max(ts) AS latest_ts
FROM candles_1m
WHERE symbol = 'BTC/USDT';

SELECT count(*) AS rows_last_60m
FROM candles_1m
WHERE symbol = 'BTC/USDT'
  AND ts >= now() - interval '60 minutes';

SELECT ts, open, high, low, close, volume
FROM candles_1m
WHERE symbol = 'BTC/USDT'
ORDER BY ts DESC
LIMIT 5;
