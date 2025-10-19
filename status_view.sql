-- Status for 1m BTC/USDT
CREATE OR REPLACE VIEW ingest_status_1m AS
SELECT
  'binance'::text AS exchange,
  'BTC/USDT'::text AS symbol,
  '1m'::text AS timeframe,
  max(ts)                    AS latest_ts,
  EXTRACT(EPOCH FROM now() - max(ts))::int AS lag_seconds,
  COUNT(*) FILTER (WHERE ts >= now() - interval '60 minutes') AS rows_last_60m
FROM candles_1m
WHERE symbol = 'BTC/USDT';

-- Optional: last 5 candles (for a tiny “recent” widget)
CREATE OR REPLACE VIEW latest_5_candles AS
SELECT ts, open, high, low, close, volume
FROM candles_1m
WHERE symbol = 'BTC/USDT'
ORDER BY ts DESC
LIMIT 5;
