SELECT exchange, symbol, timeframe, latest_ts, lag_seconds, rows_last_60m
FROM ingest_status_1m;

SELECT ts, open, high, low, close, volume
FROM latest_5_candles
ORDER BY ts DESC;
