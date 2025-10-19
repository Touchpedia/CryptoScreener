-- Create 3m & 5m continuous aggregates if missing
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_3m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('3 minutes', ts) AS bucket_ts,
  symbol,
  first(open, ts) AS open,
  max(high)       AS high,
  min(low)        AS low,
  last(close, ts) AS close,
  sum(volume)     AS volume
FROM candles_1m
GROUP BY 1,2
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS candles_5m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 minutes', ts) AS bucket_ts,
  symbol,
  first(open, ts) AS open,
  max(high)       AS high,
  min(low)        AS low,
  last(close, ts) AS close,
  sum(volume)     AS volume
FROM candles_1m
GROUP BY 1,2
WITH NO DATA;

-- Add/ensure policies via safe DO blocks
DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy(
    'candles_3m',
    start_offset => INTERVAL '2 days',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
  );
EXCEPTION WHEN OTHERS THEN
  NULL;
END$$;

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy(
    'candles_5m',
    start_offset => INTERVAL '2 days',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
  );
EXCEPTION WHEN OTHERS THEN
  NULL;
END$$;
