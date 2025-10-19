-- Idempotent schema for Crypto Signal Bot (csb_opt)
-- 1) Create database
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'csb_opt') THEN
    PERFORM dblink_connect('host=localhost port=5432 dbname=postgres user=postgres password=postgres');
  END IF;
END$$;

-- Using postgres DB context to create csb_opt if missing
-- (We will guard with IF NOT EXISTS via DO block below)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'csb_opt') THEN
    EXECUTE 'CREATE DATABASE csb_opt';
  END IF;
END$$;

-- 2) Inside csb_opt: extension + tables
\connect csb_opt

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1m candles (base)
CREATE TABLE IF NOT EXISTS candles_1m (
  ts      timestamptz NOT NULL,
  symbol  text        NOT NULL,
  open    double precision NOT NULL,
  high    double precision NOT NULL,
  low     double precision NOT NULL,
  close   double precision NOT NULL,
  volume  double precision NOT NULL,
  CONSTRAINT candles_1m_pk PRIMARY KEY (symbol, ts)
);

-- turn into hypertable (idempotent)
SELECT create_hypertable('candles_1m','ts', if_not_exists => TRUE, migrate_data => TRUE);

-- helpful index for recency scans (optional; PK already exists)
CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol_ts_desc ON candles_1m(symbol, ts DESC);

-- 3m aggregate (continuous)
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

-- 5m aggregate (continuous)
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

-- Refresh policies (auto)
SELECT add_continuous_aggregate_policy('candles_3m',
  start_offset => INTERVAL '2 days',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute')
ON CONFLICT DO NOTHING;

SELECT add_continuous_aggregate_policy('candles_5m',
  start_offset => INTERVAL '2 days',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute')
ON CONFLICT DO NOTHING;

-- sanity check
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('candles_1m','candles_3m','candles_5m')
ORDER BY table_name;
