CREATE EXTENSION IF NOT EXISTS timescaledb;

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

SELECT create_hypertable('candles_1m','ts', if_not_exists => TRUE, migrate_data => TRUE);
CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol_ts_desc ON candles_1m(symbol, ts DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS candles_3m
WITH (timescaledb.continuous) AS
SELECT time_bucket('3 minutes', ts) AS bucket_ts, symbol,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume
FROM candles_1m GROUP BY 1,2 WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS candles_5m
WITH (timescaledb.continuous) AS
SELECT time_bucket('5 minutes', ts) AS bucket_ts, symbol,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume
FROM candles_1m GROUP BY 1,2 WITH NO DATA;

SELECT add_continuous_aggregate_policy('candles_3m',
  start_offset => INTERVAL '2 days', end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute') ON CONFLICT DO NOTHING;

SELECT add_continuous_aggregate_policy('candles_5m',
  start_offset => INTERVAL '2 days', end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute') ON CONFLICT DO NOTHING;
