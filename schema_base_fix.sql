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
