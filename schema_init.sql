-- safe create
CREATE TABLE IF NOT EXISTS staging_candles (
  exchange   TEXT,
  symbol     TEXT,
  timeframe  TEXT,
  ts         TIMESTAMPTZ,
  open       DOUBLE PRECISION,
  high       DOUBLE PRECISION,
  low        DOUBLE PRECISION,
  close      DOUBLE PRECISION,
  volume     DOUBLE PRECISION,
  qvol       DOUBLE PRECISION,
  buy_quote  DOUBLE PRECISION,
  sell_quote DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS candles (
  exchange   TEXT NOT NULL,
  symbol     TEXT NOT NULL,
  timeframe  TEXT NOT NULL,
  ts         TIMESTAMPTZ NOT NULL,
  open       DOUBLE PRECISION,
  high       DOUBLE PRECISION,
  low        DOUBLE PRECISION,
  close      DOUBLE PRECISION,
  volume     DOUBLE PRECISION,
  qvol       DOUBLE PRECISION,
  buy_quote  DOUBLE PRECISION,
  sell_quote DOUBLE PRECISION,
  CONSTRAINT candles_uk UNIQUE (exchange, symbol, timeframe, ts)
);

-- helpful indexes
CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_ts
  ON candles(symbol, timeframe, ts DESC);

CREATE INDEX IF NOT EXISTS idx_candles_ts
  ON candles(ts DESC);