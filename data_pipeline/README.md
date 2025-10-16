# Binance TimescaleDB Candle Pipeline

## Overview
- `ingest.py` performs the initial backfill for the top-N Binance USDT spot pairs (default **300**, stablecoin bases auto-excluded) and stores 1m/3m/5m candles in PostgreSQL + TimescaleDB using parallel workers (default `MAX_THREADS=10`) capped at ~20 requests/sec for Binance safety.
- `updater.py` keeps the dataset fresh: it pulls the latest candles, detects gaps, backfills via the validator helpers, and applies rolling-window retention.
- `validator.py` can be run ad-hoc to inspect recent history per symbol/timeframe and optionally refill missing segments.
- Data lands in a Timescale hypertable `candles` keyed by `(exchange, symbol, timeframe, ts)`; an auxiliary table `ingestion_state` tracks the latest timestamp processed per stream.

## Prerequisites
1. Provision a PostgreSQL instance with the TimescaleDB extension.
2. Create the target database (default `candles`) and ensure the connecting role has privileges to create extensions.
3. Export the connection settings (or place them in a `.env` file at the project root; they are loaded automatically):
   ```
   export DB_HOST=localhost
   export DB_PORT=5432
   export DB_USER=postgres
   export DB_PASS=secret
   export DB_NAME=candles
   ```

Timescale retention (`90 days`) and hypertable creation are applied automatically the first time the pipeline connects.

## Environment Variables
```
CANDLE_PIPE_TOP_SYMBOLS=300              # Binance pairs to archive
CANDLE_PIPE_TIMEFRAMES=1m,3m,5m          # Base storage intervals
CANDLE_PIPE_BATCH_SIZE=500               # Per-request candle limit
CANDLE_PIPE_COOLDOWN_SEC=0.3             # Delay between requests
CANDLE_PIPE_RETRIES=3                    # Backoff attempts for CCXT calls
MAX_THREADS=10                           # Parallel ingestion workers
CANDLE_PIPE_QUOTE=USDT                   # Quote asset filter
CANDLE_PIPE_EXCHANGE=binance             # Exchange identifier
CANDLE_PIPE_ENV_FILE=.env                # Optional override for env file path
```

## Initial Backfill
```bash
# Pull one-year (1m), three-year (3m), and five-year (5m) history for top 300 pairs
python3 -m data_pipeline.ingest

# Restrict to a subset during testing
python3 -m data_pipeline.ingest --symbols BTC/USDT ETH/USDT --timeframes 1m 5m
```

## Incremental Updates
```bash
# Update the 1-minute stream with a 60 minute overlap and default retention
python3 -m data_pipeline.updater --interval 1m --lookback 60

# Update the 5-minute stream while keeping only the most recent 5000 candles per pair
python3 -m data_pipeline.updater --interval 5m --retain 5000
```

Each updater run writes structured logs to `logs/update.log`. The Streamlit UI surfaces these updates and can trigger the updater in the background at 1-minute or 5-minute cadences.

## Gap Validation
```bash
# Report gaps (latest 10k rows) without fixing them
python3 -m data_pipeline.validator --no-backfill

# Audit only BTC/USDT on 1m/5m and backfill automatically
python3 -m data_pipeline.validator --symbols BTC/USDT --timeframes 1m 5m
```

## Scheduling
A simple cron setup for continuous sync:
```
*/5 * * * * cd /path/to/project && /usr/bin/python3 -m data_pipeline.updater --interval 1m --lookback 30 >> logs/update.log 2>&1
*/15 * * * * cd /path/to/project && /usr/bin/python3 -m data_pipeline.updater --interval 5m --lookback 60 >> logs/update.log 2>&1
```

## Consumption Notes
- Downstream analytics (Streamlit screener, backtester, ML feature jobs) should source candles from TimescaleDB via SQL queries. The Streamlit app already does this through a pooled connection.
- Higher timeframes (15m, 1h, 4h, 1d) are derived on-demand from the stored 1m/3m/5m base series.
- When exporting data for modeling, prefer chunked queries or Timescale continuous aggregates to keep memory usage in check.
