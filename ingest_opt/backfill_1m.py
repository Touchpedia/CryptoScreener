import os, time, math, datetime as dt
import psycopg2
from psycopg2.extras import execute_values
from dateutil import parser as dtparser
from dotenv import load_dotenv
import ccxt

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

PG = dict(
    host=os.getenv("PGHOST", "127.0.0.1"),
    port=int(os.getenv("PGPORT", "5433")),
    dbname=os.getenv("PGDATABASE", "csb_opt"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "2715"),
)

EXCHANGE = os.getenv("EXCHANGE", "binance")
SYMBOL   = os.getenv("SYMBOL", "BTC/USDT")
TF       = "1m"

# Inputs (env > defaults)
START = os.getenv("BACKFILL_START", "")  # e.g. "2025-10-01T00:00:00Z"
END   = os.getenv("BACKFILL_END",   "")  # e.g. "2025-10-18T00:00:00Z"

def to_ms(dt_obj):  # aware -> ms
    return int(dt_obj.timestamp() * 1000)

def parse_to_utc_ms(s, default_now=False):
    if not s:
        if default_now:
            return to_ms(dt.datetime.now(dt.timezone.utc))
        return None
    d = dtparser.parse(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    else:
        d = d.astimezone(dt.timezone.utc)
    return to_ms(d)

start_ms = parse_to_utc_ms(START) or to_ms(dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=12))  # default: 12h
end_ms   = parse_to_utc_ms(END, default_now=True)

ex = getattr(ccxt, EXCHANGE)({"enableRateLimit": True})
# binance specific: convert symbol if needed (ccxt uses the same "BTC/USDT")
limit = 1000

insert_sql = """
INSERT INTO candles_1m (ts, symbol, open, high, low, close, volume)
VALUES %s
ON CONFLICT (symbol, ts) DO UPDATE
SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
    close=EXCLUDED.close, volume=EXCLUDED.volume;
"""

rows_total = 0
with psycopg2.connect(**PG) as conn:
    conn.autocommit = True
    cur = conn.cursor()
    since = start_ms
    while since <= end_ms:
        ohlcv = ex.fetch_ohlcv(SYMBOL, TF, since=since, limit=limit)
        if not ohlcv:
            break
        # Prepare batch
        batch = []
        for ts_ms, o, h, l, c, v in ohlcv:
            # ts stored as timestamptz (UTC); psycopg2 accepts Python datetime too, but we’ll send ISO
            ts_iso = dt.datetime.utcfromtimestamp(ts_ms/1000).replace(tzinfo=dt.timezone.utc).isoformat()
            batch.append((ts_iso, SYMBOL, float(o), float(h), float(l), float(c), float(v)))
        execute_values(cur, insert_sql, batch, page_size=1000)
        rows_total += len(batch)

        # Progress + advance `since`
        print(f"Upserted {len(batch)} rows: {dt.datetime.utcfromtimestamp(ohlcv[0][0]/1000)} -> {dt.datetime.utcfromtimestamp(ohlcv[-1][0]/1000)} UTC")
        # Next window: +1ms after last candle to avoid duplicate first row
        since = ohlcv[-1][0] + 60_000  # 1m step
        # polite rate limit
        time.sleep(max(0.001, ex.rateLimit/1000))
print(f"Backfill done. Total rows upserted: {rows_total} for {SYMBOL} ({TF}).")
