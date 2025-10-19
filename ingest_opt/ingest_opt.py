import os, time, math
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import ccxt

load_dotenv()

PG = dict(
    host=os.getenv("PGHOST","localhost"),
    port=int(os.getenv("PGPORT","5433")),
    dbname=os.getenv("PGDATABASE","csb_opt"),
    user=os.getenv("PGUSER","postgres"),
    password=os.getenv("PGPASSWORD","postgres"),
)
EXCHANGE_ID = os.getenv("EXCHANGE","binance").lower()
SYMBOL      = os.getenv("SYMBOL","BTC/USDT")
TIMEFRAME   = os.getenv("TIMEFRAME","1m")
SEED_MIN    = int(os.getenv("SEED_MINUTES","180"))

# 1) Connect DB
conn = psycopg2.connect(**PG)
conn.autocommit = True
cur = conn.cursor()

# 2) Find last ts we have
cur.execute("SELECT max(ts) FROM candles_1m WHERE symbol=%s;", (SYMBOL,))
row = cur.fetchone()
last_ts = row[0]  # timestamptz or None

# 3) CCXT client
ex_class = getattr(ccxt, EXCHANGE_ID)
ex = ex_class({"enableRateLimit": True})

# 4) Compute since ms
if last_ts is None:
    since_dt = datetime.now(timezone.utc) - timedelta(minutes=SEED_MIN)
else:
    # start next minute after last_ts
    since_dt = (last_ts + timedelta(minutes=1)).astimezone(timezone.utc)

since_ms = int(since_dt.timestamp() * 1000)

# 5) Fetch in loops (ccxt gives [ms, open, high, low, close, vol])
batch = []
limit = 1000
while True:
    ohlcv = ex.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, since=since_ms, limit=limit)
    if not ohlcv:
        break
    # prepare rows
    for ms, o, h, l, c, v in ohlcv:
        ts = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
        batch.append((ts, SYMBOL, float(o), float(h), float(l), float(c), float(v)))
    # move since_ms forward (last ms + 1)
    since_ms = ohlcv[-1][0] + 1
    # Stop if we are close to now (no infinite loop)
    if len(ohlcv) < limit:
        break
    # be nice to API
    time.sleep(ex.rateLimit/1000)

if not batch:
    print("No new candles to ingest.")
else:
    sql = """
    INSERT INTO candles_1m (ts, symbol, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (symbol, ts) DO UPDATE
    SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
        close=EXCLUDED.close, volume=EXCLUDED.volume;
    """
    execute_values(cur, sql, batch, page_size=1000)
    print(f"Ingested/Upserted: {len(batch)} rows for {SYMBOL}")

# 6) Quick verify: last 5 closed candles
cur.execute("""
    SELECT ts, open, high, low, close, volume
    FROM candles_1m
    WHERE symbol=%s
    ORDER BY ts DESC
    LIMIT 5;
""", (SYMBOL,))
rows = cur.fetchall()
for r in rows[::-1]:
    print(r[0].isoformat(), r[1], r[2], r[3], r[4], r[5])

cur.close(); conn.close()
