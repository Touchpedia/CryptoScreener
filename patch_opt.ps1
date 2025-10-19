# idempotent patch: creates *_opt.py files and does a quick smoke test

# --- binance_to_db_opt.py ---
$binanceContent = @"
import time, datetime, psycopg2, ccxt

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
SYMBOL = "BTC/USDT"
TF = "1m"

def last_ts_from_db(symbol, tf):
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(ts),0) FROM candles WHERE symbol=%s AND timeframe=%s;", (symbol, tf))
            return cur.fetchone()[0] or 0

def trim_incomplete(rows, tf):
    secmap = {"1m":60, "5m":300, "15m":900, "1h":3600}
    sec = secmap[tf]
    now = int(time.time())
    current_bar_start = now - (now % sec)
    cutoff_ms = current_bar_start*1000 - 1
    return [r for r in rows if r[0] <= cutoff_ms]

def fetch_bulk(ex, symbol, tf, limit=1000):
    since = last_ts_from_db(symbol, tf)
    return ex.fetch_ohlcv(symbol, timeframe=tf, limit=limit, since=(since if since>0 else None))

def ensure_table():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                  symbol text NOT NULL, timeframe text NOT NULL, ts bigint NOT NULL,
                  open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL,
                  close numeric NOT NULL, volume numeric NOT NULL,
                  PRIMARY KEY(symbol,timeframe,ts)
                );
            """)
        conn.commit()

def insert_rows(rows):
    added = 0
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            for ts,o,h,l,c,v in rows:
                cur.execute("""
                    INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol,timeframe,ts) DO NOTHING
                """, (SYMBOL, TF, int(ts), o,h,l,c,v))
                added += cur.rowcount
        conn.commit()
    return added

if __name__ == "__main__":
    ex = ccxt.binance({"enableRateLimit": True}); ex.load_markets()
    ensure_table()
    data = fetch_bulk(ex, SYMBOL, TF, limit=1000)
    data = trim_incomplete(data, TF)
    ins = insert_rows(data)
    print(f"Inserted {ins}/{len(data)} rows for {SYMBOL} {TF}")
    # show latest
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT ts, open, high, low, close, volume FROM candles
                           WHERE symbol=%s AND timeframe=%s ORDER BY ts DESC LIMIT 1;""", (SYMBOL, TF))
            row = cur.fetchone()
    if row:
        iso = datetime.datetime.utcfromtimestamp(row[0]/1000).isoformat() + "Z"
        print(f"Latest candle: {iso} OHLVC= {row[1]} {row[2]} {row[3]} {row[4]} {row[5]}")
"@
Set-Content -Path .\binance_to_db_opt.py -Value $binanceContent

# --- multi_symbols_to_db_opt.py ---
$multiContent = @"
import time, random, psycopg2, ccxt
from concurrent.futures import ThreadPoolExecutor, as_completed

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
SYMBOLS = ["BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT","ADA/USDT","DOGE/USDT","AVAX/USDT","LINK/USDT","TRX/USDT"]
TF = "1m"
MAX_CONCURRENCY = 12
LIMIT = 500

def ensure_table():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                  symbol text NOT NULL, timeframe text NOT NULL, ts bigint NOT NULL,
                  open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL,
                  close numeric NOT NULL, volume numeric NOT NULL,
                  PRIMARY KEY(symbol,timeframe,ts)
                );
            """)
        conn.commit()

def last_ts_from_db(symbol, tf):
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(ts),0) FROM candles WHERE symbol=%s AND timeframe=%s;", (symbol, tf))
            return cur.fetchone()[0] or 0

def trim_incomplete(rows, tf):
    secmap = {"1m":60, "5m":300, "15m":900, "1h":3600}
    sec = secmap[tf]
    now = int(time.time())
    current_bar_start = now - (now % sec)
    cutoff_ms = current_bar_start*1000 - 1
    return [r for r in rows if r[0] <= cutoff_ms]

def fetch_with_retry(ex, sym, tf, limit, since):
    delay = 0.5
    for _ in range(4):
        try:
            return ex.fetch_ohlcv(sym, timeframe=tf, limit=limit, since=since)
        except Exception:
            time.sleep(delay + random.uniform(0, 0.2))
            delay = min(delay*2, 8)
    return []

def fetch_one(ex, sym, tf, limit=LIMIT):
    since = last_ts_from_db(sym, tf)
    rows = fetch_with_retry(ex, sym, tf, limit, since if since>0 else None)
    return sym, trim_incomplete(rows, tf)

def insert_all(data):
    added = 0
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            for sym, rows in data.items():
                for ts,o,h,l,c,v in rows:
                    cur.execute("""
                        INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (symbol,timeframe,ts) DO NOTHING
                    """, (sym, TF, int(ts), o,h,l,c,v))
                    added += cur.rowcount
        conn.commit()
    return added

if __name__ == "__main__":
    ex = ccxt.binance({"enableRateLimit": True}); ex.load_markets()
    ensure_table()
    data = {}
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as pool:
        futs = [pool.submit(fetch_one, ex, s, TF) for s in SYMBOLS]
        for f in as_completed(futs):
            sym, rows = f.result()
            data[sym] = rows
    ins = insert_all(data)
    print(f"Inserted {ins} rows across {len(SYMBOLS)} symbols ({TF})")
"@
Set-Content -Path .\multi_symbols_to_db_opt.py -Value $multiContent

# --- minute_daemon_multi_opt.py ---
$daemonContent = @"
import time, ccxt, psycopg2

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
SYMBOLS = ["BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT",
           "ADA/USDT","DOGE/USDT","AVAX/USDT","LINK/USDT","TRX/USDT"]
TIMEFRAMES = ["1m","5m"]

def ensure_table():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                  symbol text NOT NULL, timeframe text NOT NULL, ts bigint NOT NULL,
                  open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL,
                  close numeric NOT NULL, volume numeric NOT NULL,
                  PRIMARY KEY(symbol,timeframe,ts)
                );
            """)
        conn.commit()

def insert_one(sym, tf, row):
    if not row: return 0
    ts,o,h,l,c,v = row[-1]
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol,timeframe,ts) DO NOTHING
            """, (sym, tf, int(ts), o,h,l,c,v))
        conn.commit()
        return cur.rowcount

if __name__ == "__main__":
    ex = ccxt.binance({"enableRateLimit": True}); ex.load_markets()
    ensure_table()
    print("minute_daemon_multi_opt started. CTRL+C to stop.")
    idx = 0; BATCH = 10
    while True:
        sleep_s = 60 - (time.time() % 60)
        time.sleep(sleep_s)
        now_min = int(time.time() // 60)
        do_5m = (now_min % 5 == 0)
        tfs = ["1m"] + (["5m"] if do_5m else [])

        batch_syms = SYMBOLS[idx:idx+BATCH]
        if not batch_syms:
            idx = 0
            batch_syms = SYMBOLS[idx:idx+BATCH]
        idx += BATCH

        inserted = 0
        for sym in batch_syms:
            for tf in tfs:
                try:
                    rows = ex.fetch_ohlcv(sym, timeframe=tf, limit=1)
                except Exception:
                    rows = []
                inserted += insert_one(sym, tf, rows)
        print(f"Inserted {inserted} new candles this minute across {len(batch_syms)} syms x {len(tfs)} TFs")
"@
Set-Content -Path .\minute_daemon_multi_opt.py -Value $daemonContent

# --- quick smoke test (safe/idempotent) ---
python .\binance_to_db_opt.py
