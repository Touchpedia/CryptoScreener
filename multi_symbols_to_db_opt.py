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
