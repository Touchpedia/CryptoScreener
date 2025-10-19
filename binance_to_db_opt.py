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
