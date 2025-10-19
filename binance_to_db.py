import time, datetime, psycopg2
import ccxt

HOST, PORT, USER, PASS = "127.0.0.1", 5434, "postgres", "2715"
DB   = "candles"

SYMBOL = "BTC/USDT"
TF = "1m"
LIMIT = 30   # sirf last 30 candles (halka test)

def fetch_candles():
    ex = ccxt.binance({"enableRateLimit": True})
    ex.load_markets()
    rows = ex.fetch_ohlcv(SYMBOL, timeframe=TF, limit=LIMIT)
    return rows  # [ [ts, o,h,l,c,v], ... ]

def ensure_table():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                  symbol    text    NOT NULL,
                  timeframe text    NOT NULL,
                  ts        bigint  NOT NULL,
                  open      numeric NOT NULL,
                  high      numeric NOT NULL,
                  low       numeric NOT NULL,
                  close     numeric NOT NULL,
                  volume    numeric NOT NULL,
                  PRIMARY KEY(symbol, timeframe, ts)
                );
            """)
        conn.commit()

def insert_rows(rows):
    added = 0
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            for ts,o,h,l,c,v in rows:
                cur.execute("""
                    INSERT INTO candles (symbol, timeframe, ts, open, high, low, close, volume)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol, timeframe, ts) DO NOTHING
                """, ("BTC/USDT", TF, int(ts), o,h,l,c,v))
                added += cur.rowcount
        conn.commit()
    return added

def show_latest():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM candles WHERE symbol=%s AND timeframe=%s;", ("BTC/USDT", TF))
            cnt = cur.fetchone()[0]
            cur.execute("""
                SELECT ts, open, high, low, close, volume
                FROM candles
                WHERE symbol=%s AND timeframe=%s
                ORDER BY ts DESC LIMIT 1;
            """, ("BTC/USDT", TF))
            row = cur.fetchone()
    iso = None
    if row:
        iso = datetime.datetime.utcfromtimestamp(row[0]/1000).isoformat() + "Z"
    return cnt, iso, row

if __name__ == "__main__":
    ensure_table()
    data = fetch_candles()
    ins = insert_rows(data)
    cnt, iso, last = show_latest()
    print(f"Inserted {ins}/{len(data)} rows for {SYMBOL} {TF}")
    print(f"Total BTC/USDT 1m rows now: {cnt}")
    if iso:
        print(f"Latest candle: {iso} OHLVC= {last[1]} {last[2]} {last[3]} {last[4]} {last[5]}")
