import time, ccxt, psycopg2

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
SYMBOLS = ["BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT"]
TF = "1m"

def insert_latest(rows_map):
    added = 0
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            for sym, rows in rows_map.items():
                if not rows: continue
                ts,o,h,l,c,v = rows[-1]
                cur.execute("""
                    INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol,timeframe,ts) DO NOTHING
                """, (sym, TF, int(ts), o,h,l,c,v))
                added += cur.rowcount
        conn.commit()
    return added

if __name__ == "__main__":
    ex = ccxt.binance({"enableRateLimit": True})
    ex.load_markets()
    print("minute_daemon started. CTRL+C to stop.")
    while True:
        # next minute boundary
        sleep_s = 60 - (time.time() % 60)
        time.sleep(sleep_s)
        data = {}
        for s in SYMBOLS:
            try:
                data[s] = ex.fetch_ohlcv(s, timeframe=TF, limit=1)
            except Exception:
                data[s] = []
        ins = insert_latest(data)
        print(f"Inserted {ins} new candles this minute")
