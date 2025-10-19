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
