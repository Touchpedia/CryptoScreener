import time, ccxt, psycopg2, itertools

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
SYMBOLS = ["BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT","ADA/USDT","DOGE/USDT","AVAX/USDT","LINK/USDT","TRX/USDT",
           "TON/USDT","DOT/USDT","MATIC/USDT","SUI/USDT","APT/USDT","NEAR/USDT","UNI/USDT","XLM/USDT","ATOM/USDT","FTM/USDT"]
TIMEFRAMES = ["1m","5m"]  # 2 TFs
BATCH = 10                 # har tick me 10 symbols fetch (rate-limit friendly)

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
    ex = ccxt.binance({"enableRateLimit": True})
    ex.load_markets()
    print("minute_daemon_multi started. CTRL+C to stop.")
    # round-robin chunks to stay under limits
    idx = 0
    while True:
        # minute boundary align (1m TF ke liye)
        sleep_s = 60 - (time.time() % 60)
        time.sleep(sleep_s)

        # pick batch of symbols
        batch_syms = SYMBOLS[idx:idx+BATCH]
        if not batch_syms:
            idx = 0
            batch_syms = SYMBOLS[idx:idx+BATCH]
        idx += BATCH

        inserted = 0
        for sym in batch_syms:
            for tf in TIMEFRAMES:
                try:
                    rows = ex.fetch_ohlcv(sym, timeframe=tf, limit=1)
                except Exception:
                    rows = []
                inserted += insert_one(sym, tf, rows)
        print(f"Inserted {inserted} new candles this minute across {len(batch_syms)} symbols x {len(TIMEFRAMES)} TFs")
