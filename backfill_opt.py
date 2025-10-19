import time, math, argparse, psycopg2, ccxt

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
# Default symbols (edit if needed)
SYMBOLS = ["BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT","ADA/USDT","DOGE/USDT","AVAX/USDT","LINK/USDT","TRX/USDT"]

STEP_SEC = {"1m":60, "3m":180, "5m":300, "15m":900, "1h":3600}

def now_ms():
    return int(time.time()*1000)

def ensure_table():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS candles (
              symbol text NOT NULL, timeframe text NOT NULL, ts bigint NOT NULL,
              open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL,
              close numeric NOT NULL, volume numeric NOT NULL,
              PRIMARY KEY(symbol,timeframe,ts)
            );""")
        conn.commit()

def max_ts(conn, sym, tf):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(ts),0) FROM candles WHERE symbol=%s AND timeframe=%s;", (sym, tf))
        return cur.fetchone()[0] or 0

def insert_rows(conn, sym, tf, rows):
    if not rows: return 0
    added = 0
    with conn.cursor() as cur:
        for ts,o,h,l,c,v in rows:
            cur.execute("""
                INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol,timeframe,ts) DO NOTHING
            """, (sym, tf, int(ts), o,h,l,c,v))
            added += cur.rowcount
    conn.commit()
    return added

def backfill_for_symbol(ex, sym, tf, years, limit=1000, sleep_s=0.12):
    step_ms = STEP_SEC[tf]*1000
    # end at last CLOSED bar
    now = now_ms()
    end_closed = (now // step_ms)*step_ms - 1

    lookback_ms = int(years * 365 * 24 * 3600 * 1000)
    window_start = end_closed - lookback_ms

    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        # resume from DB if present
        last = max_ts(conn, sym, tf)
        start_ms = max(window_start, (last + step_ms) if last > 0 else window_start)

        if start_ms > end_closed:
            print(f"[{sym} {tf}] already complete for requested window.")
            return 0

        inserted_total = 0
        since = start_ms
        while True:
            try:
                rows = ex.fetch_ohlcv(sym, timeframe=tf, since=since, limit=limit)
            except Exception as e:
                # mild backoff
                time.sleep(0.5)
                continue
            if not rows:
                break

            # clamp to end_closed and drop any in-progress
            rows = [r for r in rows if r[0] <= end_closed]
            if not rows:
                break

            inserted = insert_rows(conn, sym, tf, rows)
            inserted_total += inserted

            # advance since (avoid stuck)
            since = rows[-1][0] + 1
            if since > end_closed:
                break

            time.sleep(sleep_s)  # be nice to rate limits

        print(f"[{sym} {tf}] inserted {inserted_total} rows.")
        return inserted_total

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", required=True, choices=list(STEP_SEC.keys()), help="timeframe e.g. 1m/3m/5m")
    ap.add_argument("--years", required=True, type=float, help="years to backfill (e.g., 1, 3, 5)")
    ap.add_argument("--symbols", nargs="*", default=SYMBOLS, help="override symbol list")
    ap.add_argument("--limit", type=int, default=1000, help="fetch_ohlcv limit (default 1000)")
    args = ap.parse_args()

    ensure_table()
    ex = ccxt.binance({"enableRateLimit": True})
    ex.load_markets()

    total = 0
    for sym in args.symbols:
        total += backfill_for_symbol(ex, sym, args.tf, args.years, limit=args.limit)
    print(f"TOTAL inserted for {args.tf}: {total}")

if __name__ == "__main__":
    main()
