import time, argparse, psycopg2, ccxt
from datetime import datetime

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"

# required periods
PLAN = {"1m": 1, "3m": 3, "5m": 5}  # years per timeframe
STEP_SEC = {"1m":60, "3m":180, "5m":300, "15m":900, "1h":3600}

def now_ms(): return int(time.time()*1000)

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

def backfill_tf(ex, sym, tf, years, limit=1000, sleep_s=0.12):
    step_ms = STEP_SEC[tf]*1000
    end_closed = (now_ms() // step_ms)*step_ms - 1
    lookback_ms = int(years * 365 * 24 * 3600 * 1000)
    window_start = end_closed - lookback_ms

    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        last = max_ts(conn, sym, tf)
        start_ms = max(window_start, (last + step_ms) if last > 0 else window_start)
        if start_ms > end_closed:
            print(f"[{sym} {tf}] already complete.")
            return 0

        total = 0
        since = start_ms
        while True:
            try:
                rows = ex.fetch_ohlcv(sym, timeframe=tf, since=since, limit=limit)
            except Exception:
                time.sleep(0.6); continue
            if not rows: break

            rows = [r for r in rows if r[0] <= end_closed]  # trim incomplete
            if not rows: break

            total += insert_rows(conn, sym, tf, rows)
            since = rows[-1][0] + 1
            if since > end_closed: break
            time.sleep(sleep_s)
        print(f"[{sym}] {tf} done: +{total} rows")
        return total

def pick_top_symbols(ex, top_n=300, quote="USDT"):
    # Try to rank by quoteVolume; fallback to baseVolume/last if needed.
    syms = []
    try:
        tickers = ex.fetch_tickers()
        for s, t in tickers.items():
            if ("/" + quote) in s and "UP/" not in s and "DOWN/" not in s and "BULL/" not in s and "BEAR/" not in s:
                qv = t.get("quoteVolume") or (t.get("baseVolume") or 0) * (t.get("last") or 0)
                syms.append((qv or 0, s))
        syms.sort(reverse=True)
        return [s for _, s in syms[:top_n]]
    except Exception:
        # Fallback: use loaded markets filter
        return [m for m in ex.markets if m.endswith("/" + quote)][:top_n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*", help="symbols list; omit to use --top")
    ap.add_argument("--top", type=int, default=0, help="pick top N USDT symbols by volume")
    ap.add_argument("--limit", type=int, default=1000, help="fetch_ohlcv limit")
    args = ap.parse_args()

    ensure_table()
    ex = ccxt.binance({"enableRateLimit": True})
    ex.load_markets()

    if args.symbols:
        symbols = args.symbols
    elif args.top and args.top > 0:
        symbols = pick_top_symbols(ex, args.top, "USDT")
    else:
        symbols = ["BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT"]

    start_ts = datetime.utcnow().isoformat()+"Z"
    print(f"Starting backfill_by_symbol at {start_ts} | symbols={len(symbols)}")

    for sym in symbols:
        print(f"\n=== {sym} ===")
        for tf, yrs in [("1m", PLAN["1m"]), ("3m", PLAN["3m"]), ("5m", PLAN["5m"])]:
            backfill_tf(ex, sym, tf, yrs, limit=args.limit)

    print("All done.")

if __name__ == "__main__":
    main()
