# -*- coding: utf-8 -*-
import time, argparse, threading, psycopg2, ccxt
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
PLAN_YEARS = {"1m": 1, "3m": 3, "5m": 5}
STEP_SEC = {"1m":60, "3m":180, "5m":300}

def ensure_tables():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS candles (
              symbol text NOT NULL, timeframe text NOT NULL, ts bigint NOT NULL,
              open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL,
              close numeric NOT NULL, volume numeric NOT NULL,
              PRIMARY KEY(symbol,timeframe,ts)
            );""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ingest_rate_wc (
              sec_epoch bigint PRIMARY KEY,
              cnt bigint DEFAULT 0
            );""")
            cur.execute("""
            CREATE OR REPLACE FUNCTION upd_ingest_rate_wc() RETURNS trigger AS $$
            DECLARE s BIGINT := FLOOR(EXTRACT(EPOCH FROM clock_timestamp())); BEGIN
              INSERT INTO ingest_rate_wc(sec_epoch,cnt) VALUES (s,1)
              ON CONFLICT (sec_epoch) DO UPDATE SET cnt = ingest_rate_wc.cnt + 1;
              RETURN NEW;
            END; $$ LANGUAGE plpgsql;""")
            cur.execute("DROP TRIGGER IF EXISTS trg_upd_ingest_rate_wc ON candles;")
            cur.execute("""
            CREATE TRIGGER trg_upd_ingest_rate_wc
            AFTER INSERT ON candles
            FOR EACH ROW EXECUTE FUNCTION upd_ingest_rate_wc();""")
        conn.commit()

def now_ms(): return int(time.time()*1000)

def max_ts(conn, sym, tf):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(ts),0) FROM candles WHERE symbol=%s AND timeframe=%s;", (sym, tf))
        return cur.fetchone()[0] or 0

def bulk_insert(conn, sym, tf, rows):
    if not rows: return 0
    tpl = [(sym, tf, int(ts), o, h, l, c, v) for ts,o,h,l,c,v in rows]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
            VALUES %s
            ON CONFLICT (symbol,timeframe,ts) DO NOTHING
        """, tpl, page_size=1000)
    conn.commit()
    return len(tpl)

def backfill_tf(ex, conn, sym, tf, years, limit=1000, base_sleep=0.05):
    step_ms = STEP_SEC[tf]*1000
    end_closed = (now_ms() // step_ms)*step_ms - 1
    lookback_ms = int(years*365*24*3600*1000)
    window_start = end_closed - lookback_ms
    last = max_ts(conn, sym, tf)
    since = max(window_start, (last + step_ms) if last>0 else window_start)
    if since > end_closed:
        print(f"[skip] {sym} {tf} already complete"); return 0

    inserted_total, delay = 0, base_sleep
    while True:
        try:
            rows = ex.fetch_ohlcv(sym, timeframe=tf, since=since, limit=limit)
        except Exception as e:
            time.sleep(delay); delay = min(delay*2, 2.0); continue
        if not rows: break

        rows = [r for r in rows if r[0] <= end_closed]
        if not rows: break

        added = bulk_insert(conn, sym, tf, rows)
        inserted_total += added
        since = rows[-1][0] + 1
        # progress line after each batch
        print(f"[prog] {sym} {tf} +{added} (total {inserted_total})")
        if since > end_closed: break
        time.sleep(base_sleep)
    print(f"[done-tf] {sym} {tf}: +{inserted_total}")
    return inserted_total

def process_symbol(sym, limit):
    print(f"[start] {sym}")
    ex = ccxt.binance({
        "enableRateLimit": True,
        "timeout": 20000,  # 20s
        "options": {"adjustForTimeDifference": True}
    })
    ex.load_markets()
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        total = 0
        for tf in ("1m","3m","5m"):
            total += backfill_tf(ex, conn, sym, tf, PLAN_YEARS[tf], limit=limit)
    print(f"[done] {sym}: +{total} rows")
    return sym, total

def pick_top_symbols(top_n=300, quote="USDT"):
    ex = ccxt.binance({"enableRateLimit": True, "timeout": 20000})
    ex.load_markets()
    syms = []
    try:
        tickers = ex.fetch_tickers()
        for s,t in tickers.items():
            if ("/"+quote) in s and "UP/" not in s and "DOWN/" not in s and "BULL/" not in s and "BEAR/" not in s:
                qv = t.get("quoteVolume") or (t.get("baseVolume") or 0) * (t.get("last") or 0)
                syms.append((qv or 0, s))
        syms.sort(reverse=True)
        return [s for _,s in syms[:top_n]]
    except Exception:
        return [m for m in ex.markets if m.endswith("/"+quote)][:top_n]

def wall_clock_rate_10s():
    try:
        with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                WITH now_s AS (SELECT FLOOR(EXTRACT(EPOCH FROM NOW()))::bigint AS s)
                SELECT COALESCE(SUM(cnt),0)
                FROM ingest_rate_wc, now_s
                WHERE sec_epoch BETWEEN (now_s.s - 9) AND now_s.s;
                """)
                last10 = cur.fetchone()[0] or 0
        return round(last10/10.0, 2)
    except Exception:
        return 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*", help="explicit symbols")
    ap.add_argument("--top", type=int, default=300, help="top N USDT pairs by volume")
    ap.add_argument("--workers", type=int, default=8, help="parallel symbols")
    ap.add_argument("--limit", type=int, default=1000, help="fetch_ohlcv limit")
    args = ap.parse_args()

    ensure_tables()
    symbols = args.symbols if args.symbols else pick_top_symbols(args.top)

    total_symbols = len(symbols)
    print(f"Backfill concurrent | symbols={total_symbols} workers={args.workers} limit={args.limit}")

    stop = threading.Event()
    progress = {"done": 0}

    def heartbeat():
        while not stop.is_set():
            cps = wall_clock_rate_10s()
            print(f"[hb] {progress['done']}/{total_symbols} symbols done | ~{cps} cps")
            time.sleep(5)

    t = threading.Thread(target=heartbeat, daemon=True); t.start()

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = [pool.submit(process_symbol, s, args.limit) for s in symbols]
        for f in as_completed(futs):
            sym, inserted = f.result()
            results.append((sym, inserted))
            progress["done"] += 1

    stop.set()
    total = sum(x[1] for x in results)
    print(f"ALL DONE. total_inserted={total}")

if __name__ == "__main__":
    main()
