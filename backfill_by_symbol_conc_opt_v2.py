# -*- coding: utf-8 -*-
import time, argparse, threading, psycopg2, ccxt, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values

def bump_rate_wc(conn, n):
    if n <= 0: 
        return
    with conn.cursor() as cur:
        cur.execute("""
        WITH s AS (SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::bigint AS sec)
        INSERT INTO ingest_rate_wc(sec_epoch,cnt)
        SELECT s.sec, %s FROM s
        ON CONFLICT (sec_epoch) DO UPDATE SET cnt = ingest_rate_wc.cnt + EXCLUDED.cnt;
        """, (int(n),))
    conn.commit()


HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
PLAN_YEARS = {"1m": 1, "3m": 3, "5m": 5}
STEP_SEC = {"1m":60, "3m":180, "5m":300}

def log(msg): print(msg, flush=True)
from threading import Lock
_cps_lock = Lock()
_cps_total = 0
_cps_last = 0
_cps_last_t = time.time()

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

def bulk_insert(conn, sym, tf, rows):
    if not rows: return 0
    tpl = [(sym, tf, int(ts), o, h, l, c, v) for ts,o,h,l,c,v in rows]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
            VALUES %s
            ON CONFLICT (symbol,timeframe,ts) DO NOTHING
        """, tpl, page_size=500)
    conn.commit()
    return len(tpl)

def fetch_retry(ex, sym, tf, since, limit):
    delay = 0.3
    for attempt in range(6):
        try:
            t0 = time.time()
            rows = ex.fetch_ohlcv(sym, timeframe=tf, since=since, limit=limit)
            dt = time.time() - t0
            log(f"[fetch] {sym} {tf} since={since} limit={limit} -> {len(rows)} in {dt:.2f}s")
            return rows
        except Exception as e:
            log(f"[err] fetch {sym} {tf} attempt {attempt+1}: {repr(e)}")
            time.sleep(delay)
            delay = min(delay*2, 3.0)
    return []

def backfill_tf(ex, conn, sym, tf, years, limit=1000, base_sleep=0.05):
    step_ms = STEP_SEC[tf]*1000
    end_closed = (now_ms() // step_ms)*step_ms - 1
    lookback_ms = int(years*365*24*3600*1000)
    # start from requested window start (no DB MAX)
    since = end_closed - lookback_ms
    inserted_total = 0
    while True:
        rows = fetch_retry(ex, sym, tf, since, limit)
        if not rows:
            log(f"[done-tf] {sym} {tf}: +{inserted_total} (no more rows)"); break
        rows = [r for r in rows if r[0] <= end_closed]
        if not rows:
            log(f"[done-tf] {sym} {tf}: +{inserted_total} (trim to end)"); break
        added = bulk_insert(conn, sym, tf, rows)
        # local cps counter (attempted inserts; DB-independent)
        global _cps_total
        with _cps_lock:
            _cps_total += len(rows)
        bump_rate_wc(conn, added)
        inserted_total += added
        since = rows[-1][0] + 1
        log(f"[prog] {sym} {tf} +{added} (total {inserted_total}), next_since={since}")
        if since > end_closed: break
        time.sleep(base_sleep)
    log(f"[done-tf] {sym} {tf}: +{inserted_total}")
    return inserted_total

def process_symbol(sym, limit):
    log(f"[start] {sym}")
    try:
        ex = ccxt.binance({
            "enableRateLimit": True,
            "timeout": 15000,
            "options": {"adjustForTimeDifference": True, "defaultType": "spot"},
        })
        ex.load_markets()
        with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
            total = 0
            for tf in ("1m","3m","5m"):
                total += backfill_tf(ex, conn, sym, tf, PLAN_YEARS[tf], limit=limit)
        log(f"[done] {sym}: +{total} rows")
        return sym, total
    except Exception as e:
        log(f"[fatal] {sym}: {repr(e)}")
        traceback.print_exc()
        return sym, 0

def pick_top_symbols(top_n=300, quote="USDT"):
    ex = ccxt.binance({"enableRateLimit": True, "timeout": 15000})
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
    ap.add_argument("--top", type=int, default=10, help="top N USDT pairs by volume")
    ap.add_argument("--workers", type=int, default=2, help="parallel symbols")
    ap.add_argument("--limit", type=int, default=1000, help="fetch_ohlcv limit")
    args = ap.parse_args()

    ensure_tables()
    symbols = args.symbols if args.symbols else pick_top_symbols(args.top)
    total_symbols = len(symbols)
    log(f"Backfill v2 (no-DB-max) | symbols={total_symbols} workers={args.workers} limit={args.limit}")

    stop = threading.Event()
    progress = {"done": 0}
    def heartbeat():
        while not stop.is_set():
            cps = wall_clock_rate_10s()
            log(f"[hb] {progress['done']}/{total_symbols} symbols done | ~{cps} cps")
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
    log(f"ALL DONE. total_inserted={total}")

if __name__ == "__main__":
    main()


