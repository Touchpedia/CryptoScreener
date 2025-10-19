# -*- coding: utf-8 -*-
import time, ccxt

def p(*a): print(*a, flush=True)

def probe(sym="BTC/USDT"):
    ex = ccxt.binance({
        "enableRateLimit": True,
        "timeout": 10000,  # 10s
        "options": {"adjustForTimeDifference": True, "defaultType": "spot"},
    })
    t0=time.time()
    ex.load_markets()
    p("[ok] load_markets in", round(time.time()-t0,2),"s")
    p("has.OHLCV:", ex.has.get("fetchOHLCV"))
    p("timeframes:", sorted(list(ex.timeframes.keys()))[:10], "...")

    # exchange time vs local
    try:
        srv = ex.fetch_time()
        drift = int(time.time()*1000) - srv
        p("clock_drift_ms:", drift)
    except Exception as e:
        p("[warn] fetch_time:", repr(e))

    for tf in ["1m","3m","5m"]:
        if tf not in ex.timeframes: 
            p(f"[skip] {tf} not supported"); 
            continue
        for tag, since in [("recent", None), ("last_hour", int(time.time()-3600)*1000)]:
            try:
                t1=time.time()
                rows = ex.fetch_ohlcv(sym, timeframe=tf, limit=5, since=since)
                dt=round(time.time()-t1,2)
                p(f"[ok] {sym} {tf} {tag}: got {len(rows)} in {dt}s; first_ts={rows[0][0] if rows else None}")
            except Exception as e:
                p(f"[err] {sym} {tf} {tag}:", repr(e))

if __name__ == "__main__":
    for s in ["BTC/USDT","ETH/USDT","BNB/USDT","USDC/USDT"]:
        print("\n=== PROBE", s, "===")
        probe(s)
