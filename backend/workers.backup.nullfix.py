from __future__ import annotations
import os, io, time, math, json
from typing import Optional, List, Dict

import ccxt
import psycopg2
from psycopg2.extras import execute_values

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:secret@postgres:5432/candles")

def _connect():
    return psycopg2.connect(DB_URL)

def _copy_to_staging(rows: List[Dict]):
    if not rows: 
        return
    # COPY via in-memory TSV
    buf = io.StringIO()
    for r in rows:
        buf.write("\t".join([
            r["exchange"], r["symbol"], r["timeframe"],
            r["ts"],                           # ISO ts string
            str(r["open"] or ""), str(r["high"] or ""), str(r["low"] or ""), str(r["close"] or ""),
            str(r["volume"] or ""), str(r["qvol"] or ""),
            str(r["buy_quote"] or ""), str(r["sell_quote"] or ""),
        ]) + "\n")
    buf.seek(0)
    with _connect() as conn, conn.cursor() as cur:
        cur.copy_from(buf, "staging_candles", sep="\t", columns=(
            "exchange","symbol","timeframe","ts",
            "open","high","low","close","volume","qvol","buy_quote","sell_quote"
        ))

def _merge_staging_into_final():
    sql = """
    INSERT INTO candles AS c
      (exchange, symbol, timeframe, ts, open, high, low, close, volume, qvol, buy_quote, sell_quote)
    SELECT exchange, symbol, timeframe, ts, open, high, low, close, volume, qvol, buy_quote, sell_quote
    FROM staging_candles
    ON CONFLICT (exchange, symbol, timeframe, ts)
    DO UPDATE SET
      open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
      volume=EXCLUDED.volume, qvol=EXCLUDED.qvol,
      buy_quote=EXCLUDED.buy_quote, sell_quote=EXCLUDED.sell_quote;
    TRUNCATE staging_candles;
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql)

def _ms_to_iso(ms: int) -> str:
    # psycopg accept karega ISO 8601
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms/1000))

def backfill_range_job(symbol: str, timeframe: str, start_ts: Optional[int], end_ts: Optional[int]):
    print(f"[worker] start backfill -> {symbol=} {timeframe=} {start_ts=} {end_ts=}")
    ex = ccxt.binance({"enableRateLimit": True, "options": {"adjustForTimeDifference": True}})
    ex.load_markets()

    exchange = "binance"
    per_call = 1000
    since = start_ts or None
    hard_end = end_ts or int(time.time() * 1000)

    while True:
        # fetch page
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=per_call, since=since)
        if not ohlcv:
            break

        # bound to end time
        page = [c for c in ohlcv if c[0] <= hard_end]
        if not page:
            break

        # map → rows for COPY
        rows = []
        for c in page:
            ms = c[0]
            rows.append({
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "ts": _ms_to_iso(ms),
                "open": c[1], "high": c[2], "low": c[3], "close": c[4],
                "volume": c[5],
                "qvol": (c[4] or 0) * (c[5] or 0),
                "buy_quote": None, "sell_quote": None,
            })

        # COPY to staging, then merge
        _copy_to_staging(rows)
        _merge_staging_into_final()

        # advance
        next_ms = page[-1][0] + 1
        if since is not None and next_ms <= since:
            # safety
            next_ms = since + 1
        since = next_ms

        # if already reached end
        if since and since >= hard_end:
            break

        # polite sleep
        time.sleep(0.2)

    print(f"[worker] done backfill -> {symbol=} {timeframe=}")
    return {"ok": True, "symbol": symbol, "timeframe": timeframe}
