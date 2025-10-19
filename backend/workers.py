from __future__ import annotations
import os, io, time
from typing import Optional, List, Dict

import ccxt
import psycopg2

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:secret@postgres:5432/candles")

def _connect():
    return psycopg2.connect(DB_URL)

def _copy_to_staging(rows: List[Dict]):
    if not rows:
        return
    # in-memory TSV; empty string -> NULL (handled by copy_from(null=""))
    buf = io.StringIO()
    for r in rows:
        line = "\t".join([
            r.get("exchange") or "",
            r.get("symbol") or "",
            r.get("timeframe") or "",
            r.get("ts") or "",  # ISO string
            "" if r.get("open") is None else str(r.get("open")),
            "" if r.get("high") is None else str(r.get("high")),
            "" if r.get("low")  is None else str(r.get("low")),
            "" if r.get("close") is None else str(r.get("close")),
            "" if r.get("volume") is None else str(r.get("volume")),
            "" if r.get("qvol") is None else str(r.get("qvol")),
            "" if r.get("buy_quote") is None else str(r.get("buy_quote")),
            "" if r.get("sell_quote") is None else str(r.get("sell_quote")),
        ])
        buf.write(line + "\n")
    buf.seek(0)
    with _connect() as conn, conn.cursor() as cur:
        # IMPORTANT: tell Postgres to treat empty string as NULL
        cur.copy_from(
            buf,
            "staging_candles",
            sep="\t",
            null="" ,  # <--- key change
            columns=(
                "exchange","symbol","timeframe","ts",
                "open","high","low","close","volume","qvol","buy_quote","sell_quote"
            ),
        )

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
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=per_call, since=since)
        if not ohlcv:
            break

        page = [c for c in ohlcv if c[0] <= hard_end]
        if not page:
            break

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

        _copy_to_staging(rows)
        _merge_staging_into_final()

        since = page[-1][0] + 1
        if since and since >= hard_end:
            break

        time.sleep(0.2)

    print(f"[worker] done backfill -> {symbol=} {timeframe=}")
    return {"ok": True, "symbol": symbol, "timeframe": timeframe}
