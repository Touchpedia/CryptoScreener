from __future__ import annotations

import io
import os
import time
from typing import Dict, List, Optional

import ccxt
import psycopg2

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:secret@postgres:5432/candles")


def _connect():
    return psycopg2.connect(DB_URL)


def _tf_to_ms(timeframe: str) -> int:
    tf = (timeframe or "").strip().lower()

    def _safe_int(text: str, scale: int) -> int:
        try:
            return int(text) * scale
        except (TypeError, ValueError):
            return 0

    if tf.endswith("ms"):
        return _safe_int(tf[:-2], 1)
    if tf.endswith("s"):
        return _safe_int(tf[:-1], 1_000)
    if tf.endswith("m"):
        return _safe_int(tf[:-1], 60_000)
    if tf.endswith("h"):
        return _safe_int(tf[:-1], 60 * 60_000)
    if tf.endswith("d"):
        return _safe_int(tf[:-1], 24 * 60 * 60_000)

    try:
        return int(tf)
    except (TypeError, ValueError):
        return 0


def _copy_to_staging(rows: List[Dict]):
    if not rows:
        return

    buf = io.StringIO()
    for r in rows:
        line = "\t".join(
            [
                r.get("exchange") or "",
                r.get("symbol") or "",
                r.get("timeframe") or "",
                r.get("ts") or "",
                "" if r.get("open") is None else str(r.get("open")),
                "" if r.get("high") is None else str(r.get("high")),
                "" if r.get("low") is None else str(r.get("low")),
                "" if r.get("close") is None else str(r.get("close")),
                "" if r.get("volume") is None else str(r.get("volume")),
                "" if r.get("qvol") is None else str(r.get("qvol")),
                "" if r.get("buy_quote") is None else str(r.get("buy_quote")),
                "" if r.get("sell_quote") is None else str(r.get("sell_quote")),
            ]
        )
        buf.write(line + "\n")
    buf.seek(0)

    with _connect() as conn, conn.cursor() as cur:
        cur.copy_from(
            buf,
            "staging_candles",
            sep="\t",
            null="",
            columns=(
                "exchange",
                "symbol",
                "timeframe",
                "ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "qvol",
                "buy_quote",
                "sell_quote",
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
      open=EXCLUDED.open,
      high=EXCLUDED.high,
      low=EXCLUDED.low,
      close=EXCLUDED.close,
      volume=EXCLUDED.volume,
      qvol=EXCLUDED.qvol,
      buy_quote=EXCLUDED.buy_quote,
      sell_quote=EXCLUDED.sell_quote;
    TRUNCATE staging_candles;
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql)


def _ms_to_iso(ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


def backfill_range_job(symbol: str, timeframe: str, start_ts: Optional[int], end_ts: Optional[int]):
    print(f"[worker] start backfill -> {symbol=} {timeframe=} {start_ts=} {end_ts=}")

    ex = ccxt.binance({"enableRateLimit": True, "options": {"adjustForTimeDifference": True}})
    ex.load_markets()

    exchange = "binance"
    per_call_cap = 1000
    tf_ms = _tf_to_ms(timeframe)

    # Redispatch loops default to the requested window; None falls back to live tailing.
    since = start_ts if start_ts is not None else None
    hard_end = end_ts or int(time.time() * 1000)

    while True:
        if since is not None and since > hard_end:
            break

        limit = per_call_cap

        ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit, since=since)
        if not ohlcv:
            break

        page = [c for c in ohlcv if c[0] <= hard_end]
        if not page:
            break

        rows = []
        for c in page:
            ms = c[0]
            rows.append(
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "ts": _ms_to_iso(ms),
                    "open": c[1],
                    "high": c[2],
                    "low": c[3],
                    "close": c[4],
                    "volume": c[5],
                    "qvol": (c[4] or 0) * (c[5] or 0),
                    "buy_quote": None,
                    "sell_quote": None,
                }
            )

        _copy_to_staging(rows)
        _merge_staging_into_final()

        last_ts = page[-1][0]
        next_since = last_ts + 1

        if next_since > hard_end:
            break

        since = next_since
        time.sleep(0.2)

    print(f"[worker] done backfill -> {symbol=} {timeframe=}")
    return {"ok": True, "symbol": symbol, "timeframe": timeframe}
