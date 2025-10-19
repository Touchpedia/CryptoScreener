from __future__ import annotations
import time
from typing import Optional

# NOTE: Phase-1 stub.
# Yahan actual CCXT fetch + DB insert baad me wire karenge.
def backfill_range_job(symbol: str, timeframe: str, start_ts: Optional[int], end_ts: Optional[int]):
    print(f"[worker] backfill_range_job start -> {symbol=} {timeframe=} {start_ts=} {end_ts=}")
    # simulate some work
    time.sleep(2)
    print(f"[worker] backfill_range_job done  -> {symbol=} {timeframe=}")
    return {"ok": True, "symbol": symbol, "timeframe": timeframe}
