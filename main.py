from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

# --- Redis/RQ (safe import, env-driven) ---
REDIS_URL = os.getenv("REDIS_URL", "redis://cs_redis:6379/0")
RQ_QUEUE  = os.getenv("RQ_QUEUE", "ingestion-tasks")

_redis = None
q = None
try:
    from redis import Redis  # type: ignore
    from rq import Queue     # type: ignore
    _redis = Redis.from_url(REDIS_URL)
    q = Queue(RQ_QUEUE, connection=_redis)
except Exception:
    pass

app = FastAPI(title="Crypto Screener API")

# ---- Health ----
@app.get("/api/status")
def status():
    redis_ok = False
    try:
        if _redis is not None:
            _redis.ping(); redis_ok = True
    except Exception:
        redis_ok = False
    return {"ok": True, "redis": redis_ok, "server_time": datetime.now(timezone.utc).isoformat()}

# ---- Ingestion trigger (REAL enqueue) ----
class IngestionReqA(BaseModel):
    symbols: List[str]
    timeframes: List[str]
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None

# also accept UI style: top_symbols + interval + candles_per_symbol (map to defaults)
@app.post("/api/ingestion/run")
async def run_ingestion(request: Request):
    body = await request.json().catch(lambda *_: {}) if hasattr(request, "json") else {}
    # path A: explicit symbols/timeframes
    symbols = body.get("symbols")
    timeframes = body.get("timeframes")
    # path B: top_symbols/interval (fallback – enqueue just BTC/ETH/SOL as demo)
    top_symbols = body.get("top_symbols")
    interval = body.get("interval")
    if not symbols and top_symbols and interval:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"][:int(top_symbols)]
        timeframes = [str(interval)]

    if not symbols or not timeframes:
        raise HTTPException(status_code=400, detail="symbols/timeframes required")

    if q is None:
        # queue not wired — still succeed for UI flow
        return {"ok": True, "queued": False, "jobs": [], "count": 0, "note": "RQ not available"}

    jobs = []
    for s in symbols:
        for tf in timeframes:
            # REAL enqueue (same style as backup code)  # ref: workers.backfill_range_job
            j = q.enqueue("workers.backfill_range_job", s, tf, None, None, job_timeout=3600)
            jobs.append(j.id)
    return {"ok": True, "queued": True, "jobs": jobs, "count": len(jobs)}