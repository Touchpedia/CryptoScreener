from __future__ import annotations
import os, time
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

# --- Redis/RQ wiring (docker network hostnames/env) ---
REDIS_URL  = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RQ_QUEUE   = os.getenv("RQ_QUEUE", "ingestion-tasks")

_redis = None
q = None
try:
    if REDIS_URL:
        from redis import Redis; from rq import Queue
        _redis = Redis.from_url(REDIS_URL)
        q = Queue(RQ_QUEUE, connection=_redis)
    else:
        from redis import Redis; from rq import Queue
        _redis = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        q = Queue(RQ_QUEUE, connection=_redis)
except Exception:
    _redis, q = None, None

app = FastAPI(title="Crypto Screener (Backfill Restore)")

@app.get("/api/status")
def status():
    ok = False
    try:
        if _redis is not None: _redis.ping(); ok = True
    except Exception:
        ok = False
    return {"ok": True, "redis": ok, "queue": RQ_QUEUE}

# ---- Original backfill trigger shape ----
class IngestionRequest(BaseModel):
    symbols: List[str]
    timeframes: List[str]
    start_ts: Optional[int] = None   # ms epoch UTC (optional)
    end_ts: Optional[int] = None

@app.post("/api/ingestion/run")
async def run_ingestion(req: IngestionRequest):
    if not req.symbols or not req.timeframes:
        raise HTTPException(status_code=400, detail="symbols/timeframes required")
    if q is None:
        # Redis/RQ not available -> still 200 so UI flow continues
        return {"ok": True, "queued": False, "jobs": [], "count": 0}

    jobs = []
    for s in req.symbols:
        for tf in req.timeframes:
            # workers.backfill_range_job(symbol, timeframe, start_ts, end_ts)
            j = q.enqueue("workers.backfill_range_job", s, tf, req.start_ts, req.end_ts, job_timeout=3600)
            jobs.append(j.id)
    return {"ok": True, "queued": True, "jobs": jobs, "count": len(jobs)}