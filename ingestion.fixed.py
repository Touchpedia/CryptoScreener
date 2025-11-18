from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import os
from redis import Redis
from rq import Queue

router = APIRouter(prefix="/ingestion")  # prefix fix

REDIS_URL = os.getenv("REDIS_URL", "redis://cs_redis:6379/0")
RQ_QUEUE = os.getenv("RQ_QUEUE", "ingestion-tasks")

r = Redis.from_url(REDIS_URL)
q = Queue(RQ_QUEUE, connection=r)

class IngestionReq(BaseModel):
    symbols: Optional[List[str]] = None
    timeframes: Optional[List[str]] = None
    top_symbols: Optional[int] = None
    interval: Optional[str] = None
    candles_per_symbol: Optional[int] = None

@router.post("/run")
def run_ingestion(payload: IngestionReq):
    if not payload.symbols and payload.top_symbols and payload.interval:
        payload.symbols = ["BTCUSDT","ETHUSDT","SOLUSDT"][:payload.top_symbols]
        payload.timeframes = [payload.interval]
    if not payload.symbols or not payload.timeframes:
        raise HTTPException(status_code=400, detail="symbols/timeframes required")
    jobs = []
    for s in payload.symbols:
        for tf in payload.timeframes:
            j = q.enqueue("workers.backfill_range_job", s, tf, None, None, job_timeout=3600)
            jobs.append(j.id)
    return {"ok": True, "queued": True, "jobs": jobs, "count": len(jobs)}

@router.get("/status")
def status():
    try:
        redis_ok = r.ping()
    except Exception:
        redis_ok = False
    return {"ok": True, "redis": redis_ok}

@router.get("/active")
def active():
    try:
        jobs = q.jobs
        return {"active": [j.id for j in jobs]}
    except Exception:
        return {"active": []}

# alias for import
ingestion = router