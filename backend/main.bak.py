from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from redis import Redis
from rq import Queue

app = FastAPI(title="Crypto Screener API")

# CORS (UI @ 5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis/RQ
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
_redis = Redis.from_url(REDIS_URL)
q = Queue("ingestion-tasks", connection=_redis)

class IngestionRequest(BaseModel):
    symbols: List[str]
    timeframes: List[str]
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None

@app.get("/api/status")
def status():
    try:
        _redis.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"ok": True, "server_time": datetime.now(timezone.utc).isoformat(), "redis": redis_ok}

@app.post("/api/ingestion/run")
def run_ingestion(req: IngestionRequest):
    if not req.symbols or not req.timeframes:
        raise HTTPException(status_code=400, detail="symbols/timeframes required")
    jobs = []
    for s in req.symbols:
        for tf in req.timeframes:
            j = q.enqueue("workers.backfill_range_job", s, tf, req.start_ts, req.end_ts, job_timeout=3600)
            jobs.append(j.id)
    return {"ok": True, "jobs": jobs, "count": len(jobs)}
