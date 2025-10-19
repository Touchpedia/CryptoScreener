from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# Optional Redis/RQ (safe import)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
_redis = None
q = None
try:
    from redis import Redis  # type: ignore
    from rq import Queue     # type: ignore
    _redis = Redis.from_url(REDIS_URL)
    q = Queue("ingestion-tasks", connection=_redis)
except Exception:
    pass

# DB
import psycopg2
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:secret@postgres:5432/candles")

def db_rows(sql: str, params: tuple):
    rows = []
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            for r in cur.fetchall():
                rows.append({cols[i]: r[i] for i in range(len(cols))})
    return rows

app = FastAPI(title="Crypto Screener API (Minimal + Latest)")

# ---- Health/Status (always 200) ----
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/status")
def status():
    redis_ok = False
    try:
        if _redis is not None:
            _redis.ping(); redis_ok = True
    except Exception:
        redis_ok = False
    return {"ok": True, "redis": redis_ok, "server_time": datetime.now(timezone.utc).isoformat()}

# ---- Ingestion trigger (stub/real worker) ----
class IngestionRequest(BaseModel):
    symbols: List[str]
    timeframes: List[str]
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None

@app.post("/api/ingestion/run")
def run_ingestion(req: IngestionRequest):
    if not req.symbols or not req.timeframes:
        raise HTTPException(status_code=400, detail="symbols/timeframes required")
    if q is None:
        return {"ok": True, "queued": False, "jobs": [], "count": 0}
    jobs = []
    for s in req.symbols:
        for tf in req.timeframes:
            j = q.enqueue("workers.backfill_range_job", s, tf, req.start_ts, req.end_ts, job_timeout=3600)
            jobs.append(j.id)
    return {"ok": True, "queued": True, "jobs": jobs, "count": len(jobs)}

# ---- NEW: Latest candles endpoint ----
@app.get("/api/candles/latest")
def candles_latest(
    symbol: str = Query(..., description="e.g. BTC/USDT"),
    timeframe: str = Query(..., description="e.g. 1m,5m,1h"),
    limit: int = Query(5, ge=1, le=500, description="number of rows"),
):
    try:
        sql = """
        SELECT exchange, symbol, timeframe, ts, open, high, low, close, volume
        FROM candles
        WHERE symbol = %s AND timeframe = %s
        ORDER BY ts DESC
        LIMIT %s
        """
        rows = db_rows(sql, (symbol, timeframe, limit))
        return {"ok": True, "rows": rows}
    except Exception as e:
        # return 200 + error to keep UI simple
        return {"ok": False, "error": str(e)}
