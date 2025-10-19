from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import psycopg2

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

app = FastAPI(title="Crypto Screener API (Coverage)")

# ---------- Health / Status ----------
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

# ---------- Ingestion trigger ----------
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

# ---------- NEW: Coverage report ----------
@app.get("/api/report/coverage")
def report_coverage(
    timeframe: str = Query("1m", description="e.g. 1m"),
    required: int = Query(6000, ge=1, le=50000),
    limit: int = Query(100, ge=1, le=1000),
    symbols: Optional[str] = Query(None, description="comma-separated symbols to restrict (optional)")
):
    """
    For each symbol (top N or restricted list):
      - latest_ts: most recent candle ts
      - received: count of candles where ts > latest_ts - required minutes
      - required: requested number (e.g. 6000)
      - coverage_pct = received/required * 100
    """
    try:
        if symbols:
            syms = [s.strip() for s in symbols.split(",") if s.strip()]
            # latest per given symbols
            sql_latest = """
                WITH latest AS (
                  SELECT symbol, (SELECT MAX(ts) FROM (SELECT ts FROM candles WHERE symbol = :symbol AND interval = :interval ORDER BY ts DESC LIMIT :n) _lastn) AS latest_ts
                  FROM candles
                  WHERE timeframe = %s AND symbol = ANY(%s)
                  GROUP BY symbol
                )
                SELECT l.symbol, l.latest_ts
                FROM latest l
                ORDER BY l.latest_ts DESC
            """
            latest_rows = db_rows(sql_latest, (timeframe, syms))
        else:
            # top symbols by activity (row count) for timeframe
            sql_latest = """
                WITH ranked AS (
                  SELECT symbol, (SELECT MAX(ts) FROM (SELECT ts FROM candles WHERE symbol = :symbol AND interval = :interval ORDER BY ts DESC LIMIT :n) _lastn) AS latest_ts, COUNT(*) AS cnt
                  FROM candles
                  WHERE timeframe = %s
                  GROUP BY symbol
                  ORDER BY cnt DESC
                  LIMIT %s
                )
                SELECT symbol, latest_ts
                FROM ranked
                ORDER BY latest_ts DESC
            """
            latest_rows = db_rows(sql_latest, (timeframe, limit))

        if not latest_rows:
            return {"ok": True, "rows": []}

        # make symbol list & latest_ts map
        sym_list = [r["symbol"] for r in latest_rows]
        latest_map = { r["symbol"]: r["latest_ts"] for r in latest_rows }

        # count received within last "required" minutes from each symbol's latest_ts
        # Do in a single query using ANY + CASE filter
        sql_received = """
            SELECT c.symbol, (SELECT COUNT(*) FROM (SELECT ts FROM candles WHERE symbol = :symbol AND interval = :interval ORDER BY ts DESC LIMIT :n) _lastn) AS received
            FROM candles c
            JOIN (
              SELECT symbol, (SELECT MAX(ts) FROM (SELECT ts FROM candles WHERE symbol = :symbol AND interval = :interval ORDER BY ts DESC LIMIT :n) _lastn) AS latest_ts
              FROM candles
              WHERE timeframe = %s AND symbol = ANY(%s)
              GROUP BY symbol
            ) l ON l.symbol = c.symbol
            WHERE c.timeframe = %s
              AND c.symbol = ANY(%s)
              AND c.ts > l.latest_ts - (%s || ' minutes')::interval
            GROUP BY c.symbol
        """
        rec_rows = db_rows(sql_received, (timeframe, sym_list, timeframe, sym_list, str(required)))
        rec_map = { r["symbol"]: r["received"] for r in rec_rows }

        # build final rows
        out = []
        for sym in sym_list:
            received = int(rec_map.get(sym, 0))
            cov = round(received * 100.0 / required, 2) if required else 0.0
            out.append({
                "symbol": sym,
                "required": required,
                "received": received,
                "coverage_pct": cov,
                "latest_ts": latest_map.get(sym)
            })
        # sort by coverage desc, then symbol
        out.sort(key=lambda x: (-x["coverage_pct"], x["symbol"]))
        return {"ok": True, "timeframe": timeframe, "rows": out[:limit]}
    except Exception as e:
        # soft error (keep UI simple)
        return {"ok": False, "error": str(e)}

