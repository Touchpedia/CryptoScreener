import os
from dataclasses import asdict
from typing import List

import importlib.util
import psycopg2
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

if importlib.util.find_spec("services.gap_fill"):
    from services.gap_fill import GapFillRequest, compute_coverage, run_gap_fill
else:
    from backend.services.gap_fill import GapFillRequest, compute_coverage, run_gap_fill

router = APIRouter(prefix="/api/admin", tags=["admin"])

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "postgres")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

TABLES = ["staging_candles"]


class GapFillPayload(BaseModel):
    symbols: List[str] = Field(default_factory=list)
    timeframes: List[str] = Field(default_factory=list)
    start_ts: int
    end_ts: int


@router.post("/flush")
async def flush():
    try:
        conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in TABLES:
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY")
        conn.close()
        return {"ok": True, "flushed": TABLES}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/gap-fill")
async def trigger_gap_fill(payload: GapFillPayload):
    symbols = [sym.strip() for sym in payload.symbols if sym.strip()]
    timeframes = [tf.strip() for tf in payload.timeframes if tf.strip()]
    if not symbols or not timeframes:
        raise HTTPException(status_code=400, detail="symbols and timeframes are required")
    results = []
    for symbol in symbols:
        for timeframe in timeframes:
            result = await run_gap_fill(
                GapFillRequest(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_ts=payload.start_ts,
                    end_ts=payload.end_ts,
                )
            )
            results.append(asdict(result))
    return {"ok": True, "results": results}


@router.get("/gap-fill/coverage")
async def gap_fill_coverage(
    symbol: str = Query(..., description="Trading pair, e.g. BTC/USDT"),
    timeframe: str = Query(..., description="Timeframe, e.g. 5m"),
    start_ts: int = Query(..., description="Start timestamp in ms"),
    end_ts: int = Query(..., description="End timestamp in ms"),
):
    report = await compute_coverage(symbol, timeframe, start_ts, end_ts)
    return {"ok": True, "coverage": report}
