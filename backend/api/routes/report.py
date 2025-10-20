from __future__ import annotations
from typing import Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from ...core.db import get_session

router = APIRouter(prefix="/report", tags=["report"])

_TIMEFRAME_MS: Dict[str,int] = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000,
    "1w": 604_800_000, "1M": 2_592_000_000
}

class CoverageRequest(BaseModel):
    symbols: Optional[List[str]] = None
    timeframes: Optional[List[str]] = None

async def _compute(session: AsyncSession, symbols: List[str], timeframes: List[str]) -> dict:
    items: List[dict] = []
    for sym in symbols:
        for tf in timeframes:
            tf_ms = _TIMEFRAME_MS.get(tf)
            if tf_ms is None:
                raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {tf}")
            res = await session.execute(
                text("""
                    SELECT COUNT(*)::bigint AS cnt,
                           MIN(ts) AS min_ts,
                           MAX(ts) AS max_ts
                    FROM candles
                    WHERE symbol = :s AND timeframe = :tf
                """),
                {"s": sym, "tf": tf}
            )
            row = res.first()
            cnt = int(row.cnt or 0)
            min_ts = int(row.min_ts) if row.min_ts is not None else None
            max_ts = int(row.max_ts) if row.max_ts is not None else None

            total_required = None
            completeness = None
            if min_ts is not None and max_ts is not None:
                total_required = max(1, ((max_ts - min_ts) // tf_ms) + 1)
                completeness = round((cnt / total_required) * 100, 2) if total_required else None

            items.append({
                "exchange": "binance",
                "symbol": sym,
                "timeframe": tf,
                "min_ts": min_ts,
                "max_ts": max_ts,
                "rows_present": cnt,
                "total_required": total_required,
                "completeness_pct": completeness
            })
    return {"ok": True, "items": items}

@router.post("/coverage")
async def report_coverage_post(payload: CoverageRequest, session: AsyncSession = Depends(get_session)):
    try:
        symbols = payload.symbols or []
        timeframes = payload.timeframes or []
        if not symbols or not timeframes:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="symbols and timeframes are required")
        return await _compute(session, symbols, timeframes)
    except HTTPException:
        raise
    except Exception as exc:
        return {"ok": False, "error": str(exc), "source": "report.coverage.post"}

@router.get("/coverage")
async def report_coverage_get(
    symbols: List[str] = Query(default=[],
                               description="Repeat ?symbols=BTC/USDT&symbols=ETH/USDT or comma-separated"),
    timeframes: List[str] = Query(default=[],
                                  description="Repeat ?timeframes=1m&timeframes=5m or comma-separated"),
    session: AsyncSession = Depends(get_session),
):
    try:
        # allow comma-separated fallback
        def _expand(vals: List[str]) -> List[str]:
            out: List[str] = []
            for v in vals:
                out.extend([p for p in v.split(",") if p])
            return out
        syms = _expand(symbols)
        tfs  = _expand(timeframes)
        if not syms or not tfs:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="symbols and timeframes are required")
        return await _compute(session, syms, tfs)
    except HTTPException:
        raise
    except Exception as exc:
        return {"ok": False, "error": str(exc), "source": "report.coverage.get"}
