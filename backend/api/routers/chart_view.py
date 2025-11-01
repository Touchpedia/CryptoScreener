from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
import importlib.util

if importlib.util.find_spec("services.ingestion_service"):
    from services.ingestion_service import get_ingestion_service
else:
    from backend.services.ingestion_service import get_ingestion_service

router = APIRouter(prefix="/api/chart", tags=["chart"])


def _ms_to_datetime(value: Optional[int]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)


@router.get("/candles")
async def get_candles(
    symbol: str = Query(..., description="Trading pair, e.g. BTC/USDT"),
    target_timeframe: str = Query(..., description="Requested timeframe, e.g. 1h"),
    base_timeframe: Optional[str] = Query(None, description="Base timeframe to aggregate from"),
    start_ts: Optional[int] = Query(None, description="Start timestamp (ms since epoch)"),
    end_ts: Optional[int] = Query(None, description="End timestamp (ms since epoch)"),
    limit: int = Query(500, ge=1, le=5000),
):
    service = get_ingestion_service()
    base_tf = base_timeframe or target_timeframe
    start = _ms_to_datetime(start_ts)
    end = _ms_to_datetime(end_ts)
    candles = await service.aggregator.aggregate_range(
        symbol=symbol,
        base_timeframe=base_tf,
        target_timeframe=target_timeframe,
        start=start,
        end=end,
    )
    if not candles:
        raise HTTPException(status_code=404, detail="No candles found for selection")
    payload = [candle.to_dict() for candle in candles]
    return {"ok": True, "candles": payload[-limit:]}
