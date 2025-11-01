import asyncio
import json
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query

try:
    from core.config import get_settings
except ModuleNotFoundError:
    from backend.core.config import get_settings

from services.realtime_store import RealTimeCandleStore, RealTimeCandle
from services.websocket_handler import _handler as streaming_handler

router = APIRouter()

_settings = get_settings()
_store = RealTimeCandleStore(_settings.REDIS_URL)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _to_payload(candle: RealTimeCandle) -> dict[str, object]:
    return {
        "symbol": candle.symbol,
        "timeframe": candle.timeframe,
        "ts": candle.ts.isoformat(),
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
    }


@router.websocket("/ws/chart")
async def ws_chart(
    websocket: WebSocket,
    symbol: str = Query(..., description="Symbol, e.g. OG/USDT"),
    timeframe: str = Query(..., description="Base timeframe to monitor"),
    limit: int = Query(200, ge=1, le=1000),
):
    await websocket.accept()
    norm_symbol = _normalize_symbol(symbol)
    norm_timeframe = timeframe.strip().lower()
    channel = _store.channel(norm_symbol, norm_timeframe)

    try:
        candles = await _store.load(norm_symbol, norm_timeframe, limit=limit)
        if candles:
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "snapshot",
                        "symbol": norm_symbol,
                        "timeframe": norm_timeframe,
                        "count": len(candles),
                        "candles": [_to_payload(candle) for candle in candles],
                    }
                )
            )
        else:
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "snapshot",
                        "symbol": norm_symbol,
                        "timeframe": norm_timeframe,
                        "count": 0,
                        "candles": [],
                    }
                )
            )

        pubsub = _store.pubsub()
        await pubsub.subscribe(channel)
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                try:
                    payload = json.loads(data)
                except json.JSONDecodeError:
                    continue
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "candle",
                            "symbol": norm_symbol,
                            "timeframe": norm_timeframe,
                            "candle": payload,
                        }
                    )
                )
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    except WebSocketDisconnect:
        return
    except Exception as exc:
        try:
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "error",
                        "message": str(exc),
                        "symbol": norm_symbol,
                        "timeframe": norm_timeframe,
                    }
                )
            )
        except Exception:
            pass


@router.get("/ws/chart/metrics")
async def ws_chart_metrics():
    return await streaming_handler.get_stats()
