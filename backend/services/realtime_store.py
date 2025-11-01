from __future__ import annotations

import json
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from redis import asyncio as aioredis


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _epoch_seconds(dt: datetime) -> float:
    return _ensure_utc(dt).timestamp()


def _canonical_symbol(symbol: str) -> str:
    return symbol.replace("/", "_").upper()


@dataclass(frozen=True)
class RealTimeCandle:
    symbol: str
    timeframe: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> "RealTimeCandle":
        raw_ts = payload.get("ts")
        if isinstance(raw_ts, (int, float)):
            ts = datetime.fromtimestamp(float(raw_ts) / 1000.0, tz=timezone.utc)
        elif isinstance(raw_ts, str):
            ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        elif isinstance(raw_ts, datetime):
            ts = raw_ts
        else:
            raise ValueError(f"Unsupported timestamp payload: {raw_ts!r}")
        return cls(
            symbol=str(payload["symbol"]),
            timeframe=str(payload["timeframe"]),
            ts=_ensure_utc(ts),
            open=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=float(payload["volume"]),
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "ts": _ensure_utc(self.ts).isoformat(),
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "close": self.close,
                "volume": self.volume,
            }
        )


class RealTimeCandleStore:
    """
    Keeps a rolling, in-memory (Redis backed) window of candles per symbol/timeframe.
    Old entries are evicted based on retention_seconds using sorted sets.
    Publishes updates on a dedicated channel so downstream consumers can stream the latest data.
    """

    def __init__(
        self,
        redis_url: str,
        retention_seconds: int = 10_000,
        namespace: str = "rt_candles",
        stream_namespace: str = "rt_candles_stream",
    ) -> None:
        self._client = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self._namespace = namespace
        self._stream_namespace = stream_namespace
        self._retention_seconds = retention_seconds

    def key(self, symbol: str, timeframe: str) -> str:
        return f"{self._namespace}:{_canonical_symbol(symbol)}:{timeframe.lower()}"

    def channel(self, symbol: str, timeframe: str) -> str:
        return f"{self._stream_namespace}:{_canonical_symbol(symbol)}:{timeframe.lower()}"

    async def push(self, candle: RealTimeCandle) -> None:
        key = self.key(candle.symbol, candle.timeframe)
        ts_sec = _epoch_seconds(candle.ts)
        payload = candle.to_json()
        cutoff = ts_sec - float(self._retention_seconds)
        pipeline = self._client.pipeline()
        pipeline.zadd(key, {payload: ts_sec})
        pipeline.zremrangebyscore(key, 0, cutoff)
        pipeline.publish(self.channel(candle.symbol, candle.timeframe), payload)
        await pipeline.execute()

    async def bulk_push(self, candles: Iterable[RealTimeCandle]) -> None:
        tasks = [self.push(candle) for candle in candles]
        if not tasks:
            return
        await asyncio.gather(*tasks)

    async def load(
        self,
        symbol: str,
        timeframe: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[RealTimeCandle]:
        start_score = "-inf" if start is None else _epoch_seconds(start)
        end_score = "+inf" if end is None else _epoch_seconds(end)
        key = self.key(symbol, timeframe)
        rows = await self._client.zrangebyscore(key, start_score, end_score, withscores=False)
        candles: List[RealTimeCandle] = []
        for row in rows:
            try:
                payload = json.loads(row)
                candle = RealTimeCandle.from_payload(payload)
                candles.append(candle)
            except Exception:
                continue
        candles.sort(key=lambda c: c.ts)
        if limit is not None and limit > 0:
            return candles[-limit:]
        return candles

    async def close(self) -> None:
        await self._client.close()

    def pubsub(self):
        return self._client.pubsub()


__all__ = [
    "RealTimeCandle",
    "RealTimeCandleStore",
]
