from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Sequence
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from core.config import get_settings
except ModuleNotFoundError:
    from backend.core.config import get_settings
from .realtime_store import RealTimeCandle, RealTimeCandleStore

TIMEFRAME_SECONDS: Dict[str, int] = {
    "1s": 1,
    "5s": 5,
    "10s": 10,
    "15s": 15,
    "30s": 30,
    "45s": 45,
    "1m": 60,
    "3m": 3 * 60,
    "5m": 5 * 60,
    "10m": 10 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "45m": 45 * 60,
    "1h": 60 * 60,
    "2h": 2 * 60 * 60,
    "3h": 3 * 60 * 60,
    "4h": 4 * 60 * 60,
    "6h": 6 * 60 * 60,
    "8h": 8 * 60 * 60,
    "12h": 12 * 60 * 60,
    "1d": 24 * 60 * 60,
    "3d": 3 * 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
    "1M": 30 * 24 * 60 * 60,
}


def normalize_timeframe(tf: str) -> str:
    cleaned = (tf or "").strip()
    if not cleaned:
        raise ValueError("timeframe cannot be empty")
    cleaned = cleaned.lower()
    if cleaned not in {k.lower(): v for k, v in TIMEFRAME_SECONDS.items()}:
        raise ValueError(f"Unsupported timeframe: {tf}")
    # return canonical case from map keys
    for key in TIMEFRAME_SECONDS:
        if key.lower() == cleaned:
            return key
    return cleaned


def timeframe_to_seconds(tf: str) -> int:
    canonical = normalize_timeframe(tf)
    value = TIMEFRAME_SECONDS.get(canonical)
    if value is None:
        raise ValueError(f"Unsupported timeframe: {tf}")
    return value


def bucket_start(ts: datetime, bucket_seconds: int) -> datetime:
    epoch = int(ts.timestamp())
    aligned = (epoch // bucket_seconds) * bucket_seconds
    return datetime.fromtimestamp(aligned, tz=timezone.utc)


@dataclass(frozen=True)
class AggregatedCandle:
    symbol: str
    timeframe: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "ts": self.ts.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class CandleAggregator:
    """
    Aggregates lower timeframe candles into higher timeframe buckets.
    Falls back to PostgreSQL historical data when the real-time cache does not cover the request.
    """

    def __init__(self, store: RealTimeCandleStore | None = None) -> None:
        self.settings = get_settings()
        self.store = store or RealTimeCandleStore(self.settings.REDIS_URL)

    async def _fetch_from_cache(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
    ) -> List[RealTimeCandle]:
        return await self.store.load(symbol, timeframe, start=start, end=end)

    async def _fetch_from_db(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
    ) -> List[RealTimeCandle]:
        def _run_query() -> List[RealTimeCandle]:
            conn = psycopg2.connect(
                host=self.settings.db_host or os.getenv("POSTGRES_HOST", "postgres"),
                port=self.settings.db_port or int(os.getenv("POSTGRES_PORT", "5432")),
                user=self.settings.db_user or os.getenv("POSTGRES_USER", "postgres"),
                password=self.settings.db_pass or os.getenv("POSTGRES_PASSWORD", "postgres"),
                dbname=self.settings.db_name or os.getenv("POSTGRES_DB", "postgres"),
                cursor_factory=RealDictCursor,
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, timeframe, ts, open, high, low, close, volume
                    FROM candles
                    WHERE symbol = %s AND timeframe = %s
                      AND (%s IS NULL OR ts >= %s)
                      AND (%s IS NULL OR ts <= %s)
                    ORDER BY ts ASC
                    """,
                    (symbol, timeframe, start, start, end, end),
                )
                rows = cur.fetchall()
            conn.close()
            return [
                RealTimeCandle(
                    symbol=row["symbol"],
                    timeframe=row["timeframe"],
                    ts=row["ts"],
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                )
                for row in rows
            ]

        return await asyncio.to_thread(_run_query)

    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
    ) -> List[RealTimeCandle]:
        cached = await self._fetch_from_cache(symbol, timeframe, start, end)
        history = await self._fetch_from_db(symbol, timeframe, start, end)
        if not cached:
            return history
        existing = {(c.symbol, c.timeframe, c.ts) for c in history}
        for candle in cached:
            key = (candle.symbol, candle.timeframe, candle.ts)
            if key not in existing:
                history.append(candle)
        history.sort(key=lambda c: c.ts)
        return history

    def _aggregate(
        self,
        candles: Sequence[RealTimeCandle],
        *,
        base_timeframe: str,
        target_timeframe: str,
    ) -> List[AggregatedCandle]:
        base_seconds = timeframe_to_seconds(base_timeframe)
        target_seconds = timeframe_to_seconds(target_timeframe)
        if target_seconds % base_seconds != 0:
            raise ValueError(
                f"Cannot aggregate {base_timeframe} into {target_timeframe} - incompatible intervals"
            )

        grouped: Dict[datetime, List[RealTimeCandle]] = defaultdict(list)
        for candle in candles:
            bucket = bucket_start(candle.ts, target_seconds)
            grouped[bucket].append(candle)

        aggregated: List[AggregatedCandle] = []
        for bucket_ts in sorted(grouped):
            bucket_candles = sorted(grouped[bucket_ts], key=lambda c: c.ts)
            first = bucket_candles[0]
            last = bucket_candles[-1]
            high = max(c.high for c in bucket_candles)
            low = min(c.low for c in bucket_candles)
            volume = sum(c.volume for c in bucket_candles)
            aggregated.append(
                AggregatedCandle(
                    symbol=first.symbol,
                    timeframe=target_timeframe,
                    ts=bucket_ts,
                    open=first.open,
                    high=high,
                    low=low,
                    close=last.close,
                    volume=volume,
                )
            )
        return aggregated

    async def aggregate_range(
        self,
        symbol: str,
        base_timeframe: str,
        target_timeframe: str,
        *,
        start: datetime | None,
        end: datetime | None,
    ) -> List[AggregatedCandle]:
        cleaned_base = normalize_timeframe(base_timeframe)
        cleaned_target = normalize_timeframe(target_timeframe)
        candles = await self.fetch_candles(symbol, cleaned_base, start, end)
        if not candles:
            return []
        return self._aggregate(candles, base_timeframe=cleaned_base, target_timeframe=cleaned_target)


__all__ = [
    "AggregatedCandle",
    "CandleAggregator",
    "normalize_timeframe",
    "timeframe_to_seconds",
    "TIMEFRAME_SECONDS",
]



