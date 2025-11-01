from __future__ import annotations

import asyncio
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Sequence

import ccxt.async_support as ccxt_async  # type: ignore
import psycopg2

try:
    from core.config import get_settings
except ModuleNotFoundError:
    from backend.core.config import get_settings
from .aggregator import normalize_timeframe, timeframe_to_seconds
from .realtime_store import RealTimeCandle, RealTimeCandleStore


@dataclass(frozen=True)
class GapFillRequest:
    symbol: str
    timeframe: str
    start_ts: int
    end_ts: int


@dataclass(frozen=True)
class GapFillResult:
    symbol: str
    timeframe: str
    inserted: int
    start_ts: int
    end_ts: int


def _ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _tuple_from_ohlcv(symbol: str, timeframe: str, ohlcv: Sequence[float]) -> RealTimeCandle:
    ts = datetime.fromtimestamp(ohlcv[0] / 1000.0, tz=timezone.utc)
    return RealTimeCandle(
        symbol=symbol,
        timeframe=timeframe,
        ts=ts,
        open=float(ohlcv[1]),
        high=float(ohlcv[2]),
        low=float(ohlcv[3]),
        close=float(ohlcv[4]),
        volume=float(ohlcv[5]),
    )


class GapFillService:
    def __init__(self, *, store: RealTimeCandleStore | None = None) -> None:
        self.settings = get_settings()
        self.store = store or RealTimeCandleStore(self.settings.REDIS_URL)

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
    ) -> List[RealTimeCandle]:
        exchange = ccxt_async.binance({"enableRateLimit": True})
        try:
            normalized = normalize_timeframe(timeframe)
            data: List[List[float]] = []
            since = start_ts
            while True:
                batch = await exchange.fetch_ohlcv(symbol, normalized, since=since, limit=500)
                if not batch:
                    break
                data.extend(batch)
                last_ts = batch[-1][0]
                if last_ts >= end_ts or len(batch) < 500:
                    break
                since = last_ts + timeframe_to_seconds(normalized) * 1000
            candles = [_tuple_from_ohlcv(symbol, normalized, row) for row in data if row[0] <= end_ts]
            return candles
        finally:
            try:
                await exchange.close()
            except Exception:
                pass

    async def run_gap_fill(self, request: GapFillRequest) -> GapFillResult:
        candles = await self.fetch_ohlcv(
            request.symbol,
            request.timeframe,
            request.start_ts,
            request.end_ts,
        )
        if not candles:
            return GapFillResult(
                symbol=request.symbol,
                timeframe=request.timeframe,
                inserted=0,
                start_ts=request.start_ts,
                end_ts=request.end_ts,
            )

        inserted = await self._persist(request.symbol, request.timeframe, candles)
        await self.store.bulk_push(candles)
        return GapFillResult(
            symbol=request.symbol,
            timeframe=request.timeframe,
            inserted=inserted,
            start_ts=request.start_ts,
            end_ts=request.end_ts,
        )

    async def _persist(
        self,
        symbol: str,
        timeframe: str,
        candles: Iterable[RealTimeCandle],
    ) -> int:
        def _execute() -> int:
            conn = psycopg2.connect(
                host=self.settings.db_host or os.getenv("POSTGRES_HOST", "postgres"),
                port=self.settings.db_port or int(os.getenv("POSTGRES_PORT", "5432")),
                user=self.settings.db_user or os.getenv("POSTGRES_USER", "postgres"),
                password=self.settings.db_pass or os.getenv("POSTGRES_PASSWORD", "postgres"),
                dbname=self.settings.db_name or os.getenv("POSTGRES_DB", "postgres"),
            )
            inserted_rows = 0
            with conn:
                with conn.cursor() as cur:
                    for candle in candles:
                        cur.execute(
                            """
                            INSERT INTO candles (symbol, timeframe, ts, open, high, low, close, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, timeframe, ts) DO NOTHING
                            """,
                            (
                                symbol,
                                timeframe,
                                candle.ts,
                                candle.open,
                                candle.high,
                                candle.low,
                                candle.close,
                                candle.volume,
                            ),
                        )
                        inserted_rows += cur.rowcount
            conn.close()
            return inserted_rows

        return await asyncio.to_thread(_execute)

    async def compute_coverage(
        self,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
    ) -> dict[str, object]:
        def _query() -> dict[str, object]:
            conn = psycopg2.connect(
                host=self.settings.db_host or os.getenv("POSTGRES_HOST", "postgres"),
                port=self.settings.db_port or int(os.getenv("POSTGRES_PORT", "5432")),
                user=self.settings.db_user or os.getenv("POSTGRES_USER", "postgres"),
                password=self.settings.db_pass or os.getenv("POSTGRES_PASSWORD", "postgres"),
                dbname=self.settings.db_name or os.getenv("POSTGRES_DB", "postgres"),
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM candles
                    WHERE symbol = %s AND timeframe = %s
                      AND ts BETWEEN to_timestamp(%s / 1000.0) AND to_timestamp(%s / 1000.0)
                    """,
                    (symbol, timeframe, start_ts, end_ts),
                )
                count = cur.fetchone()[0]
            conn.close()
            total_expected = max(
                0,
                math.floor((end_ts - start_ts) / (timeframe_to_seconds(timeframe) * 1000)) + 1,
            )
            coverage = 0.0 if total_expected == 0 else (count / total_expected) * 100.0
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "expected": total_expected,
                "present": int(count),
                "coverage": round(coverage, 2),
            }

        return await asyncio.to_thread(_query)


_service = GapFillService()


async def run_gap_fill(request: GapFillRequest) -> GapFillResult:
    return await _service.run_gap_fill(request)


async def compute_coverage(
    symbol: str,
    timeframe: str,
    start_ts: int,
    end_ts: int,
) -> dict[str, object]:
    return await _service.compute_coverage(symbol, timeframe, start_ts, end_ts)


__all__ = [
    "GapFillRequest",
    "GapFillResult",
    "GapFillService",
    "run_gap_fill",
    "compute_coverage",
]


