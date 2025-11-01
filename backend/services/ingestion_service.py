from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

import ccxt.async_support as ccxt_async  # type: ignore
from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from core.config import get_settings
    from core.db import AsyncSessionLocal
    from repositories.pair_repository import PairRepository
    from utils.cache import CacheBackend, build_cache
    from utils.events import WebSocketManager
    from utils.queue import build_queue
except ModuleNotFoundError:
    from backend.core.config import get_settings
    from backend.core.db import AsyncSessionLocal
    from backend.repositories.pair_repository import PairRepository
    from backend.utils.cache import CacheBackend, build_cache
    from backend.utils.events import WebSocketManager
    from backend.utils.queue import build_queue
from .aggregator import CandleAggregator, timeframe_to_seconds
from .gap_fill import GapFillRequest, run_gap_fill
from .realtime_store import RealTimeCandle, RealTimeCandleStore
from .websocket_handler import update_stream_pairs

logger = logging.getLogger(__name__)

INSERT_CANDLES_SQL = text(
    """
    INSERT INTO candles (symbol, timeframe, ts, open, high, low, close, volume)
    VALUES (:symbol, :timeframe, :ts, :open, :high, :low, :close, :volume)
    ON CONFLICT (symbol, timeframe, ts) DO UPDATE
    SET open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
    """
)


@dataclass
class StatusUpdate:
    pair: str
    status: Optional[str] = None
    gaps: Optional[int] = None
    progress: Optional[float] = None
    timeframes: Optional[Dict[str, float]] = None


@dataclass
class IngestionRequest:
    symbols: Sequence[str]
    timeframes: Sequence[str]
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


class ProgressTracker:
    def __init__(self, cache: CacheBackend, ttl_seconds: int = 3600) -> None:
        self.cache = cache
        self.ttl = ttl_seconds
        self._memory: Dict[str, Dict[str, Any]] = {}
        self._latest: Optional[str] = None
        self._lock = asyncio.Lock()

    async def set(self, run_id: str, payload: Dict[str, Any]) -> None:
        enriched = dict(payload)
        enriched.setdefault("run_id", run_id)
        enriched.setdefault("updatedAt", _utc_now().isoformat())
        async with self._lock:
            self._memory[run_id] = enriched
            self._latest = run_id
        try:
            await self.cache.set(f"run:{run_id}", enriched, ttl=self.ttl)
            await self.cache.set("run:latest", {"run_id": run_id}, ttl=self.ttl)
        except Exception:
            pass

    async def get(self, run_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            local = self._memory.get(run_id)
        if local:
            return local
        try:
            return await self.cache.get(f"run:{run_id}")
        except Exception:
            return None

    async def latest(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            latest = self._latest
        if latest:
            cached = await self.get(latest)
            if cached:
                return cached
        try:
            marker = await self.cache.get("run:latest")
            if marker and "run_id" in marker:
                return await self.get(str(marker["run_id"]))
        except Exception:
            pass
        return None

    async def close(self) -> None:
        return None


class IngestionService:
    def __init__(
        self,
        repository: PairRepository | None = None,
        cache: CacheBackend | None = None,
        events: WebSocketManager | None = None,
        cache_ttl: int = 2,
    ) -> None:
        self.settings = get_settings()
        self.repository = repository or PairRepository()
        self.cache = cache or build_cache(self.settings.REDIS_URL, "ingestion-status")
        self.events = events or WebSocketManager()
        self.progress = ProgressTracker(self.cache, ttl_seconds=3600)
        self.queue = build_queue(self.settings.REDIS_URL, "ingestion-tasks")
        self.store = RealTimeCandleStore(self.settings.REDIS_URL)
        self.aggregator = CandleAggregator(self.store)
        self.cache_ttl = cache_ttl
        self._status_cache_key = "snapshot"
        self._cache_lock = asyncio.Lock()
        self._active_pairs: set[Tuple[str, str]] = set()
        self._pairs_lock = asyncio.Lock()

    async def _snapshot(self, session: AsyncSession) -> Dict[str, Any]:
        cached = await self.cache.get(self._status_cache_key)
        if cached:
            return cached
        pairs = await self.repository.list_pairs(session)
        payload = {
            "pairs": [item.to_dict() for item in pairs],
        }
        latest = await self.progress.latest()
        if latest:
            payload["run"] = latest
        await self.cache.set(self._status_cache_key, payload, ttl=self.cache_ttl)
        return payload

    async def _invalidate_snapshot(self) -> None:
        try:
            await self.cache.delete(self._status_cache_key)
        except Exception:
            pass

    async def get_status(self, session: AsyncSession) -> Dict[str, Any]:
        return await self._snapshot(session)

    async def apply_update(self, session: AsyncSession, update: StatusUpdate) -> Dict[str, Any]:
        entity = await self.repository.upsert(
            session,
            pair=update.pair,
            status=update.status,
            gaps=update.gaps,
            progress=update.progress,
            timeframes=update.timeframes,
        )
        await session.commit()
        payload = entity.to_dict()
        await self._invalidate_snapshot()
        await self.events.broadcast({"type": "status.update", "payload": payload})
        return payload

    async def enqueue_ingestion(self, request: IngestionRequest) -> Tuple[str, str]:
        symbols = [sym.strip() for sym in request.symbols if sym and sym.strip()]
        timeframes = [tf.strip() for tf in request.timeframes if tf and tf.strip()]
        if not symbols or not timeframes:
            raise ValueError("symbols and timeframes cannot be empty")

        run_id = uuid4().hex
        job_id = await self.queue.enqueue(
            "services.ingestion_worker:run_ingestion_job",
            kwargs={
                "symbols": symbols,
                "timeframes": timeframes,
                "start_ts": request.start_ts,
                "end_ts": request.end_ts,
                "run_id": run_id,
            },
        )
        await self.progress.set(
            run_id,
            {
                "status": "queued",
                "symbol": "",
                "timeframe": "",
                "step": 0,
                "total": max(1, len(symbols) * len(timeframes)),
                "percent": 0.0,
            },
        )
        await self._sync_realtime_streams(symbols, timeframes)
        return job_id, run_id

    async def register_websocket(self, websocket: WebSocket) -> None:
        await self.events.connect(websocket)
        try:
            async with AsyncSessionLocal() as session:
                snapshot = await self.get_status(session)
            await websocket.send_json({"type": "status.snapshot", "payload": snapshot})
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            await self.events.disconnect(websocket)

    async def ingest(
        self,
        *,
        run_id: str,
        symbols: Sequence[str],
        timeframes: Sequence[str],
        start_ts: Optional[int],
        end_ts: Optional[int],
    ) -> None:
        if not symbols or not timeframes:
            raise ValueError("symbols and timeframes cannot be empty")

        total_units = max(1, len(symbols) * len(timeframes))
        completed_units = 0
        await self.progress.set(
            run_id,
            {
                "status": "running",
                "symbol": "",
                "timeframe": "",
                "step": completed_units,
                "total": total_units,
                "percent": 0.0,
            },
        )
        await self._sync_realtime_streams(symbols, timeframes)

        exchange = ccxt_async.binance({"enableRateLimit": True})
        try:
            await exchange.load_markets()
            for symbol in symbols:
                for timeframe in timeframes:
                    await self._update_pair_status(symbol, "running", progress=0.0)
                    await self._ingest_symbol_timeframe(
                        exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        start_ts=start_ts,
                        end_ts=end_ts,
                    )
                    completed_units += 1
                    percent = (completed_units / total_units) * 100.0
                    await self.progress.set(
                        run_id,
                        {
                            "status": "running" if completed_units < total_units else "completed",
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "step": completed_units,
                            "total": total_units,
                            "percent": percent,
                        },
                    )
                    await self._update_pair_status(symbol, "completed", progress=percent)
            await self.progress.set(
                run_id,
                {
                    "status": "completed",
                    "symbol": "",
                    "timeframe": "",
                    "step": total_units,
                    "total": total_units,
                    "percent": 100.0,
                },
            )
        finally:
            try:
                await exchange.close()
            except Exception:
                pass

    async def _ingest_symbol_timeframe(
        self,
        exchange: ccxt_async.Exchange,
        *,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int],
        end_ts: Optional[int],
    ) -> None:
        normalized_tf = timeframe
        now_ms = int(_utc_now().timestamp() * 1000)
        target_end = end_ts or now_ms
        lookback_ms = self.settings.INGEST_LOOKBACK_MS
        target_start = start_ts or max(0, target_end - lookback_ms)
        step_ms = timeframe_to_seconds(normalized_tf) * 1000

        since = target_start
        collected: List[List[float]] = []
        while True:
            batch = await exchange.fetch_ohlcv(symbol, normalized_tf, since=since, limit=500)
            if not batch:
                break
            collected.extend(batch)
            last_ts = batch[-1][0]
            if last_ts >= target_end or len(batch) < 500:
                break
            since = last_ts + step_ms

        rows = [
            {
                "symbol": symbol,
                "timeframe": normalized_tf,
                "ts": _to_datetime(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
            for row in collected
            if row[0] <= target_end
        ]

        if not rows:
            logger.info("No candles for %s %s in requested window", symbol, timeframe)
            return

        await self._persist_rows(rows)
        await self.store.bulk_push(
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
            for row in rows[-min(200, len(rows)) :]
        )

        # Attempt targeted gap fill in case of missing slices
        await run_gap_fill(
            GapFillRequest(
                symbol=symbol,
                timeframe=normalized_tf,
                start_ts=target_start,
                end_ts=target_end,
            )
        )

    async def _persist_rows(self, rows: Sequence[Dict[str, Any]]) -> None:
        async with AsyncSessionLocal() as session:
            await session.execute(INSERT_CANDLES_SQL, rows)  # type: ignore[arg-type]
            await session.commit()
            await self._invalidate_snapshot()

    async def _update_pair_status(
        self,
        pair: str,
        status: str,
        *,
        progress: Optional[float] = None,
    ) -> None:
        async with AsyncSessionLocal() as session:
            await self.apply_update(
                session,
                StatusUpdate(
                    pair=pair,
                    status=status,
                    progress=progress,
                ),
            )

    async def _sync_realtime_streams(self, symbols: Sequence[str], timeframes: Sequence[str]) -> None:
        pairs = {(sym.upper(), tf.lower()) for sym in symbols for tf in timeframes}
        async with self._pairs_lock:
            self._active_pairs.update(pairs)
        await update_stream_pairs(sorted(self._active_pairs))

    async def close(self) -> None:
        await self.progress.close()
        await self.store.close()


_service = IngestionService()


def get_ingestion_service() -> IngestionService:
    return _service


async def shutdown_ingestion_service() -> None:
    await _service.close()


__all__ = [
    "IngestionService",
    "IngestionRequest",
    "StatusUpdate",
    "get_ingestion_service",
    "shutdown_ingestion_service",
]





