from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Sequence, Tuple, cast
from uuid import uuid4

try:
    import ccxt.async_support as ccxt  # type: ignore
except ImportError as exc:  # pragma: no cover - makes error clearer for users
    raise RuntimeError(
        "ccxt is required for ingestion. Install it via 'pip install ccxt'."
    ) from exc

from fastapi import WebSocket, WebSocketDisconnect
from redis import asyncio as aioredis  # type: ignore
from redis.exceptions import RedisError  # type: ignore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.config import get_settings
from backend.core.db import AsyncSessionLocal
from backend.models.pair_progress import PairProgress
from backend.repositories.pair_repository import PairRepository
from backend.utils.cache import CacheBackend, build_cache
from backend.utils.events import WebSocketManager
from backend.utils.queue import build_queue

logger = logging.getLogger(__name__)

settings = get_settings()

_TIMEFRAME_SECONDS: Dict[str, int] = {
    "1m": 60,
    "3m": 3 * 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "2h": 2 * 60 * 60,
    "4h": 4 * 60 * 60,
    "6h": 6 * 60 * 60,
    "8h": 8 * 60 * 60,
    "12h": 12 * 60 * 60,
    "1d": 24 * 60 * 60,
    "3d": 3 * 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
    "1M": 30 * 24 * 60 * 60,
}

INSERT_CANDLES_SQL = text(
    """
    INSERT INTO candles (symbol, timeframe, ts, open, high, low, close, volume)
    VALUES (:symbol, :timeframe, :ts, :open, :high, :low, :close, :volume)
    ON CONFLICT (symbol, timeframe, ts) DO NOTHING
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
    symbols: Optional[List[str]] = None
    timeframes: Optional[List[str]] = None
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None


class ProgressStore:
    """Simple progress tracker backed by Redis (falls back to memory)."""

    def __init__(self, url: str, ttl_seconds: int = 3600) -> None:
        self._ttl = ttl_seconds
        self._latest_key = "run:latest"
        self._memory: Dict[str, Dict[str, Any]] = {}
        self._memory_latest: Optional[str] = None
        self._lock = asyncio.Lock()
        self._client = None
        if url.startswith(("redis://", "rediss://")):
            try:
                self._client = aioredis.from_url(
                    url, encoding="utf-8", decode_responses=True
                )
            except Exception:  # pragma: no cover - fallback
                self._client = None

    @staticmethod
    def _run_key(run_id: str) -> str:
        return f"run:{run_id}"

    async def set(self, run_id: str, data: Dict[str, Any]) -> None:
        payload = {k: str(v) for k, v in data.items() if v is not None}
        client = self._client
        if client:
            key = self._run_key(run_id)
            try:
                await cast(Awaitable[Any], client.hset(key, mapping=payload))
                await cast(Awaitable[Any], client.expire(key, self._ttl))
                await cast(
                    Awaitable[Any], client.set(self._latest_key, run_id, ex=self._ttl)
                )
                return
            except RedisError as exc:  # pragma: no cover - network failures
                logging.warning("Redis unavailable for progress tracking: %s", exc)
                self._client = None  # fallback to in-memory storage

        async with self._lock:
            existing = self._memory.get(run_id, {}).copy()
            existing.update(payload)
            self._memory[run_id] = existing
            self._memory_latest = run_id

    async def get(self, run_id: str) -> Optional[Dict[str, Any]]:
        client = self._client
        if client:
            key = self._run_key(run_id)
            try:
                data = await cast(Awaitable[Any], client.hgetall(key))
                return cast(Optional[Dict[str, Any]], data) or None
            except RedisError as exc:  # pragma: no cover
                logging.warning("Redis unavailable for progress lookup: %s", exc)
                self._client = None

        async with self._lock:
            return self._memory.get(run_id)

    async def latest_run_id(self) -> Optional[str]:
        client = self._client
        if client:
            try:
                return await cast(Awaitable[Optional[str]], client.get(self._latest_key))
            except RedisError as exc:  # pragma: no cover
                logging.warning("Redis unavailable for latest run lookup: %s", exc)
                self._client = None

        async with self._lock:
            return self._memory_latest

    async def close(self) -> None:
        if self._client:
            try:
                await self._client.close()
            except RedisError:
                pass


class IngestionService:
    def __init__(
        self,
        repository: PairRepository,
        cache: CacheBackend,
        events: WebSocketManager,
        cache_ttl: int = 2,
    ) -> None:
        self.repository = repository
        self.cache = cache
        self.events = events
        self.cache_key = "status.snapshot"
        self.cache_ttl = cache_ttl
        self._lock = asyncio.Lock()
        self._progress = ProgressStore(settings.REDIS_URL)
        self.exchange: Optional[ccxt.Exchange] = None

    async def init_exchange(self) -> None:
        """Initialise the Binance exchange client if not already available."""
        if self.exchange is not None:
            return

        try:
            exchange_cls = getattr(ccxt, "binance")
        except AttributeError as exc:  # pragma: no cover
            raise RuntimeError("ccxt installation missing Binance exchange") from exc

        try:
            exchange = exchange_cls({"enableRateLimit": True})
            # ensure spot market by default
            try:
                exchange.options["defaultType"] = "spot"  # type: ignore[attr-defined]
            except AttributeError:
                exchange.options = {"defaultType": "spot"}  # type: ignore[attr-defined]

            await exchange.load_markets()
            self.exchange = exchange
        except Exception as exc:  # pragma: no cover - bubble to caller for retry
            logger.exception("Exchange initialization failed: %s", exc)
            raise

    async def _serialize(self, record: PairProgress) -> Dict[str, Any]:
        return record.to_dict()

    async def _invalidate_snapshot(self) -> None:
        await self.cache.delete(self.cache_key)

    async def _broadcast(self, payload: Dict[str, Any]) -> None:
        await self.events.broadcast({"type": "status.update", "payload": payload})

    async def _upsert_progress_entity(
        self,
        session: AsyncSession,
        *,
        pair: str,
        status: str,
        progress_pct: float,
        timeframe: Optional[str],
        timeframe_progress: Optional[float],
    ) -> Dict[str, Any]:
        tf_payload = None
        if timeframe and timeframe_progress is not None:
            tf_payload = {timeframe: timeframe_progress}

        entity = await self.repository.upsert(
            session,
            pair=pair,
            status=status,
            progress=progress_pct,
            timeframes=tf_payload,
        )
        await session.commit()
        await self._invalidate_snapshot()
        payload = await self._serialize(entity)
        await self._broadcast(payload)
        return payload

    async def _latest_progress(self) -> Optional[Dict[str, Any]]:
        run_id = await self._progress.latest_run_id()
        if not run_id:
            return None
        raw = await self._progress.get(run_id)
        if not raw:
            return None

        def _to_float(raw_value: Optional[str]) -> Optional[float]:
            if raw_value is None:
                return None
            try:
                return float(raw_value)
            except (TypeError, ValueError):
                return None

        return {
            "run_id": run_id,
            "symbol": raw.get("symbol"),
            "timeframe": raw.get("timeframe"),
            "status": raw.get("status"),
            "step": int(raw.get("step", "0")),
            "total": int(raw.get("total", "0")),
            "percent": _to_float(raw.get("percent")) or 0.0,
            "updatedAt": raw.get("updatedAt"),
            "error": raw.get("error"),
        }

    async def _build_snapshot(self, session: AsyncSession) -> Dict[str, Any]:
        cached = await self.cache.get(self.cache_key)
        if not cached:
            records = await self.repository.list_pairs(session)
            items = [await self._serialize(record) for record in records]
            cached = {
                "items": items,
                "total": len(items),
                "lastUpdated": datetime.now(timezone.utc).isoformat(),
            }
            await self.cache.set(self.cache_key, cached, ttl=self.cache_ttl)

        progress = await self._latest_progress()
        if progress:
            cached["run"] = progress
        return cached

    async def get_status(self, session: AsyncSession) -> Dict[str, Any]:
        return await self._build_snapshot(session)

    async def apply_update(self, session: AsyncSession, update: StatusUpdate) -> Dict[str, Any]:
        async with self._lock:
            entity = await self.repository.upsert(
                session,
                pair=update.pair,
                status=update.status,
                gaps=update.gaps,
                progress=update.progress,
                timeframes=update.timeframes,
            )
            await session.commit()
            payload = await self._serialize(entity)
            await self._invalidate_snapshot()
            await self._broadcast(payload)
            return payload

    async def enqueue_ingestion(self, request: IngestionRequest) -> Tuple[str, str]:
        symbols = request.symbols or []
        timeframes = request.timeframes or []
        if not symbols or not timeframes:
            raise ValueError("symbols and timeframes cannot be empty")

        run_id = uuid4().hex
        kwargs: Dict[str, Any] = {
            "symbols": symbols,
            "timeframes": timeframes,
            "start_ts": request.start_ts,
            "end_ts": request.end_ts,
            "run_id": run_id,
        }
        job_id = await _queue.enqueue(
            "backend.services.ingestion_worker:run_ingestion_job", kwargs=kwargs
        )

        total_units = max(1, len(symbols) * len(timeframes))
        await self._progress.set(
            run_id,
            {
                "status": "queued",
                "symbol": "",
                "timeframe": "",
                "step": 0,
                "total": total_units,
                "percent": 0.0,
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            },
        )
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
        total_units = max(1, len(symbols) * len(timeframes))
        completed_units = 0
        try:
            exchange_cls = getattr(ccxt, "binance")
        except AttributeError as exc:  # pragma: no cover - unexpected install issue
            raise RuntimeError(
                "ccxt installation does not include the Binance exchange."
            ) from exc

        exchange = exchange_cls({"enableRateLimit": True})
        # ensure spot by default (some installs don't accept options at init)
        try:
            exchange.options["defaultType"] = "spot"
        except AttributeError:
            exchange.options = {"defaultType": "spot"}  # type: ignore[attr-defined]
        await exchange.load_markets()

        await self._progress.set(
            run_id,
            {
                "status": "running",
                "symbol": "",
                "timeframe": "",
                "step": completed_units,
                "total": total_units,
                "percent": 0.0,
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            },
        )

        current_symbol: Optional[str] = None
        current_timeframe: Optional[str] = None

        async with AsyncSessionLocal() as session:
            try:
                for symbol in symbols:
                    normalized_symbol = self._normalize_symbol(symbol)
                    current_symbol = normalized_symbol
                    await self._upsert_progress_entity(
                        session,
                        pair=normalized_symbol,
                        status="running",
                        progress_pct=(completed_units / total_units) * 100.0,
                        timeframe=None,
                        timeframe_progress=None,
                    )

                    for timeframe in timeframes:
                        current_timeframe = timeframe
                        await self._progress.set(
                            run_id,
                            {
                                "status": "running",
                                "symbol": normalized_symbol,
                                "timeframe": timeframe,
                                "step": completed_units,
                                "total": total_units,
                                "percent": round(
                                    (completed_units / total_units) * 100.0, 2
                                ),
                                "updatedAt": datetime.now(timezone.utc).isoformat(),
                            },
                        )

                        await self._ingest_symbol_timeframe(
                            session=session,
                            exchange=exchange,
                            run_id=run_id,
                            symbol=normalized_symbol,
                            timeframe=timeframe,
                            start_ts=start_ts,
                            end_ts=end_ts,
                            total_units=total_units,
                            completed_units=completed_units,
                        )

                        completed_units += 1
                        percent = round((completed_units / total_units) * 100.0, 2)
                        status = "running" if completed_units < total_units else "completed"
                        await self._progress.set(
                            run_id,
                            {
                                "status": status,
                                "symbol": normalized_symbol,
                                "timeframe": timeframe,
                                "step": completed_units,
                                "total": total_units,
                                "percent": percent,
                                "updatedAt": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                        await self._upsert_progress_entity(
                            session,
                            pair=normalized_symbol,
                            status=status if status == "completed" else "running",
                            progress_pct=percent,
                            timeframe=timeframe,
                            timeframe_progress=1.0,
                        )

                    # Once all timeframes for this symbol are processed, bump the pair status to done.
                    await self._upsert_progress_entity(
                        session,
                        pair=normalized_symbol,
                        status="done" if completed_units < total_units else "completed",
                        progress_pct=100.0,
                        timeframe=None,
                        timeframe_progress=None,
                    )

                await self._progress.set(
                    run_id,
                    {
                        "status": "completed",
                        "step": completed_units,
                        "total": total_units,
                        "percent": 100.0,
                        "updatedAt": datetime.now(timezone.utc).isoformat(),
                    },
                )
            except Exception as exc:  # pylint: disable=broad-except
                await self._progress.set(
                    run_id,
                    {
                        "status": "error",
                        "error": str(exc),
                        "step": completed_units,
                        "total": total_units,
                        "percent": round(
                            (completed_units / total_units) * 100.0, 2
                        ),
                        "updatedAt": datetime.now(timezone.utc).isoformat(),
                    },
                )
                if current_symbol:
                    await self._upsert_progress_entity(
                        session,
                        pair=current_symbol,
                        status="error",
                        progress_pct=round(
                            (completed_units / total_units) * 100.0, 2
                        ),
                        timeframe=current_timeframe,
                        timeframe_progress=0.0 if current_timeframe else None,
                    )
                raise
            finally:
                await exchange.close()

    async def _ingest_symbol_timeframe(
        self,
        *,
        session: AsyncSession,
        exchange: ccxt.binance,  # type: ignore
        run_id: str,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int],
        end_ts: Optional[int],
        total_units: int,
        completed_units: int,
    ) -> None:
        tf_ms = self._timeframe_ms(timeframe)
        limit = 1000
        target_end = end_ts or int(time.time() * 1000)
        since = await self._resolve_start_ts(
            session=session, symbol=symbol, timeframe=timeframe, start_ts=start_ts, tf_ms=tf_ms
        )
        if since is None:
            lookback_ms = self._default_lookback_ms(timeframe, tf_ms)
            since = max(0, target_end - lookback_ms)

        initial_since = since
        range_ms = max(target_end - initial_since, tf_ms)
        latest_ts = initial_since

        while True:
            batch = await self._fetch_with_retry(
                exchange, symbol, timeframe, since, limit
            )
            if not batch:
                break

            rows, last_ts = self._filter_rows(batch, symbol, timeframe, target_end)
            if rows:
                await session.execute(INSERT_CANDLES_SQL, rows)
                await session.commit()
                latest_ts = last_ts or latest_ts
                await self._enforce_retention(session, symbol, timeframe, target_end)

                percent = self._compute_percent(
                    completed_units,
                    total_units,
                    latest_ts,
                    initial_since,
                    range_ms,
                )
                await self._progress.set(
                    run_id,
                    {
                        "status": "running",
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "step": completed_units,
                        "total": total_units,
                        "percent": percent,
                        "updatedAt": datetime.now(timezone.utc).isoformat(),
                    },
                )
                await self._upsert_progress_entity(
                    session,
                    pair=symbol,
                    status="running",
                    progress_pct=percent,
                    timeframe=timeframe,
                    timeframe_progress=min(1.0, (latest_ts - initial_since) / range_ms),
                )

            if len(batch) < limit or (last_ts and last_ts >= target_end):
                break

            if last_ts is None:
                break
            since = last_ts + tf_ms
        # final retention sweep in case no rows inserted in last batch
        await self._enforce_retention(session, symbol, timeframe, target_end)

    async def _fetch_with_retry(
        self,
        exchange: ccxt.binance,  # type: ignore
        symbol: str,
        timeframe: str,
        since: int,
        limit: int,
        retries: int = 5,
    ) -> List[List[float]]:
        delay = 1.0
        for attempt in range(retries):
            try:
                return await exchange.fetch_ohlcv(
                    symbol, timeframe, since=since, limit=limit
                )
            except Exception:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(delay)
                delay *= 2
        return []

    def _filter_rows(
        self,
        batch: Iterable[Sequence[float]],
        symbol: str,
        timeframe: str,
        end_ts: int,
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        rows: List[Dict[str, Any]] = []
        last_ts: Optional[int] = None
        for entry in batch:
            if len(entry) < 6:
                continue
            ts, open_, high, low, close, volume = entry[:6]
            ts_int = int(ts)
            if ts_int > end_ts:
                break
            rows.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "ts": ts_int,
                    "open": float(open_),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": float(volume),
                }
            )
            last_ts = ts_int
        return rows, last_ts

    def _compute_percent(
        self,
        completed_units: int,
        total_units: int,
        latest_ts: int,
        initial_since: int,
        range_ms: int,
    ) -> float:
        base_percent = completed_units / max(1, total_units)
        intra_percent = min(1.0, max(0.0, (latest_ts - initial_since) / range_ms))
        overall = (base_percent + intra_percent / max(1, total_units)) * 100.0
        return round(min(100.0, overall), 2)

    async def _resolve_start_ts(
        self,
        *,
        session: AsyncSession,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int],
        tf_ms: int,
    ) -> Optional[int]:
        if start_ts is not None:
            return start_ts

        result = await session.execute(
            text(
                "SELECT MAX(ts) FROM candles WHERE symbol = :symbol AND timeframe = :timeframe"
            ),
            {"symbol": symbol, "timeframe": timeframe},
        )
        last_ts = result.scalar()
        if last_ts is None:
            return None
        return int(last_ts) + tf_ms

    def _normalize_symbol(self, symbol: str) -> str:
        if "/" in symbol:
            return symbol.upper()

        upper_symbol = symbol.upper()
        known_quotes = ("USDT", "USDC", "BUSD", "BTC", "ETH")
        for quote in known_quotes:
            if upper_symbol.endswith(quote):
                base = upper_symbol[: -len(quote)]
                if base:
                    return f"{base}/{quote}"
        return upper_symbol

    def _timeframe_ms(self, timeframe: str) -> int:
        seconds = _TIMEFRAME_SECONDS.get(timeframe)
        if seconds is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        return seconds * 1000

    def _default_lookback_ms(self, timeframe: str, tf_ms: int) -> int:
        """Compute default lookback window when start_ts not provided."""
        lookback_ms = settings.INGEST_LOOKBACK_MS
        # For higher timeframes, keep at least 24 candles so we don't fetch too little.
        min_candles = 24 * tf_ms
        return max(lookback_ms, min_candles)

    async def _enforce_retention(
        self,
        session: AsyncSession,
        symbol: str,
        timeframe: str,
        target_end: int,
    ) -> None:
        """Delete candles older than the retention window to keep storage bounded."""
        retention_ms = settings.INGEST_RETENTION_MS
        cutoff = max(0, target_end - retention_ms)
        await session.execute(
            text(
                """
                DELETE FROM candles
                WHERE symbol = :symbol
                  AND timeframe = :timeframe
                  AND ts < :cutoff
                """
            ),
            {"symbol": symbol, "timeframe": timeframe, "cutoff": cutoff},
        )
        await session.commit()

    async def close(self) -> None:
        await self.cache.close()
        await self._progress.close()


_repository = PairRepository()
_namespace = "status"
_cache_ttl = 2
_redis_url = settings.REDIS_URL
_queue_name = "ingestion-tasks"

_cache = build_cache(_redis_url, _namespace)
_events = WebSocketManager()
_queue = build_queue(_redis_url, _queue_name)

_service = IngestionService(
    repository=_repository,
    cache=_cache,
    events=_events,
    cache_ttl=_cache_ttl,
)


def get_ingestion_service() -> IngestionService:
    return _service


async def shutdown_ingestion_service() -> None:
    await _service.close()
