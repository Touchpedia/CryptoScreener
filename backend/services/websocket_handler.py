from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import websockets
from websockets import WebSocketException

try:
    from core.config import get_settings
except ModuleNotFoundError:
    from backend.core.config import get_settings
from .gap_fill import GapFillRequest, run_gap_fill
from .realtime_store import RealTimeCandle, RealTimeCandleStore

logger = logging.getLogger(__name__)

BINANCE_STREAM_URL = "wss://stream.binance.com:9443/stream?streams={streams}"


def _stream_symbol(symbol: str) -> str:
    return symbol.replace("/", "").lower()


def _now_ts_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _normalize_symbol(symbol: str) -> str:
    if "/" in symbol:
        parts = symbol.split("/", 1)
        return f"{parts[0].upper()}/{parts[1].upper()}"
    upper = symbol.upper()
    quotes = ("USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI", "EUR", "BTC", "ETH")
    for quote in quotes:
        if upper.endswith(quote):
            base = upper[: -len(quote)]
            if not base:
                break
            return f"{base}/{quote}"
    return upper


@dataclass
class GroupMetrics:
    group_id: str
    pairs: List[Tuple[str, str]] = field(default_factory=list)
    connects: int = 0
    disconnects: int = 0
    status: str = "idle"
    last_connect: Optional[datetime] = None
    last_disconnect: Optional[datetime] = None

    def snapshot(self, max_pairs: int) -> Dict[str, object]:
        return {
            "group_id": self.group_id,
            "pair_count": len(self.pairs),
            "max_pairs": max_pairs,
            "status": self.status,
            "connects": self.connects,
            "disconnects": self.disconnects,
            "reconnections": max(0, self.disconnects),
            "last_connect": self.last_connect.isoformat() if self.last_connect else None,
            "last_disconnect": self.last_disconnect.isoformat() if self.last_disconnect else None,
            "pairs": self.pairs,
        }


class BinanceWebSocketHandler:
    """
    Manages grouped Binance combined stream connections, persists final candles to Postgres,
    and mirrors the latest window into Redis for real-time charting.
    """

    def __init__(
        self,
        *,
        store: RealTimeCandleStore | None = None,
        max_pairs_per_connection: int = 200,
        writer_batch_size: int = 50,
        writer_flush_interval: float = 1.0,
    ) -> None:
        self.settings = get_settings()
        self.store = store or RealTimeCandleStore(self.settings.REDIS_URL)
        self.max_pairs_per_connection = max(1, max_pairs_per_connection)
        self.writer_batch_size = max(1, writer_batch_size)
        self.writer_flush_interval = max(0.25, writer_flush_interval)

        self._queue: asyncio.Queue[RealTimeCandle] = asyncio.Queue(maxsize=5_000)
        self._writer_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._groups: Dict[str, List[Tuple[str, str]]] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._running = False
        self._last_seen: Dict[Tuple[str, str], datetime] = {}
        self._metrics: Dict[str, GroupMetrics] = {}
        self._conn_args = {
            "host": self.settings.db_host or os.getenv("POSTGRES_HOST", "postgres"),
            "port": self.settings.db_port or int(os.getenv("POSTGRES_PORT", "5432")),
            "user": self.settings.db_user or os.getenv("POSTGRES_USER", "postgres"),
            "password": self.settings.db_pass or os.getenv("POSTGRES_PASSWORD", "postgres"),
            "dbname": self.settings.db_name or os.getenv("POSTGRES_DB", "postgres"),
        }

    async def start(self) -> None:
        async with self._lock:
            if self._running:
                return
            self._running = True
            logger.info("Starting BinanceWebSocketHandler")
            self._writer_task = asyncio.create_task(self._writer_loop(), name="ws-writer")
            await self._restart_streams_locked()

    async def stop(self) -> None:
        async with self._lock:
            if not self._running:
                return
            self._running = False
            logger.info("Stopping BinanceWebSocketHandler")
            for task in self._tasks.values():
                task.cancel()
            self._tasks.clear()
            self._groups.clear()
            if self._writer_task:
                self._writer_task.cancel()
                self._writer_task = None
        await self._drain_queue()

    async def _drain_queue(self) -> None:
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def update_pairs(self, pairs: Sequence[Tuple[str, str]]) -> None:
        """
        Update the list of symbol/timeframe pairs we should subscribe to.
        """
        normalized = self._normalize_pairs(pairs)
        async with self._lock:
            self._groups = self._build_groups(normalized)
            self._sync_metrics()
            await self._restart_streams_locked()

    async def _restart_streams_locked(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()
        if not self._running or not self._groups:
            return
        for group_id, group_pairs in self._groups.items():
            self._tasks[group_id] = asyncio.create_task(
                self._run_group(group_id, group_pairs),
                name=f"ws-group-{group_id}",
            )

    def _normalize_pairs(self, pairs: Sequence[Tuple[str, str]]) -> List[Tuple[str, str]]:
        deduped: Dict[Tuple[str, str], None] = {}
        for symbol, timeframe in pairs:
            key = (symbol.upper(), timeframe.lower())
            deduped[key] = None
        return sorted(deduped.keys())

    def _build_groups(self, pairs: Sequence[Tuple[str, str]]) -> Dict[str, List[Tuple[str, str]]]:
        groups: Dict[str, List[Tuple[str, str]]] = {}
        current: List[Tuple[str, str]] = []
        group_index = 0
        for pair in pairs:
            current.append(pair)
            if len(current) == self.max_pairs_per_connection:
                groups[f"group-{group_index}"] = current
                current = []
                group_index += 1
        if current:
            groups[f"group-{group_index}"] = current
        return groups

    def _sync_metrics(self) -> None:
        active_keys = set(self._groups.keys())
        for group_id in list(self._metrics.keys()):
            if group_id not in active_keys:
                del self._metrics[group_id]
        for group_id, pairs in self._groups.items():
            metrics = self._metrics.get(group_id)
            if not metrics:
                metrics = GroupMetrics(group_id=group_id, pairs=list(pairs))
                self._metrics[group_id] = metrics
            else:
                metrics.pairs = list(pairs)

    async def _run_group(self, group_id: str, pairs: Sequence[Tuple[str, str]]) -> None:
        backoff = 1.0
        streams = "/".join(
            f"{_stream_symbol(symbol)}@kline_{timeframe}"
            for symbol, timeframe in pairs
        )
        url = BINANCE_STREAM_URL.format(streams=streams)
        logger.info("Starting stream %s with %d pairs", group_id, len(pairs))
        while self._running:
            metrics = self._metrics.setdefault(group_id, GroupMetrics(group_id=group_id, pairs=list(pairs)))
            metrics.pairs = list(pairs)
            try:
                async with websockets.connect(url, ping_interval=15, ping_timeout=15) as ws:
                    backoff = 1.0
                    metrics.connects += 1
                    metrics.status = "connected"
                    metrics.last_connect = datetime.now(timezone.utc)
                    async for raw_message in ws:
                        await self._handle_message(raw_message)
            except asyncio.CancelledError:
                logger.debug("Stream %s cancelled", group_id)
                metrics.status = "cancelled"
                break
            except WebSocketException as exc:
                logger.warning("Stream %s websocket error: %s", group_id, exc)
                metrics.disconnects += 1
                metrics.status = "disconnected"
                metrics.last_disconnect = datetime.now(timezone.utc)
            except Exception as exc:
                logger.exception("Stream %s crashed: %s", group_id, exc)
                metrics.disconnects += 1
                metrics.status = "disconnected"
                metrics.last_disconnect = datetime.now(timezone.utc)
            await self._handle_disconnect(pairs)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)
        metrics.status = "stopped"

    async def _handle_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        data = payload.get("data") or payload
        kline = data.get("k")
        if not kline:
            return
        if not kline.get("x"):
            # Ignore partial candles; we only persist closed intervals
            return
        symbol = data.get("s") or kline.get("s")
        timeframe = kline.get("i")
        if not symbol or not timeframe:
            return
        candle = RealTimeCandle(
            symbol=_normalize_symbol(symbol),
            timeframe=timeframe,
            ts=datetime.fromtimestamp(kline["T"] / 1000.0, tz=timezone.utc),
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
        )
        self._last_seen[(candle.symbol, candle.timeframe)] = candle.ts
        await self.store.push(candle)
        try:
            self._queue.put_nowait(candle)
        except asyncio.QueueFull:
            logger.warning("Writer queue full; dropping candle for %s %s", candle.symbol, candle.timeframe)

    async def _handle_disconnect(self, pairs: Sequence[Tuple[str, str]]) -> None:
        now_ms = _now_ts_ms()
        tasks = []
        for symbol, timeframe in pairs:
            last_ts = self._last_seen.get((symbol, timeframe))
            if not last_ts:
                continue
            gap_start = int(last_ts.timestamp() * 1000) + 1
            if gap_start >= now_ms:
                continue
            request = GapFillRequest(
                symbol=symbol,
                timeframe=timeframe,
                start_ts=gap_start,
                end_ts=now_ms,
            )
            tasks.append(run_gap_fill(request))
        if tasks:
            logger.info("Reconnection gap detected for %d pairs", len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _writer_loop(self) -> None:
        buffer: List[RealTimeCandle] = []
        try:
            while True:
                try:
                    candle = await asyncio.wait_for(
                        self._queue.get(), timeout=self.writer_flush_interval
                    )
                    buffer.append(candle)
                    self._queue.task_done()
                    if len(buffer) >= self.writer_batch_size:
                        await self._flush(buffer)
                        buffer.clear()
                except asyncio.TimeoutError:
                    if buffer:
                        await self._flush(buffer)
                        buffer.clear()
        except asyncio.CancelledError:
            pass
        finally:
            if buffer:
                await self._flush(buffer)

    async def _flush(self, candles: List[RealTimeCandle]) -> None:
        if not candles:
            return

        def _write(batch: List[RealTimeCandle]) -> None:
            conn = psycopg2.connect(**self._conn_args)
            with conn:
                with conn.cursor() as cur:
                    for candle in batch:
                        cur.execute(
                            """
                            INSERT INTO candles (symbol, timeframe, ts, open, high, low, close, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, timeframe, ts) DO UPDATE
                            SET open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                            """,
                            (
                                candle.symbol,
                                candle.timeframe,
                                candle.ts,
                                candle.open,
                                candle.high,
                                candle.low,
                                candle.close,
                                candle.volume,
                            ),
                        )
            conn.close()

        await asyncio.to_thread(_write, list(candles))

    async def get_stats(self) -> Dict[str, object]:
        async with self._lock:
            timestamp = datetime.now(timezone.utc).isoformat()
            groups_snapshot = []
            total_pairs = 0
            total_connects = 0
            total_disconnects = 0
            for group_id, pairs in self._groups.items():
                metrics = self._metrics.setdefault(group_id, GroupMetrics(group_id=group_id, pairs=list(pairs)))
                metrics.pairs = list(pairs)
                snap = metrics.snapshot(self.max_pairs_per_connection)
                total_pairs += snap["pair_count"]
                total_connects += metrics.connects
                total_disconnects += metrics.disconnects
                groups_snapshot.append(snap)
            return {
                "timestamp": timestamp,
                "max_pairs_per_connection": self.max_pairs_per_connection,
                "group_count": len(groups_snapshot),
                "total_pairs": total_pairs,
                "total_connects": total_connects,
                "total_disconnects": total_disconnects,
                "groups": groups_snapshot,
            }


_handler = BinanceWebSocketHandler()


async def start_websocket_handler() -> None:
    await _handler.start()


async def stop_websocket_handler() -> None:
    await _handler.stop()


async def update_stream_pairs(pairs: Sequence[Tuple[str, str]]) -> None:
    await _handler.update_pairs(pairs)


__all__ = [
    "BinanceWebSocketHandler",
    "start_websocket_handler",
    "stop_websocket_handler",
    "update_stream_pairs",
]
