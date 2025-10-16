from __future__ import annotations

import asyncio
from typing import Any, Dict, Set

from fastapi import WebSocket


class WebSocketManager:
    def __init__(self) -> None:
        self._connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        stale: Set[WebSocket] = set()
        async with self._lock:
            for connection in list(self._connections):
                try:
                    await connection.send_json(message)
                except Exception:
                    stale.add(connection)
            for connection in stale:
                self._connections.discard(connection)

    async def active(self) -> int:
        async with self._lock:
            return len(self._connections)
