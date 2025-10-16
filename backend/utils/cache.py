from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from redis import asyncio as aioredis


class CacheBackend:
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    async def set(self, key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
        raise NotImplementedError

    async def delete(self, key: str) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None


def _serialize(value: Dict[str, Any]) -> str:
    return json.dumps(value, default=_default_serializer)


def _deserialize(payload: Optional[str]) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    return json.loads(payload)


def _default_serializer(obj: Any):
    if isinstance(obj, datetime):
        return obj.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class RedisCache(CacheBackend):
    def __init__(self, url: str, namespace: str = "cache"):
        self._client = aioredis.from_url(url, encoding="utf-8", decode_responses=True)
        self._namespace = namespace

    def _key(self, key: str) -> str:
        return f"{self._namespace}:{key}"

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        payload = await self._client.get(self._key(key))
        return _deserialize(payload)

    async def set(self, key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
        payload = _serialize(value)
        await self._client.set(self._key(key), payload, ex=ttl)

    async def delete(self, key: str) -> None:
        await self._client.delete(self._key(key))

    async def close(self) -> None:
        await self._client.close()


class InMemoryCache(CacheBackend):
    def __init__(self):
        self._store: Dict[str, tuple[Optional[datetime], Dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            expires_at, value = entry
            if expires_at and expires_at < datetime.now(timezone.utc):
                self._store.pop(key, None)
                return None
            return value

    async def set(self, key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
        expires_at = None
        if ttl:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
        async with self._lock:
            self._store[key] = (expires_at, value)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)


def build_cache(url: str, namespace: str) -> CacheBackend:
    if url.startswith("redis://") or url.startswith("rediss://"):
        return RedisCache(url, namespace=namespace)
    return InMemoryCache()
