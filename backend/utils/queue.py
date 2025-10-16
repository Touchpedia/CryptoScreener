from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import rq
import redis


def _import_func(path: str) -> Callable[..., Any]:
    # supports 'pkg.mod:func' (preferred) OR 'pkg.mod.func'
    if ":" in path:
        mod, fn = path.split(":", 1)
    else:
        parts = path.split(".")
        mod, fn = ".".join(parts[:-1]), parts[-1]
    module = importlib.import_module(mod)
    return getattr(module, fn)


@dataclass
class TaskResult:
    id: str


class TaskQueue:
    """RQ-backed queue (Redis)."""

    def __init__(self, url: str, name: str) -> None:
        self._connection = redis.from_url(url)
        self._queue = rq.Queue(name, connection=self._connection)

    async def enqueue(
        self,
        func_path: str,
        args: Optional[list] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> str:
        job = self._queue.enqueue(func_path, args=args or [], kwargs=kwargs or {})
        return job.id

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass


class MemoryQueue:
    """In-process fallback (for dev when no Redis)."""

    _counter = 0

    def __init__(self, name: str) -> None:
        self.name = name

    async def enqueue(
        self,
        func_path: str,
        args: Optional[list] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> str:
        MemoryQueue._counter += 1
        job_id = f"local-{MemoryQueue._counter}"

        fn = _import_func(func_path)
        t = threading.Thread(target=fn, args=tuple(args or []), kwargs=kwargs or {}, daemon=True)
        t.start()
        return job_id

    def close(self) -> None:
        pass


def build_queue(url: str, name: str):
    if url and url.startswith(("redis://", "rediss://", "unix://")):
        return TaskQueue(url, name)
    # anything else => memory
    return MemoryQueue(name)
