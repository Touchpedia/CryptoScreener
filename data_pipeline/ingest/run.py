from __future__ import annotations
import asyncio
from typing import Iterable, Sequence
import redis

from backend.core.config import get_settings
from backend.core.db import AsyncSessionLocal
from backend.repositories.pair_repository import PairRepository

settings = get_settings()
CACHE_KEY = "status.snapshot"
NAMESPACE = settings.redis_namespace or "dp"

def _invalidate_cache():
    try:
        r = redis.from_url(settings.REDIS_URL)
        r.delete(f"{NAMESPACE}:{CACHE_KEY}")
        r.close()
    except Exception:
        pass

async def _run_async(symbols: Sequence[str], timeframes: Sequence[str]):
    repo = PairRepository()
    async with AsyncSessionLocal() as session:
        total = len(symbols)
        for idx, sym in enumerate(symbols, start=1):
            pct = round((idx / total) * 100, 2)   # demo progress
            await repo.upsert(
                session,
                pair=sym,
                status="running" if pct < 100 else "done",
                gaps=0,
                progress=pct,
                timeframes={tf: pct/100 for tf in timeframes},
            )
            await session.commit()
            _invalidate_cache()
            await asyncio.sleep(0.01)  # small pause for visible updates

def run_ingestion(
    symbols: Iterable[str] | None = None,
    timeframes: Iterable[str] | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> None:
    _ = (start_ts, end_ts)  # preserved for compatibility; not used in demo pipeline
    default_symbols = [f"COIN{i}USDT" for i in range(1, 301)] if symbols is None else list(symbols)
    default_tfs = ["1m", "5m", "1h"] if timeframes is None else list(timeframes)
    asyncio.run(_run_async(default_symbols, default_tfs))
