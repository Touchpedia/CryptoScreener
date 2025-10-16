from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.pair_progress import PairProgress


class PairRepository:
    async def list_pairs(self, session: AsyncSession) -> List[PairProgress]:
        result = await session.execute(select(PairProgress).order_by(PairProgress.pair.asc()))
        return list(result.scalars().all())

    async def get_pair(self, session: AsyncSession, pair: str) -> Optional[PairProgress]:
        return await session.get(PairProgress, pair)

    async def upsert(
        self,
        session: AsyncSession,
        *,
        pair: str,
        status: Optional[str] = None,
        gaps: Optional[int] = None,
        progress: Optional[float] = None,
        timeframes: Optional[Dict[str, float]] = None,
    ) -> PairProgress:
        instance = await self.get_pair(session, pair)
        now = datetime.now(timezone.utc)

        if instance is None:
            instance = PairProgress(
                pair=pair,
                status=status or "idle",
                gaps=gaps or 0,
                progress=progress,
                timeframes=timeframes or {},
                updated_at=now,
            )
            session.add(instance)
        else:
            if status is not None:
                instance.status = status
            if gaps is not None:
                instance.gaps = gaps
            if progress is not None:
                instance.progress = progress
            if timeframes:
                current = dict(instance.timeframes or {})
                current.update({k: float(v) for k, v in timeframes.items()})
                instance.timeframes = current
            instance.updated_at = now

        await session.flush()
        await session.refresh(instance)
        return instance
