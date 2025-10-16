from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import DateTime, Float, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ..core.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PairProgress(Base):
    __tablename__ = "pair_progress"

    pair: Mapped[str] = mapped_column(String(length=64), primary_key=True)
    status: Mapped[str] = mapped_column(String(length=16), default="idle")
    gaps: Mapped[int] = mapped_column(Integer, default=0)
    progress: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    timeframes: Mapped[Dict[str, float]] = mapped_column(JSONB, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, server_default=func.now())

    def to_dict(self) -> Dict[str, object]:
        return {
            "pair": self.pair,
            "status": self.status,
            "gaps": self.gaps,
            "progress": self.progress,
            "timeframes": self.timeframes or {},
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }
