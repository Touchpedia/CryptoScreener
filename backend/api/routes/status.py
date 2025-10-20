import redis
from ...core.config import get_settings
from sqlalchemy import text
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Dict, Optional

from ...core.db import get_session
from ...services.ingestion_service import get_ingestion_service, StatusUpdate

router = APIRouter(prefix='/status', tags=['status'])

class UpdatePayload(BaseModel):
    pair: str
    status: Optional[str] = None
    gaps: Optional[int] = None
    progress: Optional[float] = None
    timeframes: Optional[Dict[str, float]] = None

@router.get('')
async def get_status(session: AsyncSession = Depends(get_session)):
    svc = get_ingestion_service()
    return await svc.get_status(session)

@router.post('/update')
async def post_update(payload: UpdatePayload, session: AsyncSession = Depends(get_session)):
    svc = get_ingestion_service()
    return await svc.apply_update(session, StatusUpdate(**payload.model_dump()))

@router.get("/health")
async def health(session: AsyncSession = Depends(get_session)):
    db_ok = True
    redis_ok = True
    db_error = None
    redis_error = None

    try:
        await session.execute(text("SELECT 1"))
    except Exception as exc:
        db_ok = False
        db_error = str(exc)

    try:
        r = redis.from_url(get_settings().REDIS_URL)
        r.ping()
    except Exception as exc:
        redis_ok = False
        redis_error = str(exc)

    overall = "ok" if (db_ok and redis_ok) else ("degraded" if (db_ok or redis_ok) else "down")
    return {
        "ok": overall == "ok",
        "status": overall,
        "db": {"ok": db_ok, "error": db_error},
        "redis": {"ok": redis_ok, "error": redis_error},
    }
