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
