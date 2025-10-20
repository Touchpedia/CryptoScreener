from __future__ import annotations
from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from ...core.db import get_session
from ...core.config import get_settings

router = APIRouter(prefix="/db", tags=["admin"])

def _check(token: str | None) -> None:
    settings = get_settings()
    if not settings.admin_token or token != settings.admin_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid admin token")

@router.post("/flush")
async def flush_db(x_admin_token: str | None = Header(default=None), session: AsyncSession = Depends(get_session)):
    _check(x_admin_token)
    await session.execute(text("TRUNCATE TABLE candles RESTART IDENTITY"))
    await session.commit()
    return {"ok": True, "message": "candles truncated"}
