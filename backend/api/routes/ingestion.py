from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.db import get_session
from ...services.ingestion_service import IngestionRequest, get_ingestion_service

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

DEFAULT_SYMBOLS_FILE = os.getenv("DEFAULT_SYMBOLS_FILE", "symbols.txt")
_BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
_PROJECT_ROOT = _BACKEND_DIR.parent
DEFAULT_SYMBOLS_PROVIDER = os.getenv(
    "SYMBOLS_PROVIDER_URL", "https://api.binance.com/api/v3/ticker/24hr"
)
DEFAULT_SYMBOLS_PROVIDER_QUOTE = os.getenv("SYMBOLS_PROVIDER_QUOTE", "USDT")
DEFAULT_FALLBACK_SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "XRP/USDT",
    "SOL/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "MATIC/USDT",
    "LTC/USDT",
    "DOT/USDT",
]


class RunPayload(BaseModel):
    symbols: Optional[List[str]] = None
    timeframes: Optional[List[str]] = None
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None
    limit: Optional[int] = None
    symbol_source: Optional[str] = None


def load_default_symbols() -> List[str]:
    """Load the default symbols list from file or env fallback."""
    candidates: List[Path] = []
    configured_path = Path(DEFAULT_SYMBOLS_FILE)
    if configured_path.is_absolute():
        candidates.append(configured_path)
    else:
        candidates.extend(
            [
                configured_path,
                _PROJECT_ROOT / configured_path,
                _BACKEND_DIR / configured_path,
            ]
        )

    for path in candidates:
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                data = [line.strip() for line in handle if line.strip()]
                if data:
                    return data

    env_symbols = os.getenv("DEFAULT_SYMBOLS", "")
    return [item.strip() for item in env_symbols.split(",") if item.strip()]


def generate_fallback_symbols(limit: int | None = None) -> List[str]:
    """Final fallback so ingestion always has something to process."""
    if not DEFAULT_FALLBACK_SYMBOLS:
        return ["BTC/USDT"]
    max_items = limit if limit and limit > 0 else len(DEFAULT_FALLBACK_SYMBOLS)
    repeats = (max_items + len(DEFAULT_FALLBACK_SYMBOLS) - 1) // len(DEFAULT_FALLBACK_SYMBOLS)
    pool = (DEFAULT_FALLBACK_SYMBOLS * repeats)[:max_items]
    return pool


async def fetch_top_symbols(limit: int | None = None) -> List[str]:
    """Fetch top trading symbols from the configured provider."""
    quote = DEFAULT_SYMBOLS_PROVIDER_QUOTE.upper()
    target = DEFAULT_SYMBOLS_PROVIDER
    target_limit = limit if limit and limit > 0 else 50

    try:
        import httpx  # type: ignore
    except ImportError:
        return []

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(target)
            response.raise_for_status()
            payload = response.json()
    except Exception:
        return []

    if not isinstance(payload, Sequence):
        return []

    scored: List[tuple[str, float]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "")
        if not symbol.endswith(quote):
            continue
        try:
            volume = float(item.get("quoteVolume") or 0)
        except (TypeError, ValueError):
            volume = 0.0
        if volume <= 0:
            continue
        scored.append((symbol, volume))

    scored.sort(key=lambda entry: entry[1], reverse=True)
    return [symbol for symbol, _ in scored[:target_limit]]


@router.post("/run")
async def run_ingestion_api(
    payload: RunPayload, _session: AsyncSession = Depends(get_session)
):
    symbols = [sym.strip() for sym in payload.symbols or [] if sym and sym.strip()]
    source = (payload.symbol_source or "default").lower()

    if source == "top":
        symbols = await fetch_top_symbols(payload.limit)

    if not symbols:
        symbols = load_default_symbols()

    if payload.limit and payload.limit > 0:
        symbols = symbols[: payload.limit]

    if not symbols:
        symbols = generate_fallback_symbols(payload.limit)

    if not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No symbols available to ingest. Provide symbols or configure defaults.",
        )

    timeframes = payload.timeframes or ["1m"]

    svc = get_ingestion_service()
    try:
        job_id, run_id = await svc.enqueue_ingestion(
            IngestionRequest(
                symbols=symbols,
                timeframes=timeframes,
                start_ts=payload.start_ts,
                end_ts=payload.end_ts,
            )
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc
    return {"ok": True, "job_id": job_id, "run_id": run_id, "count": len(symbols)}
