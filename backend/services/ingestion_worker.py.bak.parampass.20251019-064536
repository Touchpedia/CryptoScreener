from __future__ import annotations

import asyncio
import logging
import platform
from typing import Optional, Sequence

from backend.services.ingestion_service import get_ingestion_service

logger = logging.getLogger(__name__)


def _ensure_loop_policy() -> None:
    # Windows pe RQ + asyncio ke liye selector policy safe rehti hai
    if platform.system() == "Windows":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass


def run_ingestion_job(
    symbols: Optional[Sequence[str]] = None,
    timeframes: Optional[Sequence[str]] = None,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    run_id: Optional[str] = None,
) -> None:
    """
    RQ worker entrypoint (sync). Iske andar hum async ingest ko safely run karte hain.
    """
    _ensure_loop_policy()

    svc = get_ingestion_service()

    async def _main() -> None:
        await svc.ingest(
            run_id=run_id or "run-unknown",
            symbols=list(symbols or []),
            timeframes=list(timeframes or []),
            start_ts=start_ts,
            end_ts=end_ts,
        )

    try:
        # SimpleWorker me loop running nahi hota; safety ke liye check.
        try:
            loop = asyncio.get_running_loop()
            loop.run_until_complete(_main())  # type: ignore[attr-defined]
        except RuntimeError:
            asyncio.run(_main())
    except Exception as exc:  # pragma: no cover
        logger.exception("Ingestion job failed: %s", exc)
        raise
