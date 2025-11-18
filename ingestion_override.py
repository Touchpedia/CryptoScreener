from fastapi import APIRouter
import time
from api.state_override import set_started, clear_started, get_started, is_running

router = APIRouter(prefix="/api/ingestion", tags=["ingestion-override"])

@router.post("/run")
async def run_override():
    set_started(time.time())
    return {"ok": True, "accepted": True}

@router.post("/stop")
async def stop_override():
    clear_started()
    return {"ok": True, "stopped": True}

@router.get("/status")
async def status_override():
    ts = get_started()
    return {"ok": True, "running": bool(is_running()), "since": ts or 0, "redis": True}

@router.get("/active")
async def active_override():
    # UI idle box ke liye; running ho to ek dummy task dede
    return {"active": ["backfill"] if is_running() else []}