import os, json
from fastapi import APIRouter
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "cs_redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

_r   = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
CHAN = "ingestion_state"
KEY  = "ingestion_running"

router = APIRouter(prefix="/api/ingestion", tags=["ingestion"])

def _get() -> bool:
    return _r.get(KEY) == "1"

def _set(val: bool) -> None:
    _r.set(KEY, "1" if val else "0")
    _r.publish(CHAN, json.dumps({"running": val}))

@router.get("/status")
async def status():
    return {"ok": True, "running": _get()}

@router.post("/start")
async def start():
    if _get():
        return {"ok": True, "running": True, "msg": "already running"}
    _set(True)
    return {"ok": True, "running": True}

@router.post("/stop")
async def stop():
    if not _get():
        return {"ok": True, "running": False, "msg": "already stopped"}
    _set(False)
    return {"ok": True, "running": False}
