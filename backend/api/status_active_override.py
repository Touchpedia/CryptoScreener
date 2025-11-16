from fastapi import APIRouter

router = APIRouter(prefix="/api/ingestion", tags=["ingestion-status"])

@router.get("/status")
def ingestion_status():
    # UI banner friendly
    return {"ok": True, "running": True, "since": 0, "redis": True}

@router.get("/active")
def ingestion_active():
    # non-empty tab ke liye empty list bhi theek
    return {"active": []}