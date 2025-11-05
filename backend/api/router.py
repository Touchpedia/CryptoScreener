from fastapi import APIRouter
from api.ingestion import ingestion

router = APIRouter(prefix="/api")
router.include_router(ingestion)
@router.get("/status")
def status():
    return {"status": "ok"}
