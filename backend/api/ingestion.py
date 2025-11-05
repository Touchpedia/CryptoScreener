from fastapi import APIRouter
from typing import List, Optional
from datetime import datetime, timezone

# NOTE: prefix ONLY '/ingestion' because parent router already has '/api'
ingestion = APIRouter(prefix="/ingestion", tags=["ingestion"])

@ingestion.get("/status")
def get_status():
    return {"running": True, "since": datetime.now(timezone.utc).isoformat(), "active_pairs": 0}

@ingestion.get("/active")
def get_active():
    return {"active": []}

@ingestion.get("/scan")
def scan_symbols(segment: Optional[str] = None, top: Optional[int] = None):
    symbols: List[str] = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT"]
    return {"segment": segment or "all", "count": len(symbols), "symbols": symbols[: top or len(symbols)]}

@ingestion.get("/symbols_clean")
def symbols_clean(segment: Optional[str] = None, top: Optional[int] = 50):
    syms = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","LINKUSDT","TONUSDT","DOTUSDT",
            "AVAXUSDT","TRXUSDT","MATICUSDT","LTCUSDT","ATOMUSDT","NEARUSDT","OPUSDT","ARBUSDT","APTUSDT","FILUSDT"]
    return syms[:top]

@ingestion.post("/run")
def run_ingestion(payload: dict):
    return {"ok": True, "job_id": "mock-job-001", "received": payload}
