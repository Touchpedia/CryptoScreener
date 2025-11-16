from fastapi import APIRouter, Query

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

_DUMMY = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","TRXUSDT","MATICUSDT","DOTUSDT"
]

@router.get("/symbols")
def symbols(segment: str = "all", top: int = 100):
    # TODO: real exchange list; abhi placeholder
    return {"ok": True, "segment": segment, "symbols": _DUMMY[:top]}

@router.get("/symbols_clean")
def symbols_clean(segment: str = "all", top: int = 100):
    # UI is endpoint ko call karti hai
    return {"ok": True, "segment": segment, "symbols": _DUMMY[:top]}