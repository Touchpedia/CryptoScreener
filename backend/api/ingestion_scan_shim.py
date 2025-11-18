from fastapi import APIRouter, Query

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

@router.get("/scan")
def scan(segment: str = "all", top: int = 100):
    # TODO: real exchange scan; abhi placeholder list
    symbols = [
        "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
        "ADAUSDT","DOGEUSDT","TRXUSDT","MATICUSDT","DOTUSDT"
    ]
    return {"segment": segment, "count": len(symbols), "symbols": symbols[:top]}