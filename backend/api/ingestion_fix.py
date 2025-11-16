from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/ingestion", tags=["ingestion-fix"])

_DUMMY = [
    "BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT",
    "ADA/USDT","DOGE/USDT","TRX/USDT","MATIC/USDT","DOT/USDT"
]

def _try_load_symbols_from_binance(top: int):
    try:
        import ccxt  # lazy import (server boot par crash na ho)
        ex = ccxt.binance({"enableRateLimit": True, "options": {"adjustForTimeDifference": True}})
        ex.load_markets()
        tickers = ex.fetch_tickers()
        rows = []
        for sym, t in tickers.items():
            if not sym.endswith("/USDT"):
                continue
            vol = 0.0
            if isinstance(t, dict):
                vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try:
                vol = float(vol)
            except Exception:
                vol = 0.0
            rows.append((sym, vol))
        rows.sort(key=lambda x: x[1], reverse=True)
        return [s for (s, _) in rows[:max(1, min(top, 200))]]
    except Exception:
        return None  # fallback to dummy

@router.get("/scan")
def scan(segment: str = Query("volume"), top: int = Query(25, ge=1, le=200)):
    syms = _try_load_symbols_from_binance(top)
    if not syms:
        syms = _DUMMY[:top]
    return {"segment": segment, "count": len(syms), "symbols": syms}

@router.get("/symbols_clean")
def symbols_clean(segment: str = Query("volume"), top: int = Query(50, ge=1, le=200)):
    out = scan(segment=segment, top=top)
    return {"ok": True, "segment": out["segment"], "symbols": out["symbols"]}