from fastapi import APIRouter, Query
import os, time
from typing import List

router = APIRouter(prefix="/api/ingestion", tags=["ingestion"])

# simple in-memory cache
_SYMBOL_CACHE = {}
_CACHE_TTL = int(os.getenv("SYMBOL_CACHE_TTL", "300"))

def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def _load_symbols(segment: str) -> List[str]:
    """
    segment: one of all | market_cap | volume | gainers | losers
    returns a list of symbols (USDT pairs)
    """
    key = segment.lower() if segment in {"all","market_cap","volume","gainers","losers"} else "all"
    now = time.time()
    cached = _SYMBOL_CACHE.get(key)
    if cached and now - cached["ts"] < _CACHE_TTL:
        return list(cached["symbols"])

    try:
        import ccxt
        ex = ccxt.binance({"enableRateLimit": True})

        if key == "all":
            markets = ex.load_markets()
            symbols = sorted([s for s in markets if s.endswith("/USDT")])
        else:
            tickers = ex.fetch_tickers()
            ranked = []
            for sym, t in tickers.items():
                if not sym.endswith("/USDT"):
                    continue
                info = t.get("info") or {}
                metric = 0.0
                if key == "market_cap":
                    metric = _safe_float(info.get("marketCap") or info.get("market_cap") or info.get("circulating_market_cap"))
                    if metric == 0.0:
                        metric = _safe_float(t.get("last")) * _safe_float(t.get("baseVolume"))
                elif key == "volume":
                    metric = _safe_float(t.get("quoteVolume") or t.get("baseVolume") or info.get("quoteVolume") or info.get("volume"))
                elif key in {"gainers","losers"}:
                    metric = _safe_float(t.get("percentage") or info.get("priceChangePercent") or info.get("priceChangePercent24h"))
                ranked.append((sym, metric))
            reverse = key in {"market_cap","volume","gainers"}
            ranked.sort(key=lambda x: x[1], reverse=reverse)
            if key == "losers":
                ranked = [r for r in ranked if r[1] != 0.0] + [r for r in ranked if r[1] == 0.0]
            symbols = [s for s,_ in ranked]
        if not symbols:
            symbols = []
    except Exception as e:
        print(f"[symbols] fallback due to error: {e}")
        symbols = []

    _SYMBOL_CACHE[key] = {"symbols": symbols, "ts": now}
    return symbols

@router.get("/symbols")
def symbols(segment: str = Query("all"), limit: int = Query(50, ge=1, le=500)):
    seg = segment.lower()
    if seg not in {"all","market_cap","volume","gainers","losers"}:
        seg = "all"
    syms = _load_symbols(seg)
    return {"ok": True, "segment": seg, "symbols": syms[:limit]}
