import json
import os
import time

import ccxt
import redis
from fastapi import APIRouter, Query
from redis import Redis
from rq import Queue

REDIS_HOST = os.getenv("REDIS_HOST", "cs_redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

_r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
CHAN = "ingestion_state"
KEY = "ingestion_running"
ACTIVE_SET_KEY = os.getenv("INGESTION_ACTIVE_SET", "ingestion_active_pairs")
ACTIVE_PREFIX = os.getenv("INGESTION_ACTIVE_PREFIX", "ingestion_active_detail:")

_SYMBOL_CACHE: dict[str, dict[str, object]] = {}
_SYMBOL_CACHE_TTL = int(os.getenv("SYMBOL_CACHE_TTL", "300"))

router = APIRouter(prefix="/api/ingestion", tags=["ingestion"])

DEFAULT_USDT_SYMBOLS: list[str] = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "TON/USDT",
    "TRX/USDT",
    "LINK/USDT",
    "DOT/USDT",
    "MATIC/USDT",
    "LTC/USDT",
    "BCH/USDT",
    "AVAX/USDT",
    "XLM/USDT",
    "UNI/USDT",
    "ATOM/USDT",
    "ETC/USDT",
    "APT/USDT",
    "NEAR/USDT",
    "OP/USDT",
    "ARB/USDT",
    "TIA/USDT",
    "INJ/USDT",
    "SUI/USDT",
    "SEI/USDT",
    "FIL/USDT",
    "AAVE/USDT",
    "ALGO/USDT",
    "FTM/USDT",
    "RUNE/USDT",
]

def _cache_key(segment: str | None) -> str:
    return (segment or "all").lower()


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_symbols(segment: str | None = None) -> list[str]:
    key = _cache_key(segment)
    now = time.time()
    cached = _SYMBOL_CACHE.get(key)
    if cached and (now - float(cached.get("fetched_at", 0.0))) < _SYMBOL_CACHE_TTL:
        return list(cached.get("symbols") or [])

    ex = ccxt.binance({"enableRateLimit": True})
    sort_key = key
    symbols: list[str] = []

    try:
        if sort_key == "all":
            markets = ex.load_markets()
            symbols = sorted(sym for sym in markets if sym.endswith("/USDT"))
        else:
            tickers = ex.fetch_tickers()
            ranked: list[tuple[str, float]] = []
            for sym, ticker in tickers.items():
                if not sym.endswith("/USDT"):
                    continue
                info = ticker.get("info") or {}
                metric = 0.0
                if sort_key == "market_cap":
                    metric = _safe_float(
                        info.get("marketCap")
                        or info.get("market_cap")
                        or info.get("circulating_market_cap")
                    )
                    if metric == 0.0:
                        last_price = _safe_float(ticker.get("last"))
                        base_volume = _safe_float(ticker.get("baseVolume"))
                        metric = last_price * base_volume
                elif sort_key == "volume":
                    metric = _safe_float(
                        ticker.get("quoteVolume")
                        or ticker.get("baseVolume")
                        or info.get("quoteVolume")
                        or info.get("volume")
                    )
                elif sort_key in {"gainers", "losers"}:
                    metric = _safe_float(
                        ticker.get("percentage")
                        or info.get("priceChangePercent")
                        or info.get("priceChangePercent24h")
                    )
                else:
                    metric = 0.0
                ranked.append((sym, metric))

            reverse = sort_key in {"market_cap", "volume", "gainers"}
            ranked.sort(key=lambda item: item[1], reverse=reverse)

            if sort_key == "losers":
                ranked = [item for item in ranked if item[1] != 0.0] + [
                    item for item in ranked if item[1] == 0.0
                ]

            symbols = [sym for sym, _ in ranked]

        if not symbols:
            raise ValueError("empty symbol list from exchange")

        _SYMBOL_CACHE[key] = {"symbols": symbols, "fetched_at": now}
        return symbols
    except Exception as exc:
        print(f"?? failed to load symbols (segment={segment}): {exc}")
        if cached:
            return list(cached.get("symbols") or [])
        fallback = _SYMBOL_CACHE.get("all")
        if fallback:
            return list(fallback.get("symbols") or [])
        return list(DEFAULT_USDT_SYMBOLS)


def _get() -> bool:
    try:
        return _r.get(KEY) == "1"
    except Exception:
        return False


def _set(val: bool) -> None:
    try:
        _r.set(KEY, "1" if val else "0")
        _r.publish(CHAN, json.dumps({"running": val}))
    except Exception:
        pass


def _purge_queue() -> int:
    try:
        r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        q = Queue("ingestion-tasks", connection=r)
        n = q.count
        q.empty()
        return n
    except Exception:
        return -1


@router.get("/status")
async def status():
    return {"ok": True, "running": _get()}


def _active_items() -> list[dict]:
    try:
        tokens = _r.smembers(ACTIVE_SET_KEY)
    except Exception:
        return []
    items: list[dict] = []
    for token in tokens:
        detail_key = f"{ACTIVE_PREFIX}{token}"
        raw = _r.get(detail_key)
        if not raw:
            _r.srem(ACTIVE_SET_KEY, token)
            continue
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                items.append(payload)
                continue
        except Exception:
            pass
        # fallback if payload missing or malformed
        if "::" in token:
            sym, tf = token.split("::", 1)
        else:
            sym, tf = token, ""
        items.append({"symbol": sym, "timeframe": tf})
    return items


@router.get("/active")
async def active():
    try:
        return {"ok": True, "items": _active_items()}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "items": []}


@router.get("/symbols")
async def symbols(segment: str = Query("all")):
    cleaned = segment.lower()
    if cleaned not in {"all", "market_cap", "volume", "gainers", "losers"}:
        cleaned = "all"
    symbols = _load_symbols(cleaned)
    fallback_used = False
    if not symbols:
        symbols = list(DEFAULT_USDT_SYMBOLS)
        fallback_used = True
    return {"ok": True, "segment": cleaned, "symbols": symbols, "fallback": fallback_used}


@router.post("/start")
async def start():
    if _get():
        return {"ok": True, "running": True, "msg": "already running"}
    _set(True)
    return {"ok": True, "running": True}


@router.post("/stop")
async def stop():
    if not _get():
        purged = _purge_queue()
        return {"ok": True, "running": False, "msg": "already stopped", "purged": purged}
    _set(False)
    purged = _purge_queue()
    return {"ok": True, "running": False, "purged": purged}


@router.post("/purge")
async def purge():
    purged = _purge_queue()
    return {"ok": True, "purged": purged}
