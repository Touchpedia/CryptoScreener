"""
Binance-specific helpers for pulling OHLCV + taker buy data via ccxt.
"""

from __future__ import annotations

import math
import time
from typing import Dict, Iterable, List, Optional

import ccxt  # type: ignore
import pandas as pd

from .constants import BINANCE_MAX_LIMIT, STABLE_BASES, TF_SECONDS


def init_exchange(rate_limited: bool = True) -> ccxt.binance:
    ex = ccxt.binance({"enableRateLimit": rate_limited})
    ex.load_markets()
    return ex


def fetch_top_symbols(
    ex: ccxt.binance,
    quote: str,
    top_n: int,
) -> List[str]:
    tickers = ex.fetch_tickers()
    ranked: List[tuple[str, float]] = []
    for symbol, ticker in tickers.items():
        market = ex.markets.get(symbol)
        if not market:
            continue
        if market.get("quote") != quote:
            continue
        if market.get("contract"):
            continue
        if market.get("active") is False:
            continue
        base = str(market.get("base", "")).upper()
        if base in STABLE_BASES:
            continue
        qv = None
        qv_val = ticker.get("quoteVolume")
        if isinstance(qv_val, (int, float)):
            qv = float(qv_val)
        if qv is None:
            info = ticker.get("info") or {}
            for key in ("quoteVolume", "quoteVolume24h", "volumeQuote", "vol"):
                if key in info:
                    try:
                        qv = float(info[key])
                        break
                    except Exception:
                        continue
        if qv is None:
            continue
        ranked.append((symbol, qv))
    ranked.sort(key=lambda item: item[1], reverse=True)
    return [symbol for symbol, _ in ranked[:top_n]]


def _binance_klines(
    ex: ccxt.binance,
    symbol: str,
    timeframe: str,
    limit: int,
    since: Optional[int] = None,
) -> pd.DataFrame:
    params = {
        "symbol": symbol.replace("/", ""),
        "interval": timeframe,
        "limit": min(limit, BINANCE_MAX_LIMIT),
    }
    if since is not None:
        params["startTime"] = since
    data = ex.public_get_klines(params)
    cols = [
        "openTime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "closeTime",
        "quoteAssetVolume",
        "numberOfTrades",
        "takerBuyBaseVolume",
        "takerBuyQuoteVolume",
        "ignore",
    ]
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        return df
    df["ts"] = df["openTime"].astype("int64")
    float_cols = ["open", "high", "low", "close", "volume", "quoteAssetVolume", "takerBuyQuoteVolume"]
    for col in float_cols:
        df[col] = df[col].astype(float)
    df["buy_quote"] = df["takerBuyQuoteVolume"]
    df["sell_quote"] = df["quoteAssetVolume"] - df["buy_quote"]
    return df[["ts", "open", "high", "low", "close", "volume", "quoteAssetVolume", "buy_quote", "sell_quote"]]


def fetch_ohlcv(
    ex: ccxt.binance,
    symbol: str,
    timeframe: str,
    limit: int,
    since: Optional[int] = None,
) -> pd.DataFrame:
    try:
        df = _binance_klines(ex, symbol, timeframe, limit, since)
        if not df.empty:
            df = df.rename(columns={"quoteAssetVolume": "qvol"})
            return df
    except Exception:
        # fallback to generic OHLCV (no taker data)
        pass

    ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit, since=since)
    if not ohlcv:
        return pd.DataFrame()
    df = pd.DataFrame(
        ohlcv,
        columns=["ts", "open", "high", "low", "close", "volume"],
    )
    df["qvol"] = df["close"] * df["volume"]
    df["buy_quote"] = None
    df["sell_quote"] = None
    return df


def calc_required_candles(timeframe: str, lookback_seconds: int, buffer: int = 5) -> int:
    tf_sec = TF_SECONDS.get(timeframe, 60)
    return max(1, math.ceil(lookback_seconds / tf_sec) + buffer)
