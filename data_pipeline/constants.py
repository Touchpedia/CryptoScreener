"""
Shared constants for the Binance candle ingestion stack.
"""

TF_SECONDS = {
    "1m": 60,
    "3m": 3 * 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "2h": 2 * 60 * 60,
    "4h": 4 * 60 * 60,
    "6h": 6 * 60 * 60,
    "8h": 8 * 60 * 60,
    "12h": 12 * 60 * 60,
    "1d": 24 * 60 * 60,
}

# Conservative per-request candle limit (Binance maximum is 1000).
BINANCE_MAX_LIMIT = 1000

# Default timeframes we want to archive for strategies/backtesting.
DEFAULT_TIMEFRAMES = ["1m", "3m", "5m"]

# Stablecoin tickers to exclude from the download universe (base asset).
STABLE_BASES = {
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "SUSD",
    "USDD",
    "USTC",
    "GUSD",
    "PAX",
}

# Historical coverage targets per timeframe.
HISTORY_WINDOWS_DAYS = {
    "1m": 365,
    "3m": 365 * 3,
    "5m": 365 * 5,
}
