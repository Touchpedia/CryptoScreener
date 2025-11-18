"""
Shared constants for the Binance candle ingestion stack.
"""

TF_SECONDS = {
    "1m": 60,
    "3m": 3 * 60,
    "5m": 5 * 60
}

# Conservative per-request candle limit (Binance maximum is 1000).
BINANCE_MAX_LIMIT = 1000

# Default timeframes we want to archive for strategies/backtesting.
DEFAULT_TIMEFRAMES = ["1m", "3m", "5m"]  # Only these timeframes are supported  # Only these timeframes supported

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
