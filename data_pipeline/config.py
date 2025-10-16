"""
Configuration surface for the candle ingestion pipeline.
Override via environment variables where needed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from .constants import DEFAULT_TIMEFRAMES


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _load_env_file() -> None:
    env_file = os.environ.get("CANDLE_PIPE_ENV_FILE", ".env")
    path = Path(env_file)
    if not path.exists():
        return
    try:
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip()
    except Exception:
        pass

@dataclass(frozen=True)
class PipelineConfig:
    exchange: str
    quote_asset: str
    top_symbols: int
    timeframes: List[str]
    max_threads: int
    db_host: str
    db_port: int
    db_user: str
    db_pass: str
    db_name: str
    batch_size: int
    request_cooldown_sec: float
    retry_attempts: int
    throttle_min: float
    throttle_max: float


def load_config() -> PipelineConfig:
    _load_env_file()
    exchange = _env("CANDLE_PIPE_EXCHANGE", "binance")
    quote = _env("CANDLE_PIPE_QUOTE", "USDT")
    top_symbols = int(_env("CANDLE_PIPE_TOP_SYMBOLS", "300"))
    timeframes = _env(
        "CANDLE_PIPE_TIMEFRAMES",
        ",".join(DEFAULT_TIMEFRAMES),
    ).split(",")
    max_threads = int(_env("MAX_THREADS", "10"))
    batch_size = int(_env("CANDLE_PIPE_BATCH_SIZE", "500"))
    cooldown = float(_env("CANDLE_PIPE_COOLDOWN_SEC", "0.3"))
    retries = int(_env("CANDLE_PIPE_RETRIES", "3"))
    throttle_min = float(_env("THROTTLE_MIN", "0.05"))
    throttle_max = float(_env("THROTTLE_MAX", "2.0"))
    db_host = _env("DB_HOST", "localhost")
    db_port = int(_env("DB_PORT", "5432"))
    db_user = _env("DB_USER", "postgres")
    db_pass = _env("DB_PASS", "")
    db_name = _env("DB_NAME", "candles")

    return PipelineConfig(
        exchange=exchange,
        quote_asset=quote,
        top_symbols=top_symbols,
        timeframes=[tf.strip() for tf in timeframes if tf.strip()],
        max_threads=max_threads,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_pass=db_pass,
        db_name=db_name,
        batch_size=batch_size,
        request_cooldown_sec=cooldown,
        retry_attempts=retries,
        throttle_min=throttle_min,
        throttle_max=throttle_max,
    )
