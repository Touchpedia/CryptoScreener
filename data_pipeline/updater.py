"""
Streaming updater for Binance candles with TimescaleDB persistence.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Sequence

import pandas as pd

from .binance_api import fetch_ohlcv, fetch_top_symbols, init_exchange
from .config import PipelineConfig, load_config
from .constants import BINANCE_MAX_LIMIT, HISTORY_WINDOWS_DAYS, TF_SECONDS
from .db import (
    count_candles,
    delete_oldest,
    get_last_ts,
    pg_session,
    upsert_candles,
    update_last_ts,
)
from .validator import backfill_gap, find_gaps_for_pair

LOGGER_NAME = "data_pipeline.updater"


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/update.log")
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def _tf_delta(timeframe: str) -> timedelta:
    return timedelta(seconds=TF_SECONDS.get(timeframe, 60))


def _history_target(timeframe: str) -> datetime:
    days = HISTORY_WINDOWS_DAYS.get(timeframe, 365)
    return datetime.now(timezone.utc) - timedelta(days=days)


def _next_since_ms(last_ts: Optional[datetime], timeframe: str, overlap_minutes: int) -> Optional[int]:
    if last_ts is None:
        return None
    delta = _tf_delta(timeframe)
    base = last_ts + delta
    if overlap_minutes > 0:
        base -= timedelta(minutes=overlap_minutes)
    return int(base.timestamp() * 1000)


def _df_to_rows(exchange: str, symbol: str, timeframe: str, df: pd.DataFrame) -> List[tuple]:
    rows: List[tuple] = []
    for _, row in df.iterrows():
        ts_val = datetime.fromtimestamp(int(row["ts"]) / 1000.0, tz=timezone.utc)
        rows.append(
            (
                exchange,
                symbol,
                timeframe,
                ts_val,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                float(row["buy_quote"]) if not pd.isna(row.get("buy_quote")) else None,
                float(row["sell_quote"]) if not pd.isna(row.get("sell_quote")) else None,
            )
        )
    return rows


def _fetch_with_backoff(cfg: PipelineConfig, func, *args, **kwargs):
    attempt = 0
    delay = cfg.request_cooldown_sec
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            attempt += 1
            if attempt > cfg.retry_attempts:
                raise
            sleep_for = min(delay * (2**attempt), 60.0)
            time.sleep(sleep_for)


def _apply_retention(
    conn,
    cfg: PipelineConfig,
    symbol: str,
    timeframe: str,
    inserted: int,
    retain_override: Optional[int],
) -> None:
    if inserted <= 0:
        return
    if retain_override:
        total = count_candles(conn, cfg.exchange, symbol, timeframe)
        overflow = max(0, total - retain_override)
        if overflow > 0:
            delete_oldest(conn, cfg.exchange, symbol, timeframe, overflow)
        return
    delete_oldest(conn, cfg.exchange, symbol, timeframe, inserted)


def _sync_symbol_timeframe(
    cfg: PipelineConfig,
    exchange_api,
    conn,
    symbol: str,
    timeframe: str,
    lookback_minutes: int,
) -> int:
    last_ts = get_last_ts(conn, cfg.exchange, symbol, timeframe)
    since_ms = _next_since_ms(last_ts, timeframe, overlap_minutes=lookback_minutes)
    if since_ms is None:
        target_start = _history_target(timeframe)
        since_ms = int(target_start.timestamp() * 1000)

    limit = min(cfg.batch_size, BINANCE_MAX_LIMIT)
    total_inserted = 0

    while True:
        df = _fetch_with_backoff(
            cfg,
            fetch_ohlcv,
            exchange_api,
            symbol,
            timeframe,
            limit=limit,
            since=since_ms,
        )
        if df.empty:
            break

        rows = _df_to_rows(cfg.exchange, symbol, timeframe, df)
        inserted = upsert_candles(conn, rows)
        if inserted:
            latest_ts = datetime.fromtimestamp(int(df["ts"].max()) / 1000.0, tz=timezone.utc)
            update_last_ts(conn, cfg.exchange, symbol, timeframe, latest_ts)
        total_inserted += inserted

        if len(df) < limit:
            break

        since_ms = int(df["ts"].max()) + TF_SECONDS.get(timeframe, 60) * 1000
        time.sleep(cfg.request_cooldown_sec)

    return total_inserted


def update_timeframe(
    cfg: PipelineConfig,
    timeframe: str,
    retain: Optional[int],
    lookback_minutes: int,
) -> None:
    logger = setup_logger()
    logger.info("Starting update for timeframe %s (retain=%s, lookback=%s)", timeframe, retain, lookback_minutes)
    exchange_api = init_exchange()

    symbols = fetch_top_symbols(exchange_api, cfg.quote_asset, cfg.top_symbols)
    with pg_session(cfg) as conn:
        for symbol in symbols:
            try:
                inserted = _sync_symbol_timeframe(cfg, exchange_api, conn, symbol, timeframe, lookback_minutes)
                if inserted:
                    _apply_retention(conn, cfg, symbol, timeframe, inserted, retain)
                    gaps = find_gaps_for_pair(conn, cfg.exchange, symbol, timeframe, lookback_limit=retain or 2000)
                    if gaps:
                        logger.info("%s %s: detected %d gap(s) post-insert, attempting backfill", symbol, timeframe, len(gaps))
                        for gap in gaps:
                            try:
                                replenished = backfill_gap(cfg, exchange_api, conn, symbol, timeframe, gap)
                                if replenished:
                                    _apply_retention(conn, cfg, symbol, timeframe, replenished, retain)
                            except Exception as gap_exc:
                                logger.error("Gap backfill failed for %s %s: %s", symbol, timeframe, gap_exc)
                logger.info("%s %s: inserted=%d", symbol, timeframe, inserted)
            except Exception as exc:
                logger.error("Symbol update failed for %s %s: %s", symbol, timeframe, exc)
                continue

    logger.info("Completed update for timeframe %s", timeframe)


def cli(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="TimescaleDB candle updater")
    parser.add_argument(
        "--interval",
        required=True,
        choices=["1m", "3m", "5m"],
        help="Timeframe interval to update.",
    )
    parser.add_argument(
        "--retain",
        type=int,
        help="Keep only N most recent candles per symbol/timeframe.",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=60,
        help="Overlap lookback window in minutes (default 60).",
    )
    args = parser.parse_args(argv)

    cfg = load_config()
    update_timeframe(cfg, args.interval, retain=args.retain, lookback_minutes=args.lookback)


if __name__ == "__main__":
    cli()

