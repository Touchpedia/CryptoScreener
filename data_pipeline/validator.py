"""
Data validation & gap backfilling for the Binance candle store.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .binance_api import fetch_ohlcv, init_exchange
from .config import PipelineConfig, load_config
from .constants import TF_SECONDS
from .db import get_last_ts, pg_session, upsert_candles, update_last_ts


@dataclass
class Gap:
    start_ts: datetime
    end_ts: datetime
    missing_bars: int


def _detect_gaps(timestamps: List[datetime], step: timedelta) -> List[Gap]:
    if not timestamps:
        return []
    gaps: List[Gap] = []
    for prev, curr in zip(timestamps, timestamps[1:]):
        diff = curr - prev
        if diff > step * 1.5:  # allow slight jitter
            ratio = diff.total_seconds() / step.total_seconds()
            missing = int(round(ratio)) - 1
            gaps.append(Gap(start_ts=prev + step, end_ts=curr - step, missing_bars=max(missing, 0)))
    return gaps


def find_gaps_for_pair(
    conn,
    exchange: str,
    symbol: str,
    timeframe: str,
    lookback_limit: Optional[int] = None,
) -> List[Gap]:
    base_query = """
        SELECT ts FROM candles
        WHERE exchange = %s AND symbol = %s AND timeframe = %s
    """
    params: Tuple
    if lookback_limit:
        query = (
            f"SELECT ts FROM ({base_query} ORDER BY ts DESC LIMIT %s) sub "
            "ORDER BY ts ASC"
        )
        params = (exchange, symbol, timeframe, lookback_limit)
    else:
        query = base_query + " ORDER BY ts ASC"
        params = (exchange, symbol, timeframe)
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    timestamps = [row[0].astimezone(timezone.utc) for row in rows if row and row[0] is not None]
    return _detect_gaps(timestamps, timedelta(seconds=TF_SECONDS.get(timeframe, 60)))


def backfill_gap(
    cfg: PipelineConfig,
    exchange,
    conn,
    symbol: str,
    timeframe: str,
    gap: Gap,
) -> int:
    tf_ms = TF_SECONDS.get(timeframe, 60) * 1000
    limit_ts = gap.end_ts
    since = int((gap.start_ts - timedelta(milliseconds=tf_ms)).timestamp() * 1000)
    total_inserted = 0
    while since <= int(limit_ts.timestamp() * 1000):
        df = fetch_ohlcv(exchange, symbol, timeframe, limit=1000, since=since)
        if df.empty:
            break
        window_start = int((gap.start_ts - timedelta(milliseconds=tf_ms)).timestamp() * 1000)
        window_end = int((gap.end_ts + timedelta(milliseconds=tf_ms)).timestamp() * 1000)
        df = df[(df["ts"] >= window_start) & (df["ts"] <= window_end)]
        if df.empty:
            break
        rows = [
            (
                cfg.exchange,
                symbol,
                timeframe,
                datetime.fromtimestamp(int(row["ts"]) / 1000.0, tz=timezone.utc),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                float(row["buy_quote"]) if not pd.isna(row.get("buy_quote")) else None,
                float(row["sell_quote"]) if not pd.isna(row.get("sell_quote")) else None,
            )
            for _, row in df.iterrows()
        ]
        inserted = upsert_candles(conn, rows)
        total_inserted += inserted
        since = int(df["ts"].max()) + tf_ms
        time.sleep(cfg.request_cooldown_sec)

    last_ts = get_last_ts(conn, cfg.exchange, symbol, timeframe)
    if last_ts is None or last_ts < gap.end_ts:
        update_last_ts(conn, cfg.exchange, symbol, timeframe, gap.end_ts)
    return total_inserted


def run_validation(
    symbols: Optional[Sequence[str]] = None,
    timeframes: Optional[Sequence[str]] = None,
    auto_backfill: bool = True,
    lookback_limit: Optional[int] = 10000,
) -> None:
    cfg = load_config()
    selected_timeframes = list(timeframes) if timeframes else cfg.timeframes
    exchange = init_exchange()
    with pg_session(cfg) as conn:
        if symbols is None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT symbol FROM candles
                    WHERE exchange = %s
                    ORDER BY symbol ASC
                    """,
                    (cfg.exchange,),
                )
                rows = cur.fetchall()
            universe = [row[0] for row in rows]
        else:
            universe = list(symbols)

        for symbol in universe:
            for tf in selected_timeframes:
                gaps = find_gaps_for_pair(conn, cfg.exchange, symbol, tf, lookback_limit=lookback_limit)
                if not gaps:
                    continue
                print(f"[validator] {symbol} {tf}: detected {len(gaps)} gap(s)", flush=True)
                if not auto_backfill:
                    for gap in gaps:
                        print(f"  missing from {gap.start_ts.isoformat()} to {gap.end_ts.isoformat()} ({gap.missing_bars} bars)")
                    continue
                for gap in gaps:
                    try:
                        inserted = backfill_gap(cfg, exchange, conn, symbol, tf, gap)
                        print(
                            f"[validator] backfilled {symbol} {tf}: gap {gap.start_ts.isoformat()}-{gap.end_ts.isoformat()}, rows {inserted}",
                            flush=True,
                        )
                    except Exception as exc:
                        print(f"[validator] failed to backfill {symbol} {tf}: {exc}", file=sys.stderr, flush=True)
                        time.sleep(cfg.request_cooldown_sec * 2)


def cli(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Validate and backfill Binance candle store")
    parser.add_argument("--symbols", nargs="*", help="Restrict to specific symbols")
    parser.add_argument("--timeframes", nargs="*", help="Restrict to specific timeframes")
    parser.add_argument(
        "--no-backfill",
        action="store_true",
        help="Only report gaps without attempting to backfill",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=10000,
        help="Only inspect the latest N rows per symbol/timeframe (default 10k)",
    )
    args = parser.parse_args(argv)
    run_validation(
        symbols=args.symbols,
        timeframes=args.timeframes,
        auto_backfill=not args.no_backfill,
        lookback_limit=args.lookback,
    )


if __name__ == "__main__":
    cli()
