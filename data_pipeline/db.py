"""
PostgreSQL / TimescaleDB helpers for persisting Binance candle data.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple

import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore
import psycopg2.pool  # type: ignore
from psycopg2.extensions import connection as PGConnection  # type: ignore

from .config import PipelineConfig

CANDLE_COLUMNS = (
    "exchange",
    "symbol",
    "timeframe",
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "taker_buy_quote",
    "taker_sell_quote",
)


def get_connection(cfg: PipelineConfig) -> PGConnection:
    return psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        user=cfg.db_user,
        password=cfg.db_pass,
        dbname=cfg.db_name,
    )


def create_pool(cfg: PipelineConfig, minconn: int = 1, maxconn: int = 5) -> psycopg2.pool.SimpleConnectionPool:
    return psycopg2.pool.SimpleConnectionPool(
        minconn,
        maxconn,
        host=cfg.db_host,
        port=cfg.db_port,
        user=cfg.db_user,
        password=cfg.db_pass,
        dbname=cfg.db_name,
    )


def ensure_schema(conn: PGConnection) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS candles (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                taker_buy_quote DOUBLE PRECISION,
                taker_sell_quote DOUBLE PRECISION,
                PRIMARY KEY(exchange, symbol, timeframe, ts)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_state (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                last_ts TIMESTAMPTZ,
                PRIMARY KEY(exchange, symbol, timeframe)
            );
            """
        )
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_name = 'candles'
            );
            """
        )
        is_hypertable = cur.fetchone()[0]
        if not is_hypertable:
            try:
                cur.execute("SELECT create_hypertable('candles', by_range('ts'), if_not_exists => TRUE);")
            except Exception:
                cur.execute("SELECT create_hypertable('candles', 'ts', if_not_exists => TRUE);")
        cur.execute(
            """
            DO $$
            BEGIN
                PERFORM add_retention_policy('candles', INTERVAL '90 days');
            EXCEPTION
                WHEN duplicate_object THEN NULL;
                WHEN undefined_function THEN NULL;
            END;
            $$;
            """
        )
    conn.commit()


def upsert_candles(
    conn: PGConnection,
    rows: Iterable[Tuple[str, str, str, datetime, float, float, float, float, float, Optional[float], Optional[float]]],
) -> int:
    payload = list(rows)
    if not payload:
        return 0
    sql = (
        "INSERT INTO candles (exchange, symbol, timeframe, ts, open, high, low, close, volume, taker_buy_quote, taker_sell_quote) "
        "VALUES %s "
        "ON CONFLICT (exchange, symbol, timeframe, ts) DO UPDATE SET "
        "open = EXCLUDED.open,"
        "high = EXCLUDED.high,"
        "low = EXCLUDED.low,"
        "close = EXCLUDED.close,"
        "volume = EXCLUDED.volume,"
        "taker_buy_quote = EXCLUDED.taker_buy_quote,"
        "taker_sell_quote = EXCLUDED.taker_sell_quote;"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, payload, page_size=1000)
    conn.commit()
    return len(payload)


def get_last_ts(
    conn: PGConnection,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_ts
            FROM ingestion_state
            WHERE exchange = %s AND symbol = %s AND timeframe = %s;
            """,
            (exchange, symbol, timeframe),
        )
        row = cur.fetchone()
    return row[0] if row else None


def update_last_ts(
    conn: PGConnection,
    exchange: str,
    symbol: str,
    timeframe: str,
    last_ts: datetime,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_state (exchange, symbol, timeframe, last_ts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(exchange, symbol, timeframe)
            DO UPDATE SET last_ts = EXCLUDED.last_ts;
            """,
            (exchange, symbol, timeframe, last_ts),
        )
    conn.commit()


def fetch_oldest_ts(
    conn: PGConnection,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts
            FROM candles
            WHERE exchange = %s AND symbol = %s AND timeframe = %s
            ORDER BY ts ASC
            LIMIT 1;
            """,
            (exchange, symbol, timeframe),
        )
        row = cur.fetchone()
    return row[0] if row else None


def count_candles(
    conn: PGConnection,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM candles
            WHERE exchange = %s AND symbol = %s AND timeframe = %s;
            """,
            (exchange, symbol, timeframe),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


def delete_oldest(
    conn: PGConnection,
    exchange: str,
    symbol: str,
    timeframe: str,
    count: int,
) -> int:
    if count <= 0:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM candles
            WHERE ctid IN (
                SELECT ctid FROM candles
                WHERE exchange = %s AND symbol = %s AND timeframe = %s
                ORDER BY ts ASC
                LIMIT %s
            );
            """,
            (exchange, symbol, timeframe, count),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


@contextmanager
def pg_session(cfg: PipelineConfig):
    conn = get_connection(cfg)
    try:
        ensure_schema(conn)
        yield conn
    finally:
        conn.close()


@contextmanager
def pooled_connection(pool: psycopg2.pool.AbstractConnectionPool):
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)
