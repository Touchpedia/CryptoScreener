"""
Incremental ingestion entrypoint for Binance spot candles.
"""

from __future__ import annotations

import argparse
import math
import sys
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

try:
    from rich.console import Console
except Exception:
    Console = None

import pandas as pd

from .binance_api import fetch_ohlcv, fetch_top_symbols, init_exchange
from .config import PipelineConfig, load_config
from .constants import BINANCE_MAX_LIMIT, HISTORY_WINDOWS_DAYS, TF_SECONDS
from .db import (
    create_pool,
    ensure_schema,
    get_last_ts,
    pooled_connection,
    upsert_candles,
    update_last_ts,
)
from .validator import Gap, find_gaps_for_pair


class RateLimiter:
    def __init__(self, initial_delay: float, throttle_min: float, throttle_max: float):
        self.min_delay = min(throttle_min, throttle_max)
        self.max_delay = max(throttle_min, throttle_max)
        self.delay = self._clamp(initial_delay if initial_delay > 0 else self.min_delay)
        self.lock = threading.Lock()
        self.last_call = 0.0
        self.smooth_counter = 0
        self.diagnostics: Optional["Diagnostics"] = None

    def attach(self, diagnostics: "Diagnostics") -> None:
        self.diagnostics = diagnostics

    def _clamp(self, value: float) -> float:
        return max(self.min_delay, min(self.max_delay, value))

    def wait(self) -> None:
        while True:
            with self.lock:
                now = time.perf_counter()
                target = self.last_call + self.delay
                wait_for = target - now
                if wait_for <= 0:
                    self.last_call = now
                    return
            if wait_for > 0:
                time.sleep(wait_for)

    def on_success(self, response_ms: float) -> None:
        if response_ms > 2000:
            self._adjust(0.05, reason=f"slow>{int(response_ms)}ms", reset_smooth=True)
            return
        with self.lock:
            self.smooth_counter += 1
            reached = self.smooth_counter >= 10
            if reached:
                self.smooth_counter = 0
        if reached:
            self._adjust(-0.05, reason="smooth")

    def on_rate_limit(self) -> None:
        self._adjust(0.05, reason="rate-limit", reset_smooth=True)

    def on_error(self) -> None:
        with self.lock:
            self.smooth_counter = 0

    def _adjust(self, delta: float, reason: str, reset_smooth: bool = False) -> None:
        with self.lock:
            if reset_smooth:
                self.smooth_counter = 0
            new_delay = self._clamp(self.delay + delta)
            if math.isclose(new_delay, self.delay, abs_tol=1e-6):
                return
            self.delay = new_delay
        if self.diagnostics:
            self.diagnostics.log_throttle(new_delay, reason)

    def current_delay(self) -> float:
        with self.lock:
            return self.delay


@dataclass
class FetchContext:
    symbol: str
    timeframe: str
    since_ms: Optional[int]
    tag: str = "fetch"


@dataclass
class PairProgress:
    start: datetime
    target_end: datetime
    last_ts: Optional[datetime] = None
    last_percent: float = -1.0


@dataclass
class PairStats:
    responses: int = 0
    total_ms: float = 0.0

    def average_ms(self) -> float:
        if not self.responses:
            return 0.0
        return self.total_ms / self.responses


class Diagnostics:
    def __init__(self, total_jobs: int, max_threads: int, rate_limiter: RateLimiter):
        self.total_jobs = total_jobs
        self.max_threads = max_threads
        self.rate_limiter = rate_limiter
        self._console = Console() if Console is not None else None
        self._print_lock = threading.Lock()
        self._status_lock = threading.Lock()
        self._progress_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._pair_progress: Dict[Tuple[str, str], PairProgress] = {}
        self._pair_stats: Dict[Tuple[str, str], PairStats] = defaultdict(PairStats)
        self._request_times: deque = deque()
        self._status: Dict[str, int] = {"active": 0, "completed": 0}
        self._summary_interval = 3.0
        self._stop_event = threading.Event()
        self._summary_thread = threading.Thread(target=self._summary_loop, daemon=True)
        self._completed_pairs: Set[Tuple[str, str]] = set()
        self._completion_callback: Optional[Callable[[str, str, int, int], None]] = None

    def start(self) -> None:
        if self.total_jobs == 0:
            return
        if not self._summary_thread.is_alive():
            self._summary_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._summary_thread.is_alive():
            self._summary_thread.join(timeout=1.0)

    def log(self, message: str, stderr: bool = False) -> None:
        with self._print_lock:
            if self._console is not None and not stderr:
                self._console.print(message, highlight=False)
            else:
                stream = sys.stderr if stderr else sys.stdout
                print(message, file=stream, flush=True)

    def log_throttle(self, delay: float, reason: str) -> None:
        self.log(f"[rate] throttle={delay:.2f}s reason={reason}")

    def set_completion_callback(self, callback: Optional[Callable[[str, str, int, int], None]]) -> None:
        with self._status_lock:
            self._completion_callback = callback

    def task_started(self, symbol: str, timeframe: str) -> None:
        with self._status_lock:
            self._status["active"] += 1
            active = self._status["active"]
        self.log(f"[ingest] start {symbol} {timeframe} (active={active})")

    def _finalize_success(
        self, symbol: str, timeframe: str, inserted: int, gaps: int
    ) -> Tuple[int, int]:
        key = (symbol, timeframe)
        callback: Optional[Callable[[str, str, int, int], None]]
        with self._status_lock:
            self._status["active"] = max(0, self._status["active"] - 1)
            if key not in self._completed_pairs:
                self._completed_pairs.add(key)
                self._status["completed"] += 1
            active = self._status["active"]
            completed = self._status["completed"]
            callback = self._completion_callback
        if callback:
            try:
                callback(symbol, timeframe, inserted, gaps)
            except Exception as exc:
                self.log(
                    f"[warn] completion callback failed for {symbol}/{timeframe}: {self._exception_reason(exc)}",
                    stderr=True,
                )
        return active, completed

    def task_completed(self, symbol: str, timeframe: str, inserted: int, gaps: int) -> None:
        active, completed = self._finalize_success(symbol, timeframe, inserted, gaps)
        self.complete_pair(symbol, timeframe)
        avg_ms = self.get_pair_avg_ms(symbol, timeframe)
        avg_display = int(round(avg_ms))
        self.log(
            f"[done ✅] {symbol}/{timeframe} total_inserted={inserted} avg_response={avg_display}ms gaps_filled={gaps} "
            f"({completed}/{self.total_jobs}, active={active})"
        )

    def task_failed(self, symbol: str, timeframe: str, error: Exception) -> None:
        with self._status_lock:
            self._status["active"] = max(0, self._status["active"] - 1)
            self._status["completed"] += 1
            active = self._status["active"]
            completed = self._status["completed"]
        self.log(
            f"[error] {symbol}/{timeframe} {self._exception_reason(error)} "
            f"({completed}/{self.total_jobs}, active={active})",
            stderr=True,
        )

    def register_pair(self, symbol: str, timeframe: str, start: datetime, target_end: datetime) -> None:
        with self._progress_lock:
            self._pair_progress[(symbol, timeframe)] = PairProgress(start=start, target_end=target_end)

    def update_progress(self, symbol: str, timeframe: str, last_ts: datetime) -> None:
        key = (symbol, timeframe)
        with self._progress_lock:
            progress = self._pair_progress.get(key)
            if not progress:
                return
            progress.last_ts = last_ts
            percent = self._percent_complete(progress.start, progress.target_end, last_ts)
            should_emit = percent >= 100.0 or percent >= progress.last_percent + 1.0 or progress.last_percent < 0
            if should_emit:
                progress.last_percent = percent
        if should_emit:
            self.log(f"[ingest] {symbol} {timeframe} {percent:.1f}% (up to {self._fmt_dt(last_ts)})")

    def complete_pair(self, symbol: str, timeframe: str) -> None:
        key = (symbol, timeframe)
        with self._progress_lock:
            progress = self._pair_progress.get(key)
            if not progress:
                return
            already_complete = progress.last_percent >= 100.0
            progress.last_percent = 100.0
            end_ts = progress.target_end
            progress.last_ts = end_ts
        if not already_complete:
            self.log(f"[ingest] {symbol} {timeframe} 100.0% (up to {self._fmt_dt(end_ts)})")

    def log_fetch_success(self, context: FetchContext, data: Any, response_ms: float) -> None:
        candles = len(data) if (data is not None and hasattr(data, "__len__")) else 0
        start_ms = context.since_ms
        end_ms = start_ms
        if isinstance(data, pd.DataFrame) and not data.empty and "ts" in data.columns:
            try:
                start_ms = int(data["ts"].min())
                end_ms = int(data["ts"].max())
            except (ValueError, TypeError):
                start_ms = context.since_ms
                end_ms = start_ms
        if end_ms is None:
            end_ms = start_ms

        message = (
            f"[{context.tag}] {context.symbol} {context.timeframe} "
            f"{self._fmt_ms(start_ms)} → {self._fmt_ms(end_ms)} {candles} candles in {int(response_ms)} ms"
        )
        if candles == 0:
            message += " reason=empty"
        self.log(message)
        self._update_pair_stats(context.symbol, context.timeframe, response_ms, candles > 0)
        self._record_request(response_ms)

    def log_fetch_error(
        self,
        context: FetchContext,
        error: Exception,
        response_ms: float,
        attempt: int,
        cooldown: Optional[float],
        will_retry: bool,
        rate_limited: bool,
    ) -> None:
        reason = self._exception_reason(error)
        prefix = "retry" if will_retry else "error"
        message = (
            f"[{prefix}] {context.symbol} {context.timeframe} "
            f"{self._fmt_ms(context.since_ms)} → error={reason}"
        )
        if rate_limited:
            message += " reason=rate-limit"
        if cooldown:
            message += f" cooldown={cooldown:.2f}s"
        self.log(message, stderr=not will_retry)
        self._record_request(response_ms)

    def get_pair_avg_ms(self, symbol: str, timeframe: str) -> float:
        with self._progress_lock:
            stats = self._pair_stats.get((symbol, timeframe))
            return stats.average_ms() if stats else 0.0

    def record_generic_request(self, response_ms: float) -> None:
        self._record_request(response_ms)

    def _update_pair_stats(self, symbol: str, timeframe: str, response_ms: float, countable: bool) -> None:
        if not countable:
            return
        key = (symbol, timeframe)
        with self._progress_lock:
            stats = self._pair_stats[key]
            stats.responses += 1
            stats.total_ms += response_ms

    def _record_request(self, response_ms: float) -> None:
        now = time.time()
        with self._metrics_lock:
            self._request_times.append(now)
            self._trim_request_times(now)

    def _trim_request_times(self, now: float) -> None:
        cutoff = now - 60.0
        while self._request_times and self._request_times[0] < cutoff:
            self._request_times.popleft()

    def _summary_loop(self) -> None:
        while not self._stop_event.wait(self._summary_interval):
            with self._status_lock:
                active = self._status["active"]
                completed = self._status["completed"]
            rate = self._current_rate_per_min()
            self.log(
                f"[status] Active threads: {active} / {self.max_threads} | completed: {completed} / {self.total_jobs} | "
                f"rate: {rate:.1f} requests/min"
            )

    def _current_rate_per_min(self) -> float:
        now = time.time()
        with self._metrics_lock:
            self._trim_request_times(now)
            if not self._request_times:
                return 0.0
            elapsed = now - self._request_times[0]
            if elapsed <= 0:
                return float(len(self._request_times))
            return len(self._request_times) * 60.0 / elapsed

    @staticmethod
    def _percent_complete(start: datetime, end: datetime, current: datetime) -> float:
        if current <= start:
            return 0.0
        total = (end - start).total_seconds()
        if total <= 0:
            return 100.0
        elapsed = (current - start).total_seconds()
        percent = (elapsed / total) * 100.0
        return max(0.0, min(100.0, percent))

    @staticmethod
    def _fmt_ms(ms: Optional[int]) -> str:
        if ms is None:
            return "--"
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _fmt_dt(dt_value: Optional[datetime]) -> str:
        if dt_value is None:
            return "--"
        return dt_value.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _exception_reason(error: Exception) -> str:
        text = str(error).strip()
        if not text:
            return error.__class__.__name__
        if len(text) > 160:
            return f"{text[:157]}..."
        return text


def _is_rate_limit_error(error: Exception) -> bool:
    status_code = getattr(error, "status_code", None)
    if status_code == 429:
        return True
    code = getattr(error, "code", None)
    if code == 429:
        return True
    http_status = getattr(error, "http_status", None)
    if http_status == 429:
        return True
    lowercase = str(error).lower()
    return any(token in lowercase for token in ("429", "rate limit", "too many requests"))


def _fetch_with_backoff(
    cfg: PipelineConfig,
    rate_limiter: RateLimiter,
    diagnostics: Optional[Diagnostics],
    fetch_fn,
    *args,
    context: Optional[FetchContext] = None,
    **kwargs,
):
    attempt = 0
    base_delay = max(cfg.request_cooldown_sec, 0.05)
    while True:
        rate_limiter.wait()
        try:
            start_ts = time.perf_counter()
            result = fetch_fn(*args, **kwargs)
            response_ms = (time.perf_counter() - start_ts) * 1000.0
            if diagnostics:
                if context:
                    diagnostics.log_fetch_success(context, result, response_ms)
                else:
                    diagnostics.record_generic_request(response_ms)
            rate_limiter.on_success(response_ms)
            return result
        except Exception as exc:
            response_ms = (time.perf_counter() - start_ts) * 1000.0
            attempt += 1
            is_rate_limit = _is_rate_limit_error(exc)
            if is_rate_limit:
                rate_limiter.on_rate_limit()
            else:
                rate_limiter.on_error()
            cooldown = min(base_delay * (2 ** attempt), 60.0) if base_delay else 0.0
            will_retry = attempt <= cfg.retry_attempts
            if diagnostics:
                if context:
                    diagnostics.log_fetch_error(
                        context,
                        exc,
                        response_ms,
                        attempt,
                        cooldown if will_retry and cooldown > 0 else None,
                        will_retry,
                        is_rate_limit,
                    )
                else:
                    diagnostics.record_generic_request(response_ms)
            if attempt > cfg.retry_attempts:
                raise
            if cooldown > 0:
                time.sleep(cooldown)


def _tf_delta(timeframe: str) -> timedelta:
    seconds = TF_SECONDS.get(timeframe, 60)
    return timedelta(seconds=seconds)


def _history_target(timeframe: str) -> datetime:
    days = HISTORY_WINDOWS_DAYS.get(timeframe, 365)
    return datetime.now(timezone.utc) - timedelta(days=days)


def _next_since_ms(last_ts: Optional[datetime], timeframe: str, overlap_minutes: int = 0) -> Optional[int]:
    if last_ts is None:
        return None
    delta = _tf_delta(timeframe)
    base = last_ts + delta
    if overlap_minutes > 0:
        base -= timedelta(minutes=overlap_minutes)
    return int(base.timestamp() * 1000)


def _df_to_rows(
    df: pd.DataFrame,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> List[tuple]:
    rows = []
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


def _backfill_gap(
    cfg: PipelineConfig,
    exchange_api,
    conn,
    symbol: str,
    timeframe: str,
    gap: Gap,
    rate_limiter: RateLimiter,
    diagnostics: Diagnostics,
) -> int:
    tf_ms = TF_SECONDS.get(timeframe, 60) * 1000
    window_start = int((gap.start_ts - timedelta(milliseconds=tf_ms)).timestamp() * 1000)
    window_end = int((gap.end_ts + timedelta(milliseconds=tf_ms)).timestamp() * 1000)
    since = window_start
    total_inserted = 0
    while since <= window_end:
        df = _fetch_with_backoff(
            cfg,
            rate_limiter,
            diagnostics,
            fetch_ohlcv,
            exchange_api,
            symbol,
            timeframe,
            limit=BINANCE_MAX_LIMIT,
            since=since,
            context=FetchContext(symbol, timeframe, since, tag="backfill"),
        )
        if df.empty:
            break
        df = df[(df["ts"] >= window_start) & (df["ts"] <= window_end)]
        if df.empty:
            since += tf_ms
            continue
        rows = _df_to_rows(df, cfg.exchange, symbol, timeframe)
        inserted = upsert_candles(conn, rows)
        total_inserted += inserted
        since = int(df["ts"].max()) + tf_ms
        if inserted:
            update_last_ts(conn, cfg.exchange, symbol, timeframe, gap.end_ts)
    return total_inserted


def _heal_gaps(
    cfg: PipelineConfig,
    exchange_api,
    conn,
    symbol: str,
    timeframe: str,
    rate_limiter: RateLimiter,
    diagnostics: Diagnostics,
) -> int:
    gaps = find_gaps_for_pair(conn, cfg.exchange, symbol, timeframe, lookback_limit=5000)
    total = 0
    for gap in gaps:
        try:
            total += _backfill_gap(cfg, exchange_api, conn, symbol, timeframe, gap, rate_limiter, diagnostics)
        except Exception as exc:
            diagnostics.log(
                f"[error] gap backfill failed for {symbol}/{timeframe}: {Diagnostics._exception_reason(exc)}",
                stderr=True,
            )
    return total


def _ingest_symbol_timeframe(
    cfg: PipelineConfig,
    exchange_api,
    conn,
    symbol: str,
    timeframe: str,
    rate_limiter: RateLimiter,
    diagnostics: Diagnostics,
    overlap_minutes: int = 0,
) -> Tuple[int, int]:
    last_ts = get_last_ts(conn, cfg.exchange, symbol, timeframe)
    since_ms = _next_since_ms(last_ts, timeframe, overlap_minutes=overlap_minutes)
    if since_ms is None:
        target_start = _history_target(timeframe)
        since_ms = int(target_start.timestamp() * 1000)
        start_boundary = target_start
    else:
        start_boundary = datetime.fromtimestamp(since_ms / 1000.0, tz=timezone.utc)

    target_end = datetime.now(timezone.utc)
    diagnostics.register_pair(symbol, timeframe, start_boundary, target_end)

    limit = min(cfg.batch_size, BINANCE_MAX_LIMIT)
    total_inserted = 0

    while True:
        context = FetchContext(symbol, timeframe, since_ms, tag="fetch")
        df = _fetch_with_backoff(
            cfg,
            rate_limiter,
            diagnostics,
            fetch_ohlcv,
            exchange_api,
            symbol,
            timeframe,
            limit=limit,
            since=since_ms,
            context=context,
        )
        if df.empty:
            break

        rows = _df_to_rows(df, cfg.exchange, symbol, timeframe)
        inserted = upsert_candles(conn, rows)
        latest_ts = datetime.fromtimestamp(int(df["ts"].max()) / 1000.0, tz=timezone.utc)
        if inserted:
            update_last_ts(conn, cfg.exchange, symbol, timeframe, latest_ts)
        diagnostics.update_progress(symbol, timeframe, latest_ts)
        total_inserted += inserted

        if len(df) < limit:
            break

        since_ms = int(df["ts"].max()) + TF_SECONDS.get(timeframe, 60) * 1000

    gaps_filled = 0
    if total_inserted:
        gaps_filled = _heal_gaps(cfg, exchange_api, conn, symbol, timeframe, rate_limiter, diagnostics)

    return total_inserted, gaps_filled


def _load_symbol_universe(cfg: PipelineConfig, exchange, rate_limiter: RateLimiter, override: Optional[Sequence[str]]) -> List[str]:
    if override:
        return list(override)
    rate_limiter.wait()
    started = time.perf_counter()
    symbols = fetch_top_symbols(exchange, cfg.quote_asset, cfg.top_symbols)
    rate_limiter.on_success((time.perf_counter() - started) * 1000.0)
    print(f"[ingest] selected {len(symbols)} symbols for {cfg.quote_asset} universe", flush=True)
    return symbols


def run_ingestion(symbols: Optional[Sequence[str]] = None, timeframes: Optional[Sequence[str]] = None) -> None:
    cfg = load_config()
    selected_timeframes = list(timeframes) if timeframes else cfg.timeframes

    rate_limiter = RateLimiter(cfg.request_cooldown_sec, cfg.throttle_min, cfg.throttle_max)
    base_exchange = init_exchange()
    universe = _load_symbol_universe(cfg, base_exchange, rate_limiter, symbols)
    if not universe:
        print("[ingest] no symbols to process", flush=True)
        return

    jobs = [(symbol, tf) for symbol in universe for tf in selected_timeframes]
    total_jobs = len(jobs)
    diagnostics = Diagnostics(total_jobs, cfg.max_threads, rate_limiter)
    rate_limiter.attach(diagnostics)
    diagnostics.log(
        f"[ingest] starting parallel ingestion for {total_jobs} symbol/timeframe combos (max threads={cfg.max_threads})"
    )
    diagnostics.start()

    pool = create_pool(cfg, minconn=1, maxconn=max(2, cfg.max_threads))
    try:
        with pooled_connection(pool) as conn:
            ensure_schema(conn)

        def worker(job):
            symbol, timeframe = job
            exchange_api = init_exchange()
            diagnostics.task_started(symbol, timeframe)
            try:
                with pooled_connection(pool) as conn:
                    inserted, gaps = _ingest_symbol_timeframe(
                        cfg,
                        exchange_api,
                        conn,
                        symbol,
                        timeframe,
                        rate_limiter,
                        diagnostics,
                    )
                diagnostics.task_completed(symbol, timeframe, inserted, gaps)
                return symbol, timeframe, inserted, gaps, None
            except Exception as exc:
                diagnostics.task_failed(symbol, timeframe, exc)
                return symbol, timeframe, 0, 0, exc

        with ThreadPoolExecutor(max_workers=cfg.max_threads) as executor:
            futures = {executor.submit(worker, job): job for job in jobs}
            for future in as_completed(futures):
                symbol, timeframe, inserted, gaps, error = future.result()
                if error:
                    diagnostics.log(
                        f"[warn] {symbol}/{timeframe} completed with errors; see above logs for details.",
                        stderr=True,
                    )
    finally:
        diagnostics.stop()
        pool.closeall()


def cli(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Binance candle ingestion pipeline")
    parser.add_argument("--symbols", nargs="*", help="Override symbol universe (e.g. BTC/USDT ETH/USDT)")
    parser.add_argument("--timeframes", nargs="*", help="Override timeframes (default from config)")
    args = parser.parse_args(argv)
    run_ingestion(symbols=args.symbols, timeframes=args.timeframes)


if __name__ == "__main__":
    cli()
