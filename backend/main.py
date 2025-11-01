from __future__ import annotations

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import importlib.util
import os, time, datetime
from redis import Redis
from rq import Queue
import psycopg2
import ccxt

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Redis/RQ config ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RQ_QUEUE   = os.getenv("RQ_QUEUE", "ingestion-tasks")

# --- Timescale/PG settings ---
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

# Remember the last ingestion request (for coverage filtering)
LAST_REQ = {
    "symbols": [],
    "tasks": [],
}

def tf_to_ms(tf: str) -> int:
    tf = (tf or "1m").lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60_000
    if tf.endswith("d"):
        return int(tf[:-1]) * 24 * 60 * 60_000
    return 60_000

def get_dynamic_top_symbols(n: int) -> list[str]:
    n = max(1, min(int(n or 1), 200))
    try:
        ex = ccxt.binance({"enableRateLimit": True})
        tickers = ex.fetch_tickers()
        rows = []
        for sym, t in tickers.items():
            if not sym.endswith("/USDT"):
                continue
            vol = 0.0
            if isinstance(t, dict):
                vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try:
                vol = float(vol)
            except Exception:
                vol = 0.0
            rows.append((sym, vol))
        rows.sort(key=lambda x: x[1], reverse=True)
        symbols = [s for (s, _) in rows[:n]]
        if symbols:
            return symbols
    except Exception as e:
        print(f"?? CCXT top symbols failed: {e}")
    majors = [
        "BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT","ADA/USDT","DOGE/USDT",
        "TON/USDT","TRX/USDT","LINK/USDT","DOT/USDT","MATIC/USDT","LTC/USDT","BCH/USDT",
        "AVAX/USDT","XLM/USDT","UNI/USDT","ATOM/USDT","ETC/USDT","APT/USDT","NEAR/USDT",
        "OP/USDT","ARB/USDT","TIA/USDT","INJ/USDT","SUI/USDT","SEI/USDT","FIL/USDT",
        "AAVE/USDT","ALGO/USDT","EGLD/USDT","FTM/USDT","RON/USDT","HNT/USDT","RUNE/USDT"
    ]
    return majors[:n]

def _parse_iso_to_ms(value: str | None) -> int | None:
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def _compute_window(
    timeframe: str,
    now_ms: int,
    candles: int | None,
    start_ms: int | None,
    end_ms: int | None,
) -> tuple[int, int]:
    tf_ms = tf_to_ms(timeframe)
    if start_ms is not None and end_ms is not None:
        if end_ms <= start_ms:
            raise ValueError("end must be greater than start")
        return start_ms, end_ms

    if candles is None or candles <= 0:
        raise ValueError("candles_per_symbol must be a positive integer")

    tf_ms = tf_ms or 60_000
    start = max(0, now_ms - (candles + 2) * tf_ms)
    end = now_ms
    return start, end


@app.get("/api/status")
async def status():
    return {"status": "ok"}

@app.post("/api/ingestion/run")
async def run_ingestion(request: Request):
    """
    Enqueue jobs with explicit window:
      workers.backfill_range_job(symbol, timeframe, start_ts, end_ts)
    start_ts/end_ts are ms since epoch UTC.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}

    symbols_override = body.get("symbols")
    symbols: list[str] = []
    if isinstance(symbols_override, list):
        seen: set[str] = set()
        for raw in symbols_override:
            if not isinstance(raw, str):
                continue
            sym = raw.strip()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            symbols.append(sym)

    if not symbols:
        raise HTTPException(status_code=400, detail="symbols list required")

    tasks_input = body.get("tasks")
    tasks: list[dict] = []

    if isinstance(tasks_input, list) and tasks_input:
        now_ms = int(time.time() * 1000)
        for item in tasks_input:
            if not isinstance(item, dict):
                continue
            timeframe = str(item.get("timeframe") or "").strip()
            if not timeframe:
                continue
            candles = item.get("candles") or item.get("candles_per_symbol")
            candles_int = None
            if candles is not None:
                try:
                    candles_int = int(candles)
                except (TypeError, ValueError):
                    candles_int = None

            start_ms = item.get("start_ms")
            end_ms = item.get("end_ms")
            try:
                start_ms = int(start_ms) if start_ms is not None else None
            except (TypeError, ValueError):
                start_ms = None
            try:
                end_ms = int(end_ms) if end_ms is not None else None
            except (TypeError, ValueError):
                end_ms = None

            if start_ms is None and item.get("start_iso"):
                start_ms = _parse_iso_to_ms(item.get("start_iso"))
            if end_ms is None and item.get("end_iso"):
                end_ms = _parse_iso_to_ms(item.get("end_iso"))

            try:
                start_ts, end_ts = _compute_window(timeframe, now_ms, candles_int, start_ms, end_ms)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))

            tasks.append(
                {
                    "timeframe": timeframe,
                    "candles_per_symbol": candles_int,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                }
            )

    else:
        # backward compatibility: interval + candles_per_symbol
        interval = str(body.get("interval", "1m"))
        candles_per_symbol = int(body.get("candles_per_symbol", 6000))
        now_ms = int(time.time() * 1000)
        start_ts, end_ts = _compute_window(interval, now_ms, candles_per_symbol, None, None)
        tasks.append(
            {
                "timeframe": interval,
                "candles_per_symbol": candles_per_symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )

    if not tasks:
        raise HTTPException(status_code=400, detail="no valid tasks provided")

    LAST_REQ["symbols"] = symbols
    LAST_REQ["tasks"] = tasks

    conn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    q = Queue(RQ_QUEUE, connection=conn, default_timeout=900)

    enqueued_jobs = 0
    for task in tasks:
        timeframe = task["timeframe"]
        start_ts = task["start_ts"]
        end_ts = task["end_ts"]
        for sym in symbols:
            q.enqueue("workers.backfill_range_job", sym, timeframe, start_ts, end_ts)
            enqueued_jobs += 1

    tf_summary = ", ".join(
        f"{task['timeframe']}({task.get('candles_per_symbol') or 'custom'})" for task in tasks
    )
    msg = (
        f"Enqueued {enqueued_jobs} jobs across {len(symbols)} symbols "
        f"on {RQ_QUEUE} (tasks: {tf_summary})"
    )
    print(f"? {msg}")
        # mark ingestion as running for UI
    try:
        rconn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        rconn.set("ingestion_running", "1")
        rconn.publish("ingestion_state", "{\"running\": true}")
    except Exception as _e:
        print(f"?? failed to set ingestion_running: {_e}")
        # mark ingestion as running for UI
    try:
        rconn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        rconn.set("ingestion_running", "1")
        rconn.publish("ingestion_state", "{\"running\": true}")
    except Exception as _e:
        print(f"?? failed to set ingestion_running: {_e}")
        # mark ingestion as running for UI
    try:
        rconn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        rconn.set("ingestion_running", "1")
        rconn.publish("ingestion_state", "{\"running\": true}")
    except Exception as _e:
        print(f"?? failed to set ingestion_running: {_e}")
    return {"message": msg, "symbols": symbols, "tasks": tasks}

def _coerce_int(value) -> int | None:
    try:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.strip():
            return int(float(value))
    except (ValueError, TypeError):
        return None
    return None


def _clean_symbols(raw_symbols) -> list[str]:
    cleaned: list[str] = []
    if not raw_symbols:
        return cleaned
    for sym in raw_symbols:
        if isinstance(sym, str):
            token = sym.strip()
            if token:
                cleaned.append(token)
    return cleaned


def _normalize_tasks(raw_tasks) -> list[dict]:
    normalized: list[dict] = []
    if not raw_tasks:
        return normalized
    now_ms = int(time.time() * 1000)
    for item in raw_tasks:
        if not isinstance(item, dict):
            continue
        timeframe = str(item.get("timeframe") or "").strip()
        if not timeframe:
            continue
        candles_val = (
            item.get("candles_per_symbol")
            or item.get("candles")
            or item.get("window")
        )
        try:
            candles_int = int(candles_val)
        except (TypeError, ValueError):
            continue
        if candles_int <= 0:
            continue
        start_ms = _coerce_int(item.get("start_ts") or item.get("start_ms"))
        end_ms = _coerce_int(item.get("end_ts") or item.get("end_ms"))
        try:
            start_ts, end_ts = _compute_window(timeframe, now_ms, candles_int, start_ms, end_ms)
        except ValueError:
            continue
        normalized.append(
            {
                "timeframe": timeframe,
                "candles_per_symbol": candles_int,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )
    return normalized


def _to_epoch_ms(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    if isinstance(value, (int, float)):
        if value > 1_000_000_000_000:
            return int(value)
        return int(value * 1000)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            dt = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


def _ms_to_iso(ms: int) -> str:
    return datetime.datetime.utcfromtimestamp(ms / 1000).replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _fetch_coverage_rows(symbols: list[str], tasks: list[dict]) -> list[dict]:
    if not symbols or not tasks:
        return []

    results: list[dict] = []
    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
    ) as conn:
        with conn.cursor() as cur:
            for task in tasks:
                timeframe = task["timeframe"]
                required = int(task["candles_per_symbol"])
                start_ts = int(task["start_ts"])
                end_ts = int(task["end_ts"])
                start_iso = _ms_to_iso(start_ts)
                end_iso = _ms_to_iso(end_ts)

                cur.execute(
                    """
                    SELECT symbol, COUNT(*) AS received, MAX(ts) AS latest_ts
                    FROM candles
                    WHERE symbol = ANY(%s) AND timeframe = %s AND ts BETWEEN %s AND %s
                    GROUP BY symbol
                    """,
                    (symbols, timeframe, start_iso, end_iso),
                )
                rows = {sym: (int(rec or 0), latest) for sym, rec, latest in cur.fetchall()}

                for sym in symbols:
                    received, latest = rows.get(sym, (0, None))
                    results.append(
                        {
                            "symbol": sym,
                            "timeframe": timeframe,
                            "required": required,
                            "received": received,
                            "latest_ts": _to_epoch_ms(latest),
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                        }
                    )
    return results


@app.get("/api/report/coverage")
async def report_coverage(request: Request):
    query = request.query_params
    requested_symbols = query.getlist("symbols")
    timeframe = query.get("timeframe")
    window = query.get("window")

    fallback_symbols = LAST_REQ.get("symbols", [])
    fallback_tasks = LAST_REQ.get("tasks", [])

    symbols = _clean_symbols(requested_symbols) or fallback_symbols

    tasks_source: list[dict]
    if timeframe and window:
        tasks_source = [
            {"timeframe": timeframe, "candles_per_symbol": window},
        ]
    elif fallback_tasks:
        tasks_source = fallback_tasks
    else:
        tasks_source = []

    tasks = _normalize_tasks(tasks_source)
    rows = _fetch_coverage_rows(symbols, tasks)
    return {"rows": rows}

@app.post("/api/db/flush")
async def flush_db():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE staging_candles, candles RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close(); conn.close()
    print("? Database flushed")
        # mark ingestion as running for UI
    try:
        rconn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        rconn.set("ingestion_running", "1")
        rconn.publish("ingestion_state", "{\"running\": true}")
    except Exception as _e:
        print(f"?? failed to set ingestion_running: {_e}")
        # mark ingestion as running for UI
    try:
        rconn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        rconn.set("ingestion_running", "1")
        rconn.publish("ingestion_state", "{\"running\": true}")
    except Exception as _e:
        print(f"?? failed to set ingestion_running: {_e}")
        # mark ingestion as running for UI
    try:
        rconn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        rconn.set("ingestion_running", "1")
        rconn.publish("ingestion_state", "{\"running\": true}")
    except Exception as _e:
        print(f"?? failed to set ingestion_running: {_e}")
    return {"message": "Database flushed successfully"}

@app.post("/api/report/coverage")
async def report_coverage_post(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    requested_symbols = payload.get("symbols")
    requested_tasks = payload.get("tasks") or payload.get("timeframes")

    symbols = _clean_symbols(requested_symbols) or LAST_REQ.get("symbols", [])
    tasks = _normalize_tasks(requested_tasks or LAST_REQ.get("tasks", []))
    rows = _fetch_coverage_rows(symbols, tasks)
    return {"rows": rows}


from api.routers.ingestion_control import router as ingestion_control_router

app.include_router(ingestion_control_router)

from api.routers.ws_ingestion import router as ws_router
from api.routers.ws_chart import router as ws_chart_router
app.include_router(ws_router)
app.include_router(ws_chart_router)


from api.routers.report import router as report_router
from api.routers.admin_control import router as admin_router
from api.routers.chart_view import router as chart_router
app.include_router(report_router)
app.include_router(admin_router)
app.include_router(chart_router)

if importlib.util.find_spec("services.websocket_handler"):
    from services.websocket_handler import start_websocket_handler, stop_websocket_handler
    from services.ingestion_service import shutdown_ingestion_service
else:
    from backend.services.websocket_handler import start_websocket_handler, stop_websocket_handler
    from backend.services.ingestion_service import shutdown_ingestion_service


@app.on_event("startup")
async def _startup_events() -> None:
    await start_websocket_handler()


@app.on_event("shutdown")
async def _shutdown_events() -> None:
    await stop_websocket_handler()
    await shutdown_ingestion_service()





# ======= AUTO-ADDED: symbols_clean endpoint (stable BASE excluded) =======
from typing import List
try:
    import httpx
except Exception:
    httpx = None

try:
    STABLE_BASES
except NameError:
    STABLE_BASES = {"USDT","USDC","BUSD","FDUSD","TUSD","USDP","DAI","SUSD","USDD","USTC","GUSD","PAX","USDE"}

@app.get("/api/ingestion/symbols_clean")
async def api_ingestion_symbols_clean(limit: int = 200):
    # 1) try native helper
    syms: List[str] = []
    try:
        syms = list(get_dynamic_top_symbols(limit))  # type: ignore
    except Exception:
        syms = []
    # 2) fallback to internal call if empty
    if (not syms) and httpx is not None:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(f"http://localhost:8000/api/ingestion/symbols?limit={int(limit or 200)}")
                j = r.json() if r.status_code == 200 else {}
                syms = list(j.get("symbols") or [])
        except Exception:
            syms = []

    # 3) filter out BASE stablecoins (keep USDT as QUOTE)
    out = []
    for s in syms:
        if isinstance(s, str) and "/" in s:
            base = s.split("/", 1)[0]
            if base in STABLE_BASES:
                continue
            out.append(s)
    return {"symbols": out, "count": len(out)}
# ======= END AUTO-ADDED =======
