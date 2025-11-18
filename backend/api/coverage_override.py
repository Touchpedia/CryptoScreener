from fastapi import APIRouter, Query
from typing import List, Optional
import time, datetime
from api.state_override import get_started, is_running

router = APIRouter(prefix="/api/report", tags=["coverage-override"])

def _iso_now():
    try:
        return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    except Exception:
        return "now"

def _tf_minutes(tf: str) -> int:
    try:
        if tf.endswith("m"): return max(1, int(tf[:-1]))
        if tf.endswith("h"): return max(1, int(tf[:-1])) * 60
        if tf.endswith("d"): return max(1, int(tf[:-1])) * 1440
    except Exception:
        pass
    return 1

@router.get("/coverage")
def coverage_get(
    timeframe: str = Query("1m"),
    window: int = Query(1440, ge=1, le=50000),
    symbols: Optional[List[str]] = Query(None, alias="symbols")
):
    started = get_started()
    rows = []
    rec = 0
    if is_running() and started:
        # fast demo growth: ~2 sec per unit, min 1 immediately
        tf_min = max(1, _tf_minutes(timeframe))
        elapsed = max(0, int(time.time() - float(started)))
        step = max(1, tf_min * 2)
        rec = max(1, min(window, elapsed // step))
    ts = _iso_now() if rec > 0 else "-"

    for s in (symbols or [])[:300]:
        rows.append({
            "symbol": s,
            "timeframe": timeframe,       # <<< UI table expects this
            "required": window,           # <<< display column name
            "total_required": window,     # keep both for safety
            "received": rec,
            "latest_ts": ts,
            "window": window
        })
    return {"rows": rows}