from fastapi import APIRouter, Query
import os, psycopg2

router = APIRouter(prefix="/api/report", tags=["report"])

PG_HOST=os.getenv("POSTGRES_HOST","postgres")
PG_DB=os.getenv("POSTGRES_DB","postgres")
PG_USER=os.getenv("POSTGRES_USER","postgres")
PG_PASS=os.getenv("POSTGRES_PASSWORD","postgres")

@router.get("/coverage")
def coverage(timeframe: str="1m", window: int=6000, limit: int=10):
    # Returns top symbols with total required, received count, latest ts
    q = """
    WITH latest AS (
      SELECT symbol, MAX(ts) AS latest_ts, COUNT(*) AS received
      FROM candles
      WHERE timeframe = %s
      GROUP BY symbol
    )
    SELECT l.symbol,
           %s AS total_required,
           l.received,
           l.latest_ts
    FROM latest l
    ORDER BY l.received DESC NULLS LAST, l.latest_ts DESC NULLS LAST
    LIMIT %s
    """
    try:
        conn=psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
        with conn.cursor() as cur:
            cur.execute(q, (timeframe, window, limit))
            rows = cur.fetchall()
        conn.close()
        return {"ok": True, "rows": [
            {"symbol": r[0], "total_required": r[1], "received": r[2], "latest_ts": None if r[3] is None else r[3].isoformat()}
            for r in rows
        ]}
    except Exception as e:
        return {"ok": False, "error": str(e), "rows": []}
