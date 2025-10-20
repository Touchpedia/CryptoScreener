import os
from fastapi import APIRouter
import psycopg2

router = APIRouter(prefix="/api/admin", tags=["admin"])

PG_HOST=os.getenv("POSTGRES_HOST","postgres")
PG_DB=os.getenv("POSTGRES_DB","postgres")
PG_USER=os.getenv("POSTGRES_USER","postgres")
PG_PASS=os.getenv("POSTGRES_PASSWORD","postgres")

TABLES = ["staging_candles"]

@router.post("/flush")
async def flush():
    try:
        conn=psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
        conn.autocommit=True
        with conn.cursor() as cur:
            for t in TABLES:
                cur.execute(f'TRUNCATE TABLE {t} RESTART IDENTITY')
        conn.close()
        return {"ok": True, "flushed": TABLES}
    except Exception as e:
        return {"ok": False, "error": str(e)}
