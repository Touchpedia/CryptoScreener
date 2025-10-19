from __future__ import annotations
import os
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis

app = FastAPI(title="Crypto Screener API")

# CORS: UI dev server allow
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis (optional)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
_redis = Redis.from_url(REDIS_URL)

@app.get("/api/status")
def status():
    try:
        _redis.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "ok": True,
        "redis": redis_ok,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }
