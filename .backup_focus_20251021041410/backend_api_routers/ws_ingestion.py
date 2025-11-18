import os, json, asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import Set

router = APIRouter()

def _redis_conn():
    import redis  # lazy import
    return redis.Redis(
        host=os.getenv("REDIS_HOST","cs_redis"),
        port=int(os.getenv("REDIS_PORT","6379")),
        db=int(os.getenv("REDIS_DB","0")),
        decode_responses=True,
    )

@router.websocket("/ws/ingestion")
async def ws_ingestion(websocket: WebSocket):
    await websocket.accept()
    r = None
    try:
        r = _redis_conn()
        # Send current state immediately
        running = r.get("ingestion_running") == "1"
        await websocket.send_text(json.dumps({"running": running, "source": "snapshot"}))
        # Subscribe to events
        p = r.pubsub()
        p.subscribe("ingestion_events")
        # Poll pubsub in an async-friendly loop
        while True:
            msg = p.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg and msg.get("type") == "message":
                data = msg.get("data")
                if isinstance(data, bytes):
                    data = data.decode()
                await websocket.send_text(data)
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        return
    except Exception as e:
        try:
            await websocket.send_text(json.dumps({"error": str(e)}))
        except Exception:
            pass
    finally:
        try:
            if r: r.close()
        except Exception:
            pass
