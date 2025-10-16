# Real-Time Candle Ingestion

1. Install dependencies
```powershell
cd D:\data_pipeline\backend
pip install -r requirements.txt
```

Environment knobs (optional, in `.env`):

```
INGEST_LOOKBACK_DAYS=365   # how far back the first run fetches when start_ts not provided
INGEST_RETENTION_DAYS=365  # how much history to retain per symbol/timeframe
```

2. Run database & redis (adjust if already running)
```powershell
docker compose up -d postgres redis
```

3. Start the FastAPI app
```powershell
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

4. Start the RQ worker (same virtualenv)
```powershell
rq worker ingestion-tasks --url redis://localhost:6379/0
```

5. Trigger ingestion from the UI, or:
```powershell
Invoke-RestMethod `
  -Uri http://127.0.0.1:8000/api/ingestion/run `
  -Method POST `
  -ContentType 'application/json' `
  -Body '{"symbols":["BTC/USDT"],"timeframes":["1m","5m"],"start_ts":null,"end_ts":null}'
```

6. Monitor status
```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/status
```

Candles are stored in the `candles` table. Progress is tracked in Redis (`run:{run_id}` hashes) and surfaced via `/api/status` for the web UI.
