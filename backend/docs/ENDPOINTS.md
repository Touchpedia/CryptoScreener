# CryptoScreener Backend — Endpoints Quick Reference

## Health
GET /api/status  
- returns: { status: "ok", ... } (consider: db/redis flags in future)

## Ingestion
POST /api/ingestion/run  
- body: { "top_symbols": 100, "interval": "1m", "candles_per_symbol": 6000 }  
- action: enqueue RQ jobs per symbol

## Coverage Report
GET /api/report/coverage  
- filters by last ingestion request window if available; returns counts and latest_ts

## Maintenance (DANGEROUS)
POST /api/db/flush  
- truncates staging_candles and candles (recommend: require X-Admin-Token)

## Notes
- Redis: host=redis, port=6379, default queue=ingestion-tasks
- Env: prefer POSTGRES_* + DATABASE_URL; scripts\env_opt.ps1 can harmonize
