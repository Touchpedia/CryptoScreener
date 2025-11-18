# CryptoScreener Backend — Quick API

Base URL: http://localhost:8000

## Endpoints
| Method | Path                 | Purpose                        |
|-------:|----------------------|--------------------------------|
| GET    | /api/status          | Health check ({"status":"ok"}) |
| POST   | /api/ingestion/run   | Enqueue backfill jobs          |
| GET    | /api/report/coverage | Coverage snapshot              |
| POST   | /api/db/flush        | Truncate staging & candles     |

Notes:
- /api/db/flush currently has **no auth guard** — use carefully.
- /api/ingestion/run defaults: 	op_symbols=100, interval=1m, candles_per_symbol=6000.