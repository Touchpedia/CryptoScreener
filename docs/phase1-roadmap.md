# Phase 1 – Crypto Screener Data Pipeline (Summary)

## 1. Project Goals
- Niche focus: non‑stable USDT pairs.
- Bring historical (backfill) + live (websocket) candles into our own storage.
- Keep storage footprint small while retaining enough history for testing.
- Everything should be configurable from the UI; no hardcoded tuning knobs.

## 2. High-Level Architecture
```
┌─────────────┐        ┌───────────────┐        ┌───────────────────┐
│  UI (React) │ ─────▶ │ FastAPI Router│ ─────▶ │ Queue (Redis + RQ)│
└─────────────┘        └───────────────┘        └─────────┬─────────┘
          ▲                              jobs              │
          │                              ▲                 │
          │                              │                 ▼
          │                     ┌────────────────┐   ┌──────────────┐
          │                     │ Ingestion       │   │ Aggregator   │
          │                     │ Workers         │   │ (Redis stream│
          │   coverage/chart ◀──┤ - Backfills     │   │  + DB flush) │
          │                     │ - Websockets    │   └──────┬───────┘
          │                     └────────────────┘          │
          │                                                 ▼
          │                                      ┌───────────────────┐
          └──────────────────────────────────────┤ TimescaleDB (ts)  │
                                                 └───────────────────┘
```

## 3. Key Flows
### 3.1 Pair Selection → Backfill
1. User selects non‑stable pairs, timeframe(s), duration(s) from UI.
2. UI sends `POST /api/ingestion/run` with symbols + task configs.
3. FastAPI validates, records request metadata, enqueues RQ jobs.
4. Worker fetches REST klines in batches, writes to TimescaleDB.
5. Coverage endpoint (`/api/report/coverage`) confirms required vs received candles.

### 3.2 Websocket Streaming (Independent)
1. UI can toggle websocket ingestion for a wider/non-overlapping symbol set.
2. Worker opens exchange websocket, pushes micro events into Redis stream.
3. Aggregator module consumes Redis, aggregates to configured target timeframes, upserts into DB.
4. Watermark table tracks last finalized candle per symbol/timeframe to resume safely.

### 3.3 Live Chart (DB-driven)
1. When user selects a row in UI, frontend calls `GET /api/candles?symbol=...&timeframe=...` to pull recent N candles from DB.
2. UI renders them using an in-app chart library (not TradingView iframe) to verify our own data.
3. Frontend also subscribes to websocket endpoint that forwards Redis stream events → chart updates in near real time.
4. Chart thus reflects our ingestion health, not the exchange directly.

## 4. UI Configuration Surface (no hardcoded values)
- Symbol presets, search, custom watchlists.
- Exchange selector (initial targets: Binance, Bybit, OKX, KuCoin – extendable to other high-liquidity venues; UI can pick multiple exchanges and the pipeline averages prices before writing).
- Backfill tasks: timeframe list, window (days/candles), start/end overrides.
- Websocket stream settings: source interval, target intervals, latency tolerances.
- Coverage thresholds (ready/warning, freshness window), websocket connection limit.
- Data retention/compression toggles (per symbol/timeframe).
- Save/restore presets for repeatable test scenarios.

## 5. Storage Strategy
- Timescale hypertables with compression + retention policies (UI controls retention window).
- Redis holds recent ticks/candles with TTL; periodic flush commits to Timescale.
- Rollup jobs optional (e.g., downsample historical > N months).
- Exchange adapters must expose consistent error handling (network issues, rate limits, missing pair); when a source fails the aggregator logs the issue, retries with back-off, and continues using available exchanges without blocking ingestion.

## 6. Testing & Observability Hooks
- RQ job status + history surfaced in UI (already partly implemented).
- Per symbol/timeframe coverage + drift monitored (existing tables to be extended).
- Logs/metrics for queue depth, websocket lag, aggregator latency (Prometheus/Grafana later phase).

## 7. Phase 1 Deliverables Check
1. Modular backend packages (symbols, ingestion, coverage, storage, stream).
2. Configurable UI panels for every adjustable parameter noted above.
3. Candle chart powered by `/api/candles` + websocket stream from our own DB/Redis.
4. Documentation of retention/compression policies and how testers adjust them.
5. Smoke tests: backfill run, websocket streaming, chart update, coverage verification.

## 8. Next Steps After Approval
- Define precise API contracts (`/api/candles`, websocket channel schema, aggregation config endpoints).
- Break down backend refactor tasks (service/repository split, unit tests).
- Choose frontend charting library and build the live update pipeline.
- Implement Redis stream → aggregator → Timescale flush loop.

_Document owner: Codex assistant – ready for review before implementation._

## 9. Guardrails & Engineering Playbook
- **Architecture boundaries:** UI, API, domain, infra modules isolated; communication via interfaces/adapters only. Domain layer unaware of infra details.
- **Contract-first:** REST/WS/event schemas versioned (v1/v2). Breaking change demands new version and dual support window.
- **Dependency rule:** outer layers may depend on inner, never vice-versa. Adapters glue infra ↔ domain.
- **CQRS:** write path (ingestion) and read path (coverage/chart) tuned independently.
- **Config over code:** every knob (timeframes, retention, merge rules, limits) lives in runtime configs with hot-reload capability.
- **Feature flags:** new capability rolled out behind flags for staged activation.
- **Migrations:** forward-only, zero-downtime; backfill jobs idempotent (safe re-run).
- **Quality gates:** test pyramid (unit > integration > e2e) with contract tests; mandatory lint, type-check, formatter, security scan in CI.
- **Definition of done:** metrics, logs, traces wired; docs, dashboards, runbooks updated.
- **Observability:** capture QPS, p95 latency, error rate, queue backlog, data freshness & coverage; propagate request-id/event-id across services; SLO breach alerts only (no noisy spam).
- **Data rules:** single source of truth for base series; derived views reproducible. All writes/jobs idempotent with dedupe keys. Retention/TTL per timeframe configurable; compression defaults on; document cold↔hot restore.
- **Reliability:** implement backpressure limits on queues, graceful shedding, retry + DLQ. Circuit breakers around upstream exchanges; degrade using cached/last-good data. Achieve “exactly-once” via at-least-once + idempotent upserts.
- **Security:** secrets via vault/env, rotate-able; never log plain secrets. RBAC on admin/config changes with audit trail. Public endpoints rate-limited and abuse-protected.
- **Performance budgets:** p95 targets (reads ≤200–800 ms depending on endpoint); review query plans. Job concurrency & CPU caps set via config with cost telemetry visible. Cache strategy with hit-rate goals and safe stale-while-revalidate.
- **Team workflow:** capture ADRs for major decisions, keep PRs small & toggle-protected, require green CI before merge. Maintain runbooks with on-call steps and examples.
