const JSON_HEADERS = { "Content-Type": "application/json" };
async function asJson(res) {
    if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}: ${text}`);
    }
    return (await res.json());
}
export async function fetchSymbols(segment) {
    const url = new URL("/api/ingestion/symbols", window.location.origin);
    url.searchParams.set("segment", segment);
    const response = await fetch(url.toString());
    const json = await asJson(response);
    return {
        symbols: json.symbols ?? [],
        fallback: Boolean(json.fallback),
        segment: json.segment ?? segment,
    };
}
export async function runIngestion(payload) {
    const res = await fetch("/api/ingestion/run", {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify(payload),
    });
    return asJson(res);
}
export async function fetchIngestionStatus() {
    const response = await fetch("/api/ingestion/status");
    const json = await asJson(response);
    return { running: Boolean(json.running) };
}
export async function startIngestion() {
    const res = await fetch("/api/ingestion/start", { method: "POST", headers: JSON_HEADERS });
    const json = await asJson(res);
    if (json.ok === false) {
        throw new Error(json.error || "Failed to start ingestion");
    }
}
export async function stopIngestion() {
    const res = await fetch("/api/ingestion/stop", { method: "POST", headers: JSON_HEADERS });
    const json = await asJson(res);
    if (json.ok === false) {
        throw new Error(json.error || "Failed to stop ingestion");
    }
}
export async function flushDatabaseAdmin() {
    const res = await fetch("/api/admin/flush", { method: "POST", headers: JSON_HEADERS });
    const json = await asJson(res);
    if (json.ok === false) {
        throw new Error(json.error || "Failed to flush database");
    }
}
export async function fetchStatusSnapshot() {
    const [statusRes, activeRes] = await Promise.all([
        fetch("/api/ingestion/status").then((res) => res.ok ? res.json() : Promise.reject(new Error("status"))).catch(() => null),
        fetch("/api/ingestion/active").then((res) => res.ok ? res.json() : Promise.reject(new Error("active"))).catch(() => null),
    ]);
    const pairs = (activeRes?.items ?? [])
        .map((item) => {
        const symbol = String(item?.symbol ?? item?.pair ?? "").trim();
        const timeframe = String(item?.timeframe ?? item?.tf ?? "").trim();
        if (!symbol) {
            return null;
        }
        const rawUpdated = item?.updatedAt ?? item?.updated_at ?? item?.updated;
        const updatedAt = (typeof rawUpdated === "number"
            ? new Date(rawUpdated).toISOString()
            : rawUpdated ?? null);
        return {
            pair: timeframe ? `${symbol} (${timeframe})` : symbol,
            status: item?.status ?? "running",
            progress: item?.progress != null ? Number(item?.progress) : null,
            timeframes: timeframe ? { [timeframe]: Number(item?.progress ?? 0) } : item?.timeframes ?? {},
            updatedAt,
        };
    })
        .filter((entry) => Boolean(entry));
    if (pairs.length === 0) {
        // fallback to legacy endpoint if no active items are available
        try {
            const legacy = await asJson(await fetch("/api/status"));
            return legacy;
        }
        catch {
            /* ignore legacy failure */
        }
    }
    const running = statusRes?.running === true;
    const run = {
        run_id: statusRes?.run_id ?? statusRes?.runId ?? "",
        status: running ? "running" : "idle",
        percent: running ? Number(statusRes?.percent ?? 0) : 100,
        updatedAt: statusRes?.updatedAt ?? new Date().toISOString(),
    };
    return { pairs, run };
}
export async function fetchChartCandles(params) {
    const url = new URL("/api/chart/candles", window.location.origin);
    url.searchParams.set("symbol", params.symbol);
    url.searchParams.set("target_timeframe", params.target_timeframe);
    if (params.base_timeframe)
        url.searchParams.set("base_timeframe", params.base_timeframe);
    if (typeof params.start_ts === "number")
        url.searchParams.set("start_ts", String(params.start_ts));
    if (typeof params.end_ts === "number")
        url.searchParams.set("end_ts", String(params.end_ts));
    if (typeof params.limit === "number")
        url.searchParams.set("limit", String(params.limit));
    return asJson(await fetch(url.toString()));
}
export async function triggerGapFill(payload) {
    const res = await fetch("/api/admin/gap-fill", {
        method: "POST",
        headers: JSON_HEADERS,
        body: JSON.stringify(payload),
    });
    return asJson(res);
}
export async function fetchGapCoverage(args) {
    const url = new URL("/api/admin/gap-fill/coverage", window.location.origin);
    url.searchParams.set("symbol", args.symbol);
    url.searchParams.set("timeframe", args.timeframe);
    url.searchParams.set("start_ts", String(args.start_ts));
    url.searchParams.set("end_ts", String(args.end_ts));
    return asJson(await fetch(url.toString()));
}
export async function flushDatabase() {
    const res = await fetch("/api/db/flush", { method: "POST" });
    return asJson(res);
}
export async function fetchCoverage(params) {
    const url = new URL("/api/report/coverage", window.location.origin);
    url.searchParams.set("timeframe", params.timeframe);
    url.searchParams.set("window", String(params.window));
    url.searchParams.set("limit", String(params.limit));
    const json = await asJson(await fetch(url.toString()));
    if (json.ok === false) {
        throw new Error(json.error || "Failed to load coverage");
    }
    return json.rows ?? [];
}
export async function fetchWsMetrics() {
    return asJson(await fetch("/ws/chart/metrics"));
}
