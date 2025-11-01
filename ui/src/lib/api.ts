export type IngestionTaskPayload = {
  timeframe: string;
  candles_per_symbol?: number;
  start_ts?: number;
  end_ts?: number;
};

export type IngestionPayload = {
  symbols: string[];
  tasks: IngestionTaskPayload[];
};

export type PairProgress = {
  pair: string;
  status?: string | null;
  gaps?: number | null;
  progress?: number | null;
  timeframes?: Record<string, number> | null;
  updatedAt?: string | null;
};

export type RunProgress = {
  run_id: string;
  status?: string;
  symbol?: string;
  timeframe?: string;
  step?: number;
  total?: number;
  percent?: number;
  updatedAt?: string;
  error?: string;
};

export type StatusSnapshot = {
  pairs: PairProgress[];
  run?: RunProgress;
};

export type ChartCandle = {
  symbol: string;
  timeframe: string;
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type GapFillPayload = {
  symbols: string[];
  timeframes: string[];
  start_ts: number;
  end_ts: number;
};

export type GapFillResult = {
  symbol: string;
  timeframe: string;
  inserted: number;
  start_ts: number;
  end_ts: number;
};

export type GapCoverage = {
  symbol: string;
  timeframe: string;
  expected: number;
  present: number;
  coverage: number;
};

export type CoverageRow = {
  symbol: string;
  total_required: number;
  received: number;
  latest_ts: string | null;
};

const JSON_HEADERS = { "Content-Type": "application/json" } as const;

async function asJson<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return (await res.json()) as T;
}

export async function fetchSymbols(segment: string): Promise<{ symbols: string[]; fallback: boolean; segment: string }> {
  const url = new URL("/api/ingestion/symbols", window.location.origin);
  url.searchParams.set("segment", segment);
  const response = await fetch(url.toString());
  const json = await asJson<{ ok?: boolean; symbols?: string[]; fallback?: boolean; segment?: string }>(response);
  return {
    symbols: json.symbols ?? [],
    fallback: Boolean(json.fallback),
    segment: json.segment ?? segment,
  };
}

export async function runIngestion(payload: IngestionPayload) {
  const res = await fetch("/api/ingestion/run", {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(payload),
  });
  return asJson<{ run_id?: string; runId?: string; message?: string }>(res);
}

export async function fetchIngestionStatus(): Promise<{ running: boolean }> {
  const response = await fetch("/api/ingestion/status");
  const json = await asJson<{ ok?: boolean; running?: boolean }>(response);
  return { running: Boolean(json.running) };
}

export async function startIngestion(): Promise<void> {
  const res = await fetch("/api/ingestion/start", { method: "POST", headers: JSON_HEADERS });
  const json = await asJson<{ ok?: boolean; error?: string }>(res);
  if (json.ok === false) {
    throw new Error(json.error || "Failed to start ingestion");
  }
}

export async function stopIngestion(): Promise<void> {
  const res = await fetch("/api/ingestion/stop", { method: "POST", headers: JSON_HEADERS });
  const json = await asJson<{ ok?: boolean; error?: string }>(res);
  if (json.ok === false) {
    throw new Error(json.error || "Failed to stop ingestion");
  }
}

export async function flushDatabaseAdmin(): Promise<void> {
  const res = await fetch("/api/admin/flush", { method: "POST", headers: JSON_HEADERS });
  const json = await asJson<{ ok?: boolean; error?: string }>(res);
  if (json.ok === false) {
    throw new Error(json.error || "Failed to flush database");
  }
}

export async function fetchStatusSnapshot(): Promise<StatusSnapshot> {
  const [statusRes, activeRes] = await Promise.all([
    fetch("/api/ingestion/status").then((res) => res.ok ? res.json() : Promise.reject(new Error("status"))).catch(() => null),
    fetch("/api/ingestion/active").then((res) => res.ok ? res.json() : Promise.reject(new Error("active"))).catch(() => null),
  ]);

  const pairs: PairProgress[] = (activeRes?.items ?? [])
    .map((item: any): PairProgress | null => {
      const symbol = String(item?.symbol ?? item?.pair ?? "").trim();
      const timeframe = String(item?.timeframe ?? item?.tf ?? "").trim();
      if (!symbol) {
        return null;
      }
      const rawUpdated = item?.updatedAt ?? item?.updated_at ?? item?.updated;
      const updatedAt = (
        typeof rawUpdated === "number"
          ? new Date(rawUpdated).toISOString()
          : rawUpdated ?? null
      );
      return {
        pair: timeframe ? `${symbol} (${timeframe})` : symbol,
        status: item?.status ?? "running",
        progress: item?.progress != null ? Number(item?.progress) : null,
        timeframes: timeframe ? { [timeframe]: Number(item?.progress ?? 0) } : item?.timeframes ?? {},
        updatedAt,
      };
    })
    .filter((entry: PairProgress | null): entry is PairProgress => Boolean(entry));

  if (pairs.length === 0) {
    // fallback to legacy endpoint if no active items are available
    try {
      const legacy = await asJson<StatusSnapshot>(await fetch("/api/status"));
      return legacy;
    } catch {
      /* ignore legacy failure */
    }
  }

  const running = statusRes?.running === true;
  const run: RunProgress = {
    run_id: statusRes?.run_id ?? statusRes?.runId ?? "",
    status: running ? "running" : "idle",
    percent: running ? Number(statusRes?.percent ?? 0) : 100,
    updatedAt: statusRes?.updatedAt ?? new Date().toISOString(),
  };

  return { pairs, run };
}

export async function fetchChartCandles(params: {
  symbol: string;
  target_timeframe: string;
  base_timeframe?: string;
  start_ts?: number;
  end_ts?: number;
  limit?: number;
}): Promise<{ candles: ChartCandle[] }> {
  const url = new URL("/api/chart/candles", window.location.origin);
  url.searchParams.set("symbol", params.symbol);
  url.searchParams.set("target_timeframe", params.target_timeframe);
  if (params.base_timeframe) url.searchParams.set("base_timeframe", params.base_timeframe);
  if (typeof params.start_ts === "number") url.searchParams.set("start_ts", String(params.start_ts));
  if (typeof params.end_ts === "number") url.searchParams.set("end_ts", String(params.end_ts));
  if (typeof params.limit === "number") url.searchParams.set("limit", String(params.limit));
  return asJson<{ candles: ChartCandle[] }>(await fetch(url.toString()));
}

export async function triggerGapFill(payload: GapFillPayload): Promise<{ results: GapFillResult[] }> {
  const res = await fetch("/api/admin/gap-fill", {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(payload),
  });
  return asJson(res);
}

export async function fetchGapCoverage(args: {
  symbol: string;
  timeframe: string;
  start_ts: number;
  end_ts: number;
}): Promise<{ coverage: GapCoverage }> {
  const url = new URL("/api/admin/gap-fill/coverage", window.location.origin);
  url.searchParams.set("symbol", args.symbol);
  url.searchParams.set("timeframe", args.timeframe);
  url.searchParams.set("start_ts", String(args.start_ts));
  url.searchParams.set("end_ts", String(args.end_ts));
  return asJson(await fetch(url.toString()));
}

export async function flushDatabase(): Promise<any> {
  const res = await fetch("/api/db/flush", { method: "POST" });
  return asJson(res);
}

export async function fetchCoverage(params: { timeframe: string; window: number; limit: number }): Promise<CoverageRow[]> {
  const url = new URL("/api/report/coverage", window.location.origin);
  url.searchParams.set("timeframe", params.timeframe);
  url.searchParams.set("window", String(params.window));
  url.searchParams.set("limit", String(params.limit));
  const json = await asJson<{ rows?: CoverageRow[]; ok?: boolean; error?: string }>(await fetch(url.toString()));
  if (json.ok === false) {
    throw new Error(json.error || "Failed to load coverage");
  }
  return json.rows ?? [];
}

export type WsGroupMetrics = {
  group_id: string;
  pair_count: number;
  max_pairs: number;
  status: string;
  connects: number;
  disconnects: number;
  reconnections: number;
  last_connect: string | null;
  last_disconnect: string | null;
  pairs: [string, string][];
};

export type WsMetricsSummary = {
  timestamp: string;
  max_pairs_per_connection: number;
  group_count: number;
  total_pairs: number;
  total_connects: number;
  total_disconnects: number;
  groups: WsGroupMetrics[];
};

export async function fetchWsMetrics(): Promise<WsMetricsSummary> {
  return asJson(await fetch("/ws/chart/metrics"));
}

