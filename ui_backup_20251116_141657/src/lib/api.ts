export type IngestionTaskPayload = {
  timeframe: string;
  candles_per_symbol?: number;
  start_iso?: string;
  end_iso?: string;
};

export type IngestionPayload = {
  symbols: string[];
  tasks: IngestionTaskPayload[];
};

async function asJson(res: Response) {
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

export async function runIngestion(payload: IngestionPayload): Promise<any> {
  const res = await fetch("/api/ingestion/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return asJson(res);
}

export async function getStatus(): Promise<any> {
  const res = await fetch("/api/status", { method: "GET" });
  return asJson(res);
}

export async function flushDatabase(): Promise<any> {
  const res = await fetch("/api/db/flush", { method: "POST" });
  return asJson(res);
}
