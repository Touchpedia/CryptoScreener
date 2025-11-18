export type RunReq = {
  symbols: string[];
  timeframes: string[];
  start_ts?: number | null;
  end_ts?: number | null;
};
const BASE = ""; // Vite proxy karega -> /api

export async function runIngestion(payload: RunReq) {
  const res = await fetch(`${BASE}/api/ingestion/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`run failed ${res.status}`);
  return res.json();
}

export async function fetchStatus() {
  const res = await fetch(`${BASE}/api/status`);
  if (!res.ok) throw new Error(`status ${res.status}`);
  return res.json();
}
