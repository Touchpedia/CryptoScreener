async function asJson(res) {
    if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}: ${text}`);
    }
    return res.json();
}
export async function runIngestion(payload) {
    const res = await fetch("/api/ingestion/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    });
    return asJson(res);
}
export async function getStatus() {
    const res = await fetch("/api/status", { method: "GET" });
    return asJson(res);
}
export async function flushDatabase() {
    const res = await fetch("/api/db/flush", { method: "POST" });
    return asJson(res);
}
