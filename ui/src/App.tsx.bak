import { useState, useEffect } from "react";

export default function App() {
  const [topSymbols, setTopSymbols] = useState(10);
  const [interval, setIntervalTf] = useState("1m");
  const [candlesPerSymbol, setCandlesPerSymbol] = useState(6000);
  const [rows, setRows] = useState<CoverageRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [ingesting, setIngesting] = useState(false);
  const [starting, setStarting] = useState(false);

  async function startIngestion() {
    try {
      await fetch("/api/ingestion/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          top_symbols: topSymbols,
          interval,
          candles_per_symbol: candlesPerSymbol,
        }),
      });
    } catch {}
  }

  async function stopIngestion() {
    try {
      await fetch("/api/ingestion/stop", { method: "POST" });
    } catch {}
  }

  async function checkStatus() {
    try {
      const r = await fetch("/api/status");
      const j = await r.json();
      setIngesting(j?.running === true);
    } catch {
      setIngesting(false);
    }
  }

  async function flushDB() {
    try {
      const r=await fetch("/api/db/flush",{method:"POST"});const j=await r.json().catch(()=>({message:"Flushed"}));alert(j?.message||"DB Flushed!");
      alert("DB Flushed!");
    } catch {
      alert("Flush failed");
    }
  }

  async function loadCoverage() {
    try {
      setLoading(true);
      const r = await fetch("/api/report/coverage?timeframe=" + interval + "&window=" + candlesPerSymbol + "&limit=" + topSymbols + "&t=" + Date.now() + interval + "&window=" + candlesPerSymbol + "&limit=" + topSymbols + "&t=" + Date.now());
      const j = await r.json();
      setRows(Array.isArray(j) ? j : j?.rows ?? []);
    } catch {
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    checkStatus();
    loadCoverage();
    const id = setInterval(() => {
      checkStatus();
      loadCoverage();
    }, 10000);
    return () => clearInterval(id);
  }, []);

  return (
    <div style={{ padding: 20, fontFamily: "Inter, sans-serif" }}>
      <h2 style={{ marginBottom: 10 }}>CryptoScreener</h2>

      <div
        style={{
          border: "1px solid #e5e7eb",
          borderRadius: 10,
          padding: 12,
          marginBottom: 14,
        }}
      >
        <h3>Ingestion Settings</h3>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr 1fr auto",
            gap: 10,
            alignItems: "end",
          }}
        >
          <div>
            <label>Top Symbols</label>
            <input
              type="number"
              value={topSymbols}
              onChange={(e) => setTopSymbols(+e.target.value)}
              style={{
                width: "100%",
                border: "1px solid #ccc",
                borderRadius: 6,
                padding: 6,
              }}
            />
          </div>
          <div>
            <label>Interval</label>
            <select
              value={interval}
              onChange={(e) => setIntervalTf(e.target.value)}
              style={{
                width: "100%",
                border: "1px solid #ccc",
                borderRadius: 6,
                padding: 6,
              }}
            >
              <option value="1m">1m</option>
              <option value="5m">5m</option>
              <option value="1h">1h</option>
            </select>
          </div>
          <div>
            <label>Candles per Symbol</label>
            <input
              type="number"
              value={candlesPerSymbol}
              onChange={(e) => setCandlesPerSymbol(+e.target.value)}
              style={{
                width: "100%",
                border: "1px solid #ccc",
                borderRadius: 6,
                padding: 6,
              }}
            />
          </div>
          <button
            onClick={async () => {
              if (ingesting) {
                await stopIngestion();
                setIngesting(false);
              } else {
                setStarting(true);
                await startIngestion();
                for (let i = 0; i < 10; i++) {
                  await new Promise((r) => setTimeout(r, 1000));
                  const s = await fetch("/api/status");
                  const j = await s.json();
                  if (j?.running) {
                    setIngesting(true);
                    break;
                  }
                }
                setStarting(false);
              }
            }}
            style={{
              background: starting
                ? "#facc15"
                : ingesting
                ? "#ef4444"
                : "#22c55e",
              color: "#fff",
              padding: "8px 14px",
              borderRadius: 8,
              border: "none",
            }}
          >
            {starting
              ? "Starting..."
              : ingesting
              ? "Stop Ingestion (backend) (backend)"
              : "Start Ingestion"}
          </button>
        </div>
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 10 }}>
        <button
          onClick={loadCoverage}
          disabled={loading}
          style={{
            border: "1px solid #ccc",
            borderRadius: 8,
            padding: "8px 14px",
          }}
        >
          {loading ? "Loading..." : "Refresh"}
        </button> <button onClick={async()=>{setStarting(true);try{await fetch("/api/ingestion/stop",{method:"POST"});for(let i=0;i<15;i++){await new Promise(r=>setTimeout(r,1000));const s=await fetch("/api/status");const j=await s.json();if(!j?.running){setIngesting(false);break}}}catch(e){}finally{setStarting(false)}}} style={{marginLeft:8,background:"#ef4444",color:"#fff",padding:"8px 14px",borderRadius:8,border:"none"}}>Stop Ingestion (backend) (backend)</button>
        <button
          onClick={flushDB}
          style={{
            background: "#0ea5e9",
            color: "white",
            padding: "8px 14px",
            borderRadius: 8,
            border: "none",
          }}
        >
          🗑 Flush DB
        </button>
      </div>

      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #e5e7eb" }}>
            <th align="left" style={{ padding: 6 }}>
              Symbol
            </th>
            <th align="right" style={{ padding: 6 }}>
              Total Required
            </th>
            <th align="right" style={{ padding: 6 }}>
              Received
            </th>
            <th align="left" style={{ padding: 6 }}>
              Latest TS
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.symbol} style={{ borderBottom: "1px solid #f3f4f6" }}>
              <td style={{ padding: 6 }}>{r.symbol}</td>
              <td align="right" style={{ padding: 6 }}>
                {r.total_required ?? 6000}
              </td>
              <td align="right" style={{ padding: 6 }}>
                {r.received ?? 0}
              </td>
              <td style={{ padding: 6 }}>
                {r.latest_ts
                  ? new Date(r.latest_ts).toLocaleString([], { timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone })
                  : "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}









