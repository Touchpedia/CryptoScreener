import { useEffect, useMemo, useState } from "react";

type Row = { symbol:string; total_required:number; received:number; latest_ts:string|null };

export default function App() {
  const [top, setTop] = useState(10);
  const [tf, setTf] = useState("1m");
  const [windowSize, setWindowSize] = useState(6000);
  const [rows, setRows] = useState<Row[]>([]);
  const [running, setRunning] = useState<boolean>(false);
  const [busy, setBusy] = useState<boolean>(false);
  const [err, setErr] = useState<string>("");

  const loadStatus = async () => {
    try {
      const r = await fetch("/api/ingestion/status"); const j = await r.json();
      setRunning(!!j.running);
    } catch (e:any) { setErr(e.message||String(e)); }
  };

  const loadCoverage = async () => {
    try {
      const url = `/api/report/coverage?timeframe=${encodeURIComponent(tf)}&window=${windowSize}&limit=${top}`;
      const r = await fetch(url); const j = await r.json();
      if (!j.ok) throw new Error(j.error||"not ok");
      setRows(j.rows||[]);
    } catch (e:any) { setErr(e.message||String(e)); }
  };

  const toggle = async () => {
    setBusy(true); setErr("");
    try {
      const path = running ? "/api/ingestion/stop" : "/api/ingestion/start";
      const r = await fetch(path, { method:"POST" }); const j = await r.json();
      if (!j.ok) throw new Error(j.error||"not ok");
      setRunning(!running);
    } catch (e:any) { setErr(e.message||String(e)); }
    finally { setBusy(false); }
  };

  const flushDB = async () => {
    if (!confirm("Flush staging tables?")) return;
    setBusy(true); setErr("");
    try {
      const r = await fetch("/api/admin/flush", { method:"POST" }); const j = await r.json();
      if (!j.ok) throw new Error(j.error||"not ok");
    } catch (e:any) { setErr(e.message||String(e)); }
    finally { setBusy(false); }
  };

  useEffect(() => { loadStatus(); loadCoverage(); }, []);

  return (
    <div style={{maxWidth:1100, margin:"24px auto", padding:16, fontFamily:"system-ui"}}>
      <h2>CryptoScreener</h2>

      <div style={{padding:12, border:"1px solid #eee", borderRadius:12, marginBottom:16}}>
        <h3>Ingestion Settings</h3>
        <div style={{display:"grid", gridTemplateColumns:"1fr 1fr 1fr auto", gap:8, alignItems:"end"}}>
          <div>
            <label>Top Symbols</label>
            <input type="number" value={top} onChange={e=>setTop(parseInt(e.target.value||"0"))} style={{width:"100%", padding:8}}/>
          </div>
          <div>
            <label>Interval</label>
            <select value={tf} onChange={e=>setTf(e.target.value)} style={{width:"100%", padding:8}}>
              <option value="1m">1m</option><option value="5m">5m</option><option value="15m">15m</option>
            </select>
          </div>
          <div>
            <label>Candles per Symbol</label>
            <input type="number" value={windowSize} onChange={e=>setWindowSize(parseInt(e.target.value||"0"))} style={{width:"100%", padding:8}}/>
          </div>
          <div style={{display:"flex", gap:8}}>
            <button onClick={loadCoverage} style={{padding:"10px 14px"}}>Refresh</button>
            <button onClick={toggle} disabled={busy} style={{padding:"10px 14px", background: running ? "#ef4444" : "#22c55e", color:"#fff", border:"none", borderRadius:8}}>
              {busy ? (running ? "Stopping..." : "Starting...") : (running ? "Stop Ingestion" : "Start Ingestion")}
            </button>
            <button onClick={flushDB} disabled={busy} style={{padding:"10px 14px", background:"#0ea5e9", color:"#fff", border:"none", borderRadius:8}}>
              Flush DB
            </button>
          </div>
        </div>
      </div>

      {err && <pre style={{background:"#200", color:"#f88", padding:10, borderRadius:8}}>{err}</pre>}

      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%", borderCollapse:"collapse"}}>
          <thead><tr><th style={{textAlign:"left"}}>Symbol</th><th>Total Required</th><th>Received</th><th>Latest TS</th></tr></thead>
          <tbody>
            {rows.map((r:Row,i:number)=>(
              <tr key={i}>
                <td>{r.symbol}</td>
                <td>{r.total_required}</td>
                <td>{r.received}</td>
                <td>{r.latest_ts ?? "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
