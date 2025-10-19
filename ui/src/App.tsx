import { useState } from "react";

type Row = {
  exchange?: string;
  symbol: string;
  timeframe?: string;
  ts?: string;
  open?: number; high?: number; low?: number; close?: number; volume?: number;
};

type CoverageRow = {
  symbol: string;
  total_required_candles?: number | null;
  received_candles?: number | null;
  latest_ts: string | null;
};

export default function App() {
  const [status, setStatus] = useState<any>(null);
  const [err, setErr] = useState<string>("");

  // latest candles UI
  const [symbol, setSymbol] = useState("BTC/USDT");
  const [tf, setTf] = useState("1m");
  const [limit, setLimit] = useState(5);
  const [rows, setRows] = useState<Row[]>([]);

  // coverage UI
  const [covRows, setCovRows] = useState<CoverageRow[]>([]);
  const [covBusy, setCovBusy] = useState(false);

  const check = async () => {
    setErr(""); setStatus(null);
    try {
      const res = await fetch("/api/status");
      if (!res.ok) throw new Error("status " + res.status);
      setStatus(await res.json());
    } catch (e:any) {
      setErr(e.message || String(e));
    }
  };

  const loadLatest = async () => {
    setErr("");
    try {
      const url = `/api/candles/latest?symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(tf)}&limit=${limit}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error("latest " + res.status);
      const j = await res.json();
      if (!j.ok) throw new Error(j.error || "unknown");
      setRows(j.rows || []);
    } catch (e:any) {
      setErr(e.message || String(e));
    }
  };

  const loadCoverage = async () => {
    setErr(""); setCovBusy(true);
    try {
      const url = `/api/report/coverage?timeframe=1m&window=6000&limit=100`;
      const res = await fetch(url);
      if (!res.ok) throw new Error("coverage " + res.status);
      const j = await res.json();
      if (!j.ok) throw new Error(j.error || "unknown");
      setCovRows(j.rows || []);
    } catch (e:any) {
      setErr(e.message || String(e));
    } finally {
      setCovBusy(false);
    }
  };

  return (
    <div style={{maxWidth: 1100, margin: "24px auto", padding: 16, fontFamily: "system-ui"}}>
      <h2>Crypto Screener — Health, Latest & Coverage</h2>

      <div style={{display:"flex", gap:8, alignItems:"center"}}>
        <button onClick={check} style={{padding:"8px 12px"}}>Check Status</button>
        {status && <code style={{background:"#111",color:"#0f0",padding:"4px 8px",borderRadius:6}}>
          {JSON.stringify(status)}
        </code>}
      </div>

      {err && <pre style={{background:"#200", color:"#f88", padding:12, marginTop:12}}>Error: {err}</pre>}

      <hr style={{margin:"16px 0"}}/>

      <h3>Latest Candles</h3>
      <div style={{display:"grid", gridTemplateColumns:"2fr 1fr 1fr auto", gap:8, alignItems:"end"}}>
        <div>
          <label>Symbol</label>
          <input value={symbol} onChange={e=>setSymbol(e.target.value)} style={{width:"100%"}}/>
        </div>
        <div>
          <label>Timeframe</label>
          <input value={tf} onChange={e=>setTf(e.target.value)} style={{width:"100%"}}/>
        </div>
        <div>
          <label>Limit</label>
          <input type="number" value={limit} min={1} max={500}
                 onChange={e=>setLimit(parseInt(e.target.value||"5"))} style={{width:"100%"}}/>
        </div>
        <button onClick={loadLatest} style={{padding:"8px 12px"}}>Load Latest</button>
      </div>

      <div style={{marginTop:16, overflowX:"auto"}}>
        <table style={{width:"100%", borderCollapse:"collapse"}}>
          <thead>
            <tr>
              <th style={{textAlign:"left",borderBottom:"1px solid #444"}}>TS</th>
              <th style={{textAlign:"right",borderBottom:"1px solid #444"}}>Open</th>
              <th style={{textAlign:"right",borderBottom:"1px solid #444"}}>High</th>
              <th style={{textAlign:"right",borderBottom:"1px solid #444"}}>Low</th>
              <th style={{textAlign:"right",borderBottom:"1px solid #444"}}>Close</th>
              <th style={{textAlign:"right",borderBottom:"1px solid #444"}}>Vol</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r,i)=>(
              <tr key={i}>
                <td>{r.ts ? new Date(r.ts).toLocaleString() : ""}</td>
                <td style={{textAlign:"right"}}>{r.open}</td>
                <td style={{textAlign:"right"}}>{r.high}</td>
                <td style={{textAlign:"right"}}>{r.low}</td>
                <td style={{textAlign:"right"}}>{r.close}</td>
                <td style={{textAlign:"right"}}>{r.volume}</td>
              </tr>
            ))}
            {rows.length===0 && <tr><td colSpan={6} style={{opacity:0.6,padding:8}}>No data loaded yet.</td></tr>}
          </tbody>
        </table>
      </div>

      <hr style={{margin:"24px 0"}}/>

      <h3>Coverage Report — Top 100 (USDT), 1m, last 6000</h3>
      <button onClick={loadCoverage} disabled={covBusy} style={{padding:"8px 12px"}}>
        {covBusy ? "Loading…" : "Generate Coverage"}
      </button>

      <div style={{marginTop:16, overflowX:"auto"}}>
        <table style={{width:"100%", borderCollapse:"collapse"}}>
          <thead>
            <tr>
              <th style={{textAlign:"left",borderBottom:"1px solid #444"}}>Symbol</th>
              <th style={{textAlign:"right",borderBottom:"1px solid #444"}}>Total Required (6000)</th>
              <th style={{textAlign:"right",borderBottom:"1px solid #444"}}>Received</th>
              <th style={{textAlign:"left",borderBottom:"1px solid #444"}}>Latest TS</th>
            </tr>
          </thead>
          <tbody>
            {covRows.map((r,i)=>(
              <tr key={i}>
                <td>{r.symbol}</td>
                <td style={{textAlign:"right"}}>{(r.total_required_candles ?? 0).toString()}</td>
                <td style={{textAlign:"right"}}>{(r.received_candles ?? 0).toString()}</td>
                <td>{r.latest_ts ? new Date(r.latest_ts).toLocaleString() : "-"}</td>
              </tr>
            ))}
            {covRows.length===0 && <tr><td colSpan={4} style={{opacity:0.6,padding:8}}>Press "Generate Coverage".</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}
