import { useEffect, useState } from "react";

type Segment = "all" | "market_cap" | "volume" | "gainers" | "losers";

export default function SymbolsPanel() {
  const [segment, setSegment] = useState<Segment>("market_cap");
  const [limit, setLimit] = useState<number>(26);
  const [loading, setLoading] = useState(false);
  const [symbols, setSymbols] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true); setError(null);
    try {
      const r = await fetch(`/api/ingestion/symbols?segment=${segment}&limit=${limit}`);
      const j = await r.json();
      if (!j.ok) throw new Error("bad response");
      setSymbols(Array.isArray(j.symbols) ? j.symbols : []);
    } catch (e: any) {
      setError(e?.message || "failed to load");
      setSymbols([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []); // auto-load once

  return (
    <div style={{display:"grid", gap:12, marginTop:12}}>
      <div style={{display:"flex", gap:12, alignItems:"center"}}>
        <label>
          Segment:&nbsp;
          <select value={segment} onChange={e => setSegment(e.target.value as Segment)}>
            <option value="market_cap">Market Cap (Top)</option>
            <option value="volume">Volume (Top)</option>
            <option value="gainers">Gainers (24h)</option>
            <option value="losers">Losers (24h)</option>
            <option value="all">All (USDT Pairs)</option>
          </select>
        </label>
        <label>
          Count:&nbsp;
          <input type="number" min={1} max={500} value={limit} onChange={e => setLimit(Number(e.target.value)||1)} style={{width:80}} />
        </label>
        <button onClick={load} disabled={loading} style={{padding:"8px 12px", borderRadius:8}}>
          {loading ? "Loading..." : "Load"}
        </button>
      </div>

      {error && <div style={{color:"#b91c1c"}}>Error: {error}</div>}

      <div style={{border:"1px solid #ddd", borderRadius:10, padding:12}}>
        <div style={{fontWeight:700, marginBottom:8}}>Symbols ({symbols.length})</div>
        <div style={{display:"flex", flexWrap:"wrap", gap:8}}>
          {symbols.map((s) => (
            <span key={s} style={{padding:"6px 10px", border:"1px solid #eee", borderRadius:999}}>
              {s}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
