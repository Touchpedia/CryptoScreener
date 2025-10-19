import { useState } from "react";
import "./App.css";
import { SymbolSelect } from "./components/SymbolSelect";

function App() {
  const [diag, setDiag] = useState<null | string>(null);
  const [symbol, setSymbol] = useState<string | null>(null);
  const [tf, setTf] = useState<string>("1m");
  const [loading, setLoading] = useState(false);

  async function pingCoverage() {
    setLoading(true);
    setDiag("Checking…");
    try {
      const res = await fetch("/api/report/coverage", { cache: "no-store", headers: { "x-ui-diag": "true" }});
      if (!res.ok) throw new Error(res.status + " " + res.statusText);
      const data = await res.json();
      const rows = (data?.rows ?? (Array.isArray(data) ? data.length : "n/a"));
      setDiag(`OK • rows=${rows}`);
    } catch (e: any) { setDiag("ERR • " + (e?.message ?? "unknown")); }
    finally { setLoading(false); }
  }

  return (
    <div className="p-6 max-w-3xl mx-auto space-y-6">
      <h1 className="text-2xl font-bold">CryptoScreener — UI OK (phase1 020719)</h1>

      <div className="rounded-xl border p-4 space-y-3">
        <h2 className="font-semibold">Diagnostics</h2>
        <button onClick={pingCoverage} disabled={loading} className="border rounded-md px-3 py-1">
          Ping /api/report/coverage
        </button>
        <div><span className="font-medium">Status:&nbsp;</span><span>{diag ?? "(not checked)"}</span></div>
        <p className="text-sm text-gray-600">
          If this page is visible, React render pipeline is healthy. Next we add Symbol dropdown & your existing controls safely.
        </p>
      </div>

      <div className="rounded-xl border p-4 space-y-4">
        <h2 className="font-semibold">Controls (Phase 1)</h2>
        <div className="flex flex-wrap items-center gap-4">
          <SymbolSelect value={symbol} onChange={setSymbol} />
          <div className="flex items-center gap-2">
            <label className="text-sm font-medium">Timeframe</label>
            <select className="border rounded-md px-2 py-1" value={tf} onChange={(e) => setTf(e.target.value)}>
              <option value="1m">1m</option><option value="5m">5m</option><option value="15m">15m</option>
              <option value="1h">1h</option><option value="4h">4h</option><option value="1d">1d</option>
            </select>
          </div>
          <button
            className="border rounded-md px-3 py-1"
            onClick={() => alert(`TODO: load latest candles for ${symbol ?? "?"} @ ${tf}`)}
            disabled={!symbol}
          >
            Load Latest Candles
          </button>
        </div>
        <div className="text-sm text-gray-600">
          TODO (Phase 2): wire to <code>/api/candles/latest</code> and render mini chart; then restore Coverage/Status widgets.
        </div>
      </div>
    </div>
  );
}
export default App;


