import { useEffect, useState } from "react";
import { fetchStatus, fetchHealth } from "../lib/api";

type StatusRow = {
  exchange: string; symbol: string; timeframe: string;
  latest_ts: string; lag_seconds: number; rows_last_60m: number;
};
type Candle = { ts:string; open:number; high:number; low:number; close:number; volume:number; };

export default function StatusCard() {
  const [row, setRow] = useState<StatusRow | null>(null);
  const [latest, setLatest] = useState<Candle[]>([]);
  const [ok, setOk] = useState<boolean | null>(null);
  const [err, setErr] = useState<string | null>(null);

  async function load() {
    try {
      setErr(null);
      const [s, h] = await Promise.all([fetchStatus(), fetchHealth()]);
      setRow(s?.status?.[0] ?? null);
      setLatest(s?.latest_5 ?? []);
      setOk(!!h?.ok);
    } catch (e:any) {
      setErr(e?.message || "fetch error");
    }
  }

  useEffect(() => { load(); const id = setInterval(load, 30000); return () => clearInterval(id); }, []);

  const badgeClass = ok === null ? "bg-gray-500" : ok ? "bg-green-600" : "bg-red-600";

  return (
    <div className="max-w-3xl mx-auto bg-slate-900 text-slate-100 border border-slate-700 rounded-2xl p-5 shadow-lg">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold">Ingest Status</h2>
        <div className="flex items-center gap-2">
          <span className={`inline-block w-3 h-3 rounded-full ${badgeClass}`}></span>
          <span className="text-sm">{ok === null ? "checking…" : ok ? "OK" : "LATE"}</span>
        </div>
      </div>

      {row ? (
        <div className="mt-3 flex flex-wrap gap-2 text-sm">
          <span className="px-2 py-1 rounded-full bg-slate-800 border border-slate-700">exch: {row.exchange}</span>
          <span className="px-2 py-1 rounded-full bg-slate-800 border border-slate-700">symbol: {row.symbol}</span>
          <span className="px-2 py-1 rounded-full bg-slate-800 border border-slate-700">tf: {row.timeframe}</span>
          <span className="px-2 py-1 rounded-full bg-slate-800 border border-slate-700">lag: {row.lag_seconds}s</span>
          <span className="px-2 py-1 rounded-full bg-slate-800 border border-slate-700">rows_60m: {row.rows_last_60m}</span>
          <span className="px-2 py-1 rounded-full bg-slate-800 border border-slate-700">latest: {row.latest_ts}</span>
        </div>
      ) : (
        <p className="mt-3 text-sm text-slate-300">loading status…</p>
      )}

      {err && <p className="mt-3 text-sm text-rose-400">Error: {err}</p>}

      <div className="mt-5">
        <h3 className="font-medium mb-2">Latest 5 candles</h3>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm border border-slate-700 rounded-lg overflow-hidden">
            <thead className="bg-slate-800">
              <tr>
                <th className="text-left px-3 py-2 border-b border-slate-700">TS</th>
                <th className="px-3 py-2 border-b border-slate-700">Open</th>
                <th className="px-3 py-2 border-b border-slate-700">High</th>
                <th className="px-3 py-2 border-b border-slate-700">Low</th>
                <th className="px-3 py-2 border-b border-slate-700">Close</th>
                <th className="px-3 py-2 border-b border-slate-700">Vol</th>
              </tr>
            </thead>
            <tbody>
              {latest.map((c) => (
                <tr key={c.ts} className="odd:bg-slate-900 even:bg-slate-950">
                  <td className="text-left px-3 py-2">{c.ts}</td>
                  <td className="px-3 py-2">{c.open.toLocaleString()}</td>
                  <td className="px-3 py-2">{c.high.toLocaleString()}</td>
                  <td className="px-3 py-2">{c.low.toLocaleString()}</td>
                  <td className="px-3 py-2">{c.close.toLocaleString()}</td>
                  <td className="px-3 py-2">{c.volume.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

    </div>
  );
}

// synced 2025-10-20 01:39:11Z

