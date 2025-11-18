import React, { useEffect, useState } from "react";

export default function SymbolsPanel() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [err, setErr] = useState<string>("");

  async function load() {
    try {
      const res = await fetch("/api/ingestion/symbols_clean?segment=volume&count=50", { cache: "no-store" });
      if (!res.ok) throw new Error("HTTP " + (res?.status ?? 0));
      const data = await res.json();
      const arr = Array.isArray(data) ? data : (Array.isArray(data?.symbols) ? data.symbols : []);
      const list = arr.map((x:any) => typeof x === "string" ? x : (x?.symbol ?? "")).filter(Boolean);
      setSymbols(list);
      setErr("");
    } catch (e:any) {
      setErr(e?.message ?? "failed");
      setSymbols([]);
    }
  }

  useEffect(() => { load(); }, []);

  return (
    <div style={{ padding: 12 }}>
      <div style={{ marginBottom: 8 }}>
        <button onClick={load} style={{ padding: "6px 10px", border: "1px solid #d1d5db", borderRadius: 6, cursor: "pointer" }}>
          Load
        </button>
        {err && <span style={{ marginLeft: 10, color: "crimson", fontSize: 12 }}>ERR: {err}</span>}
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {symbols.map((s, i) => (
          <span key={i} style={{ border: "1px solid #e5e7eb", padding: "4px 10px", borderRadius: 999 }}>{s}</span>
        ))}
        {!symbols.length && !err && <div style={{ color: "#6b7280", fontSize: 13 }}>No symbols yet — “Load” dabayein.</div>}
      </div>
    </div>
  );
}
