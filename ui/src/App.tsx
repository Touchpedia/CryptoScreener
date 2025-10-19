import React, { useState } from "react";

export default function App() {
  const [ping, setPing] = useState<string>("(not checked)");

  async function pingApi() {
    try {
      const res = await fetch("/api/report/coverage?t=" + Date.now(), { cache: "no-store" });
      if (!res.ok) throw new Error("HTTP " + res.status);
      const data = await res.json();
      const count = Array.isArray(data) ? data.length : (Array.isArray(data?.rows) ? data.rows.length : 0);
      setPing(`OK • rows=${count}`);
    } catch (e:any) {
      setPing("ERR • " + (e?.message ?? "failed"));
      console.error(e);
    }
  }

  return (
    <div style={{ fontFamily: "Inter, system-ui, Arial, sans-serif", padding: 16 }}>
      <h2 style={{ marginBottom: 8 }}>CryptoScreener — UI OK</h2>
      <p style={{ color: "#374151", marginBottom: 16 }}>
        Ye minimal screen is liye hai taa-ke blank-page bug isolate ho. Ab is par step-by-step features dubara add karenge.
      </p>

      <div style={{ border: "1px solid #e5e7eb", borderRadius: 10, padding: 12, marginBottom: 12 }}>
        <h3 style={{ marginTop: 0 }}>Diagnostics</h3>
        <button
          onClick={pingApi}
          style={{ padding: "6px 10px", border: "1px solid #d1d5db", borderRadius: 6, cursor: "pointer" }}
        >
          Ping /api/report/coverage
        </button>
        <span style={{ marginLeft: 12, fontSize: 12, color: "#6b7280" }}>Status: {ping}</span>
      </div>

      <div style={{ fontSize: 12, color: "#6b7280" }}>
        If this page is visible, React render pipeline is healthy. Next we add Symbol dropdown & your existing controls safely.
      </div>
    </div>
  );
}
