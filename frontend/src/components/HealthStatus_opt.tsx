import React from "react";
import { useStatus } from "../useStatus_opt";

export default function HealthStatusOpt() {
  const { data, err, loading, refresh } = useStatus();

  const Chip = ({ ok, label }: { ok: boolean | undefined; label: string }) => (
    <span style={{
      padding: "4px 10px",
      borderRadius: "999px",
      fontSize: 12,
      background: ok ? "rgba(16,185,129,0.15)" : "rgba(239,68,68,0.15)",
      color: ok ? "rgb(16,185,129)" : "rgb(239,68,68)",
      border: `1px solid ${ok ? "rgba(16,185,129,0.35)" : "rgba(239,68,68,0.35)"}`
    }}>
      {label}: {ok ? "OK" : "Fail"}
    </span>
  );

  return (
    <div style={{
      display: "flex", flexDirection: "column", gap: 8, padding: 16,
      border: "1px solid rgba(0,0,0,0.08)", borderRadius: 12, boxShadow: "0 2px 10px rgba(0,0,0,0.04)"
    }}>
      <div style={{display:"flex", justifyContent:"space-between", alignItems:"center"}}>
        <strong>System Health</strong>
        <button onClick={refresh} style={{padding:"6px 10px", borderRadius:8, border:"1px solid rgba(0,0,0,0.12)", background:"white", cursor:"pointer"}}>
          Refresh
        </button>
      </div>
      {loading && <div>Loading…</div>}
      {!loading && err && <div style={{color:"#ef4444"}}>Error: {err}</div>}
      {!loading && data && (
        <div style={{display:"flex", gap:8, flexWrap:"wrap"}}>
          <Chip ok={data.db_ok} label="DB" />
          <Chip ok={data.redis_ok} label="Redis" />
          <span style={{fontSize:12, opacity:0.8}}>Queue: {data.rq_queue ?? "-"}</span>
        </div>
      )}
    </div>
  );
}
