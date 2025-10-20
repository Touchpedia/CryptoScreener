import { useEffect, useRef, useState } from "react";

// KILL flicker: no interim phases, immediate UI switch, hard polling every 500ms.
// (WS optional later; this guarantees stability with Redis-backed /status.)

async function getStatus(): Promise<boolean> {
  const r = await fetch("/api/ingestion/status");
  const j = await r.json();
  return !!j.running;
}

export default function Controls() {
  const [running, setRunning] = useState<boolean>(false);
  const timerRef = useRef<any>(null);

  // Hard polling loop (500ms) to keep UI in lockstep with backend
  useEffect(() => {
    (async () => { try { setRunning(await getStatus()); } catch {} })();
    timerRef.current = setInterval(async () => {
      try { const v = await getStatus(); setRunning(v); } catch {}
    }, 500);
    return () => { try { clearInterval(timerRef.current); } catch {} };
  }, []);

  const toggle = async () => {
    // Instant optimistic flip
    const next = !running;
    setRunning(next);
    try {
      const r = await fetch(next ? "/api/ingestion/start" : "/api/ingestion/stop", { method:"POST" });
      const j = await r.json();
      if (!j.ok) throw new Error(j.error || "not ok");
      // backend will confirm via polling within 0.5s; no extra waits here
    } catch {
      // revert on failure; polling will also correct
      setRunning(!next);
    }
  };

  const bg = running ? "#ef4444" : "#22c55e";
  const label = running ? "Stop Ingestion" : "Start Ingestion";

  return (
    <div style={{display:"grid", gap:12}}>
      <button
        onClick={toggle}
        style={{ padding:"12px 16px", borderRadius:12, border:"none", background:bg, color:"#fff", cursor:"pointer", width:"100%", fontWeight:800 }}
        aria-pressed={running}
      >
        {label}
      </button>
      <div style={{display:"flex", justifyContent:"space-between", alignItems:"center", padding:"8px 12px", border:"1px solid #ddd", borderRadius:10}}>
        <span>Status:</span>
        <strong style={{color: running ? "#16a34a" : "#b91c1c"}}>{running ? "Running" : "Stopped"}</strong>
        <button onClick={async()=>{ try{ setRunning(await getStatus()); }catch{} }} style={{padding:"6px 10px", borderRadius:8}}>Refresh</button>
      </div>
    </div>
  );
}
