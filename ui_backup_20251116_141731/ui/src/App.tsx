import React, { useState } from "react";

export default function App() {
  const [status, setStatus] = useState<string>("(not checked)");
  async function ping() {
    try {
      const r = await fetch("/api/status?t=" + Date.now(), { cache: "no-store" });
      setStatus(r.ok ? "OK" : ("HTTP " + r.status));
    } catch (e:any) { setStatus("ERR: " + (e?.message ?? "failed")); }
  }
  return (
    <div style={{fontFamily:"Inter, system-ui, Arial", padding:16}}>
      <h1 style={{margin:0}}>CryptoScreener — MAIN UI</h1>
      <p style={{color:"#555"}}>Ye default dashboard hai. Test extensions kabhi yahan replace nahi hongi.</p>
      <div style={{border:"1px solid #e5e7eb", borderRadius:8, padding:12}}>
        <button onClick={ping} style={{padding:"6px 10px", border:"1px solid #d1d5db", borderRadius:6, cursor:"pointer"}}>Ping /api/status</button>
        <span style={{marginLeft:10, fontSize:12, color:"#6b7280"}}> Status: {status}</span>
      </div>
    </div>
  );
}
