import React, { useEffect, useState } from 'react';
import StatusToolbar from './components/StatusToolbar';

type Row = { pair: string; status?: string; progress?: number; gaps?: number; timeframes?: Record<string, number> };

export default function StatusPage() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const load = async () => {
    setLoading(true); setErr(null);
    try {
      const res = await fetch('http://127.0.0.1:8000/api/status');
      const json = await res.json();
      setRows(Array.isArray(json?.items) ? json.items : []);
    } catch (e:any) { setErr(e?.message || 'error'); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, []);

  return (
    <div style={{ padding: 16 }}>
      <div style={{ marginBottom: 8, fontWeight: 600 }}>STATUS PAGE ?</div>
      <div style={{ marginBottom: 12 }}>
        <StatusToolbar />
        <button onClick={load} style={{marginLeft:8, padding:'6px 10px', border:'1px solid #ddd', borderRadius:6}}>Manual Refresh</button>
      </div>
      {err && <div style={{color:'red'}}>Error: {err}</div>}
      {loading && <div>loading…</div>}
      {rows.length === 0 ? (
        <div>No rows</div>
      ) : (
        <div>
          <div style={{marginBottom:8}}>Rows: {rows.length}</div>
          {rows.map(r => (
            <div key={r.pair} style={{padding:'8px 10px', border:'1px solid #eee', borderRadius:8, marginBottom:6}}>
              <div><b>{r.pair}</b> — {r.status ?? 'n/a'}</div>
              <div>progress: {r.progress ?? 0} | gaps: {r.gaps ?? 0}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
