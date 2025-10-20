type Row = { id:number; symbol:string; state:string; progress:number };
export default function StatusTable({ rows }:{ rows: Row[] }) {
  if (!rows?.length) return <div style={{padding:8,opacity:.7}}>No rows</div>;
  return (
    <div style={{border:'1px solid #ddd', borderRadius:6, overflow:'hidden'}}>
      <div style={{display:'grid',gridTemplateColumns:'1fr 100px 120px',background:'#f7f7f7',padding:'8px 12px',fontWeight:600}}>
        <div>Pair</div><div>State</div><div>Progress</div>
      </div>
      {rows.map(r=>(
        <div key={r.id} style={{display:'grid',gridTemplateColumns:'1fr 100px 120px',padding:'8px 12px',borderTop:'1px solid #eee'}}>
          <div>{r.symbol}</div><div>{r.state}</div><div>{r.progress}%</div>
        </div>
      ))}
    </div>
  );
}
