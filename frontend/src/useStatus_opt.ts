import { useEffect, useState } from "react";

type Status = {
  status: string;
  redis_ok?: boolean;
  db_ok?: boolean;
  rq_queue?: string;
};

export function useStatus(base = "http://localhost:8000") {
  const [data, setData] = useState<Status | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  async function refresh() {
    try {
      setLoading(true);
      setErr(null);
      const res = await fetch(`${base}/api/status`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setData(await res.json());
    } catch (e:any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { refresh(); }, []);
  return { data, err, loading, refresh };
}
