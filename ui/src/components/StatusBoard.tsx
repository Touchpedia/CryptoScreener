import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchStatusSnapshot, PairProgress, RunProgress } from "../lib/api";

function formatPercent(value: number | null | undefined) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "0%";
  }
  return `${value.toFixed(1)}%`;
}

function formatTime(value: string | null | undefined) {
  if (!value) {
    return "-";
  }
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

function statusClass(status: string | null | undefined) {
  if (!status) return "status-pill";
  return `status-pill status-pill--${status.toLowerCase()}`;
}

export default function StatusBoard() {
  const [pairs, setPairs] = useState<PairProgress[]>([]);
  const [run, setRun] = useState<RunProgress | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const displayedPairs = useMemo(() => {
    const copy = [...pairs];
    copy.sort((a, b) => {
      const aUpdated = a.updatedAt ? Date.parse(a.updatedAt) : 0;
      const bUpdated = b.updatedAt ? Date.parse(b.updatedAt) : 0;
      if (bUpdated !== aUpdated) {
        return bUpdated - aUpdated;
      }
      return a.pair.localeCompare(b.pair);
    });
    return copy;
  }, [pairs]);

  const load = useCallback(async () => {
    try {
      setError(null);
      const snapshot = await fetchStatusSnapshot();
      setPairs(snapshot.pairs ?? []);
      setRun(snapshot.run ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load status");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    const id = window.setInterval(load, 5000);
    return () => window.clearInterval(id);
  }, [load]);

  useEffect(() => {
    const handleStart = () => {
      window.setTimeout(load, 500);
    };
    window.addEventListener("ingestion:started", handleStart);
    return () => window.removeEventListener("ingestion:started", handleStart);
  }, [load]);

  return (
    <section className="panel" aria-labelledby="status-heading">
      <header className="panel__header">
        <div>
          <h2 id="status-heading">Ingestion Status</h2>
          <p>Live view of pair progress and the most recent run.</p>
        </div>
        {run && (
          <div className="run-indicator">
            <span className={statusClass(run.status)}>{run.status ?? "unknown"}</span>
            <span>{formatPercent(run.percent)}</span>
            {run.symbol && run.timeframe && <span>{`${run.symbol} (${run.timeframe})`}</span>}
          </div>
        )}
      </header>

      {loading && <p className="hint">Loading status...</p>}
      {error && <p className="error">{error}</p>}

      {!loading && !error && displayedPairs.length === 0 && <p className="hint">No status available yet.</p>}

      {displayedPairs.length > 0 && (
        <div className="status-table">
          <div className="status-table__header">
            <span>Pair</span>
            <span>Status</span>
            <span>Progress</span>
            <span>Updated</span>
          </div>
          <div className="status-table__body">
            {displayedPairs.map((pair) => (
              <div key={pair.pair} className="status-table__row">
                <span>{pair.pair}</span>
                <span className={statusClass(pair.status)}>{pair.status ?? "idle"}</span>
                <span>{formatPercent(pair.progress)}</span>
                <span>{formatTime(pair.updatedAt)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
