import { FormEvent, useMemo, useState } from "react";
import { fetchGapCoverage, GapCoverage, GapFillPayload, GapFillResult, triggerGapFill } from "../lib/api";

function parseList(value: string): string[] {
  return value
    .split(/[\n,]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function toTimestamp(value: string): number {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? Date.now() : date.getTime();
}

function toInputValue(date: Date): string {
  const iso = date.toISOString();
  return iso.slice(0, 16);
}

export default function AdminPanel() {
  const now = useMemo(() => new Date(), []);
  const [symbolsText, setSymbolsText] = useState("BTC/USDT\nETH/USDT");
  const [timeframesText, setTimeframesText] = useState("1m\n5m");
  const [start, setStart] = useState(toInputValue(new Date(now.getTime() - 24 * 60 * 60 * 1000)));
  const [end, setEnd] = useState(toInputValue(now));
  const [loadingFill, setLoadingFill] = useState(false);
  const [fillResults, setFillResults] = useState<GapFillResult[]>([]);
  const [fillMessage, setFillMessage] = useState<string>("");
  const [coverage, setCoverage] = useState<GapCoverage | null>(null);
  const [coverageLoading, setCoverageLoading] = useState(false);
  const [coverageError, setCoverageError] = useState<string | null>(null);

  const handleGapFill = async (event: FormEvent) => {
    event.preventDefault();
    const symbols = parseList(symbolsText);
    const timeframes = parseList(timeframesText);
    if (!symbols.length || !timeframes.length) {
      setFillMessage("Provide at least one symbol and timeframe.");
      return;
    }
    const payload: GapFillPayload = {
      symbols,
      timeframes,
      start_ts: toTimestamp(start),
      end_ts: toTimestamp(end),
    };
    try {
      setLoadingFill(true);
      setFillMessage("");
      const response = await triggerGapFill(payload);
      setFillResults(response.results ?? []);
      setFillMessage(`Gap fill completed for ${response.results?.length ?? 0} combinations.`);
    } catch (err) {
      setFillMessage(err instanceof Error ? err.message : "Gap fill failed");
      setFillResults([]);
    } finally {
      setLoadingFill(false);
    }
  };

  const handleCoverage = async () => {
    const [symbol] = parseList(symbolsText);
    const [timeframe] = parseList(timeframesText);
    if (!symbol || !timeframe) {
      setCoverageError("Provide at least one symbol and timeframe to compute coverage.");
      return;
    }
    try {
      setCoverageLoading(true);
      setCoverageError(null);
      const response = await fetchGapCoverage({
        symbol,
        timeframe,
        start_ts: toTimestamp(start),
        end_ts: toTimestamp(end),
      });
      setCoverage(response.coverage);
    } catch (err) {
      setCoverageError(err instanceof Error ? err.message : "Unable to compute coverage");
      setCoverage(null);
    } finally {
      setCoverageLoading(false);
    }
  };

  return (
    <section className="panel" aria-labelledby="admin-heading">
      <header className="panel__header">
        <div>
          <h2 id="admin-heading">Admin Tools</h2>
          <p>Manually trigger gap fills and inspect coverage for critical pairs.</p>
        </div>
      </header>

      <form className="panel__body" onSubmit={handleGapFill}>
        <div className="grid two-columns">
          <label>
            Symbols
            <textarea value={symbolsText} onChange={(event) => setSymbolsText(event.target.value)} rows={6} />
          </label>
          <label>
            Timeframes
            <textarea value={timeframesText} onChange={(event) => setTimeframesText(event.target.value)} rows={6} />
          </label>
        </div>

        <div className="date-grid">
          <label>
            Start time
            <input type="datetime-local" value={start} onChange={(event) => setStart(event.target.value)} />
          </label>
          <label>
            End time
            <input type="datetime-local" value={end} onChange={(event) => setEnd(event.target.value)} />
          </label>
        </div>

        <footer className="panel__footer">
          <button type="submit" disabled={loadingFill}>
            {loadingFill ? "Running gap fill…" : "Run gap fill"}
          </button>
          <button type="button" className="secondary" onClick={handleCoverage} disabled={coverageLoading}>
            {coverageLoading ? "Checking…" : "Coverage report"}
          </button>
        </footer>
      </form>

      {fillMessage && <p className="message">{fillMessage}</p>}
      {fillResults.length > 0 && (
        <div className="gap-results">
          <h3>Latest gap fill results</h3>
          <ul>
            {fillResults.map((result) => (
              <li key={`${result.symbol}-${result.timeframe}-${result.start_ts}`}>
                {result.symbol} · {result.timeframe} — inserted {result.inserted} candles
              </li>
            ))}
          </ul>
        </div>
      )}

      {coverageError && <p className="error">{coverageError}</p>}
      {coverage && (
        <div className="gap-coverage">
          <h3>Coverage</h3>
          <p>
            {coverage.symbol} · {coverage.timeframe} — {coverage.present}/{coverage.expected} candles ({coverage.coverage.toFixed(2)}%)
          </p>
        </div>
      )}
    </section>
  );
}
