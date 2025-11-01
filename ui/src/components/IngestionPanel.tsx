import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import {
  CoverageRow,
  fetchCoverage,
  fetchIngestionStatus,
  flushDatabaseAdmin,
  runIngestion,
  startIngestion,
  stopIngestion,
} from "../lib/api";
import { useSymbols } from "../hooks/useSymbols";

type TaskRow = { timeframe: string; days: string };

const DEFAULT_TASKS: TaskRow[] = [
  { timeframe: "1m", days: "1" },
  { timeframe: "3m", days: "1" },
  { timeframe: "5m", days: "1" },
];

const TIMEFRAME_MINUTES: Record<string, number> = {
  "1m": 1,
  "3m": 3,
  "5m": 5,
  "15m": 15,
  "30m": 30,
  "1h": 60,
  "2h": 120,
  "4h": 240,
  "1d": 1440,
};

const SEGMENTS = [
  { label: "Alphabetical", value: "all" },
  { label: "Market Cap", value: "market_cap" },
  { label: "24h Volume", value: "volume" },
  { label: "24h Gainers", value: "gainers" },
  { label: "24h Losers", value: "losers" },
] as const;

const PRESETS = [
  { label: "Top 10", limit: 10 },
  { label: "Top 20", limit: 20 },
  { label: "Top 50", limit: 50 },
  { label: "Top 100", limit: 100 },
  { label: "All", limit: null },
] as const;

function computeCandles(timeframe: string, daysValue: string) {
  const minutesPerCandle = TIMEFRAME_MINUTES[timeframe];
  const days = Number(daysValue);
  if (!minutesPerCandle || !Number.isFinite(days) || days <= 0) {
    return null;
  }
  const totalCandles = Math.ceil((days * 24 * 60) / minutesPerCandle);
  return {
    timeframe,
    candles_per_symbol: totalCandles,
  };
}

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "-";
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return value;
  }
  return new Date(parsed).toLocaleString();
}

export default function IngestionPanel() {
  const [segment, setSegment] = useState<string>("all");
  const { symbols: availableSymbols, loading: loadingSymbols, error: symbolError, refresh } = useSymbols(segment);
  const [filter, setFilter] = useState("");
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>([]);
  const [tasks, setTasks] = useState<TaskRow[]>(DEFAULT_TASKS);
  const [selectedPreset, setSelectedPreset] = useState<number | null>(10);
  const [message, setMessage] = useState<string>("");
  const [starting, setStarting] = useState(false);

  const [isRunning, setIsRunning] = useState(false);
  const [controlsBusy, setControlsBusy] = useState(false);

  const [coverageRows, setCoverageRows] = useState<CoverageRow[]>([]);
  const [coverageTimeframe, setCoverageTimeframe] = useState("1m");
  const [coverageWindow, setCoverageWindow] = useState(6000);
  const [coverageLimit, setCoverageLimit] = useState(10);
  const [coverageLoading, setCoverageLoading] = useState(false);
  const [coverageError, setCoverageError] = useState<string | null>(null);

  const filteredSymbols = useMemo(() => {
    if (!filter) {
      return availableSymbols;
    }
    return availableSymbols.filter((symbol) => symbol.toLowerCase().includes(filter.toLowerCase()));
  }, [availableSymbols, filter]);

  const refreshStatus = useCallback(async () => {
    try {
      const status = await fetchIngestionStatus();
      setIsRunning(Boolean(status.running));
    } catch (error) {
      if (error instanceof Error) {
        setMessage((prev) => (prev ? `${prev}\n${error.message}` : error.message));
      }
    }
  }, []);

  const refreshCoverage = useCallback(async () => {
    setCoverageLoading(true);
    setCoverageError(null);
    try {
      const rows = await fetchCoverage({
        timeframe: coverageTimeframe,
        window: coverageWindow,
        limit: coverageLimit,
      });
      setCoverageRows(rows);
    } catch (error) {
      setCoverageRows([]);
      setCoverageError(error instanceof Error ? error.message : "Unable to load coverage");
    } finally {
      setCoverageLoading(false);
    }
  }, [coverageLimit, coverageTimeframe, coverageWindow]);

  useEffect(() => {
    refreshStatus();
    const id = window.setInterval(refreshStatus, 5000);
    return () => window.clearInterval(id);
  }, [refreshStatus]);

  useEffect(() => {
    refreshCoverage();
  }, [refreshCoverage]);

  useEffect(() => {
    if (!availableSymbols.length) {
      return;
    }
    if (selectedPreset === null) {
      setSelectedSymbols(availableSymbols);
    } else {
      setSelectedSymbols(availableSymbols.slice(0, selectedPreset));
    }
  }, [segment, selectedPreset, availableSymbols]);

  const toggleSymbol = (symbol: string) => {
    setSelectedSymbols((prev) =>
      prev.includes(symbol) ? prev.filter((item) => item !== symbol) : [...prev, symbol],
    );
  };

  const applyPreset = (limit: number | null) => {
    setSelectedPreset(limit);
    if (limit === null) {
      setSelectedSymbols(availableSymbols);
    } else {
      setSelectedSymbols(availableSymbols.slice(0, limit));
    }
  };

  const addTask = () => {
    setTasks((prev) => [...prev, { timeframe: "15m", days: "1" }]);
  };

  const updateTask = (index: number, patch: Partial<TaskRow>) => {
    setTasks((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  };

  const removeTask = (index: number) => {
    setTasks((prev) => prev.filter((_, i) => i !== index));
  };

  const handleRunIngestion = async (event: FormEvent) => {
    event.preventDefault();
    setMessage("");
    const payloadTasks = tasks
      .map((row) => computeCandles(row.timeframe, row.days))
      .filter((row): row is { timeframe: string; candles_per_symbol: number } => Boolean(row));

    if (!selectedSymbols.length) {
      setMessage("Select at least one symbol.");
      return;
    }
    if (!payloadTasks.length) {
      setMessage("Add at least one valid timeframe.");
      return;
    }

    try {
      setStarting(true);
      const response = await runIngestion({ symbols: selectedSymbols, tasks: payloadTasks });
      setMessage(`Ingestion started (run ${response.run_id ?? response.runId ?? "-"})`);
      window.dispatchEvent(new CustomEvent("ingestion:started"));
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to start ingestion run");
    } finally {
      setStarting(false);
    }
  };

  const handleToggleIngestion = async () => {
    setControlsBusy(true);
    setMessage("");
    try {
      if (isRunning) {
        await stopIngestion();
        setMessage("Ingestion stopped");
      } else {
        await startIngestion();
        setMessage("Ingestion started");
      }
      await refreshStatus();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Command failed");
    } finally {
      setControlsBusy(false);
    }
  };

  const handleFlushDb = async () => {
    if (!window.confirm("Flush staging tables?")) {
      return;
    }
    setControlsBusy(true);
    setMessage("");
    try {
      await flushDatabaseAdmin();
      setMessage("Database flush completed");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Flush failed");
    } finally {
      setControlsBusy(false);
    }
  };

  const handleSelectAll = () => setSelectedSymbols(availableSymbols);
  const handleClear = () => setSelectedSymbols([]);

  return (
    <section className="panel" aria-labelledby="ingestion-heading">
      <header className="panel__header">
        <div>
          <h2 id="ingestion-heading">Ingestion Settings</h2>
          <p>Select the symbols and timeframes you want to backfill.</p>
        </div>
        <button type="button" className="link" onClick={refresh} disabled={loadingSymbols}>
          Refresh symbols
        </button>
      </header>

      <div className="panel__body">
        <div className="grid two-columns">
          <div>
            <h3>Worker Controls</h3>
            <p className="hint">Status: {isRunning ? "Running" : "Stopped"}</p>
            <div className="symbol-actions__buttons">
              <button type="button" className="secondary" onClick={refreshStatus} disabled={controlsBusy}>
                Refresh status
              </button>
              <button type="button" className="secondary" onClick={handleToggleIngestion} disabled={controlsBusy}>
                {controlsBusy ? "Working..." : isRunning ? "Stop ingestion" : "Start ingestion"}
              </button>
              <button type="button" className="secondary" onClick={handleFlushDb} disabled={controlsBusy}>
                Flush DB
              </button>
            </div>
          </div>

          <div>
            <h3>Coverage Snapshot</h3>
            <div className="symbol-controls">
              <label>
                Timeframe
                <select value={coverageTimeframe} onChange={(event) => setCoverageTimeframe(event.target.value)}>
                  {Object.keys(TIMEFRAME_MINUTES).map((tf) => (
                    <option key={tf} value={tf}>
                      {tf}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Candles per symbol
                <input
                  type="number"
                  min={1}
                  value={coverageWindow}
                  onChange={(event) => setCoverageWindow(Number(event.target.value) || 0)}
                />
              </label>
              <label>
                Limit
                <input
                  type="number"
                  min={1}
                  value={coverageLimit}
                  onChange={(event) => setCoverageLimit(Number(event.target.value) || 0)}
                />
              </label>
            </div>
            <div className="symbol-actions__buttons" style={{ marginTop: "0.75rem" }}>
              <button type="button" className="secondary" onClick={refreshCoverage} disabled={coverageLoading}>
                {coverageLoading ? "Loading..." : "Refresh coverage"}
              </button>
            </div>
            {coverageError && <p className="error" style={{ marginTop: "0.5rem" }}>{coverageError}</p>}
          </div>
        </div>
      </div>

      <form className="panel__body" onSubmit={handleRunIngestion}>
        <div className="grid two-columns">
          <div>
            <h3>Symbols</h3>
            <div className="symbol-controls">
              <label>
                Segment
                <select
                  value={segment}
                  onChange={(event) => {
                    const nextSegment = event.target.value;
                    setSegment(nextSegment);
                    setSelectedSymbols([]);
                  }}
                >
                  {SEGMENTS.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Preset
                <select
                  value={selectedPreset ?? "all"}
                  onChange={(event) => {
                    const value = event.target.value === "all" ? null : Number(event.target.value);
                    applyPreset(value);
                  }}
                >
                  {PRESETS.map((item) => (
                    <option key={item.label} value={item.limit ?? "all"}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="symbol-actions">
              <input
                type="search"
                value={filter}
                onChange={(event) => setFilter(event.target.value)}
                placeholder="Search symbols"
              />
              <div className="symbol-actions__buttons">
                <button type="button" className="secondary" onClick={handleSelectAll}>
                  Select all
                </button>
                <button type="button" className="secondary" onClick={handleClear}>
                  Clear
                </button>
              </div>
            </div>
            <div className="symbol-list" role="listbox" aria-label="Available symbols">
              {loadingSymbols && <div className="hint">Loading symbols...</div>}
              {symbolError && <div className="error">{symbolError}</div>}
              {!loadingSymbols && !symbolError && filteredSymbols.length === 0 && (
                <div className="hint">No symbols found for the current filter.</div>
              )}
              {!loadingSymbols && !symbolError &&
                filteredSymbols.map((symbol) => {
                  const checked = selectedSymbols.includes(symbol);
                  return (
                    <label key={symbol} className={checked ? "symbol-item selected" : "symbol-item"}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleSymbol(symbol)}
                      />
                      {symbol}
                    </label>
                  );
                })}
            </div>
            <p className="hint">Selected: {selectedSymbols.length}</p>
          </div>

          <div>
            <h3>Timeframes</h3>
            <div className="task-list">
              {tasks.map((row, index) => {
                const info = computeCandles(row.timeframe, row.days);
                return (
                  <div key={`${row.timeframe}-${index}`} className="task-item">
                    <select
                      value={row.timeframe}
                      onChange={(event) => updateTask(index, { timeframe: event.target.value })}
                    >
                      {Object.keys(TIMEFRAME_MINUTES).map((tf) => (
                        <option key={tf} value={tf}>
                          {tf}
                        </option>
                      ))}
                    </select>
                    <input
                      type="number"
                      min={1}
                      value={row.days}
                      onChange={(event) => updateTask(index, { days: event.target.value })}
                    />
                    <span className="hint">
                      {info ? `${info.candles_per_symbol.toLocaleString()} candles` : "Invalid days"}
                    </span>
                    <button type="button" className="icon" onClick={() => removeTask(index)} disabled={tasks.length === 1}>
                      Remove
                    </button>
                  </div>
                );
              })}
            </div>
            <button type="button" className="secondary" onClick={addTask}>
              + Add timeframe
            </button>
          </div>
        </div>

        <footer className="panel__footer">
          <span className="hint">
            The ingestion worker will backfill approximately the specified number of candles for each symbol/timeframe.
          </span>
          <button type="submit" disabled={starting || !selectedSymbols.length}>
            {starting ? "Starting..." : "Start ingestion"}
          </button>
        </footer>
      </form>

      {message && <p className="message">{message}</p>}

      <div className="panel__body">
        <h3>Coverage Report</h3>
        {coverageLoading ? (
          <p className="hint">Loading coverage...</p>
        ) : coverageRows.length === 0 ? (
          <p className="hint">No coverage data available.</p>
        ) : (
          <div className="status-table">
            <div className="status-table__header">
              <span>Symbol</span>
              <span>Total required</span>
              <span>Received</span>
              <span>Latest timestamp</span>
            </div>
            <div className="status-table__body">
              {coverageRows.map((row) => (
                <div className="status-table__row" key={row.symbol}>
                  <span>{row.symbol}</span>
                  <span>{row.total_required.toLocaleString()}</span>
                  <span>{row.received.toLocaleString()}</span>
                  <span>{formatTimestamp(row.latest_ts)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
