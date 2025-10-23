import { ChangeEvent, useEffect, useMemo, useState } from "react";

type CoverageRow = {
  symbol: string;
  total_required: number;
  received: number;
  latest_ts: string | number | null;
};

const STATUS_POLL_ATTEMPTS = 20;
const STATUS_POLL_DELAY_MS = 500;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const parsePositiveInt = (value: string) => {
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? NaN : parsed;
};

const SYMBOL_PRESETS: { label: string; limit: number | null }[] = [
  { label: "All", limit: null },
  { label: "Top 10", limit: 10 },
  { label: "Top 20", limit: 20 },
  { label: "Top 50", limit: 50 },
  { label: "Top 100", limit: 100 },
  { label: "Top 200", limit: 200 },
];

const SYMBOL_SEGMENTS: { label: string; value: string }[] = [
  { label: "Alphabetical", value: "all" },
  { label: "Market Cap", value: "market_cap" },
  { label: "24h Volume", value: "volume" },
  { label: "24h Gainers", value: "gainers" },
  { label: "24h Losers", value: "losers" },
];

type FetchOptions = { force?: boolean };

export default function App() {
  const [interval, setIntervalTf] = useState<string>("1m");
  const [candlesPerSymbol, setCandlesPerSymbol] = useState<string>("6000");
  const [rows, setRows] = useState<CoverageRow[]>([]);

  const [availableSymbols, setAvailableSymbols] = useState<string[]>([]);
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>([]);
  const [loadingSymbols, setLoadingSymbols] = useState<boolean>(false);
  const [symbolSegment, setSymbolSegment] = useState<string>("all");
  const [symbolSearch, setSymbolSearch] = useState<string>("");
  const [symbolFilterLimit, setSymbolFilterLimit] = useState<number | null>(null);
  const [customLimit, setCustomLimit] = useState<string>("");

  const [loading, setLoading] = useState<boolean>(false);
  const [ingesting, setIngesting] = useState<boolean>(false);
  const [starting, setStarting] = useState<boolean>(false);
  const [suspendRefresh, setSuspendRefresh] = useState<boolean>(false);
  const [msg, setMsg] = useState<string>("");

  const fallbackCandles = parsePositiveInt(candlesPerSymbol);
  const fallbackCandlesValue = Number.isNaN(fallbackCandles) ? undefined : fallbackCandles;

  const filteredSymbols = useMemo(() => {
    let list = [...availableSymbols];
    if (symbolSearch.trim()) {
      const query = symbolSearch.trim().toLowerCase();
      list = list.filter((sym) => sym.toLowerCase().includes(query));
    }
    if (symbolFilterLimit && symbolFilterLimit > 0) {
      list = list.slice(0, symbolFilterLimit);
    }
    return list;
  }, [availableSymbols, symbolSearch, symbolFilterLimit]);

  const selectOptions = useMemo(() => {
    if (!availableSymbols.length) return [];
    const optionSet = new Set<string>(filteredSymbols);
    selectedSymbols.forEach((sym) => {
      if (availableSymbols.includes(sym)) {
        optionSet.add(sym);
      }
    });
    return availableSymbols.filter((sym) => optionSet.has(sym));
  }, [availableSymbols, filteredSymbols, selectedSymbols]);

  const currentSegmentLabel = useMemo(() => {
    return SYMBOL_SEGMENTS.find((option) => option.value === symbolSegment)?.label ?? "Alphabetical";
  }, [symbolSegment]);

  const pollIngestion = async (expectedRunning: boolean) => {
    for (let attempt = 0; attempt < STATUS_POLL_ATTEMPTS; attempt += 1) {
      await sleep(STATUS_POLL_DELAY_MS);
      try {
        const response = await fetch("/api/ingestion/status");
        const json = (await response.json().catch(() => ({}))) as { running?: boolean };
        if (typeof json?.running === "boolean") {
          const running = Boolean(json.running);
          setIngesting(running);
          if (running === expectedRunning) {
            return true;
          }
        }
      } catch {
        // ignore and keep polling
      }
    }
    return false;
  };

  async function loadSymbols(segmentOverride?: string) {
    const requested = (segmentOverride ?? symbolSegment ?? "all").toLowerCase();
    const targetSegment = SYMBOL_SEGMENTS.some((opt) => opt.value === requested) ? requested : "all";

    try {
      setLoadingSymbols(true);
      const response = await fetch(`/api/ingestion/symbols?segment=${encodeURIComponent(targetSegment)}`);
      const json = await response.json().catch(() => null);
      const list = Array.isArray(json?.symbols)
        ? (json.symbols as string[]).filter((sym) => typeof sym === "string" && sym.endsWith("/USDT"))
        : [];
      const responseSegment = typeof json?.segment === "string" ? json.segment.toLowerCase() : targetSegment;
      if (responseSegment !== symbolSegment) {
        setSymbolSegment(responseSegment);
      }
      setAvailableSymbols(list);
      setSelectedSymbols((prev) => {
        const filtered = prev.filter((sym) => list.includes(sym));
        if (filtered.length > 0) {
          if (filtered.length === prev.length && filtered.every((sym, idx) => sym === prev[idx])) {
            return prev;
          }
          return filtered;
        }
        return list.length ? [...list] : [];
      });
    } catch (error) {
      console.error(error);
    } finally {
      setLoadingSymbols(false);
    }
  }

  async function checkStatus(options?: FetchOptions) {
    const force = options?.force ?? false;
    if (!force && suspendRefresh) return;
    try {
      const response = await fetch("/api/ingestion/status");
      const json = (await response.json().catch(() => ({}))) as { running?: boolean };
      if (typeof json?.running === "boolean") {
        setIngesting(Boolean(json.running));
      }
    } catch {
      // network error: keep previous state
    }
  }

  async function loadCoverage(options?: FetchOptions) {
    const force = options?.force ?? false;
    if (!force && suspendRefresh) return;

    const activeSymbols = selectedSymbols.filter((sym) => availableSymbols.includes(sym));
    const cps = parsePositiveInt(candlesPerSymbol);

    if (!Number.isInteger(cps) || cps <= 0 || activeSymbols.length === 0) {
      setRows([]);
      return;
    }

    try {
      setLoading(true);
      const params = new URLSearchParams({
        timeframe: interval,
        window: String(cps),
        limit: String(activeSymbols.length),
        t: String(Date.now()),
      });
      activeSymbols.forEach((sym) => params.append("symbols", sym));

      const response = await fetch(`/api/report/coverage?${params.toString()}`);
      const json = await response.json().catch(() => null);
      const data: CoverageRow[] = Array.isArray(json) ? json : json?.rows ?? [];
      const normalized = data.map((row) => ({
        ...row,
        total_required: cps,
      }));
      setRows(normalized);
    } catch {
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  const handleSymbolSelection = (event: ChangeEvent<HTMLSelectElement>) => {
    const values = Array.from(event.target.selectedOptions).map((option) => option.value);
    setSelectedSymbols(values);
  };

  const handleSelectAllSymbols = () => {
    if (!availableSymbols.length) return;
    setSelectedSymbols([...availableSymbols]);
  };

  const handleClearSelection = () => setSelectedSymbols([]);

  const handleSelectFilteredSymbols = () => {
    if (!filteredSymbols.length) {
      setSelectedSymbols([]);
      return;
    }
    setSelectedSymbols([...filteredSymbols]);
  };

  const handlePresetChange = (limit: number | null) => {
    setSymbolFilterLimit(limit);
    setCustomLimit(limit && limit > 0 ? String(limit) : "");
  };

  const handleSegmentChange = (segment: string) => {
    const cleaned = (segment || "all").toLowerCase();
    if (cleaned === symbolSegment) return;
    setSymbolSegment(cleaned);
    void loadSymbols(cleaned);
  };

  const applyCustomLimit = () => {
    const value = customLimit.trim();
    if (!value) {
      setSymbolFilterLimit(null);
      return;
    }
    const parsed = Number.parseInt(value, 10);
    if (Number.isInteger(parsed) && parsed > 0) {
      setSymbolFilterLimit(parsed);
    } else {
      setSymbolFilterLimit(null);
      setCustomLimit("");
    }
  };

  async function startIngestion() {
    const cps = parsePositiveInt(candlesPerSymbol);
    const activeSymbols = selectedSymbols.filter((sym) => availableSymbols.includes(sym));

    if (!Number.isInteger(cps) || cps <= 0) {
      alert("Please enter a valid positive number for Candles per Symbol.");
      return;
    }

    if (!activeSymbols.length) {
      alert("Please select at least one symbol.");
      return;
    }

    setStarting(true);
    setMsg("");

    try {
      const body: Record<string, unknown> = {
        interval,
        candles_per_symbol: cps,
        symbols: activeSymbols,
      };
      const response = await fetch("/api/ingestion/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        throw new Error(`Failed to start ingestion (${response.status})`);
      }

      await response.json().catch(() => ({}));

      setIngesting(true);

      const started = await pollIngestion(true);
      setMsg(started ? "Ingestion started" : "Ingestion enqueued; waiting for status...");
    } catch (error) {
      console.error(error);
      setIngesting(false);
      setMsg("Start failed");
    } finally {
      setStarting(false);
      setTimeout(() => {
        checkStatus({ force: true });
        loadCoverage({ force: true });
      }, 800);
    }
  }

  async function stopIngestion(opts: { silent?: boolean } = {}) {
    const { silent = false } = opts;
    if (!silent) {
      setStarting(true);
      setMsg("");
    }

    try {
      const response = await fetch("/api/ingestion/stop", { method: "POST" });
      if (!response.ok) {
        throw new Error(`Failed to stop ingestion (${response.status})`);
      }

      setIngesting(false);

      const stopped = await pollIngestion(false);
      if (!silent) {
        setMsg(stopped ? "Ingestion stopped" : "Stop requested; waiting for status...");
      }
    } catch (error) {
      console.error(error);
      if (!silent) {
        setMsg("Stop failed");
      }
      throw error;
    } finally {
      if (silent) {
        setIngesting(false);
      }
      if (!silent) {
        setStarting(false);
        setTimeout(() => {
          checkStatus({ force: true });
          loadCoverage({ force: true });
        }, 800);
      }
    }
  }

  async function flushDB() {
    setSuspendRefresh(true);
    setStarting(true);
    setMsg("");

    try {
      if (ingesting) {
        const confirmStop = confirm("Ingestion is running. I will stop it, then flush the DB. Continue?");
        if (!confirmStop) {
          setSuspendRefresh(false);
          setStarting(false);
          return;
        }

        setMsg("Stopping ingestion...");
        setIngesting(false);
        setRows([]);
        await stopIngestion({ silent: true });
      } else {
        setRows([]);
        setIngesting(false);
      }

      setMsg("Flushing database...");
      const response = await fetch("/api/db/flush", { method: "POST" });
      const json = (await response.json().catch(() => ({}))) as { message?: string };

      if (!response.ok) {
        throw new Error(json?.message || "Flush failed");
      }

      const message = json?.message || "DB flushed";
      setMsg(message);
    } catch (error) {
      console.error(error);
      setMsg("Flush failed");
    } finally {
      setStarting(false);
      setTimeout(() => {
        setSuspendRefresh(false);
        checkStatus({ force: true });
        loadCoverage({ force: true });
      }, 800);
    }
  }

  useEffect(() => {
    loadSymbols();
    checkStatus({ force: true });
  }, []);

  useEffect(() => {
    if (suspendRefresh) return;
    loadCoverage({ force: true });
  }, [interval, candlesPerSymbol, suspendRefresh, selectedSymbols, availableSymbols]);

  useEffect(() => {
    if (!ingesting || suspendRefresh) return;
    const id = setInterval(() => {
      checkStatus();
      loadCoverage();
    }, 2000);
    return () => clearInterval(id);
  }, [ingesting, suspendRefresh, interval, candlesPerSymbol, selectedSymbols]);

  const selectedCount = selectedSymbols.length;
  const fetchedCount = rows.length;

  return (
    <div style={{ padding: 20, fontFamily: "Inter, sans-serif" }}>
      <h2 style={{ marginBottom: 10 }}>CryptoScreener</h2>

      {msg && (
        <div style={{ marginBottom: 10, padding: 8, background: "#f3f4f6", borderRadius: 8 }}>
          {msg}
        </div>
      )}

      <div
        style={{
          border: "1px solid #e5e7eb",
          borderRadius: 12,
          padding: 20,
          marginBottom: 18,
          background: "#ffffff",
          boxShadow: "0 2px 6px rgba(15, 23, 42, 0.05)",
        }}
      >
        <h3 style={{ marginTop: 0, marginBottom: 16 }}>Ingestion Settings</h3>

        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }}>
              <div>
                <label style={{ display: "block", fontWeight: 600, marginBottom: 4 }}>Symbols</label>
                <div style={{ fontSize: 12, color: "#6b7280" }}>
                  {availableSymbols.length
                    ? `${selectedCount.toLocaleString()} of ${availableSymbols.length.toLocaleString()} selected - Sorted by ${currentSegmentLabel}`
                    : "Loading symbol list..."}
                </div>
              </div>

              <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }}>
                <input
                  type="text"
                  placeholder="Search (e.g. BTC)"
                  value={symbolSearch}
                  onChange={(e) => setSymbolSearch(e.target.value)}
                  style={{
                    border: "1px solid #d1d5db",
                    borderRadius: 6,
                    padding: "6px 10px",
                    minWidth: 160,
                  }}
                />
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                  {SYMBOL_SEGMENTS.map(({ label, value }) => {
                    const isActive = symbolSegment === value;
                    return (
                      <button
                        key={value}
                        type="button"
                        onClick={() => handleSegmentChange(value)}
                        style={{
                          border: "1px solid " + (isActive ? "#16a34a" : "#d1d5db"),
                          background: isActive ? "#16a34a" : "#f0fdf4",
                          color: isActive ? "#fff" : "#065f46",
                          borderRadius: 999,
                          padding: "6px 12px",
                          fontSize: 12,
                          cursor: "pointer",
                        }}
                      >
                        {label}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>

            <div style={{ display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center" }}>
              {SYMBOL_PRESETS.map(({ label, limit }) => {
                const isActive = symbolFilterLimit === limit || (symbolFilterLimit === null && limit === null);
                return (
                  <button
                    key={label}
                    type="button"
                    onClick={() => handlePresetChange(limit)}
                    style={{
                      border: "1px solid " + (isActive ? "#2563eb" : "#d1d5db"),
                      background: isActive ? "#2563eb" : "#f8fafc",
                      color: isActive ? "#fff" : "#1f2937",
                      borderRadius: 999,
                      padding: "6px 12px",
                      fontSize: 12,
                      cursor: "pointer",
                    }}
                  >
                    {label}
                  </button>
                );
              })}
              <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                <input
                  type="number"
                  min="1"
                  placeholder="Custom"
                  value={customLimit}
                  onChange={(e) => setCustomLimit(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      applyCustomLimit();
                    }
                  }}
                  style={{
                    width: 90,
                    border: "1px solid #d1d5db",
                    borderRadius: 6,
                    padding: "6px 10px",
                  }}
                />
                <button
                  type="button"
                  onClick={applyCustomLimit}
                  style={{
                    border: "1px solid #d1d5db",
                    borderRadius: 6,
                    padding: "6px 12px",
                    background: "#eef2ff",
                    cursor: "pointer",
                  }}
                >
                  Apply
                </button>
              </div>
            </div>
          </div>

          <div style={{ display: "flex", gap: 12, alignItems: "stretch", flexWrap: "wrap" }}>
            <select
              multiple
              value={selectedSymbols}
              onChange={handleSymbolSelection}
              size={Math.min(14, Math.max(6, selectOptions.length || 6))}
              style={{
                flex: "1 1 440px",
                border: "1px solid #d1d5db",
                borderRadius: 8,
                padding: 8,
                minHeight: 190,
                background: "#f9fafb",
                fontFamily: "mono, monospace",
              }}
            >
              {selectOptions.map((sym, idx) => {
                const globalIndex = availableSymbols.indexOf(sym);
                const displayIndex = globalIndex >= 0 ? globalIndex + 1 : idx + 1;
                return (
                  <option key={sym} value={sym}>
                    {`${displayIndex}. ${sym}`}
                  </option>
                );
              })}
            </select>

            <div style={{ display: "flex", flexDirection: "column", gap: 8, minWidth: 170 }}>
              <button
                type="button"
                onClick={handleSelectFilteredSymbols}
                disabled={!filteredSymbols.length}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: filteredSymbols.length ? "#2563eb" : "#e5e7eb",
                  color: "#fff",
                  fontWeight: 600,
                  cursor: filteredSymbols.length ? "pointer" : "not-allowed",
                }}
              >
                Select Filtered
              </button>
              <button
                type="button"
                onClick={handleSelectAllSymbols}
                disabled={!availableSymbols.length}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: "#f8fafc",
                  cursor: availableSymbols.length ? "pointer" : "not-allowed",
                }}
              >
                Select All
              </button>
              <button
                type="button"
                onClick={handleClearSelection}
                disabled={!selectedSymbols.length}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: "#fdf2f8",
                  color: "#db2777",
                  cursor: selectedSymbols.length ? "pointer" : "not-allowed",
                }}
              >
                Clear Selection
              </button>
              <button
                type="button"
                onClick={() => loadSymbols()}
                disabled={loadingSymbols}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: "#f8fafc",
                  cursor: loadingSymbols ? "not-allowed" : "pointer",
                }}
              >
                {loadingSymbols ? "Refreshing..." : "Reload List"}
              </button>
              {!loadingSymbols && !availableSymbols.length && (
                <span style={{ fontSize: 12, color: "#ef4444" }}>No symbols available</span>
              )}
            </div>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
              gap: 12,
              alignItems: "end",
            }}
          >
            <div>
              <label>Interval</label>
              <select
                value={interval}
                onChange={(e) => setIntervalTf(e.target.value)}
                style={{
                  width: "100%",
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 10px",
                }}
              >
                <option value="1m">1m</option>
                <option value="5m">5m</option>
                <option value="1h">1h</option>
              </select>
            </div>
            <div>
              <label>Candles per Symbol</label>
              <input
                type="number"
                inputMode="numeric"
                min="1"
                step="1"
                value={candlesPerSymbol}
                onChange={(e) => setCandlesPerSymbol(e.target.value)}
                style={{
                  width: "100%",
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 10px",
                }}
              />
            </div>
            <button
              onClick={async () => {
                if (ingesting) {
                  try {
                    await stopIngestion();
                  } catch {
                    // stopIngestion handles messaging on failure
                  }
                } else {
                  await startIngestion();
                }
              }}
              style={{
                background: starting ? "#facc15" : ingesting ? "#ef4444" : "#22c55e",
                color: "#fff",
                padding: "10px 14px",
                borderRadius: 8,
                border: "none",
                fontWeight: 600,
                cursor: "pointer",
                transition: "background 0.2s ease",
              }}
            >
              {starting ? "Working..." : ingesting ? "Stop Ingestion" : "Start Ingestion"}
            </button>
          </div>
        </div>
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 10 }}>
        <button
          onClick={() => loadCoverage({ force: true })}
          disabled={loading}
          style={{ border: "1px solid #ccc", borderRadius: 8, padding: "8px 14px" }}
        >
          {loading ? "Loading..." : "Refresh"}
        </button>
        <button
          onClick={flushDB}
          style={{ background: "#0ea5e9", color: "white", padding: "8px 14px", borderRadius: 8, border: "none" }}
        >
          Flush DB
        </button>
      </div>

      <div style={{ marginBottom: 10, fontSize: 12, color: "#4b5563" }}>
        Showing {fetchedCount.toLocaleString()} symbols (selected {selectedCount.toLocaleString()}).
      </div>

      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #e5e7eb" }}>
            <th align="left" style={{ padding: 6 }}>
              Symbol
            </th>
            <th align="right" style={{ padding: 6 }}>
              Total Required
            </th>
            <th align="right" style={{ padding: 6 }}>
              Received
            </th>
            <th align="left" style={{ padding: 6 }}>
              Latest TS
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.symbol} style={{ borderBottom: "1px solid #f3f4f6" }}>
              <td style={{ padding: 6 }}>{row.symbol}</td>
              <td align="right" style={{ padding: 6 }}>
                {row.total_required ?? fallbackCandlesValue ?? "-"}
              </td>
              <td align="right" style={{ padding: 6 }}>
                {row.received ?? 0}
              </td>
              <td style={{ padding: 6 }}>
                {row.latest_ts
                  ? new Date(row.latest_ts).toLocaleString([], {
                      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                    })
                  : "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
