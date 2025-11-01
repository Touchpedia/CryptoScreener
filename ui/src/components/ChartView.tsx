import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import TradingChart from "./TradingChart";
import { ChartCandle, fetchChartCandles, fetchWsMetrics, WsMetricsSummary } from "../lib/api";
import { useSymbols } from "../hooks/useSymbols";

const TIMEFRAME_SECONDS: Record<string, number> = {
  "1s": 1,
  "5s": 5,
  "10s": 10,
  "15s": 15,
  "30s": 30,
  "45s": 45,
  "1m": 60,
  "3m": 3 * 60,
  "5m": 5 * 60,
  "10m": 10 * 60,
  "15m": 15 * 60,
  "30m": 30 * 60,
  "45m": 45 * 60,
  "1h": 60 * 60,
  "2h": 2 * 60 * 60,
  "3h": 3 * 60 * 60,
  "4h": 4 * 60 * 60,
  "6h": 6 * 60 * 60,
  "8h": 8 * 60 * 60,
  "12h": 12 * 60 * 60,
  "1d": 24 * 60 * 60,
};

const TIMEFRAME_OPTIONS = [
  "1s",
  "5s",
  "10s",
  "15s",
  "30s",
  "45s",
  "1m",
  "3m",
  "5m",
  "10m",
  "15m",
  "30m",
  "45m",
  "1h",
  "2h",
  "3h",
  "4h",
  "6h",
  "8h",
  "12h",
  "1d",
] as const;

const DEFAULT_BAR_WINDOW: Record<string, number> = {
  "1s": 900,
  "5s": 900,
  "10s": 900,
  "15s": 900,
  "30s": 720,
  "45s": 600,
  "1m": 720,
  "3m": 480,
  "5m": 360,
  "10m": 300,
  "15m": 240,
  "30m": 180,
  "45m": 160,
  "1h": 150,
  "2h": 120,
  "3h": 100,
  "4h": 90,
  "6h": 72,
  "8h": 64,
  "12h": 56,
  "1d": 60,
};

const BAR_OPTIONS = [150, 300, 600, 900, 1200] as const;

const BASE_BY_TARGET: Record<string, string> = {
  "1s": "1s",
  "5s": "1s",
  "10s": "5s",
  "15s": "5s",
  "30s": "5s",
  "45s": "15s",
  "1m": "1m",
  "3m": "1m",
  "5m": "1m",
  "10m": "1m",
  "15m": "1m",
  "30m": "1m",
  "45m": "1m",
  "1h": "1m",
  "2h": "1m",
  "3h": "1m",
  "4h": "1m",
  "6h": "1m",
  "8h": "1m",
  "12h": "1m",
  "1d": "1m",
};

const REFRESH_INTERVAL_MS = 5000;

function resolveBaseTimeframe(target: string) {
  return BASE_BY_TARGET[target] ?? target;
}

function formatNumber(value: number, digits = 4) {
  return Number.isFinite(value) ? value.toFixed(digits) : "-";
}

export default function ChartView() {
  const { symbols: availableSymbols, loading: loadingSymbols, error: symbolError } = useSymbols("all");
  const [symbol, setSymbol] = useState<string>("");
  const [timeframe, setTimeframe] = useState<string>("1h");
  const [barWindow, setBarWindow] = useState<number>(DEFAULT_BAR_WINDOW["1h"]);
  const [candles, setCandles] = useState<ChartCandle[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const [suspendAutoRefresh, setSuspendAutoRefresh] = useState(false);
  const [wsConnected, setWsConnected] = useState(false);
  const [wsMetrics, setWsMetrics] = useState<WsMetricsSummary | null>(null);
  const [wsMetricsError, setWsMetricsError] = useState<string | null>(null);
  const missingCacheRef = useRef<Record<string, number>>({});
  const socketRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    if (!symbol && availableSymbols.length) {
      setSymbol(availableSymbols[0]);
    }
  }, [availableSymbols, symbol]);

  useEffect(() => {
    setBarWindow(DEFAULT_BAR_WINDOW[timeframe] ?? 300);
    setSuspendAutoRefresh(false);
  }, [timeframe]);

  useEffect(() => {
    setSuspendAutoRefresh(false);
  }, [symbol, barWindow]);

  const loadCandles = useCallback(
    async (options?: { force?: boolean }) => {
      if (!symbol || !timeframe) {
        return;
      }
      const seconds = TIMEFRAME_SECONDS[timeframe];
      if (!seconds) {
        setError(`Unsupported timeframe: ${timeframe}`);
        setCandles([]);
        return;
      }

      const cacheKey = `${symbol}::${timeframe}`;
      const suppressUntil = missingCacheRef.current[cacheKey];
      if (!options?.force && suppressUntil && suppressUntil > Date.now()) {
        setError("No candles available for this timeframe yet. Please ingest lower timeframe data or choose a higher interval.");
        setCandles([]);
        setSuspendAutoRefresh(true);
        return;
      }

      setLoading(true);
      setError(null);
      try {
        const end = Date.now();
        const start = end - seconds * barWindow * 1000;
        const base = resolveBaseTimeframe(timeframe);
        const response = await fetchChartCandles({
          symbol,
          target_timeframe: timeframe,
          base_timeframe: base,
          start_ts: start,
          end_ts: end,
          limit: barWindow,
        });
        setCandles(response.candles ?? []);
        setLastUpdated(Date.now());
        setSuspendAutoRefresh(false);
        delete missingCacheRef.current[cacheKey];
      } catch (err) {
        if (err instanceof Error && err.message.startsWith("HTTP 404")) {
          setError("No candles available for this timeframe yet. Please ingest lower timeframe data or choose a higher interval.");
          setCandles([]);
          setSuspendAutoRefresh(true);
          missingCacheRef.current[cacheKey] = Date.now() + 60_000;
        } else {
          setError(err instanceof Error ? err.message : "Unable to load chart data");
          setCandles([]);
        }
      } finally {
        setLoading(false);
      }
    },
    [symbol, timeframe, barWindow],
  );

  useEffect(() => {
    loadCandles();
  }, [loadCandles]);

  useEffect(() => {
    if (!autoRefresh || suspendAutoRefresh || wsConnected) {
      return;
    }
    const id = window.setInterval(() => {
      loadCandles();
    }, REFRESH_INTERVAL_MS);
    return () => window.clearInterval(id);
  }, [autoRefresh, suspendAutoRefresh, loadCandles]);

  useEffect(() => {
    if (!symbol || !timeframe || suspendAutoRefresh) {
      if (socketRef.current) {
        socketRef.current.close();
        socketRef.current = null;
      }
      setWsConnected(false);
      return;
    }

    let active = true;
    let reconnectTimer: number | undefined;
    const baseTimeframe = resolveBaseTimeframe(timeframe);

    const connect = () => {
      if (!active) {
        return;
      }
      const { protocol, host } = window.location;
      const wsProtocol = protocol === "https:" ? "wss" : "ws";
      const params = new URLSearchParams({
        symbol,
        timeframe: baseTimeframe,
      });
      const url = `${wsProtocol}://${host}/ws/chart?${params.toString()}`;
      const socket = new WebSocket(url);
      socketRef.current = socket;

      socket.addEventListener("open", () => {
        setWsConnected(true);
      });

      socket.addEventListener("message", (event) => {
        try {
          const data = JSON.parse(event.data);
          const eventSymbol = (data?.symbol ?? "").toUpperCase();
          const eventTimeframe = (data?.timeframe ?? "").toLowerCase();
          if (!eventSymbol || !eventTimeframe) {
            return;
          }
          if (
            eventSymbol === symbol.toUpperCase() &&
            eventTimeframe === baseTimeframe.toLowerCase() &&
            (data?.type === "candle" || data?.type === "snapshot")
          ) {
            loadCandles({ force: true });
          }
        } catch {
          /* ignore malformed payload */
        }
      });

      socket.addEventListener("close", () => {
        if (!active) {
          return;
        }
        setWsConnected(false);
        reconnectTimer = window.setTimeout(connect, 5000);
      });

      socket.addEventListener("error", () => {
        setWsConnected(false);
        socket.close();
      });
    };

    connect();

    return () => {
      active = false;
      if (reconnectTimer) {
        window.clearTimeout(reconnectTimer);
      }
      if (socketRef.current) {
        socketRef.current.close();
        socketRef.current = null;
      }
      setWsConnected(false);
    };
  }, [symbol, timeframe, suspendAutoRefresh, loadCandles]);

  useEffect(() => {
    let cancelled = false;

    const loadMetrics = async () => {
      try {
        const summary = await fetchWsMetrics();
        if (!cancelled) {
          setWsMetrics(summary);
          setWsMetricsError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setWsMetricsError(err instanceof Error ? err.message : "Unable to load stream metrics");
        }
      }
    };

    loadMetrics();
    const id = window.setInterval(loadMetrics, 15000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  const recentRows = useMemo(() => {
    return [...candles].slice(-200).reverse();
  }, [candles]);

  const lastCandle = candles[candles.length - 1] ?? null;
  const firstCandle = candles[0] ?? null;
  const changeValue =
    lastCandle && firstCandle ? lastCandle.close - firstCandle.open : null;
  const changePercent =
    changeValue !== null && firstCandle
      ? (changeValue / firstCandle.open) * 100
      : null;
  const changeClass =
    changeValue == null ? "" : changeValue < 0 ? "text-danger" : "text-success";
  const changePercentClass =
    changePercent == null ? "" : changePercent < 0 ? "text-danger" : "text-success";

  return (
    <section className="panel" aria-labelledby="chart-heading">
      <header className="panel__header">
        <div>
          <h2 id="chart-heading">Chart View</h2>
          <p>TradingView-style candlesticks sourced from ingestion (DB + realtime).</p>
        </div>
        <div className="chart-refresh">
          <label className="toggle">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(event) => setAutoRefresh(event.target.checked)}
            />
            <span>Auto refresh</span>
          </label>
          {lastUpdated && (
            <span className="muted">
              Updated {new Date(lastUpdated).toLocaleTimeString()}
            </span>
          )}
        </div>
      </header>

      <div className="chart-toolbar">
        <label>
          Symbol
          <select value={symbol} onChange={(event) => setSymbol(event.target.value)} disabled={loadingSymbols}>
            {availableSymbols.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>
        <label>
          Bars
          <select value={barWindow} onChange={(event) => setBarWindow(Number(event.target.value) || barWindow)}>
            {BAR_OPTIONS.map((item) => (
              <option key={item} value={item}>
                {item.toLocaleString()} bars
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className="timeframe-tabs" role="tablist" aria-label="Timeframes">
        {TIMEFRAME_OPTIONS.map((item) => (
          <button
            key={item}
            type="button"
            role="tab"
            aria-selected={timeframe === item}
            className={timeframe === item ? "active" : ""}
            onClick={() => setTimeframe(item)}
          >
            {item}
          </button>
        ))}
      </div>

      {symbolError && <p className="error">{symbolError}</p>}
      {error && <p className="error">{error}</p>}
      {loading && <p className="hint">Loading candles...</p>}

      {!loading && !error && (
        <div className="chart-board">
          <div className="chart-surface">
            <TradingChart candles={candles} timeframe={timeframe} />
          </div>
          <div className="chart-stats">
            <div>
              <span className="muted">Symbol</span>
              <strong>{symbol || "-"}</strong>
            </div>
            <div>
              <span className="muted">Timeframe</span>
              <strong>{timeframe}</strong>
            </div>
            <div>
              <span className="muted">Last close</span>
              <strong>{lastCandle ? formatNumber(lastCandle.close, 6) : "-"}</strong>
            </div>
            <div>
              <span className="muted">Volume</span>
              <strong>{lastCandle ? lastCandle.volume.toLocaleString(undefined, { maximumFractionDigits: 2 }) : "-"}</strong>
            </div>
            <div>
              <span className="muted">Change</span>
              <strong className={changeClass || undefined}>
                {changeValue !== null ? formatNumber(changeValue, 6) : "-"}
              </strong>
            </div>
            <div>
              <span className="muted">Change %</span>
              <strong className={changePercentClass || undefined}>
                {changePercent !== null ? `${changePercent.toFixed(2)}%` : "-"}
              </strong>
            </div>
          </div>
        </div>
      )}

      {wsMetricsError && !wsMetrics && <p className="error">{wsMetricsError}</p>}

      {wsMetrics && (
        <div className="ws-metrics">
          <div className="ws-metrics__header">
            <h3>Stream Status</h3>
            <span className="muted">Updated {new Date(wsMetrics.timestamp).toLocaleTimeString()}</span>
          </div>
          <div className="ws-metrics__summary">
            <span>{`${wsMetrics.total_pairs} pairs across ${wsMetrics.group_count} connections (max ${wsMetrics.max_pairs_per_connection} per connection)`}</span>
            <span>{`Connects: ${wsMetrics.total_connects} · Disconnects: ${wsMetrics.total_disconnects}`}</span>
          </div>
          {wsMetrics.groups.length === 0 ? (
            <p className="hint">No upstream websockets are active yet.</p>
          ) : (
            <div className="ws-metrics__groups">
              {wsMetrics.groups.map((group) => {
                const statusClass = group.status === "connected" ? "text-success" : group.status === "disconnected" ? "text-danger" : "muted";
                return (
                  <div key={group.group_id} className="ws-metrics__group">
                    <div className="ws-metrics__group-title">
                      <strong>{group.group_id}</strong>
                      <span className="muted">{`${group.pair_count}/${group.max_pairs} pairs`}</span>
                    </div>
                    <div className="ws-metrics__group-body">
                      <span>Status: <span className={statusClass}>{group.status}</span></span>
                      <span>{`Connects: ${group.connects}`}</span>
                      <span>{`Disconnects: ${group.disconnects}`}</span>
                      <span>{`Reconnections: ${group.reconnections}`}</span>
                      {group.last_disconnect && (
                        <span className="muted">Last disconnect: {new Date(group.last_disconnect).toLocaleTimeString()}</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {!loading && !error && recentRows.length > 0 && (
        <div className="chart-table">
          <div className="chart-table__header">
            <span>Time</span>
            <span>Open</span>
            <span>High</span>
            <span>Low</span>
            <span>Close</span>
            <span>Volume</span>
          </div>
          <div className="chart-table__body">
            {recentRows.map((candle) => {
              const date = new Date(candle.ts);
              return (
                <div key={`${candle.ts}-${candle.open}`} className="chart-table__row">
                  <span>{date.toLocaleString()}</span>
                  <span>{formatNumber(candle.open, 6)}</span>
                  <span>{formatNumber(candle.high, 6)}</span>
                  <span>{formatNumber(candle.low, 6)}</span>
                  <span>{formatNumber(candle.close, 6)}</span>
                  <span>{candle.volume.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {!loading && !error && recentRows.length === 0 && (
        <p className="hint">No candles found for the current selection.</p>
      )}
    </section>
  );
}


