import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import TradingChart from "./TradingChart";
import { fetchChartCandles, fetchWsMetrics } from "../lib/api";
import { useSymbols } from "../hooks/useSymbols";
const TIMEFRAME_SECONDS = {
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
];
const DEFAULT_BAR_WINDOW = {
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
const BAR_OPTIONS = [150, 300, 600, 900, 1200];
const BASE_BY_TARGET = {
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
function resolveBaseTimeframe(target) {
    return BASE_BY_TARGET[target] ?? target;
}
function formatNumber(value, digits = 4) {
    return Number.isFinite(value) ? value.toFixed(digits) : "-";
}
export default function ChartView() {
    const { symbols: availableSymbols, loading: loadingSymbols, error: symbolError } = useSymbols("all");
    const [symbol, setSymbol] = useState("");
    const [timeframe, setTimeframe] = useState("1h");
    const [barWindow, setBarWindow] = useState(DEFAULT_BAR_WINDOW["1h"]);
    const [candles, setCandles] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [autoRefresh, setAutoRefresh] = useState(true);
    const [lastUpdated, setLastUpdated] = useState(null);
    const [suspendAutoRefresh, setSuspendAutoRefresh] = useState(false);
    const [wsConnected, setWsConnected] = useState(false);
    const [wsMetrics, setWsMetrics] = useState(null);
    const [wsMetricsError, setWsMetricsError] = useState(null);
    const missingCacheRef = useRef({});
    const socketRef = useRef(null);
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
    const loadCandles = useCallback(async (options) => {
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
        }
        catch (err) {
            if (err instanceof Error && err.message.startsWith("HTTP 404")) {
                setError("No candles available for this timeframe yet. Please ingest lower timeframe data or choose a higher interval.");
                setCandles([]);
                setSuspendAutoRefresh(true);
                missingCacheRef.current[cacheKey] = Date.now() + 60_000;
            }
            else {
                setError(err instanceof Error ? err.message : "Unable to load chart data");
                setCandles([]);
            }
        }
        finally {
            setLoading(false);
        }
    }, [symbol, timeframe, barWindow]);
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
        let reconnectTimer;
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
                    if (eventSymbol === symbol.toUpperCase() &&
                        eventTimeframe === baseTimeframe.toLowerCase() &&
                        (data?.type === "candle" || data?.type === "snapshot")) {
                        loadCandles({ force: true });
                    }
                }
                catch {
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
            }
            catch (err) {
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
    const changeValue = lastCandle && firstCandle ? lastCandle.close - firstCandle.open : null;
    const changePercent = changeValue !== null && firstCandle
        ? (changeValue / firstCandle.open) * 100
        : null;
    const changeClass = changeValue == null ? "" : changeValue < 0 ? "text-danger" : "text-success";
    const changePercentClass = changePercent == null ? "" : changePercent < 0 ? "text-danger" : "text-success";
    return (_jsxs("section", { className: "panel", "aria-labelledby": "chart-heading", children: [_jsxs("header", { className: "panel__header", children: [_jsxs("div", { children: [_jsx("h2", { id: "chart-heading", children: "Chart View" }), _jsx("p", { children: "TradingView-style candlesticks sourced from ingestion (DB + realtime)." })] }), _jsxs("div", { className: "chart-refresh", children: [_jsxs("label", { className: "toggle", children: [_jsx("input", { type: "checkbox", checked: autoRefresh, onChange: (event) => setAutoRefresh(event.target.checked) }), _jsx("span", { children: "Auto refresh" })] }), lastUpdated && (_jsxs("span", { className: "muted", children: ["Updated ", new Date(lastUpdated).toLocaleTimeString()] }))] })] }), _jsxs("div", { className: "chart-toolbar", children: [_jsxs("label", { children: ["Symbol", _jsx("select", { value: symbol, onChange: (event) => setSymbol(event.target.value), disabled: loadingSymbols, children: availableSymbols.map((item) => (_jsx("option", { value: item, children: item }, item))) })] }), _jsxs("label", { children: ["Bars", _jsx("select", { value: barWindow, onChange: (event) => setBarWindow(Number(event.target.value) || barWindow), children: BAR_OPTIONS.map((item) => (_jsxs("option", { value: item, children: [item.toLocaleString(), " bars"] }, item))) })] })] }), _jsx("div", { className: "timeframe-tabs", role: "tablist", "aria-label": "Timeframes", children: TIMEFRAME_OPTIONS.map((item) => (_jsx("button", { type: "button", role: "tab", "aria-selected": timeframe === item, className: timeframe === item ? "active" : "", onClick: () => setTimeframe(item), children: item }, item))) }), symbolError && _jsx("p", { className: "error", children: symbolError }), error && _jsx("p", { className: "error", children: error }), loading && _jsx("p", { className: "hint", children: "Loading candles..." }), !loading && !error && (_jsxs("div", { className: "chart-board", children: [_jsx("div", { className: "chart-surface", children: _jsx(TradingChart, { candles: candles, timeframe: timeframe }) }), _jsxs("div", { className: "chart-stats", children: [_jsxs("div", { children: [_jsx("span", { className: "muted", children: "Symbol" }), _jsx("strong", { children: symbol || "-" })] }), _jsxs("div", { children: [_jsx("span", { className: "muted", children: "Timeframe" }), _jsx("strong", { children: timeframe })] }), _jsxs("div", { children: [_jsx("span", { className: "muted", children: "Last close" }), _jsx("strong", { children: lastCandle ? formatNumber(lastCandle.close, 6) : "-" })] }), _jsxs("div", { children: [_jsx("span", { className: "muted", children: "Volume" }), _jsx("strong", { children: lastCandle ? lastCandle.volume.toLocaleString(undefined, { maximumFractionDigits: 2 }) : "-" })] }), _jsxs("div", { children: [_jsx("span", { className: "muted", children: "Change" }), _jsx("strong", { className: changeClass || undefined, children: changeValue !== null ? formatNumber(changeValue, 6) : "-" })] }), _jsxs("div", { children: [_jsx("span", { className: "muted", children: "Change %" }), _jsx("strong", { className: changePercentClass || undefined, children: changePercent !== null ? `${changePercent.toFixed(2)}%` : "-" })] })] })] })), wsMetricsError && !wsMetrics && _jsx("p", { className: "error", children: wsMetricsError }), wsMetrics && (_jsxs("div", { className: "ws-metrics", children: [_jsxs("div", { className: "ws-metrics__header", children: [_jsx("h3", { children: "Stream Status" }), _jsxs("span", { className: "muted", children: ["Updated ", new Date(wsMetrics.timestamp).toLocaleTimeString()] })] }), _jsxs("div", { className: "ws-metrics__summary", children: [_jsx("span", { children: `${wsMetrics.total_pairs} pairs across ${wsMetrics.group_count} connections (max ${wsMetrics.max_pairs_per_connection} per connection)` }), _jsx("span", { children: `Connects: ${wsMetrics.total_connects} � Disconnects: ${wsMetrics.total_disconnects}` })] }), wsMetrics.groups.length === 0 ? (_jsx("p", { className: "hint", children: "No upstream websockets are active yet." })) : (_jsx("div", { className: "ws-metrics__groups", children: wsMetrics.groups.map((group) => {
                            const statusClass = group.status === "connected" ? "text-success" : group.status === "disconnected" ? "text-danger" : "muted";
                            return (_jsxs("div", { className: "ws-metrics__group", children: [_jsxs("div", { className: "ws-metrics__group-title", children: [_jsx("strong", { children: group.group_id }), _jsx("span", { className: "muted", children: `${group.pair_count}/${group.max_pairs} pairs` })] }), _jsxs("div", { className: "ws-metrics__group-body", children: [_jsxs("span", { children: ["Status: ", _jsx("span", { className: statusClass, children: group.status })] }), _jsx("span", { children: `Connects: ${group.connects}` }), _jsx("span", { children: `Disconnects: ${group.disconnects}` }), _jsx("span", { children: `Reconnections: ${group.reconnections}` }), group.last_disconnect && (_jsxs("span", { className: "muted", children: ["Last disconnect: ", new Date(group.last_disconnect).toLocaleTimeString()] }))] })] }, group.group_id));
                        }) }))] })), !loading && !error && recentRows.length > 0 && (_jsxs("div", { className: "chart-table", children: [_jsxs("div", { className: "chart-table__header", children: [_jsx("span", { children: "Time" }), _jsx("span", { children: "Open" }), _jsx("span", { children: "High" }), _jsx("span", { children: "Low" }), _jsx("span", { children: "Close" }), _jsx("span", { children: "Volume" })] }), _jsx("div", { className: "chart-table__body", children: recentRows.map((candle) => {
                            const date = new Date(candle.ts);
                            return (_jsxs("div", { className: "chart-table__row", children: [_jsx("span", { children: date.toLocaleString() }), _jsx("span", { children: formatNumber(candle.open, 6) }), _jsx("span", { children: formatNumber(candle.high, 6) }), _jsx("span", { children: formatNumber(candle.low, 6) }), _jsx("span", { children: formatNumber(candle.close, 6) }), _jsx("span", { children: candle.volume.toLocaleString(undefined, { maximumFractionDigits: 2 }) })] }, `${candle.ts}-${candle.open}`));
                        }) })] })), !loading && !error && recentRows.length === 0 && (_jsx("p", { className: "hint", children: "No candles found for the current selection." }))] }));
}
