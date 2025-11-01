import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchCoverage, fetchIngestionStatus, flushDatabaseAdmin, runIngestion, startIngestion, stopIngestion, } from "../lib/api";
import { useSymbols } from "../hooks/useSymbols";
const DEFAULT_TASKS = [
    { timeframe: "1m", days: "1" },
    { timeframe: "3m", days: "1" },
    { timeframe: "5m", days: "1" },
];
const TIMEFRAME_MINUTES = {
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
];
const PRESETS = [
    { label: "Top 10", limit: 10 },
    { label: "Top 20", limit: 20 },
    { label: "Top 50", limit: 50 },
    { label: "Top 100", limit: 100 },
    { label: "All", limit: null },
];
function computeCandles(timeframe, daysValue) {
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
function formatTimestamp(value) {
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
    const [segment, setSegment] = useState("all");
    const { symbols: availableSymbols, loading: loadingSymbols, error: symbolError, refresh } = useSymbols(segment);
    const [filter, setFilter] = useState("");
    const [selectedSymbols, setSelectedSymbols] = useState([]);
    const [tasks, setTasks] = useState(DEFAULT_TASKS);
    const [selectedPreset, setSelectedPreset] = useState(10);
    const [message, setMessage] = useState("");
    const [starting, setStarting] = useState(false);
    const [isRunning, setIsRunning] = useState(false);
    const [controlsBusy, setControlsBusy] = useState(false);
    const [coverageRows, setCoverageRows] = useState([]);
    const [coverageTimeframe, setCoverageTimeframe] = useState("1m");
    const [coverageWindow, setCoverageWindow] = useState(6000);
    const [coverageLimit, setCoverageLimit] = useState(10);
    const [coverageLoading, setCoverageLoading] = useState(false);
    const [coverageError, setCoverageError] = useState(null);
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
        }
        catch (error) {
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
        }
        catch (error) {
            setCoverageRows([]);
            setCoverageError(error instanceof Error ? error.message : "Unable to load coverage");
        }
        finally {
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
        }
        else {
            setSelectedSymbols(availableSymbols.slice(0, selectedPreset));
        }
    }, [segment, selectedPreset, availableSymbols]);
    const toggleSymbol = (symbol) => {
        setSelectedSymbols((prev) => prev.includes(symbol) ? prev.filter((item) => item !== symbol) : [...prev, symbol]);
    };
    const applyPreset = (limit) => {
        setSelectedPreset(limit);
        if (limit === null) {
            setSelectedSymbols(availableSymbols);
        }
        else {
            setSelectedSymbols(availableSymbols.slice(0, limit));
        }
    };
    const addTask = () => {
        setTasks((prev) => [...prev, { timeframe: "15m", days: "1" }]);
    };
    const updateTask = (index, patch) => {
        setTasks((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
    };
    const removeTask = (index) => {
        setTasks((prev) => prev.filter((_, i) => i !== index));
    };
    const handleRunIngestion = async (event) => {
        event.preventDefault();
        setMessage("");
        const payloadTasks = tasks
            .map((row) => computeCandles(row.timeframe, row.days))
            .filter((row) => Boolean(row));
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
        }
        catch (error) {
            setMessage(error instanceof Error ? error.message : "Unable to start ingestion run");
        }
        finally {
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
            }
            else {
                await startIngestion();
                setMessage("Ingestion started");
            }
            await refreshStatus();
        }
        catch (error) {
            setMessage(error instanceof Error ? error.message : "Command failed");
        }
        finally {
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
        }
        catch (error) {
            setMessage(error instanceof Error ? error.message : "Flush failed");
        }
        finally {
            setControlsBusy(false);
        }
    };
    const handleSelectAll = () => setSelectedSymbols(availableSymbols);
    const handleClear = () => setSelectedSymbols([]);
    return (_jsxs("section", { className: "panel", "aria-labelledby": "ingestion-heading", children: [_jsxs("header", { className: "panel__header", children: [_jsxs("div", { children: [_jsx("h2", { id: "ingestion-heading", children: "Ingestion Settings" }), _jsx("p", { children: "Select the symbols and timeframes you want to backfill." })] }), _jsx("button", { type: "button", className: "link", onClick: refresh, disabled: loadingSymbols, children: "Refresh symbols" })] }), _jsx("div", { className: "panel__body", children: _jsxs("div", { className: "grid two-columns", children: [_jsxs("div", { children: [_jsx("h3", { children: "Worker Controls" }), _jsxs("p", { className: "hint", children: ["Status: ", isRunning ? "Running" : "Stopped"] }), _jsxs("div", { className: "symbol-actions__buttons", children: [_jsx("button", { type: "button", className: "secondary", onClick: refreshStatus, disabled: controlsBusy, children: "Refresh status" }), _jsx("button", { type: "button", className: "secondary", onClick: handleToggleIngestion, disabled: controlsBusy, children: controlsBusy ? "Working..." : isRunning ? "Stop ingestion" : "Start ingestion" }), _jsx("button", { type: "button", className: "secondary", onClick: handleFlushDb, disabled: controlsBusy, children: "Flush DB" })] })] }), _jsxs("div", { children: [_jsx("h3", { children: "Coverage Snapshot" }), _jsxs("div", { className: "symbol-controls", children: [_jsxs("label", { children: ["Timeframe", _jsx("select", { value: coverageTimeframe, onChange: (event) => setCoverageTimeframe(event.target.value), children: Object.keys(TIMEFRAME_MINUTES).map((tf) => (_jsx("option", { value: tf, children: tf }, tf))) })] }), _jsxs("label", { children: ["Candles per symbol", _jsx("input", { type: "number", min: 1, value: coverageWindow, onChange: (event) => setCoverageWindow(Number(event.target.value) || 0) })] }), _jsxs("label", { children: ["Limit", _jsx("input", { type: "number", min: 1, value: coverageLimit, onChange: (event) => setCoverageLimit(Number(event.target.value) || 0) })] })] }), _jsx("div", { className: "symbol-actions__buttons", style: { marginTop: "0.75rem" }, children: _jsx("button", { type: "button", className: "secondary", onClick: refreshCoverage, disabled: coverageLoading, children: coverageLoading ? "Loading..." : "Refresh coverage" }) }), coverageError && _jsx("p", { className: "error", style: { marginTop: "0.5rem" }, children: coverageError })] })] }) }), _jsxs("form", { className: "panel__body", onSubmit: handleRunIngestion, children: [_jsxs("div", { className: "grid two-columns", children: [_jsxs("div", { children: [_jsx("h3", { children: "Symbols" }), _jsxs("div", { className: "symbol-controls", children: [_jsxs("label", { children: ["Segment", _jsx("select", { value: segment, onChange: (event) => {
                                                            const nextSegment = event.target.value;
                                                            setSegment(nextSegment);
                                                            setSelectedSymbols([]);
                                                        }, children: SEGMENTS.map((item) => (_jsx("option", { value: item.value, children: item.label }, item.value))) })] }), _jsxs("label", { children: ["Preset", _jsx("select", { value: selectedPreset ?? "all", onChange: (event) => {
                                                            const value = event.target.value === "all" ? null : Number(event.target.value);
                                                            applyPreset(value);
                                                        }, children: PRESETS.map((item) => (_jsx("option", { value: item.limit ?? "all", children: item.label }, item.label))) })] })] }), _jsxs("div", { className: "symbol-actions", children: [_jsx("input", { type: "search", value: filter, onChange: (event) => setFilter(event.target.value), placeholder: "Search symbols" }), _jsxs("div", { className: "symbol-actions__buttons", children: [_jsx("button", { type: "button", className: "secondary", onClick: handleSelectAll, children: "Select all" }), _jsx("button", { type: "button", className: "secondary", onClick: handleClear, children: "Clear" })] })] }), _jsxs("div", { className: "symbol-list", role: "listbox", "aria-label": "Available symbols", children: [loadingSymbols && _jsx("div", { className: "hint", children: "Loading symbols..." }), symbolError && _jsx("div", { className: "error", children: symbolError }), !loadingSymbols && !symbolError && filteredSymbols.length === 0 && (_jsx("div", { className: "hint", children: "No symbols found for the current filter." })), !loadingSymbols && !symbolError &&
                                                filteredSymbols.map((symbol) => {
                                                    const checked = selectedSymbols.includes(symbol);
                                                    return (_jsxs("label", { className: checked ? "symbol-item selected" : "symbol-item", children: [_jsx("input", { type: "checkbox", checked: checked, onChange: () => toggleSymbol(symbol) }), symbol] }, symbol));
                                                })] }), _jsxs("p", { className: "hint", children: ["Selected: ", selectedSymbols.length] })] }), _jsxs("div", { children: [_jsx("h3", { children: "Timeframes" }), _jsx("div", { className: "task-list", children: tasks.map((row, index) => {
                                            const info = computeCandles(row.timeframe, row.days);
                                            return (_jsxs("div", { className: "task-item", children: [_jsx("select", { value: row.timeframe, onChange: (event) => updateTask(index, { timeframe: event.target.value }), children: Object.keys(TIMEFRAME_MINUTES).map((tf) => (_jsx("option", { value: tf, children: tf }, tf))) }), _jsx("input", { type: "number", min: 1, value: row.days, onChange: (event) => updateTask(index, { days: event.target.value }) }), _jsx("span", { className: "hint", children: info ? `${info.candles_per_symbol.toLocaleString()} candles` : "Invalid days" }), _jsx("button", { type: "button", className: "icon", onClick: () => removeTask(index), disabled: tasks.length === 1, children: "Remove" })] }, `${row.timeframe}-${index}`));
                                        }) }), _jsx("button", { type: "button", className: "secondary", onClick: addTask, children: "+ Add timeframe" })] })] }), _jsxs("footer", { className: "panel__footer", children: [_jsx("span", { className: "hint", children: "The ingestion worker will backfill approximately the specified number of candles for each symbol/timeframe." }), _jsx("button", { type: "submit", disabled: starting || !selectedSymbols.length, children: starting ? "Starting..." : "Start ingestion" })] })] }), message && _jsx("p", { className: "message", children: message }), _jsxs("div", { className: "panel__body", children: [_jsx("h3", { children: "Coverage Report" }), coverageLoading ? (_jsx("p", { className: "hint", children: "Loading coverage..." })) : coverageRows.length === 0 ? (_jsx("p", { className: "hint", children: "No coverage data available." })) : (_jsxs("div", { className: "status-table", children: [_jsxs("div", { className: "status-table__header", children: [_jsx("span", { children: "Symbol" }), _jsx("span", { children: "Total required" }), _jsx("span", { children: "Received" }), _jsx("span", { children: "Latest timestamp" })] }), _jsx("div", { className: "status-table__body", children: coverageRows.map((row) => (_jsxs("div", { className: "status-table__row", children: [_jsx("span", { children: row.symbol }), _jsx("span", { children: row.total_required.toLocaleString() }), _jsx("span", { children: row.received.toLocaleString() }), _jsx("span", { children: formatTimestamp(row.latest_ts) })] }, row.symbol))) })] }))] })] }));
}
