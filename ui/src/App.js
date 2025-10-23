import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useMemo, useState } from "react";
const STATUS_POLL_ATTEMPTS = 20;
const STATUS_POLL_DELAY_MS = 500;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const parsePositiveInt = (value) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? NaN : parsed;
};
const SYMBOL_PRESETS = [
    { label: "All", limit: null },
    { label: "Top 10", limit: 10 },
    { label: "Top 20", limit: 20 },
    { label: "Top 50", limit: 50 },
    { label: "Top 100", limit: 100 },
    { label: "Top 200", limit: 200 },
];
const SYMBOL_SEGMENTS = [
    { label: "Alphabetical", value: "all" },
    { label: "Market Cap", value: "market_cap" },
    { label: "24h Volume", value: "volume" },
    { label: "24h Gainers", value: "gainers" },
    { label: "24h Losers", value: "losers" },
];
const TIMEFRAME_OPTIONS = ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"];
const DEFAULT_TASKS = [
    { id: "task-1m", timeframe: "1m", candles: "6000", enabled: true, useCustomRange: false, start: "", end: "" },
    { id: "task-5m", timeframe: "5m", candles: "2880", enabled: true, useCustomRange: false, start: "", end: "" },
    { id: "task-1h", timeframe: "1h", candles: "720", enabled: false, useCustomRange: false, start: "", end: "" },
];
export default function App() {
    const [interval, setIntervalTf] = useState("1m");
    const [candlesPerSymbol, setCandlesPerSymbol] = useState("6000");
    const [rows, setRows] = useState([]);
    const [availableSymbols, setAvailableSymbols] = useState([]);
    const [selectedSymbols, setSelectedSymbols] = useState([]);
    const [loadingSymbols, setLoadingSymbols] = useState(false);
    const [symbolSegment, setSymbolSegment] = useState("all");
    const [symbolSearch, setSymbolSearch] = useState("");
    const [symbolFilterLimit, setSymbolFilterLimit] = useState(null);
    const [customLimit, setCustomLimit] = useState("");
    const [taskConfigs, setTaskConfigs] = useState(DEFAULT_TASKS);
    const [loading, setLoading] = useState(false);
    const [ingesting, setIngesting] = useState(false);
    const [starting, setStarting] = useState(false);
    const [suspendRefresh, setSuspendRefresh] = useState(false);
    const [msg, setMsg] = useState("");
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
        if (!availableSymbols.length)
            return [];
        const optionSet = new Set(filteredSymbols);
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
    const pollIngestion = async (expectedRunning) => {
        for (let attempt = 0; attempt < STATUS_POLL_ATTEMPTS; attempt += 1) {
            await sleep(STATUS_POLL_DELAY_MS);
            try {
                const response = await fetch("/api/ingestion/status");
                const json = (await response.json().catch(() => ({})));
                if (typeof json?.running === "boolean") {
                    const running = Boolean(json.running);
                    setIngesting(running);
                    if (running === expectedRunning) {
                        return true;
                    }
                }
            }
            catch {
                // ignore and keep polling
            }
        }
        return false;
    };
    async function loadSymbols(segmentOverride) {
        const requested = (segmentOverride ?? symbolSegment ?? "all").toLowerCase();
        const targetSegment = SYMBOL_SEGMENTS.some((opt) => opt.value === requested) ? requested : "all";
        try {
            setLoadingSymbols(true);
            const response = await fetch(`/api/ingestion/symbols?segment=${encodeURIComponent(targetSegment)}`);
            const json = await response.json().catch(() => null);
            const list = Array.isArray(json?.symbols)
                ? json.symbols.filter((sym) => typeof sym === "string" && sym.endsWith("/USDT"))
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
        }
        catch (error) {
            console.error(error);
        }
        finally {
            setLoadingSymbols(false);
        }
    }
    async function checkStatus(options) {
        const force = options?.force ?? false;
        if (!force && suspendRefresh)
            return;
        try {
            const response = await fetch("/api/ingestion/status");
            const json = (await response.json().catch(() => ({})));
            if (typeof json?.running === "boolean") {
                setIngesting(Boolean(json.running));
            }
        }
        catch {
            // network error: keep previous state
        }
    }
    async function loadCoverage(options) {
        const force = options?.force ?? false;
        if (!force && suspendRefresh)
            return;
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
            const data = Array.isArray(json) ? json : json?.rows ?? [];
            const normalized = data.map((row) => ({
                ...row,
                total_required: cps,
            }));
            setRows(normalized);
        }
        catch {
            setRows([]);
        }
        finally {
            setLoading(false);
        }
    }
    const handleSymbolSelection = (event) => {
        const values = Array.from(event.target.selectedOptions).map((option) => option.value);
        setSelectedSymbols(values);
    };
    const handleSelectAllSymbols = () => {
        if (!availableSymbols.length)
            return;
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
    const handlePresetChange = (limit) => {
        setSymbolFilterLimit(limit);
        setCustomLimit(limit && limit > 0 ? String(limit) : "");
    };
    const handleSegmentChange = (segment) => {
        const cleaned = (segment || "all").toLowerCase();
        if (cleaned === symbolSegment)
            return;
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
        }
        else {
            setSymbolFilterLimit(null);
            setCustomLimit("");
        }
    };
    const updateTask = (id, patch) => {
        setTaskConfigs((prev) => prev.map((task) => (task.id === id ? { ...task, ...patch } : task)));
    };
    const addTask = () => {
        const newTask = {
            id: `task-${Date.now()}`,
            timeframe: "15m",
            candles: "480",
            enabled: true,
            useCustomRange: false,
            start: "",
            end: "",
        };
        setTaskConfigs((prev) => [...prev, newTask]);
    };
    const removeTask = (id) => {
        setTaskConfigs((prev) => prev.filter((task) => task.id !== id));
    };
    const toIsoString = (value) => {
        if (!value)
            return null;
        const date = new Date(value);
        if (Number.isNaN(date.getTime()))
            return null;
        return date.toISOString();
    };
    async function startIngestion() {
        const activeSymbols = selectedSymbols.filter((sym) => availableSymbols.includes(sym));
        if (!activeSymbols.length) {
            alert("Please select at least one symbol.");
            return;
        }
        let taskPayload;
        try {
            taskPayload = taskConfigs
                .filter((task) => task.enabled)
                .map((task) => {
                const useRange = task.useCustomRange;
                const base = { timeframe: task.timeframe };
                if (useRange) {
                    const startIso = toIsoString(task.start);
                    const endIso = toIsoString(task.end);
                    if (!startIso || !endIso) {
                        throw new Error("Please provide valid start and end times for enabled custom ranges.");
                    }
                    base.start_iso = startIso;
                    base.end_iso = endIso;
                }
                else {
                    const candlesInt = parsePositiveInt(task.candles);
                    if (!Number.isInteger(candlesInt) || candlesInt <= 0) {
                        throw new Error("Please enter a valid positive number of candles for each enabled timeframe.");
                    }
                    base.candles_per_symbol = candlesInt;
                }
                return base;
            });
        }
        catch (err) {
            alert(err instanceof Error ? err.message : "Invalid timeframe task configuration.");
            return;
        }
        if (!taskPayload.length) {
            alert("Please enable at least one timeframe task.");
            return;
        }
        setStarting(true);
        setMsg("");
        try {
            const body = {
                interval,
                symbols: activeSymbols,
                tasks: taskPayload,
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
        }
        catch (error) {
            console.error(error);
            setIngesting(false);
            setMsg("Start failed");
        }
        finally {
            setStarting(false);
            setTimeout(() => {
                checkStatus({ force: true });
                loadCoverage({ force: true });
            }, 800);
        }
    }
    async function stopIngestion(opts = {}) {
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
        }
        catch (error) {
            console.error(error);
            if (!silent) {
                setMsg("Stop failed");
            }
            throw error;
        }
        finally {
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
            }
            else {
                setRows([]);
                setIngesting(false);
            }
            setMsg("Flushing database...");
            const response = await fetch("/api/db/flush", { method: "POST" });
            const json = (await response.json().catch(() => ({})));
            if (!response.ok) {
                throw new Error(json?.message || "Flush failed");
            }
            const message = json?.message || "DB flushed";
            setMsg(message);
        }
        catch (error) {
            console.error(error);
            setMsg("Flush failed");
        }
        finally {
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
        if (suspendRefresh)
            return;
        loadCoverage({ force: true });
    }, [interval, candlesPerSymbol, suspendRefresh, selectedSymbols, availableSymbols]);
    useEffect(() => {
        if (!ingesting || suspendRefresh)
            return;
        const id = setInterval(() => {
            checkStatus();
            loadCoverage();
        }, 2000);
        return () => clearInterval(id);
    }, [ingesting, suspendRefresh, interval, candlesPerSymbol, selectedSymbols]);
    const selectedCount = selectedSymbols.length;
    const fetchedCount = rows.length;
    return (_jsxs("div", { style: { padding: 20, fontFamily: "Inter, sans-serif" }, children: [_jsx("h2", { style: { marginBottom: 10 }, children: "CryptoScreener" }), msg && (_jsx("div", { style: { marginBottom: 10, padding: 8, background: "#f3f4f6", borderRadius: 8 }, children: msg })), _jsxs("div", { style: {
                    border: "1px solid #e5e7eb",
                    borderRadius: 12,
                    padding: 20,
                    marginBottom: 18,
                    background: "#ffffff",
                    boxShadow: "0 2px 6px rgba(15, 23, 42, 0.05)",
                }, children: [_jsx("h3", { style: { marginTop: 0, marginBottom: 16 }, children: "Ingestion Settings" }), _jsxs("div", { style: { display: "flex", flexDirection: "column", gap: 18 }, children: [_jsxs("div", { style: { display: "flex", flexDirection: "column", gap: 12 }, children: [_jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }, children: [_jsxs("div", { children: [_jsx("label", { style: { display: "block", fontWeight: 600, marginBottom: 4 }, children: "Symbols" }), _jsx("div", { style: { fontSize: 12, color: "#6b7280" }, children: availableSymbols.length
                                                            ? `${selectedCount.toLocaleString()} of ${availableSymbols.length.toLocaleString()} selected - Sorted by ${currentSegmentLabel}`
                                                            : "Loading symbol list..." })] }), _jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }, children: [_jsx("input", { type: "text", placeholder: "Search (e.g. BTC)", value: symbolSearch, onChange: (e) => setSymbolSearch(e.target.value), style: {
                                                            border: "1px solid #d1d5db",
                                                            borderRadius: 6,
                                                            padding: "6px 10px",
                                                            minWidth: 160,
                                                        } }), _jsx("div", { style: { display: "flex", flexWrap: "wrap", gap: 6 }, children: SYMBOL_SEGMENTS.map(({ label, value }) => {
                                                            const isActive = symbolSegment === value;
                                                            return (_jsx("button", { type: "button", onClick: () => handleSegmentChange(value), style: {
                                                                    border: "1px solid " + (isActive ? "#16a34a" : "#d1d5db"),
                                                                    background: isActive ? "#16a34a" : "#f0fdf4",
                                                                    color: isActive ? "#fff" : "#065f46",
                                                                    borderRadius: 999,
                                                                    padding: "6px 12px",
                                                                    fontSize: 12,
                                                                    cursor: "pointer",
                                                                }, children: label }, value));
                                                        }) })] })] }), _jsxs("div", { style: { display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center" }, children: [SYMBOL_PRESETS.map(({ label, limit }) => {
                                                const isActive = symbolFilterLimit === limit || (symbolFilterLimit === null && limit === null);
                                                return (_jsx("button", { type: "button", onClick: () => handlePresetChange(limit), style: {
                                                        border: "1px solid " + (isActive ? "#2563eb" : "#d1d5db"),
                                                        background: isActive ? "#2563eb" : "#f8fafc",
                                                        color: isActive ? "#fff" : "#1f2937",
                                                        borderRadius: 999,
                                                        padding: "6px 12px",
                                                        fontSize: 12,
                                                        cursor: "pointer",
                                                    }, children: label }, label));
                                            }), _jsxs("div", { style: { display: "flex", gap: 6, alignItems: "center" }, children: [_jsx("input", { type: "number", min: "1", placeholder: "Custom", value: customLimit, onChange: (e) => setCustomLimit(e.target.value), onKeyDown: (e) => {
                                                            if (e.key === "Enter") {
                                                                e.preventDefault();
                                                                applyCustomLimit();
                                                            }
                                                        }, style: {
                                                            width: 90,
                                                            border: "1px solid #d1d5db",
                                                            borderRadius: 6,
                                                            padding: "6px 10px",
                                                        } }), _jsx("button", { type: "button", onClick: applyCustomLimit, style: {
                                                            border: "1px solid #d1d5db",
                                                            borderRadius: 6,
                                                            padding: "6px 12px",
                                                            background: "#eef2ff",
                                                            cursor: "pointer",
                                                        }, children: "Apply" })] })] })] }), _jsxs("div", { style: { display: "flex", gap: 12, alignItems: "stretch", flexWrap: "wrap" }, children: [_jsx("select", { multiple: true, value: selectedSymbols, onChange: handleSymbolSelection, size: Math.min(14, Math.max(6, selectOptions.length || 6)), style: {
                                            flex: "1 1 440px",
                                            border: "1px solid #d1d5db",
                                            borderRadius: 8,
                                            padding: 8,
                                            minHeight: 190,
                                            background: "#f9fafb",
                                            fontFamily: "mono, monospace",
                                        }, children: selectOptions.map((sym, idx) => {
                                            const globalIndex = availableSymbols.indexOf(sym);
                                            const displayIndex = globalIndex >= 0 ? globalIndex + 1 : idx + 1;
                                            return (_jsx("option", { value: sym, children: `${displayIndex}. ${sym}` }, sym));
                                        }) }), _jsxs("div", { style: { display: "flex", flexDirection: "column", gap: 8, minWidth: 170 }, children: [_jsx("button", { type: "button", onClick: handleSelectFilteredSymbols, disabled: !filteredSymbols.length, style: {
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 8,
                                                    padding: "8px 12px",
                                                    background: filteredSymbols.length ? "#2563eb" : "#e5e7eb",
                                                    color: "#fff",
                                                    fontWeight: 600,
                                                    cursor: filteredSymbols.length ? "pointer" : "not-allowed",
                                                }, children: "Select Filtered" }), _jsx("button", { type: "button", onClick: handleSelectAllSymbols, disabled: !availableSymbols.length, style: {
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 8,
                                                    padding: "8px 12px",
                                                    background: "#f8fafc",
                                                    cursor: availableSymbols.length ? "pointer" : "not-allowed",
                                                }, children: "Select All" }), _jsx("button", { type: "button", onClick: handleClearSelection, disabled: !selectedSymbols.length, style: {
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 8,
                                                    padding: "8px 12px",
                                                    background: "#fdf2f8",
                                                    color: "#db2777",
                                                    cursor: selectedSymbols.length ? "pointer" : "not-allowed",
                                                }, children: "Clear Selection" }), _jsx("button", { type: "button", onClick: () => loadSymbols(), disabled: loadingSymbols, style: {
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 8,
                                                    padding: "8px 12px",
                                                    background: "#f8fafc",
                                                    cursor: loadingSymbols ? "not-allowed" : "pointer",
                                                }, children: loadingSymbols ? "Refreshing..." : "Reload List" }), !loadingSymbols && !availableSymbols.length && (_jsx("span", { style: { fontSize: 12, color: "#ef4444" }, children: "No symbols available" }))] })] }), _jsxs("div", { style: { display: "flex", flexDirection: "column", gap: 12 }, children: [_jsxs("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "center" }, children: [_jsx("h4", { style: { margin: 0 }, children: "Timeframe Tasks" }), _jsx("button", { type: "button", onClick: addTask, style: {
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 6,
                                                    padding: "6px 12px",
                                                    background: "#f8fafc",
                                                    cursor: "pointer",
                                                }, children: "+ Add Timeframe" })] }), taskConfigs.map((task) => (_jsxs("div", { style: {
                                            display: "grid",
                                            gridTemplateColumns: "auto 120px 120px 130px 1fr auto",
                                            gap: 8,
                                            alignItems: "center",
                                            border: "1px solid #e2e8f0",
                                            borderRadius: 10,
                                            padding: "10px 12px",
                                            background: task.enabled ? "#ffffff" : "#f8fafc",
                                        }, children: [_jsxs("label", { style: { display: "flex", alignItems: "center", gap: 6 }, children: [_jsx("input", { type: "checkbox", checked: task.enabled, onChange: (e) => updateTask(task.id, { enabled: e.target.checked }) }), "Enable"] }), _jsx("select", { value: task.timeframe, onChange: (e) => updateTask(task.id, { timeframe: e.target.value }), style: {
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 6,
                                                    padding: "6px 8px",
                                                }, children: TIMEFRAME_OPTIONS.map((option) => (_jsx("option", { value: option, children: option }, option))) }), _jsx("input", { type: "number", inputMode: "numeric", min: "1", step: "1", value: task.candles, onChange: (e) => updateTask(task.id, { candles: e.target.value }), disabled: task.useCustomRange, placeholder: "Candles", style: {
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 6,
                                                    padding: "6px 8px",
                                                    background: task.useCustomRange ? "#f3f4f6" : "#ffffff",
                                                } }), _jsxs("label", { style: { display: "flex", alignItems: "center", gap: 6 }, children: [_jsx("input", { type: "checkbox", checked: task.useCustomRange, onChange: (e) => {
                                                            const enabled = e.target.checked;
                                                            updateTask(task.id, {
                                                                useCustomRange: enabled,
                                                                start: enabled ? task.start : "",
                                                                end: enabled ? task.end : "",
                                                            });
                                                        } }), "Custom Range"] }), _jsxs("div", { style: { display: "flex", gap: 6 }, children: [_jsx("input", { type: "datetime-local", value: task.start, onChange: (e) => updateTask(task.id, { start: e.target.value }), disabled: !task.useCustomRange, style: {
                                                            border: "1px solid #d1d5db",
                                                            borderRadius: 6,
                                                            padding: "6px 8px",
                                                            flex: "1 1 auto",
                                                        } }), _jsx("input", { type: "datetime-local", value: task.end, onChange: (e) => updateTask(task.id, { end: e.target.value }), disabled: !task.useCustomRange, style: {
                                                            border: "1px solid #d1d5db",
                                                            borderRadius: 6,
                                                            padding: "6px 8px",
                                                            flex: "1 1 auto",
                                                        } })] }), _jsxs("div", { style: { display: "flex", gap: 6 }, children: [_jsx("button", { type: "button", onClick: () => setTaskConfigs((prev) => [
                                                            ...prev,
                                                            {
                                                                ...task,
                                                                id: `task-${Date.now()}`,
                                                                enabled: task.enabled,
                                                            },
                                                        ]), style: {
                                                            border: "1px solid #d1d5db",
                                                            borderRadius: 6,
                                                            padding: "6px 10px",
                                                            background: "#eef2ff",
                                                            cursor: "pointer",
                                                        }, children: "Duplicate" }), _jsx("button", { type: "button", onClick: () => removeTask(task.id), style: {
                                                            border: "1px solid #fca5a5",
                                                            borderRadius: 6,
                                                            padding: "6px 10px",
                                                            background: "#fee2e2",
                                                            color: "#b91c1c",
                                                            cursor: "pointer",
                                                        }, disabled: taskConfigs.length <= 1, children: "Remove" })] })] }, task.id))), !taskConfigs.length && (_jsx("div", { style: { fontSize: 12, color: "#ef4444" }, children: "Add at least one timeframe task." }))] }), _jsxs("div", { style: {
                                    display: "grid",
                                    gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
                                    gap: 12,
                                    alignItems: "end",
                                }, children: [_jsxs("div", { children: [_jsx("label", { children: "Interval" }), _jsxs("select", { value: interval, onChange: (e) => setIntervalTf(e.target.value), style: {
                                                    width: "100%",
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 8,
                                                    padding: "8px 10px",
                                                }, children: [_jsx("option", { value: "1m", children: "1m" }), _jsx("option", { value: "5m", children: "5m" }), _jsx("option", { value: "1h", children: "1h" })] })] }), _jsxs("div", { children: [_jsx("label", { children: "Candles per Symbol" }), _jsx("input", { type: "number", inputMode: "numeric", min: "1", step: "1", value: candlesPerSymbol, onChange: (e) => setCandlesPerSymbol(e.target.value), style: {
                                                    width: "100%",
                                                    border: "1px solid #d1d5db",
                                                    borderRadius: 8,
                                                    padding: "8px 10px",
                                                } })] }), _jsx("button", { onClick: async () => {
                                            if (ingesting) {
                                                try {
                                                    await stopIngestion();
                                                }
                                                catch {
                                                    // stopIngestion handles messaging on failure
                                                }
                                            }
                                            else {
                                                await startIngestion();
                                            }
                                        }, style: {
                                            background: starting ? "#facc15" : ingesting ? "#ef4444" : "#22c55e",
                                            color: "#fff",
                                            padding: "10px 14px",
                                            borderRadius: 8,
                                            border: "none",
                                            fontWeight: 600,
                                            cursor: "pointer",
                                            transition: "background 0.2s ease",
                                        }, children: starting ? "Working..." : ingesting ? "Stop Ingestion" : "Start Ingestion" })] })] })] }), _jsxs("div", { style: { display: "flex", gap: 8, marginBottom: 10 }, children: [_jsx("button", { onClick: () => loadCoverage({ force: true }), disabled: loading, style: { border: "1px solid #ccc", borderRadius: 8, padding: "8px 14px" }, children: loading ? "Loading..." : "Refresh" }), _jsx("button", { onClick: flushDB, style: { background: "#0ea5e9", color: "white", padding: "8px 14px", borderRadius: 8, border: "none" }, children: "Flush DB" })] }), _jsxs("div", { style: { marginBottom: 10, fontSize: 12, color: "#4b5563" }, children: ["Showing ", fetchedCount.toLocaleString(), " symbols (selected ", selectedCount.toLocaleString(), ")."] }), _jsxs("table", { style: { width: "100%", borderCollapse: "collapse" }, children: [_jsx("thead", { children: _jsxs("tr", { style: { borderBottom: "1px solid #e5e7eb" }, children: [_jsx("th", { align: "left", style: { padding: 6 }, children: "Symbol" }), _jsx("th", { align: "right", style: { padding: 6 }, children: "Total Required" }), _jsx("th", { align: "right", style: { padding: 6 }, children: "Received" }), _jsx("th", { align: "left", style: { padding: 6 }, children: "Latest TS" })] }) }), _jsx("tbody", { children: rows.map((row) => (_jsxs("tr", { style: { borderBottom: "1px solid #f3f4f6" }, children: [_jsx("td", { style: { padding: 6 }, children: row.symbol }), _jsx("td", { align: "right", style: { padding: 6 }, children: row.total_required ?? fallbackCandlesValue ?? "-" }), _jsx("td", { align: "right", style: { padding: 6 }, children: row.received ?? 0 }), _jsx("td", { style: { padding: 6 }, children: row.latest_ts
                                        ? new Date(row.latest_ts).toLocaleString([], {
                                            timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                                        })
                                        : "-" })] }, row.symbol))) })] })] }));
}
