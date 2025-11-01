import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchStatusSnapshot } from "../lib/api";
function formatPercent(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
        return "0%";
    }
    return `${value.toFixed(1)}%`;
}
function formatTime(value) {
    if (!value) {
        return "-";
    }
    try {
        return new Date(value).toLocaleString();
    }
    catch {
        return value;
    }
}
function statusClass(status) {
    if (!status)
        return "status-pill";
    return `status-pill status-pill--${status.toLowerCase()}`;
}
export default function StatusBoard() {
    const [pairs, setPairs] = useState([]);
    const [run, setRun] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
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
        }
        catch (err) {
            setError(err instanceof Error ? err.message : "Unable to load status");
        }
        finally {
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
    return (_jsxs("section", { className: "panel", "aria-labelledby": "status-heading", children: [_jsxs("header", { className: "panel__header", children: [_jsxs("div", { children: [_jsx("h2", { id: "status-heading", children: "Ingestion Status" }), _jsx("p", { children: "Live view of pair progress and the most recent run." })] }), run && (_jsxs("div", { className: "run-indicator", children: [_jsx("span", { className: statusClass(run.status), children: run.status ?? "unknown" }), _jsx("span", { children: formatPercent(run.percent) }), run.symbol && run.timeframe && _jsx("span", { children: `${run.symbol} (${run.timeframe})` })] }))] }), loading && _jsx("p", { className: "hint", children: "Loading status..." }), error && _jsx("p", { className: "error", children: error }), !loading && !error && displayedPairs.length === 0 && _jsx("p", { className: "hint", children: "No status available yet." }), displayedPairs.length > 0 && (_jsxs("div", { className: "status-table", children: [_jsxs("div", { className: "status-table__header", children: [_jsx("span", { children: "Pair" }), _jsx("span", { children: "Status" }), _jsx("span", { children: "Progress" }), _jsx("span", { children: "Updated" })] }), _jsx("div", { className: "status-table__body", children: displayedPairs.map((pair) => (_jsxs("div", { className: "status-table__row", children: [_jsx("span", { children: pair.pair }), _jsx("span", { className: statusClass(pair.status), children: pair.status ?? "idle" }), _jsx("span", { children: formatPercent(pair.progress) }), _jsx("span", { children: formatTime(pair.updatedAt) })] }, pair.pair))) })] }))] }));
}
