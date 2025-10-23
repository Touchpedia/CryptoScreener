import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
const STATUS_ENDPOINTS = ['/api/status', 'http://127.0.0.1:8000/api/status'];
const formatPercent = (value, digits = 0) => {
    if (typeof value !== 'number' || Number.isNaN(value)) {
        return 'â€”';
    }
    const factor = 10 ** digits;
    const rounded = Math.round(value * factor) / factor;
    return `${rounded.toFixed(digits)}%`;
};
const formatTime = (value) => {
    if (!value) {
        return 'â€”';
    }
    try {
        const date = new Date(value);
        return date.toLocaleTimeString();
    }
    catch {
        return value;
    }
};
const summariseTimeframes = (value) => {
    if (!value || Object.keys(value).length === 0) {
        return '';
    }
    return Object.entries(value)
        .map(([key, pct]) => {
        const normalized = pct > 1 ? pct : pct * 100;
        return `${key}: ${Math.round(normalized)}%`;
    })
        .join(', ');
};
export default function StatusBoard() {
    const [snapshot, setSnapshot] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);
    const timerRef = useRef(null);
    const hasLoadedRef = useRef(false);
    const fetchStatus = useCallback(async () => {
        if (!hasLoadedRef.current) {
            setLoading(true);
        }
        setError(null);
        let lastError = null;
        for (const endpoint of STATUS_ENDPOINTS) {
            try {
                const response = await fetch(endpoint, { method: 'GET', headers: { Accept: 'application/json' } });
                if (!response.ok) {
                    throw new Error(`Status request failed (${response.status})`);
                }
                const data = (await response.json());
                setSnapshot(data);
                hasLoadedRef.current = true;
                setLoading(false);
                return;
            }
            catch (err) {
                lastError = err;
                if (!(err instanceof TypeError)) {
                    break;
                }
            }
        }
        const reason = lastError instanceof Error ? lastError.message : 'Unable to reach the API. Check that the backend is running.';
        setError(reason);
        setLoading(false);
    }, []);
    useEffect(() => {
        fetchStatus();
        timerRef.current = window.setInterval(fetchStatus, 5000);
        return () => {
            if (timerRef.current) {
                window.clearInterval(timerRef.current);
            }
        };
    }, [fetchStatus]);
    useEffect(() => {
        const handleStart = () => {
            window.setTimeout(fetchStatus, 500);
        };
        window.addEventListener('ingestion:started', handleStart);
        return () => {
            window.removeEventListener('ingestion:started', handleStart);
        };
    }, [fetchStatus]);
    const items = useMemo(() => snapshot?.items ?? [], [snapshot]);
    const runProgress = snapshot?.run;
    return (_jsxs("section", { className: "status-board", children: [_jsxs("header", { className: "status-board__header", children: [_jsx("h2", { children: "Latest Progress" }), _jsxs("div", { className: "status-board__meta", children: [_jsxs("span", { children: ["Pairs tracked: ", snapshot?.total ?? 0] }), _jsxs("span", { children: ["Last update: ", formatTime(snapshot?.lastUpdated)] })] }), runProgress && (_jsxs("div", { className: "status-board__run", children: [_jsx("span", { className: `status-pill status-pill--${(runProgress.status ?? 'unknown').toLowerCase()}`, children: runProgress.status ?? 'unknown' }), _jsx("span", { children: formatPercent(runProgress.percent ?? 0, 1) }), _jsxs("span", { children: ["Step ", runProgress.step ?? 0, "/", runProgress.total ?? 0] }), runProgress.symbol && runProgress.timeframe && (_jsxs("span", { children: [runProgress.symbol, " \u00C2\u00B7 ", runProgress.timeframe] })), runProgress.error && _jsx("span", { className: "status-board__run-error", children: runProgress.error })] }))] }), loading && !items.length && _jsx("p", { className: "status-board__hint", children: "Loading status..." }), error && _jsx("p", { className: "message error", children: error }), !loading && !error && !items.length && (_jsx("p", { className: "status-board__hint", children: "No progress yet. Start an ingestion run to populate status." })), items.length > 0 && (_jsxs("div", { className: "status-table", children: [_jsxs("div", { className: "status-table__header", children: [_jsx("span", { children: "Pair" }), _jsx("span", { children: "Status" }), _jsx("span", { children: "Progress" }), _jsx("span", { children: "Last Updated" })] }), _jsx("div", { className: "status-table__body", children: items.map((item) => (_jsxs("div", { className: "status-table__row", children: [_jsx("span", { children: item.pair }), _jsx("span", { className: `status-pill status-pill--${(item.status ?? 'idle').toLowerCase()}`, children: item.status ?? 'idle' }), _jsx("span", { title: summariseTimeframes(item.timeframes), children: formatPercent(item.progress) }), _jsx("span", { children: formatTime(item.updatedAt) })] }, item.pair))) })] }))] }));
}
