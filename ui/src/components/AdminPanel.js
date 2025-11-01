import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo, useState } from "react";
import { fetchGapCoverage, triggerGapFill } from "../lib/api";
function parseList(value) {
    return value
        .split(/[\n,]+/)
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean);
}
function toTimestamp(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? Date.now() : date.getTime();
}
function toInputValue(date) {
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
    const [fillResults, setFillResults] = useState([]);
    const [fillMessage, setFillMessage] = useState("");
    const [coverage, setCoverage] = useState(null);
    const [coverageLoading, setCoverageLoading] = useState(false);
    const [coverageError, setCoverageError] = useState(null);
    const handleGapFill = async (event) => {
        event.preventDefault();
        const symbols = parseList(symbolsText);
        const timeframes = parseList(timeframesText);
        if (!symbols.length || !timeframes.length) {
            setFillMessage("Provide at least one symbol and timeframe.");
            return;
        }
        const payload = {
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
        }
        catch (err) {
            setFillMessage(err instanceof Error ? err.message : "Gap fill failed");
            setFillResults([]);
        }
        finally {
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
        }
        catch (err) {
            setCoverageError(err instanceof Error ? err.message : "Unable to compute coverage");
            setCoverage(null);
        }
        finally {
            setCoverageLoading(false);
        }
    };
    return (_jsxs("section", { className: "panel", "aria-labelledby": "admin-heading", children: [_jsx("header", { className: "panel__header", children: _jsxs("div", { children: [_jsx("h2", { id: "admin-heading", children: "Admin Tools" }), _jsx("p", { children: "Manually trigger gap fills and inspect coverage for critical pairs." })] }) }), _jsxs("form", { className: "panel__body", onSubmit: handleGapFill, children: [_jsxs("div", { className: "grid two-columns", children: [_jsxs("label", { children: ["Symbols", _jsx("textarea", { value: symbolsText, onChange: (event) => setSymbolsText(event.target.value), rows: 6 })] }), _jsxs("label", { children: ["Timeframes", _jsx("textarea", { value: timeframesText, onChange: (event) => setTimeframesText(event.target.value), rows: 6 })] })] }), _jsxs("div", { className: "date-grid", children: [_jsxs("label", { children: ["Start time", _jsx("input", { type: "datetime-local", value: start, onChange: (event) => setStart(event.target.value) })] }), _jsxs("label", { children: ["End time", _jsx("input", { type: "datetime-local", value: end, onChange: (event) => setEnd(event.target.value) })] })] }), _jsxs("footer", { className: "panel__footer", children: [_jsx("button", { type: "submit", disabled: loadingFill, children: loadingFill ? "Running gap fill�" : "Run gap fill" }), _jsx("button", { type: "button", className: "secondary", onClick: handleCoverage, disabled: coverageLoading, children: coverageLoading ? "Checking�" : "Coverage report" })] })] }), fillMessage && _jsx("p", { className: "message", children: fillMessage }), fillResults.length > 0 && (_jsxs("div", { className: "gap-results", children: [_jsx("h3", { children: "Latest gap fill results" }), _jsx("ul", { children: fillResults.map((result) => (_jsxs("li", { children: [result.symbol, " \uFFFD ", result.timeframe, " \uFFFD inserted ", result.inserted, " candles"] }, `${result.symbol}-${result.timeframe}-${result.start_ts}`))) })] })), coverageError && _jsx("p", { className: "error", children: coverageError }), coverage && (_jsxs("div", { className: "gap-coverage", children: [_jsx("h3", { children: "Coverage" }), _jsxs("p", { children: [coverage.symbol, " \uFFFD ", coverage.timeframe, " \uFFFD ", coverage.present, "/", coverage.expected, " candles (", coverage.coverage.toFixed(2), "%)"] })] }))] }));
}
