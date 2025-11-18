import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useEffect, useState } from "react";
import { runIngestion, getStatus } from "../lib/api";
const DEFAULT_TASKS = [
    { timeframe: "1m", candles: "600" },
    { timeframe: "5m", candles: "288" },
    { timeframe: "1h", candles: "168" },
];
export default function IngestionPanel() {
    const [symbols, setSymbols] = useState("BTC/USDT,ETH/USDT");
    const [tasks, setTasks] = useState(DEFAULT_TASKS);
    const [runId, setRunId] = useState(null);
    const [message, setMessage] = useState("");
    const [percent, setPercent] = useState(0);
    const updateTask = (index, patch) => {
        setTasks((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
    };
    const addTask = () => setTasks((prev) => [...prev, { timeframe: "15m", candles: "96" }]);
    const removeTask = (index) => setTasks((prev) => prev.filter((_, i) => i !== index));
    const buildPayload = () => {
        const symbolList = symbols
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
        const taskPayload = tasks
            .map((row) => ({
            timeframe: row.timeframe.trim(),
            candles_per_symbol: Number(row.candles),
        }))
            .filter((row) => row.timeframe &&
            Number.isFinite(row.candles_per_symbol) &&
            row.candles_per_symbol > 0);
        if (!symbolList.length) {
            throw new Error("Please provide at least one symbol.");
        }
        if (!taskPayload.length) {
            throw new Error("Please add at least one timeframe.");
        }
        return { symbols: symbolList, tasks: taskPayload };
    };
    async function handleStart() {
        try {
            const payload = buildPayload();
            const res = await runIngestion(payload);
            const rid = (res && (res.run_id || res.runId)) ?? null;
            setRunId(rid);
            setMessage(res?.message ?? "Ingestion started");
        }
        catch (error) {
            setMessage(error instanceof Error ? error.message : "Failed to start ingestion");
        }
    }
    useEffect(() => {
        if (!runId)
            return;
        const id = setInterval(async () => {
            try {
                const status = await getStatus();
                if (status?.run?.run_id === runId) {
                    const pct = Number(status.run.percent ?? 0);
                    setPercent(Number.isFinite(pct) ? pct : 0);
                }
            }
            catch {
                /* ignore polling errors */
            }
        }, 1500);
        return () => clearInterval(id);
    }, [runId]);
    return (_jsxs("div", { className: "p-4 border rounded-xl space-y-4", children: [_jsxs("div", { className: "flex gap-2 items-start flex-wrap", children: [_jsx("input", { className: "border p-2 flex-1 min-w-[240px]", value: symbols, onChange: (e) => setSymbols(e.target.value), placeholder: "BTC/USDT,ETH/USDT" }), _jsx("button", { className: "px-4 py-2 rounded-lg border", onClick: addTask, children: "+ Add TF" }), _jsx("button", { className: "px-4 py-2 rounded-lg border bg-blue-500 text-white", onClick: handleStart, children: "Start Ingestion" })] }), _jsx("div", { className: "space-y-2", children: tasks.map((row, index) => (_jsxs("div", { className: "flex gap-2 flex-wrap items-center", children: [_jsx("select", { value: row.timeframe, onChange: (e) => updateTask(index, { timeframe: e.target.value }), className: "border rounded px-2 py-1", children: ["1m", "5m", "15m", "1h", "4h", "1d"].map((tf) => (_jsx("option", { value: tf, children: tf }, tf))) }), _jsx("input", { type: "number", min: "1", value: row.candles, onChange: (e) => updateTask(index, { candles: e.target.value }), className: "border rounded px-2 py-1 w-24", placeholder: "Candles" }), _jsx("button", { className: "px-2 py-1 border rounded", onClick: () => removeTask(index), disabled: tasks.length === 1, children: "Remove" })] }, index))) }), message && _jsx("div", { className: "text-sm", children: message }), runId && (_jsxs("div", { className: "text-sm", children: ["Run: ", _jsx("span", { className: "font-mono", children: runId }), " - Progress: ", percent, "%"] }))] }));
}
