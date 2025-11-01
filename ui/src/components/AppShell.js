import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from "react";
import AdminPanel from "./AdminPanel";
import ChartView from "./ChartView";
import IngestionPanel from "./IngestionPanel";
import StatusBoard from "./StatusBoard";
import ThemeToggle from "./ThemeToggle";
const TABS = [
    { id: "ingestion", label: "Ingestion" },
    { id: "chart", label: "Chart" },
    { id: "admin", label: "Admin" },
];
export default function AppShell() {
    const [activeTab, setActiveTab] = useState("ingestion");
    return (_jsxs("div", { className: "app-shell", children: [_jsxs("header", { className: "app-header", children: [_jsxs("div", { children: [_jsx("h1", { children: "Data Pipeline Console" }), _jsx("p", { children: "Monitor ingestion, review aggregated charts, and supervise gap fills." })] }), _jsx(ThemeToggle, {})] }), _jsx("nav", { className: "tab-bar", "aria-label": "Main navigation", children: TABS.map((tab) => (_jsx("button", { type: "button", className: tab.id === activeTab ? "tab active" : "tab", onClick: () => setActiveTab(tab.id), children: tab.label }, tab.id))) }), _jsxs("main", { className: "tab-content", children: [activeTab === "ingestion" && (_jsxs("div", { className: "tab-grid", children: [_jsx(IngestionPanel, {}), _jsx(StatusBoard, {})] })), activeTab === "chart" && _jsx(ChartView, {}), activeTab === "admin" && _jsx(AdminPanel, {})] })] }));
}
