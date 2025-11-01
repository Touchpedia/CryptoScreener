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
] as const;

type TabId = (typeof TABS)[number]["id"];

export default function AppShell() {
  const [activeTab, setActiveTab] = useState<TabId>("ingestion");

  return (
    <div className="app-shell">
      <header className="app-header">
        <div>
          <h1>Data Pipeline Console</h1>
          <p>Monitor ingestion, review aggregated charts, and supervise gap fills.</p>
        </div>
        <ThemeToggle />
      </header>

      <nav className="tab-bar" aria-label="Main navigation">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            className={tab.id === activeTab ? "tab active" : "tab"}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      <main className="tab-content">
        {activeTab === "ingestion" && (
          <div className="tab-grid">
            <IngestionPanel />
            <StatusBoard />
          </div>
        )}
        {activeTab === "chart" && <ChartView />}
        {activeTab === "admin" && <AdminPanel />}
      </main>
    </div>
  );
}
