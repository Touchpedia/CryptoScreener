import { ChangeEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

type CoverageRow = {
  symbol: string;
  timeframe: string;
  required: number;
  received: number;
  coverage: number;
  latest_ts: string | number | null;
  start_ts: number | null;
  end_ts: number | null;
};

const STATUS_POLL_ATTEMPTS = 20;
const STATUS_POLL_DELAY_MS = 500;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const SYMBOL_PRESETS: { label: string; limit: number | null }[] = [
  { label: "All", limit: null },
  { label: "Top 10", limit: 10 },
  { label: "Top 20", limit: 20 },
  { label: "Top 50", limit: 50 },
  { label: "Top 100", limit: 100 },
  { label: "Top 200", limit: 200 },
];

const SYMBOL_SEGMENTS: { label: string; value: string }[] = [
  { label: "Alphabetical", value: "all" },
  { label: "Market Cap", value: "market_cap" },
  { label: "24h Volume", value: "volume" },
  { label: "24h Gainers", value: "gainers" },
  { label: "24h Losers", value: "losers" },
];

type TaskConfig = {
  id: string;
  timeframe: string;
  days: string;
  enabled: boolean;
};

type StoredSettings = {
  symbolSegment?: string;
  symbolFilterLimit?: number | null;
  customLimit?: string;
  selectedSymbols?: string[];
  taskConfigs?: TaskConfig[];
  symbolSearch?: string;
  rowSymbolFilter?: string[];
  rowTimeframeFilter?: string[];
  rowSortField?: SortField;
  rowSortDirection?: SortDirection;
  settingsCollapsed?: boolean;
  historyCollapsed?: boolean;
  statusHistory?: Array<{ message: string; when: number }>;
  activeTab?: "backfills" | "websockets" | "validation";
  websocketConnectionLimit?: number;
};

const STORAGE_KEY = "ingestion-settings-v1";

type SortField = "symbol" | "timeframe" | "required" | "received" | "coverage" | "latest";
type SortDirection = "asc" | "desc";

const TIMEFRAME_OPTIONS = ["1m", "3m", "5m", "15m", "1h", "4h", "1d"];

const MINUTES_PER_CANDLE: Record<string, number> = {
  "1m": 1,
  "3m": 3,
  "5m": 5,
  "15m": 15,
  "1h": 60,
  "4h": 240,
  "1d": 1440,
};

const DEFAULT_WEBSOCKET_CONNECTION_LIMIT = 48;
const WEBSOCKET_COVERAGE_READY_THRESHOLD = 0.98;
const WEBSOCKET_COVERAGE_WARNING_THRESHOLD = 0.9;
const WEBSOCKET_FRESH_MS_READY = 5 * 60 * 1000;
const WEBSOCKET_FRESH_MS_WARN = 30 * 60 * 1000;

function computeCandleInfo(timeframe: string, daysValue: string) {
  const minutesPerCandle = MINUTES_PER_CANDLE[timeframe];
  const days = Number(daysValue);
  if (!minutesPerCandle || !Number.isFinite(days) || days <= 0) {
    return null;
  }
  const totalCandles = Math.ceil((days * 24 * 60) / minutesPerCandle);
  const now = new Date();
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  const formatUtc = (d: Date) =>
    d.toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
  return {
    totalCandles,
    startIso: formatUtc(start),
    endIso: formatUtc(now),
    startMs: start.getTime(),
    endMs: now.getTime(),
  };
}

function formatSince(durationMs: number): string {
  const clamped = Math.max(0, durationMs);
  const seconds = Math.floor(clamped / 1000);
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    const remSeconds = seconds % 60;
    return remSeconds ? `${minutes}m ${remSeconds}s` : `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    const remMinutes = minutes % 60;
    return remMinutes ? `${hours}h ${remMinutes}m` : `${hours}h`;
  }
  const days = Math.floor(hours / 24);
  const remHours = hours % 24;
  if (days < 7) {
    return remHours ? `${days}d ${remHours}h` : `${days}d`;
  }
  const weeks = Math.floor(days / 7);
  const remDays = days % 7;
  return remDays ? `${weeks}w ${remDays}d` : `${weeks}w`;
}

function toEpochMs(value: number | string | null | undefined): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? -Infinity : parsed;
  }
  return -Infinity;
}

function formatDateTime(
  value: number | string | null | undefined,
  timeZone: string,
): string {
  if (value === null || value === undefined) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  return date.toLocaleString([], { timeZone });
}

const DEFAULT_TASKS: TaskConfig[] = [
  { id: "task-1m", timeframe: "1m", days: "7", enabled: true },
  { id: "task-3m", timeframe: "3m", days: "14", enabled: true },
  { id: "task-5m", timeframe: "5m", days: "30", enabled: true }
];

type FetchOptions = { force?: boolean };

type ActiveRow = {
  symbol: string;
  timeframe: string;
  status?: string;
  started_at?: number;
  updated_at?: number;
  last_ts?: number;
};

type TimeframeWebsocketStat = {
  timeframe: string;
  coveragePct: number;
  latestLabel: string;
  active: boolean;
  required: number;
  received: number;
};

type SymbolWebsocketStat = {
  symbol: string;
  status: "ready" | "watch" | "issue";
  ready: boolean;
  watch: boolean;
  minCoveragePct: number;
  aggregateCoveragePct: number;
  activeStreams: number;
  totalTimeframes: number;
  timeframes: TimeframeWebsocketStat[];
  latestLabel: string;
  driftLabel: string;
  driftMs: number;
};

type ValidationRow = {
  id: string;
  symbol: string;
  timeframe: string;
  required: number;
  received: number;
  coveragePct: number;
  latestLabel: string;
  websocketActive: boolean;
  status: "good" | "stale" | "missing";
  freshnessLabel: string;
};

export default function App() {
  const storedSettings = useMemo<StoredSettings | null>(() => {
    if (typeof window === "undefined") {
      return null;
    }
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as StoredSettings;
      const sanitized: StoredSettings = {};
      if (typeof parsed.symbolSegment === "string") {
        sanitized.symbolSegment = parsed.symbolSegment;
      }
      if (parsed.symbolFilterLimit === null || typeof parsed.symbolFilterLimit === "number") {
        sanitized.symbolFilterLimit = parsed.symbolFilterLimit;
      }
      if (typeof parsed.customLimit === "string") {
        sanitized.customLimit = parsed.customLimit;
      }
      if (Array.isArray(parsed.selectedSymbols)) {
        sanitized.selectedSymbols = parsed.selectedSymbols.filter(
          (sym): sym is string => typeof sym === "string" && sym.trim().length > 0,
        );
      }
      if (typeof parsed.symbolSearch === "string") {
        sanitized.symbolSearch = parsed.symbolSearch;
      }
      if (Array.isArray(parsed.taskConfigs)) {
        const sanitizedTasks: TaskConfig[] = [];
        parsed.taskConfigs.forEach((task, index) => {
          if (!task || typeof task !== "object") return;
          const timeframe = typeof task.timeframe === "string" ? task.timeframe : null;
          if (!timeframe) return;
          const daysValue =
            typeof task.days === "string"
              ? task.days
              : typeof task.days === "number"
                ? String(task.days)
                : null;
          if (!daysValue || !daysValue.trim()) return;
          sanitizedTasks.push({
            id: typeof task.id === "string" && task.id.trim() ? task.id : `task-${timeframe}-${index}`,
            timeframe,
            days: daysValue,
            enabled: typeof task.enabled === "boolean" ? task.enabled : true,
          });
        });
        if (sanitizedTasks.length) {
          sanitized.taskConfigs = sanitizedTasks;
        }
      }
      if (Array.isArray(parsed.rowSymbolFilter)) {
        sanitized.rowSymbolFilter = parsed.rowSymbolFilter.filter(
          (sym): sym is string => typeof sym === "string" && sym.trim().length > 0,
        );
      }
      if (Array.isArray(parsed.rowTimeframeFilter)) {
        sanitized.rowTimeframeFilter = parsed.rowTimeframeFilter.filter(
          (tf): tf is string => typeof tf === "string" && tf.trim().length > 0,
        );
      }
      if (parsed.rowSortField && ["symbol", "timeframe", "required", "received", "coverage", "latest"].includes(parsed.rowSortField)) {
        sanitized.rowSortField = parsed.rowSortField as SortField;
      }
      if (parsed.rowSortDirection && (parsed.rowSortDirection === "asc" || parsed.rowSortDirection === "desc")) {
        sanitized.rowSortDirection = parsed.rowSortDirection as SortDirection;
      }
      if (typeof parsed.settingsCollapsed === "boolean") {
        sanitized.settingsCollapsed = parsed.settingsCollapsed;
      }
      if (typeof parsed.historyCollapsed === "boolean") {
        sanitized.historyCollapsed = parsed.historyCollapsed;
      }
      if (Array.isArray(parsed.statusHistory)) {
        const cleaned = parsed.statusHistory
          .filter(
            (item): item is { message: string; when: number } =>
              item &&
              typeof item === "object" &&
              typeof (item as any).message === "string" &&
              typeof (item as any).when === "number",
          )
          .map((item) => ({
            message: (item as any).message as string,
            when: Number((item as any).when),
          }))
          .sort((a, b) => (b.when ?? 0) - (a.when ?? 0))
          .slice(0, 30);
        if (cleaned.length) {
          sanitized.statusHistory = cleaned;
        }
      }
      if (
        parsed.activeTab === "backfills" ||
        parsed.activeTab === "websockets" ||
        parsed.activeTab === "validation"
      ) {
        sanitized.activeTab = parsed.activeTab;
      }
      if (
        typeof parsed.websocketConnectionLimit === "number" &&
        Number.isFinite(parsed.websocketConnectionLimit) &&
        parsed.websocketConnectionLimit > 0
      ) {
        sanitized.websocketConnectionLimit = parsed.websocketConnectionLimit;
      }
      return sanitized;
    } catch {
      return null;
    }
  }, []);

  const initialHistory = (storedSettings?.statusHistory ?? []).slice(0, 30);
  const initialLastEntry = initialHistory[0] ?? { message: null, when: null };

  const [rows, setRows] = useState<CoverageRow[]>([]);

  const [availableSymbols, setAvailableSymbols] = useState<string[]>([]);
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>(
    () => storedSettings?.selectedSymbols ?? [],
  );
  const [loadingSymbols, setLoadingSymbols] = useState<boolean>(false);
  const [symbolSegment, setSymbolSegment] = useState<string>(
    () => storedSettings?.symbolSegment ?? "all",
  );
  const [symbolSearch, setSymbolSearch] = useState<string>(
    () => storedSettings?.symbolSearch ?? "",
  );
  const [symbolFilterLimit, setSymbolFilterLimit] = useState<number | null>(
    () => storedSettings?.symbolFilterLimit ?? null,
  );
  const [customLimit, setCustomLimit] = useState<string>(
    () => storedSettings?.customLimit ?? "",
  );
  const [taskConfigs, setTaskConfigs] = useState<TaskConfig[]>(
    () => storedSettings?.taskConfigs ?? DEFAULT_TASKS,
  );
  const [rowSymbolFilter, setRowSymbolFilter] = useState<string[]>(
    () => storedSettings?.rowSymbolFilter ?? [],
  );
  const [rowTimeframeFilter, setRowTimeframeFilter] = useState<string[]>(
    () => storedSettings?.rowTimeframeFilter ?? [],
  );
  const [rowSortField, setRowSortField] = useState<SortField>(
    () => storedSettings?.rowSortField ?? "symbol",
  );
  const [rowSortDirection, setRowSortDirection] = useState<SortDirection>(
    () => storedSettings?.rowSortDirection ?? "asc",
  );
  const [settingsCollapsed, setSettingsCollapsed] = useState<boolean>(
    () => storedSettings?.settingsCollapsed ?? false,
  );
  const [historyCollapsed, setHistoryCollapsed] = useState<boolean>(
    () => storedSettings?.historyCollapsed ?? false,
  );
  const [statusHistory, setStatusHistory] = useState<Array<{ message: string; when: number }>>(
    initialHistory,
  );
  const [apiStatus, setApiStatus] = useState<{ active: boolean; changedAt: number | null }>({
    active: false,
    changedAt: null,
  });
  const apiActiveRef = useRef<boolean>(false);
  const apiInitRef = useRef<boolean>(true);
  const [activeRows, setActiveRows] = useState<Record<string, ActiveRow>>({});
  const [activeTab, setActiveTab] = useState<"backfills" | "websockets" | "validation">(
    () => storedSettings?.activeTab ?? "backfills",
  );
  const [websocketLimit, setWebsocketLimit] = useState<number>(
    () =>
      typeof storedSettings?.websocketConnectionLimit === "number" &&
      storedSettings.websocketConnectionLimit > 0
        ? storedSettings.websocketConnectionLimit
        : DEFAULT_WEBSOCKET_CONNECTION_LIMIT,
  );

  const [loading, setLoading] = useState<boolean>(false);
  const [ingesting, setIngesting] = useState<boolean>(false);
  const [starting, setStarting] = useState<boolean>(false);
  const [suspendRefresh, setSuspendRefresh] = useState<boolean>(false);
  const [msg, setMsg] = useState<string>(initialLastEntry.message ?? "");
  const [lastAction, setLastAction] = useState<{ message: string | null; when: number | null }>(
    initialLastEntry,
  );
  const pushHistory = useCallback((message: string, when: number = Date.now()) => {
    setStatusHistory((prev) => {
      const next = [{ message, when }, ...prev];
      return next.slice(0, 30);
    });
  }, []);
  const clearHistory = useCallback(() => {
    setStatusHistory([]);
  }, []);

  const recordStatus = useCallback((message: string) => {
    const when = Date.now();
    setMsg(message);
    setLastAction({ message, when });
    pushHistory(message, when);
  }, [pushHistory]);

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
    if (!availableSymbols.length) return [];
    const optionSet = new Set<string>(filteredSymbols);
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

  const enabledTaskSummary = useMemo(() => {
    const enabled = taskConfigs
      .filter((task) => task.enabled)
      .map((task) => `${task.timeframe} / ${task.days}d`);
    return enabled.length ? enabled.join(", ") : "no timeframes (enable at least one)";
  }, [taskConfigs]);

  const enabledTaskCount = useMemo(
    () => taskConfigs.filter((task) => task.enabled).length,
    [taskConfigs],
  );

  const localTimeZone = useMemo(
    () => Intl.DateTimeFormat().resolvedOptions().timeZone,
    [],
  );

  const rowSymbolOptions = useMemo(() => {
    const set = new Set<string>();
    rows.forEach((row) => {
      if (row.symbol) {
        set.add(row.symbol);
      }
    });
    selectedSymbols.forEach((sym) => {
      if (sym) {
        set.add(sym);
      }
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [rows, selectedSymbols]);

  const rowTimeframeOptions = useMemo(() => {
    const set = new Set<string>();
    rows.forEach((row) => {
      if (row.timeframe) {
        set.add(row.timeframe);
      }
    });
    taskConfigs.forEach((task) => {
      if (task.timeframe) {
        set.add(task.timeframe);
      }
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [rows, taskConfigs]);

  const pollIngestion = async (expectedRunning: boolean) => {
    for (let attempt = 0; attempt < STATUS_POLL_ATTEMPTS; attempt += 1) {
      await sleep(STATUS_POLL_DELAY_MS);
      try {
        const response = await fetch("/api/ingestion/status");
        const json = (await response.json().catch(() => ({}))) as { running?: boolean };
        if (typeof json?.running === "boolean") {
          const running = Boolean(json.running);
          setIngesting(running);
          if (running === expectedRunning) {
            return true;
          }
        }
      } catch {
        // ignore and keep polling
      }
    }
    return false;
  };

  async function loadSymbols(segmentOverride?: string) {
    const requested = (segmentOverride ?? symbolSegment ?? "all").toLowerCase();
    const targetSegment = SYMBOL_SEGMENTS.some((opt) => opt.value === requested) ? requested : "all";

    try {
      setLoadingSymbols(true);
      const response = await fetch(`/api/ingestion/symbols_clean?segment=${encodeURIComponent(targetSegment)}`);
      const json = await response.json().catch(() => null);
      const list = Array.isArray(json?.symbols)
        ? (json.symbols as string[]).filter((sym) => typeof sym === "string" && sym.endsWith("/USDT"))
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
    } catch (error) {
      console.error(error);
    } finally {
      setLoadingSymbols(false);
    }
  }

  async function checkStatus(options?: FetchOptions) {
    const force = options?.force ?? false;
    if (!force && suspendRefresh) return;
    try {
      const response = await fetch("/api/ingestion/status");
      const json = (await response.json().catch(() => ({}))) as { running?: boolean };
      if (typeof json?.running === "boolean") {
        setIngesting(Boolean(json.running));
      }
    } catch {
      // network error: keep previous state
    }
  }

  async function loadCoverage(options?: FetchOptions) {
    const force = options?.force ?? false;
    if (!force && suspendRefresh) return;

    const activeSymbols = selectedSymbols.filter((sym) => availableSymbols.includes(sym));
    const activeTasks = taskConfigs
      .filter((task) => task.enabled)
      .map((task) => {
        const info = computeCandleInfo(task.timeframe, task.days);
        return info
          ? {
              timeframe: task.timeframe,
              candles_per_symbol: info.totalCandles,
              start_ts: info.startMs,
              end_ts: info.endMs,
            }
          : null;
      })
      .filter(
        (
          task,
        ): task is {
          timeframe: string;
          candles_per_symbol: number;
          start_ts: number;
          end_ts: number;
        } => Boolean(task),
      );

    if (activeSymbols.length === 0 || !activeTasks.length) {
      setRows([]);
      return;
    }

    try {
      setLoading(true);
      const response = await fetch("/api/report/coverage", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          symbols: activeSymbols,
          tasks: activeTasks,
        }),
      });
      const json = await response.json().catch(() => null);
      const data: any[] = Array.isArray(json) ? json : json?.rows ?? [];
      const normalized: CoverageRow[] = data.map((row) => {
        const required = Number(row.required ?? row.candles_per_symbol ?? 0);
        const received = Number(row.received ?? 0);
        const coverage = required > 0 ? received / required : 0;
        return {
          symbol: String(row.symbol ?? ""),
          timeframe: String(row.timeframe ?? ""),
          required,
          received,
          coverage,
          latest_ts: row.latest_ts ?? null,
          start_ts:
            typeof row.start_ts === "number"
              ? row.start_ts
              : typeof row.start_ts === "string"
                ? Date.parse(row.start_ts)
                : null,
          end_ts:
            typeof row.end_ts === "number"
              ? row.end_ts
              : typeof row.end_ts === "string"
                ? Date.parse(row.end_ts)
                : null,
        };
      });
      setRows(normalized);
    } catch {
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  const handleSymbolSelection = (event: ChangeEvent<HTMLSelectElement>) => {
    const values = Array.from(event.target.selectedOptions).map((option) => option.value);
    setSelectedSymbols(values);
  };

  const handleSelectAllSymbols = () => {
    if (!availableSymbols.length) return;
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

  const handlePresetChange = (limit: number | null) => {
    setSymbolFilterLimit(limit);
    setCustomLimit(limit && limit > 0 ? String(limit) : "");
  };

  const handleSegmentChange = (segment: string) => {
    const cleaned = (segment || "all").toLowerCase();
    if (cleaned === symbolSegment) return;
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
    } else {
      setSymbolFilterLimit(null);
      setCustomLimit("");
    }
  };

  const handleWebsocketLimitChange = (event: ChangeEvent<HTMLInputElement>) => {
    const next = Number.parseInt(event.target.value, 10);
    if (Number.isNaN(next)) {
      return;
    }
    setWebsocketLimit(Math.max(1, Math.min(next, 500)));
  };

  const updateTask = (id: string, patch: Partial<TaskConfig>) => {
    setTaskConfigs((prev) => prev.map((task) => (task.id === id ? { ...task, ...patch } : task)));
  };

  const addTask = () => {
    const newTask: TaskConfig = {
      id: `task-${Date.now()}`,
      timeframe: "1m",
      days: "7",
      enabled: true,
    };
    setTaskConfigs((prev) => [...prev, newTask]);
  };

  const removeTask = (id: string) => {
    setTaskConfigs((prev) => prev.filter((task) => task.id !== id));
  };

  async function startIngestion() {
    const activeSymbols = selectedSymbols.filter((sym) => availableSymbols.includes(sym));
    if (!activeSymbols.length) {
      alert("Please select at least one symbol.");
      return;
    }

    let taskPayload: Record<string, unknown>[];
    try {
      taskPayload = taskConfigs
        .filter((task) => task.enabled)
        .map((task) => {
          const info = computeCandleInfo(task.timeframe, task.days);
          if (!info) {
            throw new Error("Please enter a positive number of days for each enabled timeframe.");
          }
          return {
            timeframe: task.timeframe,
            candles_per_symbol: info.totalCandles,
          };
        });
    } catch (err) {
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
      const body: Record<string, unknown> = {
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
      const message = started
        ? "Ingestion started"
        : "Ingestion enqueued; waiting for status...";
      recordStatus(message);
    } catch (error) {
      console.error(error);
      setIngesting(false);
      recordStatus("Start failed");
    } finally {
      setStarting(false);
      setTimeout(() => {
        checkStatus({ force: true });
        loadCoverage({ force: true });
      }, 800);
    }
  }

  async function stopIngestion(opts: { silent?: boolean } = {}) {
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
        const message = stopped
          ? "Ingestion stopped"
          : "Stop requested; waiting for status...";
        recordStatus(message);
      }
    } catch (error) {
      console.error(error);
      if (!silent) {
        recordStatus("Stop failed");
      }
      throw error;
    } finally {
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

        recordStatus("Stopping ingestion...");
        setIngesting(false);
        setRows([]);
        await stopIngestion({ silent: true });
      } else {
        setRows([]);
        setIngesting(false);
      }

      recordStatus("Flushing database...");
      const response = await fetch("/api/db/flush", { method: "POST" });
      const json = (await response.json().catch(() => ({}))) as { message?: string };

      if (!response.ok) {
        throw new Error(json?.message || "Flush failed");
      }

      const message = json?.message || "DB flushed";
      recordStatus(message);
    } catch (error) {
      console.error(error);
      recordStatus("Flush failed");
    } finally {
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
    if (suspendRefresh) return;
    loadCoverage({ force: true });
  }, [suspendRefresh, selectedSymbols, availableSymbols, taskConfigs]);

  useEffect(() => {
    if (!ingesting || suspendRefresh) return;
    const id = setInterval(() => {
      checkStatus();
      loadCoverage();
    }, 2000);
    return () => clearInterval(id);
  }, [ingesting, suspendRefresh, selectedSymbols, taskConfigs]);

  useEffect(() => {
    let isCancelled = false;
    let timer: number | undefined;

    const fetchActive = async () => {
      try {
        const res = await fetch("/api/ingestion/active");
        if (!res.ok) {
          if (!ingesting && !isCancelled) {
            setActiveRows({});
          }
          return;
        }
        const json = await res.json().catch(() => null);
        const items: ActiveRow[] =
          Array.isArray(json?.items) ? (json.items as ActiveRow[]) : [];
        const next: Record<string, ActiveRow> = {};
        items.forEach((item) => {
          if (!item || typeof item !== "object") {
            return;
          }
          const symbol = typeof item.symbol === "string" ? item.symbol : "";
          const timeframe = typeof item.timeframe === "string" ? item.timeframe : "";
          if (!symbol || !timeframe) {
            return;
          }
          const key = `${symbol}::${timeframe}`;
          next[key] = {
            symbol,
            timeframe,
            status: typeof item.status === "string" ? item.status : undefined,
            started_at:
              typeof item.started_at === "number" ? item.started_at : undefined,
            updated_at:
              typeof item.updated_at === "number" ? item.updated_at : undefined,
            last_ts: typeof item.last_ts === "number" ? item.last_ts : undefined,
          };
        });
        if (!isCancelled) {
          setActiveRows(next);
        }
      } catch {
        if (!ingesting && !isCancelled) {
          setActiveRows({});
        }
      }
    };

    if (ingesting) {
      fetchActive();
      timer = window.setInterval(fetchActive, 2000);
    } else {
      fetchActive();
    }

    return () => {
      isCancelled = true;
      if (typeof timer === "number") {
        window.clearInterval(timer);
      }
    };
  }, [ingesting]);
  useEffect(() => {
    const active = Object.keys(activeRows).length > 0;
    const now = Date.now();
    if (apiInitRef.current) {
      apiInitRef.current = false;
    }
    if (apiActiveRef.current !== active) {
      apiActiveRef.current = active;
      setApiStatus({ active, changedAt: now });
    }
  }, [activeRows]);
  const selectedCount = selectedSymbols.length;
  const fetchedCount = rows.length;
  const [nowTick, setNowTick] = useState<number>(() => Date.now());

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    try {
      const payload: StoredSettings = {
        symbolSegment,
        symbolFilterLimit,
        customLimit,
        selectedSymbols,
        taskConfigs,
        symbolSearch,
        rowSymbolFilter,
        rowTimeframeFilter,
        rowSortField,
        rowSortDirection,
        settingsCollapsed,
        historyCollapsed,
        statusHistory: statusHistory.slice(0, 30),
        activeTab,
        websocketConnectionLimit: websocketLimit,
      };
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (error) {
      console.error("Failed to persist ingestion settings", error);
    }
  }, [
    symbolSegment,
    symbolFilterLimit,
    customLimit,
    selectedSymbols,
    taskConfigs,
    symbolSearch,
    rowSymbolFilter,
    rowTimeframeFilter,
    rowSortField,
    rowSortDirection,
    settingsCollapsed,
    historyCollapsed,
    statusHistory,
    activeTab,
    websocketLimit,
  ]);

  useEffect(() => {
    const timer = window.setInterval(() => setNowTick(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, []);

  const statusMessage = msg || lastAction.message || "Ready";
  const statusTone = useMemo(() => {
    if (!statusMessage) return "info";
    const lower = statusMessage.toLowerCase();
    if (lower.includes("fail")) return "danger";
    if (lower.includes("stop")) return "warning";
    if (lower.includes("flush")) return "info";
    if (lower.includes("stopping")) return "warning";
    if (lower.includes("start")) return "success";
    return "info";
  }, [statusMessage]);

  const statusSince =
    lastAction.when != null ? formatSince(nowTick - lastAction.when) : null;
  const apiSince =
    apiStatus.changedAt != null ? formatSince(nowTick - apiStatus.changedAt) : null;
  const activeJobCount = useMemo(
    () => Object.keys(activeRows).length,
    [activeRows],
  );

  const tabs = [
    { id: "backfills" as const, label: "CryptoScreener Backfills" },
    { id: "websockets" as const, label: "Websockets & Live Feed" },
    { id: "validation" as const, label: "Data Validation" },
  ];

  const readinessPalette: Record<
    "ready" | "watch" | "issue",
    { label: string; color: string; background: string }
  > = {
    ready: {
      label: "Ready",
      color: "#047857",
      background: "rgba(4, 120, 87, 0.18)",
    },
    watch: {
      label: "Watch",
      color: "#b45309",
      background: "rgba(180, 83, 9, 0.16)",
    },
    issue: {
      label: "Needs Attention",
      color: "#b91c1c",
      background: "rgba(185, 28, 28, 0.16)",
    },
  };

  const validationPalette: Record<
    ValidationRow["status"],
    { label: string; color: string; background: string }
  > = {
    good: {
      label: "Healthy",
      color: "#047857",
      background: "rgba(4, 120, 87, 0.16)",
    },
    stale: {
      label: "Stale",
      color: "#b45309",
      background: "rgba(180, 83, 9, 0.14)",
    },
    missing: {
      label: "Missing",
      color: "#b91c1c",
      background: "rgba(185, 28, 28, 0.14)",
    },
  };

  const symbolWebsocketStats = useMemo<SymbolWebsocketStat[]>(() => {
    if (!rows.length) return [];
    type GroupedSymbol = {
      symbol: string;
      timeframes: Array<{
        timeframe: string;
        coverage: number;
        required: number;
        received: number;
        latestMs: number;
        active: boolean;
      }>;
      minCoverage: number;
      maxCoverage: number;
      totalReceived: number;
      totalRequired: number;
      latestMs: number;
    };
    const now = nowTick;
    const map = new Map<string, GroupedSymbol>();
    rows.forEach((row) => {
      const rawSymbol = row.symbol;
      if (!rawSymbol) {
        return;
      }
      const symbol = String(rawSymbol);
      const coverageValue = Number.isFinite(row.coverage)
        ? Math.max(0, Math.min(1, row.coverage))
        : 0;
      const timeframe = row.timeframe || "";
      const required = Number.isFinite(row.required) ? row.required : 0;
      const received = Number.isFinite(row.received) ? row.received : 0;
      const latestMs = toEpochMs(row.latest_ts);
      const activeKey = `${symbol}::${timeframe}`;
      const isActive =
        activeRows[activeKey] &&
        (activeRows[activeKey].status ?? "running") !== "completed";
      const existing = map.get(symbol);
      const bucket: GroupedSymbol =
        existing ??
        {
          symbol,
          timeframes: [],
          minCoverage: coverageValue,
          maxCoverage: coverageValue,
          totalReceived: 0,
          totalRequired: 0,
          latestMs,
        };
      bucket.timeframes.push({
        timeframe,
        coverage: coverageValue,
        required,
        received,
        latestMs,
        active: Boolean(isActive),
      });
      bucket.minCoverage = Math.min(bucket.minCoverage, coverageValue);
      bucket.maxCoverage = Math.max(bucket.maxCoverage, coverageValue);
      bucket.totalReceived += received;
      bucket.totalRequired += required;
      bucket.latestMs = Math.max(bucket.latestMs, latestMs);
      map.set(symbol, bucket);
    });

    const stats: SymbolWebsocketStat[] = Array.from(map.values()).map((entry) => {
      const hasData = Number.isFinite(entry.latestMs) && entry.latestMs > 0;
      const driftMs = hasData ? Math.max(0, now - entry.latestMs) : Number.POSITIVE_INFINITY;
      const ready =
        entry.minCoverage >= WEBSOCKET_COVERAGE_READY_THRESHOLD &&
        driftMs <= WEBSOCKET_FRESH_MS_READY;
      const watch =
        !ready &&
        entry.minCoverage >= WEBSOCKET_COVERAGE_WARNING_THRESHOLD &&
        driftMs <= WEBSOCKET_FRESH_MS_WARN;
      const status: "ready" | "watch" | "issue" = ready ? "ready" : watch ? "watch" : "issue";
      const activeStreams = entry.timeframes.filter((tf) => tf.active).length;
      const timeframes: TimeframeWebsocketStat[] = entry.timeframes
        .slice()
        .sort((a, b) => a.timeframe.localeCompare(b.timeframe))
        .map((tf) => ({
          timeframe: tf.timeframe || "-",
          coveragePct: Math.min(100, Math.max(0, tf.coverage * 100)),
          latestLabel:
            Number.isFinite(tf.latestMs) && tf.latestMs > 0
              ? formatDateTime(tf.latestMs, localTimeZone)
              : "-",
          active: tf.active,
          required: tf.required,
          received: tf.received,
        }));
      const aggregateCoverage =
        entry.totalRequired > 0
          ? (entry.totalReceived / entry.totalRequired) * 100
          : entry.maxCoverage * 100;
      const latestLabel =
        hasData && entry.latestMs > 0
          ? formatDateTime(entry.latestMs, localTimeZone)
          : "No candles ingested yet";
      const driftLabel =
        driftMs === Number.POSITIVE_INFINITY
          ? "no data"
          : driftMs <= 5000
            ? "just now"
            : `${formatSince(driftMs)} ago`;
      return {
        symbol: entry.symbol,
        status,
        ready,
        watch,
        minCoveragePct: Math.min(100, Math.max(0, entry.minCoverage * 100)),
        aggregateCoveragePct: Math.min(100, Math.max(0, aggregateCoverage)),
        activeStreams,
        totalTimeframes: timeframes.length,
        timeframes,
        latestLabel,
        driftLabel,
        driftMs,
      };
    });

    const statusOrder: Record<SymbolWebsocketStat["status"], number> = {
      ready: 0,
      watch: 1,
      issue: 2,
    };

    stats.sort((a, b) => {
      if (statusOrder[a.status] !== statusOrder[b.status]) {
        return statusOrder[a.status] - statusOrder[b.status];
      }
      if (b.minCoveragePct !== a.minCoveragePct) {
        return b.minCoveragePct - a.minCoveragePct;
      }
      if (a.driftMs !== b.driftMs) {
        return a.driftMs - b.driftMs;
      }
      return a.symbol.localeCompare(b.symbol);
    });

    return stats;
  }, [rows, activeRows, nowTick]);

  const websocketSummary = useMemo(() => {
    if (!symbolWebsocketStats.length) {
      return {
        total: 0,
        ready: 0,
        watch: 0,
        issues: 0,
        load: 0,
        utilizationPct: 0,
        aboveLimit: false,
        headroom: websocketLimit,
        backlog: 0,
        attention: [] as SymbolWebsocketStat[],
      };
    }
    const readyCount = symbolWebsocketStats.filter((item) => item.ready).length;
    const watchCount = symbolWebsocketStats.filter((item) => item.status === "watch").length;
    const issueCount = symbolWebsocketStats.filter((item) => item.status === "issue").length;
    const utilized = Math.min(readyCount, websocketLimit);
    const utilizationPct =
      websocketLimit > 0 ? Math.round((utilized / websocketLimit) * 100) : 0;
    const attention = symbolWebsocketStats
      .filter((item) => item.status !== "ready")
      .slice(0, 6);
    return {
      total: symbolWebsocketStats.length,
      ready: readyCount,
      watch: watchCount,
      issues: issueCount,
      load: utilized,
      utilizationPct,
      aboveLimit: readyCount > websocketLimit,
      headroom: Math.max(websocketLimit - readyCount, 0),
      backlog: readyCount > websocketLimit ? readyCount - websocketLimit : 0,
      attention,
    };
  }, [symbolWebsocketStats, websocketLimit]);

  const readyCoveragePct = Math.round(WEBSOCKET_COVERAGE_READY_THRESHOLD * 100);
  const warningCoveragePct = Math.round(WEBSOCKET_COVERAGE_WARNING_THRESHOLD * 100);
  const freshnessReadyMinutes = Math.round(WEBSOCKET_FRESH_MS_READY / 60000);
  const freshnessWarnMinutes = Math.round(WEBSOCKET_FRESH_MS_WARN / 60000);

  const validationRows = useMemo<ValidationRow[]>(() => {
    if (!rows.length) return [];
    return rows
      .filter((row) => Number(row.received ?? 0) >= 1)
      .map((row) => {
        const required = Number(row.required ?? 0);
        const received = Number(row.received ?? 0);
        const coverage = required > 0 ? (received / required) * 100 : received > 0 ? 100 : 0;
        const latestMs = toEpochMs(row.latest_ts);
        const trackerKey = `${row.symbol}::${row.timeframe}`;
        const trackerActive =
          Boolean(activeRows[trackerKey]) &&
          (activeRows[trackerKey]?.status ?? "running") !== "completed";
        const driftMs =
          Number.isFinite(latestMs) && latestMs > 0 ? Math.max(0, nowTick - latestMs) : Infinity;
        let status: ValidationRow["status"];
        if (!Number.isFinite(driftMs) || driftMs === Infinity) {
          status = "missing";
        } else if (driftMs <= WEBSOCKET_FRESH_MS_READY) {
          status = "good";
        } else if (driftMs <= WEBSOCKET_FRESH_MS_WARN) {
          status = "stale";
        } else {
          status = "missing";
        }
        const freshnessLabel =
          driftMs === Infinity ? "no data" : driftMs <= 5000 ? "just now" : `${formatSince(driftMs)} ago`;
        const wsActive =
          trackerActive || (driftMs !== Infinity && driftMs <= WEBSOCKET_FRESH_MS_WARN);
        return {
          id: `${row.symbol}::${row.timeframe}`,
          symbol: row.symbol,
          timeframe: row.timeframe,
          required,
          received,
          coveragePct: Math.min(100, Math.max(0, coverage)),
          latestLabel:
            Number.isFinite(latestMs) && latestMs > 0
              ? formatDateTime(latestMs, localTimeZone)
              : "No rows yet",
          websocketActive: wsActive,
          status,
          freshnessLabel,
        };
      })
      .sort((a, b) => {
        if (a.symbol !== b.symbol) {
          return a.symbol.localeCompare(b.symbol);
        }
        if (a.timeframe !== b.timeframe) {
          return a.timeframe.localeCompare(b.timeframe);
        }
        return 0;
      });
  }, [rows, activeRows, nowTick, localTimeZone]);

  const validationSummary = useMemo(() => {
    if (!validationRows.length) {
      return { total: 0, good: 0, stale: 0, missing: 0, noConnection: 0 };
    }
    const good = validationRows.filter((row) => row.status === "good").length;
    const stale = validationRows.filter((row) => row.status === "stale").length;
    const missing = validationRows.filter((row) => row.status === "missing").length;
    const noConnection = validationRows.filter((row) => !row.websocketActive).length;
    return {
      total: validationRows.length,
      good,
      stale,
      missing,
      noConnection,
    };
  }, [validationRows]);

  const tonePalette: Record<
    string,
    { accent: string; background: string; shadow: string }
  > = {
    success: {
      accent: "#16a34a",
      background: "rgba(22,163,74,0.14)",
      shadow: "0 10px 25px rgba(22,163,74,0.18)",
    },
    warning: {
      accent: "#f59e0b",
      background: "rgba(245,158,11,0.18)",
      shadow: "0 10px 25px rgba(245,158,11,0.22)",
    },
    info: {
      accent: "#0ea5e9",
      background: "rgba(14,165,233,0.18)",
      shadow: "0 10px 25px rgba(14,165,233,0.2)",
    },
    danger: {
      accent: "#dc2626",
      background: "rgba(220,38,38,0.18)",
      shadow: "0 10px 25px rgba(220,38,38,0.25)",
    },
  };

  const palette = tonePalette[statusTone] ?? tonePalette.info;


  const exchangePalette = apiStatus.active
    ? {
        accent: "#2563eb",
        background: "rgba(37,99,235,0.12)",
        shadow: "0 6px 18px rgba(37,99,235,0.18)",
      }
    : {
        accent: "#6b7280",
        background: "rgba(107,114,128,0.12)",
        shadow: "0 4px 14px rgba(107,114,128,0.18)",
      };
  const displayRows = useMemo(() => {
    const symbolSet =
      rowSymbolFilter.length > 0
        ? new Set(rowSymbolFilter.map((sym) => sym.toUpperCase()))
        : null;
    const timeframeSet =
      rowTimeframeFilter.length > 0
        ? new Set(rowTimeframeFilter.map((tf) => tf.toLowerCase()))
        : null;

    const toMs = toEpochMs;

    const filtered = rows.filter((row) => {
      const symbolOk = !symbolSet || symbolSet.has(row.symbol.toUpperCase());
      const timeframeOk =
        !timeframeSet || timeframeSet.has(row.timeframe.toLowerCase());
      return symbolOk && timeframeOk;
    });

    const sorted = [...filtered].sort((a, b) => {
      let result = 0;
      switch (rowSortField) {
        case "symbol":
          result = a.symbol.localeCompare(b.symbol);
          break;
        case "timeframe":
          result = a.timeframe.localeCompare(b.timeframe);
          break;
        case "required":
          result = a.required - b.required;
          break;
        case "received":
          result = a.received - b.received;
          break;
        case "coverage":
          result = a.coverage - b.coverage;
          break;
        case "latest":
          result = toMs(a.latest_ts) - toMs(b.latest_ts);
          break;
        default:
          result = 0;
      }
      if (result === 0) {
        result = a.symbol.localeCompare(b.symbol);
        if (result === 0) {
          result = a.timeframe.localeCompare(b.timeframe);
        }
      }
      return rowSortDirection === "asc" ? result : -result;
    });

    return sorted;
  }, [rows, rowSymbolFilter, rowTimeframeFilter, rowSortField, rowSortDirection]);

  const visibleCount = displayRows.length;

  const handleRowSymbolFilterChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const values = Array.from(event.target.selectedOptions).map(
      (option) => option.value,
    );
    setRowSymbolFilter(values);
  };

  const handleRowTimeframeFilterChange = (
    event: ChangeEvent<HTMLSelectElement>,
  ) => {
    const values = Array.from(event.target.selectedOptions).map(
      (option) => option.value,
    );
    setRowTimeframeFilter(values);
  };

  const setSort = (field: SortField, direction: SortDirection) => {
    setRowSortField(field);
    setRowSortDirection(direction);
  };

  const renderSortControls = (field: SortField) => (
    <span style={{ display: "inline-flex", flexDirection: "column", marginLeft: 4 }}>
      <button
        type="button"
        onClick={() => setSort(field, "asc")}
        style={{
          border: "1px solid #d1d5db",
          borderRadius: 3,
          padding: "0 4px",
          fontSize: 10,
          lineHeight: "14px",
          background:
            rowSortField === field && rowSortDirection === "asc"
              ? "#c7d2fe"
              : "#f1f5f9",
          cursor: "pointer",
        }}
        title="Sort ascending"
      >
        {"\u25B2"}
      </button>
      <button
        type="button"
        onClick={() => setSort(field, "desc")}
        style={{
          border: "1px solid #d1d5db",
          borderRadius: 3,
          padding: "0 4px",
          fontSize: 10,
          lineHeight: "14px",
          background:
            rowSortField === field && rowSortDirection === "desc"
              ? "#c7d2fe"
              : "#f1f5f9",
          cursor: "pointer",
          marginTop: 2,
        }}
        title="Sort descending"
      >
        {"\u25BC"}
      </button>
    </span>
  );

  return (
    <div
      style={{
        padding: 24,
        fontFamily: "Inter, sans-serif",
        minHeight: "100vh",
        background: "#f4f6fb",
      }}
    >
      <header style={{ marginBottom: 24 }}>
        <h1 style={{ margin: 0, fontSize: 28 }}>Data Operations Hub</h1>
        <p style={{ margin: "6px 0 0", color: "#475569" }}>
          Monitor historical backfills and upcoming live-stream ingestion from a single workspace.
        </p>
      </header>

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 24 }}>
        {tabs.map((tab) => {
          const isActive = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveTab(tab.id)}
              style={{
                border: isActive ? "none" : "1px solid #d1d5db",
                background: isActive ? "linear-gradient(135deg, #2563eb, #1e3a8a)" : "#ffffff",
                color: isActive ? "#ffffff" : "#1f2933",
                padding: "10px 18px",
                borderRadius: 999,
                fontWeight: 600,
                fontSize: 13,
                cursor: "pointer",
                boxShadow: isActive ? "0 8px 18px rgba(37,99,235,0.25)" : "none",
                transition: "all 0.25s ease",
              }}
            >
              {tab.label}
            </button>
          );
        })}
      </div>

      {activeTab === "backfills" ? (
        <>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 16,
              marginBottom: 18,
            }}
          >
            <div
              style={{
                flex: "1 1 320px",
                minWidth: 260,
                padding: "14px 18px",
                borderRadius: 14,
                border: `1px solid ${palette.accent}`,
                background: palette.background,
                boxShadow: palette.shadow,
                display: "flex",
                alignItems: "center",
                gap: 14,
              }}
            >
              <span
                style={{
                  width: 12,
                  height: 12,
                  borderRadius: "999px",
                  background: palette.accent,
                  boxShadow: `0 0 0 6px ${palette.accent}26`,
                  flexShrink: 0,
                }}
              />
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <span style={{ fontWeight: 600, fontSize: 15 }}>{statusMessage}</span>
                {statusSince && (
                  <span style={{ fontSize: 12, color: "#475569" }}>
                    {statusSince === "just now"
                      ? "since = just now"
                      : `since = ${statusSince}`}
                  </span>
                )}
              </div>
            </div>
            <div
              style={{
                flex: "1 1 260px",
                minWidth: 220,
                padding: "14px 18px",
                borderRadius: 14,
                border: `1px solid ${exchangePalette.accent}`,
                background: exchangePalette.background,
                boxShadow: exchangePalette.shadow,
                display: "flex",
                flexDirection: "column",
                gap: 6,
              }}
            >
              <span style={{ fontWeight: 600, fontSize: 15 }}>Exchange Requests</span>
              <span style={{ fontSize: 14, color: exchangePalette.accent, fontWeight: 600 }}>
                {apiStatus.active ? "Running" : "Idle"}
              </span>
              <span style={{ fontSize: 12, color: "#475569" }}>
                since = {apiSince ?? "--"}
              </span>
              <span style={{ fontSize: 12, color: "#475569" }}>
                Active pairs: {activeJobCount}
              </span>
            </div>
          </div>

          <div
            style={{
              border: "1px solid #e5e7eb",
              borderRadius: 14,
              marginBottom: 18,
              background: "#ffffff",
              boxShadow: "0 6px 18px rgba(15, 23, 42, 0.08)",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                padding: "12px 20px",
                background: "#f8fafc",
                gap: 12,
              }}
            >
              <button
                type="button"
                onClick={() => setHistoryCollapsed((prev) => !prev)}
                style={{
                  background: "transparent",
                  border: "none",
                  display: "flex",
                  alignItems: "center",
                  gap: 10,
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: 14,
                }}
              >
                <span>Status History ({Math.min(statusHistory.length, 30)})</span>
                <span
                  style={{
                    display: "inline-flex",
                    transform: historyCollapsed ? "rotate(0deg)" : "rotate(180deg)",
                    transition: "transform 0.3s ease",
                  }}
                >
                  {"\u25BC"}
                </span>
              </button>
              <button
                type="button"
                onClick={clearHistory}
                disabled={!statusHistory.length}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 20,
                  padding: "4px 12px",
                  fontSize: 12,
                  background: statusHistory.length ? "#ffffff" : "#f1f5f9",
                  cursor: statusHistory.length ? "pointer" : "not-allowed",
                  color: statusHistory.length ? "#111827" : "#9ca3af",
                }}
              >
                Clear
              </button>
            </div>
            <div
              style={{
                maxHeight: historyCollapsed
                  ? 0
                  : statusHistory.length
                  ? Math.min(statusHistory.length, 10) * 56
                  : 72,
                overflow: "hidden",
                transition: "max-height 0.45s ease",
              }}
            >
              {statusHistory.length ? (
                <ul
                  style={{
                    listStyle: "none",
                    margin: 0,
                    padding: historyCollapsed ? "0 20px" : "12px 20px 16px",
                  }}
                >
                  {statusHistory.slice(0, 12).map((entry, index) => {
                    const since = formatSince(nowTick - entry.when);
                    const absolute = new Date(entry.when).toLocaleString([], {
                      timeZone: localTimeZone,
                    });
                    const isLatest = index === 0;
                    return (
                      <li
                        key={`${entry.when}-${index}`}
                        style={{
                          display: "flex",
                          justifyContent: "space-between",
                          alignItems: "flex-start",
                          gap: 16,
                          padding: "10px 0",
                          borderBottom:
                            index === Math.min(statusHistory.length, 12) - 1
                              ? "none"
                              : "1px solid #e5e7eb",
                        }}
                      >
                        <div>
                          <div
                            style={{
                              fontWeight: isLatest ? 600 : 500,
                              color: isLatest ? palette.accent : "#111827",
                              marginBottom: 4,
                            }}
                          >
                            {entry.message}
                          </div>
                          <div style={{ fontSize: 12, color: "#6b7280" }}>{absolute}</div>
                        </div>
                        <span style={{ fontSize: 12, color: "#475569" }}>
                          {since === "just now" ? "since = just now" : `since = ${since}`}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              ) : (
                <div
                  style={{
                    padding: "16px 20px",
                    fontSize: 12,
                    color: "#6b7280",
                  }}
                >
                  No ingestion events recorded yet.
                </div>
              )}
            </div>
          </div>

      <div
        style={{
          border: "1px solid #e5e7eb",
          borderRadius: 16,
          marginBottom: 18,
          background: "#ffffff",
          boxShadow: "0 8px 24px rgba(15, 23, 42, 0.08)",
          overflow: "hidden",
          transition: "transform 0.4s ease, box-shadow 0.4s ease",
        }}
        className={settingsCollapsed ? "settings-card collapsed" : "settings-card"}
      >
        <button
          type="button"
          onClick={() => setSettingsCollapsed((prev) => !prev)}
          style={{
            width: "100%",
            background: "linear-gradient(135deg, #2563eb, #1e3a8a)",
            color: "#fff",
            padding: "18px 20px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            fontWeight: 600,
            fontSize: 18,
            letterSpacing: 0.3,
            border: "none",
            cursor: "pointer",
            position: "relative",
          }}
        >
          <span>Ingestion Settings</span>
          <span
            style={{
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              width: 32,
              height: 32,
              borderRadius: "999px",
              border: "2px solid rgba(255,255,255,0.6)",
              transition: "transform 0.35s ease",
              transform: settingsCollapsed ? "rotate(180deg)" : "rotate(0deg)",
            }}
          >
            {"\u25BC"}
          </span>
          </button>

          <div
            style={{
              padding: settingsCollapsed ? "0px 20px 0px" : "20px",
              maxHeight: settingsCollapsed ? 0 : "1500px",
              overflow: "hidden",
              transition: "max-height 0.5s ease, padding 0.35s ease",
              background: "radial-gradient(circle at top, rgba(37,99,235,0.1), transparent)",
            }}
          >
            <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }}>
              <div>
                <label style={{ display: "block", fontWeight: 600, marginBottom: 4 }}>Symbols</label>
                <div style={{ fontSize: 12, color: "#6b7280" }}>
                  {availableSymbols.length
                    ? `${selectedCount.toLocaleString()} of ${availableSymbols.length.toLocaleString()} selected - Sorted by ${currentSegmentLabel}`
                    : "Loading symbol list..."}
                </div>
              </div>

              <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center" }}>
                <input
                  type="text"
                  placeholder="1m,3m,5m"
                  value={symbolSearch}
                  onChange={(e) => setSymbolSearch(e.target.value)}
                  style={{
                    border: "1px solid #d1d5db",
                    borderRadius: 6,
                    padding: "6px 10px",
                    minWidth: 160,
                  }}
                />
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                  {SYMBOL_SEGMENTS.map(({ label, value }) => {
                    const isActive = symbolSegment === value;
                    return (
                      <button
                        key={value}
                        type="button"
                        onClick={() => handleSegmentChange(value)}
                        style={{
                          border: "1px solid " + (isActive ? "#16a34a" : "#d1d5db"),
                          background: isActive ? "#16a34a" : "#f0fdf4",
                          color: isActive ? "#fff" : "#065f46",
                          borderRadius: 999,
                          padding: "6px 12px",
                          fontSize: 12,
                          cursor: "pointer",
                        }}
                      >
                        {label}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>

            <div style={{ display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center" }}>
              {SYMBOL_PRESETS.map(({ label, limit }) => {
                const isActive = symbolFilterLimit === limit || (symbolFilterLimit === null && limit === null);
                return (
                  <button
                    key={label}
                    type="button"
                    onClick={() => handlePresetChange(limit)}
                    style={{
                      border: "1px solid " + (isActive ? "#2563eb" : "#d1d5db"),
                      background: isActive ? "#2563eb" : "#f8fafc",
                      color: isActive ? "#fff" : "#1f2937",
                      borderRadius: 999,
                      padding: "6px 12px",
                      fontSize: 12,
                      cursor: "pointer",
                    }}
                  >
                    {label}
                  </button>
                );
              })}
              <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                <input
                  type="number"
                  min="1"
                  placeholder="1m,3m,5m"
                  value={customLimit}
                  onChange={(e) => setCustomLimit(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      applyCustomLimit();
                    }
                  }}
                  style={{
                    width: 90,
                    border: "1px solid #d1d5db",
                    borderRadius: 6,
                    padding: "6px 10px",
                  }}
                />
                <button
                  type="button"
                  onClick={applyCustomLimit}
                  style={{
                    border: "1px solid #d1d5db",
                    borderRadius: 6,
                    padding: "6px 12px",
                    background: "#eef2ff",
                    cursor: "pointer",
                  }}
                >
                  Apply
                </button>
              </div>
            </div>
        </div>

        <div style={{ display: "flex", gap: 12, alignItems: "stretch", flexWrap: "wrap" }}>
            <select
              multiple
              value={selectedSymbols}
              onChange={handleSymbolSelection}
              size={Math.min(14, Math.max(6, selectOptions.length || 6))}
              style={{
                flex: "1 1 440px",
                border: "1px solid #d1d5db",
                borderRadius: 8,
                padding: 8,
                minHeight: 190,
                background: "#f9fafb",
                fontFamily: "mono, monospace",
              }}
            >
              {selectOptions.map((sym, idx) => {
                const globalIndex = availableSymbols.indexOf(sym);
                const displayIndex = globalIndex >= 0 ? globalIndex + 1 : idx + 1;
                return (
                  <option key={sym} value={sym}>
                    {`${displayIndex}. ${sym}`}
                  </option>
                );
              })}
            </select>

            <div style={{ display: "flex", flexDirection: "column", gap: 8, minWidth: 170 }}>
              <button
                type="button"
                onClick={handleSelectFilteredSymbols}
                disabled={!filteredSymbols.length}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: filteredSymbols.length ? "#2563eb" : "#e5e7eb",
                  color: "#fff",
                  fontWeight: 600,
                  cursor: filteredSymbols.length ? "pointer" : "not-allowed",
                }}
              >
                Select Filtered
              </button>
              <button
                type="button"
                onClick={handleSelectAllSymbols}
                disabled={!availableSymbols.length}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: "#f8fafc",
                  cursor: availableSymbols.length ? "pointer" : "not-allowed",
                }}
              >
                Select All
              </button>
              <button
                type="button"
                onClick={handleClearSelection}
                disabled={!selectedSymbols.length}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: "#fdf2f8",
                  color: "#db2777",
                  cursor: selectedSymbols.length ? "pointer" : "not-allowed",
                }}
              >
                Clear Selection
              </button>
              <button
                type="button"
                onClick={() => loadSymbols()}
                disabled={loadingSymbols}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 8,
                  padding: "8px 12px",
                  background: "#f8fafc",
                  cursor: loadingSymbols ? "not-allowed" : "pointer",
                }}
              >
                {loadingSymbols ? "Refreshing..." : "Reload List"}
              </button>
              {!loadingSymbols && !availableSymbols.length && (
                <span style={{ fontSize: 12, color: "#ef4444" }}>No symbols available</span>
              )}
            </div>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <h4 style={{ margin: 0 }}>Timeframe Tasks</h4>
              <button
                type="button"
                onClick={addTask}
                style={{
                  border: "1px solid #d1d5db",
                  borderRadius: 6,
                  padding: "6px 12px",
                  background: "#f8fafc",
                  cursor: "pointer",
                }}
              >
                + Add Timeframe
              </button>
            </div>

            {taskConfigs.map((task) => {
              const info = computeCandleInfo(task.timeframe, task.days);
              return (
                <div
                  key={task.id}
                  style={{
                    display: "grid",
                    gridTemplateColumns: "auto 120px 140px 1fr auto",
                    gap: 8,
                    alignItems: "center",
                    border: "1px solid #e2e8f0",
                    borderRadius: 10,
                    padding: "10px 12px",
                    background: task.enabled ? "#ffffff" : "#f8fafc",
                  }}
                >
                  <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <input
                      type="checkbox"
                      checked={task.enabled}
                      onChange={(e) => updateTask(task.id, { enabled: e.target.checked })}
                    />
                    Enable
                  </label>

                  <select
                    value={task.timeframe}
                    onChange={(e) => updateTask(task.id, { timeframe: e.target.value })}
                    style={{
                      border: "1px solid #d1d5db",
                      borderRadius: 6,
                      padding: "6px 8px",
                    }}
                  >
                    {TIMEFRAME_OPTIONS.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>

                  <input
                    type="number"
                    inputMode="numeric"
                    min="1"
                    step="1"
                    value={task.days}
                    onChange={(e) => updateTask(task.id, { days: e.target.value })}
                    placeholder="Days"
                    style={{
                      border: "1px solid #d1d5db",
                      borderRadius: 6,
                      padding: "6px 8px",
                      background: "#ffffff",
                    }}
                  />

                  <div style={{ fontSize: 12, color: info ? "#4b5563" : "#dc2626" }}>
                    {info ? (
                      <>
                        Last {Number(task.days)} days {"->"} {info.totalCandles.toLocaleString()} candles (
                        {info.startIso} {"->"} {info.endIso})
                      </>
                    ) : (
                      "Enter a positive number of days to see required candles and date range."
                    )}
                  </div>

                  <div style={{ display: "flex", gap: 6 }}>
                    <button
                      type="button"
                      onClick={() =>
                        setTaskConfigs((prev) => [
                          ...prev,
                          {
                            ...task,
                            id: `task-${Date.now()}`,
                            enabled: task.enabled,
                          },
                        ])
                      }
                      style={{
                        border: "1px solid #d1d5db",
                        borderRadius: 6,
                        padding: "6px 10px",
                        background: "#eef2ff",
                        cursor: "pointer",
                      }}
                    >
                      Duplicate
                    </button>
                    <button
                      type="button"
                      onClick={() => removeTask(task.id)}
                      style={{
                        border: "1px solid #fca5a5",
                        borderRadius: 6,
                        padding: "6px 10px",
                        background: "#fee2e2",
                        color: "#b91c1c",
                        cursor: "pointer",
                      }}
                      disabled={taskConfigs.length <= 1}
                    >
                      Remove
                    </button>
                  </div>
                </div>
              );
            })}

            {!taskConfigs.length && (
              <div style={{ fontSize: 12, color: "#ef4444" }}>Add at least one timeframe task.</div>
            )}
          </div>

          <div
            style={{
              display: "flex",
              gap: 12,
              flexWrap: "wrap",
              alignItems: "center",
            }}
          >
            <button
              onClick={async () => {
                if (ingesting) {
                  try {
                    await stopIngestion();
                  } catch {
                    // stopIngestion handles messaging on failure
                  }
                } else {
                  await startIngestion();
                }
              }}
              style={{
                background: starting ? "#facc15" : ingesting ? "#ef4444" : "#22c55e",
                color: "#fff",
                padding: "10px 14px",
                borderRadius: 8,
                border: "none",
                fontWeight: 600,
                cursor: "pointer",
                transition: "background 0.2s ease",
              }}
            >
              {starting ? "Working..." : ingesting ? "Stop Ingestion" : "Start Ingestion"}
            </button>
            <div style={{ fontSize: 12, color: "#4b5563" }}>
              Tracking&nbsp;
              {enabledTaskSummary}
            </div>
          </div>
            </div>
          </div>
        </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 10 }}>
        <button
          onClick={() => loadCoverage({ force: true })}
          disabled={loading}
          style={{ border: "1px solid #ccc", borderRadius: 8, padding: "8px 14px" }}
        >
          {loading ? "Loading..." : "Refresh"}
        </button>
        <button
          onClick={flushDB}
          style={{ background: "#0ea5e9", color: "white", padding: "8px 14px", borderRadius: 8, border: "none" }}
        >
          Flush DB
        </button>
      </div>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 12,
          marginBottom: 12,
          alignItems: "flex-end",
        }}
      >
        <div style={{ minWidth: 200 }}>
          <label style={{ display: "block", fontWeight: 600, marginBottom: 4 }}>
            Filter Symbols
          </label>
          <select
            multiple
            value={rowSymbolFilter}
            onChange={handleRowSymbolFilterChange}
            size={Math.min(8, Math.max(4, rowSymbolOptions.length || 4))}
            style={{
              width: "100%",
              border: "1px solid #d1d5db",
              borderRadius: 6,
              padding: 6,
              background: "#f9fafb",
            }}
          >
            {rowSymbolOptions.map((sym) => (
              <option key={sym} value={sym}>
                {sym}
              </option>
            ))}
          </select>
          <div style={{ display: "flex", gap: 6, marginTop: 6 }}>
            <button
              type="button"
              onClick={() => setRowSymbolFilter([])}
              style={{
                border: "1px solid #d1d5db",
                borderRadius: 6,
                padding: "4px 10px",
                background: "#f8fafc",
                cursor: "pointer",
                fontSize: 12,
              }}
            >
              Clear
            </button>
            <button
              type="button"
              onClick={() => setRowSymbolFilter(rowSymbolOptions)}
              disabled={!rowSymbolOptions.length}
              style={{
                border: "1px solid #d1d5db",
                borderRadius: 6,
                padding: "4px 10px",
                background: "#eef2ff",
                cursor: rowSymbolOptions.length ? "pointer" : "not-allowed",
                fontSize: 12,
              }}
            >
              Select All
            </button>
          </div>
        </div>
        <div style={{ minWidth: 180 }}>
          <label style={{ display: "block", fontWeight: 600, marginBottom: 4 }}>
            Filter Timeframes
          </label>
          <select
            multiple
            value={rowTimeframeFilter}
            onChange={handleRowTimeframeFilterChange}
            size={Math.min(6, Math.max(3, rowTimeframeOptions.length || 3))}
            style={{
              width: "100%",
              border: "1px solid #d1d5db",
              borderRadius: 6,
              padding: 6,
              background: "#f9fafb",
            }}
          >
            {rowTimeframeOptions.map((tf) => (
              <option key={tf} value={tf}>
                {tf}
              </option>
            ))}
          </select>
          <div style={{ display: "flex", gap: 6, marginTop: 6 }}>
            <button
              type="button"
              onClick={() => setRowTimeframeFilter([])}
              style={{
                border: "1px solid #d1d5db",
                borderRadius: 6,
                padding: "4px 10px",
                background: "#f8fafc",
                cursor: "pointer",
                fontSize: 12,
              }}
            >
              Clear
            </button>
            <button
              type="button"
              onClick={() => setRowTimeframeFilter(rowTimeframeOptions)}
              disabled={!rowTimeframeOptions.length}
              style={{
                border: "1px solid #d1d5db",
                borderRadius: 6,
                padding: "4px 10px",
                background: "#eef2ff",
                cursor: rowTimeframeOptions.length ? "pointer" : "not-allowed",
                fontSize: 12,
              }}
            >
              Select All
            </button>
          </div>
        </div>
      </div>

      <div style={{ marginBottom: 10, fontSize: 12, color: "#4b5563" }}>
        Showing {visibleCount.toLocaleString()} of {fetchedCount.toLocaleString()} symbol/timeframe rows
        (selected {selectedCount.toLocaleString()} symbols, {enabledTaskCount.toLocaleString()} timeframes
        enabled).
      </div>

      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #e5e7eb" }}>
            <th align="left" style={{ padding: 6 }}>
              <span style={{ display: "inline-flex", alignItems: "center" }}>
                Symbol
                {renderSortControls("symbol")}
              </span>
            </th>
            <th align="left" style={{ padding: 6 }}>
              <span style={{ display: "inline-flex", alignItems: "center" }}>
                Timeframe
                {renderSortControls("timeframe")}
              </span>
            </th>
            <th align="right" style={{ padding: 6 }}>
              <span style={{ display: "inline-flex", alignItems: "center" }}>
                Required
                {renderSortControls("required")}
              </span>
            </th>
            <th align="right" style={{ padding: 6 }}>
              <span style={{ display: "inline-flex", alignItems: "center" }}>
                Received
                {renderSortControls("received")}
              </span>
            </th>
            <th align="right" style={{ padding: 6 }}>
              <span style={{ display: "inline-flex", alignItems: "center" }}>
                Coverage
                {renderSortControls("coverage")}
              </span>
            </th>
            <th align="left" style={{ padding: 6 }}>
              <span style={{ display: "inline-flex", alignItems: "center" }}>
                Latest TS
                {renderSortControls("latest")}
              </span>
            </th>
            <th align="left" style={{ padding: 6 }}>
              Window
            </th>
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row) => {
            const key = `${row.symbol}-${row.timeframe}`;
            const trackerKey = `${row.symbol}::${row.timeframe}`;
            const active = activeRows[trackerKey];
            const isActive = Boolean(active && (active.status ?? "running") !== "completed");
            const required = Number(row.required ?? 0);
            const received = Number(row.received ?? 0);
            const coveragePct = Math.min(100, Math.max(0, row.coverage * 100));
            const requiredLabel = Number.isFinite(required) ? required.toLocaleString() : "-";
            const receivedLabel = Number.isFinite(received) ? received.toLocaleString() : "-";
            const coverageLabel =
              Number.isFinite(coveragePct) && required > 0 ? `${coveragePct.toFixed(1)}%` : "-";
            const latestTs = formatDateTime(row.latest_ts, localTimeZone);
            const startTs = formatDateTime(row.start_ts, localTimeZone);
            const endTs = formatDateTime(row.end_ts, localTimeZone);
            const activeTitle = isActive
              ? `Fetching ${row.symbol} ${row.timeframe}...`
              : undefined;
            return (
              <tr
                key={key}
                style={{
                  borderBottom: "1px solid #f3f4f6",
                  background: isActive ? "#ecfeff" : undefined,
                  transition: "background 0.2s ease",
                }}
              >
                <td style={{ padding: 6 }}>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                    {isActive && <span className="ingestion-active-indicator" title={activeTitle} />}
                    <span title={activeTitle}>{row.symbol}</span>
                  </span>
                </td>
                <td style={{ padding: 6 }}>{row.timeframe}</td>
                <td align="right" style={{ padding: 6 }}>{requiredLabel}</td>
                <td align="right" style={{ padding: 6 }}>{receivedLabel}</td>
                <td align="right" style={{ padding: 6 }}>{coverageLabel}</td>
                <td style={{ padding: 6 }}>{latestTs}</td>
                <td style={{ padding: 6 }}>
                  {startTs !== "-" && endTs !== "-" ? `${startTs} -> ${endTs}` : "-"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
        </>
      ) : activeTab === "websockets" ? (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 24,
          }}
        >
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 18,
              justifyContent: "space-between",
              alignItems: "stretch",
            }}
          >
            <div
              style={{
                flex: "1 1 420px",
                minWidth: 280,
                display: "flex",
                flexDirection: "column",
                gap: 10,
              }}
            >
              <h2 style={{ margin: 0, fontSize: 26 }}>Realtime Websocket Control Room</h2>
              <p style={{ margin: 0, color: "#475569", fontSize: 14, maxWidth: 640 }}>
                We track every symbol/timeframe you backfilled to confirm it is safe to promote to a
                live websocket stream. A symbol becomes ready once coverage stays above {readyCoveragePct}% and the latest
                candle is no more than {freshnessReadyMinutes} minutes old.
              </p>
              <span style={{ fontSize: 12, color: "#64748b" }}>
                Warning band: coverage &gt;= {warningCoveragePct}% within {freshnessWarnMinutes} minutes; anything lower
                drops into the attention queue below.
              </span>
            </div>
            <div
              style={{
                flex: "0 0 260px",
                minWidth: 240,
                border: "1px solid #dbeafe",
                borderRadius: 18,
                background: "#ffffff",
                padding: "18px 20px",
                boxShadow: "0 12px 28px rgba(37, 99, 235, 0.16)",
                display: "flex",
                flexDirection: "column",
                gap: 12,
              }}
            >
              <span style={{ fontWeight: 600, fontSize: 13, color: "#1e3a8a" }}>
                Connection budget (max websocket streams)
              </span>
              <input
                type="number"
                min={1}
                max={500}
                value={websocketLimit}
                onChange={handleWebsocketLimitChange}
                style={{
                  border: "1px solid #c7d2fe",
                  borderRadius: 12,
                  padding: "10px 12px",
                  fontSize: 14,
                  fontWeight: 600,
                  color: "#1f2937",
                  background: "#f8fafc",
                }}
              />
              <span style={{ fontSize: 12, color: "#475569" }}>
                Planning {websocketSummary.load.toLocaleString()} of {websocketLimit.toLocaleString()} streams.
              </span>
              <span
                style={{
                  fontSize: 12,
                  fontWeight: 600,
                  color: websocketSummary.aboveLimit ? "#b91c1c" : "#047857",
                }}
              >
                {websocketSummary.aboveLimit
                  ? `Over limit by ${websocketSummary.backlog.toLocaleString()} symbol${
                      websocketSummary.backlog === 1 ? "" : "s"
                    }.`
                  : websocketSummary.headroom > 0
                    ? `Headroom for ${websocketSummary.headroom.toLocaleString()} more symbol${
                        websocketSummary.headroom === 1 ? "" : "s"
                      }.`
                    : "At capacity — expand budget or pause some streams."}
              </span>
            </div>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
              gap: 16,
            }}
          >
            <div
              style={{
                border: "1px solid #e0f2fe",
                borderRadius: 18,
                padding: 20,
                background: "linear-gradient(135deg, rgba(37,99,235,0.12), rgba(30,64,175,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 12,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#1d4ed8" }}>Streams ready</span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#0f172a" }}>
                {websocketSummary.load.toLocaleString()} / {websocketLimit.toLocaleString()}
              </span>
              <span style={{ fontSize: 12, color: "#1e293b" }}>
                Utilization {websocketSummary.utilizationPct}%
              </span>
              <div
                style={{
                  height: 8,
                  borderRadius: 999,
                  background: "#e2e8f0",
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    width: `${Math.min(100, websocketSummary.utilizationPct)}%`,
                    background: websocketSummary.aboveLimit ? "#f97316" : "#2563eb",
                    height: "100%",
                    borderRadius: 999,
                    transition: "width 0.3s ease",
                  }}
                />
              </div>
            </div>
            <div
              style={{
                border: "1px solid #dcfce7",
                borderRadius: 18,
                padding: 20,
                background: "linear-gradient(135deg, rgba(34,197,94,0.12), rgba(21,128,61,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 12,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#047857" }}>
                Backfilled symbols
              </span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#0f172a" }}>
                {websocketSummary.ready.toLocaleString()} / {websocketSummary.total.toLocaleString()}
              </span>
              <span style={{ fontSize: 12, color: "#0f172a" }}>
                Watch {websocketSummary.watch.toLocaleString()} • Needs attention{" "}
                {websocketSummary.issues.toLocaleString()}
              </span>
            </div>
            <div
              style={{
                border: "1px solid #fee2e2",
                borderRadius: 18,
                padding: 20,
                background: "linear-gradient(135deg, rgba(248,113,113,0.14), rgba(220,38,38,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 10,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#b91c1c" }}>
                Attention queue
              </span>
              <span style={{ fontSize: 26, fontWeight: 700, color: "#7f1d1d" }}>
                {(websocketSummary.watch + websocketSummary.issues).toLocaleString()} symbol
                {websocketSummary.watch + websocketSummary.issues === 1 ? "" : "s"}
              </span>
              {websocketSummary.attention.length ? (
                <ul
                  style={{
                    listStyle: "none",
                    margin: 0,
                    padding: 0,
                    display: "flex",
                    flexDirection: "column",
                    gap: 8,
                  }}
                >
                  {websocketSummary.attention.map((item) => {
                    const palette = readinessPalette[item.status];
                    return (
                      <li
                        key={item.symbol}
                        style={{
                          display: "flex",
                          flexDirection: "column",
                          gap: 2,
                        }}
                      >
                        <div
                          style={{
                            display: "flex",
                            justifyContent: "space-between",
                            fontWeight: 600,
                            color: "#0f172a",
                          }}
                        >
                          <span>{item.symbol}</span>
                          <span>{item.minCoveragePct.toFixed(1)}%</span>
                        </div>
                        <div style={{ fontSize: 11, color: palette.color }}>
                          {palette.label} • {item.driftLabel}
                        </div>
                      </li>
                    );
                  })}
                </ul>
              ) : symbolWebsocketStats.length ? (
                <span style={{ fontSize: 12, color: "#7f1d1d" }}>
                  All tracked symbols meet the ready criteria.
                </span>
              ) : (
                <span style={{ fontSize: 12, color: "#7f1d1d" }}>
                  Load coverage data to populate websocket readiness metrics.
                </span>
              )}
            </div>
          </div>

          <div
            style={{
              border: "1px solid #e2e8f0",
              borderRadius: 20,
              background: "#ffffff",
              boxShadow: "0 14px 32px rgba(15, 23, 42, 0.12)",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                padding: "18px 22px",
                background: "#f8fafc",
                borderBottom: "1px solid #e2e8f0",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: 12,
              }}
            >
              <span style={{ fontWeight: 600, fontSize: 16, color: "#0f172a" }}>
                Symbol stream readiness
              </span>
              <span style={{ fontSize: 12, color: "#475569" }}>
                Tracking {symbolWebsocketStats.length.toLocaleString()} symbols across{" "}
                {visibleCount.toLocaleString()} backfill rows.
              </span>
            </div>
            {symbolWebsocketStats.length ? (
              <div style={{ maxHeight: 420, overflowX: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    minWidth: 720,
                  }}
                >
                  <thead>
                    <tr style={{ background: "#eef2ff" }}>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Symbol
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Status
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Coverage
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Freshness
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Active streams
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Timeframes
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {symbolWebsocketStats.map((stat) => {
                      const palette = readinessPalette[stat.status];
                      const rowBackground =
                        stat.status === "ready"
                          ? "#ffffff"
                          : stat.status === "watch"
                            ? "#fff7ed"
                            : "#fef2f2";
                      return (
                        <tr
                          key={stat.symbol}
                          style={{
                            borderBottom: "1px solid #f1f5f9",
                            background: rowBackground,
                            transition: "background 0.2s ease",
                          }}
                        >
                          <td
                            style={{
                              padding: "12px 16px",
                              fontWeight: 600,
                              color: "#0f172a",
                            }}
                          >
                            {stat.symbol}
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            <span
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                padding: "4px 10px",
                                borderRadius: 999,
                                background: palette.background,
                                color: palette.color,
                                fontSize: 12,
                                fontWeight: 600,
                              }}
                            >
                              {palette.label}
                            </span>
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            <div style={{ fontWeight: 600, color: "#0f172a" }}>
                              {stat.minCoveragePct.toFixed(1)}%
                            </div>
                            <div style={{ fontSize: 11, color: "#475569" }}>
                              weighted {stat.aggregateCoveragePct.toFixed(1)}%
                            </div>
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            <div
                              style={{
                                fontWeight: 600,
                                color:
                                  stat.driftMs === Number.POSITIVE_INFINITY
                                    ? "#b91c1c"
                                    : "#0f172a",
                              }}
                            >
                              {stat.driftLabel}
                            </div>
                            <div style={{ fontSize: 11, color: "#64748b" }}>{stat.latestLabel}</div>
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            <div style={{ fontWeight: 600, color: "#0f172a" }}>
                              {stat.activeStreams}
                            </div>
                            <div style={{ fontSize: 11, color: "#475569" }}>
                              of {stat.totalTimeframes}
                            </div>
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            {stat.timeframes.length ? (
                              <div
                                style={{
                                  display: "flex",
                                  flexWrap: "wrap",
                                  gap: 8,
                                }}
                              >
                                {stat.timeframes.map((tf) => (
                                  <span
                                    key={`${stat.symbol}-${tf.timeframe}`}
                                    title={`${tf.timeframe.toUpperCase()} • ${tf.coveragePct.toFixed(
                                      1,
                                    )}% • latest ${tf.latestLabel}`}
                                    style={{
                                      display: "inline-flex",
                                      alignItems: "center",
                                      gap: 6,
                                      padding: "4px 8px",
                                      borderRadius: 999,
                                      border: tf.active ? "1px solid #2563eb" : "1px solid #e2e8f0",
                                      background: tf.active ? "rgba(37,99,235,0.12)" : "#f8fafc",
                                      fontSize: 11,
                                      fontWeight: 600,
                                      color: tf.active ? "#1e3a8a" : "#475569",
                                    }}
                                  >
                                    <span
                                      style={{
                                        width: 6,
                                        height: 6,
                                        borderRadius: "999px",
                                        background: tf.active ? "#2563eb" : "#94a3b8",
                                      }}
                                    />
                                    <span>{tf.timeframe || "-"}</span>
                                    <span>{tf.coveragePct.toFixed(1)}%</span>
                                  </span>
                                ))}
                              </div>
                            ) : (
                              <span style={{ fontSize: 12, color: "#9ca3af" }}>No timeframe data</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div style={{ padding: "22px", fontSize: 13, color: "#475569" }}>
                No websocket readiness data yet. Refresh coverage on the backfills tab or select
                additional symbols/timeframes to populate this view.
              </div>
            )}
          </div>

          <span style={{ fontSize: 12, color: "#64748b" }}>
            Tip: hit Refresh in the backfills tab whenever you change symbol selections. The
            websocket view inherits those filters so you can validate stream capacity before going
            live.
          </span>
        </div>
      ) : (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 24,
          }}
        >
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <h2 style={{ margin: 0, fontSize: 26 }}>Data Validation Console</h2>
            <p style={{ margin: 0, color: "#475569", fontSize: 14, maxWidth: 760 }}>
              Audit the combined backfill + websocket footprint. Symbols appear here once at least
              one candle exists in the database so you can confirm volume, coverage, and live
              freshness.
            </p>
            <span style={{ fontSize: 12, color: "#64748b" }}>
              Coverage reflects the ratio between required and received candles; freshness shows the
              lag since the last write. Websocket connection is sourced from the active ingestion
              tracker.
            </span>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
              gap: 16,
            }}
          >
            <div
              style={{
                border: "1px solid #e2e8f0",
                borderRadius: 16,
                padding: 18,
                background: "linear-gradient(135deg, rgba(148,163,184,0.16), rgba(148,163,184,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 6,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#1f2937" }}>Tracked pairs</span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#0f172a" }}>
                {validationSummary.total.toLocaleString()}
              </span>
              <span style={{ fontSize: 12, color: "#475569" }}>
                Symbols/timeframes with &gt;= 1 stored candle.
              </span>
            </div>
            <div
              style={{
                border: "1px solid #dcfce7",
                borderRadius: 16,
                padding: 18,
                background: "linear-gradient(135deg, rgba(34,197,94,0.16), rgba(21,128,61,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 6,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#047857" }}>Healthy</span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#0f172a" }}>
                {validationSummary.good.toLocaleString()}
              </span>
              <span style={{ fontSize: 12, color: "#047857" }}>
                Coverage ok &amp; websocket writing within {freshnessReadyMinutes} min.
              </span>
            </div>
            <div
              style={{
                border: "1px solid #fef3c7",
                borderRadius: 16,
                padding: 18,
                background: "linear-gradient(135deg, rgba(251,191,36,0.18), rgba(217,119,6,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 6,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#b45309" }}>Stale</span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#0f172a" }}>
                {validationSummary.stale.toLocaleString()}
              </span>
              <span style={{ fontSize: 12, color: "#b45309" }}>
                Websocket delayed beyond {freshnessReadyMinutes} min but under {freshnessWarnMinutes} min.
              </span>
            </div>
            <div
              style={{
                border: "1px solid #fee2e2",
                borderRadius: 16,
                padding: 18,
                background: "linear-gradient(135deg, rgba(248,113,113,0.16), rgba(220,38,38,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 6,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#b91c1c" }}>Missing</span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#0f172a" }}>
                {validationSummary.missing.toLocaleString()}
              </span>
              <span style={{ fontSize: 12, color: "#b91c1c" }}>
                No recent inserts - investigate backfill gaps or websocket health.
              </span>
            </div>
            <div
              style={{
                border: "1px solid #e2e8f0",
                borderRadius: 16,
                padding: 18,
                background: "linear-gradient(135deg, rgba(148,163,184,0.18), rgba(148,163,184,0.08))",
                display: "flex",
                flexDirection: "column",
                gap: 6,
              }}
            >
              <span style={{ fontSize: 12, fontWeight: 600, color: "#475569" }}>No connection</span>
              <span style={{ fontSize: 28, fontWeight: 700, color: "#0f172a" }}>
                {validationSummary.noConnection.toLocaleString()}
              </span>
              <span style={{ fontSize: 12, color: "#475569" }}>
                Symbols with no active websocket session detected.
              </span>
            </div>
          </div>

          <div
            style={{
              border: "1px solid #e2e8f0",
              borderRadius: 20,
              background: "#ffffff",
              boxShadow: "0 14px 32px rgba(15, 23, 42, 0.12)",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                padding: "16px 20px",
                background: "#f8fafc",
                borderBottom: "1px solid #e2e8f0",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: 12,
              }}
            >
              <span style={{ fontWeight: 600, fontSize: 16, color: "#0f172a" }}>
                Symbol coverage &amp; freshness
              </span>
              <span style={{ fontSize: 12, color: "#475569" }}>
                {validationRows.length
                  ? `Monitoring ${validationRows.length.toLocaleString()} symbol/timeframe pairs.`
                  : "Load coverage on the backfills tab to populate these metrics."}
              </span>
            </div>
            {validationRows.length ? (
              <div style={{ maxHeight: 520, overflow: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    minWidth: 820,
                  }}
                >
                  <thead>
                    <tr style={{ background: "#eef2ff" }}>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Symbol
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Timeframe
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "right", fontSize: 12, color: "#475569" }}>
                        Required
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "right", fontSize: 12, color: "#475569" }}>
                        Received
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Coverage
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Latest update
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        WS connection
                      </th>
                      <th style={{ padding: "12px 16px", textAlign: "left", fontSize: 12, color: "#475569" }}>
                        Latest data entry age
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {validationRows.map((row) => {
                      const palette = validationPalette[row.status];
                      const rowBackground =
                        row.status === "good"
                          ? "#ffffff"
                          : row.status === "stale"
                            ? "#fff7ed"
                            : "#fef2f2";
                      const coverageColor =
                        row.coveragePct >= 99
                          ? "#22c55e"
                          : row.coveragePct >= 90
                            ? "#f97316"
                            : "#ef4444";
                      return (
                        <tr
                          key={row.id}
                          style={{
                            borderBottom: "1px solid #f1f5f9",
                            background: rowBackground,
                          }}
                        >
                          <td style={{ padding: "12px 16px", fontWeight: 600, color: "#0f172a" }}>
                            {row.symbol}
                          </td>
                          <td style={{ padding: "12px 16px", color: "#1f2937", fontWeight: 500 }}>
                            {row.timeframe}
                          </td>
                          <td style={{ padding: "12px 16px", textAlign: "right", color: "#0f172a" }}>
                            {row.required.toLocaleString()}
                          </td>
                          <td style={{ padding: "12px 16px", textAlign: "right", color: "#0f172a" }}>
                            {row.received.toLocaleString()}
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                              <span style={{ fontWeight: 600, color: coverageColor }}>
                                {row.coveragePct.toFixed(1)}%
                              </span>
                              <div
                                style={{
                                  width: "100%",
                                  height: 6,
                                  borderRadius: 999,
                                  background: "#e2e8f0",
                                  overflow: "hidden",
                                }}
                              >
                                <div
                                  style={{
                                    width: `${Math.min(100, row.coveragePct).toFixed(1)}%`,
                                    height: "100%",
                                    background: coverageColor,
                                  }}
                                />
                              </div>
                            </div>
                          </td>
                          <td style={{ padding: "12px 16px", color: "#1f2937", fontSize: 12 }}>
                            {row.latestLabel}
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            <span
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                padding: "4px 10px",
                                borderRadius: 999,
                                background: row.websocketActive
                                  ? "rgba(37, 99, 235, 0.14)"
                                  : "rgba(148, 163, 184, 0.18)",
                                color: row.websocketActive ? "#1e3a8a" : "#475569",
                                fontSize: 12,
                                fontWeight: 600,
                              }}
                            >
                              {row.websocketActive ? "Active" : "Offline"}
                            </span>
                          </td>
                          <td style={{ padding: "12px 16px" }}>
                            <span
                              style={{
                                display: "inline-flex",
                                alignItems: "center",
                                padding: "4px 10px",
                                borderRadius: 999,
                                background: palette.background,
                                color: palette.color,
                                fontSize: 12,
                                fontWeight: 600,
                              }}
                            >
                              {palette.label} · {row.freshnessLabel}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div style={{ padding: "20px", fontSize: 13, color: "#475569" }}>
                No candles loaded yet. Use the backfills tab to fetch coverage, then return here to
                validate the live footprint.
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
