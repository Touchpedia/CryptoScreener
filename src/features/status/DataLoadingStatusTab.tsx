import React, {
  Fragment,
  JSX,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

const isWindowDefined = typeof window !== "undefined";
const USE_MOCK = isWindowDefined && (window as any).USE_MOCK === true;
const USE_SSE = isWindowDefined && (window as any).USE_SSE === true;

type TimeframeKey = "1m" | "3m" | "5m";
type DashboardState = "RUNNING" | "PAUSED" | "STOPPED";

type PairRow = {
  pair: string;
  tf: Record<TimeframeKey, number>;
  total_pct: number;
  gaps_filled: number;
  status: "Active" | "Done" | "Slow" | "Pending" | "Failed";
  last_update: string;
};

type StatusSummary = {
  active_threads: number;
  completed: number;
  total: number;
  req_rate: number;
  throttle: { min: number; max: number; current: number };
  last_error?: string;
  queue_len: number;
  db: "connected" | "retrying" | "down";
  backend: "ok" | "degraded" | "down";
};

type StatusPayload = {
  pairs: PairRow[];
  summary: StatusSummary;
  server_time: string;
};

type SettingsPayload = {
  auto_refresh: boolean;
  interval: number;
  threads: number;
  throttle_min: number;
  throttle_max: number;
  retries: number;
  backoff_ms: number;
  mode: "live" | "backfill";
  date_from?: string;
  date_to?: string;
  queue_mode: "round_robin" | "priority";
  notifications: boolean;
  sound?: boolean;
  persist: boolean;
  timeframes: TimeframeKey[];
};

type ControlAction = "start" | "pause" | "stop" | "resume";

type ControlResponse = {
  ok: boolean;
  state: DashboardState;
  message?: string;
};

type SortOption = "TOTAL_DESC" | "PAIR_ASC" | "LAST_UPDATE_DESC" | "STATUS_ASC";

type StatusFilter = PairRow["status"];

type ToastKind = "success" | "error" | "info";

type Toast = {
  id: number;
  message: string;
  kind: ToastKind;
};

type PairHistoryEntry = {
  ts: number;
  total_pct: number;
  status: PairRow["status"];
  tf: PairRow["tf"];
};

const LOCAL_STORAGE_KEY = "data-loading-status-settings";
const STATUS_OPTIONS: StatusFilter[] = ["Active", "Done", "Slow", "Pending", "Failed"];
const TIMEFRAME_OPTIONS: TimeframeKey[] = ["1m", "3m", "5m"];
const STATUS_SORT_WEIGHT: Record<StatusFilter, number> = {
  Active: 1,
  Slow: 2,
  Pending: 3,
  Done: 4,
  Failed: 5,
};

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

function fmtPct(n: number): string {
  return `${n.toFixed(1)}%`;
}

function cn(...classes: Array<string | false | null | undefined>): string {
  return classes.filter(Boolean).join(" ");
}

const defaultSettings: SettingsPayload = {
  auto_refresh: true,
  interval: 5,
  threads: 7,
  throttle_min: 200,
  throttle_max: 1500,
  retries: 3,
  backoff_ms: 500,
  mode: "live",
  queue_mode: "round_robin",
  notifications: true,
  sound: false,
  persist: false,
  timeframes: TIMEFRAME_OPTIONS,
};

function readPersistedSettings(): SettingsPayload | null {
  if (!isWindowDefined) {
    return null;
  }
  try {
    const raw = window.localStorage.getItem(LOCAL_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw);
    return { ...defaultSettings, ...parsed };
  } catch {
    return null;
  }
}

function writePersistedSettings(settings: SettingsPayload): void {
  if (!isWindowDefined) {
    return;
  }
  try {
    window.localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(settings));
  } catch {
    /* ignore write errors */
  }
}

function removePersistedSettings(): void {
  if (!isWindowDefined) {
    return;
  }
  try {
    window.localStorage.removeItem(LOCAL_STORAGE_KEY);
  } catch {
    /* ignore */
  }
}

async function fetchJson<T>(input: RequestInfo, init?: RequestInit): Promise<T> {
  const res = await fetch(input, init);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json() as Promise<T>;
}

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function generateMockPairs(count = 120): PairRow[] {
  const statuses: StatusFilter[] = ["Active", "Done", "Slow", "Pending", "Failed"];
  const pairs: PairRow[] = [];
  for (let i = 0; i < count; i += 1) {
    const basePct = Math.min(100, Math.random() * 100);
    pairs.push({
      pair: `PAIR_${i.toString().padStart(3, "0")}/USDT`,
      tf: {
        "1m": clamp(basePct + Math.random() * 10 - 5, 0, 100),
        "3m": clamp(basePct + Math.random() * 10 - 5, 0, 100),
        "5m": clamp(basePct + Math.random() * 10 - 5, 0, 100),
      },
      total_pct: clamp(basePct + Math.random() * 5, 0, 100),
      gaps_filled: Math.floor(Math.random() * 20),
      status: statuses[i % statuses.length] ?? "Pending",
      last_update: new Date(Date.now() - Math.random() * 60000).toISOString(),
    });
  }
  return pairs;
}

function buildMockSummary(pairs: PairRow[]): StatusSummary {
  const active = pairs.filter((p) => p.status === "Active").length;
  const done = pairs.filter((p) => p.status === "Done").length;
  return {
    active_threads: Math.min(active, 7),
    completed: done,
    total: pairs.length,
    req_rate: 240 + Math.random() * 60,
    throttle: { min: 200, max: 1500, current: 320 + Math.random() * 80 },
    last_error: Math.random() > 0.8 ? "Timeout while fetching candles" : undefined,
    queue_len: Math.floor(Math.random() * 25),
    db: "connected",
    backend: Math.random() > 0.9 ? "degraded" : "ok",
  };
}

async function mockFetchStatus(): Promise<StatusPayload> {
  await wait(300);
  const pairs = generateMockPairs(220);
  return {
    pairs,
    summary: buildMockSummary(pairs),
    server_time: new Date().toISOString(),
  };
}

async function mockFetchSettings(): Promise<SettingsPayload> {
  await wait(150);
  return { ...defaultSettings, persist: true };
}

async function mockUpdateSettings(_settings: SettingsPayload): Promise<{ ok: true }> {
  await wait(120);
  return { ok: true };
}

async function mockControl(action: ControlAction): Promise<ControlResponse> {
  await wait(180);
  let state: DashboardState = "RUNNING";
  if (action === "pause") {
    state = "PAUSED";
  } else if (action === "stop") {
    state = "STOPPED";
  } else if (action === "resume") {
    state = "RUNNING";
  }
  return { ok: true, state };
}

function useToasts() {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const idRef = useRef(0);

  const push = useCallback((message: string, kind: ToastKind = "info") => {
    idRef.current += 1;
    const toast: Toast = { id: idRef.current, message, kind };
    setToasts((prev) => [...prev, toast]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== toast.id));
    }, 3500);
  }, []);

  const dismiss = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  return { toasts, push, dismiss };
}

function formatDateTime(iso?: string): string {
  if (!iso) return "--";
  try {
    const dt = new Date(iso);
    return dt.toLocaleTimeString([], { hour12: false });
  } catch {
    return iso;
  }
}

function isInputLike(target: EventTarget | null): target is HTMLElement {
  if (!(target instanceof HTMLElement)) return false;
  const el = target as HTMLElement;
  const tag = el.tagName.toLowerCase();
  return (
    tag === "input" ||
    tag === "textarea" ||
    tag === "select" ||
    el.isContentEditable
  );
}

function ProgressBar({
  value,
  className,
}: {
  value: number;
  className?: string;
}) {
  return (
    <div className={cn("h-2 w-full rounded-full bg-slate-200 dark:bg-slate-700", className)}>
      <div
        className="h-2 rounded-full bg-indigo-500 transition-all duration-500 ease-out"
        style={{ width: `${clamp(value, 0, 100)}%` }}
      />
    </div>
  );
}

function StatusBadge({ status }: { status: StatusFilter }) {
  const color =
    status === "Active"
      ? "bg-blue-500/15 text-blue-600 dark:text-blue-300"
      : status === "Done"
      ? "bg-emerald-500/15 text-emerald-600 dark:text-emerald-300"
      : status === "Slow"
      ? "bg-amber-500/15 text-amber-600 dark:text-amber-300"
      : status === "Failed"
      ? "bg-rose-500/15 text-rose-600 dark:text-rose-300"
      : "bg-slate-500/15 text-slate-600 dark:text-slate-300";
  return (
    <span className={cn("inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium", color)}>
      {status}
    </span>
  );
}

function Toggle({
  value,
  onChange,
  label,
  disabled,
}: {
  value: boolean;
  onChange: (next: boolean) => void;
  label?: string;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={() => onChange(!value)}
      disabled={disabled}
      className={cn(
        "inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium transition-colors",
        value
          ? "border-indigo-500 bg-indigo-500/10 text-indigo-600 dark:text-indigo-300"
          : "border-slate-300 bg-white text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-300"
      )}
    >
      <span className="h-2.5 w-2.5 rounded-full bg-current" />
      {label}
    </button>
  );
}

function NumberInput({
  value,
  onChange,
  min,
  max,
  step = 1,
  className,
}: {
  value: number;
  onChange: (next: number) => void;
  min?: number;
  max?: number;
  step?: number;
  className?: string;
}) {
  return (
    <input
      type="number"
      className={cn(
        "h-9 w-20 rounded-md border border-slate-300 bg-white px-2 text-sm text-slate-700 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200",
        className
      )}
      min={min}
      max={max}
      step={step}
      value={Number.isFinite(value) ? value : ""}
      onChange={(e) => onChange(e.target.value === "" ? 0 : Number(e.target.value))}
    />
  );
}

function RangeInput({
  value,
  onChange,
  min,
  max,
}: {
  value: number;
  onChange: (next: number) => void;
  min: number;
  max: number;
}) {
  return (
    <input
      type="range"
      min={min}
      max={max}
      value={value}
      onChange={(e) => onChange(Number(e.target.value))}
      className="h-2 w-40 cursor-pointer rounded-full accent-indigo-500"
    />
  );
}

function Dropdown<T extends string>({
  value,
  onChange,
  options,
  label,
}: {
  value: T;
  onChange: (next: T) => void;
  options: Array<{ label: string; value: T }>;
  label?: string;
}) {
  return (
    <label className="flex items-center gap-1 text-xs font-medium text-slate-600 dark:text-slate-300">
      {label}
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as T)}
        className="h-9 rounded-md border border-slate-300 bg-white px-2 text-sm text-slate-700 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
      >
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function PairDrawer({
  row,
  history,
  onClose,
}: {
  row: PairRow | null;
  history: PairHistoryEntry[];
  onClose: () => void;
}) {
  return (
    <div
      className={cn(
        "fixed inset-0 z-40 flex transition-opacity",
        row ? "pointer-events-auto" : "pointer-events-none opacity-0"
      )}
      aria-hidden={!row}
    >
      <div
        className={cn(
          "absolute inset-0 bg-slate-900/30 backdrop-blur-sm transition-opacity",
          row ? "opacity-100" : "opacity-0"
        )}
        onClick={onClose}
      />
      <aside
        className={cn(
          "ml-auto flex h-full w-full max-w-md flex-col gap-6 overflow-y-auto border-l border-slate-200 bg-white p-6 shadow-xl transition-transform dark:border-slate-700 dark:bg-slate-900",
          row ? "translate-x-0" : "translate-x-full"
        )}
      >
        {row ? (
          <>
            <header className="flex items-start justify-between">
              <div>
                <h2 className="text-lg font-semibold text-slate-800 dark:text-slate-100">
                  {row.pair}
                </h2>
                <p className="text-xs text-slate-500 dark:text-slate-400">
                  Last update {formatDateTime(row.last_update)}
                </p>
              </div>
              <button
                type="button"
                onClick={onClose}
                className="rounded-md border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-800"
              >
                Close
              </button>
            </header>

            <section className="space-y-3">
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">
                Progress by timeframe
              </h3>
              {TIMEFRAME_OPTIONS.map((tf) => (
                <div key={tf} className="space-y-1">
                  <div className="flex justify-between text-xs text-slate-500 dark:text-slate-400">
                    <span>{tf}</span>
                    <span>{fmtPct(row.tf[tf])}</span>
                  </div>
                  <ProgressBar value={row.tf[tf]} />
                </div>
              ))}
              <div className="space-y-1">
                <div className="flex justify-between text-xs text-slate-500 dark:text-slate-400">
                  <span>Total</span>
                  <span>{fmtPct(row.total_pct)}</span>
                </div>
                <ProgressBar value={row.total_pct} />
              </div>
            </section>

            <section className="space-y-2">
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">
                Recent history
              </h3>
              <div className="max-h-48 space-y-2 overflow-y-auto rounded-md border border-slate-200 p-2 dark:border-slate-600">
                {history.length === 0 ? (
                  <p className="text-xs text-slate-500 dark:text-slate-400">No history captured yet.</p>
                ) : (
                  history
                    .slice(-12)
                    .reverse()
                    .map((entry) => (
                      <div key={entry.ts} className="flex items-center justify-between text-xs text-slate-500 dark:text-slate-300">
                        <span>{new Date(entry.ts).toLocaleTimeString([], { hour12: false })}</span>
                        <span className="flex items-center gap-2">
                          <StatusBadge status={entry.status} />
                          {fmtPct(entry.total_pct)}
                        </span>
                      </div>
                    ))
                )}
              </div>
            </section>

            <section className="space-y-2">
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-200">
                Diagnostics snapshot
              </h3>
              <dl className="grid grid-cols-2 gap-2 text-xs text-slate-600 dark:text-slate-300">
                <div>
                  <dt className="font-medium text-slate-500 dark:text-slate-400">Gaps filled</dt>
                  <dd>{row.gaps_filled}</dd>
                </div>
                <div>
                  <dt className="font-medium text-slate-500 dark:text-slate-400">Status</dt>
                  <dd>
                    <StatusBadge status={row.status} />
                  </dd>
                </div>
              </dl>
            </section>
          </>
        ) : null}
      </aside>
    </div>
  );
}

type VirtualizedRows = {
  items: PairRow[];
  offset: number;
  totalHeight: number;
};

function useVirtualRows(
  rows: PairRow[],
  enabled: boolean,
  rowHeight = 60
): [VirtualizedRows, (node: HTMLDivElement | null) => void] {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [height, setHeight] = useState(0);

  useEffect(() => {
    if (!enabled) return;
    const element = containerRef.current;
    if (!element) return;
    const handleScroll = () => {
      setScrollTop(element.scrollTop);
    };
    const handleResize = () => {
      setHeight(element.clientHeight);
    };
    handleResize();
    element.addEventListener("scroll", handleScroll, { passive: true });
    window.addEventListener("resize", handleResize);
    return () => {
      element.removeEventListener("scroll", handleScroll);
      window.removeEventListener("resize", handleResize);
    };
  }, [enabled]);

  const visibleCount = height ? Math.ceil(height / rowHeight) + 6 : 20;
  const startIndex = enabled ? Math.max(0, Math.floor(scrollTop / rowHeight) - 3) : 0;
  const endIndex = enabled ? Math.min(rows.length, startIndex + visibleCount) : rows.length;
  const items = rows.slice(startIndex, endIndex);

  const virtualized: VirtualizedRows = enabled
    ? {
        items,
        offset: startIndex * rowHeight,
        totalHeight: rows.length * rowHeight,
      }
    : { items: rows, offset: 0, totalHeight: rows.length * rowHeight };

  const setRef = useCallback((node: HTMLDivElement | null) => {
    containerRef.current = node;
  }, []);

  return [virtualized, setRef];
}

export function DataLoadingStatusTab(): JSX.Element {
  const { toasts, push, dismiss } = useToasts();
  const [pairs, setPairs] = useState<PairRow[]>([]);
  const [summary, setSummary] = useState<StatusSummary | null>(null);
  const [serverTime, setServerTime] = useState<string>();
  const [dashboardState, setDashboardState] = useState<DashboardState>("STOPPED");
  const [settings, setSettings] = useState<SettingsPayload>(defaultSettings);
  const [sortOption, setSortOption] = useState<SortOption>("TOTAL_DESC");
  const [pairSearch, setPairSearch] = useState("");
  const [debouncedPair, setDebouncedPair] = useState("");
  const [statusFilters, setStatusFilters] = useState<StatusFilter[]>(STATUS_OPTIONS);
  const [isLoading, setIsLoading] = useState(true);
  const [isSavingSettings, setIsSavingSettings] = useState(false);
  const [isDarkMode, setIsDarkMode] = useState(() =>
    isWindowDefined ? document.documentElement.classList.contains("dark") : false
  );
  const [drawerRow, setDrawerRow] = useState<PairRow | null>(null);
  const historyRef = useRef<Map<string, PairHistoryEntry[]>>(new Map());
  const pollingRef = useRef<number>();
  const mountedRef = useRef(false);
  const settingsReadyRef = useRef(false);
  const lastSettingsSentRef = useRef<SettingsPayload | null>(null);

  const filteredPairs = useMemo(() => {
    const search = debouncedPair.trim().toLowerCase();
    const timeframeSet = new Set(settings.timeframes);
    return pairs
      .filter((row) => {
        if (search && !row.pair.toLowerCase().includes(search)) {
          return false;
        }
        if (!statusFilters.includes(row.status)) {
          return false;
        }
        if (timeframeSet.size === TIMEFRAME_OPTIONS.length) {
          return true;
        }
        return Array.from(timeframeSet).some((tf) => row.tf[tf as TimeframeKey] > 0);
      })
      .sort((a, b) => {
        switch (sortOption) {
          case "PAIR_ASC":
            return a.pair.localeCompare(b.pair);
          case "LAST_UPDATE_DESC":
            return (
              new Date(b.last_update).getTime() -
              new Date(a.last_update).getTime()
            );
          case "STATUS_ASC":
            return STATUS_SORT_WEIGHT[a.status] - STATUS_SORT_WEIGHT[b.status];
          case "TOTAL_DESC":
          default:
            return b.total_pct - a.total_pct;
        }
      });
  }, [pairs, debouncedPair, statusFilters, sortOption, settings.timeframes]);

  const [virtualized, setTableRef] = useVirtualRows(
    filteredPairs,
    filteredPairs.length > 200
  );

  const visibleRows = virtualized.items;

  const loadStatus = useCallback(async () => {
    try {
      const payload = USE_MOCK
        ? await mockFetchStatus()
        : await fetchJson<StatusPayload>("/api/status");
      setPairs(payload.pairs);
      setSummary(payload.summary);
      setServerTime(payload.server_time);
      payload.pairs.forEach((row) => {
        const list = historyRef.current.get(row.pair) ?? [];
        list.push({
          ts: Date.now(),
          total_pct: row.total_pct,
          status: row.status,
          tf: row.tf,
        });
        historyRef.current.set(row.pair, list.slice(-48));
      });
      setIsLoading(false);
    } catch (error) {
      setIsLoading(false);
      push(
        error instanceof Error ? error.message : "Failed to load status",
        "error"
      );
    }
  }, [push]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (pollingRef.current) {
        window.clearInterval(pollingRef.current);
      }
    };
  }, []);

  useEffect(() => {
    const handle = window.setTimeout(() => {
      setDebouncedPair(pairSearch);
    }, 400);
    return () => window.clearTimeout(handle);
  }, [pairSearch]);

  const applySettings = useCallback(
    (next: SettingsPayload, persistOnly = false) => {
      const clamped: SettingsPayload = {
        ...next,
        interval: clamp(next.interval, 1, 60),
        threads: clamp(next.threads, 1, 16),
        throttle_min: Math.max(0, next.throttle_min),
        throttle_max: Math.max(next.throttle_min + 10, next.throttle_max),
        retries: Math.max(0, Math.floor(next.retries)),
        backoff_ms: Math.max(0, next.backoff_ms),
        timeframes: next.timeframes.length ? next.timeframes : TIMEFRAME_OPTIONS,
      };
      setSettings(clamped);
      if (clamped.persist) {
        writePersistedSettings(clamped);
      } else if (persistOnly) {
        removePersistedSettings();
      }
      return clamped;
    },
    []
  );

  useEffect(() => {
    const hydrate = async () => {
      let initial = defaultSettings;
      const saved = readPersistedSettings();
      if (saved) {
        initial = { ...initial, ...saved };
      }
      applySettings(initial, true);
      try {
        const remote = USE_MOCK
          ? await mockFetchSettings()
          : await fetchJson<SettingsPayload>("/api/settings");
        settingsReadyRef.current = true;
        const merged = { ...initial, ...remote };
        applySettings(merged);
        lastSettingsSentRef.current = merged;
      } catch (error) {
        settingsReadyRef.current = true;
        push(
          error instanceof Error ? error.message : "Failed to load settings",
          "error"
        );
      } finally {
        loadStatus();
      }
    };
    hydrate();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!settingsReadyRef.current) {
      return;
    }
    if (!settings.persist) {
      removePersistedSettings();
    }
    const timeout = window.setTimeout(async () => {
      if (!mountedRef.current) return;
      const lastSent = lastSettingsSentRef.current;
      if (lastSent && JSON.stringify(lastSent) === JSON.stringify(settings)) {
        return;
      }
      setIsSavingSettings(true);
      try {
        if (USE_MOCK) {
          await mockUpdateSettings(settings);
        } else {
          await fetchJson<{ ok: true }>("/api/settings", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(settings),
          });
        }
        lastSettingsSentRef.current = settings;
        if (settings.notifications) {
          push("Settings updated", "success");
        }
      } catch (error) {
        push(
          error instanceof Error ? error.message : "Failed to save settings",
          "error"
        );
      } finally {
        setIsSavingSettings(false);
      }
    }, 600);
    return () => window.clearTimeout(timeout);
  }, [settings, push]);

  useEffect(() => {
    if (pollingRef.current) {
      window.clearInterval(pollingRef.current);
    }
    if (!settings.auto_refresh) {
      return;
    }
    const baseInterval = settings.interval * 1000;
    const tick = () => {
      if (document.visibilityState === "visible") {
        loadStatus();
      } else {
        setTimeout(loadStatus, Math.min(baseInterval * 2, 60000));
      }
    };
    pollingRef.current = window.setInterval(tick, baseInterval);
    return () => {
      if (pollingRef.current) {
        window.clearInterval(pollingRef.current);
      }
    };
  }, [settings.auto_refresh, settings.interval, loadStatus]);

  useEffect(() => {
    if (!USE_SSE || USE_MOCK) {
      return;
    }
    let eventSource: EventSource | null = null;
    try {
      eventSource = new EventSource("/api/status/stream");
      eventSource.onmessage = (event) => {
        try {
          const payload: StatusPayload = JSON.parse(event.data);
          setPairs(payload.pairs);
          setSummary(payload.summary);
          setServerTime(payload.server_time);
          payload.pairs.forEach((row) => {
            const list = historyRef.current.get(row.pair) ?? [];
            list.push({
              ts: Date.now(),
              total_pct: row.total_pct,
              status: row.status,
              tf: row.tf,
            });
            historyRef.current.set(row.pair, list.slice(-48));
          });
        } catch (err) {
          console.error("Failed to parse SSE payload", err);
        }
      };
      eventSource.onerror = (err) => {
        console.warn("SSE error", err);
        eventSource?.close();
      };
    } catch (err) {
      console.warn("Unable to open SSE", err);
    }
    return () => {
      eventSource?.close();
    };
  }, []);

  const handleControl = useCallback(
    async (action: ControlAction) => {
      try {
        const res = USE_MOCK
          ? await mockControl(action)
          : await fetchJson<ControlResponse>("/api/control", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ action }),
            });
        setDashboardState(res.state);
        if (!res.ok) {
          push(res.message || "Operation failed", "error");
        } else if (res.message) {
          push(res.message, "success");
        } else if (settings.notifications) {
          push(`${action.toUpperCase()} acknowledged`, "success");
        }
        loadStatus();
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "Control request failed";
        if (message.includes("409")) {
          push("Invalid transition (409)", "error");
        } else {
          push(message, "error");
        }
      }
    },
    [loadStatus, push, settings.notifications]
  );

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if (isInputLike(event.target)) {
        return;
      }
      if (!event.key) return;
      if (event.key === "r" || event.key === "R") {
        event.preventDefault();
        handleControl("start");
      } else if (event.key === "p" || event.key === "P") {
        event.preventDefault();
        if (dashboardState === "PAUSED") {
          handleControl("resume");
        } else {
          handleControl("pause");
        }
      } else if (event.key === "s" || event.key === "S") {
        event.preventDefault();
        handleControl("stop");
      } else if (event.key === "a" || event.key === "A") {
        event.preventDefault();
        applySettings({ ...settings, auto_refresh: !settings.auto_refresh });
      } else if (event.key === "[" && settings.interval > 1) {
        event.preventDefault();
        applySettings({
          ...settings,
          interval: clamp(settings.interval - 1, 1, 60),
        });
      } else if (event.key === "]" && settings.interval < 60) {
        event.preventDefault();
        applySettings({
          ...settings,
          interval: clamp(settings.interval + 1, 1, 60),
        });
      } else if (event.key === "f" || event.key === "F") {
        event.preventDefault();
        const input = document.getElementById("pair-search-input");
        if (input instanceof HTMLInputElement) {
          input.focus();
        }
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [applySettings, handleControl, settings, dashboardState]);

  useEffect(() => {
    if (!isWindowDefined) return;
    document.documentElement.classList.toggle("dark", isDarkMode);
  }, [isDarkMode]);

  const handleExport = useCallback(
    (format: "csv" | "json") => {
      const rows = filteredPairs;
      if (!rows.length) {
        push("No rows to export", "info");
        return;
      }
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
      if (format === "json") {
        const blob = new Blob([JSON.stringify(rows, null, 2)], {
          type: "application/json",
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `data-loading-status-${timestamp}.json`;
        a.click();
        URL.revokeObjectURL(url);
      } else {
        const header = [
          "pair",
          "1m",
          "3m",
          "5m",
          "total_pct",
          "gaps_filled",
          "status",
          "last_update",
        ];
        const lines = [header.join(",")];
        rows.forEach((row) => {
          lines.push(
            [
              row.pair,
              row.tf["1m"].toFixed(2),
              row.tf["3m"].toFixed(2),
              row.tf["5m"].toFixed(2),
              row.total_pct.toFixed(2),
              row.gaps_filled,
              row.status,
              row.last_update,
            ]
              .map((value) => `"${String(value).replace(/"/g, '""')}"`)
              .join(",")
          );
        });
        const blob = new Blob([lines.join("\n")], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `data-loading-status-${timestamp}.csv`;
        a.click();
        URL.revokeObjectURL(url);
      }
      push(`Exported ${rows.length} rows as ${format.toUpperCase()}`, "success");
    },
    [filteredPairs, push]
  );

  const toggleStatusFilter = (status: StatusFilter) => {
    setStatusFilters((prev) => {
      if (prev.includes(status)) {
        const next = prev.filter((item) => item !== status);
        return next.length ? next : prev;
      }
      return [...prev, status];
    });
  };

  const toggleTimeframe = (tf: TimeframeKey) => {
    applySettings({
      ...settings,
      timeframes: settings.timeframes.includes(tf)
        ? settings.timeframes.filter((item) => item !== tf)
        : [...settings.timeframes, tf],
    });
  };

  const handleClearStats = () => {
    historyRef.current.clear();
    push("Cleared local stats history", "info");
  };

  const activeThreadsLabel =
    summary && `${summary.active_threads} / ${settings.threads}`;
  const completedLabel =
    summary && `${summary.completed} / ${summary.total ?? pairs.length}`;
  const rateLabel = summary ? `${summary.req_rate.toFixed(1)} req/min` : "--";
  const throttleLabel = summary
    ? `${Math.round(summary.throttle.current)} ms (${summary.throttle.min}-${summary.throttle.max})`
    : "--";
  const queueLabel = summary ? `waiting: ${summary.queue_len}` : "--";
  const dbLabel = summary ? summary.db : "--";
  const backendLabel = summary ? summary.backend : "--";

  const lastErrorLabel = summary?.last_error ?? "None";
  const historyForDrawer =
    drawerRow && historyRef.current.get(drawerRow.pair)
      ? historyRef.current.get(drawerRow.pair)!
      : [];

  const statusChips: Array<{ label: string; value: string }> = [
    { label: `Active Threads ${activeThreadsLabel ?? "--"}`, value: "threads" },
    { label: `Completed ${completedLabel ?? "--"}`, value: "completed" },
    { label: `Req Rate ${rateLabel}`, value: "rate" },
    { label: `Throttle ${throttleLabel}`, value: "throttle" },
    { label: `Last Error ${lastErrorLabel}`, value: "error" },
    { label: `DB ${dbLabel}`, value: "db" },
    { label: `Backend ${backendLabel}`, value: "backend" },
    { label: `Queue ${queueLabel}`, value: "queue" },
  ];

  return (
    <div className="relative flex h-full flex-col gap-4 p-4">
      <div className="fixed inset-x-0 top-0 z-50 flex flex-col items-end gap-2 p-4">
        {toasts.map((toast) => (
          <div
            key={toast.id}
            className={cn(
              "flex items-center gap-3 rounded-md border px-4 py-2 text-sm shadow-lg",
              toast.kind === "success"
                ? "border-emerald-500 bg-emerald-500/10 text-emerald-700 dark:border-emerald-500/60 dark:bg-emerald-500/10 dark:text-emerald-200"
                : toast.kind === "error"
                ? "border-rose-500 bg-rose-500/10 text-rose-700 dark:border-rose-500/60 dark:bg-rose-500/10 dark:text-rose-200"
                : "border-slate-300 bg-white text-slate-700 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
            )}
          >
            <span>{toast.message}</span>
            <button
              type="button"
              onClick={() => dismiss(toast.id)}
              className="text-xs text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200"
            >
              x
            </button>
          </div>
        ))}
      </div>

      <header className="mt-6 space-y-4 rounded-lg border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-900">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="space-y-1">
            <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">
              Data Loading Status
            </h1>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Last sync: {serverTime ? formatDateTime(serverTime) : "--"}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => handleControl("start")}
              disabled={dashboardState === "RUNNING"}
              className="rounded-md bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white shadow-sm hover:bg-indigo-500 disabled:cursor-not-allowed disabled:bg-indigo-400"
            >
              Run
            </button>
            <button
              type="button"
              onClick={() => handleControl("pause")}
              disabled={dashboardState !== "RUNNING"}
              className="rounded-md border border-slate-300 px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:text-slate-400 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
            >
              Pause
            </button>
            <button
              type="button"
              onClick={() => handleControl("stop")}
              disabled={dashboardState === "STOPPED"}
              className="rounded-md border border-rose-400 px-3 py-1.5 text-sm font-medium text-rose-600 hover:bg-rose-50 disabled:cursor-not-allowed disabled:text-rose-300 dark:border-rose-500 dark:text-rose-300 dark:hover:bg-rose-500/10"
            >
              Stop
            </button>
            <button
              type="button"
              onClick={() => handleControl("resume")}
              disabled={dashboardState !== "PAUSED"}
              className="rounded-md border border-emerald-400 px-3 py-1.5 text-sm font-medium text-emerald-600 hover:bg-emerald-50 disabled:cursor-not-allowed disabled:text-emerald-300 dark:border-emerald-500 dark:text-emerald-300 dark:hover:bg-emerald-500/10"
            >
              Resume
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
          <div className="flex flex-wrap items-center gap-3">
            <Toggle
              label="Auto-Refresh"
              value={settings.auto_refresh}
              onChange={(next) => applySettings({ ...settings, auto_refresh: next })}
            />
            <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
              Interval (s)
              <NumberInput
                value={settings.interval}
                min={1}
                max={60}
                onChange={(val) =>
                  applySettings({ ...settings, interval: clamp(val, 1, 60) })
                }
              />
            </label>
            <span className="text-[10px] text-slate-400">(min 1 / max 60)</span>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <label className="flex flex-1 items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
              Pair
              <input
                id="pair-search-input"
                type="search"
                value={pairSearch}
                onChange={(event) => setPairSearch(event.target.value)}
                placeholder="Search pairs"
                className="h-9 flex-1 rounded-md border border-slate-300 bg-white px-2 text-sm text-slate-700 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
              />
            </label>
            <Dropdown<SortOption>
              value={sortOption}
              onChange={setSortOption}
              label="Sort"
              options={[
                { value: "TOTAL_DESC", label: "Total Progress (desc)" },
                { value: "PAIR_ASC", label: "Pair (A-Z)" },
                { value: "LAST_UPDATE_DESC", label: "Last Update" },
                { value: "STATUS_ASC", label: "Status" },
              ]}
            />
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs font-medium text-slate-600 dark:text-slate-300">
              Timeframes
            </span>
            {TIMEFRAME_OPTIONS.map((tf) => {
              const active = settings.timeframes.includes(tf);
              return (
                <button
                  key={tf}
                  type="button"
                  onClick={() => toggleTimeframe(tf)}
                  className={cn(
                    "rounded-full px-3 py-1 text-xs font-medium transition-colors",
                    active
                      ? "bg-indigo-500/15 text-indigo-600 dark:text-indigo-300"
                      : "bg-slate-100 text-slate-500 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300 dark:hover:bg-slate-700"
                  )}
                >
                  {tf}
                </button>
              );
            })}
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
              Concurrency
              <RangeInput
                value={settings.threads}
                min={1}
                max={16}
                onChange={(value) =>
                  applySettings({ ...settings, threads: clamp(value, 1, 16) })
                }
              />
              <span className="w-6 text-right text-xs text-slate-500 dark:text-slate-300">
                {settings.threads}
              </span>
            </label>
            <span className="text-[10px] text-slate-400">threads</span>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
              Throttle ms
              <NumberInput
                value={settings.throttle_min}
                min={0}
                onChange={(val) =>
                  applySettings({
                    ...settings,
                    throttle_min: Math.max(0, val),
                    throttle_max: Math.max(
                      settings.throttle_max,
                      Math.max(0, val) + 10
                    ),
                  })
                }
              />
              <span className="text-slate-400">-</span>
              <NumberInput
                value={settings.throttle_max}
                min={settings.throttle_min + 10}
                onChange={(val) =>
                  applySettings({
                    ...settings,
                    throttle_max: Math.max(settings.throttle_min + 10, val),
                  })
                }
              />
            </label>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
              Retries
              <NumberInput
                value={settings.retries}
                min={0}
                onChange={(val) =>
                  applySettings({
                    ...settings,
                    retries: Math.max(0, Math.round(val)),
                  })
                }
              />
            </label>
            <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
              Base Backoff (ms)
              <NumberInput
                value={settings.backoff_ms}
                min={0}
                onChange={(val) =>
                  applySettings({ ...settings, backoff_ms: Math.max(0, val) })
                }
              />
            </label>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Dropdown<"live" | "backfill">
              value={settings.mode}
              onChange={(mode) => applySettings({ ...settings, mode })}
              label="Scope"
              options={[
                { value: "live", label: "Live Only" },
                { value: "backfill", label: "Backfill + Live" },
              ]}
            />
            {settings.mode === "backfill" ? (
              <Fragment>
                <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
                  From
                  <input
                    type="date"
                    value={settings.date_from ?? ""}
                    onChange={(e) =>
                      applySettings({
                        ...settings,
                        date_from: e.target.value || undefined,
                      })
                    }
                    className="h-9 rounded-md border border-slate-300 bg-white px-2 text-sm text-slate-700 focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
                  />
                </label>
                <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
                  To
                  <input
                    type="date"
                    value={settings.date_to ?? ""}
                    onChange={(e) =>
                      applySettings({
                        ...settings,
                        date_to: e.target.value || undefined,
                      })
                    }
                    className="h-9 rounded-md border border-slate-300 bg-white px-2 text-sm text-slate-700 focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-200 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
                  />
                </label>
              </Fragment>
            ) : null}
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Dropdown<"round_robin" | "priority">
              value={settings.queue_mode}
              onChange={(queue_mode) =>
                applySettings({ ...settings, queue_mode })
              }
              label="Queue Mode"
              options={[
                { value: "round_robin", label: "Round-Robin" },
                { value: "priority", label: "Priority (active first)" },
              ]}
            />
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Toggle
              label="Notifications"
              value={settings.notifications}
              onChange={(next) =>
                applySettings({ ...settings, notifications: next })
              }
            />
            <Toggle
              label="Sound"
              value={settings.sound ?? false}
              disabled={!settings.notifications}
              onChange={(next) => applySettings({ ...settings, sound: next })}
            />
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={() => handleExport("csv")}
              className="rounded-md border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-800"
            >
              Export CSV
            </button>
            <button
              type="button"
              onClick={() => handleExport("json")}
              className="rounded-md border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-800"
            >
              Export JSON
            </button>
            <button
              type="button"
              onClick={handleClearStats}
              className="rounded-md border border-amber-400 px-3 py-1.5 text-xs font-medium text-amber-600 hover:bg-amber-50 dark:border-amber-500 dark:text-amber-300 dark:hover:bg-amber-500/10"
            >
              Clear Stats
            </button>
            <Toggle
              label="Dark Mode"
              value={isDarkMode}
              onChange={setIsDarkMode}
            />
            <Toggle
              label="Persist Settings"
              value={settings.persist}
              onChange={(next) =>
                applySettings({ ...settings, persist: next }, true)
              }
            />
          </div>
        </div>
        {isSavingSettings ? (
          <p className="text-xs text-indigo-500">Saving settings...</p>
        ) : null}
      </header>

      <section className="flex flex-wrap items-center gap-2">
        {statusChips.map((chip) => (
          <span
            key={chip.value}
            className="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600 dark:bg-slate-800 dark:text-slate-300"
          >
            {chip.label}
          </span>
        ))}
      </section>

      <section className="flex-1 overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-900">
        <div ref={setTableRef} className="max-h-[70vh] overflow-auto">
          <table className="min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-700">
            <thead className="sticky top-0 z-10 bg-slate-50 text-left text-xs font-semibold uppercase tracking-wide text-slate-500 dark:bg-slate-800 dark:text-slate-300">
              <tr>
                <th className="px-4 py-3">Pair</th>
                <th className="px-4 py-3">1m</th>
                <th className="px-4 py-3">3m</th>
                <th className="px-4 py-3">5m</th>
                <th className="px-4 py-3">Total</th>
                <th className="px-4 py-3">Gaps Filled</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Last Update</th>
              </tr>
            </thead>
            <tbody
              className="relative divide-y divide-slate-100 dark:divide-slate-800"
              style={
                filteredPairs.length > 200
                  ? {
                      height: virtualized.totalHeight,
                    }
                  : undefined
              }
            >
              {filteredPairs.length > 200 ? (
                <tr style={{ height: virtualized.offset }} />
              ) : null}
              {visibleRows.map((row) => (
                <tr
                  key={row.pair}
                  className="cursor-pointer bg-white transition-colors hover:bg-indigo-50/40 dark:bg-slate-900 dark:hover:bg-indigo-500/10"
                  onClick={() => setDrawerRow(row)}
                >
                  <td className="whitespace-nowrap px-4 py-3 font-medium text-slate-700 dark:text-slate-200">
                    {row.pair}
                  </td>
                  {TIMEFRAME_OPTIONS.map((tf) => (
                    <td key={tf} className="px-4 py-3">
                      <div className="flex flex-col gap-1">
                        <ProgressBar value={row.tf[tf]} />
                        <span className="text-xs text-slate-500 dark:text-slate-300">
                          {fmtPct(row.tf[tf])}
                        </span>
                      </div>
                    </td>
                  ))}
                  <td className="px-4 py-3">
                    <div className="flex flex-col gap-1">
                      <ProgressBar
                        value={row.total_pct}
                        className="bg-slate-200/60 dark:bg-slate-700"
                      />
                      <span className="text-xs text-slate-500 dark:text-slate-300">
                        {fmtPct(row.total_pct)}
                      </span>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-slate-600 dark:text-slate-300">
                    {row.gaps_filled}
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={row.status} />
                  </td>
                  <td className="px-4 py-3 text-slate-500 dark:text-slate-300">
                    {formatDateTime(row.last_update)}
                  </td>
                </tr>
              ))}
              {filteredPairs.length > 200 ? (
                <tr
                  style={{
                    height: Math.max(
                      0,
                      virtualized.totalHeight -
                        virtualized.offset -
                        visibleRows.length * 60
                    ),
                  }}
                />
              ) : null}
              {!isLoading && visibleRows.length === 0 ? (
                <tr>
                  <td
                    colSpan={8}
                    className="px-4 py-12 text-center text-sm text-slate-500 dark:text-slate-300"
                  >
                    No rows match the current filters.
                  </td>
                </tr>
              ) : null}
              {isLoading ? (
                <tr>
                  <td
                    colSpan={8}
                    className="px-4 py-12 text-center text-sm text-slate-500 dark:text-slate-300"
                  >
                    Loading...
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <PairDrawer
        row={drawerRow}
        history={historyForDrawer}
        onClose={() => setDrawerRow(null)}
      />
    </div>
  );
}

export default DataLoadingStatusTab;
*** End Patch
