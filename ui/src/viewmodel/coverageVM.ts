export type CoverageRow = {
  symbol: string;
  total_required?: number;
  received?: number;
  latest_ts?: string | null;
};

type Snapshot = {
  rows: CoverageRow[];
  lastRefresh: Date | null;
  loading: boolean;
  error?: string;
};

type Listener = (snap: Snapshot) => void;

/**
 * CoverageViewModel:
 * - Maintains snapshot of coverage
 * - Notifies listeners on change (live data view)
 * - Internally uses a 2s tick (can be swapped to SSE/WebSocket later)
 */
export class CoverageViewModel {
  private snap: Snapshot = { rows: [], lastRefresh: null, loading: false };
  private listeners: Set<Listener> = new Set();
  private timer: number | null = null;
  private url: string;

  constructor(url = "/api/report/coverage") {
    this.url = url;
  }

  getSnapshot(): Snapshot {
    return this.snap;
  }

  subscribe(fn: Listener): () => void {
    this.listeners.add(fn);
    // emit current once
    fn(this.snap);
    return () => this.listeners.delete(fn);
  }

  private emit() {
    for (const fn of this.listeners) fn(this.snap);
  }

  async fetchOnce() {
    try {
      this.snap = { ...this.snap, loading: true };
      this.emit();

      const res = await fetch(`${this.url}?t=${Date.now()}`, { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const rows = Array.isArray(data) ? data : (data?.rows ?? []);
      this.snap = { rows, lastRefresh: new Date(), loading: false };
    } catch (e: any) {
      this.snap = { ...this.snap, loading: false, error: e?.message ?? "fetch failed" };
    } finally {
      this.emit();
    }
  }

  start(intervalMs = 2000) {
    if (this.timer != null) return;
    // immediate fetch + schedule
    this.fetchOnce();
    this.timer = window.setInterval(() => this.fetchOnce(), intervalMs);
  }

  stop() {
    if (this.timer != null) {
      window.clearInterval(this.timer);
      this.timer = null;
    }
  }
}

// singleton VM (easy import)
export const coverageVM = new CoverageViewModel();
