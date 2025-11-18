import { create } from "zustand";
import { persist } from "zustand/middleware";

import type { ConnectionStatus, PairData } from "../features/status/model";
import { sortTimeframes } from "../lib/timeframes";

function deriveTimeframeKeys(rows: PairData[]): string[] {
  const keys = rows.flatMap((row) => Object.keys(row.timeframes ?? {}));
  return sortTimeframes(keys);
}

type StatusStore = {
  rows: PairData[];
  timeframeKeys: string[];
  isLoading: boolean;
  error?: string;
  filter: string;
  autoRefreshSeconds: number;
  isAutoRefreshEnabled: boolean;
  toastMessage?: string;
  connectionStatus: ConnectionStatus;
  lastUpdated?: string;
  setRows: (rows: PairData[], lastUpdated?: string) => void;
  mergeRow: (row: PairData) => void;
  setLoading: (value: boolean) => void;
  setError: (message?: string) => void;
  setFilter: (value: string) => void;
  setAutoRefreshSeconds: (value: number) => void;
  toggleAutoRefresh: () => void;
  setToast: (message?: string) => void;
  setConnectionStatus: (status: ConnectionStatus) => void;
};

export const useStatusStore = create<StatusStore>()(
  persist(
    (set) => ({
      rows: [],
      timeframeKeys: [],
      isLoading: false,
      error: undefined,
      filter: "",
      autoRefreshSeconds: 5,
      isAutoRefreshEnabled: true,
      connectionStatus: "disconnected",
      setRows: (rows, lastUpdated) =>
        set(() => ({
          rows,
          timeframeKeys: deriveTimeframeKeys(rows),
          lastUpdated: lastUpdated ?? new Date().toISOString(),
          error: undefined,
        })),
      mergeRow: (row) =>
        set((state) => {
          const existingIndex = state.rows.findIndex((item) => item.pair === row.pair);
          let nextRows: PairData[];
          if (existingIndex >= 0) {
            nextRows = state.rows.map((item, index) =>
              index === existingIndex
                ? {
                    ...item,
                    ...row,
                    timeframes: { ...item.timeframes, ...row.timeframes },
                  }
                : item
            );
          } else {
            nextRows = [...state.rows, row];
          }
          return {
            rows: nextRows,
            timeframeKeys: deriveTimeframeKeys(nextRows),
            lastUpdated: row.updatedAt ?? new Date().toISOString(),
          };
        }),
      setLoading: (value) => set({ isLoading: value }),
      setError: (message) => set({ error: message }),
      setFilter: (value) => set({ filter: value }),
      setAutoRefreshSeconds: (value) =>
        set({
          autoRefreshSeconds: Number.isFinite(value) ? Math.max(0, value) : 0,
        }),
      toggleAutoRefresh: () =>
        set((state) => ({
          isAutoRefreshEnabled: !state.isAutoRefreshEnabled,
        })),
      setToast: (message) => set({ toastMessage: message || undefined }),
      setConnectionStatus: (status) => set({ connectionStatus: status }),
    }),
    {
      name: "status-preferences",
      partialize: (state) => ({
        autoRefreshSeconds: state.autoRefreshSeconds,
        isAutoRefreshEnabled: state.isAutoRefreshEnabled,
      }),
    }
  )
);
