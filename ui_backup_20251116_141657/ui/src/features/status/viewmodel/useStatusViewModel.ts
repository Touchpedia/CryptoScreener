import { useCallback, useEffect, useMemo, useRef } from "react";
import { shallow } from "zustand/shallow";

import { clamp } from "../../../lib/number";
import { statusDataService } from "../services/statusDataService";
import type { PairData } from "../model";
import { useStatusStore } from "../../../store/statusStore";

type FetchOptions = {
  silent?: boolean;
};

export function useStatusViewModel() {
  const {
    rows,
    timeframeKeys,
    isLoading,
    error,
    filter,
    autoRefreshSeconds,
    isAutoRefreshEnabled,
    toastMessage,
    connectionStatus,
    lastUpdated,
  } = useStatusStore(
    (state) => ({
      rows: state.rows,
      timeframeKeys: state.timeframeKeys,
      isLoading: state.isLoading,
      error: state.error,
      filter: state.filter,
      autoRefreshSeconds: state.autoRefreshSeconds,
      isAutoRefreshEnabled: state.isAutoRefreshEnabled,
      toastMessage: state.toastMessage,
      connectionStatus: state.connectionStatus,
      lastUpdated: state.lastUpdated,
    }),
    shallow
  );

  const {
    setRows,
    mergeRow,
    setLoading,
    setError,
    setFilter,
    setAutoRefreshSeconds,
    toggleAutoRefresh,
    setToast,
    setConnectionStatus,
  } = useStatusStore(
    (state) => ({
      setRows: state.setRows,
      mergeRow: state.mergeRow,
      setLoading: state.setLoading,
      setError: state.setError,
      setFilter: state.setFilter,
      setAutoRefreshSeconds: state.setAutoRefreshSeconds,
      toggleAutoRefresh: state.toggleAutoRefresh,
      setToast: state.setToast,
      setConnectionStatus: state.setConnectionStatus,
    }),
    shallow
  );

  const abortRef = useRef<AbortController | null>(null);
  const toastTimerRef = useRef<number | null>(null);

  const fetchStatus = useCallback(
    async (options: FetchOptions = {}) => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      if (!options.silent) {
        setLoading(true);
        setError(undefined);
      }

      try {
        const response = await statusDataService.fetchStatus(controller.signal);
        setRows(response.items, response.lastUpdated);
      } catch (err) {
        if ((err as Error).name === "AbortError") {
          return;
        }
        const message = err instanceof Error ? err.message : "Unable to load status";
        setError(message);
        setToast(message);
      } finally {
        setLoading(false);
        abortRef.current = null;
      }
    },
    [setError, setRows, setToast, setLoading]
  );

  useEffect(() => {
    fetchStatus();
    return () => {
      abortRef.current?.abort();
    };
  }, [fetchStatus]);

  useEffect(() => {
    if (!toastMessage) {
      if (toastTimerRef.current) {
        window.clearTimeout(toastTimerRef.current);
        toastTimerRef.current = null;
      }
      return;
    }
    toastTimerRef.current = window.setTimeout(() => setToast(undefined), 4000);
    return () => {
      if (toastTimerRef.current) {
        window.clearTimeout(toastTimerRef.current);
      }
    };
  }, [toastMessage, setToast]);

  useEffect(() => {
    if (!isAutoRefreshEnabled || autoRefreshSeconds <= 0) {
      return;
    }
    const interval = window.setInterval(() => {
      fetchStatus({ silent: true });
    }, autoRefreshSeconds * 1000);

    return () => window.clearInterval(interval);
  }, [autoRefreshSeconds, isAutoRefreshEnabled, fetchStatus]);

  useEffect(() => {
    let isMounted = true;
    let disconnectTimer: number | null = null;
    let connection: { close: () => void } | null = null;

    const connect = () => {
      if (!isMounted) return;
      setConnectionStatus("connecting");
      connection = statusDataService.connectToStream({
        onOpen: () => {
          if (isMounted) {
            setConnectionStatus("connected");
          }
        },
        onSnapshot: (snapshot) => {
          if (!isMounted) return;
          setRows(snapshot.items, snapshot.lastUpdated);
        },
        onData: (payload: PairData) => {
          if (!isMounted) return;
          mergeRow(payload);
        },
        onError: (error: unknown) => {
          if (!isMounted) return;
          console.error("[status] stream error", error);
          setConnectionStatus("disconnected");
          setToast("Live updates interrupted. Reconnecting…");
          scheduleReconnect();
        },
        onClose: () => {
          if (!isMounted) return;
          setConnectionStatus("disconnected");
          scheduleReconnect();
        },
      });
    };

    const scheduleReconnect = () => {
      if (disconnectTimer !== null) {
        return;
      }
      disconnectTimer = window.setTimeout(() => {
        disconnectTimer = null;
        connect();
      }, 3000);
    };

    connect();

    return () => {
      isMounted = false;
      if (disconnectTimer !== null) {
        window.clearTimeout(disconnectTimer);
      }
      connection?.close();
    };
  }, [mergeRow, setConnectionStatus, setRows, setToast]);

  const filteredRows = useMemo(() => {
    const query = filter.trim().toLowerCase();
    if (!query) return rows;
    return rows.filter((row) => row.pair.toLowerCase().includes(query));
  }, [rows, filter]);

  const handleFilterChange = useCallback(
    (value: string) => {
      setFilter(value);
    },
    [setFilter]
  );

  const handleAutoRefreshChange = useCallback(
    (value: number) => {
      const safeValue = clamp(value, 0, 3600);
      setAutoRefreshSeconds(safeValue);
    },
    [setAutoRefreshSeconds]
  );

  const handleManualRefresh = useCallback(() => {
    fetchStatus();
  }, [fetchStatus]);

  const handleToggleAutoRefresh = useCallback(() => {
    toggleAutoRefresh();
  }, [toggleAutoRefresh]);

  return {
    state: {
      rows: filteredRows,
      timeframeKeys,
      isLoading,
      error,
      filter,
      autoRefreshSeconds,
      isAutoRefreshEnabled,
      toastMessage,
      connectionStatus,
      lastUpdated,
    },
    actions: {
      onFilterChange: handleFilterChange,
      onAutoRefreshChange: handleAutoRefreshChange,
      onManualRefresh: handleManualRefresh,
      onToggleAutoRefresh: handleToggleAutoRefresh,
      dismissToast: () => setToast(undefined),
    },
  };
}
