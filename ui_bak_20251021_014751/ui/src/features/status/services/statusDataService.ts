import { httpGet, httpPost, getWebSocketUrl } from "../../../api/httpClient";
import { sanitizeStatus } from "../../../lib/status";
import type { PairData, StatusApiResponse, StatusUpdatePayload } from "../model";

type RawPair = Record<string, unknown>;
type RawResponse = Record<string, unknown>;

export interface StatusStreamHandlers {
  onData: (data: PairData) => void;
  onSnapshot?: (snapshot: StatusApiResponse) => void;
  onError?: (error: unknown) => void;
  onOpen?: () => void;
  onClose?: () => void;
}

function coerceNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : fallback;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function normalizeTimeframes(value: unknown): Record<string, number> {
  if (!value || typeof value !== "object") {
    return {};
  }
  const entries: Array<[string, number]> = [];
  Object.entries(value as Record<string, unknown>).forEach(([key, raw]) => {
    entries.push([key, coerceNumber(raw, 0)]);
  });
  return Object.fromEntries(entries);
}

function normalizePairData(raw: unknown): PairData | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const record = raw as RawPair;
  const pair =
    typeof record.pair === "string"
      ? record.pair
      : typeof record.symbol === "string"
      ? record.symbol
      : null;
  if (!pair) {
    return null;
  }

  const status = sanitizeStatus(record.status ?? record.state);
  const timeframes =
    "timeframes" in record
      ? normalizeTimeframes(record.timeframes)
      : normalizeTimeframes(record.progress_map ?? record.progress);

  const gaps = coerceNumber(record.gaps ?? record.gaps_filled, 0);
  const updatedAt =
    typeof record.updatedAt === "string"
      ? record.updatedAt
      : typeof record.updated_at === "string"
      ? record.updated_at
      : undefined;
  const progressValue = coerceNumber(record.progress, Number.NaN);
  const progress = Number.isNaN(progressValue) ? undefined : progressValue;

  return {
    pair,
    gaps,
    status,
    timeframes,
    updatedAt,
    progress,
  };
}

function normalizeResponse(raw: RawResponse): StatusApiResponse {
  const items = Array.isArray(raw.items) ? raw.items : [];
  const parsed = items
    .map((item) => normalizePairData(item))
    .filter((item): item is PairData => Boolean(item));

  const total =
    typeof raw.total === "number"
      ? raw.total
      : typeof raw.total === "string"
      ? Number.parseInt(raw.total, 10) || parsed.length
      : parsed.length;

  const lastUpdated =
    typeof raw.lastUpdated === "string"
      ? raw.lastUpdated
      : typeof raw.updatedAt === "string"
      ? raw.updatedAt
      : undefined;

  return {
    items: parsed,
    total,
    lastUpdated,
  };
}

class StatusDataService {
  async fetchStatus(signal?: AbortSignal): Promise<StatusApiResponse> {
    const raw = await httpGet<RawResponse>("api/status", signal);
    return normalizeResponse(raw);
  }

  async updateStatus(payload: StatusUpdatePayload): Promise<void> {
    await httpPost("api/update", payload);
  }

  connectToStream(handlers: StatusStreamHandlers) {
    const url = getWebSocketUrl("/ws/status");
    const socket = new WebSocket(url);

    socket.addEventListener("open", () => {
      handlers.onOpen?.();
    });

    socket.addEventListener("message", (event) => {
      try {
        const parsed = JSON.parse(event.data as string) as Record<string, unknown>;
        const payload = "payload" in parsed ? parsed.payload : parsed;
        const type = typeof parsed.type === "string" ? parsed.type : undefined;

        if (type === "status.snapshot" && payload && typeof payload === "object") {
          handlers.onSnapshot?.(normalizeResponse(payload as RawResponse));
          return;
        }

        const normalized = normalizePairData(payload);
        if (normalized) {
          handlers.onData(normalized);
        }
      } catch (error) {
        handlers.onError?.(error);
      }
    });

    socket.addEventListener("error", (event) => {
      handlers.onError?.(event);
    });

    socket.addEventListener("close", () => {
      handlers.onClose?.();
    });

    return {
      close: () => {
        if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
          socket.close();
        }
      },
    };
  }
}

export const statusDataService = new StatusDataService();
