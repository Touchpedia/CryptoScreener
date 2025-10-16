export type PairStatus = "active" | "paused" | "idle" | "error" | "done";

export type TimeframeProgress = Record<string, number>;

export interface PairData {
  pair: string;
  status: PairStatus;
  gaps: number;
  timeframes: TimeframeProgress;
  updatedAt?: string;
  progress?: number;
}

export interface StatusApiResponse {
  items: PairData[];
  total: number;
  lastUpdated?: string;
}

export interface StatusUpdatePayload {
  pair: string;
  status?: PairStatus;
  gaps?: number;
  timeframes?: TimeframeProgress;
  progress?: number;
  updatedAt?: string;
}

export type ConnectionStatus = "connected" | "connecting" | "disconnected";
