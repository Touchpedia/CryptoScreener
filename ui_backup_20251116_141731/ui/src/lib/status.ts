import type { PairStatus } from "../features/status/model";

export const STATUS_STYLES: Record<PairStatus, string> = {
  active: "bg-emerald-600/20 text-emerald-300 ring-emerald-500/30",
  paused: "bg-amber-600/20 text-amber-300 ring-amber-500/30",
  idle: "bg-sky-600/20 text-sky-300 ring-sky-500/30",
  error: "bg-rose-600/20 text-rose-300 ring-rose-500/30",
  done: "bg-gray-600/20 text-gray-300 ring-gray-500/30",
} as const;

export function sanitizeStatus(status: unknown): PairStatus {
  if (typeof status === "string") {
    const normalized = status.toLowerCase() as PairStatus;
    if (normalized in STATUS_STYLES) {
      return normalized;
    }
  }
  return "idle";
}
