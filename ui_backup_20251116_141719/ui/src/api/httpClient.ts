const trimmed = (import.meta.env.VITE_API_BASE_URL ?? "").trim().replace(/\/$/, "");

function resolveUrl(path: string): string {
  if (/^https?:\/\//i.test(path) || /^ws[s]?:\/\//i.test(path)) {
    return path;
  }
  if (!trimmed) {
    return path.startsWith("/") ? path : `/${path}`;
  }
  if (!path.startsWith("/")) {
    return `${trimmed}/${path}`;
  }
  return `${trimmed}${path}`;
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `HTTP ${response.status}`);
  }
  if (response.status === 204) {
    return {} as T;
  }
  const data = (await response.json()) as T;
  return data;
}

export async function httpGet<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(resolveUrl(path), {
    method: "GET",
    headers: {
      Accept: "application/json",
    },
    signal,
  });
  return handleResponse<T>(response);
}

export async function httpPost<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
  const response = await fetch(resolveUrl(path), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(body),
    signal,
  });
  return handleResponse<T>(response);
}

export function getWebSocketUrl(path: string): string {
  if (/^ws(s)?:\/\//i.test(path)) {
    return path;
  }
  if (trimmed) {
    const base = new URL(trimmed);
    base.protocol = base.protocol === "https:" ? "wss:" : "ws:";
    const normalized = path.startsWith("/") ? path : `/${path}`;
    return `${base.origin}${normalized}`;
  }
  if (typeof window !== "undefined") {
    const { location } = window;
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const normalized = path.startsWith("/") ? path : `/${path}`;
    return `${protocol}//${location.host}${normalized}`;
  }
  return path;
}
