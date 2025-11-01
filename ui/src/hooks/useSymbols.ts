import { useCallback, useEffect, useState } from "react";
import { fetchSymbols } from "../lib/api";

export function useSymbols(segment: string) {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await fetchSymbols(segment);
      setSymbols(result.symbols);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to fetch symbols");
      setSymbols([]);
    } finally {
      setLoading(false);
    }
  }, [segment]);

  useEffect(() => {
    load();
  }, [load]);

  return { symbols, loading, error, refresh: load };
}
