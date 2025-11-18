import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

type PairProgress = {
  pair: string
  status?: string | null
  progress?: number | null
  timeframes?: Record<string, number>
  updatedAt?: string | null
}

type RunProgress = {
  run_id: string
  status?: string
  symbol?: string
  timeframe?: string
  step?: number
  total?: number
  percent?: number
  updatedAt?: string
  error?: string
}

type Snapshot = {
  items: PairProgress[]
  total: number
  lastUpdated?: string | null
  run?: RunProgress
}

const STATUS_ENDPOINTS = ['/api/status', 'http://127.0.0.1:8000/api/status']

const formatPercent = (value: number | null | undefined, digits = 0) => {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return 'â€”'
  }
  const factor = 10 ** digits
  const rounded = Math.round(value * factor) / factor
  return `${rounded.toFixed(digits)}%`
}

const formatTime = (value: string | null | undefined) => {
  if (!value) {
    return 'â€”'
  }
  try {
    const date = new Date(value)
    return date.toLocaleTimeString()
  } catch {
    return value
  }
}

const summariseTimeframes = (value: Record<string, number> | undefined) => {
  if (!value || Object.keys(value).length === 0) {
    return ''
  }
  return Object.entries(value)
    .map(([key, pct]) => {
      const normalized = pct > 1 ? pct : pct * 100
      return `${key}: ${Math.round(normalized)}%`
    })
    .join(', ')
}

export default function StatusBoard() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const timerRef = useRef<number | null>(null)
  const hasLoadedRef = useRef(false)

  const fetchStatus = useCallback(async () => {
    if (!hasLoadedRef.current) {
      setLoading(true)
    }
    setError(null)
    let lastError: unknown = null

    for (const endpoint of STATUS_ENDPOINTS) {
      try {
        const response = await fetch(endpoint, { method: 'GET', headers: { Accept: 'application/json' } })
        if (!response.ok) {
          throw new Error(`Status request failed (${response.status})`)
        }
        const data = (await response.json()) as Snapshot
        setSnapshot(data)
        hasLoadedRef.current = true
        setLoading(false)
        return
      } catch (err) {
        lastError = err
        if (!(err instanceof TypeError)) {
          break
        }
      }
    }

    const reason =
      lastError instanceof Error ? lastError.message : 'Unable to reach the API. Check that the backend is running.'
    setError(reason)
    setLoading(false)
  }, [])

  useEffect(() => {
    fetchStatus()
    timerRef.current = window.setInterval(fetchStatus, 5000)

    return () => {
      if (timerRef.current) {
        window.clearInterval(timerRef.current)
      }
    }
  }, [fetchStatus])

  useEffect(() => {
    const handleStart = () => {
      window.setTimeout(fetchStatus, 500)
    }

    window.addEventListener('ingestion:started', handleStart)
    return () => {
      window.removeEventListener('ingestion:started', handleStart)
    }
  }, [fetchStatus])

  const items = useMemo(() => snapshot?.items ?? [], [snapshot])
  const runProgress = snapshot?.run

  return (
    <section className="status-board">
      <header className="status-board__header">
        <h2>Latest Progress</h2>
        <div className="status-board__meta">
          <span>Pairs tracked: {snapshot?.total ?? 0}</span>
          <span>Last update: {formatTime(snapshot?.lastUpdated)}</span>
        </div>
        {runProgress && (
          <div className="status-board__run">
            <span className={`status-pill status-pill--${(runProgress.status ?? 'unknown').toLowerCase()}`}>
              {runProgress.status ?? 'unknown'}
            </span>
            <span>{formatPercent(runProgress.percent ?? 0, 1)}</span>
            <span>
              Step {runProgress.step ?? 0}/{runProgress.total ?? 0}
            </span>
            {runProgress.symbol && runProgress.timeframe && (
              <span>
                {runProgress.symbol} Â· {runProgress.timeframe}
              </span>
            )}
            {runProgress.error && <span className="status-board__run-error">{runProgress.error}</span>}
          </div>
        )}
      </header>

      {loading && !items.length && <p className="status-board__hint">Loading status...</p>}
      {error && <p className="message error">{error}</p>}

      {!loading && !error && !items.length && (
        <p className="status-board__hint">No progress yet. Start an ingestion run to populate status.</p>
      )}

      {items.length > 0 && (
        <div className="status-table">
          <div className="status-table__header">
            <span>Pair</span>
            <span>Status</span>
            <span>Progress</span>
            <span>Last Updated</span>
          </div>
          <div className="status-table__body">
            {items.map((item) => (
              <div className="status-table__row" key={item.pair}>
                <span>{item.pair}</span>
                <span className={`status-pill status-pill--${(item.status ?? 'idle').toLowerCase()}`}>
                  {item.status ?? 'idle'}
                </span>
                <span title={summariseTimeframes(item.timeframes)}>{formatPercent(item.progress)}</span>
                <span>{formatTime(item.updatedAt)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  )
}

// synced 2025-10-20 01:39:11Z

