function Invoke-PSQL($sql) { docker exec -i cs_db psql -U postgres -d postgres -v "ON_ERROR_STOP=1" -c "$sql" }

Write-Host "🔧 Removing duplicate rows from candles..."
$sqlDedupe = @"
WITH d AS (
  SELECT ctid
  FROM (
    SELECT ctid,
           ROW_NUMBER() OVER (PARTITION BY symbol, interval, ts ORDER BY ts) AS rn
    FROM candles
  ) t
  WHERE rn > 1
)
DELETE FROM candles c
USING d
WHERE c.ctid = d.ctid;
"@
Invoke-PSQL $sqlDedupe

Write-Host "🔒 Creating unique index on (symbol, interval, ts) if missing..."
# For Timescale hypertable this is valid and prevents future dupes.
$sqlIdx = @"
CREATE UNIQUE INDEX IF NOT EXISTS ux_candles_symbol_interval_ts
ON candles(symbol, interval, ts);
"@
Invoke-PSQL $sqlIdx

Write-Host "✅ Done. Now re-check counts."
