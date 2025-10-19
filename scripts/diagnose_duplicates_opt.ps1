param(
  [string]$Symbol = "ETH/USDT",
  [string]$IntervalTf = "1m"
)

function Invoke-PSQL($sql) {
  docker exec -i cs_db psql -U postgres -d postgres -t -A -F $'\t' -c $sql
}

Write-Host "🔎 Checking duplicates in candles for Symbol='$Symbol' Interval='$IntervalTf'..."

# 1) Overall totals vs distinct-ts
$sql1 = @"
SELECT
  COUNT(*)                            AS total_rows,
  COUNT(DISTINCT ts)                  AS distinct_ts,
  (COUNT(*) - COUNT(DISTINCT ts))     AS duplicate_rows
FROM candles
WHERE symbol = '$Symbol' AND interval = '$IntervalTf';
"@
$totals = Invoke-PSQL $sql1
Write-Host "`n[Totals] total_rows | distinct_ts | duplicate_rows"
Write-Host $totals

# 2) Top duplicate timestamps (if any)
$sql2 = @"
SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS') AS ts_local, COUNT(*) AS c
FROM candles
WHERE symbol = '$Symbol' AND interval = '$IntervalTf'
GROUP BY ts
HAVING COUNT(*) > 1
ORDER BY c DESC, ts DESC
LIMIT 10;
"@
$dups = Invoke-PSQL $sql2
if ([string]::IsNullOrWhiteSpace($dups)) {
  Write-Host "`n✅ No duplicate timestamps found."
} else {
  Write-Host "`n⚠️ Duplicate timestamps (top 10):"
  Write-Host "ts_local`tcount"
  Write-Host $dups
}

# 3) Rows exactly in the latest N=100000 window (to compare with your UI target)
#    This checks distinct ts in latest 100000 bars for this symbol/interval.
$sql3 = @"
WITH latest_window AS (
  SELECT ts
  FROM candles
  WHERE symbol = '$Symbol' AND interval = '$IntervalTf'
  ORDER BY ts DESC
  LIMIT 100000
)
SELECT COUNT(*) AS window_count FROM latest_window;
"@
$windowCount = Invoke-PSQL $sql3
Write-Host "`n🧮 Distinct bars in latest 100000-window: $windowCount"

Write-Host "`nDone."
