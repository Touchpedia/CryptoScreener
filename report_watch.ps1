$QUERY = "SELECT lower(split_part(symbol,'/',1)) AS pair, c1m AS m1, c3m AS m3, c5m AS m5, total FROM pair_counts_v ORDER BY total DESC, pair;"
while ($true) {
  $ts = Get-Date -Format o
  Write-Host "`n=== $ts ==="
  docker exec -it timescale-mini psql -U postgres -d candles -c "$QUERY"
  Start-Sleep -Seconds 5
}
