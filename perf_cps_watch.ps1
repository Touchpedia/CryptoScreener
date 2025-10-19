while ($true) {
  $q = @"
WITH now_s AS (SELECT FLOOR(EXTRACT(EPOCH FROM NOW()))::bigint AS s)
SELECT ROUND(COALESCE(SUM(cnt),0)/10.0, 2) AS cps
FROM ingest_rate_wc, now_s
WHERE sec_epoch BETWEEN (now_s.s - 9) AND now_s.s;
"@
  $out = docker exec -it timescale-mini psql -U postgres -d candles -t -A -c "$q"
  $ts = Get-Date -Format HH:mm:ss
  Write-Host "[perf $ts] ~$out candles/sec"
  Start-Sleep -Seconds 5
}
