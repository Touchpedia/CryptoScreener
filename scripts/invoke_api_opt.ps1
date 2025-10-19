Param(
  [string]$Base = "http://localhost:8000",
  [string]$AdminToken = $env:ADMIN_TOKEN
)
$ProgressPreference = "SilentlyContinue"
$ErrorActionPreference = "Stop"

function Invoke-Api {
  param(
    [ValidateSet("GET","POST","PUT","PATCH","DELETE")] [string]$Method,
    [string]$Path,
    [object]$Body = $null,
    [hashtable]$Headers = @{}
  )
  if ($AdminToken) { $Headers["X-Admin-Token"] = $AdminToken }

  $uri = "$Base$Path"
  if ($null -ne $Body) {
    $json = ($Body | ConvertTo-Json -Depth 8)
    return Invoke-RestMethod -Method $Method -Uri $uri -ContentType "application/json" -Body $json -Headers $Headers -TimeoutSec 120
  } else {
    return Invoke-RestMethod -Method $Method -Uri $uri -Headers $Headers -TimeoutSec 120
  }
}

function Api-Status {
  Invoke-Api -Method GET -Path "/api/status"
}

function Api-IngestionRun {
  param(
    [int]$TopSymbols = 100,
    [string]$Interval = "1m",
    [int]$CandlesPerSymbol = 6000
  )
  Invoke-Api -Method POST -Path "/api/ingestion/run" -Body @{
    top_symbols = $TopSymbols
    interval = $Interval
    candles_per_symbol = $CandlesPerSymbol
  }
}

function Api-Coverage {
  Invoke-Api -Method GET -Path "/api/report/coverage"
}

function Api-DBFlush {
  Invoke-Api -Method POST -Path "/api/db/flush"
}

# Usage examples:
# .\scripts\invoke_api_opt.ps1; Api-Status
# .\scripts\invoke_api_opt.ps1; Api-IngestionRun -TopSymbols 50 -Interval "1m" -CandlesPerSymbol 2000
# .\scripts\invoke_api_opt.ps1; Api-Coverage
# $env:ADMIN_TOKEN="your_secret"; .\scripts\invoke_api_opt.ps1; Api-DBFlush
