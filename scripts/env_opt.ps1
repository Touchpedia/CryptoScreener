# Env Harmonizer (DB_* <-> POSTGRES_*), idempotent
Param(
    [string]$EnvFile = ".env.compose"
)
$ErrorActionPreference = "Stop"
if (-not (Test-Path $EnvFile)) { Write-Host "No $EnvFile found, skipping."; exit 0 }

function Get-DotEnv { param([string]$Path)
  $h=@{}; (Get-Content $Path) | ForEach-Object {
    if ($_ -match "^\s*#") {return}
    if ($_ -match "^\s*$") {return}
    $k,$v = $_ -split "=",2
    $h[$k.Trim()] = $v.Trim()
  }; return $h
}
function Set-DotEnv { param([hashtable]$H,[string]$Path)
  $lines = $H.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }
  Set-Content -Path $Path -Value ($lines -join "`n")
}

$h = Get-DotEnv -Path $EnvFile

# compute POSTGRES_* from DB_* if missing
if (-not $h.ContainsKey("POSTGRES_HOST") -and $h.ContainsKey("DB_HOST")) { $h["POSTGRES_HOST"] = $h["DB_HOST"] }
if (-not $h.ContainsKey("POSTGRES_PORT") -and $h.ContainsKey("DB_PORT")) { $h["POSTGRES_PORT"] = $h["DB_PORT"] }
if (-not $h.ContainsKey("POSTGRES_USER") -and $h.ContainsKey("DB_USER")) { $h["POSTGRES_USER"] = $h["DB_USER"] }
if (-not $h.ContainsKey("POSTGRES_PASSWORD") -and $h.ContainsKey("DB_PASSWORD")) { $h["POSTGRES_PASSWORD"] = $h["DB_PASSWORD"] }
if (-not $h.ContainsKey("POSTGRES_DB") -and $h.ContainsKey("DB_NAME")) { $h["POSTGRES_DB"] = $h["DB_NAME"] }

# compute DATABASE_URL if missing
if (-not $h.ContainsKey("DATABASE_URL")) {
  $ph=$h["POSTGRES_HOST"]; $pp=$h["POSTGRES_PORT"]; $pu=$h["POSTGRES_USER"]; $pw=$h["POSTGRES_PASSWORD"]; $pd=$h["POSTGRES_DB"]
  if ($ph -and $pp -and $pu -and $pw -and $pd) {
    $h["DATABASE_URL"] = ("postgres://{0}:{1}@{2}:{3}/{4}" -f $pu,$pw,$ph,$pp,$pd)
  }
}

Set-DotEnv -H $h -Path "$EnvFile.opt"
Write-Host "Wrote harmonized env -> $(Resolve-Path "$EnvFile.opt")"
