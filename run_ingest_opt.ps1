$ErrorActionPreference = 'Stop'
# Make sure logs dir exists
$logDir = Join-Path 'D:\data_pipeline' 'ingest_opt\logs'
New-Item -Force -ItemType Directory -Path $logDir | Out-Null

# Paths
$py  = Join-Path 'D:\data_pipeline' 'ingest_opt\venv\Scripts\python.exe'
$app = Join-Path 'D:\data_pipeline' 'ingest_opt\ingest_opt.py'
$log = Join-Path $logDir ("ingest_" + (Get-Date -Format 'yyyyMMdd') + ".log")

# Run once and append logs (stdout+stderr)
& $py $app *>> $log
