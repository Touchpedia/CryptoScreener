@echo off
setlocal
cd /d %~dp0
"%~dp0.venv\Scripts\python.exe" -m uvicorn status_api:app --host System.Management.Automation.Internal.Host.InternalHost --port 8000 --workers 1 --no-access-log
