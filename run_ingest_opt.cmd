@echo off
setlocal
set BASE=D:\data_pipeline
set LOGDIR=D:\data_pipeline\ingest_opt\logs
set PY="D:\data_pipeline\ingest_opt\venv\Scripts\python.exe"
set APP="D:\data_pipeline\ingest_opt\ingest_opt.py"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOG=%LOGDIR%\ingest_%DATE:~10,4%%DATE:~4,2%%DATE:~7,2%.log
"%PY%" "%APP%" >> "%LOG%" 2>&1
endlocal
