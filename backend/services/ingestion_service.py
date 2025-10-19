worker command

 rq worker ingestion-tasks --url redis://localhost:6379/0


backend


cd D:\data_pipeline
.\backend\.venv\Scripts\activate
uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload --app-dir D:\data_pipeline


Venv
cd D:\data_pipeline
.\.venv\Scripts\Activate.ps1



To install venv

 py -3.11 -m venv .venv



python shell







for backup


binance_to_db.bak.py  
multi_symbols_to_db.bak.py  
minute_daemon_multi.bak.py



for revert back

move .\binance_to_db.bak.py .\binance_to_db.py -Force





bhai aik document banao jismain hamari jitni bhi discussion howe hy is main important and not important sry points likho and ham ny abhi tak kia achieve kia hy and kia agay plane kia hy wo bhi likho keon ky mujhy is discussion ko new chat main open kerna hy bcs is chat ka response bohat slow ho gaya hy




one more thing mujhy chat ko heavy nahi kera and main koe professional developer nhi hn to step by step choti choti moves sy target achieve kerna hy, aik sath bohat sary kam nhi batany single task jab tak close nhi hoga next move nahi kerna.




# sab python runs band
Stop-Process -Name python -Force
# ya
taskkill /F /IM python.exe




