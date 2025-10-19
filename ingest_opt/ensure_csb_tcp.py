import os, psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
PG = dict(host=os.getenv("PGHOST","127.0.0.1"), port=int(os.getenv("PGPORT","5433")),
          dbname=os.getenv("PGDATABASE","postgres"), user=os.getenv("PGUSER","postgres"),
          password=os.getenv("PGPASSWORD","2715"))
conn = psycopg2.connect(**PG)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", ("csb_opt",))
if not cur.fetchone():
    cur.execute("CREATE DATABASE csb_opt")
    print("created csb_opt over TCP")
else:
    print("csb_opt already present over TCP")
cur.close(); conn.close()
