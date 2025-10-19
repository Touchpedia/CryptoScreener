import os, psycopg2
PG = dict(
  host=os.getenv("PGHOST"),
  port=int(os.getenv("PGPORT")),
  dbname=os.getenv("PGDATABASE"),
  user=os.getenv("PGUSER"),
  password=os.getenv("PGPASSWORD"),
)
print("Trying:", PG["host"], PG["port"], PG["dbname"], PG["user"], "pw="+("*"*len(PG["password"])))
try:
    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute("select version()")
            print("OK:", cur.fetchone()[0].split()[0])
except Exception as e:
    print("FAIL:", repr(e))
