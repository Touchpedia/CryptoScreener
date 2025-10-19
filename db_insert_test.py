import psycopg2, csv

HOST, PORT, USER, PASS = "127.0.0.1", 5433, "postgres", "2715"

def ensure_schema_plain_pg():
    with psycopg2.connect(host=HOST, port=PORT, dbname="candles", user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            # Plain Postgres table (no timescaledb extension)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                  symbol    text    NOT NULL,
                  timeframe text    NOT NULL,
                  ts        bigint  NOT NULL,
                  open      numeric NOT NULL,
                  high      numeric NOT NULL,
                  low       numeric NOT NULL,
                  close     numeric NOT NULL,
                  volume    numeric NOT NULL,
                  PRIMARY KEY(symbol, timeframe, ts)
                );
            """)
        conn.commit()
    print("Schema OK (plain Postgres)")

def insert_csv_first_10():
    added = 0
    with psycopg2.connect(host=HOST, port=PORT, dbname="candles", user=USER, password=PASS) as conn:
        with conn.cursor() as cur, open("candles_sample.csv", newline="") as f:
            r = csv.DictReader(f)
            for i, row in enumerate(r):
                if i >= 10: break
                cur.execute("""
                    INSERT INTO candles (symbol, timeframe, ts, open, high, low, close, volume)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol, timeframe, ts) DO NOTHING
                """, (
                    row["symbol"], "1m", int(row["ts"]),
                    row["open"], row["high"], row["low"], row["close"], row["volume"]
                ))
                added += cur.rowcount
        conn.commit()
    print(f"Inserted {added} rows")

def show_total():
    with psycopg2.connect(host=HOST, port=PORT, dbname="candles", user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM candles;")
            total = cur.fetchone()[0]
    print("Total rows now:", total)

if __name__ == "__main__":
    ensure_schema_plain_pg()
    insert_csv_first_10()
    show_total()
