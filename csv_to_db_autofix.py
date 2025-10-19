import csv, psycopg2

HOST="127.0.0.1"; PORT=5433; USER="postgres"; PASS="2715"

def ensure_schema():
    with psycopg2.connect(host=HOST, port=PORT, dbname="candles", user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE EXTENSION IF NOT EXISTS timescaledb;')
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
            cur.execute("SELECT create_hypertable('candles', by_range('ts'), if_not_exists => TRUE);")
        conn.commit()

def insert_csv(n=10):
    added = 0
    with psycopg2.connect(host=HOST, port=PORT, dbname="candles", user=USER, password=PASS) as conn:
        with conn.cursor() as cur, open("candles_sample.csv", newline="") as f:
            r = csv.DictReader(f)
            for i, row in enumerate(r):
                if i >= n: break
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
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM candles;")
            total = cur.fetchone()[0]
    print(f"Inserted {added} rows. Total rows now: {total}")

if __name__ == "__main__":
    ensure_schema()
    insert_csv(10)
