import psycopg2, ccxt

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
SYMBOLS = ["BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT","ADA/USDT","DOGE/USDT","AVAX/USDT","LINK/USDT","TRX/USDT"]
TF = "1m"; LIMIT = 5  # har symbol ke 5 latest candles (halka test)

def ensure_table():
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                  symbol text NOT NULL, timeframe text NOT NULL, ts bigint NOT NULL,
                  open numeric NOT NULL, high numeric NOT NULL, low numeric NOT NULL,
                  close numeric NOT NULL, volume numeric NOT NULL,
                  PRIMARY KEY(symbol,timeframe,ts)
                );
            """)
        conn.commit()

def fetch_all():
    ex = ccxt.binance({"enableRateLimit": True})
    ex.load_markets()
    data = {}
    for s in SYMBOLS:
        rows = ex.fetch_ohlcv(s, timeframe=TF, limit=LIMIT)
        data[s] = rows
    return data

def insert_all(data):
    added = 0
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            for sym, rows in data.items():
                for ts,o,h,l,c,v in rows:
                    cur.execute("""
                        INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (symbol,timeframe,ts) DO NOTHING
                    """, (sym, TF, int(ts), o,h,l,c,v))
                    added += cur.rowcount
        conn.commit()
    return added

if __name__ == "__main__":
    ensure_table()
    data = fetch_all()
    ins = insert_all(data)
    print(f"Inserted {ins} rows across {len(SYMBOLS)} symbols ({TF})")
