import psycopg2, csv, os

HOST, PORT, USER, PASS, DB = "127.0.0.1", 5434, "postgres", "2715", "candles"
TFs = ["1m","3m","5m"]

def fetch_counts():
    q = """
    SELECT symbol, timeframe, COUNT(*)::bigint
    FROM candles
    WHERE timeframe IN ('1m','3m','5m')
    GROUP BY 1,2;
    """
    with psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PASS) as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            rows = cur.fetchall()
    return rows

def main():
    rows = fetch_counts()
    # build map: symbol -> tf -> count
    data = {}
    for sym, tf, cnt in rows:
        data.setdefault(sym, {t:0 for t in TFs})
        data[sym][tf] = int(cnt)

    # print table
    header = ["Pair","1m","3m","5m","total"]
    print(" | ".join(header))
    for sym in sorted(data.keys()):
        c1, c3, c5 = data[sym]["1m"], data[sym]["3m"], data[sym]["5m"]
        total = c1 + c3 + c5
        pair = sym.split("/")[0].lower()
        print(f"{pair} | {c1} | {c3} | {c5} | {total}")

    # also save CSV (optional)
    os.makedirs("reports", exist_ok=True)
    with open("reports/pair_counts_1m_3m_5m.csv","w",newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for sym in sorted(data.keys()):
            c1, c3, c5 = data[sym]["1m"], data[sym]["3m"], data[sym]["5m"]
            total = c1 + c3 + c5
            pair = sym.split("/")[0].lower()
            w.writerow([pair, c1, c3, c5, total])

if __name__ == "__main__":
    main()
