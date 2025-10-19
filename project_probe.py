import ccxt, pandas as pd

ex = ccxt.binance({"enableRateLimit": True})
ex.load_markets()

# 50 USDT symbols (light test)
symbols = [s for s in ex.symbols if s.endswith("/USDT") and ":" not in s][:50]

rows = []
for sym in symbols:
    data = ex.fetch_ohlcv(sym, timeframe="1m", limit=1)  # last 1 candle
    if data:
        ts,o,h,l,c,v = data[-1]
        rows.append({"symbol": sym, "ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})

df = pd.DataFrame(rows)
df.to_csv("candles_sample.csv", index=False)
print("Wrote", len(df), "rows to candles_sample.csv")
