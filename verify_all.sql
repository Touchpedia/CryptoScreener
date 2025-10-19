SELECT 'candles_1m'
WHERE EXISTS (
  SELECT 1 FROM information_schema.tables
  WHERE table_schema='public' AND table_name='candles_1m'
);

SELECT matviewname
FROM pg_matviews
WHERE schemaname='public' AND matviewname IN ('candles_3m','candles_5m')
ORDER BY 1;
