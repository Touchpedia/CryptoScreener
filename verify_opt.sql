SELECT name FROM (
  SELECT table_name AS name
  FROM information_schema.tables
  WHERE table_schema='public' AND table_name='candles_1m'
  UNION ALL
  SELECT matviewname AS name
  FROM pg_matviews
  WHERE schemaname='public' AND matviewname IN ('candles_3m','candles_5m')
) s ORDER BY name;
