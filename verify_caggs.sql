SELECT view_name
FROM timescaledb_information.continuous_aggregates
WHERE view_name IN ('candles_3m','candles_5m')
ORDER BY 1;

SELECT matviewname
FROM pg_matviews
WHERE schemaname='public' AND matviewname IN ('candles_3m','candles_5m')
ORDER BY 1;
