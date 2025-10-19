DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy(
    'candles_3m',
    start_offset => INTERVAL '2 days',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
  );
EXCEPTION WHEN OTHERS THEN
  NULL;
END$$;

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy(
    'candles_5m',
    start_offset => INTERVAL '2 days',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
  );
EXCEPTION WHEN OTHERS THEN
  NULL;
END$$;
