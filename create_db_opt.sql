DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = ''csb_opt'') THEN
    EXECUTE ''CREATE DATABASE csb_opt'';
  END IF;
END$$;
