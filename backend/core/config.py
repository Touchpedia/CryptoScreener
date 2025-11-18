from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    admin_token: str | None = None
    # direct URLs (preferred)
    database_url: str | None = None
    redis_url: str | None = None

    # legacy pieces to build DB URL if direct not provided
    db_host: str | None = None
    db_port: int | None = None
    db_user: str | None = None
    db_pass: str | None = None
    db_name: str | None = None

    # other legacy knobs (ignored unless used)
    max_threads: int | None = None
    candle_pipe_batch_size: int | None = None
    throttle_min: int | None = None
    throttle_max: int | None = None
    candle_pipe_env_file: str | None = None

    # ingestion defaults
    ingest_lookback_days: int = 365
    ingest_retention_days: int = 365

    model_config = SettingsConfigDict(
        env_prefix='',
        env_file='.env',
        extra='ignore',
    )

    @property
    def DATABASE_URL(self) -> str:
        if self.database_url:
            return self.database_url
        if all([self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name]):
            return f'postgresql+asyncpg://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}'
        return 'postgresql+asyncpg://postgres:postgres@localhost:5432/datapipeline'

    @property
    def REDIS_URL(self) -> str:
        return self.redis_url or 'redis://localhost:6379/0'

    @property
    @property
    def INGEST_LOOKBACK_MS(self) -> int:
        days = max(1, self.ingest_lookback_days)
        return days * 24 * 60 * 60 * 1000

    @property
    def INGEST_RETENTION_MS(self) -> int:
        days = max(1, self.ingest_retention_days)
        return days * 24 * 60 * 60 * 1000


_settings: Settings | None = None

def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


