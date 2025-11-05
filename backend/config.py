from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    POSTGRES_DSN: str = "postgresql://postgres:2715@postgres:5432/postgres"
    TIMESCALE_DSN: str = "postgresql://postgres:2715@db:5432/postgres"
    REDIS_URL: str = "redis://redis:6379/0"
    ENV: str = "dev"
    FEATURE_FLAGS: str = ""

    # pydantic v2 settings: accept unknown env keys (ignore extra)
    model_config = SettingsConfigDict(
        env_file=".env.compose",
        env_file_encoding="utf-8",
        extra="ignore"
    )

from functools import lru_cache
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
