from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "StockAnalysis Screener API"
    environment: str = "local"
    debug: bool = False

    # Database
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "stock_screener"
    postgres_user: str = "stock_apps"
    postgres_password: str
    otp_valid_window: str = "60"
    is_production: bool = False
    access_token_expire_minutes: str = '60'
    refresh_token_expire_days: str = '30'
    secret_key: str
    ANGLE_ONE_API_KEY: str
    ANGLE_ONE_CLIENT_ID: str
    ANGLE_ONE_CLIENT_CODE: str
    ANGLE_ONE_CLIENT_PASSWORD: str
    ANGLE_ONE_TOTP_SECRET: str
    SCRAPINGBEE_API_KEY: str
    EXTERNAL_DATABASE_URL: str
    yahoo_concurrency: int = 12
    yahoo_batch_size: int = 500
    yahoo_timeout_seconds: int = 15
    yahoo_retries: int = 3
    yahoo_requests_per_second: int = 10
    yahoo_symbol_quarantine_seconds: int = 604800
    nse_requests_per_second: int = 4
    bse_requests_per_second: int = 4
    market_shard_count: int = 8
    market_shard_size: int = 250
    market_retry_batch_size: int = 250
    market_max_retry_attempts: int = 3
    screener_worker_concurrency: int = 8
    screener_proxy_config: str = "config/proxies.json"
    enable_legacy_market_jobs: bool = False
    database_pool_size: int = 5
    database_max_overflow: int = 5
    database_pool_timeout_seconds: int = 30
    database_pool_recycle_seconds: int = 1800
    redis_url: str = "redis://localhost:6379/0"
    redis_secondary_url: str = "redis://localhost:6379/10"
    api_rate_limit_per_minute: int = 600
    api_max_concurrency: int = 50

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

@lru_cache
def get_settings() -> Settings:
    return Settings()
