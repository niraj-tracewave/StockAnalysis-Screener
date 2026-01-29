from pydantic import BaseModel
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    app_name: str = "StockAnalysis Screener API"
    environment: str = "local"
    debug: bool = True

    # Database
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "stock_announcement"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    otp_valid_window: str = "60"
    is_production: bool = True
    access_token_expire_minutes: str = '60'
    refresh_token_expire_days: str = '30'
    secret_key: str = "jhsgdjsgdjgshgdfsdkljlkjdfjklfsdkf"
    ANGLE_ONE_API_KEY: str
    ANGLE_ONE_CLIENT_ID: str
    ANGLE_ONE_CLIENT_CODE: str
    ANGLE_ONE_CLIENT_PASSWORD: str
    ANGLE_ONE_TOTP_SECRET: str

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()