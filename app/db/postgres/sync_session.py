from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings  # or wherever DB URL lives

settings = get_settings()

DATABASE_URL = settings.database_url.replace("+asyncpg", "")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

SessionLocalSync = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)
