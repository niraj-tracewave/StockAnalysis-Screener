from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings


settings = get_settings()

_pool_options = {
    "pool_pre_ping": True,
    "pool_size": settings.database_pool_size,
    "max_overflow": settings.database_max_overflow,
    "pool_timeout": settings.database_pool_timeout_seconds,
    "pool_recycle": settings.database_pool_recycle_seconds,
}

engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    future=True,
    **_pool_options,
)

external_db_engine = create_async_engine(
    settings.EXTERNAL_DATABASE_URL,
    echo=settings.debug,
    future=True,
    **_pool_options,
)

async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

external_db_session_factory = async_sessionmaker(
    bind=external_db_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session

async def get_external_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with external_db_session_factory() as session:
        yield session
