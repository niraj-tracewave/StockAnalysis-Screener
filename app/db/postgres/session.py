from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings


settings = get_settings()

engine = create_async_engine(settings.database_url, echo=settings.debug, future=True)

external_db_engine = create_async_engine(
    settings.EXTERNAL_DATABASE_URL,
    echo=settings.debug,
    future=True,
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