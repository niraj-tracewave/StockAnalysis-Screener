from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.postgres.session import get_db_session, external_db_session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async for session in get_db_session():
        yield session

async def get_external_db() -> AsyncGenerator[AsyncSession, None]:
    async with external_db_session_factory() as session:
        yield session