from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db


router = APIRouter()


@router.get("/health", summary="Health check for stocks endpoint")
async def stocks_health(db: AsyncSession = Depends(get_db)) -> dict[str, str]:
    # Placeholder to ensure DB dependency works
    _ = db
    return {"status": "ok"}