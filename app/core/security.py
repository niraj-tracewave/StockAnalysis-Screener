from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.apis.models.users_external_db_model import UserExternal
from app.core.jwt_authentication import JWTBearer
from app.core.config import get_settings
from app.db.postgres.session import get_external_db_session

settings = get_settings()
security = HTTPBearer(auto_error=False)

jwt_handler = JWTBearer()

async def optional_get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    external_db: AsyncSession = Depends(get_external_db_session)
) -> int | None:
    if credentials is None:
        return None
    token = credentials.credentials
    payload = await jwt_handler.verify_jwt(token)
    user_id: int = payload.get("user_id")

    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

    stmt = select(UserExternal.id).where(UserExternal.id == user_id)
    result = await external_db.execute(stmt)
    user_exists = result.scalar()

    if not user_exists:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    return user_id
