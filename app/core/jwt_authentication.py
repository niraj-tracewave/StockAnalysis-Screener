import jwt
from fastapi import HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime, timedelta

from app.core.config import get_settings

settings = get_settings()

class JWTBearer(HTTPBearer):
    ALGORITHM = "HS256"

    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    # CREATE ACCESS TOKEN (ADDED HERE)
    def create_access_token(self, data: dict) -> str:
        """
        Creates a JWT access token with expiration time.
        """
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(minutes=float(settings.access_token_expire_minutes))
        to_encode.update({"exp": expire})

        encoded_jwt = jwt.encode(
            to_encode,
            settings.secret_key,
            algorithm=self.ALGORITHM
        )
        return encoded_jwt

    def create_refresh_token(self, data: dict) -> str:
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(days=float(settings.refresh_token_expire_days))
        to_encode.update({"exp": expire})

        refresh_jwt = jwt.encode(
            to_encode,
            settings.secret_key,
            algorithm=self.ALGORITHM
        )
        return refresh_jwt

    def generate_access_token_from_refresh(self, refresh_token: str) -> str:
        """
        Decode refresh token → extract payload → create new access token.
        """
        try:
            payload = jwt.decode(
                refresh_token,
                settings.secret_key,
                algorithms=[self.ALGORITHM]
            )
            payload.pop("exp", None)
            new_access_token = self.create_access_token(payload)

            return new_access_token

        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Refresh token expired")

        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

    # VALIDATE TOKEN
    async def __call__(self, request: Request) -> dict:
        credentials: HTTPAuthorizationCredentials = await super(JWTBearer, self).__call__(request)

        if not credentials or credentials.scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid or missing authorization token")

        token = credentials.credentials
        return await self.verify_jwt(token)

    async def verify_jwt(self, token: str) -> dict:
        try:
            payload = jwt.decode(
                token,
                settings.secret_key,
                algorithms=[self.ALGORITHM]
            )
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Access token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid access token")