from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from starlette import status

from app.apis.deps import get_db
from app.apis.v1.schemas import users
from app.apis.v1.services.users import UsersService

router = APIRouter()

@router.post("/login", status_code=status.HTTP_200_OK)
async def login_user(request: users.UserLoginSchema):
    return await UsersService.login(request)

@router.post("/verify-otp", status_code=status.HTTP_200_OK)
async def login_user(request: users.VerifyOtpSchema, db: Session = Depends(get_db)):
    return await UsersService.verify_otp(request, db)

@router.post("/generate-access-token", status_code=status.HTTP_200_OK)
async def generate_access_token(request: users.RefreshTokenSchema):
    return await UsersService.generate_access_token(request)