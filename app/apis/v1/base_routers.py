from fastapi import APIRouter

from app.apis.v1.routers import users
from app.apis.v1.routers import follow_unfollow_company

api_router = APIRouter()

api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(follow_unfollow_company.router, prefix="/follow-unfollow-company", tags=["Follow Unfollow company"])