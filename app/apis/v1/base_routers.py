from fastapi import APIRouter

from app.apis.v1.routers import stocks

api_router = APIRouter()

api_router.include_router(stocks.router, prefix="/stocks", tags=["stocks"])