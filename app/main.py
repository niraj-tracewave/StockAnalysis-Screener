from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.apis.v1.base_routers import  api_router
from app.core.config import get_settings
from app.core.logging_config import setup_logging, logger


settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[override]
    setup_logging()
    logger.info("Starting StockAnalysis Screener API")
    yield
    logger.info("Shutting down StockAnalysis Screener API")


app = FastAPI(
    title=settings.app_name,
    debug=settings.debug,
    lifespan=lifespan,
)

app.include_router(api_router, prefix="/apis/v1")


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}