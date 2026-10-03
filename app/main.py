import asyncio
from pathlib import Path

from starlette.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

from app.core.event_loop import loop_store

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError, HTTPException
from starlette.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import text

from app.apis.v1.base_routers import  api_router
from app.core.config import get_settings
from app.core.custom_error_response import CustomValidationError
from app.core.logging_config import setup_logging, logger
from app.core.api_rate_limit import RedisRateLimitMiddleware
from app.apis.v1.websockets import stock_ws
from app.core.utils import load_angel_map
from app.db.postgres.session import async_session_factory

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[override]
    setup_logging()
    logger.info("Starting StockAnalysis Screener API")
    load_angel_map()
    loop_store.event_loop = asyncio.get_running_loop()
    yield
    logger.info("Shutting down StockAnalysis Screener API")


app = FastAPI(
    title=settings.app_name,
    debug=settings.debug,
    lifespan=lifespan,
)
app.add_middleware(
    RedisRateLimitMiddleware,
    redis_url=settings.redis_url,
    requests_per_minute=settings.api_rate_limit_per_minute,
)
app.add_middleware(GZipMiddleware, minimum_size=1_000, compresslevel=5)

app.include_router(api_router, prefix="/apis/v1")
app.include_router(stock_ws.router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def readiness() -> JSONResponse:
    checks = {"database": False, "redis": False}
    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = True
    except Exception:
        logger.exception("Database readiness check failed")

    redis = Redis.from_url(settings.redis_url)
    try:
        checks["redis"] = bool(await redis.ping())
    except Exception:
        logger.exception("Redis readiness check failed")
    finally:
        await redis.aclose()

    ready = all(checks.values())
    return JSONResponse(
        {"status": "ready" if ready else "unavailable", "checks": checks},
        status_code=200 if ready else 503,
    )

@app.exception_handler(CustomValidationError)
async def custom_validation_exception_handler(request: Request, exc: CustomValidationError):
    return JSONResponse(
        status_code=200,
        content=exc.get_response_data()
    )


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    if isinstance(exc.detail, dict):
        validations = [exc.detail]
    elif isinstance(exc.detail, list):
        validations = [err if isinstance(err, dict) else {"error": [str(err)]} for err in exc.detail]
    elif isinstance(exc.detail, str):
        validations = [{"error": [exc.detail]}]
    else:
        validations = [{"error": ["An unexpected error occurred."]}]

    platform = request.query_params.get("platform")
    response_data = {
        "meta": {
            "status": False,
            "status_code": exc.status_code,
            "message": "Validation error",
            "validations": validations,
            "message_code": "ERROR"
        },
        "data": {}
    }

    if platform not in ["AndRoiD@Trac50", "IoS@Trac60"]:
        return JSONResponse(status_code=200, content=response_data)

    return JSONResponse(status_code=200, content=response_data)


@app.exception_handler(RequestValidationError)
async def custom_request_validation_exception_handler(request: Request, exc: RequestValidationError):
    error_dict = {}

    for err in exc.errors():
        loc = err.get("loc", [])
        field = loc[-1] if loc else "unknown"
        msg = err.get("msg", "Invalid value")

        error_dict.setdefault(field, []).append(msg)

    platform = request.query_params.get("platform")
    response_data = {
        "meta": {
            "status": False,
            "status_code": 422,
            "message": "Validation error",
            "validations": [error_dict],
            "message_code": "ERROR"
        },
        "data": {}
    }

    if platform and platform not in ["AndRoiD@Trac50", "IoS@Trac60"]:
        return JSONResponse(status_code=200, content=response_data)

    return JSONResponse(status_code=200, content=response_data)


# Never expose the repository root (which contains .env and application source).
static_directory = Path(__file__).resolve().parent / "static"
if static_directory.is_dir():
    app.mount("/static", StaticFiles(directory=static_directory), name="static")
