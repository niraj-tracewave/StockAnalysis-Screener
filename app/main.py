import asyncio

from app.core.angel_auto_login import AngelAutoLogin
from app.core.angel_container import angel_container
from app.core.angel_ws import AngelWSClient
from app.core.event_loop import loop_store

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError, HTTPException
from starlette.responses import JSONResponse

from app.apis.v1.base_routers import  api_router
from app.core.config import get_settings
from app.core.custom_error_response import CustomValidationError
from app.core.logging_config import setup_logging, logger
from app.apis.v1.websockets import stock_ws



settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[override]
    setup_logging()
    logger.info("Starting StockAnalysis Screener API")
    loop_store.event_loop = asyncio.get_running_loop()
    yield
    logger.info("Shutting down StockAnalysis Screener API")


auth = AngelAutoLogin()
tokens = auth.login()

print(tokens, "---tokens----")

angel = AngelWSClient(
    client_id="6e560bba-4d04-464c-ac06-7e5c9c508792",
    access_token=tokens["access_token"],
    feed_token=tokens["feed_token"],
    api_key="m1Ak6zez",
)

angel.connect()
angel_container.angel = angel
app = FastAPI(
    title=settings.app_name,
    debug=settings.debug,
    lifespan=lifespan,
)

app.include_router(api_router, prefix="/apis/v1")
app.include_router(stock_ws.router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}

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
