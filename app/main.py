import asyncio

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

angel = AngelWSClient(
    client_id="6e560bba-4d04-464c-ac06-7e5c9c508792",
    access_token="eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IkQ0NjI0NDkiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pFeUxDSnpiM1Z5WTJVaU9pSXpJaXdpWkdWMmFXTmxYMmxrSWpvaU9EQmxOalF6TURndE16bGlaUzB6TkdWa0xXRmpOamd0TmpFNU56ZzFORFk1TXpBM0lpd2lhMmxrSWpvaWRISmhaR1ZmYTJWNVgzWXlJaXdpYjIxdVpXMWhibUZuWlhKcFpDSTZNVElzSW5CeWIyUjFZM1J6SWpwN0ltUmxiV0YwSWpwN0luTjBZWFIxY3lJNkltRmpkR2wyWlNKOUxDSnRaaUk2ZXlKemRHRjBkWE1pT2lKaFkzUnBkbVVpZlgwc0ltbHpjeUk2SW5SeVlXUmxYMnh2WjJsdVgzTmxjblpwWTJVaUxDSnpkV0lpT2lKRU5EWXlORFE1SWl3aVpYaHdJam94TnpZNE9Ua3hNak15TENKdVltWWlPakUzTmpnNU1EUTJOVElzSW1saGRDSTZNVGMyT0Rrd05EWTFNaXdpYW5ScElqb2lPVEV6TVdNelptTXROemhtTkMwME1qSTRMV0U1TlRrdE9XSTFZelJoTldWa056RTNJaXdpVkc5clpXNGlPaUlpZlEuRGZ0ci1ldFI5MF94MzQ4eUt5aTUtQ1ZReE8wQVM2bm9FLXRSWXczYWRMUG1ZeVlkbE5VaFluNmNOeTY4ZFhXX2FuZTZPbm5PcjdBRmlpMEdXVlU4aGx4NHhHVzFGbWlsYVlldjNUMFB3X0ZZZTlXWkM3UmdGdWY2VXZiWUw5cy1jZ3lkWHhHcHZURW44ZXBTQzRMYmpRMWJwN0Vlc0tkQlpFTzFvZTZielVzIiwiQVBJLUtFWSI6Im0xQWs2emV6IiwiWC1PTEQtQVBJLUtFWSI6ZmFsc2UsImlhdCI6MTc2ODkwNDgzMiwiZXhwIjoxNzY4OTMzODAwfQ.EfgjRi46ust_DSKThA3G52nQVL1SE5lVR9UVU9KolF9Vwzh8hKOPgJZ-LLfGHGwcDWm94JnOY82tYItA1_yeGg",
    api_key="m1Ak6zez",
)

angel.connect()

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
