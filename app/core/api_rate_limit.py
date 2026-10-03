"""Small Redis-backed rate limiter for public API protection.

The limiter deliberately fails open when Redis is unavailable so a cache outage
does not take the read API down. Redis performs the counter update atomically.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp


class RedisRateLimitMiddleware:
    _EXEMPT_PATHS = frozenset({"/health", "/ready", "/docs", "/openapi.json", "/redoc"})

    def __init__(self, app: ASGIApp, redis_url: str, requests_per_minute: int = 600) -> None:
        self.app = app
        self.limit = max(1, requests_per_minute)
        self.redis = Redis.from_url(redis_url, decode_responses=True)

    async def __call__(
        self,
        scope: dict,
        receive: Callable[[], Awaitable[dict]],
        send: Callable[[dict], Awaitable[None]],
    ) -> None:
        if scope["type"] != "http" or scope.get("path") in self._EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        client_ip = request.client.host if request.client else "unknown"
        current_minute = int(time.time() // 60)
        key = f"api-rate:{client_ip}:{current_minute}"
        try:
            count = await self.redis.incr(key)
            if count == 1:
                await self.redis.expire(key, 70)
            if count > self.limit:
                response: Response = JSONResponse(
                    {"detail": "Too many requests. Please retry shortly."},
                    status_code=429,
                    headers={"Retry-After": "60"},
                )
                await response(scope, receive, send)
                return
        except Exception:
            # Availability is preferable to blocking all API traffic on Redis loss.
            pass

        await self.app(scope, receive, send)
