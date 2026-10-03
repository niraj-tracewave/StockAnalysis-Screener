"""Provider-isolated, Redis-coordinated proxy selection.

Only operator-supplied endpoints are loaded. Health/cooldown and concurrency
leases are shared through Redis so Celery processes do not overload the same
egress IP. Credentials are never included in logs or Redis keys.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import urlsplit, urlunsplit

from redis.asyncio import Redis
from redis.exceptions import RedisError
from dotenv import load_dotenv


logger = logging.getLogger("proxy_pool")
load_dotenv()

_ACQUIRE_SLOT_SCRIPT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit = tonumber(ARGV[1])
if current >= limit then return 0 end
local next_value = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return next_value
"""

_RELEASE_SLOT_SCRIPT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current <= 1 then
    redis.call('DEL', KEYS[1])
    return 0
end
return redis.call('DECR', KEYS[1])
"""


def redact_proxy_url(url: str | None) -> str:
    if not url:
        return "direct"
    parsed = urlsplit(url)
    hostname = parsed.hostname or "unknown"
    port = f":{parsed.port}" if parsed.port else ""
    return urlunsplit((parsed.scheme, f"***@{hostname}{port}", "", "", ""))


def _endpoint_id(url: str | None, name: str | None = None) -> str:
    if name:
        safe_name = "".join(char for char in name.lower() if char.isalnum() or char in "-_")
        if safe_name:
            return safe_name[:48]
    if not url:
        return "direct"
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


@dataclass(slots=True)
class ProxyEndpoint:
    url: str | None
    max_concurrency: int
    weight: int = 1
    name: str | None = None
    failure_count: int = 0
    success_count: int = 0
    latency_ms: float | None = None
    cooldown_until: float = 0.0
    semaphore: asyncio.Semaphore = field(init=False, repr=False)
    endpoint_id: str = field(init=False)

    def __post_init__(self) -> None:
        self.max_concurrency = max(1, self.max_concurrency)
        self.weight = max(1, self.weight)
        self.endpoint_id = _endpoint_id(self.url, self.name)
        self.semaphore = asyncio.Semaphore(self.max_concurrency)


class AsyncProxyPool:
    def __init__(
        self,
        provider: str,
        endpoints: list[ProxyEndpoint],
        *,
        redis_url: str | None = None,
        direct_fallback: bool = True,
        failure_threshold: int = 3,
        cooldown_seconds: int = 60,
        lease_ttl_seconds: int = 45,
        acquire_timeout_seconds: float = 5.0,
        direct_max_concurrency: int = 4,
    ) -> None:
        self.provider = provider
        self.endpoints = endpoints
        self.direct_fallback = direct_fallback
        self.direct_max_concurrency = max(1, direct_max_concurrency)
        if not self.endpoints:
            self.endpoints = [
                ProxyEndpoint(None, self.direct_max_concurrency, name="direct")
            ]
            self.direct_fallback = False
        self.failure_threshold = max(1, failure_threshold)
        self.cooldown_seconds = max(1, cooldown_seconds)
        self.lease_ttl_ms = max(5, lease_ttl_seconds) * 1_000
        self.acquire_timeout_seconds = max(0.1, acquire_timeout_seconds)
        self._cursor = random.randrange(len(self.endpoints))
        self._selection_lock = asyncio.Lock()
        self._redis = (
            Redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
                max_connections=max(20, len(self.endpoints) * 4),
            )
            if redis_url
            else None
        )

    def _health_key(self, endpoint: ProxyEndpoint) -> str:
        return f"proxy:health:{self.provider}:{endpoint.endpoint_id}"

    def _cooldown_key(self, endpoint: ProxyEndpoint) -> str:
        return f"proxy:cooldown:{self.provider}:{endpoint.endpoint_id}"

    def _lease_key(self, endpoint: ProxyEndpoint) -> str:
        return f"proxy:leases:{self.provider}:{endpoint.endpoint_id}"

    def _effective_weight(self, endpoint: ProxyEndpoint) -> int:
        latency_penalty = max(1, int((endpoint.latency_ms or 250) / 250))
        failure_penalty = max(1, endpoint.failure_count + 1)
        return max(1, min(100, endpoint.weight * 10 // (latency_penalty * failure_penalty)))

    async def _select(self, excluded: set[str]) -> ProxyEndpoint | None:
        now = time.monotonic()
        async with self._selection_lock:
            healthy = [
                endpoint
                for endpoint in self.endpoints
                if endpoint.endpoint_id not in excluded and endpoint.cooldown_until <= now
            ]
            if not healthy:
                return None
            weighted = [
                endpoint
                for endpoint in healthy
                for _ in range(self._effective_weight(endpoint))
            ]
            endpoint = weighted[self._cursor % len(weighted)]
            self._cursor += 1
            return endpoint

    async def _is_shared_cooldown(self, endpoint: ProxyEndpoint) -> bool:
        if not self._redis:
            return False
        try:
            return bool(await self._redis.exists(self._cooldown_key(endpoint)))
        except RedisError:
            logger.warning("Redis proxy cooldown check failed; using local health state")
            return False

    async def _acquire_distributed_slot(self, endpoint: ProxyEndpoint) -> bool:
        if not self._redis:
            return True
        try:
            result = await self._redis.eval(
                _ACQUIRE_SLOT_SCRIPT,
                1,
                self._lease_key(endpoint),
                endpoint.max_concurrency,
                self.lease_ttl_ms,
            )
            return bool(result)
        except RedisError:
            logger.warning("Redis proxy lease failed; enforcing process-local limit only")
            return True

    async def _release_distributed_slot(self, endpoint: ProxyEndpoint) -> None:
        if not self._redis:
            return
        try:
            await self._redis.eval(_RELEASE_SLOT_SCRIPT, 1, self._lease_key(endpoint))
        except RedisError:
            logger.warning("Redis proxy lease release failed")

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[ProxyEndpoint]:
        deadline = time.monotonic() + self.acquire_timeout_seconds
        excluded: set[str] = set()
        selected: ProxyEndpoint | None = None
        local_acquired = False
        distributed_acquired = False

        while time.monotonic() < deadline:
            selected = await self._select(excluded)
            if selected is None:
                excluded.clear()
                await asyncio.sleep(0.05)
                continue
            if await self._is_shared_cooldown(selected):
                excluded.add(selected.endpoint_id)
                continue
            try:
                await asyncio.wait_for(selected.semaphore.acquire(), timeout=0.25)
                local_acquired = True
            except TimeoutError:
                excluded.add(selected.endpoint_id)
                continue
            distributed_acquired = await self._acquire_distributed_slot(selected)
            if distributed_acquired:
                break
            selected.semaphore.release()
            local_acquired = False
            excluded.add(selected.endpoint_id)
            await asyncio.sleep(0.02)
        else:
            selected = None

        if selected is None or not local_acquired or not distributed_acquired:
            if not self.direct_fallback:
                raise TimeoutError(f"No healthy {self.provider} proxy capacity available")
            selected = ProxyEndpoint(
                None,
                self.direct_max_concurrency,
                name="direct-fallback",
            )
            await selected.semaphore.acquire()
            local_acquired = True
            distributed_acquired = await self._acquire_distributed_slot(selected)
            if not distributed_acquired:
                selected.semaphore.release()
                raise TimeoutError(
                    f"No healthy {self.provider} direct fallback capacity available"
                )
            logger.warning("%s proxy pool exhausted; using direct fallback", self.provider)

        try:
            yield selected
        finally:
            if distributed_acquired:
                await self._release_distributed_slot(selected)
            if local_acquired:
                selected.semaphore.release()

    async def success(self, endpoint: ProxyEndpoint, latency_ms: float | None = None) -> None:
        endpoint.success_count += 1
        if latency_ms is not None:
            endpoint.latency_ms = latency_ms if endpoint.latency_ms is None else (endpoint.latency_ms * 0.8) + (latency_ms * 0.2)
        endpoint.failure_count = 0
        endpoint.cooldown_until = 0.0
        if not self._redis:
            return
        try:
            async with self._redis.pipeline(transaction=False) as pipeline:
                pipeline.hincrby(self._health_key(endpoint), "success_count", 1)
                pipeline.hset(
                    self._health_key(endpoint),
                    mapping={
                        "failure_count": 0,
                        "latency_ms": round(endpoint.latency_ms or 0, 2),
                        "last_success": int(time.time()),
                    },
                )
                pipeline.expire(self._health_key(endpoint), 86_400)
                pipeline.delete(self._cooldown_key(endpoint))
                await pipeline.execute()
        except RedisError:
            logger.warning("Unable to persist %s proxy success health", self.provider)

    async def failure(self, endpoint: ProxyEndpoint, error_type: str = "request") -> None:
        endpoint.failure_count += 1
        shared_failures = endpoint.failure_count
        if self._redis:
            try:
                shared_failures = await self._redis.hincrby(self._health_key(endpoint), "failure_count", 1)
                await self._redis.hset(
                    self._health_key(endpoint),
                    mapping={"last_failure": int(time.time()), "last_error": error_type[:80]},
                )
                await self._redis.expire(self._health_key(endpoint), 86_400)
            except RedisError:
                logger.warning("Unable to persist %s proxy failure health", self.provider)

        if shared_failures >= self.failure_threshold:
            multiplier = min(shared_failures - self.failure_threshold + 1, 5)
            cooldown = self.cooldown_seconds * multiplier
            endpoint.cooldown_until = time.monotonic() + cooldown
            if self._redis:
                try:
                    await self._redis.set(self._cooldown_key(endpoint), "1", ex=cooldown)
                except RedisError:
                    pass
            logger.warning(
                "%s proxy %s entered %ss cooldown after %s failures",
                self.provider,
                redact_proxy_url(endpoint.url),
                cooldown,
                shared_failures,
            )

    async def health_snapshot(self) -> list[dict[str, object]]:
        snapshot = []
        for endpoint in self.endpoints:
            shared: dict[str, str] = {}
            cooldown_seconds = max(0, round(endpoint.cooldown_until - time.monotonic()))
            active_leases = 0
            if self._redis:
                try:
                    shared = await self._redis.hgetall(self._health_key(endpoint))
                    cooldown_seconds = max(cooldown_seconds, await self._redis.ttl(self._cooldown_key(endpoint)))
                    active_leases = int(await self._redis.get(self._lease_key(endpoint)) or 0)
                except RedisError:
                    pass
            snapshot.append(
                {
                    "id": endpoint.endpoint_id,
                    "endpoint": redact_proxy_url(endpoint.url),
                    "active_leases": active_leases,
                    "max_concurrency": endpoint.max_concurrency,
                    "cooldown_seconds": max(0, cooldown_seconds),
                    "latency_ms": float(shared.get("latency_ms") or endpoint.latency_ms or 0),
                    "failure_count": int(shared.get("failure_count") or endpoint.failure_count),
                }
            )
        return snapshot

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()


def _provider_config(payload: object, provider: str) -> tuple[list[dict[str, object]], bool]:
    if not isinstance(payload, dict):
        return [], True
    provider_value = payload.get(provider, [])
    if isinstance(provider_value, list):
        return [entry for entry in provider_value if isinstance(entry, dict)], True
    if isinstance(provider_value, dict):
        raw_endpoints = provider_value.get("endpoints", [])
        entries = [entry for entry in raw_endpoints if isinstance(entry, dict)] if isinstance(raw_endpoints, list) else []
        return entries, bool(provider_value.get("direct_fallback", True))
    return [], True


def load_proxy_pool(
    provider: str,
    *,
    config_path: str | None = None,
    default_concurrency: int = 12,
    redis_url: str | None = None,
) -> AsyncProxyPool:
    path = Path(config_path or os.environ.get("SCREENER_PROXY_CONFIG", "config/proxies.json"))
    payload: object = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            logger.error("Unable to load proxy configuration %s: %s", path, exc)

    entries, direct_fallback = _provider_config(payload, provider)
    endpoints: list[ProxyEndpoint] = []
    for entry in entries:
        url_env = str(entry.get("url_env") or "")
        configured_url = entry.get("url") or (os.environ.get(url_env) if url_env else None)
        if not configured_url or entry.get("enabled", True) is False:
            continue
        url = str(configured_url)
        if not url.startswith(("http://", "https://")):
            logger.warning("Ignoring unsupported %s proxy scheme", provider)
            continue
        endpoints.append(
            ProxyEndpoint(
                name=str(entry.get("name") or "") or None,
                url=url,
                max_concurrency=int(entry.get("max_concurrency", default_concurrency)),
                weight=max(1, int(entry.get("weight", 1))),
            )
        )

    logger.info("Loaded %s configured endpoints for %s (direct_fallback=%s)", len(endpoints), provider, direct_fallback)
    return AsyncProxyPool(
        provider,
        endpoints,
        redis_url=redis_url or os.environ.get("REDIS_URL"),
        direct_fallback=direct_fallback,
        failure_threshold=int(os.environ.get("PROXY_FAILURE_THRESHOLD", "3")),
        cooldown_seconds=int(os.environ.get("PROXY_COOLDOWN_SECONDS", "60")),
        lease_ttl_seconds=int(os.environ.get("PROXY_LEASE_TTL_SECONDS", "45")),
        acquire_timeout_seconds=float(os.environ.get("PROXY_ACQUIRE_TIMEOUT_SECONDS", "60")),
        direct_max_concurrency=int(
            os.environ.get("YAHOO_DIRECT_MAX_CONCURRENCY", "4")
        ),
    )
