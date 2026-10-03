import json
import asyncio
import os

import pytest
import redis
from app.core.proxy_pool import AsyncProxyPool, ProxyEndpoint, load_proxy_pool, redact_proxy_url


def test_proxy_credentials_are_redacted():
    redacted = redact_proxy_url("http://alice:secret@proxy.example:8080")
    assert "alice" not in redacted
    assert "secret" not in redacted
    assert "proxy.example:8080" in redacted


def test_provider_pools_are_isolated(tmp_path):
    config = tmp_path / "proxies.json"
    config.write_text(
        json.dumps(
            {
                "yahoo": [{"url": "http://yahoo.example:8080"}],
                "nse": [{"url": "http://nse.example:8080"}],
            }
        ),
        encoding="utf-8",
    )

    yahoo = load_proxy_pool("yahoo", config_path=str(config))
    nse = load_proxy_pool("nse", config_path=str(config))

    assert yahoo.endpoints[0].url == "http://yahoo.example:8080"
    assert nse.endpoints[0].url == "http://nse.example:8080"


def test_proxy_url_can_come_from_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_YAHOO_PROXY", "http://env-proxy.example:8080")
    config = tmp_path / "proxies.json"
    config.write_text(
        json.dumps(
            {
                "yahoo": {
                    "direct_fallback": False,
                    "endpoints": [
                        {"name": "env-one", "url_env": "TEST_YAHOO_PROXY", "weight": 2}
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    pool = load_proxy_pool("yahoo", config_path=str(config))
    assert pool.endpoints[0].url == "http://env-proxy.example:8080"
    assert pool.endpoints[0].weight == 2
    assert pool.direct_fallback is False


def test_direct_concurrency_is_provider_specific(tmp_path, monkeypatch):
    monkeypatch.setenv("NSE_DIRECT_MAX_CONCURRENCY", "9")
    config = tmp_path / "proxies.json"
    config.write_text('{"nse": {"direct_fallback": true, "endpoints": []}}')

    pool = load_proxy_pool("nse", config_path=str(config))

    assert pool.endpoints[0].max_concurrency == 9


def test_unhealthy_endpoint_enters_cooldown_and_rotates():
    async def exercise():
        first = ProxyEndpoint("http://one.example:8080", 1, name="one")
        second = ProxyEndpoint("http://two.example:8080", 1, name="two")
        pool = AsyncProxyPool(
            "yahoo",
            [first, second],
            failure_threshold=1,
            cooldown_seconds=60,
            direct_fallback=False,
        )
        async with pool.lease() as selected:
            await pool.failure(selected, "test")
            failed_id = selected.endpoint_id
        async with pool.lease() as selected:
            assert selected.endpoint_id != failed_id

    asyncio.run(exercise())


@pytest.mark.skipif(
    os.environ.get("RUN_REDIS_INTEGRATION") != "1",
    reason="requires the local Redis integration service",
)
def test_distributed_proxy_concurrency_is_shared_between_pools():
    redis_url = "redis://localhost:6379/0"
    lease_key = "proxy:leases:test-distributed:shared"
    redis.Redis.from_url(redis_url).delete(lease_key)

    async def exercise():
        pool_one = AsyncProxyPool(
            "test-distributed",
            [ProxyEndpoint("http://one.example:8080", 1, name="shared")],
            redis_url=redis_url,
            direct_fallback=False,
            acquire_timeout_seconds=0.15,
        )
        pool_two = AsyncProxyPool(
            "test-distributed",
            [ProxyEndpoint("http://one.example:8080", 1, name="shared")],
            redis_url=redis_url,
            direct_fallback=False,
            acquire_timeout_seconds=0.15,
        )
        try:
            async with pool_one.lease():
                with pytest.raises(TimeoutError):
                    async with pool_two.lease():
                        pass
            async with pool_two.lease() as endpoint:
                assert endpoint.endpoint_id == "shared"
        finally:
            await pool_one.close()
            await pool_two.close()

    try:
        asyncio.run(exercise())
    finally:
        redis.Redis.from_url(redis_url).delete(lease_key)
