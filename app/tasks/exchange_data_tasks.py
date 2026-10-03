"""Celery orchestration for the NSE/BSE quote pipeline."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from contextlib import contextmanager

from celery import group

from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.core.exchange_data_pipeline import (
    Provider,
    load_exchange_targets,
    refresh_exchange_market_data_async,
    refresh_exchange_symbols_async,
)
from app.db.redis.redis import redis_client


LOCK_TTL_SECONDS = 55


def _key(provider: Provider, suffix: str) -> str:
    return f"market-data:{provider}:{suffix}"


def run_async_task(coroutine):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coroutine)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


@contextmanager
def redis_lock(name: str, ttl: int = LOCK_TTL_SECONDS):
    lock_key = f"celery-lock:{name}"
    value = str(uuid.uuid4())
    acquired = bool(redis_client.set(lock_key, value, nx=True, ex=ttl))
    try:
        yield acquired
    finally:
        if acquired and redis_client.get(lock_key) == value:
            redis_client.delete(lock_key)


def _enqueue_failures(provider: Provider, symbols: list[str]) -> None:
    if not symbols:
        return
    retry_key = _key(provider, "retry")
    attempts_key = _key(provider, "retry-attempts")
    pipeline = redis_client.pipeline()
    pipeline.sadd(retry_key, *symbols)
    for symbol in symbols:
        pipeline.hsetnx(attempts_key, symbol, 0)
    pipeline.execute()


def _active_quarantine(provider: Provider) -> set[str]:
    quarantine_key = _key(provider, "quarantine")
    now = int(time.time())
    pipeline = redis_client.pipeline()
    pipeline.zremrangebyscore(quarantine_key, 0, now)
    pipeline.zrangebyscore(quarantine_key, now + 1, "+inf")
    _, symbols = pipeline.execute()
    return set(symbols or [])


def _quarantine(provider: Provider, symbols: list[str], seconds: int) -> None:
    if not symbols:
        return
    expires_at = int(time.time()) + max(300, seconds)
    retry_key = _key(provider, "retry")
    attempts_key = _key(provider, "retry-attempts")
    dead_key = _key(provider, "dead-letter")
    pipeline = redis_client.pipeline()
    pipeline.zadd(_key(provider, "quarantine"), {symbol: expires_at for symbol in symbols})
    pipeline.srem(retry_key, *symbols)
    pipeline.srem(dead_key, *symbols)
    pipeline.hdel(attempts_key, *symbols)
    pipeline.execute()


def _complete_retry(
    provider: Provider,
    requested: list[str],
    failed: list[str],
    max_attempts: int,
) -> None:
    retry_key = _key(provider, "retry")
    attempts_key = _key(provider, "retry-attempts")
    dead_key = _key(provider, "dead-letter")
    failed_set = set(failed)
    if requested:
        successful = [symbol for symbol in requested if symbol not in failed_set]
        if successful:
            redis_client.hdel(attempts_key, *successful)
    for symbol in failed_set:
        attempts = redis_client.hincrby(attempts_key, symbol, 1)
        if attempts >= max_attempts:
            redis_client.sadd(dead_key, symbol)
            redis_client.hdel(attempts_key, symbol)
        else:
            redis_client.sadd(retry_key, symbol)


def _provider_settings(provider: Provider) -> dict[str, int]:
    settings = get_settings()
    return {
        "concurrency": settings.nse_concurrency if provider == "nse" else settings.bse_concurrency,
        "requests_per_second": (
            settings.nse_requests_per_second
            if provider == "nse"
            else settings.bse_requests_per_second
        ),
        "timeout_seconds": settings.exchange_timeout_seconds,
        "retries": settings.exchange_retries,
    }


@celery_app.task(
    name="refresh_exchange_quote_shard",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 2},
    acks_late=True,
)
def refresh_exchange_quote_shard(provider: Provider, offset: int, batch_size: int):
    settings = get_settings()
    provider_config = _provider_settings(provider)
    result = run_async_task(
        refresh_exchange_market_data_async(
            provider,
            limit=batch_size,
            offset=offset,
            proxy_config_path=settings.screener_proxy_config,
            excluded_symbols=_active_quarantine(provider),
            **provider_config,
        )
    )
    _enqueue_failures(provider, result["failed_symbols"])
    _quarantine(
        provider,
        result["permanent_failed_symbols"],
        settings.exchange_symbol_quarantine_seconds,
    )
    return result


@celery_app.task(name="refresh_exchange_quote_batch")
def refresh_exchange_quote_batch(provider: Provider):
    """Atomically reserve and dispatch the next non-overlapping provider wave."""

    settings = get_settings()
    with redis_lock(f"dispatch_{provider}_quote_shards") as acquired:
        if not acquired:
            return {"status": "already-dispatched", "provider": provider}

        _, total = load_exchange_targets(provider, limit=0)
        if total == 0:
            return {"status": "empty-universe", "provider": provider, "total": 0}

        cursor_key = _key(provider, "quote-offset")
        cursor = int(redis_client.get(cursor_key) or 0) % total
        shard_size = max(1, settings.exchange_shard_size)
        configured_count = (
            settings.nse_shard_count if provider == "nse" else settings.bse_shard_count
        )
        shard_count = min(max(1, configured_count), (total + shard_size - 1) // shard_size)
        shards: list[tuple[int, int]] = []
        next_offset = cursor
        for _ in range(shard_count):
            if next_offset >= total:
                next_offset = 0
            size = min(shard_size, total - next_offset)
            shards.append((next_offset, size))
            next_offset += size

        next_cursor = 0 if next_offset >= total else next_offset
        redis_client.set(cursor_key, next_cursor)
        quote_queue = f"{provider}-quotes"
        job = group(
            refresh_exchange_quote_shard.s(provider, offset, size).set(queue=quote_queue)
            for offset, size in shards
        ).apply_async()
        metrics = {
            "provider": provider,
            "group_id": job.id,
            "dispatched_at": int(time.time()),
            "total": total,
            "shards": shards,
            "next_offset": next_cursor,
        }
        redis_client.set(
            _key(provider, "last-dispatch"),
            json.dumps(metrics),
            ex=3600,
        )
        return metrics


@celery_app.task(
    name="retry_exchange_quote_failures",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 2},
    acks_late=True,
)
def retry_exchange_quote_failures(provider: Provider):
    settings = get_settings()
    retry_key = _key(provider, "retry")
    symbols = redis_client.spop(retry_key, max(1, settings.exchange_retry_batch_size))
    if not symbols:
        return {"status": "empty", "provider": provider, "requested": 0}
    if isinstance(symbols, str):
        symbols = [symbols]

    result = run_async_task(
        refresh_exchange_symbols_async(
            provider,
            list(symbols),
            proxy_config_path=settings.screener_proxy_config,
            **_provider_settings(provider),
        )
    )
    _complete_retry(
        provider,
        list(symbols),
        result["failed_symbols"],
        settings.exchange_max_retry_attempts,
    )
    _quarantine(
        provider,
        result["permanent_failed_symbols"],
        settings.exchange_symbol_quarantine_seconds,
    )
    return result
