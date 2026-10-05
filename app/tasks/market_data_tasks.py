"""Celery orchestration for sharded, resumable market-data ingestion."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from contextlib import contextmanager

from celery import group

from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.core.market_data_pipeline import (
    load_targets,
    refresh_yahoo_market_data_async,
    refresh_yahoo_symbols_async,
)
from app.db.redis.redis import redis_client


TASK_LOCK_TTL_SECONDS = 300
CURSOR_KEY = "market-data:yahoo:quote:offset"
RETRY_KEY = "market-data:yahoo:retry"
RETRY_ATTEMPTS_KEY = "market-data:yahoo:retry-attempts"
DEAD_LETTER_KEY = "market-data:yahoo:dead-letter"
QUARANTINE_KEY = "market-data:yahoo:quarantine"
LAST_DISPATCH_KEY = "market-data:yahoo:last-dispatch"


def run_async_task(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


@contextmanager
def redis_lock(lock_name: str, ttl: int = TASK_LOCK_TTL_SECONDS):
    lock_key = f"celery-lock:{lock_name}"
    lock_value = str(uuid.uuid4())
    acquired = bool(redis_client.set(lock_key, lock_value, nx=True, ex=ttl))
    try:
        yield acquired
    finally:
        if acquired and redis_client.get(lock_key) == lock_value:
            redis_client.delete(lock_key)


def _enqueue_failures(symbols: list[str]) -> None:
    if not symbols:
        return
    pipeline = redis_client.pipeline()
    pipeline.sadd(RETRY_KEY, *symbols)
    for symbol in symbols:
        pipeline.hsetnx(RETRY_ATTEMPTS_KEY, symbol, 0)
    pipeline.execute()


def _active_quarantine() -> set[str]:
    """Return symbols Yahoo currently does not serve, re-probing after expiry."""

    now = int(time.time())
    pipeline = redis_client.pipeline()
    pipeline.zremrangebyscore(QUARANTINE_KEY, 0, now)
    pipeline.zrangebyscore(QUARANTINE_KEY, now + 1, "+inf")
    _, symbols = pipeline.execute()
    return set(symbols or [])


def _quarantine_symbols(symbols: list[str], seconds: int) -> None:
    if not symbols:
        return
    expires_at = int(time.time()) + max(60, seconds)
    pipeline = redis_client.pipeline()
    pipeline.zadd(QUARANTINE_KEY, {symbol: expires_at for symbol in symbols})
    pipeline.srem(RETRY_KEY, *symbols)
    pipeline.srem(DEAD_LETTER_KEY, *symbols)
    pipeline.hdel(RETRY_ATTEMPTS_KEY, *symbols)
    pipeline.execute()


def _complete_retry_batch(requested: list[str], failed: list[str], max_attempts: int) -> None:
    failed_set = set(failed)
    pipeline = redis_client.pipeline()
    for symbol in requested:
        if symbol not in failed_set:
            pipeline.hdel(RETRY_ATTEMPTS_KEY, symbol)
    pipeline.execute()

    for symbol in failed_set:
        attempts = redis_client.hincrby(RETRY_ATTEMPTS_KEY, symbol, 1)
        if attempts >= max_attempts:
            redis_client.sadd(DEAD_LETTER_KEY, symbol)
            redis_client.hdel(RETRY_ATTEMPTS_KEY, symbol)
        else:
            redis_client.sadd(RETRY_KEY, symbol)


@celery_app.task(
    name="refresh_yahoo_quote_shard",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
)
def refresh_yahoo_quote_shard(offset: int, batch_size: int):
    settings = get_settings()
    result = run_async_task(
        refresh_yahoo_market_data_async(
            limit=batch_size,
            offset=offset,
            concurrency=settings.yahoo_concurrency,
            timeout_seconds=settings.yahoo_timeout_seconds,
            retries=settings.yahoo_retries,
            requests_per_second=settings.yahoo_requests_per_second,
            full_history=False,
            proxy_config_path=settings.screener_proxy_config,
            excluded_symbols=_active_quarantine(),
        )
    )
    _enqueue_failures(result["failed_symbols"])
    _quarantine_symbols(
        result["permanent_failed_symbols"],
        settings.yahoo_symbol_quarantine_seconds,
    )
    return result


@celery_app.task(name="refresh_yahoo_quote_batch")
def refresh_yahoo_quote_batch():
    """Reserve and dispatch non-overlapping shards for the next universe wave."""

    settings = get_settings()
    with redis_lock("dispatch_yahoo_quote_shards", ttl=55) as acquired:
        if not acquired:
            return {"status": "already-dispatched"}

        _, total = load_targets(limit=0)
        if total == 0:
            return {"status": "empty-universe", "total": 0}

        cursor = int(redis_client.get(CURSOR_KEY) or 0) % total
        shards: list[tuple[int, int]] = []
        next_offset = cursor
        shard_size = max(1, settings.market_shard_size)
        max_useful_shards = (total + shard_size - 1) // shard_size
        shard_count = min(max(1, settings.market_shard_count), max_useful_shards)
        for _ in range(shard_count):
            if next_offset >= total:
                next_offset = 0
            size = min(shard_size, total - next_offset)
            shards.append((next_offset, size))
            next_offset += size

        next_cursor = 0 if next_offset >= total else next_offset
        redis_client.set(CURSOR_KEY, next_cursor)
        job = group(
            refresh_yahoo_quote_shard.s(offset, size)
            for offset, size in shards
        ).apply_async()
        metrics = {
            "group_id": job.id,
            "dispatched_at": int(time.time()),
            "total": total,
            "shards": shards,
            "next_offset": next_cursor,
        }
        redis_client.set(LAST_DISPATCH_KEY, json.dumps(metrics), ex=3600)
        return metrics


@celery_app.task(
    name="retry_yahoo_quote_failures",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
)
def retry_yahoo_quote_failures():
    settings = get_settings()
    # SPOP atomically reserves disjoint symbols, so multiple workers can drain
    # retry shards concurrently without a global lock or duplicate requests.
    symbols = redis_client.spop(RETRY_KEY, max(1, settings.market_retry_batch_size))
    if not symbols:
        return {"status": "empty", "requested": 0}
    if isinstance(symbols, str):
        symbols = [symbols]

    result = run_async_task(
        refresh_yahoo_symbols_async(
            list(symbols),
            concurrency=settings.yahoo_concurrency,
            timeout_seconds=settings.yahoo_timeout_seconds,
            retries=settings.yahoo_retries,
            requests_per_second=settings.yahoo_requests_per_second,
            proxy_config_path=settings.screener_proxy_config,
        )
    )
    _complete_retry_batch(
        list(symbols),
        result["failed_symbols"],
        settings.market_max_retry_attempts,
    )
    _quarantine_symbols(
        result["permanent_failed_symbols"],
        settings.yahoo_symbol_quarantine_seconds,
    )
    return result


@celery_app.task(name="backfill_yahoo_history_batch", acks_late=True)
def backfill_yahoo_history_batch(offset: int = 0, batch_size: int = 100):
    """Backfill max daily history explicitly on the history queue."""

    settings = get_settings()
    with redis_lock(f"backfill_yahoo_history_batch:{offset}", ttl=3600) as acquired:
        if not acquired:
            return {"status": "already-running", "offset": offset}
        return run_async_task(
            refresh_yahoo_market_data_async(
                limit=batch_size,
                offset=offset,
                concurrency=min(settings.yahoo_concurrency, 8),
                timeout_seconds=max(settings.yahoo_timeout_seconds, 30),
                retries=settings.yahoo_retries,
                requests_per_second=min(settings.yahoo_requests_per_second, 5),
                full_history=True,
                proxy_config_path=settings.screener_proxy_config,
            )
        )
