"""Move non-equity/unmapped symbols out of the active Yahoo retry queue."""

from __future__ import annotations

import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.market_data_pipeline import load_targets_by_symbols
from app.db.redis.redis import redis_client
from app.tasks.market_data_tasks import (
    DEAD_LETTER_KEY,
    QUARANTINE_KEY,
    RETRY_ATTEMPTS_KEY,
    RETRY_KEY,
)


IGNORED_NON_EQUITY_KEY = "market-data:yahoo:ignored-non-equity"


def eligible_symbols(symbols: set[str], chunk_size: int = 1_000) -> set[str]:
    eligible: set[str] = set()
    ordered = sorted(symbols)
    for start in range(0, len(ordered), chunk_size):
        targets, _ = load_targets_by_symbols(ordered[start : start + chunk_size])
        eligible.update(target.yahoo_symbol for target in targets)
    return eligible


def main() -> None:
    retry_symbols = set(redis_client.smembers(RETRY_KEY))
    dead_symbols = set(redis_client.smembers(DEAD_LETTER_KEY))
    candidates = retry_symbols | dead_symbols
    valid = eligible_symbols(candidates)
    ignored = candidates - valid
    quarantined = set(redis_client.zrange(QUARANTINE_KEY, 0, -1))
    duplicate_permanent = candidates & quarantined

    pipeline = redis_client.pipeline()
    if ignored:
        pipeline.sadd(IGNORED_NON_EQUITY_KEY, *ignored)
        pipeline.srem(RETRY_KEY, *ignored)
        pipeline.srem(DEAD_LETTER_KEY, *ignored)
        pipeline.hdel(RETRY_ATTEMPTS_KEY, *ignored)
    if duplicate_permanent:
        pipeline.srem(RETRY_KEY, *duplicate_permanent)
        pipeline.srem(DEAD_LETTER_KEY, *duplicate_permanent)
        pipeline.hdel(RETRY_ATTEMPTS_KEY, *duplicate_permanent)
    pipeline.execute()

    print(
        json.dumps(
            {
                "active_retry_before": len(retry_symbols),
                "dead_letter_before": len(dead_symbols),
                "ignored_non_equity": len(ignored),
                "removed_quarantine_duplicates": len(duplicate_permanent),
                "active_retry_after": redis_client.scard(RETRY_KEY),
                "dead_letter_after": redis_client.scard(DEAD_LETTER_KEY),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
