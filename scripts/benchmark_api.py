#!/usr/bin/env python3
"""Small reproducible local HTTP load probe with latency percentiles."""

from __future__ import annotations

import argparse
import asyncio
import math
import time
from collections import Counter

import aiohttp


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, math.ceil(len(values) * fraction) - 1)
    return sorted(values)[index]


async def run(url: str, total: int, concurrency: int, timeout: float) -> None:
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    statuses: Counter[int | str] = Counter()

    client_timeout = aiohttp.ClientTimeout(total=timeout)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=client_timeout, connector=connector) as session:
        async def request_once() -> None:
            async with semaphore:
                started = time.perf_counter()
                try:
                    async with session.get(url) as response:
                        await response.read()
                        statuses[response.status] += 1
                except Exception as exc:  # Report failures without aborting the run.
                    statuses[type(exc).__name__] += 1
                finally:
                    latencies.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        await asyncio.gather(*(request_once() for _ in range(total)))
        elapsed = time.perf_counter() - started

    print(
        {
            "url": url,
            "requests": total,
            "concurrency": concurrency,
            "elapsed_seconds": round(elapsed, 3),
            "requests_per_second": round(total / elapsed, 2),
            "latency_ms": {
                "p50": round(percentile(latencies, 0.50), 2),
                "p95": round(percentile(latencies, 0.95), 2),
                "p99": round(percentile(latencies, 0.99), 2),
                "max": round(max(latencies, default=0), 2),
            },
            "statuses": dict(statuses),
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    asyncio.run(run(args.url, args.requests, args.concurrency, args.timeout))


if __name__ == "__main__":
    main()
