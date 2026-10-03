"""Show redacted shared proxy health and active distributed leases."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import get_settings
from app.core.proxy_pool import load_proxy_pool


async def show(provider: str) -> None:
    settings = get_settings()
    pool = load_proxy_pool(
        provider,
        config_path=settings.screener_proxy_config,
        redis_url=settings.redis_url,
    )
    try:
        print(json.dumps({"provider": provider, "proxies": await pool.health_snapshot()}, indent=2))
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("provider", choices=("yahoo", "nse", "bse"), default="yahoo", nargs="?")
    args = parser.parse_args()
    asyncio.run(show(args.provider))


if __name__ == "__main__":
    main()
